using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using Qado.Blockchain;
using Qado.Serialization;
using Qado.Logging;
using Qado.Mempool;
using Qado.Storage;

namespace Qado.Networking
{
    public static class BlockSyncStarter
    {
        private const int DefaultP2PPort = GenesisConfig.P2PPort;
        private const int MaxFramePayloadBytes = ConsensusRules.MaxBlockSizeBytes;
        private static readonly TimeSpan FrameTimeout = TimeSpan.FromSeconds(8);
        private static readonly TimeSpan NoPeersBaseDelay = TimeSpan.FromSeconds(6);
        private static readonly TimeSpan NoTipBaseDelay = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan CatchupStallBaseDelay = TimeSpan.FromSeconds(4);
        private static readonly TimeSpan InSyncBaseDelay = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan CoolingDownBaseDelay = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan MaxIdleDelay = TimeSpan.FromMinutes(2);
        private const int MaxIdleBackoffShift = 4; // up to 16x base delay
        private const double IdleJitterFraction = 0.20;
        private static readonly SemaphoreSlim ImmediateSyncSignal = new(0, 1);
        private static readonly object ImmediateSyncGate = new();
        private static readonly TimeSpan ImmediateSyncCooldown = TimeSpan.FromSeconds(5);
        private static DateTime _lastImmediateSyncUtc = DateTime.MinValue;

        public static async Task StartAsync(MempoolManager mempool, ILogSink? log = null, CancellationToken ct = default)
        {
            if (Db.Connection == null)
            {
                log?.Error("BlockSync", "Db.Connection is null. Cannot sync.");
                return;
            }

            ulong lastHeight = GetLatestHeightDirect();
            int idleRounds = 0;

            while (!ct.IsCancellationRequested)
            {
                var peers = GetPeersFromDb();
                bool madeProgress = false;
                int dialAttempts = 0;
                int skippedCoolingDown = 0;
                bool sawValidTip = false;
                bool sawPeerAhead = false;

                foreach (var (rawHost, port0) in peers)
                {
                    if (ct.IsCancellationRequested) return;

                    var host = NormalizeHost(rawHost);
                    var port = port0 > 0 ? port0 : DefaultP2PPort;
                    var peerKey = NormalizePeerKey(host);

                    if (PeerFailTracker.ShouldBan(peerKey))
                    {
                        skippedCoolingDown++;
                        continue;
                    }

                    dialAttempts++;

                    try
                    {
                        log?.Info("BlockSync", $"Connect {host}:{port} (IPv4) ...");
                        using var client = new TcpClient(AddressFamily.InterNetwork);
#if NET8_0_OR_GREATER
                        await client.ConnectAsync(host, port, ct).ConfigureAwait(false);
#else
                        await client.ConnectAsync(host, port).ConfigureAwait(false);
#endif
                        client.NoDelay = true;
                        using var ns = client.GetStream();

                        var myHs = BuildHandshakePayload(DefaultP2PPort);
                        await WriteFrame(ns, MsgType.Handshake, myHs, ct).ConfigureAwait(false);

                        await WriteFrame(ns, MsgType.GetTip, Array.Empty<byte>(), ct).ConfigureAwait(false);
                        var (tipOk, remoteHeight, remoteTipHash) = await WaitForTipAsync(ns, client, log, ct).ConfigureAwait(false);
                        if (!tipOk)
                        {
                            PeerFailTracker.ReportFailure(peerKey);
                            log?.Warn("BlockSync", "Peer did not provide a valid TIP.");
                            continue;
                        }

                        PeerFailTracker.ReportSuccess(peerKey);
                        sawValidTip = true;

                        ulong localHeight = GetCanonicalTipHeight();
                        if (remoteHeight <= localHeight)
                        {
                            log?.Info("BlockSync", $"Already up-to-date (local={localHeight}, remote={remoteHeight}).");
                            if (remoteTipHash is { Length: 32 })
                            {
                                try { ChainSelector.MaybeAdoptNewTip(remoteTipHash, log); } catch { }
                            }
                            continue;
                        }

                        sawPeerAhead = true;

                        ulong ancestor = await FindCommonAncestorAsync(ns, remoteHeight, localHeight, log, ct).ConfigureAwait(false);
                        log?.Info("BlockSync", $"Common ancestor at h={ancestor}. Sync {ancestor + 1}..{remoteHeight}");

                        byte[] expectedPrevHash = await GetAncestorHashAsync(ns, ancestor, log, ct).ConfigureAwait(false);
                        byte[]? downloadedTipHash = null;

                        for (ulong h = ancestor + 1; h <= remoteHeight; h++)
                        {
                            if (ct.IsCancellationRequested) return;

                            var req = new byte[8];
                            BinaryPrimitives.WriteUInt64LittleEndian(req, h);

                            await WriteFrame(ns, MsgType.GetBlock, req, ct).ConfigureAwait(false);

                            var blk = await WaitForBlockAtStrictAsync(ns, h, expectedPrevHash, log, ct).ConfigureAwait(false);
                            if (blk == null)
                            {
                                log?.Warn("BlockSync", $"Failed to fetch/validate block h={h}.");
                                break;
                            }

                            blk.BlockHeight = h;
                            EnsureCanonicalBlockHash(blk);
                            if (!BlockValidator.ValidateNetworkSideBlockStateless(blk, out var reason))
                            {
                                log?.Warn("BlockSync", $"Fetched block h={h} failed validation: {reason}");
                                break;
                            }

                            try
                            {
                                var txOffsets = ComputeTxOffsets(blk);

                                lock (Db.Sync)
                                {
                                    using var trx = Db.Connection!.BeginTransaction();

                                    BlockStore.SaveBlock(blk, trx);

                                    for (int i = 0; i < txOffsets.Length; i++)
                                    {
                                        var (id, off, size) = txOffsets[i];
                                        TxIndexStore.Insert(id, blk.BlockHash!, blk.BlockHeight, off, size, trx);
                                    }

                                    trx.Commit();
                                }
                            }
                            catch (Exception ex)
                            {
                                log?.Warn("BlockSync", $"Store h={h} failed: {ex.Message}");
                                break;
                            }

                            expectedPrevHash = blk.BlockHash!;
                            downloadedTipHash = blk.BlockHash!;

                            var after = GetLatestHeightDirect();
                            log?.Info("BlockSync", $"Synced {h}/{remoteHeight} (dbMax={after})");

                            if (after < h)
                            {
                                log?.Error("BlockSync",
                                    $"DB did not retain inserts: after commit MAX(height)={after}, expected >= {h}.");
                                break;
                            }

                            if (after > lastHeight) { madeProgress = true; lastHeight = after; }
                        }

                        var downloadedNow = GetLatestHeightDirect();
                        if (downloadedNow >= remoteHeight)
                        {
                            log?.Info("BlockSync", "Download done -> consider adoption ...");

                            if (downloadedTipHash is { Length: 32 } && remoteTipHash is { Length: 32 } &&
                                !BytesEqual(downloadedTipHash, remoteTipHash))
                            {
                                log?.Warn("BlockSync", "Peer TIP hash mismatched downloaded chain tip. Using downloaded tip for adoption.");
                            }

                            var candidateTip = downloadedTipHash ?? remoteTipHash;
                            if (candidateTip is { Length: 32 })
                            {
                                try { ChainSelector.MaybeAdoptNewTip(candidateTip, log); } catch { }
                            }

                            var canonNow = GetCanonicalTipHeight();
                            var canonTipHash = BlockStore.GetCanonicalHashAtHeight(canonNow);
                            bool canonHeightOk = canonNow >= remoteHeight;
                            bool canonTipOk = candidateTip is not { Length: 32 } ||
                                              (canonTipHash is { Length: 32 } && BytesEqual(canonTipHash, candidateTip));

                            if (canonHeightOk && canonTipOk)
                            {
                                log?.Info("BlockSync",
                                    $"Canonical sync complete (canon={canonNow}, remote={remoteHeight}).");
                                TryNotifyUiRefresh();
                                break;
                            }

                            log?.Warn("BlockSync",
                                $"Canonical sync incomplete after adoption (canon={canonNow}, remote={remoteHeight}). Retrying ...");
                        }
                        else
                        {
                            log?.Warn("BlockSync",
                                $"Download incomplete (dbMax={downloadedNow}, remote={remoteHeight}). Retrying ...");
                        }
                    }
                    catch (Exception ex)
                    {
                        PeerFailTracker.ReportFailure(peerKey);
                        log?.Warn("BlockSync", $"Peer {host}:{port} failed: {ex.Message}");
                    }
                }

                if (madeProgress)
                {
                    idleRounds = 0;
                    continue;
                }

                idleRounds++;
                var (idleDelay, idleReason) = ComputeIdleDelay(
                    peers.Count,
                    dialAttempts,
                    skippedCoolingDown,
                    sawValidTip,
                    sawPeerAhead,
                    idleRounds);

                int sleepSecs = Math.Max(1, (int)Math.Round(idleDelay.TotalSeconds));
                log?.Info("BlockSync", $"No sync progress ({idleReason}). Sleep {sleepSecs}s ...");
                bool wokeBySignal = false;
                try
                {
                    wokeBySignal = await WaitForIdleOrImmediateSignalAsync(idleDelay, ct).ConfigureAwait(false);
                }
                catch { }

                if (wokeBySignal)
                {
                    idleRounds = 0;
                    log?.Info("BlockSync", "Immediate sync wake-up.");
                }
            }

            log?.Warn("BlockSync", "Cancelled.");
        }

        public static void RequestImmediateSync(ILogSink? log = null, string reason = "handshake")
        {
            bool allowed;
            lock (ImmediateSyncGate)
            {
                var now = DateTime.UtcNow;
                allowed = (now - _lastImmediateSyncUtc) >= ImmediateSyncCooldown;
                if (allowed)
                    _lastImmediateSyncUtc = now;
            }

            if (!allowed)
                return;

            try { ImmediateSyncSignal.Release(); } catch (SemaphoreFullException) { }

            if (!string.IsNullOrWhiteSpace(reason))
                log?.Info("BlockSync", $"Immediate sync requested ({reason}).");
            else
                log?.Info("BlockSync", "Immediate sync requested.");
        }

        private static (TimeSpan delay, string reason) ComputeIdleDelay(
            int peerCount,
            int dialAttempts,
            int skippedCoolingDown,
            bool sawValidTip,
            bool sawPeerAhead,
            int idleRounds)
        {
            TimeSpan baseDelay;
            string reason;

            if (peerCount == 0)
            {
                baseDelay = NoPeersBaseDelay;
                reason = "no-known-peers";
            }
            else if (dialAttempts == 0 && skippedCoolingDown > 0)
            {
                baseDelay = CoolingDownBaseDelay;
                reason = "all-peers-cooling-down";
            }
            else if (sawValidTip && !sawPeerAhead)
            {
                baseDelay = InSyncBaseDelay;
                reason = "in-sync";
            }
            else if (sawPeerAhead)
            {
                baseDelay = CatchupStallBaseDelay;
                reason = "catchup-stalled";
            }
            else
            {
                baseDelay = NoTipBaseDelay;
                reason = "peer-errors";
            }

            int shift = Math.Clamp(idleRounds - 1, 0, MaxIdleBackoffShift);
            long ticks = baseDelay.Ticks;
            for (int i = 0; i < shift; i++)
            {
                if (ticks > long.MaxValue / 2)
                {
                    ticks = long.MaxValue;
                    break;
                }
                ticks *= 2;
            }

            if (ticks > MaxIdleDelay.Ticks)
                ticks = MaxIdleDelay.Ticks;

            var withBackoff = TimeSpan.FromTicks(ticks);
            return (ApplyIdleJitter(withBackoff), reason);
        }

        private static TimeSpan ApplyIdleJitter(TimeSpan delay)
        {
            if (delay <= TimeSpan.Zero || IdleJitterFraction <= 0)
                return delay;

            double delta = (Random.Shared.NextDouble() * 2.0 - 1.0) * IdleJitterFraction;
            double factor = 1.0 + delta;
            long ticks = (long)(delay.Ticks * factor);

            long minTicks = TimeSpan.FromMilliseconds(500).Ticks;
            if (ticks < minTicks) ticks = minTicks;
            if (ticks > MaxIdleDelay.Ticks) ticks = MaxIdleDelay.Ticks;

            return TimeSpan.FromTicks(ticks);
        }

        private static async Task<bool> WaitForIdleOrImmediateSignalAsync(TimeSpan idleDelay, CancellationToken ct)
        {
            if (idleDelay < TimeSpan.Zero)
                idleDelay = TimeSpan.Zero;

            try
            {
                return await ImmediateSyncSignal.WaitAsync(idleDelay, ct).ConfigureAwait(false);
            }
            catch
            {
                return false;
            }
        }

        private static ulong GetLatestHeightDirect()
        {
            if (Db.Connection == null) return 0;

            lock (Db.Sync)
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = "SELECT IFNULL(MAX(height), 0) FROM block_index;";
                var v = cmd.ExecuteScalar();
                return v is long l ? (ulong)l : 0UL;
            }
        }

        private static ulong GetCanonicalTipHeight()
        {
            if (Db.Connection == null) return 0;
            lock (Db.Sync)
            {
                return BlockStore.GetLatestHeight();
            }
        }

        private static async Task<ulong> FindCommonAncestorAsync(
            NetworkStream ns,
            ulong remoteHeight,
            ulong localHeight,
            ILogSink? log,
            CancellationToken ct)
        {
            ulong hi = Math.Min(remoteHeight, localHeight);

            var localHashes = new Dictionary<ulong, byte[]>((int)Math.Min(hi + 1, 1_000_000));
            for (ulong h = 0; h <= hi; h++)
            {
                var b = BlockStore.GetBlockByHeight(h);
                if (b == null) break;
                EnsureCanonicalBlockHash(b);
                localHashes[h] = b.BlockHash!;
            }

            for (ulong h = hi + 1; h > 0; h--)
            {
                ulong i = h - 1;

                var remote = await RequestBlockAtLooseAsync(ns, i, ct).ConfigureAwait(false);
                if (remote == null)
                {
                    log?.Warn("BlockSync", $"Remote did not provide h={i}. Fallback: 0");
                    return 0;
                }
                EnsureCanonicalBlockHash(remote);

                if (localHashes.TryGetValue(i, out var lh) && BytesEqual(lh, remote.BlockHash!))
                    return i;
            }

            return 0;
        }

        private static async Task<byte[]> GetAncestorHashAsync(NetworkStream ns, ulong ancestorHeight, ILogSink? log, CancellationToken ct)
        {
            var b = BlockStore.GetBlockByHeight(ancestorHeight);
            if (b != null)
            {
                EnsureCanonicalBlockHash(b);
                if (b.BlockHash is { Length: 32 }) return b.BlockHash!;
            }

            var remote = await RequestBlockAtLooseAsync(ns, ancestorHeight, ct).ConfigureAwait(false);
            if (remote == null)
                throw new InvalidOperationException($"Ancestor h={ancestorHeight} not available locally nor remotely.");

            EnsureCanonicalBlockHash(remote);
            log?.Warn("BlockSync", $"Ancestor h={ancestorHeight} fetched remote (local missing).");
            return remote.BlockHash!;
        }

        private static bool BytesEqual(byte[]? a, byte[]? b)
        {
            if (a == null || b == null) return a == b;
            if (a.Length != b.Length) return false;
            for (int i = 0; i < a.Length; i++) if (a[i] != b[i]) return false;
            return true;
        }

        private static async Task<Block?> WaitForBlockAtStrictAsync(
            NetworkStream ns, ulong expectedHeight, byte[] expectedPrevHash, ILogSink? log, CancellationToken ct)
        {
            var deadline = DateTime.UtcNow + FrameTimeout;

            while (DateTime.UtcNow < deadline)
            {
                var fr = await ReadFrameWithTimeout(ns, ct).ConfigureAwait(false);
                if (fr.payload == null) break;
                if (fr.type != MsgType.BlockAt) continue;

                try
                {
                    var blk = BlockBinarySerializer.Read(fr.payload);

                    if (blk.BlockHeight != 0 && blk.BlockHeight != expectedHeight)
                        continue;

                    var prev = blk.Header?.PreviousBlockHash ?? Array.Empty<byte>();
                    if (!BytesEqual(prev, expectedPrevHash))
                        continue;

                    EnsureCanonicalBlockHash(blk);
                    return blk;
                }
                catch (Exception ex)
                {
                    log?.Warn("BlockSync", $"BlockAt decode/validate failed: {ex.Message}");
                }
            }

            return null;
        }

        private static async Task<Block?> RequestBlockAtLooseAsync(NetworkStream ns, ulong height, CancellationToken ct)
        {
            var req = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(req, height);
            await WriteFrame(ns, MsgType.GetBlock, req, ct).ConfigureAwait(false);

            var deadline = DateTime.UtcNow + FrameTimeout;
            while (DateTime.UtcNow < deadline)
            {
                var fr = await ReadFrameWithTimeout(ns, ct).ConfigureAwait(false);
                if (fr.payload == null) break;
                if (fr.type != MsgType.BlockAt) continue;

                try
                {
                    var blk = BlockBinarySerializer.Read(fr.payload);
                    if (blk.BlockHeight != 0 && blk.BlockHeight != height) continue;

                    EnsureCanonicalBlockHash(blk);
                    return blk;
                }
                catch { }
            }

            return null;
        }

        private static async Task<(MsgType type, byte[]? payload)> ReadFrameWithTimeout(NetworkStream ns, CancellationToken ct)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct);
            linked.CancelAfter(FrameTimeout);

            int? tByte = await ReadByteAsync(ns, linked.Token).ConfigureAwait(false);
            if (tByte == null) return ((MsgType)0, null);

            var lenBuf = new byte[4];
            if (!await ReadExact(ns, lenBuf, 4, linked.Token).ConfigureAwait(false)) return ((MsgType)0, null);
            int len = BinaryPrimitives.ReadInt32LittleEndian(lenBuf);
            if (len < 0 || len > MaxFramePayloadBytes) return ((MsgType)0, null);

            var payload = new byte[len];
            if (len > 0 && !await ReadExact(ns, payload, len, linked.Token).ConfigureAwait(false)) return ((MsgType)0, null);

            return ((MsgType)tByte.Value, payload);
        }

        private static async Task WriteFrame(NetworkStream ns, MsgType t, byte[] payload, CancellationToken ct)
        {
            var hdr = new byte[1 + 4];
            hdr[0] = (byte)t;
            BinaryPrimitives.WriteInt32LittleEndian(hdr.AsSpan(1, 4), payload.Length);

#if NET8_0_OR_GREATER
            await ns.WriteAsync(hdr, ct).ConfigureAwait(false);
            if (payload.Length > 0) await ns.WriteAsync(payload, ct).ConfigureAwait(false);
            await ns.FlushAsync(ct).ConfigureAwait(false);
#else
            await ns.WriteAsync(hdr, 0, hdr.Length, ct).ConfigureAwait(false);
            if (payload.Length > 0) await ns.WriteAsync(payload, 0, payload.Length, ct).ConfigureAwait(false);
            await ns.FlushAsync(ct).ConfigureAwait(false);
#endif
        }

        private static async Task<int?> ReadByteAsync(NetworkStream ns, CancellationToken ct)
        {
            var one = new byte[1];
#if NET8_0_OR_GREATER
            int r = await ns.ReadAsync(one.AsMemory(0, 1), ct).ConfigureAwait(false);
#else
            int r = await ns.ReadAsync(one, 0, 1, ct).ConfigureAwait(false);
#endif
            if (r <= 0) return null;
            return one[0];
        }

        private static async Task<bool> ReadExact(NetworkStream ns, byte[] buf, int len, CancellationToken ct)
        {
            int read = 0;
            while (read < len)
            {
#if NET8_0_OR_GREATER
                int r = await ns.ReadAsync(buf.AsMemory(read, len - read), ct).ConfigureAwait(false);
#else
                int r = await ns.ReadAsync(buf, read, len - read, ct).ConfigureAwait(false);
#endif
                if (r <= 0) return false;
                read += r;
            }
            return true;
        }

        private static async Task<(bool ok, ulong height, byte[]? hash)> WaitForTipAsync(
            NetworkStream ns, TcpClient client, ILogSink? log, CancellationToken ct)
        {
            var deadline = DateTime.UtcNow + FrameTimeout;
            bool handshakeValidated = false;

            while (DateTime.UtcNow < deadline)
            {
                var fr = await ReadFrameWithTimeout(ns, ct).ConfigureAwait(false);
                if (fr.payload == null) break;

                if (fr.type == MsgType.Handshake)
                {
                    if (!TryRecordPeerFromHandshake(fr.payload, client, log))
                        return (false, 0, null);
                    handshakeValidated = true;
                    continue;
                }

                if (fr.type == MsgType.Tip)
                {
                    if (!handshakeValidated)
                    {
                        log?.Warn("BlockSync", "Ignoring TIP received before a valid handshake.");
                        continue;
                    }

                    if (fr.payload.Length < 8) return (false, 0, null);
                    ulong h = BinaryPrimitives.ReadUInt64LittleEndian(fr.payload.AsSpan(0, 8));
                    byte[]? hash = fr.payload.Length >= 40 ? fr.payload.AsSpan(8, 32).ToArray() : null;
                    return (true, h, hash);
                }
            }

            return (false, 0, null);
        }

        private static void EnsureCanonicalBlockHash(Block block)
        {
            if (block.BlockHash is { Length: 32 } && !IsZero(block.BlockHash)) return;
            block.BlockHash = block.ComputeBlockHash();
        }

        private static bool IsZero(byte[]? h)
        {
            if (h is not { Length: 32 }) return false;
            for (int i = 0; i < 32; i++) if (h[i] != 0) return false;
            return true;
        }

        private static (byte[] txid, int offset, int size)[] ComputeTxOffsets(Block block)
        {
            int o = BlockBinarySerializer.HeaderSize;
            var list = new (byte[] txid, int offset, int size)[block.Transactions.Count];
            for (int i = 0; i < block.Transactions.Count; i++)
            {
                var tx = block.Transactions[i];
                int txSize = TxBinarySerializer.GetSize(tx);
                int txStart = o + 4; // 4 bytes length field
                list[i] = (tx.ComputeTransactionHash(), txStart, txSize);
                o = txStart + txSize;
            }
            return list;
        }

        private static List<(string ip, int port)> GetPeersFromDb()
        {
            var list = new List<(string, int)>();
            if (Db.Connection == null) return list;

            lock (Db.Sync)
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = "SELECT ip, port FROM peers ORDER BY last_seen DESC LIMIT 32;";
                using var r = cmd.ExecuteReader();
                while (r.Read())
                {
                    string ip = r.GetString(0);
                    int port = r.GetInt32(1);
                    int p = port > 0 ? port : DefaultP2PPort;
                    if (!string.IsNullOrWhiteSpace(ip))
                    {
                        if (SelfPeerGuard.IsSelf(ip, p)) continue;
                        list.Add((ip, p));
                    }
                }
            }
            return list;
        }

        private static string NormalizePeerKey(string host)
            => (host ?? string.Empty).Trim().ToLowerInvariant();

        private static string NormalizeHost(string host)
        {
            if (host.StartsWith("::ffff:", StringComparison.OrdinalIgnoreCase))
            {
                var i = host.LastIndexOf(':');
                if (i >= 0 && i + 1 < host.Length) return host[(i + 1)..];
            }
            return host;
        }

        private static byte[] BuildHandshakePayload(int listenPort)
        {
            var buf = new byte[1 + 1 + 32 + 2];
            buf[0] = 1;
            buf[1] = GenesisConfig.NetworkId;
            var nodeId = GetOrCreateNodeId();
            Buffer.BlockCopy(nodeId, 0, buf, 2, 32);
            BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(34, 2), (ushort)listenPort);
            return buf;
        }

        private static byte[] GetOrCreateNodeId()
        {
            var hex = MetaStore.Get("NodeId");
            if (string.IsNullOrWhiteSpace(hex))
            {
                var id = new byte[32];
                System.Security.Cryptography.RandomNumberGenerator.Fill(id);
                MetaStore.Set("NodeId", Convert.ToHexString(id).ToLowerInvariant());
                return id;
            }
            try
            {
                var b = Convert.FromHexString(hex);
                if (b.Length == 32)
                    return b;
            }
            catch { }

            var id2 = new byte[32];
            System.Security.Cryptography.RandomNumberGenerator.Fill(id2);
            MetaStore.Set("NodeId", Convert.ToHexString(id2).ToLowerInvariant());
            return id2;
        }

        private static bool TryRecordPeerFromHandshake(byte[] payload, TcpClient client, ILogSink? log)
        {
            try
            {
                if (payload.Length < 1 + 1 + 32 + 2)
                {
                    log?.Warn("BlockSync", "Rejected legacy handshake without network id.");
                    return false;
                }

                byte networkId = payload[1];
                if (networkId != GenesisConfig.NetworkId)
                {
                    log?.Warn("BlockSync", $"Rejected peer from foreign network id {networkId}.");
                    return false;
                }

                _ = payload.AsSpan(2, 32); // peerId is untrusted metadata; do not use as DB identity.
                ushort portRaw = BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(34, 2));
                int port = portRaw == 0 ? DefaultP2PPort : portRaw;
                var ip = (client.Client.RemoteEndPoint as IPEndPoint)?.Address?.ToString();
                if (!string.IsNullOrWhiteSpace(ip))
                    PeerStore.MarkSeen(ip!, port,
                        (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                        GenesisConfig.NetworkId);

                return true;
            }
            catch (Exception ex)
            {
                log?.Warn("BlockSync", $"Handshake parse failed: {ex.Message}");
                return false;
            }
        }

        private static void TryNotifyUiRefresh()
        {
            try
            {
                Application.Current?.Dispatcher.BeginInvoke(new Action(() =>
                {
                    if (Application.Current?.MainWindow is MainWindow mw)
                        mw.RefreshUiAfterNewBlock();
                }));
            }
            catch { }
        }
    }
}

