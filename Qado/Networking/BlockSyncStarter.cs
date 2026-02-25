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
using Qado.Utils;

namespace Qado.Networking
{
    public static class BlockSyncStarter
    {
        private const int DefaultP2PPort = GenesisConfig.P2PPort;
        private const int MaxFramePayloadBytes = ConsensusRules.MaxBlockSizeBytes;
        private const int MinBlockFetchWindow = 360;
        private const int MaxBlockFetchWindow = 1440;
        private const int DefaultBlockFetchWindow = 720;
        private static readonly TimeSpan FrameTimeout = TimeSpan.FromSeconds(8);
        private static readonly TimeSpan NoPeersBaseDelay = TimeSpan.FromSeconds(6);
        private static readonly TimeSpan NoTipBaseDelay = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan CatchupStallBaseDelay = TimeSpan.FromSeconds(4);
        private static readonly TimeSpan InSyncBaseDelay = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan CoolingDownBaseDelay = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan MaxIdleDelay = TimeSpan.FromMinutes(2);
        private const int MaxIdleBackoffShift = 4; // up to 16x base delay
        private const double IdleJitterFraction = 0.20;
        private static readonly TimeSpan SyncProgressLogInterval = TimeSpan.FromSeconds(5);
        private static readonly SemaphoreSlim ImmediateSyncSignal = new(0, 1);
        private static readonly object ImmediateSyncGate = new();
        private static readonly TimeSpan ImmediateSyncCooldown = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan HandshakeImmediateSyncCooldown = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan HandshakePeerImmediateSyncCooldown = TimeSpan.FromSeconds(90);
        private static DateTime _lastImmediateSyncUtc = DateTime.MinValue;
        private static readonly Dictionary<string, DateTime> HandshakeImmediateSyncByPeer = new(StringComparer.Ordinal);
        private static readonly object DialablePeerGate = new();
        private static readonly Dictionary<string, DateTime> DialablePeerLastSuccessUtc = new(StringComparer.Ordinal);
        private static readonly TimeSpan DialablePeerTtl = TimeSpan.FromDays(1);
        private const int MaxDialablePeerEntries = 2048;
        private const int MaxPeersPerSyncRound = 8;

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
                bool inSyncConfirmed = false;

                foreach (var (rawHost, port0) in peers)
                {
                    if (ct.IsCancellationRequested) return;

                    var host = NormalizeHost(rawHost);
                    var port = port0 > 0 ? port0 : DefaultP2PPort;
                    var peerKey = NormalizePeerKey(host);

                    if (PeerFailTracker.ShouldEnforceCooldown(peerKey))
                    {
                        skippedCoolingDown++;
                        continue;
                    }

                    dialAttempts++;

                    try
                    {
                        log?.Info("BlockSync", $"Connect {EndpointLogFormatter.FormatHostPort(host, port)} (IPv4) ...");
                        using var client = new TcpClient(AddressFamily.InterNetwork);
#if NET8_0_OR_GREATER
                        await client.ConnectAsync(host, port, ct).ConfigureAwait(false);
#else
                        await client.ConnectAsync(host, port).ConfigureAwait(false);
#endif
                        client.NoDelay = true;
                        using var ns = client.GetStream();
                        ReportPeerDialSuccess(host, port);

                        var myHs = BuildHandshakePayload(DefaultP2PPort);
                        await WriteFrame(ns, MsgType.Handshake, myHs, ct).ConfigureAwait(false);

                        bool handshakeValidated = false;
                        while (!ct.IsCancellationRequested)
                        {
                            await WriteFrame(ns, MsgType.GetTip, Array.Empty<byte>(), ct).ConfigureAwait(false);
                            var (tipOk, remoteHeight, remoteTipHash, remoteTipWork, hsNowValidated) =
                                await WaitForTipAsync(ns, client, log, ct, handshakeAlreadyValidated: handshakeValidated).ConfigureAwait(false);
                            handshakeValidated = hsNowValidated;

                            if (!tipOk)
                            {
                                ReportPeerDialFailure(host, port);
                                log?.Warn("BlockSync", "Peer did not provide a valid TIP.");
                                break;
                            }

                            ReportPeerDialSuccess(host, port);
                            PeerFailTracker.ReportSuccess(peerKey);
                            sawValidTip = true;

                            ulong localHeight = GetCanonicalTipHeight();
                            UInt128 localTipWork = GetCanonicalTipWork();

                            if (remoteTipWork != 0 && localTipWork != 0 && remoteTipWork <= localTipWork)
                            {
                                log?.Info("BlockSync",
                                    $"Peer tip chainwork not stronger (remoteWork={remoteTipWork}, localWork={localTipWork}).");
                                if (remoteTipHash is { Length: 32 })
                                {
                                    try { ChainSelector.MaybeAdoptNewTip(remoteTipHash, log, mempool); } catch { }
                                }
                                inSyncConfirmed = true;
                                break;
                            }

                            if (remoteHeight <= localHeight)
                            {
                                log?.Info("BlockSync", $"Already up-to-date (local={localHeight}, remote={remoteHeight}).");
                                if (remoteTipHash is { Length: 32 })
                                {
                                    try { ChainSelector.MaybeAdoptNewTip(remoteTipHash, log, mempool); } catch { }
                                }
                                inSyncConfirmed = true;
                                break;
                            }

                            sawPeerAhead = true;

                            ulong ancestor = await FindCommonAncestorAsync(ns, remoteHeight, localHeight, log, ct).ConfigureAwait(false);
                            log?.Info("BlockSync", $"Common ancestor at h={ancestor}. Sync {ancestor + 1}..{remoteHeight}");

                            byte[] expectedPrevHash = await GetAncestorHashAsync(ns, ancestor, log, ct).ConfigureAwait(false);
                            byte[]? downloadedTipHash = null;
                            int configuredWindow = GetConfiguredBlockFetchWindow();
                            int rangeWindow = (int)Math.Min((ulong)configuredWindow, (remoteHeight - ancestor));
                            if (rangeWindow <= 0) rangeWindow = 1;

                            log?.Info("BlockSync", $"Windowed block fetch enabled (window={rangeWindow}).");

                            var dl = await DownloadRangeWindowedAsync(
                                ns: ns,
                                fromHeight: ancestor + 1,
                                toHeight: remoteHeight,
                                expectedPrevHash: expectedPrevHash,
                                fetchWindow: rangeWindow,
                                log: log,
                                ct: ct).ConfigureAwait(false);

                            downloadedTipHash = dl.lastBlockHash;
                            if (dl.madeProgress)
                            {
                                madeProgress = true;
                                if (dl.lastStoredHeight > lastHeight) lastHeight = dl.lastStoredHeight;
                            }

                            if (!dl.ok)
                                log?.Warn("BlockSync", $"Windowed download interrupted (lastStored={dl.lastStoredHeight}, target={remoteHeight}).");

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
                                    try { ChainSelector.MaybeAdoptNewTip(candidateTip, log, mempool); } catch { }
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

                                    // The remote tip may have advanced while we were downloading.
                                    // Re-query tip immediately on the same session and continue catch-up if needed.
                                    continue;
                                }

                                log?.Warn("BlockSync",
                                    $"Canonical sync incomplete after adoption (canon={canonNow}, remote={remoteHeight}). Retrying ...");
                            }
                            else
                            {
                                log?.Warn("BlockSync",
                                    $"Download incomplete (dbMax={downloadedNow}, remote={remoteHeight}). Retrying ...");
                            }

                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        ReportPeerDialFailure(host, port);
                        log?.Warn("BlockSync", $"Peer {EndpointLogFormatter.FormatHostPort(host, port)} failed: {ex.Message}");
                    }

                    if (inSyncConfirmed)
                        break;
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
            string normalizedReason = (reason ?? string.Empty).Trim();
            bool isHandshakeReason = normalizedReason.StartsWith("handshake", StringComparison.OrdinalIgnoreCase);

            bool allowed;
            lock (ImmediateSyncGate)
            {
                var now = DateTime.UtcNow;
                var minCooldown = isHandshakeReason ? HandshakeImmediateSyncCooldown : ImmediateSyncCooldown;
                allowed = (now - _lastImmediateSyncUtc) >= minCooldown;
                if (allowed)
                {
                    if (isHandshakeReason)
                    {
                        var peerKey = ExtractHandshakePeerKey(normalizedReason);
                        if (peerKey.Length != 0 &&
                            HandshakeImmediateSyncByPeer.TryGetValue(peerKey, out var lastForPeer) &&
                            (now - lastForPeer) < HandshakePeerImmediateSyncCooldown)
                        {
                            allowed = false;
                        }
                        else if (peerKey.Length != 0)
                        {
                            HandshakeImmediateSyncByPeer[peerKey] = now;
                        }

                        PruneHandshakeImmediateSyncByPeer_NoThrow(now);
                    }
                }

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

        public static void ReportPeerDialSuccess(string host, int port)
        {
            string key = BuildDialablePeerKey(host, port);
            if (key.Length == 0) return;

            lock (DialablePeerGate)
            {
                var now = DateTime.UtcNow;
                DialablePeerLastSuccessUtc[key] = now;
                PruneDialablePeers_NoThrow(now);
            }
        }

        public static void ReportPeerDialFailure(string host, int port)
        {
            string key = BuildDialablePeerKey(host, port);
            if (key.Length == 0) return;

            lock (DialablePeerGate)
            {
                DialablePeerLastSuccessUtc.Remove(key);
                PruneDialablePeers_NoThrow(DateTime.UtcNow);
            }
        }

        private static bool TrySelectBestDialablePeer(
            List<(string ip, int port)> candidates,
            out (string ip, int port) selected)
        {
            selected = default;
            if (candidates.Count == 0) return false;

            var now = DateTime.UtcNow;
            DateTime bestSeen = DateTime.MinValue;

            lock (DialablePeerGate)
            {
                PruneDialablePeers_NoThrow(now);

                for (int i = 0; i < candidates.Count; i++)
                {
                    string host = NormalizeHost(candidates[i].ip);
                    int port = candidates[i].port > 0 ? candidates[i].port : DefaultP2PPort;
                    string key = BuildDialablePeerKey(host, port);
                    if (key.Length == 0) continue;

                    if (!DialablePeerLastSuccessUtc.TryGetValue(key, out var seen))
                        continue;

                    if (seen > bestSeen)
                    {
                        bestSeen = seen;
                        selected = (host, port);
                    }
                }
            }

            return bestSeen != DateTime.MinValue;
        }

        private static void PruneDialablePeers_NoThrow(DateTime now)
        {
            try
            {
                if (DialablePeerLastSuccessUtc.Count == 0)
                    return;

                var stale = new List<string>();
                foreach (var kv in DialablePeerLastSuccessUtc)
                {
                    if ((now - kv.Value) > DialablePeerTtl)
                        stale.Add(kv.Key);
                }

                for (int i = 0; i < stale.Count; i++)
                    DialablePeerLastSuccessUtc.Remove(stale[i]);

                if (DialablePeerLastSuccessUtc.Count <= MaxDialablePeerEntries)
                    return;

                var ordered = new List<KeyValuePair<string, DateTime>>(DialablePeerLastSuccessUtc);
                ordered.Sort((a, b) => a.Value.CompareTo(b.Value));
                int toRemove = ordered.Count - MaxDialablePeerEntries;

                for (int i = 0; i < toRemove; i++)
                    DialablePeerLastSuccessUtc.Remove(ordered[i].Key);
            }
            catch
            {
            }
        }

        private static string ExtractHandshakePeerKey(string reason)
        {
            if (reason.Length == 0) return string.Empty;
            int sep = reason.IndexOf(' ');
            if (sep < 0 || sep + 1 >= reason.Length) return string.Empty;
            return reason[(sep + 1)..].Trim().ToLowerInvariant();
        }

        private static void PruneHandshakeImmediateSyncByPeer_NoThrow(DateTime now)
        {
            try
            {
                if (HandshakeImmediateSyncByPeer.Count <= 512)
                    return;

                var stale = new List<string>();
                foreach (var kv in HandshakeImmediateSyncByPeer)
                {
                    if ((now - kv.Value) > TimeSpan.FromMinutes(10))
                        stale.Add(kv.Key);
                }

                for (int i = 0; i < stale.Count; i++)
                    HandshakeImmediateSyncByPeer.Remove(stale[i]);
            }
            catch
            {
            }
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
                reason = "no-dialable-peers";
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

        private static UInt128 GetCanonicalTipWork()
        {
            if (Db.Connection == null) return 0;
            lock (Db.Sync)
            {
                ulong tipHeight = BlockStore.GetLatestHeight();
                var tipHash = BlockStore.GetCanonicalHashAtHeight(tipHeight);
                if (tipHash is not { Length: 32 }) return 0;
                return BlockIndexStore.GetChainwork(tipHash);
            }
        }

        private static int GetConfiguredBlockFetchWindow()
        {
            string? raw = Environment.GetEnvironmentVariable("QADO_BLOCKSYNC_WINDOW");
            if (int.TryParse(raw, out int parsed))
                return Math.Clamp(parsed, MinBlockFetchWindow, MaxBlockFetchWindow);

            return DefaultBlockFetchWindow;
        }

        private static async Task<(bool ok, ulong lastStoredHeight, byte[]? lastBlockHash, bool madeProgress)> DownloadRangeWindowedAsync(
            NetworkStream ns,
            ulong fromHeight,
            ulong toHeight,
            byte[] expectedPrevHash,
            int fetchWindow,
            ILogSink? log,
            CancellationToken ct)
        {
            if (fromHeight > toHeight)
                return (true, GetLatestHeightDirect(), null, false);

            if (expectedPrevHash is not { Length: 32 })
                return (false, GetLatestHeightDirect(), null, false);

            fetchWindow = Math.Clamp(fetchWindow, 1, MaxBlockFetchWindow);

            ulong nextToRequest = fromHeight;
            ulong nextToProcess = fromHeight;
            var pendingByHeight = new Dictionary<ulong, Block>(capacity: Math.Min(fetchWindow * 2, 4096));
            var expectedHeightsByResponseOrder = new Queue<ulong>(Math.Min(fetchWindow * 2, 4096));

            ulong lastStoredHeight = GetLatestHeightDirect();
            byte[]? lastBlockHash = null;
            bool madeProgress = false;
            DateTime lastProgressLogUtc = DateTime.UtcNow;

            while (nextToRequest <= toHeight && (nextToRequest - nextToProcess) < (ulong)fetchWindow)
            {
                var req = new byte[8];
                BinaryPrimitives.WriteUInt64LittleEndian(req, nextToRequest);
                await WriteFrame(ns, MsgType.GetBlock, req, ct).ConfigureAwait(false);
                expectedHeightsByResponseOrder.Enqueue(nextToRequest);
                nextToRequest++;
            }

            while (!ct.IsCancellationRequested && nextToProcess <= toHeight)
            {
                var fr = await ReadFrameWithTimeout(ns, ct).ConfigureAwait(false);
                if (fr.payload == null)
                    return (false, lastStoredHeight, lastBlockHash, madeProgress);

                if (fr.type != MsgType.BlockAt)
                    continue;

                Block blk;
                try
                {
                    blk = BlockBinarySerializer.Read(fr.payload);
                    EnsureCanonicalBlockHash(blk);
                }
                catch (Exception ex)
                {
                    log?.Warn("BlockSync", $"BlockAt decode failed in windowed mode: {ex.Message}");
                    return (false, lastStoredHeight, lastBlockHash, madeProgress);
                }

                if (expectedHeightsByResponseOrder.Count == 0)
                {
                    log?.Warn("BlockSync", "Windowed mode received unexpected BlockAt without outstanding request.");
                    return (false, lastStoredHeight, lastBlockHash, madeProgress);
                }

                ulong h = expectedHeightsByResponseOrder.Dequeue();
                if (blk.BlockHeight != 0 && blk.BlockHeight != h)
                {
                    log?.Warn("BlockSync", $"Windowed mode response height mismatch: expected h={h}, got h={blk.BlockHeight}.");
                    return (false, lastStoredHeight, lastBlockHash, madeProgress);
                }

                blk.BlockHeight = h;

                if (!pendingByHeight.ContainsKey(h))
                    pendingByHeight[h] = blk;

                while (pendingByHeight.TryGetValue(nextToProcess, out var ready))
                {
                    var prev = ready.Header?.PreviousBlockHash;
                    if (prev is not { Length: 32 } || !BytesEqual(prev, expectedPrevHash))
                    {
                        log?.Warn("BlockSync", $"Windowed mode linkage mismatch at h={nextToProcess}.");
                        return (false, lastStoredHeight, lastBlockHash, madeProgress);
                    }

                    ready.BlockHeight = nextToProcess;
                    if (!BlockValidator.ValidateNetworkSideBlockStateless(ready, out var reason))
                    {
                        log?.Warn("BlockSync", $"Fetched block h={nextToProcess} failed validation: {reason}");
                        return (false, lastStoredHeight, lastBlockHash, madeProgress);
                    }

                    try
                    {
                        var txOffsets = ComputeTxOffsets(ready);

                        lock (Db.Sync)
                        {
                            using var trx = Db.Connection!.BeginTransaction();

                            BlockStore.SaveBlock(ready, trx, BlockIndexStore.StatusSideStatelessAccepted);

                            for (int i = 0; i < txOffsets.Length; i++)
                            {
                                var (id, off, size) = txOffsets[i];
                                TxIndexStore.Insert(id, ready.BlockHash!, ready.BlockHeight, off, size, trx);
                            }

                            trx.Commit();
                        }
                    }
                    catch (Exception ex)
                    {
                        log?.Warn("BlockSync", $"Store h={nextToProcess} failed: {ex.Message}");
                        return (false, lastStoredHeight, lastBlockHash, madeProgress);
                    }

                    pendingByHeight.Remove(nextToProcess);
                    expectedPrevHash = ready.BlockHash!;
                    lastBlockHash = ready.BlockHash!;

                    var after = GetLatestHeightDirect();
                    ulong inFlight = nextToRequest > nextToProcess ? (nextToRequest - nextToProcess) : 0UL;
                    var nowUtc = DateTime.UtcNow;
                    bool progressLogByTime = (nowUtc - lastProgressLogUtc) >= SyncProgressLogInterval;
                    bool isFinalProgressLog = nextToProcess >= toHeight;
                    if (isFinalProgressLog || progressLogByTime)
                    {
                        log?.Info("BlockSync", $"Synced {nextToProcess}/{toHeight} (dbMax={after}, inFlight={inFlight})");
                        lastProgressLogUtc = nowUtc;
                    }

                    if (after < nextToProcess)
                    {
                        log?.Error("BlockSync",
                            $"DB did not retain inserts: after commit MAX(height)={after}, expected >= {nextToProcess}.");
                        return (false, lastStoredHeight, lastBlockHash, madeProgress);
                    }

                    if (after > lastStoredHeight)
                    {
                        lastStoredHeight = after;
                        madeProgress = true;
                    }

                    nextToProcess++;

                    while (nextToRequest <= toHeight && (nextToRequest - nextToProcess) < (ulong)fetchWindow)
                    {
                        var req = new byte[8];
                        BinaryPrimitives.WriteUInt64LittleEndian(req, nextToRequest);
                        await WriteFrame(ns, MsgType.GetBlock, req, ct).ConfigureAwait(false);
                        expectedHeightsByResponseOrder.Enqueue(nextToRequest);
                        nextToRequest++;
                    }
                }
            }

            return (nextToProcess > toHeight, lastStoredHeight, lastBlockHash, madeProgress);
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

        private static async Task<(bool ok, ulong height, byte[]? hash, UInt128 chainwork, bool handshakeValidated)> WaitForTipAsync(
            NetworkStream ns,
            TcpClient client,
            ILogSink? log,
            CancellationToken ct,
            bool handshakeAlreadyValidated = false)
        {
            var deadline = DateTime.UtcNow + FrameTimeout;
            bool handshakeValidated = handshakeAlreadyValidated;

            while (DateTime.UtcNow < deadline)
            {
                var fr = await ReadFrameWithTimeout(ns, ct).ConfigureAwait(false);
                if (fr.payload == null) break;

                if (fr.type == MsgType.Handshake)
                {
                    if (!TryRecordPeerFromHandshake(fr.payload, client, log))
                        return (false, 0, null, 0, handshakeValidated);
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

                    if (fr.payload.Length < 8) return (false, 0, null, 0, handshakeValidated);
                    ulong h = BinaryPrimitives.ReadUInt64LittleEndian(fr.payload.AsSpan(0, 8));
                    byte[]? hash = fr.payload.Length >= 40 ? fr.payload.AsSpan(8, 32).ToArray() : null;
                    UInt128 cw = 0;
                    if (fr.payload.Length >= 56)
                        cw = U128.ReadBE(fr.payload.AsSpan(40, 16).ToArray());
                    return (true, h, hash, cw, handshakeValidated);
                }
            }

            return (false, 0, null, 0, handshakeValidated);
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
            var candidates = new List<(string, int)>();
            if (Db.Connection == null) return candidates;

            lock (Db.Sync)
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = "SELECT ip, port FROM peers ORDER BY last_seen DESC LIMIT 64;";
                using var r = cmd.ExecuteReader();
                while (r.Read())
                {
                    string ip = r.GetString(0);
                    int port = r.GetInt32(1);
                    int p = port > 0 ? port : DefaultP2PPort;
                    if (!string.IsNullOrWhiteSpace(ip))
                    {
                        if (SelfPeerGuard.IsSelf(ip, p)) continue;
                        candidates.Add((ip, p));
                    }
                }
            }

            if (candidates.Count == 0)
                return candidates;

            var selectedPeers = new List<(string ip, int port)>(Math.Min(MaxPeersPerSyncRound, candidates.Count));
            var seen = new HashSet<string>(StringComparer.Ordinal);

            void TryAdd(string hostRaw, int portRaw)
            {
                string host = NormalizeHost(hostRaw);
                int port = portRaw > 0 && portRaw <= 65535 ? portRaw : DefaultP2PPort;
                string key = BuildDialablePeerKey(host, port);
                if (key.Length == 0) return;
                if (!seen.Add(key)) return;
                selectedPeers.Add((host, port));
            }

            if (TrySelectBestDialablePeer(candidates, out var selected))
                TryAdd(selected.Item1, selected.Item2);

            for (int i = 0; i < candidates.Count && selectedPeers.Count < MaxPeersPerSyncRound; i++)
                TryAdd(candidates[i].Item1, candidates[i].Item2);

            return selectedPeers;
        }

        private static string NormalizePeerKey(string host)
            => (host ?? string.Empty).Trim().ToLowerInvariant();

        private static string BuildDialablePeerKey(string host, int port)
        {
            string h = NormalizeDialableHost(host);
            int p = port > 0 && port <= 65535 ? port : DefaultP2PPort;
            if (h.Length == 0) return string.Empty;
            return $"{h}:{p}";
        }

        private static string NormalizeDialableHost(string host)
        {
            if (string.IsNullOrWhiteSpace(host)) return string.Empty;
            string s = host.Trim().ToLowerInvariant();

            if (s.StartsWith("[", StringComparison.Ordinal))
            {
                int close = s.IndexOf(']');
                if (close > 1)
                    s = s.Substring(1, close - 1);
            }
            else
            {
                int firstColon = s.IndexOf(':');
                int lastColon = s.LastIndexOf(':');
                if (firstColon > 0 && firstColon == lastColon)
                {
                    string tail = s[(firstColon + 1)..];
                    if (int.TryParse(tail, out _))
                        s = s[..firstColon];
                }
            }

            if (s.StartsWith("::ffff:", StringComparison.Ordinal))
                s = s[7..];

            return s;
        }

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
                    var remote = client.Client.RemoteEndPoint?.ToString() ?? "unknown";
                    log?.Warn(
                        "BlockSync",
                        $"Rejected peer {EndpointLogFormatter.FormatEndpoint(remote)} from foreign network id {networkId} (expected {GenesisConfig.NetworkId}, raw={remote}).");
                    return false;
                }

                var peerId = payload.AsSpan(2, 32); // peerId is untrusted metadata; use only for self-loop detection.
                if (peerId.SequenceEqual(GetOrCreateNodeId()))
                {
                    var endpointIp = (client.Client.RemoteEndPoint as IPEndPoint)?.Address?.ToString();
                    if (!string.IsNullOrWhiteSpace(endpointIp))
                        SelfPeerGuard.RememberSelf(endpointIp);

                    log?.Warn("BlockSync", $"Rejected self-loop peer {EndpointLogFormatter.FormatHost(endpointIp)} (same node id).");
                    return false;
                }

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

