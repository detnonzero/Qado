using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using Microsoft.Data.Sqlite;
using Qado.Blockchain;
using Qado.Serialization;
using Qado.Logging;
using Qado.Mempool;
using Qado.Storage;
using Qado.Utils;

namespace Qado.Networking
{
    public sealed class P2PNode
    {
        public const int DefaultPort = GenesisConfig.P2PPort;
        private const int MaxFramePayloadBytes = ConsensusRules.MaxBlockSizeBytes;
        private const int MaxSessions = 128;

        public static P2PNode? Instance { get; private set; }

        private readonly MempoolManager _mempool;
        private readonly ILogSink? _log;

        private TcpListener? _listener;
        private int _listenPort = DefaultPort;
        private int _stopped;

        private readonly byte[] _nodeId;
        private readonly ConcurrentDictionary<string, Session> _sessions = new(StringComparer.Ordinal);

        private sealed class Session
        {
            public TcpClient Client = null!;
            public NetworkStream Stream = null!;
            public string RemoteEndpoint = "";
            public string RemoteBanKey = "";
            public string? RemoteIpAdvertised;
            public int? RemotePortAdvertised;
            public bool HandshakeOk;
        }

        public P2PNode(MempoolManager mempool, ILogSink? log = null)
        {
            _mempool = mempool ?? throw new ArgumentNullException(nameof(mempool));
            _log = log;
            _nodeId = GetOrCreateNodeId();
            Instance = this;
        }

        public void Start(int port, CancellationToken ct)
        {
            Volatile.Write(ref _stopped, 0);
            _listenPort = port;

            _listener = new TcpListener(IPAddress.Any, port); // IPv4 only
            _listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            _listener.Start();

            _ = AcceptLoop(ct);
            _log?.Info("P2P", $"listening on 0.0.0.0:{port} (IPv4 only)");
        }

        public void Stop()
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
                return;

            try { _listener?.Stop(); } catch { }
            _listener = null;

            foreach (var kv in _sessions)
            {
                var s = kv.Value;
                try { s.Stream.Close(); } catch { }
                try { s.Client.Close(); } catch { }
                _sessions.TryRemove(kv.Key, out _);
            }

            if (ReferenceEquals(Instance, this))
                Instance = null;
        }

        public async Task ConnectAsync(string host, int port = DefaultPort, CancellationToken ct = default)
        {
            if (Volatile.Read(ref _stopped) != 0) return;

            if (SelfPeerGuard.IsSelf(host, port))
            {
                _log?.Info("P2P", $"skipping self peer {host}:{port}");
                return;
            }

            string banKey = NormalizeBanKey(host);
            if (PeerFailTracker.ShouldBan(banKey))
            {
                _log?.Warn("P2P", $"skipping banned peer {host}:{port}");
                return;
            }

            try
            {
                var client = new TcpClient(AddressFamily.InterNetwork); // IPv4 only
                client.NoDelay = true;
                client.ReceiveTimeout = 15000;
                client.SendTimeout = 15000;

#if NET8_0_OR_GREATER
                await client.ConnectAsync(host, port, ct).ConfigureAwait(false);
#else
                await client.ConnectAsync(host, port).ConfigureAwait(false);
#endif

                var ns = client.GetStream();

                var sess = new Session
                {
                    Client = client,
                    Stream = ns,
                    RemoteEndpoint = client.Client.RemoteEndPoint?.ToString() ?? $"{host}:{port}",
                    RemoteBanKey = banKey
                };

                _sessions[sess.RemoteEndpoint] = sess;

                await WriteFrame(ns, MsgType.Handshake, BuildHandshakePayload(), ct).ConfigureAwait(false);

                try { await WriteFrame(ns, MsgType.GetPeers, Array.Empty<byte>(), ct).ConfigureAwait(false); } catch { }

                PeerFailTracker.ReportSuccess(banKey);
                _log?.Info("P2P", $"dialed {host}:{port} (IPv4)");
                _ = HandleClient(sess, ct);
            }
            catch (Exception ex)
            {
                PeerFailTracker.ReportFailure(banKey);
                _log?.Warn("P2P", $"connect {host}:{port} failed: {ex.Message}");
            }
        }

        public async Task ConnectKnownPeersAsync(CancellationToken ct = default)
        {
            foreach (var (ip, port) in GetPeersFromDb())
            {
                if (ct.IsCancellationRequested) return;
                if (PeerFailTracker.ShouldBan(ip)) continue;
                await ConnectAsync(ip, port <= 0 ? DefaultPort : port, ct).ConfigureAwait(false);
            }
        }

        public void StartPeerExchangeLoop(CancellationToken ct)
        {
            _ = Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested && Volatile.Read(ref _stopped) == 0)
                {
                    try
                    {
                        PeerStore.PruneAndEnforceLimits();
                    }
                    catch { }

                    try
                    {
                        await BroadcastAsync(MsgType.GetPeers, Array.Empty<byte>(), ct: ct).ConfigureAwait(false);
                    }
                    catch { }

                    try { await Task.Delay(TimeSpan.FromMinutes(2), ct).ConfigureAwait(false); } catch { }
                }
            }, ct);
        }

        public bool IsPeerConnected(string ip, int port)
        {
            if (string.IsNullOrWhiteSpace(ip)) return false;
            if (port <= 0 || port > 65535) return false;

            string wantedIp = NormalizeBanKey(ip);

            foreach (var kv in _sessions)
            {
                var s = kv.Value;
                if (!s.HandshakeOk) continue;

                string sessionIp = "";
                int sessionPort = 0;

                if (!string.IsNullOrWhiteSpace(s.RemoteIpAdvertised))
                    sessionIp = NormalizeBanKey(s.RemoteIpAdvertised!);

                if (s.RemotePortAdvertised is int advPort && advPort > 0 && advPort <= 65535)
                    sessionPort = advPort;

                if (sessionIp.Length == 0 || sessionPort == 0)
                {
                    try
                    {
                        if (s.Client?.Client?.RemoteEndPoint is IPEndPoint iep)
                        {
                            if (sessionIp.Length == 0)
                                sessionIp = NormalizeBanKey(iep.Address.ToString());
                            if (sessionPort == 0)
                                sessionPort = iep.Port;
                        }
                    }
                    catch { }
                }

                if (sessionPort == 0 && TryParsePortFromEndpoint(s.RemoteEndpoint, out int parsedPort))
                    sessionPort = parsedPort;

                if (sessionIp.Length == 0)
                    sessionIp = EndpointToBanKey(s.Client?.Client?.RemoteEndPoint);

                if (sessionIp.Length == 0)
                    sessionIp = NormalizeBanKey(s.RemoteEndpoint);

                if (sessionIp == wantedIp && sessionPort == port)
                    return true;
            }

            return false;
        }

        public async Task BroadcastTxAsync(Transaction tx)
        {
            try
            {
                var payload = tx.ToBytes();
                await BroadcastAsync(MsgType.Tx, payload).ConfigureAwait(false);
            }
            catch { }
        }

        public async Task BroadcastBlockAsync(Block block)
        {
            try
            {
                int size = BlockBinarySerializer.GetSize(block);
                var buf = new byte[size];
                _ = BlockBinarySerializer.Write(buf, block);
                await BroadcastAsync(MsgType.Block, buf).ConfigureAwait(false);
            }
            catch { }
        }


        private async Task AcceptLoop(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested && Volatile.Read(ref _stopped) == 0)
            {
                TcpClient? c = null;
                try
                {
                    var listener = _listener;
                    if (listener == null) return;

#if NET8_0_OR_GREATER
                    c = await listener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
#else
                    c = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
#endif
                    c.NoDelay = true;
                    c.ReceiveTimeout = 15000;
                    c.SendTimeout = 15000;

                    string banKey = EndpointToBanKey(c.Client.RemoteEndPoint);
                    if (PeerFailTracker.ShouldBan(banKey))
                    {
                        try { c.Close(); } catch { }
                        continue;
                    }

                    var ns = c.GetStream();
                    if (_sessions.Count >= MaxSessions)
                    {
                        try { c.Close(); } catch { }
                        continue;
                    }

                    var sess = new Session
                    {
                        Client = c,
                        Stream = ns,
                        RemoteEndpoint = c.Client.RemoteEndPoint?.ToString() ?? "unknown",
                        RemoteBanKey = banKey
                    };

                    _sessions[sess.RemoteEndpoint] = sess;

                    await WriteFrame(ns, MsgType.Handshake, BuildHandshakePayload(), ct).ConfigureAwait(false);

                    _ = HandleClient(sess, ct);
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
                catch
                {
                    try { c?.Close(); } catch { }
                }
            }
        }

        private async Task HandleClient(Session s, CancellationToken ct)
        {
            using var client = s.Client;
            using var ns = s.Stream;

            try
            {
                while (!ct.IsCancellationRequested && Volatile.Read(ref _stopped) == 0 && client.Connected)
                {
                    var typeByte = new byte[1];
#if NET8_0_OR_GREATER
                    int r1 = await ns.ReadAsync(typeByte.AsMemory(0, 1), ct).ConfigureAwait(false);
#else
                    int r1 = await ns.ReadAsync(typeByte, 0, 1, ct).ConfigureAwait(false);
#endif
                    if (r1 <= 0) break;

                    var lenBuf = new byte[4];
                    await ReadExact(ns, lenBuf, 4, ct).ConfigureAwait(false);
                    int len = BinaryPrimitives.ReadInt32LittleEndian(lenBuf);

                    if (len < 0 || len > MaxFramePayloadBytes)
                        throw new IOException("invalid frame length");

                    byte[] payload = Array.Empty<byte>();
                    if (len > 0)
                    {
                        payload = new byte[len];
                        await ReadExact(ns, payload, len, ct).ConfigureAwait(false);
                    }

                    await Dispatch((MsgType)typeByte[0], payload, s, ct).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                var key = GetBanKey(s);
                if (!string.IsNullOrWhiteSpace(key))
                    PeerFailTracker.ReportFailure(key);
                _log?.Warn("P2P", $"session {s.RemoteEndpoint} ended: {ex.Message}");
            }
            finally
            {
                _sessions.TryRemove(s.RemoteEndpoint, out _);
            }
        }


        private async Task Dispatch(MsgType type, byte[] payload, Session s, CancellationToken ct)
        {
            if (!s.HandshakeOk &&
                type != MsgType.Handshake &&
                type != MsgType.Ping &&
                type != MsgType.Pong)
            {
                PeerFailTracker.ReportFailure(GetBanKey(s));
                _log?.Warn("P2P", $"Rejected pre-handshake message {type} from {s.RemoteEndpoint}.");
                try { s.Client.Close(); } catch { }
                _sessions.TryRemove(s.RemoteEndpoint, out _);
                return;
            }

            switch (type)
            {
                case MsgType.Handshake:
                    HandleHandshake(payload, s);
                    return;

                case MsgType.Tx:
                    await HandleTx(payload, s, ct).ConfigureAwait(false);
                    return;

                case MsgType.Block:
                    await HandleBlock(payload, s, ct).ConfigureAwait(false);
                    return;

                case MsgType.GetBlock:
                    await HandleGetBlock(payload, s, ct).ConfigureAwait(false);
                    return;

                case MsgType.GetTip:
                    await HandleGetTip(s, ct).ConfigureAwait(false);
                    return;

                case MsgType.GetPeers:
                    await PeerDiscovery.HandleGetPeersAsync(s.Stream, _log, ct).ConfigureAwait(false);
                    return;

                case MsgType.Peers:
                    PeerDiscovery.HandlePeersPayload(payload, _log);
                    return;

                case MsgType.Ping:
                    await WriteFrame(s.Stream, MsgType.Pong, Array.Empty<byte>(), ct).ConfigureAwait(false);
                    return;

                case MsgType.Pong:
                    return;

                default:
                    return;
            }
        }

        private void HandleHandshake(byte[] payload, Session s)
        {
            var banKey = GetBanKey(s);

            if (payload.Length < 1 + 1 + 32 + 2)
            {
                PeerFailTracker.ReportFailure(banKey);
                _log?.Warn("P2P", "Rejected legacy handshake without network id.");
                try { s.Client.Close(); } catch { }
                _sessions.TryRemove(s.RemoteEndpoint, out _);
                return;
            }

            byte ver = payload[0];
            byte networkId = payload[1];
            if (networkId != GenesisConfig.NetworkId)
            {
                PeerFailTracker.ReportFailure(banKey);
                _log?.Warn("P2P", $"Rejected peer on foreign network id {networkId}.");
                try { s.Client.Close(); } catch { }
                _sessions.TryRemove(s.RemoteEndpoint, out _);
                return;
            }

            _ = payload.AsSpan(2, 32); // peerId is untrusted metadata; do not use as DB identity.
            ushort portRaw = BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(34, 2));
            int port = portRaw == 0 ? DefaultPort : portRaw;

            var ip = (s.Client.Client.RemoteEndPoint as IPEndPoint)?.Address?.ToString()
                     ?? s.RemoteEndpoint;

            s.RemoteIpAdvertised = ip;
            s.RemotePortAdvertised = port;
            s.RemoteBanKey = NormalizeBanKey(ip);

            try
            {
                PeerStore.MarkSeen(ip, port, (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(), GenesisConfig.NetworkId);
            }
            catch { }

            s.HandshakeOk = true;
            PeerFailTracker.ReportSuccess(GetBanKey(s));
            BlockSyncStarter.RequestImmediateSync(_log, $"handshake {ip}:{port}");
            _log?.Info("P2P", $"handshake from {ip}:{port} (v{ver})");
        }

        private async Task HandleTx(byte[] payload, Session s, CancellationToken ct)
        {
            Transaction txMsg;
            try
            {
                txMsg = TxBinarySerializer.Read(payload);
            }
            catch (Exception ex)
            {
                PeerFailTracker.ReportFailure(GetBanKey(s));
                _log?.Warn("P2P", $"TX decode failed: {ex.Message}");
                return;
            }

            if (!_mempool.TryAdd(txMsg))
                return;

            try
            {
                if (Application.Current?.MainWindow is MainWindow mw)
                    mw.AddTxToPreview(txMsg);
            }
            catch { }

            await BroadcastAsync(MsgType.Tx, payload, exceptEndpoint: s.RemoteEndpoint, ct: ct).ConfigureAwait(false);
            _log?.Info("P2P", $"TX accepted (nonce={txMsg.TxNonce}) gossiped");
        }

        private async Task HandleBlock(byte[] payload, Session s, CancellationToken ct)
        {
            Block blk;
            try
            {
                blk = BlockBinarySerializer.Read(payload);
            }
            catch (Exception ex)
            {
                PeerFailTracker.ReportFailure(GetBanKey(s));
                _log?.Warn("P2P", $"Block decode failed: {ex.Message}");
                return;
            }

            var prevHash = blk.Header?.PreviousBlockHash;
            if (prevHash is not { Length: 32 })
            {
                PeerFailTracker.ReportFailure(GetBanKey(s));
                _log?.Warn("P2P", "Block rejected: missing/invalid PrevHash.");
                return;
            }

            if (blk.BlockHash is not { Length: 32 } || IsZero32(blk.BlockHash))
                blk.BlockHash = blk.ComputeBlockHash();

            if (BlockIndexStore.GetLocation(blk.BlockHash!) != null)
            {
                _log?.Info("P2P", $"Block already known: {Hex16(blk.BlockHash!)}");
                return;
            }

            bool isTipExtending;
            ulong tipHSnapshot;
            byte[]? tipHashSnapshot;

            lock (Db.Sync)
            {
                tipHSnapshot = BlockStore.GetLatestHeight();
                tipHashSnapshot = BlockStore.GetCanonicalHashAtHeight(tipHSnapshot);
                isTipExtending = tipHashSnapshot is { Length: 32 } && BytesEqual32(prevHash, tipHashSnapshot);
            }

            if (isTipExtending)
            {
                blk.BlockHeight = tipHSnapshot + 1UL;
            }
            else
            {
                if (BlockIndexStore.TryGetMeta(prevHash, out var prevHeight, out _, out _))
                    blk.BlockHeight = prevHeight + 1UL;
                else
                {
                    PeerFailTracker.ReportFailure(GetBanKey(s));
                    _log?.Warn("P2P", "Block rejected: prev block not known (out of order).");
                    return;
                }
            }

            if (isTipExtending)
            {
                if (!BlockValidator.ValidateNetworkTipBlock(blk, out var reason))
                {
                    PeerFailTracker.ReportFailure(GetBanKey(s));
                    _log?.Warn("P2P", $"Block rejected (tip-ext): {reason}");
                    return;
                }
            }
            else
            {
                if (!BlockValidator.ValidateNetworkSideBlockStateless(blk, out var reason))
                {
                    PeerFailTracker.ReportFailure(GetBanKey(s));
                    _log?.Warn("P2P", $"Block rejected (sidechain): {reason}");
                    return;
                }
            }

            bool extendedCanon = false;
            ulong newCanonHeight = 0;

            try
            {
                lock (Db.Sync)
                {
                    using var tx = Db.Connection!.BeginTransaction();

                    BlockStore.SaveBlock(blk, tx);

                    if (TryExtendCanonTipNoReorg(blk, tx, out newCanonHeight))
                        extendedCanon = true;

                    tx.Commit();
                }
            }
            catch (Exception ex)
            {
                PeerFailTracker.ReportFailure(GetBanKey(s));
                _log?.Warn("P2P", $"Block store failed: {ex.Message}");
                return;
            }

            try { _mempool.RemoveIncluded(blk); } catch { }

            if (!extendedCanon)
            {
                try { ChainSelector.MaybeAdoptNewTip(blk.BlockHash!, _log); } catch { }
            }

            try
            {
                Application.Current?.Dispatcher.BeginInvoke(new Action(() =>
                {
                    try
                    {
                        if (Application.Current.MainWindow is MainWindow mw)
                            mw.RefreshUiAfterNewBlock();
                    }
                    catch { }
                }));
            }
            catch { }

            await BroadcastAsync(MsgType.Block, payload, exceptEndpoint: s.RemoteEndpoint, ct: ct).ConfigureAwait(false);

            if (extendedCanon)
                _log?.Info("P2P", $"Block stored + canon-extended: h={newCanonHeight} {Hex16(blk.BlockHash!)}");
            else
                _log?.Info("P2P", $"Block stored (side/reorg candidate): h={blk.BlockHeight} {Hex16(blk.BlockHash!)}");

            PeerFailTracker.ReportSuccess(GetBanKey(s));
        }

        private async Task HandleGetBlock(byte[] payload, Session s, CancellationToken ct)
        {
            if (payload.Length != 8) return;

            ulong h = BinaryPrimitives.ReadUInt64LittleEndian(payload);

            var blk = BlockStore.GetBlockByHeight(h);
            if (blk == null) return;

            int size = BlockBinarySerializer.GetSize(blk);
            var buf = new byte[size];
            _ = BlockBinarySerializer.Write(buf, blk);

            await WriteFrame(s.Stream, MsgType.BlockAt, buf, ct).ConfigureAwait(false);
        }

        private async Task HandleGetTip(Session s, CancellationToken ct)
        {
            ulong latest;
            byte[] tipHash;
            UInt128 tipWork = 0;

            lock (Db.Sync)
            {
                latest = BlockStore.GetLatestHeight();
                tipHash = BlockStore.GetCanonicalHashAtHeight(latest) ?? new byte[32];

                if (tipHash is { Length: 32 } && !IsZero32(tipHash))
                    tipWork = BlockIndexStore.GetChainwork(tipHash);
            }

            Span<byte> cw = stackalloc byte[16];
            U128.WriteBE(cw, tipWork);

            var resp = new byte[8 + 32 + 16];
            BinaryPrimitives.WriteUInt64LittleEndian(resp.AsSpan(0, 8), latest);
            tipHash.CopyTo(resp, 8);
            cw.CopyTo(resp.AsSpan(8 + 32, 16));

            _log?.Info("P2P", $"GetTip received → sending tip height {latest}");
            await WriteFrame(s.Stream, MsgType.Tip, resp, ct).ConfigureAwait(false);
        }


        private static bool TryExtendCanonTipNoReorg(Block blk, SqliteTransaction tx, out ulong newHeight)
        {
            if (blk == null) throw new ArgumentNullException(nameof(blk));
            if (tx == null) throw new ArgumentNullException(nameof(tx));

            newHeight = 0;

            var prev = blk.Header?.PreviousBlockHash;
            if (prev is not { Length: 32 }) return false;

            ulong tipH = BlockStore.GetLatestHeight(tx);
            var tipHash = BlockStore.GetCanonicalHashAtHeight(tipH, tx);
            if (tipHash is not { Length: 32 }) return false;

            if (!BytesEqual32(prev, tipHash))
                return false;

            newHeight = tipH + 1UL;

            blk.BlockHeight = newHeight;

            if (blk.BlockHash is not { Length: 32 } || IsZero32(blk.BlockHash))
                blk.BlockHash = blk.ComputeBlockHash();

            StateApplier.ApplyBlockWithUndo(blk, tx);

            BlockStore.SetCanonicalHashAtHeight(newHeight, blk.BlockHash!, tx);

            MetaStore.Set("LatestBlockHash", Convert.ToHexString(blk.BlockHash!).ToLowerInvariant(), tx);
            MetaStore.Set("LatestHeight", newHeight.ToString(), tx);

            return true;
        }


        private async Task BroadcastAsync(MsgType t, byte[] payload, string? exceptEndpoint = null, CancellationToken ct = default)
        {
            if (Volatile.Read(ref _stopped) != 0) return;

            foreach (var kv in _sessions)
            {
                if (exceptEndpoint != null && string.Equals(kv.Key, exceptEndpoint, StringComparison.Ordinal))
                    continue;

                var sess = kv.Value;
                try
                {
                    await WriteFrame(sess.Stream, t, payload, ct).ConfigureAwait(false);
                }
                catch
                {
                    try { sess.Client.Close(); } catch { }
                    _sessions.TryRemove(kv.Key, out _);
                }
            }
        }

        public static async Task WriteFrame(NetworkStream ns, MsgType t, byte[] payload, CancellationToken ct)
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

        private static async Task ReadExact(NetworkStream ns, byte[] buf, int len, CancellationToken ct)
        {
            int read = 0;
            while (read < len)
            {
#if NET8_0_OR_GREATER
                int r = await ns.ReadAsync(buf.AsMemory(read, len - read), ct).ConfigureAwait(false);
#else
                int r = await ns.ReadAsync(buf, read, len - read, ct).ConfigureAwait(false);
#endif
                if (r <= 0) throw new IOException("connection closed");
                read += r;
            }
        }


        private byte[] BuildHandshakePayload()
        {
            var buf = new byte[1 + 1 + 32 + 2];
            buf[0] = 1;
            buf[1] = GenesisConfig.NetworkId;
            Buffer.BlockCopy(_nodeId, 0, buf, 2, 32);
            BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(34, 2), (ushort)_listenPort);
            return buf;
        }

        private static byte[] GetOrCreateNodeId()
        {
            var hex = MetaStore.Get("NodeId");
            if (string.IsNullOrWhiteSpace(hex))
            {
                var id = new byte[32];
                RandomNumberGenerator.Fill(id);
                MetaStore.Set("NodeId", Convert.ToHexString(id).ToLowerInvariant());
                return id;
            }

            try
            {
                var b = Convert.FromHexString(hex);
                if (b.Length == 32) return b;
            }
            catch { }

            var id2 = new byte[32];
            RandomNumberGenerator.Fill(id2);
            MetaStore.Set("NodeId", Convert.ToHexString(id2).ToLowerInvariant());
            return id2;
        }

        private static System.Collections.Generic.List<(string ip, int port)> GetPeersFromDb()
        {
            var list = new System.Collections.Generic.List<(string, int port)>();

            lock (Db.Sync)
            {
                using var cmd = Db.Connection!.CreateCommand();
                cmd.CommandText = "SELECT ip, port FROM peers ORDER BY last_seen DESC LIMIT 64;";
                using var r = cmd.ExecuteReader();
                while (r.Read())
                {
                    string ip = r.GetString(0);
                    int port = r.GetInt32(1);
                    int p = port > 0 ? port : DefaultPort;

                    if (!string.IsNullOrWhiteSpace(ip))
                    {
                        if (SelfPeerGuard.IsSelf(ip, p)) continue;
                        list.Add((ip, p));
                    }
                }
            }

            return list;
        }


        private static bool IsZero32(byte[]? h)
        {
            if (h is not { Length: 32 }) return true;
            for (int i = 0; i < 32; i++) if (h[i] != 0) return false;
            return true;
        }

        private static bool BytesEqual32(byte[] a, byte[] b)
        {
            if (a.Length != 32 || b.Length != 32) return false;
            for (int i = 0; i < 32; i++)
                if (a[i] != b[i]) return false;
            return true;
        }

        private static string Hex16(byte[] h)
        {
            var s = Convert.ToHexString(h).ToLowerInvariant();
            return s.Length > 16 ? s[..16] + "…" : s;
        }

        private static string GetBanKey(Session s)
        {
            if (!string.IsNullOrWhiteSpace(s.RemoteIpAdvertised))
                return NormalizeBanKey(s.RemoteIpAdvertised!);

            if (!string.IsNullOrWhiteSpace(s.RemoteBanKey))
                return NormalizeBanKey(s.RemoteBanKey);

            return NormalizeBanKey(s.RemoteEndpoint);
        }

        private static string EndpointToBanKey(EndPoint? endpoint)
        {
            if (endpoint is IPEndPoint iep)
                return NormalizeBanKey(iep.Address.ToString());

            return NormalizeBanKey(endpoint?.ToString() ?? "");
        }

        private static bool TryParsePortFromEndpoint(string endpoint, out int port)
        {
            port = 0;
            if (string.IsNullOrWhiteSpace(endpoint)) return false;

            int idx = endpoint.LastIndexOf(':');
            if (idx <= 0 || idx >= endpoint.Length - 1) return false;

            if (!int.TryParse(endpoint[(idx + 1)..], out int p)) return false;
            if (p <= 0 || p > 65535) return false;

            port = p;
            return true;
        }

        private static string NormalizeBanKey(string raw)
            => (raw ?? string.Empty).Trim().ToLowerInvariant();
    }
}

