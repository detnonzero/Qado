using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        private static readonly int MaxFramePayloadBytes =
            Math.Max(
                ConsensusRules.MaxBlockSizeBytes,
                Math.Max(BlockDownloadManager.MaxHashListPayloadBytes, HeaderSyncManager.MaxHeadersPayloadBytes));
        private const int MaxSessions = 128;
        private const int OutboundTargetConnections = 10; // bounded by [8..12]
        private const int OutboundMinConnections = 8;
        private const int OutboundMaxConnections = 12;
        private static readonly TimeSpan PublicReachabilityTimeout = TimeSpan.FromMinutes(60);
        private static readonly TimeSpan ReconnectWhenDisconnectedDelay = TimeSpan.FromSeconds(8);
        private static readonly TimeSpan ReconnectSteadyDelay = TimeSpan.FromSeconds(25);
        private const double ReconnectJitterFraction = 0.20;
        private static readonly TimeSpan PublicClaimProbeTimeout = TimeSpan.FromSeconds(8);
        private const int MaxPublicClaimProbesPerReconnectTick = 12;
        private static readonly TimeSpan BlockRateWindow = TimeSpan.FromSeconds(10);
        private const int MaxInboundBlocksPerWindowPerPeer = 120;
        private static readonly TimeSpan TxRateWindow = TimeSpan.FromSeconds(10);
        private const int MaxInboundTxPerWindowPerPeer = 300;
        private const ulong MaxSideReorgDepth = 1440;
        private const int MaxSideChildrenPerPrevHash = 8;
        private const long MaxSideCandidateCount = 20_000;
        private const long MaxSideCandidateBytes = 256L * 1024L * 1024L;
        private static readonly UInt128 SideAdmissionWorkSlack = 131_072;
        private const int MaxOrphansTotal = 2000;
        private const int MaxOrphansPerPeer = 128;
        private static readonly TimeSpan OrphanTtl = TimeSpan.FromMinutes(10);
        private const int MaxOrphanPromotionsPerPass = 256;
        private static readonly TimeSpan KnownBlockLogCooldown = TimeSpan.FromSeconds(12);
        private const int MaxKnownBlockLogEntries = 4096;
        private static readonly TimeSpan InvRelayCooldown = TimeSpan.FromMinutes(2);
        private const int MaxInvRelayEntries = 8192;

        public static P2PNode? Instance { get; private set; }
        public bool IsPubliclyReachable => _reachability.IsPublic;

        private readonly MempoolManager _mempool;
        private readonly ILogSink? _log;

        private TcpListener? _listener;
        private int _listenPort = DefaultPort;
        private int _stopped;
        private int _reconnectLoopStarted;
        private int _peerExchangeLoopStarted;

        private readonly byte[] _nodeId;
        private readonly ReachabilityState _reachability = new();
        private readonly ConcurrentDictionary<string, PeerSession> _sessions = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, RateBucket> _blockRateByPeer = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, RateBucket> _txRateByPeer = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, byte> _publicPeerEndpoints = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, byte> _publicClaimsByPex = new(StringComparer.Ordinal);
        private readonly ConcurrentQueue<byte[]> _pendingRecoveryRevalidate = new();
        private readonly object _badChainGate = new();
        private readonly object _orphanGate = new();
        private readonly Dictionary<string, OrphanEntry> _orphansByHash = new(StringComparer.Ordinal);
        private readonly Dictionary<string, List<OrphanEntry>> _orphansByPrev = new(StringComparer.Ordinal);
        private readonly Dictionary<string, int> _orphansByPeer = new(StringComparer.Ordinal);
        private readonly object _knownBlockLogGate = new();
        private readonly Dictionary<string, DateTime> _knownBlockLogByHash = new(StringComparer.Ordinal);
        private readonly object _invRelayGate = new();
        private readonly Dictionary<string, DateTime> _invRelayByHash = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, byte> _dialInFlight = new(StringComparer.Ordinal);
        private readonly SemaphoreSlim _validationSerializeGate = new(1, 1);
        private readonly HeaderSyncManager _headerSyncManager;
        private readonly BlockDownloadManager _blockDownloadManager;
        private readonly ValidationWorker _validationWorker;

        private sealed class RateBucket
        {
            public DateTime WindowStartUtc = DateTime.UtcNow;
            public int Count;
        }

        private sealed class OrphanEntry
        {
            public byte[] Payload = Array.Empty<byte>();
            public byte[] BlockHash = Array.Empty<byte>();
            public byte[] PrevHash = Array.Empty<byte>();
            public string PeerKey = "";
            public DateTime ReceivedUtc = DateTime.UtcNow;
        }

        public P2PNode(MempoolManager mempool, ILogSink? log = null)
        {
            _mempool = mempool ?? throw new ArgumentNullException(nameof(mempool));
            _log = log;
            _nodeId = GetOrCreateNodeId();
            _validationWorker = new ValidationWorker(ProcessValidationWorkItemAsync, _log);
            _headerSyncManager = new HeaderSyncManager(
                sessionSnapshot: GetSessionSnapshot,
                sendFrameAsync: SendFrameViaSessionAsync,
                onSyncCompleted: OnHeaderSyncCompleted,
                log: _log);
            _blockDownloadManager = new BlockDownloadManager(
                sessionSnapshot: GetSessionSnapshot,
                sendFrameAsync: SendFrameViaSessionAsync,
                haveBlock: HaveBlockLocally,
                isDownloadCandidate: IsHeaderCandidateDownloadable,
                requestResync: reason => _headerSyncManager.RequestResync(reason),
                enqueueValidator: item => _validationWorker.Enqueue(item),
                revalidateStoredBlock: RevalidateStoredBlock,
                log: _log);
            Instance = this;
        }

        public void Start(int port, CancellationToken ct)
        {
            Volatile.Write(ref _stopped, 0);
            _listenPort = port;
            _reachability.DecayIfExpired(PublicReachabilityTimeout);

            _listener = new TcpListener(IPAddress.Any, port); // IPv4 only
            _listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            _listener.Start();

            int workerCount = Math.Max(1, Environment.ProcessorCount / 2);
            _validationWorker.Start(workerCount, ct);
            _blockDownloadManager.Start(ct);
            _headerSyncManager.Start(ct);

            _ = AcceptLoop(ct);
            _log?.Info("P2P", $"listening on 0.0.0.0:{port} (IPv4 only)");
        }

        public void Stop()
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
                return;

            try { _listener?.Stop(); } catch { }
            _listener = null;
            Interlocked.Exchange(ref _reconnectLoopStarted, 0);
            Interlocked.Exchange(ref _peerExchangeLoopStarted, 0);

            foreach (var kv in _sessions)
            {
                var s = kv.Value;
                try { s.Stream.Close(); } catch { }
                try { s.Client.Close(); } catch { }
                _sessions.TryRemove(kv.Key, out _);
            }

            try { _headerSyncManager.Dispose(); } catch { }
            try { _blockDownloadManager.Dispose(); } catch { }
            try { _validationWorker.Dispose(); } catch { }

            if (ReferenceEquals(Instance, this))
                Instance = null;
        }

        public async Task ConnectAsync(string host, int port = DefaultPort, CancellationToken ct = default)
        {
            if (Volatile.Read(ref _stopped) != 0) return;

            if (SelfPeerGuard.IsSelf(host, port))
            {
                _log?.Info("P2P", $"skipping self peer {EndpointLogFormatter.FormatHostPort(host, port)}");
                return;
            }

            string banKey = NormalizeBanKey(host);
            if (PeerFailTracker.ShouldEnforceCooldown(banKey))
            {
                _log?.Warn("P2P", $"skipping banned peer {EndpointLogFormatter.FormatHostPort(host, port)}");
                return;
            }

            string dialKey = $"{NormalizeBanKey(host)}:{port}";
            if (!_dialInFlight.TryAdd(dialKey, 0))
                return;

            try
            {
                if (IsPeerSessionPresent(host, port))
                    return;

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

                var sess = new PeerSession
                {
                    Client = client,
                    Stream = ns,
                    RemoteEndpoint = client.Client.RemoteEndPoint?.ToString() ?? $"{host}:{port}",
                    RemoteBanKey = banKey,
                    IsInbound = false
                };

                _sessions[sess.RemoteEndpoint] = sess;

                await WriteFrame(ns, MsgType.Handshake, BuildHandshakePayload(), ct).ConfigureAwait(false);

                try { await WriteFrame(ns, MsgType.GetPeers, Array.Empty<byte>(), ct).ConfigureAwait(false); } catch { }
                try { await WriteFrame(ns, MsgType.InvBlock, Array.Empty<byte>(), ct).ConfigureAwait(false); } catch { }

                PeerFailTracker.ReportSuccess(banKey);
                _log?.Info("P2P", $"dialed {EndpointLogFormatter.FormatHostPort(host, port)} (IPv4)");
                _ = HandleClient(sess, ct);
            }
            catch (Exception ex)
            {
                _log?.Warn("P2P", $"connect {EndpointLogFormatter.FormatHostPort(host, port)} failed: {ex.Message}");
            }
            finally
            {
                _dialInFlight.TryRemove(dialKey, out _);
            }
        }

        public async Task ConnectSeedAndKnownPeersAsync(CancellationToken ct = default, int maxKnownPeers = 32)
        {
            if (Volatile.Read(ref _stopped) != 0) return;
            if (ct.IsCancellationRequested) return;

            int targetOutbound = Math.Clamp(
                OutboundTargetConnections,
                OutboundMinConnections,
                OutboundMaxConnections);

            if (CountOutboundSessions(requireHandshake: false) >= targetOutbound)
                return;

            int seedPort = DefaultPort;
            TryRememberSeedEndpointNoThrow(GenesisConfig.GenesisHost, seedPort);

            var candidates = new List<(string ip, int port)>();
            var seen = new HashSet<string>(StringComparer.Ordinal);

            void AddCandidate(string ip, int port)
            {
                if (string.IsNullOrWhiteSpace(ip)) return;
                int p = port > 0 ? port : DefaultPort;
                string key = $"{NormalizeBanKey(ip)}:{p}";
                if (seen.Add(key))
                    candidates.Add((ip, p));
            }

            AddCandidate(GenesisConfig.GenesisHost, seedPort);
            foreach (var (ip, port) in GetPeersFromDb())
                AddCandidate(ip, port);

            int attempts = 0;
            int maxAttempts = Math.Max(8, maxKnownPeers);

            for (int i = 0; i < candidates.Count; i++)
            {
                if (ct.IsCancellationRequested) return;
                if (CountOutboundSessions(requireHandshake: false) >= targetOutbound)
                    return;
                if (attempts >= maxAttempts)
                    return;

                var (ip, p) = candidates[i];
                if (SelfPeerGuard.IsSelf(ip, p)) continue;
                if (IsPeerConnected(ip, p)) continue;
                if (PeerFailTracker.ShouldEnforceCooldown(ip)) continue;

                await ConnectAsync(ip, p, ct).ConfigureAwait(false);
                attempts++;
            }
        }

        public async Task ConnectKnownPeersAsync(CancellationToken ct = default, int maxAttempts = int.MaxValue)
        {
            int attempts = 0;
            int targetOutbound = Math.Clamp(
                OutboundTargetConnections,
                OutboundMinConnections,
                OutboundMaxConnections);

            foreach (var (ip, port) in GetPeersFromDb())
            {
                if (ct.IsCancellationRequested) return;
                if (CountOutboundSessions(requireHandshake: false) >= targetOutbound)
                    return;

                int p = port <= 0 ? DefaultPort : port;

                if (SelfPeerGuard.IsSelf(ip, p)) continue;
                if (IsPeerConnected(ip, p)) continue;
                if (PeerFailTracker.ShouldEnforceCooldown(ip)) continue;

                await ConnectAsync(ip, p, ct).ConfigureAwait(false);

                attempts++;
                if (attempts >= maxAttempts) return;
            }
        }

        public void StartPeerExchangeLoop(CancellationToken ct)
        {
            if (Interlocked.Exchange(ref _peerExchangeLoopStarted, 1) != 0)
                return;

            _ = Task.Run(async () =>
            {
                try
                {
                    while (!ct.IsCancellationRequested && Volatile.Read(ref _stopped) == 0)
                    {
                        _reachability.DecayIfExpired(PublicReachabilityTimeout);

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

                        try
                        {
                            await BroadcastAsync(MsgType.InvBlock, Array.Empty<byte>(), ct: ct).ConfigureAwait(false);
                        }
                        catch { }

                        try { await Task.Delay(TimeSpan.FromMinutes(2), ct).ConfigureAwait(false); } catch { }
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref _peerExchangeLoopStarted, 0);
                }
            }, ct);
        }

        public void StartReconnectLoop(CancellationToken ct)
        {
            if (Interlocked.Exchange(ref _reconnectLoopStarted, 1) != 0)
                return;

            _ = Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested && Volatile.Read(ref _stopped) == 0)
                {
                    _reachability.DecayIfExpired(PublicReachabilityTimeout);

                    try
                    {
                        await ConnectSeedAndKnownPeersAsync(ct).ConfigureAwait(false);
                    }
                    catch
                    {
                    }

                    try
                    {
                        await ProbePublicClaimsAsync(ct).ConfigureAwait(false);
                    }
                    catch
                    {
                    }

                    var baseDelay = HasAnyHandshakePeerConnected()
                        ? ReconnectSteadyDelay
                        : ReconnectWhenDisconnectedDelay;

                    var delay = ApplyReconnectJitter(baseDelay);
                    try { await Task.Delay(delay, ct).ConfigureAwait(false); } catch { }
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
                if (block.BlockHash is not { Length: 32 } || IsZero32(block.BlockHash))
                    block.BlockHash = block.ComputeBlockHash();

                int targets = CountBroadcastTargets();
                if (targets <= 0)
                {
                    _log?.Warn("Gossip", $"No handshake peers for block gossip {Hex16(block.BlockHash!)}.");
                    return;
                }

                var invPayload = BlockDownloadManager.BuildHashListPayload(new[] { block.BlockHash! }, maxHashes: 1);
                await BroadcastAsync(MsgType.InvBlock, invPayload).ConfigureAwait(false);

                int size = BlockBinarySerializer.GetSize(block);
                var blockPayload = new byte[size];
                _ = BlockBinarySerializer.Write(blockPayload, block);
                await BroadcastAsync(MsgType.Block, blockPayload).ConfigureAwait(false);

                _log?.Info("Gossip", $"Block gossiped h={block.BlockHeight} {Hex16(block.BlockHash!)} to {targets} peer(s) (inv+block).");
            }
            catch (Exception ex)
            {
                _log?.Warn("Gossip", $"Block gossip failed: {ex.Message}");
            }
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
                    if (PeerFailTracker.ShouldEnforceCooldown(banKey))
                    {
                        _log?.Warn("P2P", $"rejecting inbound during cooldown {EndpointLogFormatter.FormatEndpoint(c.Client.RemoteEndPoint?.ToString() ?? "unknown")}");
                        try { c.Close(); } catch { }
                        continue;
                    }

                    var ns = c.GetStream();
                    if (_sessions.Count >= MaxSessions)
                    {
                        try { c.Close(); } catch { }
                        continue;
                    }

                    _reachability.MarkInbound();

                    var sess = new PeerSession
                    {
                        Client = c,
                        Stream = ns,
                        RemoteEndpoint = c.Client.RemoteEndPoint?.ToString() ?? "unknown",
                        RemoteBanKey = banKey,
                        IsInbound = true
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

        private async Task HandleClient(PeerSession s, CancellationToken ct)
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

                    s.LastMessageUtc = DateTime.UtcNow;
                    await Dispatch((MsgType)typeByte[0], payload, s, ct).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                string endpoint = EndpointLogFormatter.FormatEndpoint(s.RemoteEndpoint);
                if (IsExpectedSessionTermination(ex))
                    _log?.Info("P2P", $"session {endpoint} ended: {ex.Message}");
                else
                    _log?.Warn("P2P", $"session {endpoint} ended: {ex.Message}");
            }
            finally
            {
                _headerSyncManager.OnPeerDisconnected(s);
                _blockDownloadManager.OnPeerDisconnected(s);
                _sessions.TryRemove(s.RemoteEndpoint, out _);
            }
        }


        private async Task Dispatch(MsgType type, byte[] payload, PeerSession s, CancellationToken ct)
        {
            if (!s.HandshakeOk &&
                type != MsgType.Handshake &&
                type != MsgType.Ping &&
                type != MsgType.Pong)
            {
                PeerFailTracker.ReportFailure(GetBanKey(s));
                _log?.Warn("P2P", $"Rejected pre-handshake message {type} from {EndpointLogFormatter.FormatEndpoint(s.RemoteEndpoint)}.");
                try { s.Client.Close(); } catch { }
                _sessions.TryRemove(s.RemoteEndpoint, out _);
                return;
            }

            switch (type)
            {
                case MsgType.Handshake:
                    await HandleHandshakeAsync(payload, s, ct).ConfigureAwait(false);
                    return;

                case MsgType.Tx:
                    await HandleTx(payload, s, ct).ConfigureAwait(false);
                    return;

                case MsgType.Block:
                    if (!_blockDownloadManager.EnqueueBlockPayload(s, payload))
                        _log?.Warn("Sync", "Validation queue full. Dropping inbound block payload.");
                    return;

                case MsgType.InvBlock:
                    if (payload.Length == 0)
                        await HandleInvRequest(s, ct).ConfigureAwait(false);
                    else
                        _blockDownloadManager.OnInv(s, payload);
                    return;

                case MsgType.GetData:
                    await HandleGetData(payload, s, ct).ConfigureAwait(false);
                    return;

                case MsgType.Headers:
                    if (!s.RemoteIsPublic)
                        return; // ignore silently for non-public peers
                    _headerSyncManager.OnHeaders(s, payload);
                    return;

                case MsgType.GetHeaders:
                    await HandleGetHeaders(payload, s, ct).ConfigureAwait(false);
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

        private async Task HandleHandshakeAsync(byte[] payload, PeerSession s, CancellationToken ct)
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
                _log?.Warn(
                    "P2P",
                    $"Rejected peer {EndpointLogFormatter.FormatEndpoint(s.RemoteEndpoint)} on foreign network id {networkId} (expected {GenesisConfig.NetworkId}, inbound={s.IsInbound}, raw={s.RemoteEndpoint}).");
                try { s.Client.Close(); } catch { }
                _sessions.TryRemove(s.RemoteEndpoint, out _);
                return;
            }

            var peerId = payload.AsSpan(2, 32); // peerId is untrusted metadata; use only for self-loop detection.
            if (peerId.SequenceEqual(_nodeId))
            {
                var endpointIp = (s.Client.Client.RemoteEndPoint as IPEndPoint)?.Address?.ToString()
                                 ?? s.RemoteEndpoint;
                SelfPeerGuard.RememberSelf(endpointIp);
                SelfPeerGuard.RememberSelf(s.RemoteEndpoint);

                _log?.Warn("P2P", $"Rejected self-loop handshake from {EndpointLogFormatter.FormatHost(endpointIp)}.");
                try { s.Client.Close(); } catch { }
                _sessions.TryRemove(s.RemoteEndpoint, out _);
                return;
            }

            ushort portRaw = BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(34, 2));
            bool claimsListening = portRaw != 0;
            int port = claimsListening ? portRaw : DefaultPort;

            var ip = (s.Client.Client.RemoteEndPoint as IPEndPoint)?.Address?.ToString()
                     ?? s.RemoteEndpoint;

            s.RemoteClaimsListening = claimsListening;
            s.RemoteIpAdvertised = ip;
            s.RemotePortAdvertised = port;
            s.RemoteBanKey = NormalizeBanKey(ip);
            s.RemoteIsPublic = IsPeerMarkedPublic(ip, port);

            // Public proof for a remote peer exists when THIS node dials the peer successfully.
            // Inbound sessions (remote dialed us) are not proof of remote reachability.
            bool outboundReachabilityProof = !s.IsInbound && claimsListening && IsPublicRoutableIPv4Literal(ip);
            if (outboundReachabilityProof)
            {
                MarkPeerAsPublic(ip, port);
                s.RemoteIsPublic = true;

                if (TryBuildPeerEndpointKey(ip, port, out var endpointKey))
                    _publicClaimsByPex.TryRemove(endpointKey, out _);
            }
            else if (s.IsInbound && claimsListening && IsPublicRoutableIPv4Literal(ip))
            {
                // Inbound session is only a claim; keep it for outbound probe verification.
                _log?.Info(
                    "P2P",
                    $"inbound public claim registered: remoteEndpoint={EndpointLogFormatter.FormatEndpoint(s.RemoteEndpoint)}, detectedIp={EndpointLogFormatter.FormatHost(ip)}, advertisedPort={port}");
                NotePeerPublicClaim(ip, port);
                _ = TryProbePublicClaimImmediatelyAsync(ip, port);
            }

            if (claimsListening && IsPublicRoutableIPv4Literal(ip))
            {
                try
                {
                    PeerStore.MarkSeen(ip, port, (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(), GenesisConfig.NetworkId);
                }
                catch { }
            }

            bool firstHandshakeForSession = !s.HandshakeOk;
            s.HandshakeOk = true;
            PeerFailTracker.ReportSuccess(GetBanKey(s));
            _headerSyncManager.OnPeerReady(s);
            _blockDownloadManager.OnPeerReady(s);
            FlushRecoveryRevalidationQueue();

            if (firstHandshakeForSession)
            {
                _log?.Info("P2P",
                    $"handshake from {EndpointLogFormatter.FormatHostPort(ip, port)} (v{ver}, inbound={s.IsInbound}, public={s.RemoteIsPublic}, claimsListening={claimsListening}, advertisedPort={port})");

                try { await WriteFrame(s.Stream, MsgType.GetPeers, Array.Empty<byte>(), ct).ConfigureAwait(false); } catch { }
                try { await WriteFrame(s.Stream, MsgType.InvBlock, Array.Empty<byte>(), ct).ConfigureAwait(false); } catch { }
            }
        }

        private async Task HandleTx(byte[] payload, PeerSession s, CancellationToken ct)
        {
            string peerKey = GetBanKey(s);
            if (!AllowInboundTx(peerKey))
            {
                _log?.Warn("P2P", $"Inbound tx rate limit exceeded for {peerKey}.");
                return;
            }

            Transaction txMsg;
            try
            {
                txMsg = TxBinarySerializer.Read(payload);
            }
            catch (Exception ex)
            {
                PeerFailTracker.ReportFailure(peerKey);
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

        private async Task HandleBlock(byte[] payload, PeerSession s, CancellationToken ct)
        {
            if (!_blockDownloadManager.IsDownloadStarted)
                return;

            if (!_blockDownloadManager.EnqueueBlockPayload(s, payload))
                _log?.Warn("Sync", "Validation queue full. Dropping inbound block payload.");
            await Task.CompletedTask;
        }

        private async Task HandleBlockInternal(byte[] payload, PeerSession s, CancellationToken ct, bool? enforceRateLimitOverride)
        {
            string peerKey = GetBanKey(s);

            Block blk;
            try
            {
                blk = BlockBinarySerializer.Read(payload);
            }
            catch (Exception ex)
            {
                PeerFailTracker.ReportFailure(peerKey);
                _log?.Warn("P2P", $"Block decode failed: {ex.Message}");
                return;
            }

            var prevHash = blk.Header?.PreviousBlockHash;
            if (prevHash is not { Length: 32 })
            {
                PeerFailTracker.ReportFailure(peerKey);
                _log?.Warn("P2P", "Block rejected: missing/invalid PrevHash.");
                return;
            }

            if (blk.BlockHash is not { Length: 32 } || IsZero32(blk.BlockHash))
                blk.BlockHash = blk.ComputeBlockHash();

            bool enforceRateLimit = enforceRateLimitOverride ??
                                    (_blockDownloadManager.MarkBlockArrived(s, blk.BlockHash!) != BlockArrivalKind.Requested);

            bool replayedInternally = enforceRateLimitOverride.HasValue;
            if (!replayedInternally && !_blockDownloadManager.IsHashInActivePlan(blk.BlockHash!))
            {
                bool headerKnown = false;
                try { headerKnown = BlockIndexStore.ContainsHash(blk.BlockHash!); } catch { }

                bool outOfPlanPrevKnown = false;
                bool prevIsTip = false;
                try
                {
                    outOfPlanPrevKnown = BlockIndexStore.ContainsHash(prevHash);
                    ulong tipH = BlockStore.GetLatestHeight();
                    var tipHash = BlockStore.GetCanonicalHashAtHeight(tipH);
                    prevIsTip = tipHash is { Length: 32 } && BytesEqual32(prevHash, tipHash);
                }
                catch { }

                if (_headerSyncManager.IsCompleted && !headerKnown)
                    _headerSyncManager.RequestResync("block-out-of-plan-missing-header");

                bool allowOutOfPlan = headerKnown || outOfPlanPrevKnown || prevIsTip;
                if (!allowOutOfPlan)
                {
                    _log?.Info(
                        "Sync",
                        $"Ignoring out-of-plan block {Hex16(blk.BlockHash!)} from {peerKey} (headerKnown={headerKnown}).");
                    return;
                }

                _log?.Info(
                    "Sync",
                    $"Accepting out-of-plan block {Hex16(blk.BlockHash!)} from {peerKey} (headerKnown={headerKnown}, prevKnown={outOfPlanPrevKnown}, prevIsTip={prevIsTip}).");
            }

            if (enforceRateLimit && !AllowInboundBlock(peerKey))
            {
                _log?.Warn("P2P", $"Inbound block rate limit exceeded for {peerKey}.");
                _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: false);
                return;
            }

            if (BlockIndexStore.IsBadOrHasBadAncestor(blk.BlockHash!))
            {
                _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: false);
                return;
            }

            if (BlockIndexStore.IsBadOrHasBadAncestor(prevHash))
            {
                MarkBadChainAndReplan(blk.BlockHash!, BadChainReason.StoredAsInvalid, "parent-bad");
                _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: false);
                return;
            }

            bool alreadyHavePayload = BlockIndexStore.GetLocation(blk.BlockHash!) != null;
            if (alreadyHavePayload &&
                BlockIndexStore.TryGetBadFlags(blk.BlockHash!, out bool isBad, out bool badAncestor, out _, null) &&
                (isBad || badAncestor))
            {
                _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: false);
                return;
            }

            if (alreadyHavePayload && BlockIndexStore.TryGetStatus(blk.BlockHash!, out int existingStatus))
            {
                if (BlockIndexStore.IsValidatedStatus(existingStatus))
                {
                    _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: true);
                    LogKnownBlockAlready(blk.BlockHash!);
                    return;
                }

                if (existingStatus == BlockIndexStore.StatusSideStateInvalid)
                {
                    TryReplanFromBestHeader("known-invalid");
                    _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: false);
                    return;
                }
            }

            bool isTipExtending;
            ulong tipHSnapshot;
            byte[]? tipHashSnapshot;
            bool prevKnown;
            ulong prevHeight = 0;

            lock (Db.Sync)
            {
                tipHSnapshot = BlockStore.GetLatestHeight();
                tipHashSnapshot = BlockStore.GetCanonicalHashAtHeight(tipHSnapshot);
                isTipExtending = tipHashSnapshot is { Length: 32 } && BytesEqual32(prevHash, tipHashSnapshot);
                prevKnown = BlockIndexStore.TryGetMeta(prevHash, out prevHeight, out _, out _);
            }

            if (isTipExtending)
            {
                blk.BlockHeight = tipHSnapshot + 1UL;
            }
            else if (prevKnown)
            {
                blk.BlockHeight = prevHeight + 1UL;
            }
            else
            {
                if (!BlockValidator.ValidateNetworkBlockStateless(blk, requirePrevKnown: false, out var looseReason))
                {
                    PeerFailTracker.ReportFailure(peerKey);
                    _log?.Warn("P2P", $"Out-of-order block rejected: {looseReason}");
                    MarkBadChainAndReplan(blk.BlockHash!, BadChainReason.BlockStatelessInvalid, $"out-of-order:{looseReason}");
                    _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: false);
                    return;
                }

                if (TryStoreOrphan(payload, blk, peerKey, out var orphanReason))
                {
                    _log?.Info("P2P", $"Orphan buffered: h={blk.BlockHeight} {Hex16(blk.BlockHash!)}");
                }
                else
                {
                    _log?.Warn("P2P", $"Orphan dropped: {orphanReason}");
                    _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: false);
                }
                return;
            }

            bool persistedRaw = alreadyHavePayload;
            if (!alreadyHavePayload)
            {
                try
                {
                    PersistRawBlockAsHaveBlock(blk);
                    persistedRaw = true;
                }
                catch (Exception ex)
                {
                    _log?.Warn("P2P", $"Block raw persist failed: {ex.Message}");
                    _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: false);
                    return;
                }
            }

            bool validateAsSidechain = !isTipExtending;
            if (isTipExtending)
            {
                if (!BlockValidator.ValidateNetworkTipBlock(blk, out var reason))
                {
                    if (IsTipMovedReason(reason))
                    {
                        // Canonical tip advanced/reorged while this block was in-flight.
                        // Retry as sidechain candidate instead of poisoning the branch as invalid.
                        validateAsSidechain = true;
                        _log?.Info("P2P", $"Tip moved during validation; retrying as sidechain: {Hex16(blk.BlockHash!)}");
                    }
                    else
                    {
                        PeerFailTracker.ReportFailure(peerKey);
                        _log?.Warn("P2P", $"Block rejected (tip-ext): {reason}");
                        if (persistedRaw) MarkStoredBlockInvalidNoThrow(
                            blk.BlockHash!,
                            BadChainReason.BlockStateInvalid,
                            $"tip-invalid:{reason}");
                        _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: false);
                        return;
                    }
                }
            }

            if (validateAsSidechain)
            {
                if (!BlockValidator.ValidateNetworkSideBlockStateless(blk, out var reason))
                {
                    if (IsPreviousBlockMissingReason(reason))
                    {
                        if (TryStoreOrphan(payload, blk, peerKey, out var orphanReason))
                        {
                            _log?.Info("P2P", $"Sidechain deferred (parent missing): h={blk.BlockHeight} {Hex16(blk.BlockHash!)}");
                        }
                        else
                        {
                            _log?.Warn("P2P", $"Sidechain defer dropped: {orphanReason}");
                            _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: false);
                        }
                        return;
                    }

                    PeerFailTracker.ReportFailure(peerKey);
                    _log?.Warn("P2P", $"Block rejected (sidechain): {reason}");
                    if (persistedRaw) MarkStoredBlockInvalidNoThrow(
                        blk.BlockHash!,
                        BadChainReason.BlockStatelessInvalid,
                        $"side-invalid:{reason}");
                    _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: false);
                    return;
                }

                if (!PassesSidechainAdmission(
                        blk,
                        canonicalTipHeight: tipHSnapshot,
                        canonicalTipHash: tipHashSnapshot,
                        prevHeight: prevHeight,
                        payloadBytes: payload.Length,
                        out var admissionReason))
                {
                    _log?.Warn("P2P", $"Sidechain block rejected by admission gate: {admissionReason}");
                    _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: false);
                    return;
                }
            }

            bool extendedCanon = false;
            bool stateValidated = false;
            ulong newCanonHeight = 0;

            try
            {
                lock (Db.Sync)
                {
                    using var tx = Db.Connection!.BeginTransaction();

                    if (TryExtendCanonTipNoReorg(blk, tx, out newCanonHeight))
                    {
                        extendedCanon = true;
                        BlockIndexStore.SetStatus(blk.BlockHash!, BlockIndexStore.StatusCanonicalStateValidated, tx);
                    }
                    else
                    {
                        // Side-chain payload is persisted, but not considered state-validated yet.
                        BlockIndexStore.SetStatus(blk.BlockHash!, BlockIndexStore.StatusHaveBlockPayload, tx);
                    }

                    tx.Commit();
                }
            }
            catch (Exception ex)
            {
                _log?.Warn("P2P", $"Block store failed: {ex.Message}");
                _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: false);
                return;
            }

            try { _mempool.RemoveIncluded(blk); } catch { }

            if (!extendedCanon)
            {
                try
                {
                    ChainSelector.MaybeAdoptNewTip(blk.BlockHash!, _log, _mempool);
                    if (BlockIndexStore.TryGetStatus(blk.BlockHash!, out var afterStatus) &&
                        BlockIndexStore.IsValidatedStatus(afterStatus))
                    {
                        stateValidated = true;
                    }
                }
                catch (Exception ex)
                {
                    _log?.Warn("P2P", $"Tip adoption failed for {Hex16(blk.BlockHash!)}: {ex.Message}");
                }
            }
            else
            {
                stateValidated = true;
            }

            if (BlockIndexStore.TryGetBadFlags(blk.BlockHash!, out var isBadNow, out var badAncestorNow, out _, null) &&
                (isBadNow || badAncestorNow))
            {
                _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: false);
                TryReplanFromBestHeader("post-validation-bad");
                return;
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

            if (ShouldRelayInv(blk.BlockHash!))
                await BroadcastInvAsync(blk.BlockHash!, exceptEndpoint: s.RemoteEndpoint, ct: ct).ConfigureAwait(false);

            if (extendedCanon)
                _log?.Info("P2P", $"Block stored + canon-extended: h={newCanonHeight} {Hex16(blk.BlockHash!)}");
            else
                _log?.Info("P2P", $"Block stored (side/reorg candidate): h={blk.BlockHeight} {Hex16(blk.BlockHash!)}");

            PeerFailTracker.ReportSuccess(peerKey);
            if (stateValidated)
                _blockDownloadManager.MarkBlockValidated(blk.BlockHash!, valid: true);

            await PromoteOrphansForParentAsync(blk.BlockHash!, s, ct).ConfigureAwait(false);
        }

        private async Task HandleGetBlock(byte[] payload, PeerSession s, CancellationToken ct)
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

        private async Task HandleInvRequest(PeerSession s, CancellationToken ct)
        {
            var hashes = GetCanonicalInvHashes(BlockDownloadManager.MaxInvHashes);
            var payload = BlockDownloadManager.BuildHashListPayload(hashes, BlockDownloadManager.MaxInvHashes);
            await WriteFrame(s.Stream, MsgType.InvBlock, payload, ct).ConfigureAwait(false);
        }

        private async Task HandleGetData(byte[] payload, PeerSession s, CancellationToken ct)
        {
            if (!BlockDownloadManager.TryParseHashListPayload(payload, BlockDownloadManager.MaxInflightGlobal, out var hashes))
                return;

            int sent = 0;
            for (int i = 0; i < hashes.Count; i++)
            {
                if (ct.IsCancellationRequested) return;
                if (sent >= BlockDownloadManager.MaxInflightGlobal) return;

                var blk = BlockStore.GetBlockByHash(hashes[i]);
                if (blk == null) continue;

                int size = BlockBinarySerializer.GetSize(blk);
                var buf = new byte[size];
                _ = BlockBinarySerializer.Write(buf, blk);
                await WriteFrame(s.Stream, MsgType.Block, buf, ct).ConfigureAwait(false);
                sent++;
            }
        }

        private async Task HandleGetHeaders(byte[] payload, PeerSession s, CancellationToken ct)
        {
            var headersPayload = _headerSyncManager.BuildHeadersResponseForLocator(payload);
            await WriteFrame(s.Stream, MsgType.Headers, headersPayload, ct).ConfigureAwait(false);
        }

        private async Task HandleGetTip(PeerSession s, CancellationToken ct)
        {
            ulong latest;
            byte[] tipHash;
            UInt128 tipWork = 0;

            lock (Db.Sync)
            {
                if (BlockIndexStore.TryGetBestValidatedTip(out var bestValidatedHash, out var bestValidatedHeight, out var bestValidatedWork))
                {
                    latest = bestValidatedHeight;
                    tipHash = bestValidatedHash;
                    tipWork = bestValidatedWork;
                }
                else
                {
                    latest = BlockStore.GetLatestHeight();
                    tipHash = BlockStore.GetCanonicalHashAtHeight(latest) ?? new byte[32];
                    if (tipHash is { Length: 32 } && !IsZero32(tipHash))
                        tipWork = BlockIndexStore.GetChainwork(tipHash);
                }
            }

            Span<byte> cw = stackalloc byte[16];
            U128.WriteBE(cw, tipWork);

            var resp = new byte[8 + 32 + 16];
            BinaryPrimitives.WriteUInt64LittleEndian(resp.AsSpan(0, 8), latest);
            tipHash.CopyTo(resp, 8);
            cw.CopyTo(resp.AsSpan(8 + 32, 16));

            _log?.Info("P2P", $"GetTip received -> sending tip height {latest}");
            await WriteFrame(s.Stream, MsgType.Tip, resp, ct).ConfigureAwait(false);
        }


        private async Task BroadcastInvAsync(byte[] blockHash, string? exceptEndpoint, CancellationToken ct)
        {
            if (blockHash is not { Length: 32 })
                return;

            var payload = BlockDownloadManager.BuildHashListPayload(new[] { blockHash }, maxHashes: 1);
            await BroadcastAsync(MsgType.InvBlock, payload, exceptEndpoint: exceptEndpoint, ct: ct).ConfigureAwait(false);
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

        private static void PersistRawBlockAsHaveBlock(Block blk)
        {
            lock (Db.Sync)
            {
                using var tx = Db.Connection!.BeginTransaction();
                BlockStore.SaveBlock(blk, tx, BlockIndexStore.StatusHaveBlockPayload);
                tx.Commit();
            }
        }

        private void MarkStoredBlockInvalidNoThrow(byte[] blockHash, BadChainReason reason, string context)
        {
            if (blockHash is not { Length: 32 })
                return;

            try
            {
                MarkBadChainAndReplan(blockHash, reason, context);
            }
            catch
            {
            }
        }


        private async Task BroadcastAsync(MsgType t, byte[] payload, string? exceptEndpoint = null, CancellationToken ct = default)
        {
            if (Volatile.Read(ref _stopped) != 0) return;

            foreach (var kv in _sessions)
            {
                if (exceptEndpoint != null && string.Equals(kv.Key, exceptEndpoint, StringComparison.Ordinal))
                    continue;

                var sess = kv.Value;
                if (!sess.HandshakeOk && t != MsgType.Handshake)
                    continue;
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

        private int CountOutboundSessions(bool requireHandshake)
        {
            int count = 0;
            foreach (var kv in _sessions)
            {
                var s = kv.Value;
                if (s.IsInbound) continue;
                if (requireHandshake && !s.HandshakeOk) continue;
                count++;
            }
            return count;
        }

        private int CountBroadcastTargets(string? exceptEndpoint = null)
        {
            int count = 0;
            foreach (var kv in _sessions)
            {
                if (exceptEndpoint != null && string.Equals(kv.Key, exceptEndpoint, StringComparison.Ordinal))
                    continue;

                var s = kv.Value;
                if (s == null || !s.HandshakeOk || !s.Client.Connected)
                    continue;

                count++;
            }

            return count;
        }

        private IReadOnlyCollection<PeerSession> GetSessionSnapshot()
        {
            var list = new List<PeerSession>(_sessions.Count);
            foreach (var kv in _sessions)
                list.Add(kv.Value);
            return list;
        }

        private static async Task SendFrameViaSessionAsync(PeerSession s, MsgType t, byte[] payload, CancellationToken ct)
        {
            await WriteFrame(s.Stream, t, payload, ct).ConfigureAwait(false);
        }

        private static bool HaveBlockLocally(byte[] hash)
            => hash is { Length: 32 } && BlockIndexStore.GetLocation(hash) != null;

        private static bool IsHeaderCandidateDownloadable(byte[] hash)
            => hash is { Length: 32 } && !BlockIndexStore.IsBadOrHasBadAncestor(hash);

        public void RequestSyncNow(string reason = "manual")
        {
            if (Volatile.Read(ref _stopped) != 0)
                return;

            _headerSyncManager.RequestResync(reason);
            TryReplanFromBestHeader(reason);
        }

        private void OnHeaderSyncCompleted(HeaderSyncPlan plan)
        {
            if (_blockDownloadManager.IsDownloadStarted)
            {
                _blockDownloadManager.ReplanDownload(plan.MissingBlockHashes, plan.RevalidateBlockHashes);
            }
            else
            {
                _blockDownloadManager.BeginDownload(plan.MissingBlockHashes, plan.RevalidateBlockHashes);
            }
        }

        private bool TryReplanFromBestHeader(string context)
        {
            if (!_headerSyncManager.TryBuildCurrentPlan(out var plan))
            {
                _log?.Warn("Sync", $"Replan skipped ({context}): no eligible header chain.");
                _blockDownloadManager.ReplanDownload(Array.Empty<byte[]>(), Array.Empty<byte[]>());
                return false;
            }

            _blockDownloadManager.ReplanDownload(plan.MissingBlockHashes, plan.RevalidateBlockHashes);
            _log?.Info(
                "Sync",
                $"Replan ({context}): best={Hex16(plan.BestHeaderHash)}, fork={Hex16(plan.ForkPointHash)}, missing={plan.MissingBlockHashes.Count}, revalidate={plan.RevalidateBlockHashes.Count}");
            return true;
        }

        private void MarkBadChainAndReplan(byte[] blockHash, BadChainReason reason, string context)
        {
            if (blockHash is not { Length: 32 })
                return;

            lock (_badChainGate)
            {
                int affected;
                try
                {
                    affected = BlockIndexStore.MarkBadAndDescendants(blockHash, (int)reason);
                }
                catch (Exception ex)
                {
                    _log?.Warn("Sync", $"MarkBad failed ({context}): {ex.Message}");
                    return;
                }

                _log?.Warn("Sync", $"Marked bad chain: root={Hex16(blockHash)} reason={(int)reason} affected={affected} ({context})");
                TryReplanFromBestHeader(context);
            }
        }

        private void RevalidateStoredBlock(byte[] hash)
        {
            if (hash is not { Length: 32 })
                return;

            if (BlockIndexStore.IsBadOrHasBadAncestor(hash))
                return;

            try
            {
                var block = BlockStore.GetBlockByHash(hash);
                if (block?.Header != null && TryGetAnyValidationPeer(out var peer))
                {
                    int size = BlockBinarySerializer.GetSize(block);
                    var payload = new byte[size];
                    _ = BlockBinarySerializer.Write(payload, block);
                    _blockDownloadManager.EnqueueBlockPayload(peer, payload, enforceRateLimitOverride: false);
                    return;
                }

                if (block?.Header != null)
                {
                    _pendingRecoveryRevalidate.Enqueue((byte[])hash.Clone());
                    return;
                }

                ChainSelector.MaybeAdoptNewTip(hash, _log, _mempool);
            }
            catch
            {
            }
        }

        private void FlushRecoveryRevalidationQueue()
        {
            if (!TryGetAnyValidationPeer(out var peer))
                return;

            int drained = 0;
            while (drained < 256 && _pendingRecoveryRevalidate.TryDequeue(out var hash))
            {
                try
                {
                    var block = BlockStore.GetBlockByHash(hash);
                    if (block?.Header == null)
                        continue;

                    int size = BlockBinarySerializer.GetSize(block);
                    var payload = new byte[size];
                    _ = BlockBinarySerializer.Write(payload, block);
                    _blockDownloadManager.EnqueueBlockPayload(peer, payload, enforceRateLimitOverride: false);
                    drained++;
                }
                catch
                {
                }
            }
        }

        private bool TryGetAnyValidationPeer(out PeerSession peer)
        {
            peer = null!;
            foreach (var kv in _sessions)
            {
                var s = kv.Value;
                if (s == null || !s.HandshakeOk || !s.Client.Connected)
                    continue;
                peer = s;
                return true;
            }

            return false;
        }

        private async Task ProcessValidationWorkItemAsync(ValidationWorkItem item, CancellationToken ct)
        {
            await _validationSerializeGate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await HandleBlockInternal(item.Payload, item.Peer, ct, item.EnforceRateLimitOverride).ConfigureAwait(false);
            }
            finally
            {
                _validationSerializeGate.Release();
            }
        }

        private async Task ProbePublicClaimsAsync(CancellationToken ct)
        {
            int probed = 0;

            foreach (var kv in _publicClaimsByPex)
            {
                if (ct.IsCancellationRequested || probed >= MaxPublicClaimProbesPerReconnectTick)
                    break;

                string key = kv.Key;
                if (!TryParsePeerEndpointKey(key, out var ip, out var port))
                {
                    _publicClaimsByPex.TryRemove(key, out _);
                    continue;
                }

                if (!IsPublicRoutableIPv4Literal(ip) || SelfPeerGuard.IsSelf(ip, port))
                {
                    _publicClaimsByPex.TryRemove(key, out _);
                    continue;
                }

                if (IsPeerMarkedPublic(ip, port))
                {
                    _publicClaimsByPex.TryRemove(key, out _);
                    continue;
                }

                _log?.Info("P2P", $"probing public claim (periodic): {EndpointLogFormatter.FormatHostPort(ip, port)}");
                var probe = await TryTcpReachabilityProbeAsync(ip, port, ct).ConfigureAwait(false);
                probed++;

                if (!probe.reachable)
                {
                    _log?.Info("P2P", $"public claim probe failed for {EndpointLogFormatter.FormatHostPort(ip, port)}: {probe.reason}");
                    continue;
                }

                MarkPeerAsPublic(ip, port);
                _publicClaimsByPex.TryRemove(key, out _);

                try
                {
                    PeerStore.MarkSeen(ip, port, (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(), GenesisConfig.NetworkId);
                }
                catch { }

                _log?.Info("P2P", $"public reachability proven via outbound probe: {EndpointLogFormatter.FormatHostPort(ip, port)}");
            }
        }

        private async Task TryProbePublicClaimImmediatelyAsync(string ip, int port)
        {
            if (Volatile.Read(ref _stopped) != 0)
                return;
            if (!IsPublicRoutableIPv4Literal(ip))
                return;
            if (port <= 0 || port > 65535)
                return;
            if (SelfPeerGuard.IsSelf(ip, port))
                return;
            if (IsPeerMarkedPublic(ip, port))
                return;
            if (!TryBuildPeerEndpointKey(ip, port, out var key))
                return;
            if (!_publicClaimsByPex.ContainsKey(key))
                return;

            _log?.Info("P2P", $"probing public claim (immediate): {EndpointLogFormatter.FormatHostPort(ip, port)}");
            var probe = await TryTcpReachabilityProbeAsync(ip, port, CancellationToken.None).ConfigureAwait(false);
            if (!probe.reachable)
            {
                _log?.Info("P2P", $"immediate public claim probe failed for {EndpointLogFormatter.FormatHostPort(ip, port)}: {probe.reason}");
                return;
            }

            MarkPeerAsPublic(ip, port);
            _publicClaimsByPex.TryRemove(key, out _);

            try
            {
                PeerStore.MarkSeen(ip, port, (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(), GenesisConfig.NetworkId);
            }
            catch { }

            _log?.Info("P2P", $"public reachability proven via immediate outbound probe: {EndpointLogFormatter.FormatHostPort(ip, port)}");
        }

        private static async Task<(bool reachable, string reason)> TryTcpReachabilityProbeAsync(string ip, int port, CancellationToken ct)
        {
            if (!IsPublicRoutableIPv4Literal(ip))
                return (false, "non-public IPv4");
            if (port <= 0 || port > 65535)
                return (false, "invalid port");

            using var client = new TcpClient(AddressFamily.InterNetwork)
            {
                NoDelay = true,
                ReceiveTimeout = (int)PublicClaimProbeTimeout.TotalMilliseconds,
                SendTimeout = (int)PublicClaimProbeTimeout.TotalMilliseconds
            };

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(PublicClaimProbeTimeout);

            try
            {
#if NET8_0_OR_GREATER
                await client.ConnectAsync(ip, port, timeoutCts.Token).ConfigureAwait(false);
#else
                var connectTask = client.ConnectAsync(ip, port);
                var done = await Task.WhenAny(connectTask, Task.Delay(PublicClaimProbeTimeout, timeoutCts.Token)).ConfigureAwait(false);
                if (!ReferenceEquals(done, connectTask))
                    return (false, $"timeout after {(int)PublicClaimProbeTimeout.TotalSeconds}s");
                await connectTask.ConfigureAwait(false);
#endif
                return client.Connected
                    ? (true, "connected")
                    : (false, "connect call returned disconnected");
            }
            catch (OperationCanceledException)
            {
                return (false, $"timeout after {(int)PublicClaimProbeTimeout.TotalSeconds}s");
            }
            catch (SocketException se)
            {
                return (false, $"socket error: {se.SocketErrorCode}");
            }
            catch
            {
                return (false, "connection failed");
            }
        }

        public void NotePeerPublicClaim(string ip, int port)
        {
            if (!IsPublicRoutableIPv4Literal(ip)) return;
            if (port <= 0 || port > 65535) return;
            if (TryBuildPeerEndpointKey(ip, port, out var key))
                _publicClaimsByPex[key] = 0;
        }

        public List<(string ip, int port)> GetPublicPeerCandidates(int maxPeers)
        {
            if (maxPeers <= 0)
                return new List<(string ip, int port)>();

            var list = new List<(string ip, int port)>(maxPeers);
            var seen = new HashSet<string>(StringComparer.Ordinal);

            foreach (var kv in _sessions)
            {
                if (list.Count >= maxPeers) break;

                var s = kv.Value;
                if (!s.HandshakeOk || !s.RemoteIsPublic) continue;
                if (string.IsNullOrWhiteSpace(s.RemoteIpAdvertised)) continue;
                if (s.RemotePortAdvertised is not int p || p <= 0 || p > 65535) continue;
                if (!IsPublicRoutableIPv4Literal(s.RemoteIpAdvertised!)) continue;
                if (SelfPeerGuard.IsSelf(s.RemoteIpAdvertised!, p)) continue;

                if (!TryBuildPeerEndpointKey(s.RemoteIpAdvertised!, p, out var key)) continue;
                if (!seen.Add(key)) continue;
                list.Add((s.RemoteIpAdvertised!, p));
            }

            foreach (var kv in _publicPeerEndpoints)
            {
                if (list.Count >= maxPeers) break;
                if (!seen.Add(kv.Key)) continue;
                if (!TryParsePeerEndpointKey(kv.Key, out var ip, out var port)) continue;
                if (!IsPublicRoutableIPv4Literal(ip)) continue;
                if (SelfPeerGuard.IsSelf(ip, port)) continue;
                list.Add((ip, port));
            }

            return list;
        }

        public List<(string ip, int port)> GetPeerCandidatesForPex(int maxPeers, int unverifiedPercent = 20)
        {
            if (maxPeers <= 0)
                return new List<(string ip, int port)>();

            unverifiedPercent = Math.Clamp(unverifiedPercent, 0, 100);
            int reserveForUnverified = (int)Math.Ceiling(maxPeers * (unverifiedPercent / 100.0));

            var result = new List<(string ip, int port)>(maxPeers);
            var seen = new HashSet<string>(StringComparer.Ordinal);

            var verified = GetPublicPeerCandidates(maxPeers);
            var verifiedDeferred = new List<(string ip, int port)>();
            int verifiedDirectLimit = Math.Max(0, maxPeers - reserveForUnverified);

            for (int i = 0; i < verified.Count; i++)
            {
                var (ip, port) = verified[i];
                if (!TryBuildPeerEndpointKey(ip, port, out var key)) continue;
                if (!seen.Add(key)) continue;

                if (result.Count < verifiedDirectLimit)
                    result.Add((ip, port));
                else
                    verifiedDeferred.Add((ip, port));
            }

            var recent = PeerStore.GetRecentPeers(limit: Math.Max(64, maxPeers * 4));
            int unverifiedAdded = 0;

            for (int i = 0; i < recent.Count && result.Count < maxPeers; i++)
            {
                var (ip, port, _) = recent[i];
                int p = (port <= 0 || port > 65535) ? DefaultPort : port;

                if (!IsPublicRoutableIPv4Literal(ip)) continue;
                if (SelfPeerGuard.IsSelf(ip, p)) continue;
                if (IsPeerMarkedPublic(ip, p)) continue;
                if (!TryBuildPeerEndpointKey(ip, p, out var key)) continue;
                if (!seen.Add(key)) continue;

                result.Add((ip, p));
                unverifiedAdded++;
                if (unverifiedAdded >= reserveForUnverified)
                    break;
            }

            for (int i = 0; i < verifiedDeferred.Count && result.Count < maxPeers; i++)
            {
                result.Add(verifiedDeferred[i]);
            }

            for (int i = 0; i < recent.Count && result.Count < maxPeers; i++)
            {
                var (ip, port, _) = recent[i];
                int p = (port <= 0 || port > 65535) ? DefaultPort : port;

                if (!IsPublicRoutableIPv4Literal(ip)) continue;
                if (SelfPeerGuard.IsSelf(ip, p)) continue;
                if (!TryBuildPeerEndpointKey(ip, p, out var key)) continue;
                if (!seen.Add(key)) continue;

                result.Add((ip, p));
            }

            return result;
        }

        private void MarkPeerAsPublic(string ip, int port)
        {
            if (!IsPublicRoutableIPv4Literal(ip)) return;
            if (port <= 0 || port > 65535) return;
            if (!TryBuildPeerEndpointKey(ip, port, out var key)) return;

            _publicPeerEndpoints[key] = 0;

            foreach (var kv in _sessions)
            {
                var s = kv.Value;
                if (string.IsNullOrWhiteSpace(s.RemoteIpAdvertised)) continue;
                if (s.RemotePortAdvertised is not int p || p <= 0 || p > 65535) continue;
                if (!TryBuildPeerEndpointKey(s.RemoteIpAdvertised!, p, out var skey)) continue;
                if (string.Equals(skey, key, StringComparison.Ordinal))
                    s.RemoteIsPublic = true;
            }
        }

        private bool IsPeerMarkedPublic(string ip, int port)
        {
            if (!TryBuildPeerEndpointKey(ip, port, out var key))
                return false;
            return _publicPeerEndpoints.ContainsKey(key);
        }

        private static bool TryBuildPeerEndpointKey(string ip, int port, out string key)
        {
            key = string.Empty;
            if (string.IsNullOrWhiteSpace(ip)) return false;
            if (port <= 0 || port > 65535) return false;

            string host = NormalizeBanKey(ip);
            if (host.StartsWith("::ffff:", StringComparison.Ordinal))
                host = host[7..];

            if (!IPAddress.TryParse(host, out var addr)) return false;
            if (addr.AddressFamily != AddressFamily.InterNetwork) return false;

            key = $"{addr}:{port}";
            return true;
        }

        private static bool TryParsePeerEndpointKey(string key, out string ip, out int port)
        {
            ip = string.Empty;
            port = 0;
            if (string.IsNullOrWhiteSpace(key)) return false;

            int idx = key.LastIndexOf(':');
            if (idx <= 0 || idx >= key.Length - 1) return false;
            if (!int.TryParse(key[(idx + 1)..], out port)) return false;
            if (port <= 0 || port > 65535) return false;

            ip = key[..idx];
            return !string.IsNullOrWhiteSpace(ip);
        }

        private static List<byte[]> GetCanonicalInvHashes(int maxHashes)
        {
            if (maxHashes <= 0)
                return new List<byte[]>();

            var hashes = new List<byte[]>(Math.Min(maxHashes, 4096));

            lock (Db.Sync)
            {
                ulong tip = BlockStore.GetLatestHeight();
                if (tip == 0)
                {
                    var genesis = BlockStore.GetCanonicalHashAtHeight(0);
                    if (genesis is { Length: 32 })
                        hashes.Add(genesis);
                    return hashes;
                }

                ulong start = tip >= (ulong)maxHashes ? tip - (ulong)maxHashes + 1UL : 0UL;
                for (ulong h = start; h <= tip; h++)
                {
                    var hash = BlockStore.GetCanonicalHashAtHeight(h);
                    if (hash is { Length: 32 })
                        hashes.Add(hash);
                }
            }

            return hashes;
        }

        private bool HasAnyHandshakePeerConnected()
        {
            foreach (var kv in _sessions)
            {
                if (kv.Value.HandshakeOk)
                    return true;
            }
            return false;
        }

        private static TimeSpan ApplyReconnectJitter(TimeSpan baseDelay)
        {
            if (baseDelay <= TimeSpan.Zero || ReconnectJitterFraction <= 0)
                return baseDelay;

            double delta = (Random.Shared.NextDouble() * 2.0 - 1.0) * ReconnectJitterFraction;
            double factor = 1.0 + delta;
            long ticks = (long)(baseDelay.Ticks * factor);

            long minTicks = TimeSpan.FromSeconds(1).Ticks;
            if (ticks < minTicks) ticks = minTicks;

            return TimeSpan.FromTicks(ticks);
        }

        private static void TryRememberSeedEndpointNoThrow(string hostOrIp, int port)
        {
            if (string.IsNullOrWhiteSpace(hostOrIp)) return;
            if (port <= 0 || port > 65535) return;

            try
            {
                string normalized = NormalizeBanKey(hostOrIp);
                if (normalized.StartsWith("::ffff:", StringComparison.Ordinal))
                    normalized = normalized[7..];

                if (!IPAddress.TryParse(normalized, out var addr)) return;
                if (addr.AddressFamily != AddressFamily.InterNetwork) return;

                string ip = addr.ToString();
                if (SelfPeerGuard.IsSelf(ip, port)) return;

                PeerStore.AnnouncePeer(ip, port, GenesisConfig.NetworkId);
            }
            catch
            {
            }
        }

        private bool AllowInboundBlock(string peerKey)
        {
            if (string.IsNullOrWhiteSpace(peerKey))
                return true;

            var now = DateTime.UtcNow;
            var bucket = _blockRateByPeer.GetOrAdd(peerKey, _ => new RateBucket());
            lock (bucket)
            {
                if ((now - bucket.WindowStartUtc) >= BlockRateWindow)
                {
                    bucket.WindowStartUtc = now;
                    bucket.Count = 0;
                }

                bucket.Count++;
                return bucket.Count <= MaxInboundBlocksPerWindowPerPeer;
            }
        }

        private bool AllowInboundTx(string peerKey)
        {
            if (string.IsNullOrWhiteSpace(peerKey))
                return true;

            var now = DateTime.UtcNow;
            var bucket = _txRateByPeer.GetOrAdd(peerKey, _ => new RateBucket());
            lock (bucket)
            {
                if ((now - bucket.WindowStartUtc) >= TxRateWindow)
                {
                    bucket.WindowStartUtc = now;
                    bucket.Count = 0;
                }

                bucket.Count++;
                return bucket.Count <= MaxInboundTxPerWindowPerPeer;
            }
        }

        private bool PassesSidechainAdmission(
            Block blk,
            ulong canonicalTipHeight,
            byte[]? canonicalTipHash,
            ulong prevHeight,
            int payloadBytes,
            out string reason)
        {
            reason = "ok";

            var prevHash = blk.Header?.PreviousBlockHash;
            if (prevHash is not { Length: 32 })
            {
                reason = "invalid prev hash";
                return false;
            }

            if (BlockIndexStore.IsBadOrHasBadAncestor(prevHash))
            {
                reason = "parent is marked bad";
                return false;
            }

            ulong candidateHeight = prevHeight + 1UL;
            if (candidateHeight + MaxSideReorgDepth < canonicalTipHeight)
            {
                reason = $"fork too deep below tip (candidate={candidateHeight}, tip={canonicalTipHeight})";
                return false;
            }

            int siblingCount = BlockIndexStore.CountChildren(prevHash);
            if (siblingCount >= MaxSideChildrenPerPrevHash)
            {
                reason = $"too many side children for parent ({siblingCount} >= {MaxSideChildrenPerPrevHash})";
                return false;
            }

            var (sideCount, sideBytes) = BlockIndexStore.GetNonCanonicalPayloadStats();
            if (sideCount >= MaxSideCandidateCount)
            {
                reason = $"side pool count limit reached ({sideCount} >= {MaxSideCandidateCount})";
                return false;
            }

            if (sideBytes + payloadBytes > MaxSideCandidateBytes)
            {
                reason = $"side pool byte limit reached ({sideBytes + payloadBytes} > {MaxSideCandidateBytes})";
                return false;
            }

            UInt128 parentWork = BlockIndexStore.GetChainwork(prevHash);
            if (parentWork == 0 && candidateHeight > 0)
            {
                reason = "missing parent chainwork";
                return false;
            }

            UInt128 delta = ChainworkUtil.IncrementFromTarget(blk.Header!.Target);
            UInt128 candidateWork = parentWork + (delta == 0 ? 1u : delta);

            UInt128 canonicalWork = 0;
            if (canonicalTipHash is { Length: 32 } && !IsZero32(canonicalTipHash))
                canonicalWork = BlockIndexStore.GetChainwork(canonicalTipHash);

            UInt128 candidateWithSlack = candidateWork > UInt128.MaxValue - SideAdmissionWorkSlack
                ? UInt128.MaxValue
                : candidateWork + SideAdmissionWorkSlack;

            if (canonicalWork != 0 && candidateWithSlack < canonicalWork)
            {
                reason = $"candidate chainwork too far behind (candidate={candidateWork}, canon={canonicalWork})";
                return false;
            }

            return true;
        }

        private void LogKnownBlockAlready(byte[] blockHash)
        {
            if (blockHash is not { Length: 32 })
            {
                _log?.Info("P2P", "Block already known.");
                return;
            }

            bool shouldLog = false;
            var now = DateTime.UtcNow;
            string key = Convert.ToHexString(blockHash).ToLowerInvariant();

            lock (_knownBlockLogGate)
            {
                if (!_knownBlockLogByHash.TryGetValue(key, out var lastLogUtc) ||
                    (now - lastLogUtc) >= KnownBlockLogCooldown)
                {
                    _knownBlockLogByHash[key] = now;
                    shouldLog = true;

                    if (_knownBlockLogByHash.Count > MaxKnownBlockLogEntries)
                    {
                        var staleKeys = new List<string>();
                        foreach (var kv in _knownBlockLogByHash)
                        {
                            if ((now - kv.Value) > TimeSpan.FromMinutes(5))
                                staleKeys.Add(kv.Key);
                        }

                        for (int i = 0; i < staleKeys.Count; i++)
                            _knownBlockLogByHash.Remove(staleKeys[i]);
                    }
                }
            }

            if (shouldLog)
                _log?.Info("P2P", $"Block already known: {Hex16(blockHash)}");
        }

        private bool ShouldRelayInv(byte[] blockHash)
        {
            if (blockHash is not { Length: 32 })
                return false;

            string key = Convert.ToHexString(blockHash).ToLowerInvariant();
            DateTime now = DateTime.UtcNow;

            lock (_invRelayGate)
            {
                if (_invRelayByHash.TryGetValue(key, out var lastRelayUtc) &&
                    (now - lastRelayUtc) < InvRelayCooldown)
                {
                    return false;
                }

                _invRelayByHash[key] = now;

                if (_invRelayByHash.Count > MaxInvRelayEntries)
                {
                    var staleKeys = new List<string>();
                    foreach (var kv in _invRelayByHash)
                    {
                        if ((now - kv.Value) > InvRelayCooldown)
                            staleKeys.Add(kv.Key);
                    }

                    for (int i = 0; i < staleKeys.Count; i++)
                        _invRelayByHash.Remove(staleKeys[i]);

                    while (_invRelayByHash.Count > MaxInvRelayEntries)
                    {
                        string? oldestKey = null;
                        DateTime oldestUtc = DateTime.MaxValue;

                        foreach (var kv in _invRelayByHash)
                        {
                            if (kv.Value < oldestUtc)
                            {
                                oldestUtc = kv.Value;
                                oldestKey = kv.Key;
                            }
                        }

                        if (oldestKey == null)
                            break;

                        _invRelayByHash.Remove(oldestKey);
                    }
                }

                return true;
            }
        }

        private bool TryStoreOrphan(byte[] payload, Block blk, string peerKey, out string reason)
        {
            reason = string.Empty;
            if (payload == null || payload.Length == 0)
            {
                reason = "empty payload";
                return false;
            }
            if (blk?.BlockHash is not { Length: 32 } || blk.Header?.PreviousBlockHash is not { Length: 32 })
            {
                reason = "missing block hash or prev hash";
                return false;
            }

            var now = DateTime.UtcNow;
            var orphan = new OrphanEntry
            {
                Payload = (byte[])payload.Clone(),
                BlockHash = (byte[])blk.BlockHash.Clone(),
                PrevHash = (byte[])blk.Header.PreviousBlockHash.Clone(),
                PeerKey = peerKey ?? "",
                ReceivedUtc = now
            };

            string orphanHashKey = Convert.ToHexString(orphan.BlockHash).ToLowerInvariant();
            string prevKey = Convert.ToHexString(orphan.PrevHash).ToLowerInvariant();

            lock (_orphanGate)
            {
                PruneOrphansNoLock(now);

                if (_orphansByHash.ContainsKey(orphanHashKey))
                {
                    reason = "already buffered";
                    return true;
                }

                int peerCount = _orphansByPeer.TryGetValue(orphan.PeerKey, out var n) ? n : 0;
                if (peerCount >= MaxOrphansPerPeer)
                {
                    reason = "per-peer orphan limit reached";
                    return false;
                }

                while (_orphansByHash.Count >= MaxOrphansTotal)
                {
                    if (!EvictOldestOrphanNoLock())
                    {
                        reason = "orphan pool full";
                        return false;
                    }
                }

                _orphansByHash[orphanHashKey] = orphan;
                if (!_orphansByPrev.TryGetValue(prevKey, out var list))
                {
                    list = new List<OrphanEntry>();
                    _orphansByPrev[prevKey] = list;
                }
                list.Add(orphan);
                _orphansByPeer[orphan.PeerKey] = peerCount + 1;
            }

            return true;
        }

        private async Task PromoteOrphansForParentAsync(byte[] parentHash, PeerSession s, CancellationToken ct)
        {
            if (parentHash is not { Length: 32 }) return;

            int promoted = 0;
            var queue = new Queue<byte[]>();
            queue.Enqueue((byte[])parentHash.Clone());

            while (queue.Count > 0 && promoted < MaxOrphanPromotionsPerPass && !ct.IsCancellationRequested)
            {
                var parent = queue.Dequeue();
                List<OrphanEntry> children;

                lock (_orphanGate)
                {
                    children = TakeOrphansByParentNoLock(parent);
                }

                for (int i = 0; i < children.Count; i++)
                {
                    var orphan = children[i];
                    await HandleBlockInternal(orphan.Payload, s, ct, enforceRateLimitOverride: false).ConfigureAwait(false);
                    promoted++;

                    if (orphan.BlockHash is { Length: 32 })
                        queue.Enqueue((byte[])orphan.BlockHash.Clone());

                    if (promoted >= MaxOrphanPromotionsPerPass || ct.IsCancellationRequested)
                        break;
                }
            }
        }

        private List<OrphanEntry> TakeOrphansByParentNoLock(byte[] parentHash)
        {
            var result = new List<OrphanEntry>();
            if (parentHash is not { Length: 32 }) return result;

            string prevKey = Convert.ToHexString(parentHash).ToLowerInvariant();
            if (!_orphansByPrev.TryGetValue(prevKey, out var list) || list.Count == 0)
                return result;

            _orphansByPrev.Remove(prevKey);

            for (int i = 0; i < list.Count; i++)
            {
                var orphan = list[i];
                string hashKey = Convert.ToHexString(orphan.BlockHash).ToLowerInvariant();
                _orphansByHash.Remove(hashKey);

                if (_orphansByPeer.TryGetValue(orphan.PeerKey, out var count))
                {
                    count--;
                    if (count <= 0) _orphansByPeer.Remove(orphan.PeerKey);
                    else _orphansByPeer[orphan.PeerKey] = count;
                }

                result.Add(orphan);
            }

            return result;
        }

        private void PruneOrphansNoLock(DateTime nowUtc)
        {
            if (_orphansByHash.Count == 0) return;

            var stale = new List<string>();
            foreach (var kv in _orphansByHash)
            {
                if ((nowUtc - kv.Value.ReceivedUtc) > OrphanTtl)
                    stale.Add(kv.Key);
            }

            for (int i = 0; i < stale.Count; i++)
                RemoveOrphanByHashNoLock(stale[i]);
        }

        private bool EvictOldestOrphanNoLock()
        {
            if (_orphansByHash.Count == 0) return false;

            string? oldestKey = null;
            DateTime oldest = DateTime.MaxValue;
            foreach (var kv in _orphansByHash)
            {
                if (kv.Value.ReceivedUtc < oldest)
                {
                    oldest = kv.Value.ReceivedUtc;
                    oldestKey = kv.Key;
                }
            }

            if (oldestKey == null) return false;
            return RemoveOrphanByHashNoLock(oldestKey);
        }

        private bool RemoveOrphanByHashNoLock(string hashKey)
        {
            if (!_orphansByHash.TryGetValue(hashKey, out var orphan))
                return false;

            _orphansByHash.Remove(hashKey);

            string prevKey = Convert.ToHexString(orphan.PrevHash).ToLowerInvariant();
            if (_orphansByPrev.TryGetValue(prevKey, out var list))
            {
                list.RemoveAll(o => string.Equals(Convert.ToHexString(o.BlockHash).ToLowerInvariant(), hashKey, StringComparison.Ordinal));
                if (list.Count == 0)
                    _orphansByPrev.Remove(prevKey);
            }

            if (_orphansByPeer.TryGetValue(orphan.PeerKey, out var count))
            {
                count--;
                if (count <= 0) _orphansByPeer.Remove(orphan.PeerKey);
                else _orphansByPeer[orphan.PeerKey] = count;
            }

            return true;
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
            return s.Length > 16 ? s[..16] + "..." : s;
        }

        private static bool IsPreviousBlockMissingReason(string? reason)
        {
            if (string.IsNullOrWhiteSpace(reason))
                return false;

            return reason.IndexOf("previous block missing", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private static bool IsTipMovedReason(string? reason)
        {
            if (string.IsNullOrWhiteSpace(reason))
                return false;

            return reason.IndexOf("block must extend current canonical tip", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private static string GetBanKey(PeerSession s)
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

        private bool IsPeerSessionPresent(string ipOrHost, int port)
        {
            if (string.IsNullOrWhiteSpace(ipOrHost))
                return false;
            if (port <= 0 || port > 65535)
                return false;

            string wantedIp = NormalizeBanKey(ipOrHost);

            foreach (var kv in _sessions)
            {
                var s = kv.Value;
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

        private static bool IsPublicRoutableIPv4Literal(string ip)
        {
            if (string.IsNullOrWhiteSpace(ip)) return false;
            if (!IPAddress.TryParse(ip, out var a)) return false;
            if (a.AddressFamily != AddressFamily.InterNetwork) return false;

            var b = a.GetAddressBytes();
            if (b.Length != 4) return false;

            if (b[0] == 0) return false;
            if (b[0] == 10) return false;
            if (b[0] == 100 && b[1] >= 64 && b[1] <= 127) return false;
            if (b[0] == 127) return false;
            if (b[0] == 169 && b[1] == 254) return false;
            if (b[0] == 172 && b[1] >= 16 && b[1] <= 31) return false;
            if (b[0] == 192 && b[1] == 168) return false;
            if (b[0] == 198 && (b[1] == 18 || b[1] == 19)) return false;
            if (b[0] >= 224) return false;

            return true;
        }

        private static bool IsExpectedSessionTermination(Exception ex)
        {
            for (Exception? cur = ex; cur != null; cur = cur.InnerException)
            {
                if (cur is OperationCanceledException)
                    return true;

                if (cur is SocketException se)
                {
                    if (se.SocketErrorCode == SocketError.ConnectionReset ||
                        se.SocketErrorCode == SocketError.ConnectionAborted ||
                        se.SocketErrorCode == SocketError.Shutdown ||
                        se.SocketErrorCode == SocketError.TimedOut)
                    {
                        return true;
                    }
                }

                string msg = cur.Message ?? string.Empty;
                if (msg.IndexOf("closed by remote host", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    msg.IndexOf("forcibly closed", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    msg.IndexOf("vom remotehost geschlossen", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    msg.IndexOf("remote host closed", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    return true;
                }
            }

            return false;
        }

        private static string NormalizeBanKey(string raw)
            => (raw ?? string.Empty).Trim().ToLowerInvariant();
    }
}


