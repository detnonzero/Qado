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
        private const byte CurrentHandshakeVersion = 2;
        private static readonly int MaxFramePayloadBytes =
            Math.Max(
                Math.Max(ConsensusRules.MaxBlockSizeBytes, BlockSyncProtocol.MaxFramePayloadBytes),
                SmallNetSyncProtocol.MaxAncestorPackPayloadBytes);
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
        private static readonly TimeSpan MissingHeaderResyncCooldown = TimeSpan.FromSeconds(12);
        private static readonly TimeSpan BlockRelayCooldown = TimeSpan.FromMinutes(2);
        private const int MaxBlockRelayEntries = 8192;
        private static readonly LruSet ParentRequestSeen = new(capacity: 200_000, ttl: TimeSpan.FromMinutes(5));
        private static readonly TimeSpan PeerExchangeInterval = TimeSpan.FromMinutes(15);
        private static readonly TimeSpan InventoryRefreshInterval = TimeSpan.FromSeconds(20);
        private static readonly TimeSpan LatencyProbeInterval = TimeSpan.FromSeconds(60);
        private static readonly TimeSpan LatencyProbeTimeout = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan ConnectedPeerSeenRefreshInterval = TimeSpan.FromHours(1);

        public static P2PNode? Instance { get; private set; }
        public bool IsPubliclyReachable => _reachability.IsPublic;
        public bool IsInitialBlockSyncActive => _blockSyncClient.IsActive;

        private readonly MempoolManager _mempool;
        private readonly ILogSink? _log;

        private TcpListener? _listener;
        private int _listenPort = DefaultPort;
        private int _stopped;
        private int _reconnectLoopStarted;
        private int _peerExchangeLoopStarted;
        private int _inventoryRefreshLoopStarted;
        private int _latencyProbeLoopStarted;
        private DateTime _nextConnectedPeerSeenRefreshUtc = DateTime.MinValue;

        private readonly byte[] _nodeId;
        private readonly ReachabilityState _reachability = new();
        private readonly ConcurrentDictionary<string, PeerSession> _sessions = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, RateBucket> _blockRateByPeer = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, RateBucket> _txRateByPeer = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, byte> _publicPeerEndpoints = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, byte> _nonPublicPeerEndpoints = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, byte> _publicClaimsByPex = new(StringComparer.Ordinal);
        private readonly ConcurrentQueue<byte[]> _pendingRecoveryRevalidate = new();
        private readonly object _orphanGate = new();
        private readonly Dictionary<string, OrphanEntry> _orphansByHash = new(StringComparer.Ordinal);
        private readonly Dictionary<string, List<OrphanEntry>> _orphansByPrev = new(StringComparer.Ordinal);
        private readonly Dictionary<string, int> _orphansByPeer = new(StringComparer.Ordinal);
        private readonly object _knownBlockLogGate = new();
        private readonly Dictionary<string, DateTime> _knownBlockLogByHash = new(StringComparer.Ordinal);
        private readonly object _missingHeaderResyncGate = new();
        private DateTime _nextMissingHeaderResyncAllowedUtc = DateTime.MinValue;
        private readonly object _blockRelayGate = new();
        private readonly Dictionary<string, DateTime> _blockRelayByHash = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, byte> _dialInFlight = new(StringComparer.Ordinal);
        private readonly SemaphoreSlim _validationSerializeGate = new(1, 1);
        private readonly BlockDownloadManager _blockDownloadManager;
        private readonly ValidationWorker _validationWorker;
        private readonly BulkSyncRuntime _bulkSyncRuntime;
        private readonly BlockSyncClient _blockSyncClient;
        private readonly SmallNetPeerFlow _smallNetPeerFlow;
        private readonly BlockIngressFlow _blockIngressFlow;

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
            public string SessionKey = "";
            public BlockIngressKind Ingress = BlockIngressKind.LivePush;
            public DateTime ReceivedUtc = DateTime.UtcNow;
        }

        private enum ConnectOutcome
        {
            Skipped = 0,
            Succeeded = 1,
            Failed = 2
        }

        public P2PNode(MempoolManager mempool, ILogSink? log = null)
        {
            _mempool = mempool ?? throw new ArgumentNullException(nameof(mempool));
            _log = log;
            _nodeId = GetOrCreateNodeId();
            var smallNetPeers = new SmallNetPeerDirectory();
            var smallNetCoordinator = new SmallNetCoordinator();
            _validationWorker = new ValidationWorker(ProcessValidationWorkItemAsync, _log);
            _bulkSyncRuntime = new BulkSyncRuntime(
                validationSerializeGate: _validationSerializeGate,
                mempool: _mempool,
                notifyUiAfterCanonicalChange: NotifyUiAfterCanonicalChange,
                log: _log);
            _blockSyncClient = new BlockSyncClient(
                sessionSnapshot: GetSessionSnapshot,
                sendFrameAsync: SendFrameViaSessionAsync,
                getLocalTipChainwork: () =>
                {
                    lock (Db.Sync)
                    {
                        if (BlockIndexStore.TryGetBestValidatedTip(out _, out _, out var bestValidatedWork))
                            return bestValidatedWork;

                        var tipHash = BlockStore.GetCanonicalHashAtHeight(BlockStore.GetLatestHeight()) ?? new byte[32];
                        return tipHash is { Length: 32 } && !IsZero32(tipHash)
                            ? BlockIndexStore.GetChainwork(tipHash)
                            : 0;
                    }
                },
                getLocalTipHash: () =>
                {
                    lock (Db.Sync)
                    {
                        if (BlockIndexStore.TryGetBestValidatedTip(out var bestValidatedHash, out _, out _))
                            return (byte[])bestValidatedHash.Clone();
                        return BlockStore.GetCanonicalHashAtHeight(BlockStore.GetLatestHeight()) ?? new byte[32];
                    }
                },
                prepareBatchAsync: _bulkSyncRuntime.PrepareBatchAsync,
                commitChunkAsync: _bulkSyncRuntime.CommitChunkAsync,
                completeBatchAsync: _bulkSyncRuntime.CompleteBatchAsync,
                abortBatchAsync: _bulkSyncRuntime.AbortBatchAsync,
                penalizePeer: (peer, reason) =>
                {
                    PeerFailTracker.ReportFailure(GetBanKey(peer));
                    _log?.Warn("Sync", $"Block sync peer penalized {EndpointLogFormatter.FormatEndpoint(peer.RemoteEndpoint)}: {reason}");
                },
                log: _log);
            _blockDownloadManager = new BlockDownloadManager(
                sessionSnapshot: GetSessionSnapshot,
                sendFrameAsync: SendFrameViaSessionAsync,
                haveBlock: HaveBlockLocally,
                enqueueValidator: item => _validationWorker.Enqueue(item),
                revalidateStoredBlock: RevalidateStoredBlock,
                log: _log);
            _smallNetPeerFlow = new SmallNetPeerFlow(
                nodeId: _nodeId,
                peers: smallNetPeers,
                coordinator: smallNetCoordinator,
                blockSyncClient: _blockSyncClient,
                getLocalChainView: GetLocalChainView,
                getCanonicalHashAtHeight: GetCanonicalHashAtHeightSnapshot,
                getCanonicalPayloadAtHeight: GetCanonicalPayloadAtHeightSnapshot,
                sendFrameAsync: SendFrameViaSessionAsync,
                dropSession: DropSessionNoThrow,
                log: _log);
            _blockIngressFlow = new BlockIngressFlow(
                blockDownloadManager: _blockDownloadManager,
                blockSyncClient: _blockSyncClient,
                mempool: _mempool,
                getPeerKey: GetBanKey,
                allowInboundBlock: AllowInboundBlock,
                shouldRequestMissingHeaderResync: ShouldRequestMissingHeaderResync,
                requestSyncNow: RequestSyncNow,
                tryStoreOrphan: TryStoreOrphan,
                requestParentFromPeer: RequestParentFromPeerNoThrow,
                markStoredBlockInvalid: MarkStoredBlockInvalidNoThrow,
                passesSidechainAdmission: PassesSidechainAdmission,
                logKnownBlockAlready: LogKnownBlockAlready,
                notifyUiAfterAcceptedBlock: NotifyUiAfterCanonicalChange,
                relayValidatedBlockAsync: RelayValidatedBlockAsync,
                promoteOrphansForParentAsync: PromoteOrphansForParentAsync,
                log: _log);
            Instance = this;
        }

        public void Start(int port, CancellationToken ct)
        {
            Volatile.Write(ref _stopped, 0);
            _listenPort = port;
            _reachability.DecayIfExpired(PublicReachabilityTimeout);

            _listener = new TcpListener(IPAddress.Any, port); // IPv4 only
            _listener.Server.ExclusiveAddressUse = true;
            _listener.Start();

            // Validation is serialized by _validationSerializeGate, so one dispatcher
            // preserves FIFO for sync/recovery payloads and avoids synthetic orphan cascades.
            _validationWorker.Start(1, ct);
            _blockDownloadManager.Start(ct);

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
            Interlocked.Exchange(ref _inventoryRefreshLoopStarted, 0);
            Interlocked.Exchange(ref _latencyProbeLoopStarted, 0);

            foreach (var kv in _sessions)
            {
                var s = kv.Value;
                try { s.Stream.Close(); } catch { }
                try { s.Client.Close(); } catch { }
                _sessions.TryRemove(kv.Key, out _);
            }

            try { _blockDownloadManager.Dispose(); } catch { }
            try { _validationWorker.Dispose(); } catch { }
            try { _blockSyncClient.Dispose(); } catch { }

            if (ReferenceEquals(Instance, this))
                Instance = null;
        }

        public async Task ConnectAsync(string host, int port = DefaultPort, CancellationToken ct = default)
            => _ = await TryConnectAsync(host, port, ct).ConfigureAwait(false);

        private async Task<ConnectOutcome> TryConnectAsync(string host, int port = DefaultPort, CancellationToken ct = default)
        {
            if (Volatile.Read(ref _stopped) != 0) return ConnectOutcome.Skipped;

            if (SelfPeerGuard.IsSelf(host, port))
            {
                _log?.Info("P2P", $"skipping self peer {EndpointLogFormatter.FormatHostPort(host, port)}");
                return ConnectOutcome.Skipped;
            }

            string banKey = NormalizeBanKey(host);
            if (PeerFailTracker.ShouldEnforceCooldown(banKey))
            {
                _log?.Warn("P2P", $"skipping banned peer {EndpointLogFormatter.FormatHostPort(host, port)}");
                return ConnectOutcome.Skipped;
            }

            string dialKey = $"{NormalizeBanKey(host)}:{port}";
            if (!_dialInFlight.TryAdd(dialKey, 0))
                return ConnectOutcome.Skipped;

            try
            {
                if (IsPeerSessionPresent(host, port))
                    return ConnectOutcome.Skipped;

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

                await SendFrameAsync(sess, MsgType.Handshake, BuildHandshakePayload(), ct).ConfigureAwait(false);

                PeerFailTracker.ReportSuccess(banKey);
                _log?.Info("P2P", $"dialed {EndpointLogFormatter.FormatHostPort(host, port)} (IPv4)");
                _ = HandleClient(sess, ct);
                return ConnectOutcome.Succeeded;
            }
            catch (Exception ex)
            {
                _log?.Warn("P2P", $"connect {EndpointLogFormatter.FormatHostPort(host, port)} failed: {ex.Message}");
                return ConnectOutcome.Failed;
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

            var candidates = new List<(string ip, int port, ulong lastSeen, bool isSeed)>();
            var seen = new HashSet<string>(StringComparer.Ordinal);

            void AddCandidate(string ip, int port, ulong lastSeen, bool isSeed)
            {
                if (string.IsNullOrWhiteSpace(ip)) return;
                int p = port > 0 ? port : DefaultPort;
                string key = $"{NormalizeBanKey(ip)}:{p}";
                if (seen.Add(key))
                    candidates.Add((ip, p, lastSeen, isSeed));
            }

            AddCandidate(GenesisConfig.GenesisHost, seedPort, 0UL, isSeed: true);
            foreach (var (ip, port, lastSeen) in GetPeersFromDb())
                AddCandidate(ip, port, lastSeen, isSeed: false);

            int attempts = 0;
            int maxAttempts = Math.Max(8, maxKnownPeers);

            for (int i = 0; i < candidates.Count; i++)
            {
                if (ct.IsCancellationRequested) return;
                if (CountOutboundSessions(requireHandshake: false) >= targetOutbound)
                    return;
                if (attempts >= maxAttempts)
                    return;

                var (ip, p, lastSeen, isSeed) = candidates[i];
                if (SelfPeerGuard.IsSelf(ip, p)) continue;
                if (IsPeerConnected(ip, p)) continue;
                if (PeerFailTracker.ShouldEnforceCooldown(ip)) continue;
                PeerDialClass dialClass = PeerDialClass.RecentlySeen;
                if (!isSeed &&
                    !PeerDialPolicy.ShouldAttempt(ip, p, lastSeen, out dialClass, out _))
                    continue;

                var outcome = await TryConnectAsync(ip, p, ct).ConfigureAwait(false);
                if (!isSeed && outcome == ConnectOutcome.Failed)
                    PeerDialPolicy.ReportFailure(ip, p, dialClass);

                if (outcome != ConnectOutcome.Skipped)
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

            foreach (var (ip, port, lastSeen) in GetPeersFromDb())
            {
                if (ct.IsCancellationRequested) return;
                if (CountOutboundSessions(requireHandshake: false) >= targetOutbound)
                    return;

                int p = port <= 0 ? DefaultPort : port;

                if (SelfPeerGuard.IsSelf(ip, p)) continue;
                if (IsPeerConnected(ip, p)) continue;
                if (PeerFailTracker.ShouldEnforceCooldown(ip)) continue;
                if (!PeerDialPolicy.ShouldAttempt(ip, p, lastSeen, out var dialClass, out _))
                    continue;

                var outcome = await TryConnectAsync(ip, p, ct).ConfigureAwait(false);
                if (outcome == ConnectOutcome.Failed)
                    PeerDialPolicy.ReportFailure(ip, p, dialClass);
                if (outcome == ConnectOutcome.Skipped)
                    continue;

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

                        try { await Task.Delay(PeerExchangeInterval, ct).ConfigureAwait(false); } catch { }
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref _peerExchangeLoopStarted, 0);
                }
            }, ct);
        }

        public void StartInventoryRefreshLoop(CancellationToken ct)
        {
            if (Interlocked.Exchange(ref _inventoryRefreshLoopStarted, 1) != 0)
                return;

            _ = Task.Run(async () =>
            {
                try
                {
                    while (!ct.IsCancellationRequested && Volatile.Read(ref _stopped) == 0)
                    {
                        try
                        {
                            var payload = _smallNetPeerFlow.BuildLocalTipStatePayload();
                            await BroadcastAsync(MsgType.TipState, payload, ct: ct).ConfigureAwait(false);
                        }
                        catch { }

                        try { await Task.Delay(InventoryRefreshInterval, ct).ConfigureAwait(false); } catch { }
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref _inventoryRefreshLoopStarted, 0);
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

                    RefreshConnectedPeerLastSeenNoThrow();

                    var baseDelay = HasAnyHandshakePeerConnected()
                        ? ReconnectSteadyDelay
                        : ReconnectWhenDisconnectedDelay;

                    var delay = ApplyReconnectJitter(baseDelay);
                    try { await Task.Delay(delay, ct).ConfigureAwait(false); } catch { }
                }
            }, ct);
        }

        public void StartLatencyProbeLoop(CancellationToken ct)
        {
            if (Interlocked.Exchange(ref _latencyProbeLoopStarted, 1) != 0)
                return;

            _ = Task.Run(async () =>
            {
                try
                {
                    while (!ct.IsCancellationRequested && Volatile.Read(ref _stopped) == 0)
                    {
                        await ProbeConnectedPeersLatencyAsync(ct).ConfigureAwait(false);
                        try { await Task.Delay(LatencyProbeInterval, ct).ConfigureAwait(false); } catch { }
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref _latencyProbeLoopStarted, 0);
                }
            }, ct);
        }

        private async Task ProbeConnectedPeersLatencyAsync(CancellationToken ct)
        {
            foreach (var kv in _sessions)
            {
                if (ct.IsCancellationRequested)
                    return;

                var s = kv.Value;
                if (s == null || !s.HandshakeOk || !s.Client.Connected)
                    continue;

                await SendLatencyProbeAsync(s, ct).ConfigureAwait(false);
            }
        }

        private void RefreshConnectedPeerLastSeenNoThrow()
        {
            try
            {
                var nowUtc = DateTime.UtcNow;
                if (nowUtc < _nextConnectedPeerSeenRefreshUtc)
                    return;

                var endpoints = new List<(string ip, int port)>();
                var seen = new HashSet<string>(StringComparer.Ordinal);

                foreach (var kv in _sessions)
                {
                    var s = kv.Value;
                    if (s == null || !s.HandshakeOk || !s.Client.Connected)
                        continue;
                    if (!s.RemoteIsPublic)
                        continue;
                    if (string.IsNullOrWhiteSpace(s.RemoteIpAdvertised))
                        continue;
                    if (s.RemotePortAdvertised is not int p || p <= 0 || p > 65535)
                        continue;
                    if (!IsPublicRoutableIPv4Literal(s.RemoteIpAdvertised!))
                        continue;
                    if (SelfPeerGuard.IsSelf(s.RemoteIpAdvertised!, p))
                        continue;

                    string key = $"{NormalizeBanKey(s.RemoteIpAdvertised!)}:{p}";
                    if (!seen.Add(key))
                        continue;

                    endpoints.Add((s.RemoteIpAdvertised!, p));
                }

                _nextConnectedPeerSeenRefreshUtc = nowUtc + ConnectedPeerSeenRefreshInterval;
                if (endpoints.Count == 0)
                    return;

                ulong nowUnix = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                PeerStore.MarkSeenBatch(endpoints, nowUnix, GenesisConfig.NetworkId);
            }
            catch
            {
                _nextConnectedPeerSeenRefreshUtc = DateTime.UtcNow + ConnectedPeerSeenRefreshInterval;
            }
        }

        private async Task SendLatencyProbeAsync(PeerSession s, CancellationToken ct)
        {
            if (s == null || !s.HandshakeOk || !s.Client.Connected)
                return;

            long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            long pendingMs = s.LastPingSentUnixMs;
            long timeoutMs = (long)LatencyProbeTimeout.TotalMilliseconds;

            if (pendingMs > 0)
            {
                long inFlightMs = nowMs - pendingMs;
                if (inFlightMs >= 0 && inFlightMs < timeoutMs)
                    return;

                s.LastPingSentUnixMs = 0;
                s.LastLatencyMs = -1;
                s.PingTimeoutStreak++;
            }

            var payload = new byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(payload, nowMs);
            s.LastPingSentUnixMs = nowMs;

            try
            {
                await SendFrameAsync(s, MsgType.Ping, payload, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                DropSessionNoThrow(s, $"latency probe send failed: {ex.Message}");
            }
        }

        private void DropSessionNoThrow(PeerSession s, string reason)
        {
            if (s == null)
                return;

            try { s.Client.Close(); } catch { }
            _sessions.TryRemove(s.RemoteEndpoint, out _);
            try { _blockDownloadManager.OnPeerDisconnected(s); } catch { }
            _ = _blockSyncClient.OnPeerDisconnectedAsync(s, CancellationToken.None);
            _log?.Warn("P2P", $"dropping session {EndpointLogFormatter.FormatEndpoint(s.RemoteEndpoint)}: {reason}");
        }

        public bool IsPeerConnected(string ip, int port)
        {
            if (string.IsNullOrWhiteSpace(ip)) return false;
            if (port <= 0 || port > 65535) return false;

            string wantedIp = NormalizeBanKey(ip);

            foreach (var kv in _sessions)
            {
                var s = kv.Value;
                if (!s.HandshakeOk || !s.Client.Connected) continue;

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
                int sent = await BroadcastAsync(MsgType.Tx, payload).ConfigureAwait(false);
                _log?.Info("Gossip", $"Tx gossiped to {sent} peer(s).");
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

                int blockSent = await BroadcastBlockPayloadAsync(block, exceptEndpoint: null, ct: default).ConfigureAwait(false);
                var tipStatePayload = _smallNetPeerFlow.BuildLocalTipStatePayload();
                int tipStateSent = await BroadcastAsync(MsgType.TipState, tipStatePayload, ct: default).ConfigureAwait(false);
                _log?.Info("Gossip", $"Block pushed h={block.BlockHeight} {Hex16(block.BlockHash!)} to {blockSent} peer(s), tipstate to {tipStateSent} peer(s) (handshakeTargets={targets}).");
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

                    await SendFrameAsync(sess, MsgType.Handshake, BuildHandshakePayload(), ct).ConfigureAwait(false);

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
                _blockDownloadManager.OnPeerDisconnected(s);
                _sessions.TryRemove(s.RemoteEndpoint, out _);
                _ = _blockSyncClient.OnPeerDisconnectedAsync(s, CancellationToken.None);
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

                case MsgType.Hello:
                    await _smallNetPeerFlow.HandleHelloAsync(payload, s, ct).ConfigureAwait(false);
                    return;

                case MsgType.TipState:
                    await _smallNetPeerFlow.HandleTipStateAsync(payload, s, ct).ConfigureAwait(false);
                    return;

                case MsgType.Tx:
                    await HandleTx(payload, s, ct).ConfigureAwait(false);
                    return;

                case MsgType.Block:
                    if (!_blockDownloadManager.EnqueueBlockPayload(s, payload, BlockIngressKind.LivePush))
                        _log?.Warn("Sync", "Validation queue full. Dropping inbound live block payload.");
                    return;

                case MsgType.GetAncestorPack:
                    await HandleGetAncestorPackAsync(payload, s, ct).ConfigureAwait(false);
                    return;

                case MsgType.AncestorPack:
                    await HandleAncestorPackAsync(payload, s, ct).ConfigureAwait(false);
                    return;

                case MsgType.GetBlock:
                    await HandleGetBlock(payload, s, ct).ConfigureAwait(false);
                    return;

                case MsgType.GetPeers:
                    var peersPayload = PeerDiscovery.BuildPeersPayload(maxPeers: 64);
                    await SendFrameAsync(s, MsgType.Peers, peersPayload, ct).ConfigureAwait(false);
                    return;

                case MsgType.Peers:
                    PeerDiscovery.HandlePeersPayload(payload, _log);
                    return;

                case MsgType.Ping:
                    await SendFrameAsync(s, MsgType.Pong, payload ?? Array.Empty<byte>(), ct).ConfigureAwait(false);
                    return;

                case MsgType.Pong:
                    UpdatePeerLatencyFromPong(s, payload);
                    return;

                case MsgType.GetBlocksByLocator:
                    await BlockSyncServer.HandleGetBlocksByLocatorAsync(payload, s, SendFrameViaSessionAsync, _log, ct).ConfigureAwait(false);
                    return;

                case MsgType.GetBlocksFrom:
                    await BlockSyncServer.HandleGetBlocksFromAsync(payload, s, SendFrameViaSessionAsync, _log, ct).ConfigureAwait(false);
                    return;

                case MsgType.BlocksBatchStart:
                    await _smallNetPeerFlow.HandleBlocksBatchStartAsync(payload, s, ct).ConfigureAwait(false);
                    return;

                case MsgType.BlocksChunk:
                    await _blockSyncClient.OnBlocksChunkAsync(s, payload, ct).ConfigureAwait(false);
                    return;

                case MsgType.BlocksBatchEnd:
                    await _smallNetPeerFlow.HandleBlocksBatchEndAsync(payload, s, ct).ConfigureAwait(false);
                    return;

                case MsgType.NoCommonAncestor:
                    await _blockSyncClient.OnNoCommonAncestorAsync(s, ct).ConfigureAwait(false);
                    return;

                default:
                    return;
            }
        }

        private void UpdatePeerLatencyFromPong(PeerSession s, byte[] payload)
        {
            if (s == null)
                return;

            long pendingMs = s.LastPingSentUnixMs;
            if (pendingMs <= 0)
                return;

            long sentMs = pendingMs;
            if (payload is { Length: >= 8 })
            {
                long fromPayload = BinaryPrimitives.ReadInt64LittleEndian(payload.AsSpan(0, 8));
                if (fromPayload > 0)
                    sentMs = fromPayload;
            }

            long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            long rtt = nowMs - sentMs;
            if (rtt < 0 || rtt > (long)TimeSpan.FromMinutes(5).TotalMilliseconds)
                return;

            s.LastLatencyMs = rtt > int.MaxValue ? int.MaxValue : (int)rtt;
            s.LastLatencyUpdatedUtc = DateTime.UtcNow;
            s.LastPingSentUnixMs = 0;
            s.PingTimeoutStreak = 0;
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
            if (ver != CurrentHandshakeVersion)
            {
                PeerFailTracker.ReportFailure(banKey);
                _log?.Warn(
                    "P2P",
                    $"Rejected peer {EndpointLogFormatter.FormatEndpoint(s.RemoteEndpoint)} on unsupported handshake version {ver} (expected {CurrentHandshakeVersion}).");
                try { s.Client.Close(); } catch { }
                _sessions.TryRemove(s.RemoteEndpoint, out _);
                return;
            }

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

            ushort portRaw = BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(34, 2));
            bool claimsListening = portRaw != 0;
            int port = claimsListening ? portRaw : DefaultPort;

            var peerId = payload.AsSpan(2, 32); // peerId is untrusted metadata; use only for self-loop detection.
            if (peerId.SequenceEqual(_nodeId))
            {
                var endpointIp = (s.Client.Client.RemoteEndPoint as IPEndPoint)?.Address?.ToString()
                                 ?? s.RemoteEndpoint;
                SelfPeerGuard.RememberSelf(endpointIp, port);

                _log?.Warn("P2P", $"Rejected self-loop handshake from {EndpointLogFormatter.FormatHost(endpointIp)}.");
                try { s.Client.Close(); } catch { }
                _sessions.TryRemove(s.RemoteEndpoint, out _);
                return;
            }

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
                if (IsPeerMarkedNonPublic(ip, port))
                {
                    _log?.Info(
                        "P2P",
                        $"inbound public claim ignored (probe previously failed): remoteEndpoint={EndpointLogFormatter.FormatEndpoint(s.RemoteEndpoint)}, detectedIp={EndpointLogFormatter.FormatHost(ip)}, advertisedPort={port}");
                }
                else
                {
                    // Inbound session is only a claim; verify exactly once via immediate outbound probe.
                    _log?.Info(
                        "P2P",
                        $"inbound public claim registered: remoteEndpoint={EndpointLogFormatter.FormatEndpoint(s.RemoteEndpoint)}, detectedIp={EndpointLogFormatter.FormatHost(ip)}, advertisedPort={port}");
                    NotePeerPublicClaim(ip, port);
                    _ = TryProbePublicClaimImmediatelyAsync(ip, port);
                }
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
            if (IsPublicRoutableIPv4Literal(ip) && port > 0 && port <= 65535)
                PeerDialPolicy.ReportSuccess(ip, port);
            _blockDownloadManager.OnPeerReady(s);
            await _blockSyncClient.OnPeerReadyAsync(s, ct).ConfigureAwait(false);
            FlushRecoveryRevalidationQueue();

            if (firstHandshakeForSession)
            {
                _log?.Info("P2P",
                    $"handshake from {EndpointLogFormatter.FormatHostPort(ip, port)} (v{ver}, inbound={s.IsInbound}, public={s.RemoteIsPublic}, claimsListening={claimsListening}, advertisedPort={port})");

                try { await SendFrameAsync(s, MsgType.GetPeers, Array.Empty<byte>(), ct).ConfigureAwait(false); } catch { }
                try { await _smallNetPeerFlow.SendLocalHelloAsync(s, ct).ConfigureAwait(false); } catch { }
                try { await _smallNetPeerFlow.SendLocalTipStateAsync(s, ct).ConfigureAwait(false); } catch { }
                try { await SendLatencyProbeAsync(s, ct).ConfigureAwait(false); } catch { }
            }
        }

        private SmallNetLocalChainView GetLocalChainView()
        {
            ulong height;
            byte[] tipHash;
            UInt128 tipChainwork;
            IReadOnlyList<byte[]> recentCanonical;

            lock (Db.Sync)
            {
                if (BlockIndexStore.TryGetBestValidatedTip(out var bestValidatedHash, out var bestValidatedHeight, out var bestValidatedWork))
                {
                    height = bestValidatedHeight;
                    tipHash = (byte[])bestValidatedHash.Clone();
                    tipChainwork = bestValidatedWork;
                }
                else
                {
                    height = BlockStore.GetLatestHeight();
                    tipHash = BlockStore.GetCanonicalHashAtHeight(height) ?? new byte[32];
                    tipChainwork = tipHash is { Length: 32 } && !IsZero32(tipHash)
                        ? BlockIndexStore.GetChainwork(tipHash)
                        : 0;
                }

                recentCanonical = BuildRecentCanonicalWindowNoLock(height, SmallNetSyncProtocol.MaxRecentCanonicalHashes);
            }

            return new SmallNetLocalChainView(height, tipHash, tipChainwork, recentCanonical, StateDigest: null);
        }

        private byte[]? GetCanonicalHashAtHeightSnapshot(ulong height)
        {
            lock (Db.Sync)
            {
                var hash = BlockStore.GetCanonicalHashAtHeight(height);
                return hash is { Length: 32 } ? (byte[])hash.Clone() : null;
            }
        }

        private byte[]? GetCanonicalPayloadAtHeightSnapshot(ulong height)
        {
            lock (Db.Sync)
            {
                return BlockStore.TryGetSerializedCanonicalBlockAtHeight(height, out var payload)
                    ? payload
                    : null;
            }
        }

        private List<byte[]> BuildRecentCanonicalWindowNoLock(ulong tipHeight, int maxHashes)
        {
            maxHashes = Math.Max(1, Math.Min(maxHashes, SmallNetSyncProtocol.MaxRecentCanonicalHashes));
            var hashes = new List<byte[]>(maxHashes);

            ulong current = tipHeight;
            for (int i = 0; i < maxHashes; i++)
            {
                var hash = BlockStore.GetCanonicalHashAtHeight(current);
                if (hash is { Length: 32 })
                    hashes.Add((byte[])hash.Clone());
                else
                    break;

                if (current == 0)
                    break;
                current--;
            }

            return hashes;
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

        private async Task HandleGetBlock(byte[] payload, PeerSession s, CancellationToken ct)
        {
            if (payload.Length != 8) return;

            ulong h = BinaryPrimitives.ReadUInt64LittleEndian(payload);

            if (!BlockStore.TryGetSerializedCanonicalBlockAtHeight(h, out var payloadBlob))
                return;

            await SendFrameAsync(s, MsgType.BlockAt, payloadBlob, ct).ConfigureAwait(false);
        }

        private async Task HandleGetAncestorPackAsync(byte[] payload, PeerSession s, CancellationToken ct)
        {
            if (!SmallNetSyncProtocol.TryParseGetAncestorPack(payload, out var frame))
                return;

            if (!TryBuildAncestorPack(frame.StartHash, frame.MaxBlocks, out var blocks) || blocks.Count == 0)
                return;

            var responsePayload = SmallNetSyncProtocol.BuildAncestorPack(blocks);
            await SendFrameAsync(s, MsgType.AncestorPack, responsePayload, ct).ConfigureAwait(false);
        }

        private async Task HandleAncestorPackAsync(byte[] payload, PeerSession s, CancellationToken ct)
        {
            if (!SmallNetSyncProtocol.TryParseAncestorPack(payload, out var frame))
                return;

            for (int i = 0; i < frame.Blocks.Count; i++)
            {
                if (ct.IsCancellationRequested)
                    return;

                var blockPayload = frame.Blocks[i];
                if (blockPayload == null || blockPayload.Length == 0)
                    continue;

                if (!_blockDownloadManager.EnqueueBlockPayload(s, blockPayload, BlockIngressKind.Recovery, enforceRateLimitOverride: false))
                {
                    _log?.Warn("Sync", "Validation queue full. Dropping ancestor-pack block payload.");
                    return;
                }
            }
        }

        private bool TryBuildAncestorPack(byte[] startHash, int maxBlocks, out List<byte[]> blocks)
        {
            blocks = new List<byte[]>();
            if (startHash is not { Length: 32 })
                return false;

            int remaining = Math.Max(1, Math.Min(maxBlocks, SmallNetSyncProtocol.MaxAncestorPackBlocks));
            byte[] current = (byte[])startHash.Clone();

            while (remaining > 0)
            {
                if (!BlockStore.TryGetSerializedBlockByHash(current, out var payloadBlob))
                    break;

                blocks.Add(payloadBlob);
                remaining--;

                if (!BlockIndexStore.TryGetMeta(current, out _, out var prevHash, out _))
                    break;
                if (prevHash is not { Length: 32 } || IsZero32(prevHash))
                    break;

                current = prevHash;
            }

            blocks.Reverse();
            return blocks.Count > 0;
        }

        private void NotifyUiAfterCanonicalChange()
        {
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
        }

        private void MarkStoredBlockInvalidNoThrow(byte[] blockHash, BadChainReason reason, string context)
        {
            if (blockHash is not { Length: 32 })
                return;

            try
            {
                int affected;
                if (reason == BadChainReason.BlockStatelessInvalid)
                {
                    affected = BlockIndexStore.MarkBadAndDescendants(blockHash, (int)reason);
                    if (affected > 0)
                        _log?.Warn("Sync", $"Marked bad block with known descendants: root={Hex16(blockHash)} affected={affected} reason={(int)reason} ({context})");
                }
                else
                {
                    // Keep state-invalid marking local until we can prove the failure is not a local/runtime artifact.
                    affected = BlockIndexStore.MarkBadSelf(blockHash, (int)reason);
                    if (affected > 0)
                        _log?.Warn("Sync", $"Marked bad block (self-only): root={Hex16(blockHash)} reason={(int)reason} ({context})");
                }

                _blockSyncClient.RequestResync(context);
            }
            catch
            {
            }
        }

        private void RequestParentFromPeerNoThrow(PeerSession peer, byte[] parentHash, string source)
        {
            if (peer == null || parentHash is not { Length: 32 })
                return;

            try
            {
                string key = $"{NormalizeBanKey(peer.SessionKey)}:{Convert.ToHexString(parentHash).ToLowerInvariant()}";
                if (!ParentRequestSeen.TryAdd(key))
                    return;

                var payload = SmallNetSyncProtocol.BuildGetAncestorPack(parentHash, SmallNetSyncProtocol.MaxAncestorPackBlocks);
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await SendFrameAsync(peer, MsgType.GetAncestorPack, payload, CancellationToken.None).ConfigureAwait(false);
                        _log?.Info("Sync", $"{source}: requested parent pack {Hex16(parentHash)} from {EndpointLogFormatter.FormatEndpoint(peer.RemoteEndpoint)}");
                    }
                    catch (Exception ex)
                    {
                        _log?.Warn("Sync", $"{source}: parent-pack request failed for {Hex16(parentHash)} via {EndpointLogFormatter.FormatEndpoint(peer.RemoteEndpoint)}: {ex.Message}");
                    }
                }, CancellationToken.None);
            }
            catch
            {
            }
        }

        private async Task<int> BroadcastAsync(MsgType t, byte[] payload, string? exceptEndpoint = null, CancellationToken ct = default)
        {
            if (Volatile.Read(ref _stopped) != 0) return 0;

            int sent = 0;

            foreach (var kv in _sessions)
            {
                if (exceptEndpoint != null && string.Equals(kv.Key, exceptEndpoint, StringComparison.Ordinal))
                    continue;

                var sess = kv.Value;
                if (!sess.HandshakeOk && t != MsgType.Handshake)
                    continue;
                try
                {
                    await SendFrameAsync(sess, t, payload, ct).ConfigureAwait(false);
                    sent++;
                }
                catch (Exception ex)
                {
                    try { sess.Client.Close(); } catch { }
                    try { _blockDownloadManager.OnPeerDisconnected(sess); } catch { }
                    _sessions.TryRemove(kv.Key, out _);
                    _ = _blockSyncClient.OnPeerDisconnectedAsync(sess, CancellationToken.None);
                    _log?.Warn("Gossip", $"send {t} failed for {EndpointLogFormatter.FormatEndpoint(kv.Key)}: {ex.Message}");
                }
            }

            return sent;
        }

        private async Task<int> BroadcastBlockPayloadAsync(Block block, string? exceptEndpoint, CancellationToken ct)
        {
            if (block == null)
                return 0;

            if (block.BlockHash is not { Length: 32 } || IsZero32(block.BlockHash))
                block.BlockHash = block.ComputeBlockHash();

            int size = BlockBinarySerializer.GetSize(block);
            var blockPayload = new byte[size];
            _ = BlockBinarySerializer.Write(blockPayload, block);
            return await BroadcastAsync(MsgType.Block, blockPayload, exceptEndpoint: exceptEndpoint, ct: ct).ConfigureAwait(false);
        }

        private async Task RelayValidatedBlockAsync(Block block, PeerSession sourcePeer, CancellationToken ct)
        {
            if (block?.BlockHash is not { Length: 32 } || !ShouldRelayBlock(block.BlockHash))
                return;

            await BroadcastBlockPayloadAsync(block, exceptEndpoint: sourcePeer.RemoteEndpoint, ct: ct).ConfigureAwait(false);
            await BroadcastAsync(
                MsgType.TipState,
                _smallNetPeerFlow.BuildLocalTipStatePayload(),
                exceptEndpoint: sourcePeer.RemoteEndpoint,
                ct: ct).ConfigureAwait(false);
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
            buf[0] = CurrentHandshakeVersion;
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

        private static System.Collections.Generic.List<(string ip, int port, ulong lastSeen)> GetPeersFromDb()
        {
            var list = new System.Collections.Generic.List<(string, int port, ulong lastSeen)>();

            lock (Db.Sync)
            {
                using var cmd = Db.Connection!.CreateCommand();
                cmd.CommandText = "SELECT ip, port, last_seen FROM peers ORDER BY last_seen DESC LIMIT 64;";
                using var r = cmd.ExecuteReader();
                while (r.Read())
                {
                    string ip = r.GetString(0);
                    int port = r.GetInt32(1);
                    long seenRaw = r.GetInt64(2);
                    int p = port > 0 ? port : DefaultPort;
                    ulong lastSeen = seenRaw > 0 ? (ulong)seenRaw : 0UL;

                    if (!string.IsNullOrWhiteSpace(ip))
                    {
                        if (SelfPeerGuard.IsSelf(ip, p)) continue;
                        list.Add((ip, p, lastSeen));
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

        private async Task SendFrameAsync(PeerSession s, MsgType t, byte[] payload, CancellationToken ct)
        {
            await s.SendLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await WriteFrame(s.Stream, t, payload, ct).ConfigureAwait(false);
            }
            finally
            {
                s.SendLock.Release();
            }
        }

        private Task SendFrameViaSessionAsync(PeerSession s, MsgType t, byte[] payload, CancellationToken ct)
        {
            return SendFrameAsync(s, t, payload, ct);
        }

        private static bool HaveBlockLocally(byte[] hash)
            => hash is { Length: 32 } && BlockIndexStore.HasPayload(hash);

        public void RequestSyncNow(string reason = "manual")
        {
            if (Volatile.Read(ref _stopped) != 0)
                return;

            _blockDownloadManager.ResetTransientSyncState(reason);
            _blockSyncClient.RequestResync(reason);

            _ = Task.Run(async () =>
            {
                try
                {
                    await ConnectSeedAndKnownPeersAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _log?.Warn("Sync", $"Try Sync redial failed ({reason}): {ex.Message}");
                }
            }, CancellationToken.None);
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
                    _blockDownloadManager.EnqueueBlockPayload(peer, payload, BlockIngressKind.Recovery, enforceRateLimitOverride: false);
                    return;
                }

                if (block?.Header != null)
                {
                    _pendingRecoveryRevalidate.Enqueue((byte[])hash.Clone());
                    return;
                }

                _log?.Warn("Sync", $"Stored payload missing/corrupt for {Hex16(hash)}; scheduling re-download.");
                _blockSyncClient.RequestResync("revalidate-missing-payload");
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
                    _blockDownloadManager.EnqueueBlockPayload(peer, payload, BlockIngressKind.Recovery, enforceRateLimitOverride: false);
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

        public bool? GetPeerPublicStatus(string ip, int port)
        {
            if (string.IsNullOrWhiteSpace(ip)) return null;
            if (port <= 0 || port > 65535) return null;

            if (IsPeerMarkedNonPublic(ip, port))
                return false;
            if (IsPeerMarkedPublic(ip, port))
                return true;

            string wantedIp = NormalizeBanKey(ip);

            foreach (var kv in _sessions)
            {
                var s = kv.Value;
                if (!s.HandshakeOk || !s.Client.Connected) continue;

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
                    return s.RemoteIsPublic;
            }

            return null;
        }

        public int? GetPeerLatencyMs(string ip, int port)
        {
            if (string.IsNullOrWhiteSpace(ip)) return null;
            if (port <= 0 || port > 65535) return null;

            string wantedIp = NormalizeBanKey(ip);

            foreach (var kv in _sessions)
            {
                var s = kv.Value;
                if (s == null || !s.HandshakeOk || !s.Client.Connected)
                    continue;

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

                if (sessionIp != wantedIp || sessionPort != port)
                    continue;

                return s.LastLatencyMs >= 0 ? s.LastLatencyMs : null;
            }

            return null;
        }

        private async Task ProcessValidationWorkItemAsync(ValidationWorkItem item, CancellationToken ct)
        {
            await _validationSerializeGate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await _blockIngressFlow.HandleBlockAsync(item.Payload, item.Peer, ct, item.Ingress, item.EnforceRateLimitOverride).ConfigureAwait(false);
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
                    MarkPeerAsNonPublic(ip, port);
                    _publicClaimsByPex.TryRemove(key, out _);
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
                MarkPeerAsNonPublic(ip, port);
                _publicClaimsByPex.TryRemove(key, out _);
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
            if (IsPeerMarkedNonPublic(ip, port)) return;
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
                if (!s.HandshakeOk || !s.Client.Connected || !s.RemoteIsPublic) continue;
                if (string.IsNullOrWhiteSpace(s.RemoteIpAdvertised)) continue;
                if (s.RemotePortAdvertised is not int p || p <= 0 || p > 65535) continue;
                if (!IsPublicRoutableIPv4Literal(s.RemoteIpAdvertised!)) continue;
                if (SelfPeerGuard.IsSelf(s.RemoteIpAdvertised!, p)) continue;
                if (IsPeerMarkedNonPublic(s.RemoteIpAdvertised!, p)) continue;

                if (!TryBuildPeerEndpointKey(s.RemoteIpAdvertised!, p, out var key)) continue;
                if (!seen.Add(key)) continue;
                list.Add((s.RemoteIpAdvertised!, p));
            }

            return list;
        }

        public List<(string ip, int port)> GetPeerCandidatesForPex(int maxPeers, int unverifiedPercent = 20)
        {
            if (maxPeers <= 0)
                return new List<(string ip, int port)>();
            _ = unverifiedPercent;
            return GetPublicPeerCandidates(maxPeers);
        }

        private void MarkPeerAsPublic(string ip, int port)
        {
            if (!IsPublicRoutableIPv4Literal(ip)) return;
            if (port <= 0 || port > 65535) return;
            if (!TryBuildPeerEndpointKey(ip, port, out var key)) return;

            _publicPeerEndpoints[key] = 0;
            _nonPublicPeerEndpoints.TryRemove(key, out _);

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

        private void MarkPeerAsNonPublic(string ip, int port)
        {
            if (!IsPublicRoutableIPv4Literal(ip)) return;
            if (port <= 0 || port > 65535) return;
            if (!TryBuildPeerEndpointKey(ip, port, out var key)) return;

            _publicPeerEndpoints.TryRemove(key, out _);
            _publicClaimsByPex.TryRemove(key, out _);
            _nonPublicPeerEndpoints[key] = 0;

            foreach (var kv in _sessions)
            {
                var s = kv.Value;
                if (string.IsNullOrWhiteSpace(s.RemoteIpAdvertised)) continue;
                if (s.RemotePortAdvertised is not int p || p <= 0 || p > 65535) continue;
                if (!TryBuildPeerEndpointKey(s.RemoteIpAdvertised!, p, out var skey)) continue;
                if (string.Equals(skey, key, StringComparison.Ordinal))
                    s.RemoteIsPublic = false;
            }
        }

        private bool IsPeerMarkedPublic(string ip, int port)
        {
            if (!TryBuildPeerEndpointKey(ip, port, out var key))
                return false;
            if (_nonPublicPeerEndpoints.ContainsKey(key))
                return false;
            return _publicPeerEndpoints.ContainsKey(key);
        }

        private bool IsPeerMarkedNonPublic(string ip, int port)
        {
            if (!TryBuildPeerEndpointKey(ip, port, out var key))
                return false;
            return _nonPublicPeerEndpoints.ContainsKey(key);
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
            if (parentWork == 0)
            {
                reason = "missing parent chainwork";
                return false;
            }

            UInt128 delta = ChainworkUtil.IncrementFromTarget(blk.Header!.Target);
            UInt128 candidateWork = ChainworkUtil.Add(parentWork, delta);

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

        private bool ShouldRelayBlock(byte[] blockHash)
        {
            if (blockHash is not { Length: 32 })
                return false;

            string key = Convert.ToHexString(blockHash).ToLowerInvariant();
            DateTime now = DateTime.UtcNow;

            lock (_blockRelayGate)
            {
                if (_blockRelayByHash.TryGetValue(key, out var lastRelayUtc) &&
                    (now - lastRelayUtc) < BlockRelayCooldown)
                {
                    return false;
                }

                _blockRelayByHash[key] = now;

                if (_blockRelayByHash.Count > MaxBlockRelayEntries)
                {
                    var staleKeys = new List<string>();
                    foreach (var kv in _blockRelayByHash)
                    {
                        if ((now - kv.Value) > BlockRelayCooldown)
                            staleKeys.Add(kv.Key);
                    }

                    for (int i = 0; i < staleKeys.Count; i++)
                        _blockRelayByHash.Remove(staleKeys[i]);

                    while (_blockRelayByHash.Count > MaxBlockRelayEntries)
                    {
                        string? oldestKey = null;
                        DateTime oldestUtc = DateTime.MaxValue;

                        foreach (var kv in _blockRelayByHash)
                        {
                            if (kv.Value < oldestUtc)
                            {
                                oldestUtc = kv.Value;
                                oldestKey = kv.Key;
                            }
                        }

                        if (oldestKey == null)
                            break;

                        _blockRelayByHash.Remove(oldestKey);
                    }
                }

                return true;
            }
        }

        private bool ShouldRequestMissingHeaderResync()
        {
            var now = DateTime.UtcNow;
            lock (_missingHeaderResyncGate)
            {
                if (now < _nextMissingHeaderResyncAllowedUtc)
                    return false;

                _nextMissingHeaderResyncAllowedUtc = now + MissingHeaderResyncCooldown;
                return true;
            }
        }

        private bool TryStoreOrphan(
            byte[] payload,
            Block blk,
            PeerSession peer,
            string peerKey,
            BlockIngressKind ingress,
            out string reason)
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
                SessionKey = peer?.SessionKey ?? string.Empty,
                Ingress = ingress,
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
                    var replayPeer = ResolveOrphanReplayPeer(orphan, s);
                    await _blockIngressFlow.HandleBlockAsync(
                        orphan.Payload,
                        replayPeer,
                        ct,
                        orphan.Ingress,
                        enforceRateLimitOverride: false).ConfigureAwait(false);
                    promoted++;

                    if (orphan.BlockHash is { Length: 32 })
                        queue.Enqueue((byte[])orphan.BlockHash.Clone());

                    if (promoted >= MaxOrphanPromotionsPerPass || ct.IsCancellationRequested)
                        break;
                }
            }
        }

        private PeerSession ResolveOrphanReplayPeer(OrphanEntry orphan, PeerSession fallbackPeer)
        {
            if (!string.IsNullOrWhiteSpace(orphan.SessionKey) &&
                _sessions.TryGetValue(orphan.SessionKey, out var replayPeer))
            {
                return replayPeer;
            }

            return fallbackPeer;
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


