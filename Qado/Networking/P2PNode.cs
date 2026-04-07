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
        private static readonly HandshakeCapabilities DefaultHandshakeCapabilities =
            HandshakeCapabilities.DirectTransactions |
            HandshakeCapabilities.DirectBlocks |
            HandshakeCapabilities.BlocksBatchData |
            HandshakeCapabilities.PeerExchangeIpv6 |
            HandshakeCapabilities.DualStack |
            HandshakeCapabilities.LegacyChunkSync |
            HandshakeCapabilities.ExtendedSyncWindow |
            HandshakeCapabilities.SyncWindowPreview;
        private const int HandshakeBaseLength = 1 + 1 + 32 + 2;
        private const int HandshakeCapabilitiesLength = 4;
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
        private const int MaxOrphansPerPeer = 64;
        private static readonly TimeSpan OrphanTtl = TimeSpan.FromMinutes(10);
        private const int MaxOrphanPromotionsPerPass = 256;
        private const int OrphanPeerCooldownTriggerCount = 3;
        private const int MaxOrphanPeerCooldownEntries = 4096;
        private static readonly TimeSpan OrphanPeerStrikeWindow = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan OrphanPeerInitialCooldown = TimeSpan.FromSeconds(15);
        private static readonly TimeSpan OrphanPeerMaxCooldown = TimeSpan.FromSeconds(60);
        private static readonly TimeSpan KnownBlockLogCooldown = TimeSpan.FromSeconds(60);
        private static readonly TimeSpan KnownBlockCatchupLogWindow = TimeSpan.FromSeconds(15);
        private const int MaxKnownBlockLogEntries = 4096;
        private static readonly TimeSpan ValidationQueueFullLogWindow = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan ParentPackBenignFailureLogWindow = TimeSpan.FromSeconds(15);
        private static readonly TimeSpan ParentPackRequestLogWindow = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan ParentPackBurstCooldown = TimeSpan.FromSeconds(8);
        private static readonly TimeSpan ParentPackPeerRetryCooldown = TimeSpan.FromSeconds(20);
        private static readonly TimeSpan DuplicateSessionLogWindow = TimeSpan.FromSeconds(20);
        private static readonly TimeSpan ParentPackRequestBudgetWindow = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan MissingHeaderResyncCooldown = TimeSpan.FromSeconds(12);
        private static readonly TimeSpan BlockRelayCooldown = TimeSpan.FromMinutes(2);
        private const int MaxBlockRelayEntries = 8192;
        private static readonly TimeSpan TxRelayCooldown = TimeSpan.FromMinutes(10);
        private const int MaxTxRelayEntries = 200_000;
        private static readonly TimeSpan PeerRelayDedupeTtl = TimeSpan.FromMinutes(10);
        private const int MaxPeerRelayEntries = 8192;
        private const int MaxRelayQueuePerPeer = 256;
        private static readonly TimeSpan RelayQueueOverflowLogWindow = TimeSpan.FromSeconds(10);
        private const int MaxParentPackRequestsPerBudgetWindowGlobal = 4;
        private const int MaxParentPackRequestsPerBudgetWindowPerPeer = 2;
        private const int MaxParentPackPeersPerRecovery = 3;
        private static readonly LruSet ParentRequestBurstSeen = new(capacity: 200_000, ttl: ParentPackBurstCooldown);
        private static readonly LruSet ParentRequestSeenByPeer = new(capacity: 400_000, ttl: ParentPackPeerRetryCooldown);
        private static readonly LruSet RecentRelayedTxSeen = new(capacity: MaxTxRelayEntries, ttl: TxRelayCooldown);
        private static readonly TimeSpan PeerExchangeInterval = TimeSpan.FromMinutes(15);
        private static readonly TimeSpan InventoryRefreshInterval = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan ImmediateTipStateBroadcastCooldown = TimeSpan.FromSeconds(2);
        private static readonly TimeSpan LatencyProbeInterval = TimeSpan.FromSeconds(60);
        private static readonly TimeSpan LatencyProbeTimeout = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan ConnectedPeerSeenRefreshInterval = TimeSpan.FromHours(1);

        public static P2PNode? Instance { get; private set; }
        public bool IsPubliclyReachable => _reachability.IsPublic;
        public bool IsInitialBlockSyncActive => _blockSyncClient.IsActive;
        public bool HasBetterPeerTipThanLocal => _blockSyncClient.HasBetterPeerTipThanLocal;
        public int GetConnectedHandshakePeerCount() => CountBroadcastTargets();

        private readonly MempoolManager _mempool;
        private readonly ILogSink? _log;

        private TcpListener? _listenerV4;
        private TcpListener? _listenerV6;
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
        private readonly object _peerOrphanCooldownGate = new();
        private readonly Dictionary<string, PeerOrphanCooldownState> _peerOrphanCooldowns = new(StringComparer.Ordinal);
        private readonly object _knownBlockLogGate = new();
        private readonly Dictionary<string, DateTime> _knownBlockLogByHash = new(StringComparer.Ordinal);
        private DateTime _nextKnownBlockCatchupLogUtc = DateTime.MinValue;
        private int _knownBlockCatchupSuppressed;
        private readonly object _validationQueueLogGate = new();
        private DateTime _lastLiveQueueFullLogUtc = DateTime.MinValue;
        private int _liveQueueFullSuppressed;
        private DateTime _lastAncestorPackQueueFullLogUtc = DateTime.MinValue;
        private int _ancestorPackQueueFullSuppressed;
        private readonly object _parentPackFailureLogGate = new();
        private readonly object _parentPackRequestBudgetGate = new();
        private readonly object _parentPackRequestLogGate = new();
        private readonly object _duplicateSessionLogGate = new();
        private DateTime _lastBenignParentPackFailureLogUtc = DateTime.MinValue;
        private int _benignParentPackFailureSuppressed;
        private string _lastBenignParentPackFailureMessage = string.Empty;
        private DateTime _parentPackRequestWindowStartUtc = DateTime.MinValue;
        private int _parentPackRequestCountInWindow;
        private readonly Dictionary<string, int> _parentPackRequestCountByPeer = new(StringComparer.Ordinal);
        private DateTime _lastParentPackRequestLogUtc = DateTime.MinValue;
        private int _parentPackRequestSuppressed;
        private string _lastParentPackRequestMessage = string.Empty;
        private DateTime _lastDuplicateSessionLogUtc = DateTime.MinValue;
        private int _duplicateSessionLogSuppressed;
        private string _lastDuplicateSessionLogKey = string.Empty;
        private string _lastDuplicateSessionMessage = string.Empty;
        private readonly object _missingHeaderResyncGate = new();
        private DateTime _nextMissingHeaderResyncAllowedUtc = DateTime.MinValue;
        private readonly object _blockRelayGate = new();
        private readonly Dictionary<string, DateTime> _blockRelayByHash = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, LruSet> _peerSentBlocks = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, LruSet> _peerSentTxs = new(StringComparer.Ordinal);
        private readonly object _tipStateBroadcastGate = new();
        private DateTime _nextImmediateTipStateBroadcastAllowedUtc = DateTime.MinValue;
        private int _immediateTipStateBroadcastInFlight;
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

        private sealed class PeerOrphanCooldownState
        {
            public DateTime WindowStartUtc = DateTime.UtcNow;
            public int StrikeCount;
            public DateTime CooldownUntilUtc = DateTime.MinValue;
            public TimeSpan CooldownDuration = TimeSpan.Zero;
            public DateTime LastTouchedUtc = DateTime.UtcNow;
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
                getLocalTipHeight: () =>
                {
                    lock (Db.Sync)
                    {
                        if (BlockIndexStore.TryGetBestValidatedTip(out _, out var bestValidatedHeight, out _))
                            return bestValidatedHeight;
                        return BlockStore.GetLatestHeight();
                    }
                },
                prepareBatchAsync: _bulkSyncRuntime.PrepareBatchAsync,
                commitBlocksAsync: _bulkSyncRuntime.CommitBlocksAsync,
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
                shouldPushBlockToPeer: ShouldPushBlockToPeer,
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
            bool listeningV4;
            bool listeningV6 = false;
            _listenerV4 = TryStartListener(IPAddress.Any, port, ipv6Only: false, out listeningV4);
            if (NetworkParams.EnableIpv6 && Socket.OSSupportsIPv6)
                _listenerV6 = TryStartListener(IPAddress.IPv6Any, port, ipv6Only: true, out listeningV6);

            // Validation is serialized by _validationSerializeGate, so one dispatcher
            // preserves FIFO for sync/recovery payloads and avoids synthetic orphan cascades.
            _validationWorker.Start(1, ct);
            _blockDownloadManager.Start(ct);

            if (listeningV4 && _listenerV4 != null)
                _ = AcceptLoop(_listenerV4, "ipv4", ct);

            if (_listenerV6 != null)
                _ = AcceptLoop(_listenerV6, "ipv6", ct);

            if (!listeningV4 && _listenerV6 == null)
                throw new InvalidOperationException("No P2P listener could be started for IPv4 or IPv6.");

            if (listeningV4 && _listenerV6 != null)
                _log?.Info("P2P", $"listening on 0.0.0.0:{port} and [::]:{port}");
            else if (listeningV4)
                _log?.Info("P2P", $"listening on 0.0.0.0:{port}");
            else
                _log?.Info("P2P", $"listening on [::]:{port}");
        }

        public void Stop()
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
                return;

            try { _listenerV4?.Stop(); } catch { }
            try { _listenerV6?.Stop(); } catch { }
            _listenerV4 = null;
            _listenerV6 = null;
            Interlocked.Exchange(ref _reconnectLoopStarted, 0);
            Interlocked.Exchange(ref _peerExchangeLoopStarted, 0);
            Interlocked.Exchange(ref _inventoryRefreshLoopStarted, 0);
            Interlocked.Exchange(ref _latencyProbeLoopStarted, 0);

            foreach (var kv in _sessions)
            {
                var s = kv.Value;
                TeardownSessionNoThrow(s);
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

                var dialAddresses = await ResolveDialAddressesAsync(host, ct).ConfigureAwait(false);
                Exception? lastError = null;

                for (int i = 0; i < dialAddresses.Count; i++)
                {
                    var address = dialAddresses[i];
                    TcpClient? client = null;
                    try
                    {
                        client = new TcpClient(address.AddressFamily);
                        client.NoDelay = true;
                        client.ReceiveTimeout = 15000;
                        client.SendTimeout = 15000;

#if NET8_0_OR_GREATER
                        await client.ConnectAsync(address, port, ct).ConfigureAwait(false);
#else
                        await client.ConnectAsync(address.ToString(), port).ConfigureAwait(false);
#endif

                        var ns = client.GetStream();
                        string remoteIp = PeerAddress.NormalizeHost(address.ToString());
                        var sess = new PeerSession
                        {
                            Client = client,
                            Stream = ns,
                            RemoteEndpoint = client.Client.RemoteEndPoint?.ToString() ?? EndpointLogFormatter.FormatHostPort(remoteIp, port),
                            RemoteBanKey = NormalizeBanKey(remoteIp),
                            IsInbound = false
                        };

                        _sessions[sess.RemoteEndpoint] = sess;

                        await SendFrameAsync(sess, MsgType.Handshake, BuildHandshakePayload(), ct).ConfigureAwait(false);

                        PeerFailTracker.ReportSuccess(sess.RemoteBanKey);
                        _log?.Info("P2P", $"dialed {EndpointLogFormatter.FormatHostPort(remoteIp, port)} ({address.AddressFamily})");
                        _ = HandleClient(sess, ct);
                        return ConnectOutcome.Succeeded;
                    }
                    catch (Exception ex)
                    {
                        lastError = ex;
                        try { client?.Close(); } catch { }
                    }
                }

                _log?.Warn("P2P", $"connect {EndpointLogFormatter.FormatHostPort(host, port)} failed: {lastError?.Message ?? "no address candidates"}");
                return ConnectOutcome.Failed;
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
            var configuredSeeds = GenesisConfig.BootstrapHosts ?? Array.Empty<string>();
            for (int i = 0; i < configuredSeeds.Length; i++)
                TryRememberSeedEndpointNoThrow(configuredSeeds[i], seedPort);

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

            for (int i = 0; i < configuredSeeds.Length; i++)
                AddCandidate(configuredSeeds[i], seedPort, 0UL, isSeed: true);
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

                    try
                    {
                        await ProbePublicClaimsAsync(ct).ConfigureAwait(false);
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
                    if (!PeerAddress.IsPublicRoutable(s.RemoteIpAdvertised!))
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

            TeardownSessionNoThrow(s);

            if (reason.StartsWith("duplicate peer session resolved", StringComparison.Ordinal))
            {
                LogDuplicateSessionDrop(reason);
                return;
            }

            _log?.Warn("P2P", $"dropping session {EndpointLogFormatter.FormatEndpoint(s.RemoteEndpoint)}: {reason}");
        }

        private void TeardownSessionNoThrow(PeerSession session)
        {
            if (session == null)
                return;

            try
            {
                if (Interlocked.Exchange(ref session.RelayStopRequested, 1) == 0)
                    session.RelaySignal.Release();
            }
            catch { }

            try { session.Client.Close(); } catch { }
            _sessions.TryRemove(session.RemoteEndpoint, out _);
            _peerSentBlocks.TryRemove(session.SessionKey, out _);
            _peerSentTxs.TryRemove(session.SessionKey, out _);
            try { _blockDownloadManager.OnPeerDisconnected(session); } catch { }
            _ = _blockSyncClient.OnPeerDisconnectedAsync(session, CancellationToken.None);
        }

        private bool ResolveDuplicatePeerSessionAfterHandshake(PeerSession session, string ip, int port)
        {
            if (session == null || session.RemoteNodeId is not { Length: 32 })
                return true;

            var duplicates = new List<PeerSession>();
            TryGetAdvertisedPeerEndpointKey(session, out var sessionEndpointKey);
            foreach (var kv in _sessions)
            {
                var other = kv.Value;
                if (other == null || ReferenceEquals(other, session))
                    continue;

                bool sameRemoteNode =
                    other.RemoteNodeId is { Length: 32 } &&
                    BytesEqual32(other.RemoteNodeId, session.RemoteNodeId) &&
                    string.Equals(other.RemoteBanKey, session.RemoteBanKey, StringComparison.Ordinal) &&
                    (!HasConflictingAdvertisedPort(other, session));

                bool sameAdvertisedEndpoint =
                    sessionEndpointKey != null &&
                    TryGetAdvertisedPeerEndpointKey(other, out var otherEndpointKey) &&
                    string.Equals(otherEndpointKey, sessionEndpointKey, StringComparison.Ordinal);

                if (!sameRemoteNode && !sameAdvertisedEndpoint)
                    continue;

                duplicates.Add(other);
            }

            if (duplicates.Count == 0)
                return true;

            duplicates.Add(session);
            var preferred = SelectPreferredDuplicateSession(duplicates, session.RemoteNodeId);
            if (preferred == null)
                return true;

            foreach (var candidate in duplicates)
            {
                if (ReferenceEquals(candidate, preferred))
                    continue;

                string loserDirection = candidate.IsInbound ? "inbound" : "outbound";
                string winnerDirection = preferred.IsInbound ? "inbound" : "outbound";
                string loserLabel = loserDirection == winnerDirection
                    ? $"duplicate {loserDirection}"
                    : loserDirection;
                string winnerLabel = loserDirection == winnerDirection
                    ? $"existing {winnerDirection}"
                    : winnerDirection;
                DropSessionNoThrow(
                    candidate,
                    $"duplicate peer session resolved for {EndpointLogFormatter.FormatHostPort(ip, port)}: keeping {winnerLabel}, dropping {loserLabel}");
            }

            return ReferenceEquals(preferred, session);
        }

        private void ScheduleDuplicatePeerSessionRecheck(PeerSession session, string ip, int port)
        {
            if (session == null)
                return;

            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(250).ConfigureAwait(false);
                    if (Volatile.Read(ref _stopped) != 0)
                        return;

                    if (!_sessions.TryGetValue(session.RemoteEndpoint, out var current) || !ReferenceEquals(current, session))
                        return;

                    ResolveDuplicatePeerSessionAfterHandshake(session, ip, port);
                }
                catch
                {
                }
            }, CancellationToken.None);
        }

        private PeerSession? SelectPreferredDuplicateSession(IReadOnlyList<PeerSession> candidates, byte[] remoteNodeId)
        {
            if (candidates == null || candidates.Count == 0 || remoteNodeId is not { Length: 32 })
                return null;

            bool? preferOutbound = null;
            int tie = CompareNodeIds(_nodeId, remoteNodeId);
            if (tie < 0)
                preferOutbound = true;
            else if (tie > 0)
                preferOutbound = false;

            PeerSession? best = null;
            foreach (var candidate in candidates)
            {
                if (candidate == null)
                    continue;

                if (best == null)
                {
                    best = candidate;
                    continue;
                }

                if (preferOutbound.HasValue)
                {
                    bool candidatePreferredDir = candidate.IsInbound != preferOutbound.Value;
                    bool bestPreferredDir = best.IsInbound != preferOutbound.Value;
                    if (candidatePreferredDir != bestPreferredDir)
                    {
                        if (candidatePreferredDir)
                            best = candidate;
                        continue;
                    }
                }

                if (candidate.ConnectedUtc < best.ConnectedUtc)
                {
                    best = candidate;
                    continue;
                }

                if (candidate.ConnectedUtc == best.ConnectedUtc &&
                    string.CompareOrdinal(candidate.RemoteEndpoint, best.RemoteEndpoint) < 0)
                {
                    best = candidate;
                }
            }

            return best;
        }

        private void LogDuplicateSessionDrop(string reason)
        {
            var message = $"duplicate session drop: {reason}";
            var key = GetDuplicateSessionLogKey(reason);
            lock (_duplicateSessionLogGate)
            {
                var now = DateTime.UtcNow;
                if (string.Equals(_lastDuplicateSessionLogKey, key, StringComparison.Ordinal) &&
                    (now - _lastDuplicateSessionLogUtc) < DuplicateSessionLogWindow)
                {
                    _duplicateSessionLogSuppressed++;
                    return;
                }

                if (_duplicateSessionLogSuppressed > 0 && !string.IsNullOrWhiteSpace(_lastDuplicateSessionMessage))
                    _log?.Info("P2P", $"{_lastDuplicateSessionMessage} (+{_duplicateSessionLogSuppressed} similar drops suppressed)");

                _lastDuplicateSessionLogKey = key;
                _lastDuplicateSessionMessage = message;
                _lastDuplicateSessionLogUtc = now;
                _duplicateSessionLogSuppressed = 0;
                _log?.Warn("P2P", message);
            }
        }

        private static string GetDuplicateSessionLogKey(string reason)
        {
            if (string.IsNullOrWhiteSpace(reason))
                return string.Empty;

            int keepingIndex = reason.IndexOf(": keeping ", StringComparison.Ordinal);
            return keepingIndex >= 0 ? reason[keepingIndex..] : reason;
        }

        private PeerSession? SelectPreferredPeerEndpointSession(IReadOnlyList<PeerSession> candidates)
        {
            if (candidates == null || candidates.Count == 0)
                return null;

            if (candidates.Count == 1)
                return candidates[0];

            byte[]? remoteNodeId = null;
            bool consistentRemoteNode = true;

            foreach (var candidate in candidates)
            {
                if (candidate?.RemoteNodeId is not { Length: 32 } candidateNodeId)
                    continue;

                if (remoteNodeId == null)
                {
                    remoteNodeId = candidateNodeId;
                    continue;
                }

                if (!BytesEqual32(remoteNodeId, candidateNodeId))
                {
                    consistentRemoteNode = false;
                    break;
                }
            }

            if (consistentRemoteNode && remoteNodeId is { Length: 32 })
            {
                var preferred = SelectPreferredDuplicateSession(candidates, remoteNodeId);
                if (preferred != null)
                    return preferred;
            }

            PeerSession? best = null;
            foreach (var candidate in candidates)
            {
                if (candidate == null)
                    continue;

                if (best == null)
                {
                    best = candidate;
                    continue;
                }

                if (candidate.ConnectedUtc < best.ConnectedUtc)
                {
                    best = candidate;
                    continue;
                }

                if (candidate.ConnectedUtc == best.ConnectedUtc &&
                    string.CompareOrdinal(candidate.RemoteEndpoint, best.RemoteEndpoint) < 0)
                {
                    best = candidate;
                }
            }

            return best;
        }

        private static bool HasConflictingAdvertisedPort(PeerSession left, PeerSession right)
        {
            if (left.RemotePortAdvertised is not int leftPort ||
                right.RemotePortAdvertised is not int rightPort)
            {
                return false;
            }

            if (leftPort <= 0 || leftPort > 65535 || rightPort <= 0 || rightPort > 65535)
                return false;

            return leftPort != rightPort;
        }

        private static bool TryGetAdvertisedPeerEndpointKey(PeerSession session, out string? key)
        {
            key = null;
            if (session == null ||
                !session.RemoteClaimsListening ||
                string.IsNullOrWhiteSpace(session.RemoteIpAdvertised) ||
                session.RemotePortAdvertised is not int port ||
                port <= 0 || port > 65535)
            {
                return false;
            }

            if (!PeerAddress.IsPublicRoutable(session.RemoteIpAdvertised))
                return false;

            return TryBuildPeerEndpointKey(session.RemoteIpAdvertised!, port, out key);
        }

        private bool TryGetPreferredConnectedPeerSession(string wantedIp, int port, out PeerSession? preferred)
        {
            preferred = null;
            if (string.IsNullOrWhiteSpace(wantedIp) || port <= 0 || port > 65535)
                return false;

            List<PeerSession>? matches = null;
            foreach (var kv in _sessions)
            {
                var s = kv.Value;
                if (!IsMatchingConnectedPeerSession(s, wantedIp, port))
                    continue;

                matches ??= new List<PeerSession>();
                matches.Add(s);
            }

            if (matches == null || matches.Count == 0)
                return false;

            preferred = SelectPreferredPeerEndpointSession(matches) ?? matches[0];
            return true;
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

        public Task BroadcastTxAsync(Transaction tx)
        {
            try
            {
                var payload = tx.ToBytes();
                int sent = QueueTxRelay(tx, payload, exceptEndpoint: null);
                _log?.Info("Gossip", $"Tx gossiped to {sent} peer(s).");
            }
            catch { }

            return Task.CompletedTask;
        }

        public Task BroadcastBlockAsync(Block block)
        {
            try
            {
                if (block.BlockHash is not { Length: 32 } || IsZero32(block.BlockHash))
                    block.BlockHash = block.ComputeBlockHash();

                int targets = CountBroadcastTargets();
                if (targets <= 0)
                {
                    _log?.Warn("Gossip", $"No handshake peers for block gossip {Hex16(block.BlockHash!)}.");
                    return Task.CompletedTask;
                }

                int blockSent = QueueBlockRelay(block, exceptEndpoint: null, includeTipState: true);
                int tipStateSent = blockSent;
                _log?.Info("Gossip", $"Block pushed h={block.BlockHeight} {Hex16(block.BlockHash!)} to {blockSent} peer(s), tipstate to {tipStateSent} peer(s) (handshakeTargets={targets}).");
            }
            catch (Exception ex)
            {
                _log?.Warn("Gossip", $"Block gossip failed: {ex.Message}");
            }

            return Task.CompletedTask;
        }


        private async Task AcceptLoop(TcpListener listener, string listenerTag, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested && Volatile.Read(ref _stopped) == 0)
            {
                TcpClient? c = null;
                try
                {
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
                    if (Volatile.Read(ref _stopped) == 0)
                        _log?.Warn("P2P", $"accept loop ({listenerTag}) recovered after socket error");
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
                TeardownSessionNoThrow(s);
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
                TeardownSessionNoThrow(s);
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
                        LogValidationQueueFull(isAncestorPack: false);
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
                    var peersPayload = PeerDiscovery.BuildPeersPayload(s, maxPeers: 64);
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

                case MsgType.BlocksBatchData:
                    await _blockSyncClient.OnBlocksBatchDataAsync(s, payload, ct).ConfigureAwait(false);
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

            if (payload.Length < HandshakeBaseLength)
            {
                PeerFailTracker.ReportFailure(banKey);
                _log?.Warn("P2P", "Rejected legacy handshake without network id.");
                TeardownSessionNoThrow(s);
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
                TeardownSessionNoThrow(s);
                return;
            }

            if (networkId != GenesisConfig.NetworkId)
            {
                PeerFailTracker.ReportFailure(banKey);
                _log?.Warn(
                    "P2P",
                    $"Rejected peer {EndpointLogFormatter.FormatEndpoint(s.RemoteEndpoint)} on foreign network id {networkId} (expected {GenesisConfig.NetworkId}, inbound={s.IsInbound}, raw={s.RemoteEndpoint}).");
                TeardownSessionNoThrow(s);
                return;
            }

            ushort portRaw = BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(34, 2));
            bool claimsListening = portRaw != 0;
            int port = claimsListening ? portRaw : DefaultPort;
            HandshakeCapabilities capabilities = ParseHandshakeCapabilities(payload);

            var peerId = payload.AsSpan(2, 32); // peerId is untrusted metadata; use only for self-loop detection.
            if (peerId.SequenceEqual(_nodeId))
            {
                var endpointIp = (s.Client.Client.RemoteEndPoint as IPEndPoint)?.Address?.ToString()
                                 ?? s.RemoteEndpoint;
                SelfPeerGuard.RememberSelf(endpointIp, port);

                _log?.Warn("P2P", $"Rejected self-loop handshake from {EndpointLogFormatter.FormatHost(endpointIp)}.");
                TeardownSessionNoThrow(s);
                return;
            }

            var ip = PeerAddress.NormalizeHost(
                (s.Client.Client.RemoteEndPoint as IPEndPoint)?.Address?.ToString()
                ?? s.RemoteEndpoint);

            s.RemoteClaimsListening = claimsListening;
            s.RemoteIpAdvertised = ip;
            s.RemotePortAdvertised = port;
            s.RemoteNodeId = peerId.ToArray();
            s.RemoteBanKey = NormalizeBanKey(ip);
            s.Capabilities = capabilities;
            s.RemoteIsPublic = IsPeerMarkedPublic(ip, port);

            if (!ResolveDuplicatePeerSessionAfterHandshake(s, ip, port))
                return;

            // Public proof for a remote peer exists when THIS node dials the peer successfully.
            // Inbound sessions (remote dialed us) are not proof of remote reachability.
            bool outboundReachabilityProof = !s.IsInbound && claimsListening && PeerAddress.IsPublicRoutable(ip);
            if (outboundReachabilityProof)
            {
                MarkPeerAsPublic(ip, port);
                s.RemoteIsPublic = true;

                if (TryBuildPeerEndpointKey(ip, port, out var endpointKey))
                    _publicClaimsByPex.TryRemove(endpointKey, out _);
            }
            else if (s.IsInbound && claimsListening && PeerAddress.IsPublicRoutable(ip))
            {
                if (IsPeerMarkedNonPublic(ip, port))
                {
                    _log?.Info(
                        "P2P",
                        $"inbound public claim ignored (probe previously failed): remoteEndpoint={EndpointLogFormatter.FormatEndpoint(s.RemoteEndpoint)}, detectedIp={EndpointLogFormatter.FormatHost(ip)}, advertisedPort={port}");
                }
                else
                {
                    // Inbound session is only a claim; queue it for the bounded periodic probe path.
                    _log?.Info(
                        "P2P",
                        $"inbound public claim registered: remoteEndpoint={EndpointLogFormatter.FormatEndpoint(s.RemoteEndpoint)}, detectedIp={EndpointLogFormatter.FormatHost(ip)}, advertisedPort={port}");
                    NotePeerPublicClaim(ip, port);
                }
            }

            if (claimsListening && PeerAddress.IsPublicRoutable(ip))
            {
                try
                {
                    PeerStore.MarkSeen(ip, port, (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(), GenesisConfig.NetworkId);
                }
                catch { }
            }

            bool firstHandshakeForSession = !s.HandshakeOk;
            s.HandshakeOk = true;
            ScheduleDuplicatePeerSessionRecheck(s, ip, port);
            PeerFailTracker.ReportSuccess(GetBanKey(s));
            if (PeerAddress.IsPublicRoutable(ip) && port > 0 && port <= 65535)
                PeerDialPolicy.ReportSuccess(ip, port);
            _blockDownloadManager.OnPeerReady(s);
            await _blockSyncClient.OnPeerReadyAsync(s, ct).ConfigureAwait(false);
            FlushRecoveryRevalidationQueue();

            if (firstHandshakeForSession)
            {
                _log?.Info("P2P",
                    $"handshake from {EndpointLogFormatter.FormatHostPort(ip, port)} (v{ver}, inbound={s.IsInbound}, public={s.RemoteIsPublic}, claimsListening={claimsListening}, advertisedPort={port}, caps={(uint)capabilities})");

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

            int sent = QueueTxRelay(txMsg, payload, exceptEndpoint: s.RemoteEndpoint);
            _log?.Info("P2P", $"TX accepted (nonce={txMsg.TxNonce}) gossiped to {sent} peer(s).");
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
                    LogValidationQueueFull(isAncestorPack: true);
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
            TriggerImmediateTipStateBroadcast();

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

        private void TriggerImmediateTipStateBroadcast()
        {
            if (Volatile.Read(ref _stopped) != 0)
                return;
            if (Volatile.Read(ref _immediateTipStateBroadcastInFlight) != 0)
                return;

            var nowUtc = DateTime.UtcNow;
            lock (_tipStateBroadcastGate)
            {
                if (nowUtc < _nextImmediateTipStateBroadcastAllowedUtc)
                    return;
                if (Volatile.Read(ref _immediateTipStateBroadcastInFlight) != 0)
                    return;

                _nextImmediateTipStateBroadcastAllowedUtc = nowUtc + ImmediateTipStateBroadcastCooldown;
            }

            if (Interlocked.Exchange(ref _immediateTipStateBroadcastInFlight, 1) != 0)
                return;

            _ = Task.Run(async () =>
            {
                try
                {
                    var payload = _smallNetPeerFlow.BuildLocalTipStatePayload();
                    await BroadcastAsync(MsgType.TipState, payload, ct: default).ConfigureAwait(false);
                }
                catch
                {
                }
                finally
                {
                    Interlocked.Exchange(ref _immediateTipStateBroadcastInFlight, 0);
                }
            });
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
                string key = Convert.ToHexString(parentHash).ToLowerInvariant();
                if (!ParentRequestBurstSeen.TryAdd(key))
                    return;

                var payload = SmallNetSyncProtocol.BuildGetAncestorPack(parentHash, SmallNetSyncProtocol.MaxAncestorPackBlocks);
                var targets = GetParentPackRequestTargets(peer, MaxParentPackPeersPerRecovery);
                for (int i = 0; i < targets.Count; i++)
                {
                    var target = targets[i];
                    string peerKey = NormalizeBanKey(target.SessionKey);
                    string peerSeenKey = $"{key}:{peerKey}";
                    if (ParentRequestSeenByPeer.Seen(peerSeenKey))
                        continue;

                    if (!TryReserveParentPackRequest(peerKey))
                        continue;

                    if (!ParentRequestSeenByPeer.TryAdd(peerSeenKey))
                        continue;

                    string targetSource = ReferenceEquals(target, peer)
                        ? source
                        : $"{source}-backup";

                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await SendFrameAsync(target, MsgType.GetAncestorPack, payload, CancellationToken.None).ConfigureAwait(false);
                            LogParentPackRequest(targetSource, parentHash, target);
                        }
                        catch (Exception ex)
                        {
                            LogParentPackRequestFailure(targetSource, parentHash, target, ex);
                        }
                    }, CancellationToken.None);
                }
            }
            catch
            {
            }
        }

        private IReadOnlyList<PeerSession> GetParentPackRequestTargets(PeerSession primaryPeer, int maxPeers)
        {
            maxPeers = Math.Max(1, maxPeers);

            var result = new List<PeerSession>(maxPeers);
            var seen = new HashSet<string>(StringComparer.Ordinal);
            TryAddParentPackTarget(primaryPeer, result, seen);
            if (result.Count >= maxPeers)
                return result;

            var candidates = new List<PeerSession>();
            foreach (var candidate in GetSessionSnapshot())
            {
                if (candidate == null || ReferenceEquals(candidate, primaryPeer))
                    continue;
                if (!IsParentPackRequestCandidate(candidate))
                    continue;

                candidates.Add(candidate);
            }

            candidates.Sort(CompareParentPackRequestCandidates);
            for (int i = 0; i < candidates.Count && result.Count < maxPeers; i++)
                TryAddParentPackTarget(candidates[i], result, seen);

            return result;
        }

        private void TryAddParentPackTarget(PeerSession? candidate, List<PeerSession> result, HashSet<string> seen)
        {
            if (!IsParentPackRequestCandidate(candidate))
                return;

            var target = candidate!;
            string sessionKey = target.SessionKey ?? string.Empty;
            string key = NormalizeBanKey(sessionKey);
            if (string.IsNullOrWhiteSpace(key))
                key = sessionKey;

            if (!seen.Add(key))
                return;

            result.Add(target);
        }

        private static bool IsParentPackRequestCandidate(PeerSession? candidate)
        {
            if (candidate == null || !candidate.HandshakeOk)
                return false;

            if (candidate.Client != null && !candidate.Client.Connected)
                return false;

            return true;
        }

        private static int CompareParentPackRequestCandidates(PeerSession? x, PeerSession? y)
        {
            int xScore = GetParentPackRequestCandidateScore(x);
            int yScore = GetParentPackRequestCandidateScore(y);
            int scoreCompare = yScore.CompareTo(xScore);
            if (scoreCompare != 0)
                return scoreCompare;

            int xLatency = x != null && x.LastLatencyMs >= 0 ? x.LastLatencyMs : int.MaxValue;
            int yLatency = y != null && y.LastLatencyMs >= 0 ? y.LastLatencyMs : int.MaxValue;
            int latencyCompare = xLatency.CompareTo(yLatency);
            if (latencyCompare != 0)
                return latencyCompare;

            return string.CompareOrdinal(x?.RemoteEndpoint, y?.RemoteEndpoint);
        }

        private static int GetParentPackRequestCandidateScore(PeerSession? candidate)
        {
            if (candidate == null)
                return int.MinValue;

            int score = 0;
            if (!candidate.IsInbound)
                score++;
            if (candidate.Supports(HandshakeCapabilities.BlocksBatchData))
                score += 4;
            if (candidate.Supports(HandshakeCapabilities.ExtendedSyncWindow))
                score += 2;
            if (candidate.Supports(HandshakeCapabilities.SyncWindowPreview))
                score += 1;

            return score;
        }

        private bool TryReserveParentPackRequest(string peerKey)
        {
            lock (_parentPackRequestBudgetGate)
            {
                var now = DateTime.UtcNow;
                if ((now - _parentPackRequestWindowStartUtc) >= ParentPackRequestBudgetWindow)
                {
                    _parentPackRequestWindowStartUtc = now;
                    _parentPackRequestCountInWindow = 0;
                    _parentPackRequestCountByPeer.Clear();
                }

                if (_parentPackRequestCountInWindow >= MaxParentPackRequestsPerBudgetWindowGlobal)
                    return false;

                if (!string.IsNullOrWhiteSpace(peerKey) &&
                    _parentPackRequestCountByPeer.TryGetValue(peerKey, out var perPeerCount) &&
                    perPeerCount >= MaxParentPackRequestsPerBudgetWindowPerPeer)
                {
                    return false;
                }

                _parentPackRequestCountInWindow++;
                if (!string.IsNullOrWhiteSpace(peerKey))
                {
                    int updated = _parentPackRequestCountByPeer.TryGetValue(peerKey, out var current)
                        ? current + 1
                        : 1;
                    _parentPackRequestCountByPeer[peerKey] = updated;
                }

                return true;
            }
        }

        private void LogParentPackRequest(string source, byte[] parentHash, PeerSession peer)
        {
            string message = $"{source}: requested parent pack {Hex16(parentHash)} from {EndpointLogFormatter.FormatEndpoint(peer.RemoteEndpoint)}";
            lock (_parentPackRequestLogGate)
            {
                var now = DateTime.UtcNow;
                _lastParentPackRequestMessage = message;
                if ((now - _lastParentPackRequestLogUtc) >= ParentPackRequestLogWindow)
                {
                    if (_parentPackRequestSuppressed > 0)
                        _log?.Info("Sync", $"{_lastParentPackRequestMessage} (+{_parentPackRequestSuppressed} similar requests suppressed)");
                    else
                        _log?.Info("Sync", _lastParentPackRequestMessage);

                    _lastParentPackRequestLogUtc = now;
                    _parentPackRequestSuppressed = 0;
                }
                else
                {
                    _parentPackRequestSuppressed++;
                }
            }
        }

        private void LogValidationQueueFull(bool isAncestorPack)
        {
            lock (_validationQueueLogGate)
            {
                var now = DateTime.UtcNow;
                ref var lastLogUtc = ref isAncestorPack ? ref _lastAncestorPackQueueFullLogUtc : ref _lastLiveQueueFullLogUtc;
                ref var suppressed = ref isAncestorPack ? ref _ancestorPackQueueFullSuppressed : ref _liveQueueFullSuppressed;
                string message = isAncestorPack
                    ? "Validation queue full. Dropping ancestor-pack block payload."
                    : "Validation queue full. Dropping inbound live block payload.";

                if ((now - lastLogUtc) >= ValidationQueueFullLogWindow)
                {
                    if (suppressed > 0)
                        _log?.Warn("Sync", $"{message} (+{suppressed} similar drops suppressed)");
                    else
                        _log?.Warn("Sync", message);

                    lastLogUtc = now;
                    suppressed = 0;
                }
                else
                {
                    suppressed++;
                }
            }
        }

        private void LogParentPackRequestFailure(string source, byte[] parentHash, PeerSession peer, Exception ex)
        {
            string endpoint = EndpointLogFormatter.FormatEndpoint(peer.RemoteEndpoint);
            string message = $"{source}: parent-pack request failed for {Hex16(parentHash)} via {endpoint}: {ex.Message}";

            if (!IsBenignParentPackFailure(ex))
            {
                _log?.Warn("Sync", message);
                return;
            }

            lock (_parentPackFailureLogGate)
            {
                var now = DateTime.UtcNow;
                _lastBenignParentPackFailureMessage = message;

                if ((now - _lastBenignParentPackFailureLogUtc) >= ParentPackBenignFailureLogWindow)
                {
                    if (_benignParentPackFailureSuppressed > 0)
                    {
                        _log?.Info(
                            "Sync",
                            $"{_lastBenignParentPackFailureMessage} (+{_benignParentPackFailureSuppressed} similar benign send failures suppressed)");
                    }
                    else
                    {
                        _log?.Info("Sync", _lastBenignParentPackFailureMessage);
                    }

                    _lastBenignParentPackFailureLogUtc = now;
                    _benignParentPackFailureSuppressed = 0;
                }
                else
                {
                    _benignParentPackFailureSuppressed++;
                }
            }
        }

        private static bool IsBenignParentPackFailure(Exception ex)
        {
            if (ex is OperationCanceledException || ex is ObjectDisposedException)
                return true;

            string message = ex.Message ?? string.Empty;
            return message.Contains("disposed object", StringComparison.OrdinalIgnoreCase) ||
                   message.Contains("task was canceled", StringComparison.OrdinalIgnoreCase) ||
                   message.Contains("operation was canceled", StringComparison.OrdinalIgnoreCase);
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
                    TeardownSessionNoThrow(sess);
                    _log?.Warn("Gossip", $"send {t} failed for {EndpointLogFormatter.FormatEndpoint(kv.Key)}: {ex.Message}");
                }
            }

            return sent;
        }

        private int QueueTxRelay(Transaction tx, byte[] payload, string? exceptEndpoint)
        {
            if (tx == null || payload == null || payload.Length == 0)
                return 0;

            byte[] txid;
            try
            {
                txid = tx.ComputeTransactionHash();
            }
            catch
            {
                return 0;
            }

            if (txid is not { Length: 32 } || !RecentRelayedTxSeen.TryAdd(txid))
                return 0;

            return QueueRelayBroadcast(MsgType.Tx, payload, txid, exceptEndpoint);
        }

        private int QueueBlockRelay(Block block, string? exceptEndpoint, bool includeTipState)
        {
            if (block == null)
                return 0;

            if (block.BlockHash is not { Length: 32 } || IsZero32(block.BlockHash))
                block.BlockHash = block.ComputeBlockHash();

            int size = BlockBinarySerializer.GetSize(block);
            var blockPayload = new byte[size];
            _ = BlockBinarySerializer.Write(blockPayload, block);

            if (!ShouldRelayBlock(block.BlockHash))
                return 0;

            int sent = QueueRelayBroadcast(MsgType.Block, blockPayload, block.BlockHash, exceptEndpoint);
            if (includeTipState && sent > 0)
            {
                _ = QueueRelayBroadcast(
                    MsgType.TipState,
                    _smallNetPeerFlow.BuildLocalTipStatePayload(),
                    itemId: null,
                    exceptEndpoint: exceptEndpoint);
            }

            return sent;
        }

        private int QueueRelayBroadcast(MsgType t, byte[] payload, byte[]? itemId, string? exceptEndpoint)
        {
            if (Volatile.Read(ref _stopped) != 0 || payload == null || payload.Length == 0)
                return 0;

            int sent = 0;
            foreach (var kv in _sessions)
            {
                if (exceptEndpoint != null && string.Equals(kv.Key, exceptEndpoint, StringComparison.Ordinal))
                    continue;

                var sess = kv.Value;
                if (sess == null || !sess.HandshakeOk || !sess.Client.Connected)
                    continue;

                if (itemId is { Length: 32 } && !ShouldRelayToPeer(sess, t, itemId))
                    continue;

                if (!TryEnqueueRelayFrame(sess, t, payload))
                    continue;

                sent++;
            }

            return sent;
        }

        private bool TryEnqueueRelayFrame(PeerSession session, MsgType type, byte[] payload)
        {
            if (session == null || payload == null || payload.Length == 0)
                return false;

            if (Volatile.Read(ref session.RelayStopRequested) != 0)
                return false;

            int queued = Interlocked.Increment(ref session.RelayQueuedCount);
            if (queued > MaxRelayQueuePerPeer)
            {
                Interlocked.Decrement(ref session.RelayQueuedCount);
                LogRelayQueueOverflow(session, type);
                return false;
            }

            session.RelayQueue.Enqueue(new QueuedRelayFrame(type, payload));
            EnsureRelayPump(session);
            try { session.RelaySignal.Release(); } catch (SemaphoreFullException) { }
            return true;
        }

        private void EnsureRelayPump(PeerSession session)
        {
            if (session == null)
                return;

            if (Interlocked.CompareExchange(ref session.RelayPumpStarted, 1, 0) != 0)
                return;

            _ = Task.Run(async () =>
            {
                try
                {
                    while (true)
                    {
                        await session.RelaySignal.WaitAsync(CancellationToken.None).ConfigureAwait(false);

                        while (session.RelayQueue.TryDequeue(out var frame))
                        {
                            Interlocked.Decrement(ref session.RelayQueuedCount);

                            if (Volatile.Read(ref _stopped) != 0 || Volatile.Read(ref session.RelayStopRequested) != 0)
                                continue;

                            try
                            {
                                await SendFrameAsync(session, frame.Type, frame.Payload, CancellationToken.None).ConfigureAwait(false);
                            }
                            catch (Exception ex)
                            {
                                TeardownSessionNoThrow(session);
                                _log?.Warn("Gossip", $"queued send {frame.Type} failed for {EndpointLogFormatter.FormatEndpoint(session.RemoteEndpoint)}: {ex.Message}");
                                return;
                            }
                        }

                        Interlocked.Exchange(ref session.RelayPumpStarted, 0);
                        if (session.RelayQueue.IsEmpty || Interlocked.CompareExchange(ref session.RelayPumpStarted, 1, 0) != 0)
                            return;
                    }
                }
                catch (ObjectDisposedException)
                {
                }
                catch (OperationCanceledException)
                {
                }
            }, CancellationToken.None);
        }

        private void LogRelayQueueOverflow(PeerSession session, MsgType type)
        {
            lock (session.RelayOverflowLogGate)
            {
                var now = DateTime.UtcNow;
                if ((now - session.RelayOverflowLastLogUtc) >= RelayQueueOverflowLogWindow)
                {
                    if (session.RelayOverflowSuppressed > 0)
                    {
                        _log?.Warn(
                            "Gossip",
                            $"Relay queue full for {EndpointLogFormatter.FormatEndpoint(session.RemoteEndpoint)} while queueing {type} (+{session.RelayOverflowSuppressed} similar drops suppressed)");
                    }
                    else
                    {
                        _log?.Warn("Gossip", $"Relay queue full for {EndpointLogFormatter.FormatEndpoint(session.RemoteEndpoint)} while queueing {type}.");
                    }

                    session.RelayOverflowLastLogUtc = now;
                    session.RelayOverflowSuppressed = 0;
                }
                else
                {
                    session.RelayOverflowSuppressed++;
                }
            }
        }

        private bool ShouldRelayToPeer(PeerSession session, MsgType type, byte[] itemId)
        {
            if (session == null || itemId is not { Length: 32 })
                return false;

            var bucket = GetPeerRelaySet(session, type);
            return bucket.TryAdd(itemId);
        }

        private bool ShouldPushBlockToPeer(PeerSession session, byte[] blockHash)
            => ShouldRelayToPeer(session, MsgType.Block, blockHash);

        private LruSet GetPeerRelaySet(PeerSession session, MsgType type)
        {
            var map = type == MsgType.Block ? _peerSentBlocks : _peerSentTxs;
            return map.GetOrAdd(
                session.SessionKey,
                static _ => new LruSet(MaxPeerRelayEntries, PeerRelayDedupeTtl));
        }

        private Task RelayValidatedBlockAsync(Block block, PeerSession sourcePeer, CancellationToken ct)
        {
            if (block == null)
                return Task.CompletedTask;

            _ = QueueBlockRelay(block, exceptEndpoint: sourcePeer?.RemoteEndpoint, includeTipState: true);
            return Task.CompletedTask;
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
            var buf = new byte[HandshakeBaseLength + HandshakeCapabilitiesLength];
            buf[0] = CurrentHandshakeVersion;
            buf[1] = GenesisConfig.NetworkId;
            Buffer.BlockCopy(_nodeId, 0, buf, 2, 32);
            BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(34, 2), (ushort)_listenPort);
            BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(HandshakeBaseLength, HandshakeCapabilitiesLength), (uint)DefaultHandshakeCapabilities);
            return buf;
        }

        private static HandshakeCapabilities ParseHandshakeCapabilities(byte[] payload)
        {
            if (payload == null || payload.Length < HandshakeBaseLength + HandshakeCapabilitiesLength)
                return HandshakeCapabilities.None;

            uint raw = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(HandshakeBaseLength, HandshakeCapabilitiesLength));
            return (HandshakeCapabilities)raw;
        }

        private TcpListener? TryStartListener(IPAddress bindAddress, int port, bool ipv6Only, out bool listening)
        {
            listening = false;
            try
            {
                var listener = new TcpListener(bindAddress, port);
                listener.Server.ExclusiveAddressUse = true;
                if (listener.Server.AddressFamily == AddressFamily.InterNetworkV6)
                {
                    try { listener.Server.DualMode = !ipv6Only; } catch { }
                    try { listener.Server.SetSocketOption(SocketOptionLevel.IPv6, SocketOptionName.IPv6Only, ipv6Only); } catch { }
                }

                listener.Start();
                listening = true;
                return listener;
            }
            catch (SocketException ex)
            {
                _log?.Warn("P2P", $"listener {bindAddress} failed on port {port}: {ex.SocketErrorCode}");
                return null;
            }
        }

        private static async Task<IReadOnlyList<IPAddress>> ResolveDialAddressesAsync(string host, CancellationToken ct)
        {
            if (PeerAddress.TryParseIp(host, out var literal))
            {
                if (!NetworkParams.EnableIpv6 && literal.AddressFamily == AddressFamily.InterNetworkV6)
                    return Array.Empty<IPAddress>();

                return new[] { literal };
            }

            try
            {
#if NET8_0_OR_GREATER
                var resolved = await Dns.GetHostAddressesAsync(host, ct).ConfigureAwait(false);
#else
                var resolved = await Dns.GetHostAddressesAsync(host).ConfigureAwait(false);
#endif
                return PeerAddress.OrderDialAddresses(resolved);
            }
            catch
            {
                return Array.Empty<IPAddress>();
            }
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
            return TryGetPreferredConnectedPeerSession(wantedIp, port, out var preferred)
                ? preferred?.RemoteIsPublic
                : null;
        }

        public string? GetPeerDirectionText(string ip, int port)
        {
            if (string.IsNullOrWhiteSpace(ip)) return null;
            if (port <= 0 || port > 65535) return null;

            string wantedIp = NormalizeBanKey(ip);
            return TryGetPreferredConnectedPeerSession(wantedIp, port, out var preferred)
                ? (preferred!.IsInbound ? "inbound" : "outbound")
                : null;
        }

        public int? GetPeerLatencyMs(string ip, int port)
        {
            if (string.IsNullOrWhiteSpace(ip)) return null;
            if (port <= 0 || port > 65535) return null;

            string wantedIp = NormalizeBanKey(ip);
            return TryGetPreferredConnectedPeerSession(wantedIp, port, out var preferred)
                ? (preferred!.LastLatencyMs >= 0 ? preferred.LastLatencyMs : null)
                : null;
        }

        private bool IsMatchingConnectedPeerSession(PeerSession? s, string wantedIp, int port)
        {
            if (s == null || !s.HandshakeOk || !s.Client.Connected)
                return false;

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

            return sessionIp == wantedIp && sessionPort == port;
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

                if (!PeerAddress.IsPublicRoutable(ip) || SelfPeerGuard.IsSelf(ip, port))
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
            if (!PeerAddress.IsPublicRoutable(ip))
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
            if (!PeerAddress.TryParseIp(ip, out var address) || !PeerAddress.IsPublicRoutable(address))
                return (false, "non-public endpoint");
            if (port <= 0 || port > 65535)
                return (false, "invalid port");

            using var client = new TcpClient(address.AddressFamily)
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
                await client.ConnectAsync(address, port, timeoutCts.Token).ConfigureAwait(false);
#else
                var connectTask = client.ConnectAsync(address.ToString(), port);
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
            if (!PeerAddress.IsPublicRoutable(ip)) return;
            if (port <= 0 || port > 65535) return;
            if (IsPeerMarkedNonPublic(ip, port)) return;
            if (TryBuildPeerEndpointKey(ip, port, out var key))
                _publicClaimsByPex[key] = 0;
        }

        public List<(string ip, int port)> GetPublicPeerCandidates(int maxPeers, bool includeIpv6 = true)
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
                if (!PeerAddress.IsPublicRoutable(s.RemoteIpAdvertised!)) continue;
                if (!includeIpv6 && !IsPublicRoutableIPv4Literal(s.RemoteIpAdvertised!)) continue;
                if (SelfPeerGuard.IsSelf(s.RemoteIpAdvertised!, p)) continue;
                if (IsPeerMarkedNonPublic(s.RemoteIpAdvertised!, p)) continue;

                if (!TryBuildPeerEndpointKey(s.RemoteIpAdvertised!, p, out var key)) continue;
                if (!seen.Add(key)) continue;
                list.Add((s.RemoteIpAdvertised!, p));
            }

            return list;
        }

        public List<(string ip, int port)> GetPeerCandidatesForPex(int maxPeers, int unverifiedPercent = 20, bool includeIpv6 = true)
        {
            if (maxPeers <= 0)
                return new List<(string ip, int port)>();

            int percent = Math.Clamp(unverifiedPercent, 0, 100);
            int unverifiedBudget = percent == 0
                ? 0
                : Math.Min(maxPeers, Math.Max(1, (maxPeers * percent + 99) / 100));
            int verifiedTarget = Math.Max(0, maxPeers - unverifiedBudget);

            var verified = new List<(string ip, int port)>(maxPeers);
            var unverified = new List<(string ip, int port)>(maxPeers);
            var seen = new HashSet<string>(StringComparer.Ordinal);

            AppendConnectedPublicCandidates(verified, seen, maxPeers, includeIpv6);
            AppendStoredPexCandidates(verified, unverified, seen, maxPeers, includeIpv6);

            var selected = new List<(string ip, int port)>(maxPeers);
            int verifiedAdded = AddCandidates(selected, verified, startIndex: 0, maxToAdd: verifiedTarget);
            int unverifiedAdded = AddCandidates(selected, unverified, startIndex: 0, maxToAdd: maxPeers - selected.Count);
            AddCandidates(selected, verified, startIndex: verifiedAdded, maxToAdd: maxPeers - selected.Count);
            AddCandidates(selected, unverified, startIndex: unverifiedAdded, maxToAdd: maxPeers - selected.Count);
            return selected;
        }

        private void MarkPeerAsPublic(string ip, int port)
        {
            if (!PeerAddress.IsPublicRoutable(ip)) return;
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
            if (!PeerAddress.IsPublicRoutable(ip)) return;
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

        private void AppendConnectedPublicCandidates(
            List<(string ip, int port)> target,
            HashSet<string> seen,
            int maxPeers,
            bool includeIpv6)
        {
            foreach (var kv in _sessions)
            {
                if (target.Count >= maxPeers)
                    break;

                var s = kv.Value;
                if (!s.HandshakeOk || !s.Client.Connected || !s.RemoteIsPublic)
                    continue;
                if (string.IsNullOrWhiteSpace(s.RemoteIpAdvertised))
                    continue;
                if (s.RemotePortAdvertised is not int p || p <= 0 || p > 65535)
                    continue;

                TryAddPexCandidate(target, seen, s.RemoteIpAdvertised!, p, includeIpv6);
            }
        }

        private void AppendStoredPexCandidates(
            List<(string ip, int port)> verified,
            List<(string ip, int port)> unverified,
            HashSet<string> seen,
            int maxPeers,
            bool includeIpv6)
        {
            int storeLimit = Math.Min(256, Math.Max(maxPeers * 4, maxPeers));
            foreach (var (ip, port, _) in PeerStore.GetRecentPeers(storeLimit))
            {
                if ((verified.Count + unverified.Count) >= maxPeers * 2)
                    break;
                if (IsPeerMarkedNonPublic(ip, port))
                    continue;

                bool isVerified = IsPeerMarkedPublic(ip, port);
                var target = isVerified ? verified : unverified;
                TryAddPexCandidate(target, seen, ip, port, includeIpv6);
            }
        }

        private bool TryAddPexCandidate(
            List<(string ip, int port)> target,
            HashSet<string> seen,
            string ip,
            int port,
            bool includeIpv6)
        {
            if (string.IsNullOrWhiteSpace(ip))
                return false;
            if (port <= 0 || port > 65535)
                return false;
            if (!PeerAddress.IsPublicRoutable(ip))
                return false;
            if (!includeIpv6 && !IsPublicRoutableIPv4Literal(ip))
                return false;
            if (SelfPeerGuard.IsSelf(ip, port))
                return false;
            if (!TryBuildPeerEndpointKey(ip, port, out var key))
                return false;
            if (!seen.Add(key))
                return false;

            target.Add((ip, port));
            return true;
        }

        private static int AddCandidates(
            List<(string ip, int port)> target,
            List<(string ip, int port)> source,
            int startIndex,
            int maxToAdd)
        {
            if (maxToAdd <= 0 || source.Count == 0)
                return 0;

            int added = 0;
            for (int i = Math.Max(0, startIndex); i < source.Count && maxToAdd > 0; i++, maxToAdd--)
            {
                target.Add(source[i]);
                added++;
            }

            return added;
        }

        private static bool TryBuildPeerEndpointKey(string ip, int port, out string key)
            => PeerAddress.TryBuildEndpointKey(ip, port, out key);

        private static bool TryParsePeerEndpointKey(string key, out string ip, out int port)
            => PeerAddress.TryParseEndpointKey(key, out ip, out port);

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
                if (!PeerAddress.TryParseIp(hostOrIp, out var addr)) return;
                string ip = PeerAddress.NormalizeHost(addr.ToString());
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
            if (IsInitialBlockSyncActive && HasBetterPeerTipThanLocal)
            {
                bool shouldLogCatchup = false;
                int suppressedCount = 0;
                var catchupNow = DateTime.UtcNow;

                lock (_knownBlockLogGate)
                {
                    _knownBlockCatchupSuppressed++;
                    if (catchupNow >= _nextKnownBlockCatchupLogUtc)
                    {
                        _nextKnownBlockCatchupLogUtc = catchupNow + KnownBlockCatchupLogWindow;
                        suppressedCount = _knownBlockCatchupSuppressed;
                        _knownBlockCatchupSuppressed = 0;
                        shouldLogCatchup = true;
                    }
                }

                if (shouldLogCatchup)
                {
                    _log?.Info("P2P",
                        suppressedCount <= 1
                            ? "Block already known (catch-up tail)."
                            : $"Block already known (catch-up tail) x{suppressedCount}.");
                }

                return;
            }

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
            if (ingress == BlockIngressKind.LivePush &&
                ShouldIgnorePeerLiveOrphans(peerKey, now))
            {
                reason = "peer orphan cooldown active";
                return false;
            }

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
                    if (ingress == BlockIngressKind.LivePush &&
                        NotePeerLiveOrphanLimitHit(orphan.PeerKey, now))
                    {
                        reason = "peer orphan cooldown active";
                        return false;
                    }

                    if (!EvictOldestOrphanForPeerNoLock(orphan.PeerKey))
                    {
                        reason = "per-peer orphan limit reached";
                        return false;
                    }

                    peerCount = _orphansByPeer.TryGetValue(orphan.PeerKey, out n) ? n : 0;
                    if (peerCount >= MaxOrphansPerPeer)
                    {
                        reason = "per-peer orphan limit reached";
                        return false;
                    }
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

        private bool ShouldIgnorePeerLiveOrphans(string peerKey, DateTime nowUtc)
        {
            if (string.IsNullOrWhiteSpace(peerKey))
                return false;

            lock (_peerOrphanCooldownGate)
            {
                PrunePeerOrphanCooldownsNoLock(nowUtc);

                if (!_peerOrphanCooldowns.TryGetValue(peerKey, out var state))
                    return false;

                state.LastTouchedUtc = nowUtc;
                if (nowUtc < state.CooldownUntilUtc)
                    return true;

                if ((nowUtc - state.WindowStartUtc) > OrphanPeerStrikeWindow)
                {
                    state.WindowStartUtc = nowUtc;
                    state.StrikeCount = 0;
                    state.CooldownDuration = TimeSpan.Zero;
                }

                return false;
            }
        }

        private bool NotePeerLiveOrphanLimitHit(string peerKey, DateTime nowUtc)
        {
            if (string.IsNullOrWhiteSpace(peerKey))
                return false;

            lock (_peerOrphanCooldownGate)
            {
                PrunePeerOrphanCooldownsNoLock(nowUtc);

                if (!_peerOrphanCooldowns.TryGetValue(peerKey, out var state))
                {
                    state = new PeerOrphanCooldownState
                    {
                        WindowStartUtc = nowUtc,
                        LastTouchedUtc = nowUtc
                    };
                    _peerOrphanCooldowns[peerKey] = state;
                }

                state.LastTouchedUtc = nowUtc;
                if ((nowUtc - state.WindowStartUtc) > OrphanPeerStrikeWindow)
                {
                    state.WindowStartUtc = nowUtc;
                    state.StrikeCount = 0;
                    if (state.CooldownUntilUtc <= nowUtc)
                        state.CooldownDuration = TimeSpan.Zero;
                }

                state.StrikeCount++;
                if (state.StrikeCount < OrphanPeerCooldownTriggerCount)
                    return false;

                var nextCooldown = state.CooldownDuration <= TimeSpan.Zero
                    ? OrphanPeerInitialCooldown
                    : TimeSpan.FromSeconds(Math.Min(state.CooldownDuration.TotalSeconds * 2.0, OrphanPeerMaxCooldown.TotalSeconds));

                state.CooldownDuration = nextCooldown;
                state.CooldownUntilUtc = nowUtc + nextCooldown;
                state.WindowStartUtc = nowUtc;
                state.StrikeCount = 0;
                return true;
            }
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

        private void PrunePeerOrphanCooldownsNoLock(DateTime nowUtc)
        {
            if (_peerOrphanCooldowns.Count == 0)
                return;

            var stale = new List<string>();
            foreach (var kv in _peerOrphanCooldowns)
            {
                var state = kv.Value;
                bool cooldownExpired = state.CooldownUntilUtc <= nowUtc;
                bool quietLongEnough = (nowUtc - state.LastTouchedUtc) > OrphanPeerStrikeWindow;
                if (cooldownExpired && quietLongEnough)
                    stale.Add(kv.Key);
            }

            for (int i = 0; i < stale.Count; i++)
                _peerOrphanCooldowns.Remove(stale[i]);

            while (_peerOrphanCooldowns.Count > MaxOrphanPeerCooldownEntries)
            {
                string? oldestKey = null;
                DateTime oldest = DateTime.MaxValue;
                foreach (var kv in _peerOrphanCooldowns)
                {
                    if (kv.Value.LastTouchedUtc < oldest)
                    {
                        oldest = kv.Value.LastTouchedUtc;
                        oldestKey = kv.Key;
                    }
                }

                if (oldestKey == null)
                    break;

                _peerOrphanCooldowns.Remove(oldestKey);
            }
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

        private bool EvictOldestOrphanForPeerNoLock(string peerKey)
        {
            if (string.IsNullOrWhiteSpace(peerKey) || _orphansByHash.Count == 0)
                return false;

            string? oldestKey = null;
            DateTime oldest = DateTime.MaxValue;
            foreach (var kv in _orphansByHash)
            {
                if (!string.Equals(kv.Value.PeerKey, peerKey, StringComparison.Ordinal))
                    continue;

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

        private static int CompareNodeIds(byte[] left, byte[] right)
        {
            if (left is not { Length: 32 } || right is not { Length: 32 })
                return 0;

            for (int i = 0; i < 32; i++)
            {
                int diff = left[i].CompareTo(right[i]);
                if (diff != 0)
                    return diff;
            }

            return 0;
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
            return PeerAddress.TrySplitHostPort(endpoint, out _, out port);
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
            => PeerAddress.IsPublicRoutableIPv4(ip);

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
        {
            string normalized = PeerAddress.NormalizeHost(raw);
            return normalized.Length > 0
                ? normalized
                : (raw ?? string.Empty).Trim().ToLowerInvariant();
        }
    }
}


