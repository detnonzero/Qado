using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Qado.Logging;

namespace Qado.Networking
{
    public enum BlockSyncState
    {
        Idle = 0,
        AwaitingBatchBegin = 1,
        ReceivingBatch = 2
    }

    public readonly record struct BlockSyncPrepareResult(bool Success, string Error);

    public readonly record struct BlockSyncCommitResult(bool Success, byte[] LastBlockHash, string Error);

    internal readonly record struct PeerTipState(ulong Height, byte[] Hash, UInt128 Chainwork);

    public sealed class BlockSyncClient : IDisposable
    {
        private const string PeerTipNotAheadErrorPrefix = "peer tip chainwork not better than local chain";
        public static readonly TimeSpan BatchResponseTimeout = TimeSpan.FromSeconds(20);
        private static readonly TimeSpan PeerCooldown = TimeSpan.FromSeconds(60);
        private static readonly TimeSpan TimeoutCooldownShort = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan TimeoutCooldownMedium = TimeSpan.FromSeconds(25);
        private static readonly TimeSpan PlannerLivenessPollInterval = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan IdleBehindRecoveryThreshold = TimeSpan.FromSeconds(15);
        private static readonly TimeSpan ActiveBehindRecoveryThreshold = TimeSpan.FromSeconds(30);
        private const int BaseMaxParallelWindows = 3;
        private const int MediumMaxParallelWindows = 4;
        private const int PreferredMaxParallelWindows = 5;

        private sealed class PeerSyncStats
        {
            public int PreferredWindowBlocks = BlockSyncProtocol.BatchMaxBlocks;
            public int ConsecutiveSuccesses;
            public int ConsecutiveTimeouts;
            public int TimeoutStrikes;
            public int CompletedWindows;
            public int TimeoutEvents;
            public DateTime LastWindowCompletedUtc = DateTime.MinValue;
            public DateTime LastTimeoutUtc = DateTime.MinValue;
        }

        private enum WindowRequestKind
        {
            Locator = 0,
            FromHash = 1
        }

        private sealed class SyncWindowRequest
        {
            public int Generation;
            public int Serial;
            public string PeerKey = string.Empty;
            public WindowRequestKind Kind;
            public byte[] RequestedFromHash = Array.Empty<byte>();
            public ulong RequestedFromHeight;
            public int MaxBlocks;
            public UInt128 AdvertisedTipChainwork;
            public bool BatchPrepared;
            public Guid BatchId;
            public byte[] ForkHash = Array.Empty<byte>();
            public ulong ForkHeight;
            public ulong StartHeight;
            public int ExpectedBlocks;
            public int ReceivedBlocks;
            public int CommittedBlocks;
            public byte[] NextCommitPrevHash = Array.Empty<byte>();
            public ulong NextCommitHeight;
            public bool HasPreview;
            public byte[] PreviewLastHash = Array.Empty<byte>();
            public ulong PreviewLastHeight;
            public int TimeoutRevision;
            public TimeSpan TimeoutDuration;
            public readonly List<byte[]> Blocks = new();
        }

        private sealed class CompletedWindow
        {
            public int Generation;
            public PeerSession Peer = null!;
            public string PeerKey = string.Empty;
            public ulong StartHeight;
            public byte[] PrevHash = Array.Empty<byte>();
            public ulong PrevHeight;
            public byte[] LastHash = Array.Empty<byte>();
            public ulong LastHeight;
            public bool MoreAvailable;
            public bool ContinueAfterCommit;
            public List<byte[]> Blocks = new();
        }

        private readonly record struct RequestCommitSlice(
            int Generation,
            int Serial,
            ulong StartHeight,
            byte[] PrevHash,
            List<byte[]> Blocks);

        private readonly object _gate = new();
        private readonly Func<IReadOnlyCollection<PeerSession>> _sessionSnapshot;
        private readonly Func<PeerSession, MsgType, byte[], CancellationToken, Task> _sendFrameAsync;
        private readonly Func<UInt128> _getLocalTipChainwork;
        private readonly Func<byte[]> _getLocalTipHash;
        private readonly Func<ulong> _getLocalTipHeight;
        private readonly Func<byte[], ulong, UInt128, PeerSession, CancellationToken, Task<BlockSyncPrepareResult>> _prepareBatchAsync;
        private readonly Func<IReadOnlyList<byte[]>, ulong, byte[], PeerSession, CancellationToken, Task<BlockSyncCommitResult>> _commitBlocksAsync;
        private readonly Func<PeerSession, BlocksEndStatus, CancellationToken, Task> _completeBatchAsync;
        private readonly Func<PeerSession?, string, CancellationToken, Task> _abortBatchAsync;
        private readonly Action<PeerSession, string>? _penalizePeer;
        private readonly ILogSink? _log;

        private readonly Dictionary<string, PeerTipState> _tipByPeerKey = new(StringComparer.Ordinal);
        private readonly Dictionary<string, DateTime> _cooldownUntilByPeerKey = new(StringComparer.Ordinal);
        private readonly Dictionary<string, SyncWindowRequest> _requestsByPeerKey = new(StringComparer.Ordinal);
        private readonly Dictionary<string, PeerSyncStats> _peerSyncStatsByPeerKey = new(StringComparer.Ordinal);
        private readonly SortedDictionary<ulong, CompletedWindow> _completedWindowsByStartHeight = new();
        private readonly CancellationTokenSource _disposeCts = new();

        private BlockSyncState _state;
        private bool _planningReady = true;
        private bool _useLocatorNext = true;
        private bool _havePlanningCursor;
        private byte[] _planningCursorHash = Array.Empty<byte>();
        private ulong _planningCursorHeight;
        private bool _haveCommittedCursor;
        private byte[] _committedHash = Array.Empty<byte>();
        private ulong _committedHeight;
        private byte[] _resumeFromHash = Array.Empty<byte>();
        private int _requestSerial;
        private int _pipelineGeneration;
        private bool _commitLoopRunning;
        private DateTime _lastProgressUtc = DateTime.UtcNow;
        private string _lastProgressReason = "startup";
        private DateTime _lastPlannerRecoveryLogUtc = DateTime.MinValue;
        private bool _haveExhaustedContinuation;
        private byte[] _exhaustedContinuationFromHash = Array.Empty<byte>();
        private ulong _exhaustedContinuationFromHeight;
        private byte[] _exhaustedContinuationTipHash = Array.Empty<byte>();
        private ulong _exhaustedContinuationTipHeight;
        private UInt128 _exhaustedContinuationTipChainwork;
        private byte[] _lastRemoteTipLogHash = Array.Empty<byte>();
        private ulong _lastRemoteTipLogHeight;

        public BlockSyncClient(
            Func<IReadOnlyCollection<PeerSession>> sessionSnapshot,
            Func<PeerSession, MsgType, byte[], CancellationToken, Task> sendFrameAsync,
            Func<UInt128> getLocalTipChainwork,
            Func<byte[]> getLocalTipHash,
            Func<byte[], ulong, UInt128, PeerSession, CancellationToken, Task<BlockSyncPrepareResult>> prepareBatchAsync,
            Func<IReadOnlyList<byte[]>, ulong, byte[], PeerSession, CancellationToken, Task<BlockSyncCommitResult>> commitBlocksAsync,
            Func<PeerSession, BlocksEndStatus, CancellationToken, Task> completeBatchAsync,
            Func<PeerSession?, string, CancellationToken, Task> abortBatchAsync,
            Action<PeerSession, string>? penalizePeer = null,
            ILogSink? log = null,
            Func<ulong>? getLocalTipHeight = null)
        {
            _sessionSnapshot = sessionSnapshot ?? throw new ArgumentNullException(nameof(sessionSnapshot));
            _sendFrameAsync = sendFrameAsync ?? throw new ArgumentNullException(nameof(sendFrameAsync));
            _getLocalTipChainwork = getLocalTipChainwork ?? throw new ArgumentNullException(nameof(getLocalTipChainwork));
            _getLocalTipHash = getLocalTipHash ?? throw new ArgumentNullException(nameof(getLocalTipHash));
            _getLocalTipHeight = getLocalTipHeight ?? (() => 0UL);
            _prepareBatchAsync = prepareBatchAsync ?? throw new ArgumentNullException(nameof(prepareBatchAsync));
            _commitBlocksAsync = commitBlocksAsync ?? throw new ArgumentNullException(nameof(commitBlocksAsync));
            _completeBatchAsync = completeBatchAsync ?? throw new ArgumentNullException(nameof(completeBatchAsync));
            _abortBatchAsync = abortBatchAsync ?? throw new ArgumentNullException(nameof(abortBatchAsync));
            _penalizePeer = penalizePeer;
            _log = log;

            _ = MonitorPlannerLivenessAsync();
        }

        public BlockSyncState State
        {
            get
            {
                lock (_gate)
                    return _state;
            }
        }

        public bool IsActive
        {
            get
            {
                lock (_gate)
                    return _state != BlockSyncState.Idle;
            }
        }

        public async Task OnPeerReadyAsync(PeerSession peer, CancellationToken ct)
        {
            if (peer == null)
                return;

            await EnsurePipelineAsync(ct).ConfigureAwait(false);
        }

        public void RequestResync(string reason = "manual")
        {
            List<PeerSession> activePeers;

            lock (_gate)
            {
                activePeers = ResolveActivePeers_NoLock();
                _tipByPeerKey.Clear();
                _haveCommittedCursor = false;
                _committedHash = Array.Empty<byte>();
                _committedHeight = 0;
                ResetPipeline_NoLock(forceLocator: true);
                _resumeFromHash = SafeCloneHash(_getLocalTipHash());
                NoteProgress_NoLock($"resync:{reason}");
            }

            for (int i = 0; i < activePeers.Count; i++)
                _ = _abortBatchAsync(activePeers[i], $"resync requested ({reason})", CancellationToken.None);

            _log?.Info("Sync", $"Block sync resync requested ({reason}).");
            _ = EnsurePipelineAsync(CancellationToken.None);
        }

        public async Task OnTipStateAsync(PeerSession peer, SmallNetTipStateFrame frame, CancellationToken ct)
        {
            if (peer == null)
                return;

            lock (_gate)
            {
                _tipByPeerKey[peer.SessionKey] = new PeerTipState(frame.Height, SafeCloneHash(frame.TipHash), frame.Chainwork);
                MaybeClearExhaustedContinuation_NoLock(frame.TipHash, frame.Height, frame.Chainwork);
            }

            await EnsurePipelineAsync(ct).ConfigureAwait(false);
        }

        public async Task OnBlocksBatchStartAsync(PeerSession peer, SmallNetBlocksBatchStartFrame frame, CancellationToken ct)
        {
            if (peer == null)
                return;

            SyncWindowRequest? request;
            int staleSerial = 0;
            string? staleReason = null;
            string? failureReason = null;
            lock (_gate)
            {
                if (!_requestsByPeerKey.TryGetValue(peer.SessionKey, out request))
                {
                    staleReason = $"stale batch start ignored: no active request batchId={frame.BatchId} fork={ShortHash(frame.ForkHash)} forkHeight={frame.ForkHeight}";
                    request = null;
                }

                if (request != null && request.BatchPrepared)
                    return;

                if (request != null && (frame.TotalBlocks < 0 || frame.TotalBlocks > BlockSyncProtocol.ExtendedSyncWindowBlocks))
                {
                    failureReason = $"invalid batch start size: totalBlocks={frame.TotalBlocks}";
                }
                else if (request != null &&
                         request.Kind == WindowRequestKind.FromHash &&
                         (!BytesEqual32(frame.ForkHash, request.RequestedFromHash) ||
                          frame.ForkHeight != request.RequestedFromHeight))
                {
                    staleSerial = request.Serial;
                    staleReason =
                        $"stale batch start ignored: expected={ShortHash(request.RequestedFromHash)}@{request.RequestedFromHeight} " +
                        $"got={ShortHash(frame.ForkHash)}@{frame.ForkHeight} batchId={frame.BatchId}";
                    request = null;
                }
            }

            if (staleReason != null)
            {
                await HandleObsoleteBatchStartAsync(peer, staleSerial, staleReason, ct).ConfigureAwait(false);
                return;
            }

            if (failureReason != null)
            {
                await HandlePipelineFailureAsync(peer, failureReason, forceLocator: true, penalize: true, ct)
                    .ConfigureAwait(false);
                return;
            }

            var activeRequest = request!;
            BlockSyncPrepareResult prep = new(true, string.Empty);
            if (activeRequest.Kind == WindowRequestKind.Locator)
            {
                prep = await _prepareBatchAsync(
                    frame.ForkHash,
                    frame.ForkHeight + 1UL,
                    activeRequest.AdvertisedTipChainwork,
                    peer,
                    ct).ConfigureAwait(false);
                if (!prep.Success)
                {
                    if (IsNoLongerNeededPrepareFailure(prep.Error))
                    {
                        await HandlePrepareNoLongerNeededAsync(peer, activeRequest.Serial, prep.Error, ct)
                            .ConfigureAwait(false);
                        return;
                    }

                    await HandlePipelineFailureAsync(peer, $"batch prepare failed: {prep.Error}", forceLocator: true, penalize: true, ct)
                        .ConfigureAwait(false);
                    return;
                }
            }

            bool scheduleMore = false;
            lock (_gate)
            {
                if (!_requestsByPeerKey.TryGetValue(peer.SessionKey, out var active) || active.Serial != activeRequest.Serial)
                    return;

                active.BatchPrepared = true;
                active.BatchId = frame.BatchId;
                active.ForkHash = SafeCloneHash(frame.ForkHash);
                active.ForkHeight = frame.ForkHeight;
                active.StartHeight = frame.ForkHeight + 1UL;
                active.ExpectedBlocks = frame.TotalBlocks;
                active.ReceivedBlocks = 0;
                active.CommittedBlocks = 0;
                active.NextCommitPrevHash = SafeCloneHash(frame.ForkHash);
                active.NextCommitHeight = frame.ForkHeight + 1UL;
                active.Blocks.Clear();
                NoteProgress_NoLock("batch-start");

                if (TryValidatePreview(frame, out var previewHash, out var previewHeight))
                {
                    active.HasPreview = true;
                    active.PreviewLastHash = previewHash;
                    active.PreviewLastHeight = previewHeight;
                    _planningCursorHash = SafeCloneHash(previewHash);
                    _planningCursorHeight = previewHeight;
                    _havePlanningCursor = true;
                    _planningReady = true;
                    _useLocatorNext = false;
                    scheduleMore = true;
                }

                ArmTimeout_NoLock(active);
                RefreshState_NoLock();
            }

            if (scheduleMore)
                await EnsurePipelineAsync(ct).ConfigureAwait(false);
        }

        public async Task OnBlocksBatchDataAsync(PeerSession peer, byte[] payload, CancellationToken ct)
        {
            if (peer == null)
                return;

            if (!BlockSyncProtocol.TryParseBlockBatch(payload, out var frame))
            {
                await HandlePipelineFailureAsync(peer, "invalid block-batch payload", forceLocator: true, penalize: true, ct)
                    .ConfigureAwait(false);
                return;
            }

            await OnBlocksPayloadsAsync(peer, frame.FirstHeight, frame.Blocks, "batch", ct).ConfigureAwait(false);
        }

        public async Task OnBlocksChunkAsync(PeerSession peer, byte[] payload, CancellationToken ct)
        {
            if (peer == null)
                return;

            if (!BlockSyncProtocol.TryParseBlockChunk(payload, out var frame))
            {
                await HandlePipelineFailureAsync(peer, "invalid block-chunk payload", forceLocator: true, penalize: true, ct)
                    .ConfigureAwait(false);
                return;
            }

            await OnBlocksPayloadsAsync(peer, frame.FirstHeight, frame.Blocks, "legacy-chunk", ct).ConfigureAwait(false);
        }

        public async Task OnBlocksBatchEndAsync(PeerSession peer, SmallNetBlocksBatchEndFrame frame, CancellationToken ct)
        {
            if (peer == null)
                return;

            await TryCommitBufferedProgressAsync(peer, flushRemainder: true, ct).ConfigureAwait(false);

            CompletedWindow? completed = null;
            BlocksEndStatus status = frame.MoreAvailable ? BlocksEndStatus.MoreAvailable : BlocksEndStatus.TipReached;
            bool scheduleMore = false;
            bool tipReached = false;
            string? failureReason = null;

            lock (_gate)
            {
                if (!_requestsByPeerKey.TryGetValue(peer.SessionKey, out var request))
                    return;

                if (!request.BatchPrepared)
                    return;

                if (request.BatchId != Guid.Empty && frame.BatchId != request.BatchId)
                    return;

                if (request.ReceivedBlocks != request.ExpectedBlocks)
                {
                    failureReason = "batch ended before declared block count arrived";
                }
                else if (request.HasPreview &&
                         (!BytesEqual32(request.PreviewLastHash, frame.LastHash) ||
                          request.PreviewLastHeight != frame.LastHeight))
                {
                    failureReason = "batch end preview mismatch";
                }
                else
                {
                    _requestsByPeerKey.Remove(peer.SessionKey);
                    RecordSuccessfulWindow_NoLock(peer, request.ReceivedBlocks, request.MaxBlocks);
                    if (!frame.MoreAvailable)
                        MarkExhaustedContinuation_NoLock(request, frame);

                    int remainingBlocks = request.ReceivedBlocks - request.CommittedBlocks;
                    if (remainingBlocks > 0)
                    {
                        var remaining = request.Blocks.GetRange(request.CommittedBlocks, remainingBlocks);
                        completed = new CompletedWindow
                        {
                            Generation = request.Generation,
                            Peer = peer,
                            PeerKey = peer.SessionKey,
                            StartHeight = request.NextCommitHeight,
                            PrevHash = SafeCloneHash(request.NextCommitPrevHash),
                            PrevHeight = request.NextCommitHeight == 0 ? request.ForkHeight : request.NextCommitHeight - 1UL,
                            LastHash = SafeCloneHash(frame.LastHash),
                            LastHeight = frame.LastHeight,
                            MoreAvailable = frame.MoreAvailable,
                            ContinueAfterCommit = !request.HasPreview && frame.MoreAvailable,
                            Blocks = remaining
                        };
                        _completedWindowsByStartHeight[completed.StartHeight] = completed;
                    }
                    else
                    {
                        if (!request.HasPreview)
                            _planningReady = true;

                        if (!request.HasPreview && frame.MoreAvailable && _haveCommittedCursor)
                        {
                            _planningCursorHash = SafeCloneHash(_committedHash);
                            _planningCursorHeight = _committedHeight;
                            _havePlanningCursor = _committedHash is { Length: 32 };
                            _useLocatorNext = false;
                            scheduleMore = true;
                        }

                        // A preview continuation may have failed to pipeline earlier because no
                        // other eligible peer was free yet. Retry once the current request is gone.
                        if (request.HasPreview && frame.MoreAvailable && _planningReady)
                            scheduleMore = true;

                        tipReached = !frame.MoreAvailable && _requestsByPeerKey.Count == 0 && _completedWindowsByStartHeight.Count == 0;
                    }

                    if (remainingBlocks > 0 && request.HasPreview && frame.MoreAvailable && _planningReady)
                        scheduleMore = true;

                    NoteProgress_NoLock(frame.MoreAvailable ? "batch-end-more" : "batch-end-tip");
                    RefreshState_NoLock();
                }
            }

            if (failureReason != null)
            {
                await HandlePipelineFailureAsync(peer, failureReason, forceLocator: true, penalize: true, ct)
                    .ConfigureAwait(false);
                return;
            }

            await _completeBatchAsync(peer, status, ct).ConfigureAwait(false);

            if (scheduleMore)
                await EnsurePipelineAsync(ct).ConfigureAwait(false);

            if (completed != null)
                await TryCommitReadyWindowsAsync(ct).ConfigureAwait(false);
            else if (tipReached)
            {
                bool logTipReached;
                lock (_gate)
                    logTipReached = ShouldLogRemoteTipReached_NoLock(frame.LastHash, frame.LastHeight);

                if (logTipReached)
                    _log?.Info("Sync", "Block sync reached remote tip.");
            }
        }

        public async Task OnNoCommonAncestorAsync(PeerSession peer, CancellationToken ct)
        {
            if (peer == null)
                return;

            await HandlePipelineFailureAsync(peer, "peer reported no common ancestor", forceLocator: true, penalize: true, ct)
                .ConfigureAwait(false);
        }

        public async Task OnPeerDisconnectedAsync(PeerSession peer, CancellationToken ct)
        {
            if (peer == null)
                return;

            bool hadRequest;
            bool forceLocator;

            lock (_gate)
            {
                _tipByPeerKey.Remove(peer.SessionKey);
                hadRequest = _requestsByPeerKey.TryGetValue(peer.SessionKey, out var request);
                forceLocator = !hadRequest || !request!.BatchPrepared || !_haveCommittedCursor;
            }

            if (!hadRequest)
            {
                await EnsurePipelineAsync(ct).ConfigureAwait(false);
                return;
            }

            await HandlePipelineFailureAsync(peer, "peer disconnected mid-batch", forceLocator, penalize: false, ct)
                .ConfigureAwait(false);
        }

        public void Dispose()
        {
            try { _disposeCts.Cancel(); } catch { }
            try { _disposeCts.Dispose(); } catch { }
        }

        private async Task OnBlocksPayloadsAsync(
            PeerSession peer,
            ulong firstHeight,
            IReadOnlyList<byte[]> payloads,
            string source,
            CancellationToken ct)
        {
            string? failureReason = null;

            lock (_gate)
            {
                if (!_requestsByPeerKey.TryGetValue(peer.SessionKey, out var request) || !request.BatchPrepared)
                    return;

                ulong expectedFrameHeight = request.StartHeight + (ulong)request.ReceivedBlocks;
                if (firstHeight != expectedFrameHeight)
                {
                    failureReason = $"{source} first_height mismatch";
                }
                else if (request.ReceivedBlocks + payloads.Count > request.ExpectedBlocks)
                {
                    failureReason = $"{source} exceeds declared batch size";
                }
                else
                {
                    for (int i = 0; i < payloads.Count; i++)
                        request.Blocks.Add(payloads[i]);

                    request.ReceivedBlocks += payloads.Count;
                    NoteProgress_NoLock($"{source}-data");
                    ArmTimeout_NoLock(request);
                    RefreshState_NoLock();
                }
            }

            if (failureReason != null)
            {
                await HandlePipelineFailureAsync(peer, failureReason, forceLocator: true, penalize: true, ct)
                    .ConfigureAwait(false);
                return;
            }

            await TryCommitBufferedProgressAsync(peer, flushRemainder: false, ct).ConfigureAwait(false);
        }

        private async Task TryCommitBufferedProgressAsync(PeerSession peer, bool flushRemainder, CancellationToken ct)
        {
            while (true)
            {
                RequestCommitSlice slice;
                lock (_gate)
                {
                    if (!TryBuildRequestCommitSlice_NoLock(peer, flushRemainder, out slice))
                    {
                        _commitLoopRunning = false;
                        RefreshState_NoLock();
                        return;
                    }

                    _commitLoopRunning = true;
                    RefreshState_NoLock();
                }

                var commit = await _commitBlocksAsync(
                    slice.Blocks,
                    slice.StartHeight,
                    slice.PrevHash,
                    peer,
                    ct).ConfigureAwait(false);

                if (!commit.Success || commit.LastBlockHash is not { Length: 32 })
                {
                    lock (_gate)
                    {
                        _commitLoopRunning = false;
                        RefreshState_NoLock();
                    }

                    await HandlePipelineFailureAsync(
                            peer,
                            $"commit failed: {commit.Error}",
                            forceLocator: true,
                            penalize: true,
                            ct)
                        .ConfigureAwait(false);
                    return;
                }

                lock (_gate)
                {
                    if (!_requestsByPeerKey.TryGetValue(peer.SessionKey, out var request) || request.Serial != slice.Serial)
                    {
                        _commitLoopRunning = false;
                        RefreshState_NoLock();
                        continue;
                    }

                    if (request.Generation != slice.Generation)
                    {
                        _commitLoopRunning = false;
                        RefreshState_NoLock();
                        continue;
                    }

                    request.CommittedBlocks += slice.Blocks.Count;
                    request.NextCommitPrevHash = SafeCloneHash(commit.LastBlockHash);
                    request.NextCommitHeight += (ulong)slice.Blocks.Count;

                    _haveCommittedCursor = true;
                    _committedHash = SafeCloneHash(commit.LastBlockHash);
                    _committedHeight = request.NextCommitHeight - 1UL;
                    _resumeFromHash = SafeCloneHash(commit.LastBlockHash);
                    NoteProgress_NoLock("progress-commit");

                    int available = request.ReceivedBlocks - request.CommittedBlocks;
                    bool hasImmediateMore = flushRemainder ? available > 0 : available >= BlockSyncProtocol.BatchMaxBlocks;
                    if (!hasImmediateMore && _completedWindowsByStartHeight.Count == 0)
                        _commitLoopRunning = false;

                    RefreshState_NoLock();
                }
            }
        }

        private async Task EnsurePipelineAsync(CancellationToken ct)
        {
            PeerSession? selectedPeer;
            bool useLocator;
            byte[] fromHash;
            int maxBlocks;
            int serial;
            byte[] advertisedTipHash;
            ulong advertisedTipHeight;
            UInt128 advertisedTipChainwork;

            lock (_gate)
            {
                if (!_planningReady)
                    return;

                if (_requestsByPeerKey.Count + _completedWindowsByStartHeight.Count >= GetMaxParallelWindows_NoLock())
                    return;

                PruneCooldown_NoLock();

                selectedPeer = SelectBestPeer_NoLock();
                if (selectedPeer == null)
                    return;

                _tipByPeerKey.TryGetValue(selectedPeer.SessionKey, out var tip);
                advertisedTipHash = SafeCloneHash(tip.Hash);
                advertisedTipHeight = tip.Height;
                advertisedTipChainwork = tip.Chainwork;
                useLocator = _useLocatorNext || !_havePlanningCursor;
                fromHash = useLocator ? GetResumeHash_NoLock() : SafeCloneHash(_planningCursorHash);
                if (!useLocator &&
                    IsExhaustedContinuation_NoLock(
                        fromHash,
                        _planningCursorHeight,
                        advertisedTipHash,
                        advertisedTipHeight,
                        advertisedTipChainwork))
                {
                    return;
                }

                if (!useLocator && HasEquivalentActiveContinuationRequest_NoLock(fromHash, _planningCursorHeight))
                    return;

                maxBlocks = GetPreferredSyncWindowBlocks_NoLock(selectedPeer);
                serial = ++_requestSerial;

                _requestsByPeerKey[selectedPeer.SessionKey] = new SyncWindowRequest
                {
                    Generation = _pipelineGeneration,
                    Serial = serial,
                    PeerKey = selectedPeer.SessionKey,
                    Kind = useLocator ? WindowRequestKind.Locator : WindowRequestKind.FromHash,
                    RequestedFromHash = fromHash,
                    RequestedFromHeight = useLocator ? 0UL : _planningCursorHeight,
                    MaxBlocks = maxBlocks,
                    AdvertisedTipChainwork = advertisedTipChainwork
                };

                _planningReady = false;
                NoteProgress_NoLock(useLocator ? "request-locator" : "request-from");
                RefreshState_NoLock();
            }

            try
            {
                if (useLocator)
                {
                    var locator = BlockLocatorBuilder.BuildCanonicalLocator();
                    await SendRequestAsync(
                        selectedPeer,
                        MsgType.GetBlocksByLocator,
                        BlockSyncProtocol.BuildGetBlocksByLocator(locator, maxBlocks),
                        serial,
                        ct).ConfigureAwait(false);
                    _log?.Info("Sync", $"Block sync requesting locator batch from {selectedPeer.RemoteEndpoint}.");
                }
                else
                {
                    await SendRequestAsync(
                        selectedPeer,
                        MsgType.GetBlocksFrom,
                        BlockSyncProtocol.BuildGetBlocksFrom(fromHash, maxBlocks),
                        serial,
                        ct).ConfigureAwait(false);
                    _log?.Info("Sync", $"Block sync continuing from {ShortHash(fromHash)} via {selectedPeer.RemoteEndpoint}.");
                }
            }
            catch (Exception ex)
            {
                await HandlePipelineFailureAsync(
                        selectedPeer,
                        $"request send failed: {ex.Message}",
                        forceLocator: useLocator,
                        penalize: true,
                        ct)
                    .ConfigureAwait(false);
            }
        }

        private async Task SendRequestAsync(PeerSession peer, MsgType type, byte[] payload, int serial, CancellationToken ct)
        {
            await _sendFrameAsync(peer, type, payload, ct).ConfigureAwait(false);

            lock (_gate)
            {
                if (_requestsByPeerKey.TryGetValue(peer.SessionKey, out var request) && request.Serial == serial)
                    ArmTimeout_NoLock(request);
            }
        }

        private async Task TryCommitReadyWindowsAsync(CancellationToken ct)
        {
            lock (_gate)
            {
                if (_commitLoopRunning)
                    return;

                _commitLoopRunning = true;
                RefreshState_NoLock();
            }

            try
            {
                while (true)
                {
                    CompletedWindow? window;
                    lock (_gate)
                    {
                        if (!TryDequeueNextCommitWindow_NoLock(out window))
                        {
                            _commitLoopRunning = false;
                            RefreshState_NoLock();
                            return;
                        }
                    }

                    byte[] expectedPrevHash = SafeCloneHash(window!.PrevHash);
                    ulong expectedHeight = window.StartHeight;

                    for (int offset = 0; offset < window.Blocks.Count; offset += BlockSyncProtocol.BatchMaxBlocks)
                    {
                        int take = Math.Min(BlockSyncProtocol.BatchMaxBlocks, window.Blocks.Count - offset);
                        var slice = window.Blocks.GetRange(offset, take);
                        var commit = await _commitBlocksAsync(
                            slice,
                            expectedHeight,
                            expectedPrevHash,
                            window.Peer,
                            ct).ConfigureAwait(false);

                        if (!commit.Success || commit.LastBlockHash is not { Length: 32 })
                        {
                            lock (_gate)
                            {
                                _commitLoopRunning = false;
                                RefreshState_NoLock();
                            }

                            await HandlePipelineFailureAsync(
                                    window.Peer,
                                    $"commit failed: {commit.Error}",
                                    forceLocator: true,
                                    penalize: true,
                                    ct)
                                .ConfigureAwait(false);
                            return;
                        }

                        expectedPrevHash = SafeCloneHash(commit.LastBlockHash);
                        expectedHeight += (ulong)take;
                    }

                    lock (_gate)
                    {
                        if (window.Generation != _pipelineGeneration)
                        {
                            _commitLoopRunning = false;
                            RefreshState_NoLock();
                            return;
                        }

                        _haveCommittedCursor = true;
                        _committedHash = SafeCloneHash(expectedPrevHash);
                        _committedHeight = expectedHeight - 1UL;
                        _resumeFromHash = SafeCloneHash(_committedHash);
                        NoteProgress_NoLock("window-commit");
                        if (window.ContinueAfterCommit)
                        {
                            _planningCursorHash = SafeCloneHash(_committedHash);
                            _planningCursorHeight = _committedHeight;
                            _havePlanningCursor = _committedHash is { Length: 32 };
                            _planningReady = true;
                            _useLocatorNext = false;
                        }
                        RefreshState_NoLock();
                    }

                    if (_planningReady)
                        await EnsurePipelineAsync(ct).ConfigureAwait(false);

                    if (window.ContinueAfterCommit)
                        await EnsurePipelineAsync(ct).ConfigureAwait(false);

                    if (!window.MoreAvailable)
                    {
                        bool tipReached;
                        lock (_gate)
                        {
                            tipReached = _requestsByPeerKey.Count == 0 && _completedWindowsByStartHeight.Count == 0;
                            if (tipReached)
                                tipReached = ShouldLogRemoteTipReached_NoLock(window.LastHash, window.LastHeight);
                        }

                        if (tipReached)
                            _log?.Info("Sync", "Block sync reached remote tip.");
                    }
                }
            }
            finally
            {
                lock (_gate)
                {
                    if (_requestsByPeerKey.Count == 0 && _completedWindowsByStartHeight.Count == 0)
                    {
                        _commitLoopRunning = false;
                        RefreshState_NoLock();
                    }
                }
            }
        }

        private async Task HandlePipelineFailureAsync(
            PeerSession? peer,
            string reason,
            bool forceLocator,
            bool penalize,
            CancellationToken ct)
        {
            List<PeerSession> activePeers;
            lock (_gate)
            {
                activePeers = ResolveActivePeers_NoLock();
            }

            for (int i = 0; i < activePeers.Count; i++)
                await _abortBatchAsync(activePeers[i], reason, ct).ConfigureAwait(false);

            lock (_gate)
            {
                if (peer != null)
                    _cooldownUntilByPeerKey[peer.SessionKey] = DateTime.UtcNow + PeerCooldown;

                ResetPipeline_NoLock(forceLocator);
                if (_haveCommittedCursor)
                    _resumeFromHash = SafeCloneHash(_committedHash);
                else
                    _resumeFromHash = SafeCloneHash(_getLocalTipHash());
                NoteProgress_NoLock($"reset:{reason}");
            }

            if (peer != null && penalize)
                _penalizePeer?.Invoke(peer, reason);

            _log?.Warn("Sync", $"Block sync reset: {reason}");
            await EnsurePipelineAsync(ct).ConfigureAwait(false);
        }

        private async Task HandlePrepareNoLongerNeededAsync(
            PeerSession peer,
            int serial,
            string reason,
            CancellationToken ct)
        {
            await _abortBatchAsync(peer, reason, ct).ConfigureAwait(false);

            bool removed = false;
            lock (_gate)
            {
                if (_requestsByPeerKey.TryGetValue(peer.SessionKey, out var active) && active.Serial == serial)
                {
                    _requestsByPeerKey.Remove(peer.SessionKey);
                    _tipByPeerKey.Remove(peer.SessionKey);
                    _planningReady = true;
                    _useLocatorNext = true;
                    NoteProgress_NoLock("prepare-no-longer-needed");
                    RefreshState_NoLock();
                    removed = true;
                }
            }

            if (removed)
                _log?.Info("Sync", $"Block sync prepare skipped for {peer.RemoteEndpoint}: {reason}");

            await EnsurePipelineAsync(ct).ConfigureAwait(false);
        }

        private async Task HandleObsoleteBatchStartAsync(
            PeerSession peer,
            int serial,
            string reason,
            CancellationToken ct)
        {
            await _abortBatchAsync(peer, reason, ct).ConfigureAwait(false);

            bool removed = false;
            lock (_gate)
            {
                if (serial != 0 &&
                    _requestsByPeerKey.TryGetValue(peer.SessionKey, out var active) &&
                    active.Serial == serial &&
                    !active.BatchPrepared)
                {
                    _requestsByPeerKey.Remove(peer.SessionKey);
                    _planningReady = true;
                    _useLocatorNext = true;
                    if (_haveCommittedCursor)
                    {
                        _planningCursorHash = SafeCloneHash(_committedHash);
                        _planningCursorHeight = _committedHeight;
                        _havePlanningCursor = _committedHash is { Length: 32 };
                    }
                    else
                    {
                        _planningCursorHash = Array.Empty<byte>();
                        _planningCursorHeight = 0;
                        _havePlanningCursor = false;
                    }

                    NoteProgress_NoLock("obsolete-batch-start");
                    RefreshState_NoLock();
                    removed = true;
                }
            }

            if (removed)
                _log?.Info("Sync", $"Block sync obsolete batch start dropped for {peer.RemoteEndpoint}: {reason}");
            else
                _log?.Info("Sync", $"Block sync stale batch start ignored from {peer.RemoteEndpoint}: {reason}");

            await EnsurePipelineAsync(ct).ConfigureAwait(false);
        }

        private void ResetPipeline_NoLock(bool forceLocator)
        {
            TryRefreshCommittedCursorFromLocalTip_NoLock();
            _requestsByPeerKey.Clear();
            _completedWindowsByStartHeight.Clear();
            _pipelineGeneration++;
            _commitLoopRunning = false;
            _planningReady = true;
            _useLocatorNext = forceLocator;
            _planningCursorHash = _haveCommittedCursor ? SafeCloneHash(_committedHash) : Array.Empty<byte>();
            _planningCursorHeight = _haveCommittedCursor ? _committedHeight : 0UL;
            _havePlanningCursor = _haveCommittedCursor;
            ClearExhaustedContinuation_NoLock();
            _lastRemoteTipLogHash = Array.Empty<byte>();
            _lastRemoteTipLogHeight = 0;
            RefreshState_NoLock();
        }

        private async Task MonitorPlannerLivenessAsync()
        {
            while (!_disposeCts.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(PlannerLivenessPollInterval, _disposeCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                try
                {
                    await RecoverIdleBehindAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    // Best-effort watchdog; never let diagnostic recovery tear down the node.
                }
            }
        }

        private async Task RecoverIdleBehindAsync(CancellationToken ct)
        {
            bool shouldRecover = false;
            string? snapshot = null;

            lock (_gate)
            {
                DateTime now = DateTime.UtcNow;
                UInt128 localChainwork = _getLocalTipChainwork();
                if (!TryGetBestAheadPeer_NoLock(localChainwork, out string? bestPeerKey, out ulong bestHeight, out UInt128 bestChainwork))
                    return;

                TimeSpan idleFor = now - _lastProgressUtc;
                bool idleWithoutWork = _requestsByPeerKey.Count == 0 && _completedWindowsByStartHeight.Count == 0 && !_commitLoopRunning;
                bool activeButStalled = !idleWithoutWork &&
                                        (_requestsByPeerKey.Count > 0 || _completedWindowsByStartHeight.Count > 0 || _commitLoopRunning) &&
                                        idleFor >= ActiveBehindRecoveryThreshold;

                if (!idleWithoutWork && !activeButStalled)
                    return;

                if (idleWithoutWork && idleFor < IdleBehindRecoveryThreshold)
                    return;

                if (_lastPlannerRecoveryLogUtc != DateTime.MinValue &&
                    now - _lastPlannerRecoveryLogUtc < (activeButStalled ? ActiveBehindRecoveryThreshold : IdleBehindRecoveryThreshold))
                {
                    return;
                }

                ResetPipeline_NoLock(forceLocator: true);
                if (_haveCommittedCursor)
                    _resumeFromHash = SafeCloneHash(_committedHash);
                else
                    _resumeFromHash = SafeCloneHash(_getLocalTipHash());

                NoteProgress_NoLock("watchdog-revive");
                _lastPlannerRecoveryLogUtc = now;
                string mode = activeButStalled ? "stalled-sync" : "idle-planner";
                snapshot =
                    $"Block sync watchdog revived {mode}: idleFor={idleFor.TotalSeconds:F1}s " +
                    $"localChainwork={localChainwork} bestPeer={bestPeerKey} bestHeight={bestHeight} " +
                    $"bestChainwork={bestChainwork} requests={_requestsByPeerKey.Count} " +
                    $"completed={_completedWindowsByStartHeight.Count} commitLoop={_commitLoopRunning} " +
                    $"planningReady={_planningReady} useLocatorNext={_useLocatorNext} " +
                    $"haveCommittedCursor={_haveCommittedCursor} committedHeight={_committedHeight} " +
                    $"state={_state} lastProgress={_lastProgressReason}.";
                shouldRecover = true;
            }

            if (snapshot != null)
                _log?.Warn("Sync", snapshot);

            if (shouldRecover)
                await EnsurePipelineAsync(ct).ConfigureAwait(false);
        }

        private void ArmTimeout_NoLock(SyncWindowRequest request)
        {
            var peer = ResolvePeerByKey(request.PeerKey);
            request.TimeoutRevision++;
            request.TimeoutDuration = GetRequestTimeout(peer, request.MaxBlocks);
            _ = MonitorTimeoutAsync(request.PeerKey, request.Serial, request.TimeoutRevision, request.TimeoutDuration);
        }

        private async Task MonitorTimeoutAsync(string peerKey, int serial, int timeoutRevision, TimeSpan timeout)
        {
            try
            {
                await Task.Delay(timeout, _disposeCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            PeerSession? peer = null;
            bool hadOtherWork;
            string reason;
            TimeSpan cooldown;

            lock (_gate)
            {
                if (!_requestsByPeerKey.TryGetValue(peerKey, out var request) || request.Serial != serial)
                    return;

                if (request.TimeoutRevision != timeoutRevision)
                    return;

                peer = ResolvePeerByKey(peerKey);
                hadOtherWork = _requestsByPeerKey.Count > 1 || _completedWindowsByStartHeight.Count > 0 || _commitLoopRunning;

                reason =
                    $"batch timeout: peer={peerKey} serial={serial} kind={request.Kind} prepared={request.BatchPrepared} " +
                    $"batchId={request.BatchId} requested={ShortHash(request.RequestedFromHash)}@{request.RequestedFromHeight} " +
                    $"start={request.StartHeight} received={request.ReceivedBlocks}/{request.ExpectedBlocks} " +
                    $"committed={request.CommittedBlocks} timeout={request.TimeoutDuration.TotalSeconds:F1}s " +
                    $"otherRequests={Math.Max(0, _requestsByPeerKey.Count - 1)} completed={_completedWindowsByStartHeight.Count}";

                _requestsByPeerKey.Remove(peerKey);
                cooldown = RecordTimeoutAndGetCooldown_NoLock(peer, request.MaxBlocks);
                _cooldownUntilByPeerKey[peerKey] = DateTime.UtcNow + cooldown;
                _planningReady = true;
                _useLocatorNext = true;
                if (_haveCommittedCursor)
                {
                    _planningCursorHash = SafeCloneHash(_committedHash);
                    _planningCursorHeight = _committedHeight;
                    _havePlanningCursor = _committedHash is { Length: 32 };
                    _resumeFromHash = SafeCloneHash(_committedHash);
                }
                else
                {
                    _planningCursorHash = Array.Empty<byte>();
                    _planningCursorHeight = 0;
                    _havePlanningCursor = false;
                    _resumeFromHash = SafeCloneHash(_getLocalTipHash());
                }

                if (_requestsByPeerKey.Count == 0 && _completedWindowsByStartHeight.Count == 0)
                    _commitLoopRunning = false;

                NoteProgress_NoLock(hadOtherWork ? "timeout-prune" : "timeout-rearm");
                RefreshState_NoLock();
            }

            if (peer != null)
            {
                await _abortBatchAsync(peer, "batch timeout", CancellationToken.None).ConfigureAwait(false);
                _penalizePeer?.Invoke(peer, "batch timeout");
            }

            _log?.Warn("Sync", hadOtherWork
                ? $"{reason} cooldown={cooldown.TotalSeconds:F0}s -> pruned stalled window and continued planner"
                : $"{reason} cooldown={cooldown.TotalSeconds:F0}s -> rearmed planner");

            await EnsurePipelineAsync(CancellationToken.None).ConfigureAwait(false);
            await TryCommitReadyWindowsAsync(CancellationToken.None).ConfigureAwait(false);
        }

        private PeerSession? SelectBestPeer_NoLock()
        {
            var peers = _sessionSnapshot();
            PeerSession? best = null;
            UInt128 bestChainwork = 0;
            ulong bestHeight = 0;
            bool bestPreferredSync = false;
            int bestLatency = int.MaxValue;
            int bestTimeoutStrikes = int.MaxValue;
            int bestPreferredWindowBlocks = 0;
            int bestCompletedWindows = 0;
            UInt128 localChainwork = _getLocalTipChainwork();
            bool anyPreferredEligible = false;

            foreach (var peer in peers)
            {
                if (peer == null || !peer.HandshakeOk)
                    continue;

                string key = peer.SessionKey;
                if (_requestsByPeerKey.ContainsKey(key))
                    continue;

                if (_cooldownUntilByPeerKey.TryGetValue(key, out var until) && until > DateTime.UtcNow)
                    continue;

                if (!_tipByPeerKey.TryGetValue(key, out var tip))
                    continue;

                if (tip.Chainwork == 0 || tip.Chainwork <= localChainwork)
                    continue;

                if (SupportsPreferredHistoricalSync(peer))
                {
                    anyPreferredEligible = true;
                    break;
                }
            }

            foreach (var peer in peers)
            {
                if (peer == null || !peer.HandshakeOk)
                    continue;

                string key = peer.SessionKey;
                if (_requestsByPeerKey.ContainsKey(key))
                    continue;

                if (_cooldownUntilByPeerKey.TryGetValue(key, out var until) && until > DateTime.UtcNow)
                    continue;

                if (!_tipByPeerKey.TryGetValue(key, out var tip))
                    continue;

                if (tip.Chainwork == 0 || tip.Chainwork <= localChainwork)
                    continue;

                bool preferredSync = SupportsPreferredHistoricalSync(peer);
                if (anyPreferredEligible && !preferredSync)
                    continue;

                var stats = GetOrCreatePeerSyncStats_NoLock(key);
                int latency = peer.LastLatencyMs >= 0 ? peer.LastLatencyMs : int.MaxValue;
                int preferredWindowBlocks = preferredSync
                    ? Math.Max(BlockSyncProtocol.BatchMaxBlocks, Math.Min(BlockSyncProtocol.ExtendedSyncWindowBlocks, stats.PreferredWindowBlocks))
                    : BlockSyncProtocol.BatchMaxBlocks;

                if (best == null ||
                    (preferredSync && !bestPreferredSync) ||
                    (preferredSync == bestPreferredSync && stats.TimeoutStrikes < bestTimeoutStrikes) ||
                    (preferredSync == bestPreferredSync && stats.TimeoutStrikes == bestTimeoutStrikes && tip.Chainwork > bestChainwork) ||
                    (preferredSync == bestPreferredSync && stats.TimeoutStrikes == bestTimeoutStrikes && tip.Chainwork == bestChainwork && tip.Height > bestHeight) ||
                    (preferredSync == bestPreferredSync && stats.TimeoutStrikes == bestTimeoutStrikes && tip.Chainwork == bestChainwork && tip.Height == bestHeight && preferredWindowBlocks > bestPreferredWindowBlocks) ||
                    (preferredSync == bestPreferredSync && stats.TimeoutStrikes == bestTimeoutStrikes && tip.Chainwork == bestChainwork && tip.Height == bestHeight && preferredWindowBlocks == bestPreferredWindowBlocks && stats.CompletedWindows > bestCompletedWindows) ||
                    (preferredSync == bestPreferredSync && stats.TimeoutStrikes == bestTimeoutStrikes && tip.Chainwork == bestChainwork && tip.Height == bestHeight && preferredWindowBlocks == bestPreferredWindowBlocks && stats.CompletedWindows == bestCompletedWindows && latency < bestLatency))
                {
                    best = peer;
                    bestChainwork = tip.Chainwork;
                    bestHeight = tip.Height;
                    bestPreferredSync = preferredSync;
                    bestLatency = latency;
                    bestTimeoutStrikes = stats.TimeoutStrikes;
                    bestPreferredWindowBlocks = preferredWindowBlocks;
                    bestCompletedWindows = stats.CompletedWindows;
                }
            }

            return best;
        }

        private bool TryBuildRequestCommitSlice_NoLock(PeerSession peer, bool flushRemainder, out RequestCommitSlice slice)
        {
            slice = default;
            if (peer == null)
                return false;

            TryRefreshCommittedCursorFromLocalTip_NoLock();

            if (!_requestsByPeerKey.TryGetValue(peer.SessionKey, out var request) || !request.BatchPrepared)
                return false;

            int available = request.ReceivedBlocks - request.CommittedBlocks;
            if (available <= 0)
                return false;

            bool isFirstProgressCommit = request.CommittedBlocks == 0;
            if (isFirstProgressCommit)
            {
                if (_haveCommittedCursor)
                {
                    if (request.StartHeight != _committedHeight + 1UL || !BytesEqual32(request.ForkHash, _committedHash))
                        return false;
                }
                else
                {
                    ulong lowestKnownStart = ulong.MaxValue;
                    foreach (var candidate in _requestsByPeerKey.Values)
                    {
                        if (!candidate.BatchPrepared)
                            continue;

                        if (candidate.StartHeight < lowestKnownStart)
                            lowestKnownStart = candidate.StartHeight;
                    }

                    if (lowestKnownStart != request.StartHeight)
                        return false;
                }
            }
            else
            {
                if (!_haveCommittedCursor ||
                    request.NextCommitHeight != _committedHeight + 1UL ||
                    !BytesEqual32(request.NextCommitPrevHash, _committedHash))
                {
                    return false;
                }
            }

            int take = flushRemainder ? available : Math.Min(BlockSyncProtocol.BatchMaxBlocks, available);
            if (!flushRemainder && take < BlockSyncProtocol.BatchMaxBlocks)
                return false;

            slice = new RequestCommitSlice(
                request.Generation,
                request.Serial,
                request.NextCommitHeight,
                SafeCloneHash(request.NextCommitPrevHash),
                request.Blocks.GetRange(request.CommittedBlocks, take));
            return true;
        }

        private bool TryDequeueNextCommitWindow_NoLock(out CompletedWindow? window)
        {
            window = null;
            TryRefreshCommittedCursorFromLocalTip_NoLock();
            if (_completedWindowsByStartHeight.Count == 0)
                return false;

            using var enumerator = _completedWindowsByStartHeight.GetEnumerator();
            if (!enumerator.MoveNext())
                return false;

            var first = enumerator.Current;
            var candidate = first.Value;

            if (_haveCommittedCursor)
            {
                if (candidate.StartHeight != _committedHeight + 1UL)
                    return false;

                if (!BytesEqual32(candidate.PrevHash, _committedHash))
                    return false;
            }
            else
            {
                ulong lowestKnownStart = candidate.StartHeight;
                foreach (var request in _requestsByPeerKey.Values)
                {
                    if (!request.BatchPrepared)
                        continue;

                    if (request.StartHeight < lowestKnownStart)
                        lowestKnownStart = request.StartHeight;
                }

                if (candidate.StartHeight != lowestKnownStart)
                    return false;
            }

            _completedWindowsByStartHeight.Remove(first.Key);
            window = candidate;
            return true;
        }

        private List<PeerSession> ResolveActivePeers_NoLock()
        {
            var resolved = new List<PeerSession>();
            var peers = _sessionSnapshot();
            foreach (var peer in peers)
            {
                if (peer == null)
                    continue;

                if (_requestsByPeerKey.ContainsKey(peer.SessionKey))
                    resolved.Add(peer);
            }

            return resolved;
        }

        private PeerSession? ResolvePeerByKey(string peerKey)
        {
            var peers = _sessionSnapshot();
            foreach (var peer in peers)
            {
                if (peer != null && string.Equals(peer.SessionKey, peerKey, StringComparison.Ordinal))
                    return peer;
            }

            return null;
        }

        private PeerSyncStats GetOrCreatePeerSyncStats_NoLock(string peerKey)
        {
            if (!_peerSyncStatsByPeerKey.TryGetValue(peerKey, out var stats))
            {
                stats = new PeerSyncStats();
                _peerSyncStatsByPeerKey[peerKey] = stats;
            }

            return stats;
        }

        private bool HasEquivalentActiveContinuationRequest_NoLock(byte[] fromHash, ulong fromHeight)
        {
            if (fromHash is not { Length: 32 })
                return false;

            foreach (var request in _requestsByPeerKey.Values)
            {
                if (request.Kind != WindowRequestKind.FromHash)
                    continue;

                if (request.RequestedFromHeight != fromHeight)
                    continue;

                if (BytesEqual32(request.RequestedFromHash, fromHash))
                    return true;
            }

            return false;
        }

        private bool IsExhaustedContinuation_NoLock(
            byte[] fromHash,
            ulong fromHeight,
            byte[] advertisedTipHash,
            ulong advertisedTipHeight,
            UInt128 advertisedTipChainwork)
        {
            if (!_haveExhaustedContinuation ||
                fromHash is not { Length: 32 } ||
                !BytesEqual32(_exhaustedContinuationFromHash, fromHash) ||
                _exhaustedContinuationFromHeight != fromHeight)
            {
                return false;
            }

            bool sameTip =
                advertisedTipHeight == _exhaustedContinuationTipHeight &&
                advertisedTipChainwork <= _exhaustedContinuationTipChainwork &&
                BytesEqual32(_exhaustedContinuationTipHash, advertisedTipHash);

            if (!sameTip)
            {
                ClearExhaustedContinuation_NoLock();
                return false;
            }

            return true;
        }

        private void MarkExhaustedContinuation_NoLock(SyncWindowRequest request, SmallNetBlocksBatchEndFrame frame)
        {
            if (request.Kind != WindowRequestKind.FromHash ||
                request.RequestedFromHash is not { Length: 32 } ||
                frame.LastHash is not { Length: 32 })
            {
                return;
            }

            _haveExhaustedContinuation = true;
            _exhaustedContinuationFromHash = SafeCloneHash(request.RequestedFromHash);
            _exhaustedContinuationFromHeight = request.RequestedFromHeight;
            _exhaustedContinuationTipHash = SafeCloneHash(frame.LastHash);
            _exhaustedContinuationTipHeight = frame.LastHeight;
            _exhaustedContinuationTipChainwork = request.AdvertisedTipChainwork;
        }

        private void MaybeClearExhaustedContinuation_NoLock(byte[] tipHash, ulong tipHeight, UInt128 tipChainwork)
        {
            if (!_haveExhaustedContinuation)
                return;

            bool sameTip =
                tipHeight == _exhaustedContinuationTipHeight &&
                tipChainwork <= _exhaustedContinuationTipChainwork &&
                BytesEqual32(_exhaustedContinuationTipHash, tipHash);

            if (!sameTip)
                ClearExhaustedContinuation_NoLock();
        }

        private void ClearExhaustedContinuation_NoLock()
        {
            _haveExhaustedContinuation = false;
            _exhaustedContinuationFromHash = Array.Empty<byte>();
            _exhaustedContinuationFromHeight = 0;
            _exhaustedContinuationTipHash = Array.Empty<byte>();
            _exhaustedContinuationTipHeight = 0;
            _exhaustedContinuationTipChainwork = 0;
        }

        private bool ShouldLogRemoteTipReached_NoLock(byte[] tipHash, ulong tipHeight)
        {
            if (_lastRemoteTipLogHeight == tipHeight &&
                BytesEqual32(_lastRemoteTipLogHash, tipHash))
            {
                return false;
            }

            _lastRemoteTipLogHash = SafeCloneHash(tipHash);
            _lastRemoteTipLogHeight = tipHeight;
            return true;
        }

        private static bool SupportsPreferredHistoricalSync(PeerSession? peer)
            => peer != null &&
               peer.Supports(HandshakeCapabilities.BlocksBatchData) &&
               peer.Supports(HandshakeCapabilities.ExtendedSyncWindow) &&
               peer.Supports(HandshakeCapabilities.SyncWindowPreview);

        private int GetMaxParallelWindows_NoLock()
        {
            int preferredAheadPeers = 0;
            UInt128 localChainwork = _getLocalTipChainwork();
            var peers = _sessionSnapshot();

            foreach (var peer in peers)
            {
                if (peer == null || !peer.HandshakeOk)
                    continue;

                if (!SupportsPreferredHistoricalSync(peer))
                    continue;

                string key = peer.SessionKey;
                if (_cooldownUntilByPeerKey.TryGetValue(key, out var until) && until > DateTime.UtcNow)
                    continue;

                if (!_tipByPeerKey.TryGetValue(key, out var tip) || tip.Chainwork <= localChainwork)
                    continue;

                preferredAheadPeers++;
            }

            if (preferredAheadPeers >= 3)
                return PreferredMaxParallelWindows;
            if (preferredAheadPeers >= 2)
                return MediumMaxParallelWindows;

            return BaseMaxParallelWindows;
        }

        private void RecordSuccessfulWindow_NoLock(PeerSession peer, int deliveredBlocks, int requestedBlocks)
        {
            if (peer == null)
                return;

            var stats = GetOrCreatePeerSyncStats_NoLock(peer.SessionKey);
            stats.CompletedWindows++;
            stats.ConsecutiveSuccesses++;
            stats.ConsecutiveTimeouts = 0;
            if (stats.TimeoutStrikes > 0)
                stats.TimeoutStrikes--;
            stats.LastWindowCompletedUtc = DateTime.UtcNow;

            if (!SupportsPreferredHistoricalSync(peer))
            {
                stats.PreferredWindowBlocks = BlockSyncProtocol.BatchMaxBlocks;
                return;
            }

            if (requestedBlocks <= BlockSyncProtocol.BatchMaxBlocks)
            {
                stats.PreferredWindowBlocks = BlockSyncProtocol.BatchMaxBlocks;
            }
            else if (requestedBlocks < BlockSyncProtocol.ExtendedSyncWindowBlocks &&
                     stats.PreferredWindowBlocks < requestedBlocks)
            {
                stats.PreferredWindowBlocks = requestedBlocks;
            }

            if (deliveredBlocks < requestedBlocks)
                return;

            if (stats.PreferredWindowBlocks < 256 && stats.ConsecutiveSuccesses >= 2)
                stats.PreferredWindowBlocks = 256;
            else if (stats.PreferredWindowBlocks < BlockSyncProtocol.ExtendedSyncWindowBlocks && stats.ConsecutiveSuccesses >= 4)
                stats.PreferredWindowBlocks = BlockSyncProtocol.ExtendedSyncWindowBlocks;
        }

        private TimeSpan RecordTimeoutAndGetCooldown_NoLock(PeerSession? peer, int requestedBlocks)
        {
            if (peer == null)
                return TimeoutCooldownShort;

            var stats = GetOrCreatePeerSyncStats_NoLock(peer.SessionKey);
            stats.TimeoutEvents++;
            stats.ConsecutiveTimeouts++;
            stats.ConsecutiveSuccesses = 0;
            stats.LastTimeoutUtc = DateTime.UtcNow;
            stats.TimeoutStrikes = Math.Min(3, Math.Max(stats.TimeoutStrikes + 1, stats.ConsecutiveTimeouts));

            if (requestedBlocks > 256)
                stats.PreferredWindowBlocks = 256;
            else
                stats.PreferredWindowBlocks = BlockSyncProtocol.BatchMaxBlocks;

            return stats.TimeoutStrikes switch
            {
                <= 1 => TimeoutCooldownShort,
                2 => TimeoutCooldownMedium,
                _ => PeerCooldown
            };
        }

        private void PruneCooldown_NoLock()
        {
            if (_cooldownUntilByPeerKey.Count == 0)
                return;

            var now = DateTime.UtcNow;
            var stale = new List<string>();
            foreach (var kv in _cooldownUntilByPeerKey)
            {
                if (kv.Value <= now)
                    stale.Add(kv.Key);
            }

            for (int i = 0; i < stale.Count; i++)
                _cooldownUntilByPeerKey.Remove(stale[i]);
        }

        private bool TryGetBestAheadPeer_NoLock(
            UInt128 localChainwork,
            out string? bestPeerKey,
            out ulong bestHeight,
            out UInt128 bestChainwork)
        {
            bestPeerKey = null;
            bestHeight = 0;
            bestChainwork = 0;

            foreach (var kv in _tipByPeerKey)
            {
                if (kv.Value.Chainwork <= localChainwork)
                    continue;

                if (bestPeerKey == null ||
                    kv.Value.Chainwork > bestChainwork ||
                    (kv.Value.Chainwork == bestChainwork && kv.Value.Height > bestHeight))
                {
                    bestPeerKey = kv.Key;
                    bestHeight = kv.Value.Height;
                    bestChainwork = kv.Value.Chainwork;
                }
            }

            return bestPeerKey != null;
        }

        private byte[] GetResumeHash_NoLock()
        {
            TryRefreshCommittedCursorFromLocalTip_NoLock();
            var localTipHash = SafeCloneHash(_getLocalTipHash());
            if (localTipHash is { Length: 32 })
            {
                if (!_haveCommittedCursor || _committedHash is not { Length: 32 } || !BytesEqual32(_committedHash, localTipHash))
                {
                    _resumeFromHash = SafeCloneHash(localTipHash);
                    return localTipHash;
                }

                return SafeCloneHash(_committedHash);
            }

            if (_haveCommittedCursor && _committedHash is { Length: 32 })
                return SafeCloneHash(_committedHash);

            if (_resumeFromHash is { Length: 32 })
                return SafeCloneHash(_resumeFromHash);

            return Array.Empty<byte>();
        }

        private void TryRefreshCommittedCursorFromLocalTip_NoLock()
        {
            ulong localTipHeight = _getLocalTipHeight();
            if (localTipHeight == 0)
                return;

            var localTipHash = SafeCloneHash(_getLocalTipHash());
            if (localTipHash is not { Length: 32 })
                return;

            if (_haveCommittedCursor &&
                _committedHash is { Length: 32 } &&
                _committedHeight == localTipHeight &&
                BytesEqual32(_committedHash, localTipHash))
            {
                return;
            }

            _haveCommittedCursor = true;
            _committedHash = SafeCloneHash(localTipHash);
            _committedHeight = localTipHeight;
            _resumeFromHash = SafeCloneHash(localTipHash);
        }

        private int GetPreferredSyncWindowBlocks_NoLock(PeerSession peer)
        {
            if (peer == null)
                return BlockSyncProtocol.BatchMaxBlocks;

            if (!SupportsPreferredHistoricalSync(peer))
                return BlockSyncProtocol.BatchMaxBlocks;

            var stats = GetOrCreatePeerSyncStats_NoLock(peer.SessionKey);
            int preferred = stats.PreferredWindowBlocks;
            if (preferred < BlockSyncProtocol.BatchMaxBlocks)
                preferred = BlockSyncProtocol.BatchMaxBlocks;
            if (preferred > BlockSyncProtocol.ExtendedSyncWindowBlocks)
                preferred = BlockSyncProtocol.ExtendedSyncWindowBlocks;

            return preferred;
        }

        private static TimeSpan GetRequestTimeout(PeerSession? peer, int maxBlocks)
        {
            double seconds = BatchResponseTimeout.TotalSeconds;

            if (maxBlocks > BlockSyncProtocol.BatchMaxBlocks)
            {
                double extraWindowSeconds =
                    4.0 * (maxBlocks - BlockSyncProtocol.BatchMaxBlocks) /
                    (BlockSyncProtocol.ExtendedSyncWindowBlocks - BlockSyncProtocol.BatchMaxBlocks);
                seconds += Math.Max(2, extraWindowSeconds);
            }

            int latencyMs = peer?.LastLatencyMs ?? -1;
            if (latencyMs > 0)
                seconds += Math.Min(12, Math.Ceiling(latencyMs / 500.0));

            if (seconds < 20)
                seconds = 20;
            if (seconds > 45)
                seconds = 45;

            return TimeSpan.FromSeconds(seconds);
        }

        private static bool TryValidatePreview(
            SmallNetBlocksBatchStartFrame frame,
            out byte[] previewHash,
            out ulong previewHeight)
        {
            previewHash = Array.Empty<byte>();
            previewHeight = 0;

            if (frame.PreviewLastHash is not { Length: 32 })
                return false;

            ulong expectedHeight = frame.ForkHeight + (ulong)frame.TotalBlocks;
            if (frame.PreviewLastHeight != expectedHeight)
                return false;

            if (frame.TotalBlocks == 0 && !BytesEqual32(frame.PreviewLastHash, frame.ForkHash))
                return false;

            previewHash = SafeCloneHash(frame.PreviewLastHash);
            previewHeight = frame.PreviewLastHeight;
            return true;
        }

        private void RefreshState_NoLock()
        {
            if (_requestsByPeerKey.Count == 0 && _completedWindowsByStartHeight.Count == 0 && !_commitLoopRunning)
            {
                _state = BlockSyncState.Idle;
                return;
            }

            foreach (var request in _requestsByPeerKey.Values)
            {
                if (request.BatchPrepared || request.ReceivedBlocks > 0)
                {
                    _state = BlockSyncState.ReceivingBatch;
                    return;
                }
            }

            if (_completedWindowsByStartHeight.Count > 0 || _commitLoopRunning)
            {
                _state = BlockSyncState.ReceivingBatch;
                return;
            }

            _state = BlockSyncState.AwaitingBatchBegin;
        }

        private static byte[] SafeCloneHash(byte[]? hash)
        {
            if (hash is not { Length: 32 })
                return Array.Empty<byte>();
            return (byte[])hash.Clone();
        }

        private static bool IsNoLongerNeededPrepareFailure(string? error)
            => !string.IsNullOrWhiteSpace(error) &&
               error.StartsWith(PeerTipNotAheadErrorPrefix, StringComparison.Ordinal);

        private static bool BytesEqual32(byte[]? left, byte[]? right)
        {
            if (left is not { Length: 32 } || right is not { Length: 32 })
                return false;

            int diff = 0;
            for (int i = 0; i < 32; i++)
                diff |= left[i] ^ right[i];
            return diff == 0;
        }

        private void NoteProgress_NoLock(string reason)
        {
            _lastProgressUtc = DateTime.UtcNow;
            _lastProgressReason = reason ?? string.Empty;
        }

        private static string ShortHash(byte[] hash)
        {
            if (hash is not { Length: 32 })
                return "unknown";

            var hex = Convert.ToHexString(hash).ToLowerInvariant();
            return hex[..16];
        }
    }
}
