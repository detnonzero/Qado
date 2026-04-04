using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Qado.Blockchain;
using Qado.Logging;
using Qado.Mempool;
using Qado.Serialization;
using Qado.Storage;

namespace Qado.Networking
{
    public sealed class BlockIngressFlow
    {
        private static readonly TimeSpan TooFarBehindAdmissionLogWindow = TimeSpan.FromSeconds(15);
        private static readonly TimeSpan BufferedOrphanLogWindow = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan TooFarBehindLiveRejectTtl = TimeSpan.FromSeconds(90);
        private static readonly TimeSpan MissingParentResyncTtl = TimeSpan.FromSeconds(60);
        private const int MaxTooFarBehindLiveRejectEntries = 20_000;
        private const int MaxMissingParentResyncEntries = 20_000;
        private const ulong LiveNearTipWindow = 8UL;

        public delegate bool TryStoreOrphanDelegate(
            byte[] payload,
            Block block,
            PeerSession peer,
            string peerKey,
            BlockIngressKind ingress,
            out string reason);

        public delegate bool SidechainAdmissionDelegate(
            Block block,
            byte[]? canonicalTipHash,
            ulong prevHeight,
            int payloadBytes,
            out string reason);

        private readonly BlockDownloadManager _blockDownloadManager;
        private readonly BlockSyncClient _blockSyncClient;
        private readonly MempoolManager _mempool;
        private readonly Func<PeerSession, string> _getPeerKey;
        private readonly Func<string, bool> _allowInboundBlock;
        private readonly Func<bool> _shouldRequestMissingHeaderResync;
        private readonly Action<string> _requestSyncNow;
        private readonly TryStoreOrphanDelegate _tryStoreOrphan;
        private readonly Action<PeerSession, byte[], string> _requestParentFromPeer;
        private readonly Action<byte[], BadChainReason, string> _markStoredBlockInvalid;
        private readonly SidechainAdmissionDelegate _passesSidechainAdmission;
        private readonly Action<byte[]> _logKnownBlockAlready;
        private readonly Action _notifyUiAfterAcceptedBlock;
        private readonly Func<Block, PeerSession, CancellationToken, Task> _relayValidatedBlockAsync;
        private readonly Func<byte[], PeerSession, CancellationToken, Task> _promoteOrphansForParentAsync;
        private readonly ILogSink? _log;
        private readonly object _admissionLogGate = new();
        private readonly object _tooFarBehindLiveRejectGate = new();
        private readonly Dictionary<string, DateTime> _tooFarBehindLiveRejectUntilUtc = new(StringComparer.Ordinal);
        private readonly object _missingParentResyncGate = new();
        private readonly Dictionary<string, DateTime> _missingParentResyncUntilUtc = new(StringComparer.Ordinal);
        private readonly object _bufferedOrphanLogGate = new();
        private readonly Dictionary<string, SuppressedInfoLogState> _bufferedOrphanLogsByCategory = new(StringComparer.Ordinal);
        private DateTime _lastTooFarBehindAdmissionLogUtc = DateTime.MinValue;
        private int _tooFarBehindAdmissionSuppressed;
        private string _lastTooFarBehindAdmissionReason = string.Empty;

        private sealed class SuppressedInfoLogState
        {
            public DateTime LastLogUtc = DateTime.MinValue;
            public int Suppressed;
            public string LastMessage = string.Empty;
        }

        public BlockIngressFlow(
            BlockDownloadManager blockDownloadManager,
            BlockSyncClient blockSyncClient,
            MempoolManager mempool,
            Func<PeerSession, string> getPeerKey,
            Func<string, bool> allowInboundBlock,
            Func<bool> shouldRequestMissingHeaderResync,
            Action<string> requestSyncNow,
            TryStoreOrphanDelegate tryStoreOrphan,
            Action<PeerSession, byte[], string> requestParentFromPeer,
            Action<byte[], BadChainReason, string> markStoredBlockInvalid,
            SidechainAdmissionDelegate passesSidechainAdmission,
            Action<byte[]> logKnownBlockAlready,
            Action notifyUiAfterAcceptedBlock,
            Func<Block, PeerSession, CancellationToken, Task> relayValidatedBlockAsync,
            Func<byte[], PeerSession, CancellationToken, Task> promoteOrphansForParentAsync,
            ILogSink? log = null)
        {
            _blockDownloadManager = blockDownloadManager ?? throw new ArgumentNullException(nameof(blockDownloadManager));
            _blockSyncClient = blockSyncClient ?? throw new ArgumentNullException(nameof(blockSyncClient));
            _mempool = mempool ?? throw new ArgumentNullException(nameof(mempool));
            _getPeerKey = getPeerKey ?? throw new ArgumentNullException(nameof(getPeerKey));
            _allowInboundBlock = allowInboundBlock ?? throw new ArgumentNullException(nameof(allowInboundBlock));
            _shouldRequestMissingHeaderResync = shouldRequestMissingHeaderResync ?? throw new ArgumentNullException(nameof(shouldRequestMissingHeaderResync));
            _requestSyncNow = requestSyncNow ?? throw new ArgumentNullException(nameof(requestSyncNow));
            _tryStoreOrphan = tryStoreOrphan ?? throw new ArgumentNullException(nameof(tryStoreOrphan));
            _requestParentFromPeer = requestParentFromPeer ?? throw new ArgumentNullException(nameof(requestParentFromPeer));
            _markStoredBlockInvalid = markStoredBlockInvalid ?? throw new ArgumentNullException(nameof(markStoredBlockInvalid));
            _passesSidechainAdmission = passesSidechainAdmission ?? throw new ArgumentNullException(nameof(passesSidechainAdmission));
            _logKnownBlockAlready = logKnownBlockAlready ?? throw new ArgumentNullException(nameof(logKnownBlockAlready));
            _notifyUiAfterAcceptedBlock = notifyUiAfterAcceptedBlock ?? throw new ArgumentNullException(nameof(notifyUiAfterAcceptedBlock));
            _relayValidatedBlockAsync = relayValidatedBlockAsync ?? throw new ArgumentNullException(nameof(relayValidatedBlockAsync));
            _promoteOrphansForParentAsync = promoteOrphansForParentAsync ?? throw new ArgumentNullException(nameof(promoteOrphansForParentAsync));
            _log = log;
        }

        public async Task HandleBlockAsync(
            byte[] payload,
            PeerSession peer,
            CancellationToken ct,
            BlockIngressKind ingress,
            bool? enforceRateLimitOverride)
        {
            if (ingress == BlockIngressKind.LivePush)
            {
                await HandleLiveBlockAsync(payload, peer, ct, ingress, enforceRateLimitOverride).ConfigureAwait(false);
                return;
            }

            await HandleSyncOrRequestedBlockAsync(payload, peer, ct, ingress, enforceRateLimitOverride).ConfigureAwait(false);
        }

        private async Task HandleLiveBlockAsync(
            byte[] payload,
            PeerSession peer,
            CancellationToken ct,
            BlockIngressKind ingress,
            bool? enforceRateLimitOverride)
        {
            if (TryHandleLiveOutOfPlanGate(payload, peer, ingress, enforceRateLimitOverride))
                return;

            await HandleSyncOrRequestedBlockAsync(payload, peer, ct, ingress, enforceRateLimitOverride).ConfigureAwait(false);
        }

        private bool TryHandleLiveOutOfPlanGate(
            byte[] payload,
            PeerSession peer,
            BlockIngressKind ingress,
            bool? enforceRateLimitOverride)
        {
            if (enforceRateLimitOverride.HasValue)
                return false;

            string peerKey = _getPeerKey(peer);

            Block block;
            try
            {
                block = BlockBinarySerializer.Read(payload);
            }
            catch (Exception ex)
            {
                PeerFailTracker.ReportFailure(peerKey);
                _log?.Warn("P2P", $"Block decode failed: {ex.Message}");
                return true;
            }

            var prevHash = block.Header?.PreviousBlockHash;
            if (prevHash is not { Length: 32 })
            {
                PeerFailTracker.ReportFailure(peerKey);
                _log?.Warn("P2P", "Block rejected: missing/invalid PrevHash.");
                return true;
            }

            if (block.BlockHash is not { Length: 32 } || IsZero32(block.BlockHash))
                block.BlockHash = block.ComputeBlockHash();

            bool headerKnown = false;
            bool prevKnown = false;
            bool prevIsTip = false;
            ulong tipHeight = 0;
            ulong prevHeight = 0;

            try
            {
                headerKnown = BlockIndexStore.ContainsHash(block.BlockHash!);
                prevKnown = BlockIndexStore.ContainsHash(prevHash);
                if (prevKnown)
                    _ = BlockIndexStore.TryGetMeta(prevHash, out prevHeight, out _, out _);

                tipHeight = BlockStore.GetLatestHeight();
                var tipHash = BlockStore.GetCanonicalHashAtHeight(tipHeight);
                prevIsTip = tipHash is { Length: 32 } && BytesEqual32(prevHash, tipHash);
            }
            catch
            {
            }

            if (!_blockSyncClient.IsActive &&
                !headerKnown &&
                _shouldRequestMissingHeaderResync() &&
                ShouldRequestMissingParentResync(prevHash))
                _requestSyncNow("block-out-of-plan-missing-block");

            ulong candidateHeight = block.BlockHeight;
            if (prevKnown)
                candidateHeight = prevHeight + 1UL;

            bool nearTip = IsNearTipCandidate(candidateHeight, tipHeight);
            bool armedOutOfPlanFallback =
                _blockDownloadManager.IsOutOfPlanFallbackHash(block.BlockHash!) ||
                _blockDownloadManager.IsOutOfPlanFallbackHash(prevHash);
            bool allowOutOfPlan = prevIsTip ||
                                  (prevKnown && nearTip) ||
                                  armedOutOfPlanFallback ||
                                  (_blockSyncClient.IsActive && nearTip && headerKnown);
            if (allowOutOfPlan)
                return false;

            if (!headerKnown && !prevKnown)
            {
                if (!armedOutOfPlanFallback && !_blockSyncClient.IsActive)
                {
                    _requestParentFromPeer(peer, prevHash, "live-orphan-parent");
                    _ = _blockDownloadManager.QueueOutOfPlanFallback(prevHash, "live-orphan-parent-fallback");
                    if (_shouldRequestMissingHeaderResync() &&
                        ShouldRequestMissingParentResync(prevHash))
                    {
                        _requestSyncNow("live-orphan-parent-missing");
                    }

                    _log?.Info(
                        "P2P",
                        $"Live orphan deferred until recovery is armed: hash={Hex16(block.BlockHash!)}");
                    return true;
                }

                if (!nearTip && !armedOutOfPlanFallback)
                {
                    _log?.Info(
                        "P2P",
                        $"Live orphan ignored outside near-tip window: hash={Hex16(block.BlockHash!)} claimedHeight={FormatHeightForLog(block.BlockHeight)} tip={tipHeight.ToString(CultureInfo.InvariantCulture)}");
                    return true;
                }

                TryLooseValidateAndBufferOrphanPayload(
                    payload,
                    block,
                    prevHash,
                    peer,
                    peerKey,
                    ingress,
                    rejectPrefix: "Live block rejected before orphan buffering",
                    invalidContextPrefix: "live-orphan",
                    bufferedPrefix: "Live orphan buffered",
                    droppedPrefix: "Live orphan dropped",
                    parentRequestSource: "live-orphan-parent",
                    fallbackSource: "live-orphan-parent-fallback",
                    syncReason: "live-orphan-parent-missing");
                return true;
            }

            if (prevKnown && !nearTip)
            {
                RememberTooFarBehindLiveReject(block.BlockHash!);
                _ = _blockDownloadManager.QueueOutOfPlanFallback(prevHash, "live-side-too-far-parent-fallback");
                if (!_blockSyncClient.IsActive &&
                    _shouldRequestMissingHeaderResync() &&
                    ShouldRequestMissingParentResync(prevHash))
                {
                    _requestSyncNow("live-side-too-far-parent");
                }

                _log?.Info(
                    "P2P",
                    $"Live sidechain deferred to request/response path: hash={Hex16(block.BlockHash!)} height={FormatHeightForLog(candidateHeight)} tip={tipHeight.ToString(CultureInfo.InvariantCulture)}");
                return true;
            }

            _log?.Info(
                "P2P",
                $"Live block ignored outside sync plan: hash={Hex16(block.BlockHash!)} ingress={ingress} prevKnown={prevKnown} prevIsTip={prevIsTip} nearTip={nearTip} headerKnown={headerKnown} fallback={armedOutOfPlanFallback} syncActive={_blockSyncClient.IsActive}");
            return true;
        }

        private bool TryLooseValidateAndBufferOrphanPayload(
            byte[] payload,
            Block block,
            byte[] prevHash,
            PeerSession peer,
            string peerKey,
            BlockIngressKind ingress,
            string rejectPrefix,
            string invalidContextPrefix,
            string bufferedPrefix,
            string droppedPrefix,
            string parentRequestSource,
            string fallbackSource,
            string syncReason)
        {
            if (!BlockValidator.ValidateNetworkOrphanBlockStateless(block, out var reason))
            {
                PeerFailTracker.ReportFailure(peerKey);
                _log?.Warn("P2P", $"{rejectPrefix}: {reason}");
                _markStoredBlockInvalid(
                    block.BlockHash!,
                    BadChainReason.BlockStatelessInvalid,
                    $"{invalidContextPrefix}:{reason}");
                _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                return true;
            }

            BufferOrphanAndRequestParent(
                payload,
                block,
                prevHash,
                peer,
                peerKey,
                ingress,
                bufferedPrefix,
                droppedPrefix,
                parentRequestSource,
                fallbackSource,
                syncReason);
            return true;
        }

        private void BufferOrphanAndRequestParent(
            byte[] payload,
            Block block,
            byte[] prevHash,
            PeerSession peer,
            string peerKey,
            BlockIngressKind ingress,
            string bufferedPrefix,
            string droppedPrefix,
            string parentRequestSource,
            string fallbackSource,
            string syncReason)
        {
            if (_tryStoreOrphan(payload, block, peer, peerKey, ingress, out var orphanReason))
            {
                if (IsAlreadyBufferedReason(orphanReason))
                    return;

                LogBufferedOrphan(
                    bufferedPrefix,
                    $"{bufferedPrefix}: h={FormatHeightForLog(block.BlockHeight)} {Hex16(block.BlockHash!)}");
                ScheduleMissingParentRecovery(peer, prevHash, ingress, parentRequestSource, fallbackSource, syncReason);
            }
            else
            {
                if (!IsBenignOrphanDropReason(orphanReason))
                    _log?.Warn("P2P", $"{droppedPrefix}: {orphanReason}");
                _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
            }
        }

        private async Task HandleSyncOrRequestedBlockAsync(
            byte[] payload,
            PeerSession peer,
            CancellationToken ct,
            BlockIngressKind ingress,
            bool? enforceRateLimitOverride)
        {
            string peerKey = _getPeerKey(peer);

            Block block;
            try
            {
                block = BlockBinarySerializer.Read(payload);
            }
            catch (Exception ex)
            {
                PeerFailTracker.ReportFailure(peerKey);
                _log?.Warn("P2P", $"Block decode failed: {ex.Message}");
                return;
            }

            var prevHash = block.Header?.PreviousBlockHash;
            if (prevHash is not { Length: 32 })
            {
                PeerFailTracker.ReportFailure(peerKey);
                _log?.Warn("P2P", "Block rejected: missing/invalid PrevHash.");
                return;
            }

            if (block.BlockHash is not { Length: 32 } || IsZero32(block.BlockHash))
                block.BlockHash = block.ComputeBlockHash();

            var arrivalKind = _blockDownloadManager.MarkBlockArrived(peer, block.BlockHash!);
            bool enforceRateLimit = enforceRateLimitOverride ??
                                    arrivalKind != BlockArrivalKind.Requested;

            if (ingress == BlockIngressKind.LivePush &&
                arrivalKind != BlockArrivalKind.Requested &&
                ShouldDropCachedTooFarBehindLiveBranch(block.BlockHash!, prevHash))
            {
                _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                return;
            }

            bool replayedInternally = enforceRateLimitOverride.HasValue;
            if (ingress != BlockIngressKind.LivePush &&
                !replayedInternally &&
                arrivalKind != BlockArrivalKind.Requested)
            {
                bool headerKnown = false;
                try { headerKnown = BlockIndexStore.ContainsHash(block.BlockHash!); } catch { }

                bool outOfPlanPrevKnown = false;
                bool prevIsTip = false;
                ulong tipHeight = 0;
                ulong prevHeightKnown = 0;
                try
                {
                    outOfPlanPrevKnown = BlockIndexStore.ContainsHash(prevHash);
                    if (outOfPlanPrevKnown)
                        _ = BlockIndexStore.TryGetMeta(prevHash, out prevHeightKnown, out _, out _);

                    tipHeight = BlockStore.GetLatestHeight();
                    var tipHash = BlockStore.GetCanonicalHashAtHeight(tipHeight);
                    prevIsTip = tipHash is { Length: 32 } && BytesEqual32(prevHash, tipHash);
                }
                catch
                {
                }

                if (!_blockSyncClient.IsActive &&
                    !headerKnown &&
                    _shouldRequestMissingHeaderResync() &&
                    ShouldRequestMissingParentResync(prevHash))
                    _requestSyncNow("block-out-of-plan-missing-block");

                ulong candidateHeight = block.BlockHeight;
                if (outOfPlanPrevKnown && prevHeightKnown < ulong.MaxValue)
                    candidateHeight = prevHeightKnown + 1UL;

                bool nearTip = candidateHeight >= tipHeight || (tipHeight - candidateHeight) <= 32UL;
                bool armedOutOfPlanFallback =
                    _blockDownloadManager.IsOutOfPlanFallbackHash(block.BlockHash!) ||
                    _blockDownloadManager.IsOutOfPlanFallbackHash(prevHash);
                bool allowOutOfPlan = prevIsTip ||
                                      outOfPlanPrevKnown ||
                                      armedOutOfPlanFallback ||
                                      (_blockSyncClient.IsActive && nearTip && headerKnown);
                if (!allowOutOfPlan)
                {
                    if (!headerKnown && !outOfPlanPrevKnown)
                    {
                        if (!BlockValidator.ValidateNetworkOrphanBlockStateless(block, out var liveLooseReason))
                        {
                            PeerFailTracker.ReportFailure(peerKey);
                            _log?.Warn("P2P", $"Out-of-plan block rejected before orphan buffering: {liveLooseReason}");
                            _markStoredBlockInvalid(block.BlockHash!, BadChainReason.BlockStatelessInvalid, $"out-of-plan:{liveLooseReason}");
                            _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                            return;
                        }

                        if (_tryStoreOrphan(payload, block, peer, peerKey, ingress, out var orphanReason))
                        {
                            if (IsAlreadyBufferedReason(orphanReason))
                                return;

                            LogBufferedOrphan(
                                "Out-of-plan orphan buffered",
                                $"Out-of-plan orphan buffered: h={FormatHeightForLog(block.BlockHeight)} {Hex16(block.BlockHash!)}");
                            ScheduleMissingParentRecovery(
                                peer,
                                prevHash,
                                ingress,
                                "out-of-plan-parent",
                                "out-of-plan-parent-fallback",
                                "out-of-plan-parent-missing");
                        }
                        else
                        {
                            if (!IsBenignOrphanDropReason(orphanReason))
                                _log?.Warn("P2P", $"Out-of-plan orphan dropped: {orphanReason}");
                            _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                        }

                        return;
                    }

                    _log?.Info(
                        "P2P",
                        $"Live block ignored outside sync plan: hash={Hex16(block.BlockHash!)} ingress={ingress} prevKnown={outOfPlanPrevKnown} prevIsTip={prevIsTip} nearTip={nearTip} headerKnown={headerKnown} fallback={armedOutOfPlanFallback} syncActive={_blockSyncClient.IsActive}");
                    return;
                }
            }

            if (enforceRateLimit && !_allowInboundBlock(peerKey))
            {
                _log?.Warn("P2P", $"Inbound block rate limit exceeded for {peerKey}.");
                _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                return;
            }

            if (BlockIndexStore.IsBadOrHasBadAncestor(block.BlockHash!))
            {
                _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                return;
            }

            if (BlockIndexStore.IsBadOrHasBadAncestor(prevHash))
            {
                _log?.Warn("P2P", $"Dropping block with bad parent: {Hex16(block.BlockHash!)}");
                _blockSyncClient.RequestResync("parent-bad");
                _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                return;
            }

            bool alreadyHavePayload = BlockIndexStore.HasPayload(block.BlockHash!);
            if (alreadyHavePayload &&
                BlockIndexStore.TryGetBadFlags(block.BlockHash!, out bool isBad, out bool badAncestor, out _, null) &&
                (isBad || badAncestor))
            {
                _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                return;
            }

            if (alreadyHavePayload && BlockIndexStore.TryGetStatus(block.BlockHash!, out int existingStatus))
            {
                if (BlockIndexStore.IsValidatedStatus(existingStatus))
                {
                    _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: true);
                    _logKnownBlockAlready(block.BlockHash!);
                    return;
                }

                if (existingStatus == BlockIndexStore.StatusSideStateInvalid)
                {
                    _blockSyncClient.RequestResync("known-invalid");
                    _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                    return;
                }
            }

            bool isTipExtending;
            ulong tipHeightSnapshot;
            byte[]? tipHashSnapshot;
            bool prevKnown;
            ulong prevHeight = 0;

            lock (Db.Sync)
            {
                tipHeightSnapshot = BlockStore.GetLatestHeight();
                tipHashSnapshot = BlockStore.GetCanonicalHashAtHeight(tipHeightSnapshot);
                isTipExtending = tipHashSnapshot is { Length: 32 } && BytesEqual32(prevHash, tipHashSnapshot);
                prevKnown = BlockIndexStore.TryGetMeta(prevHash, out prevHeight, out _, out _);
            }

            if (isTipExtending)
            {
                block.BlockHeight = tipHeightSnapshot + 1UL;
            }
            else if (prevKnown)
            {
                block.BlockHeight = prevHeight + 1UL;
            }
            else
            {
                if (!BlockValidator.ValidateNetworkOrphanBlockStateless(block, out var looseReason))
                {
                    PeerFailTracker.ReportFailure(peerKey);
                    _log?.Warn("P2P", $"Out-of-order block rejected: {looseReason}");
                    _markStoredBlockInvalid(block.BlockHash!, BadChainReason.BlockStatelessInvalid, $"out-of-order:{looseReason}");
                    _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                    return;
                }

                if (_tryStoreOrphan(payload, block, peer, peerKey, ingress, out var orphanReason))
                {
                    if (IsAlreadyBufferedReason(orphanReason))
                        return;

                    LogBufferedOrphan(
                        "Orphan buffered",
                        $"Orphan buffered: h={FormatHeightForLog(block.BlockHeight)} {Hex16(block.BlockHash!)}");
                    ScheduleMissingParentRecovery(
                        peer,
                        prevHash,
                        ingress,
                        "orphan-parent",
                        "orphan-parent-fallback",
                        "orphan-parent-missing");
                }
                else
                {
                    if (!IsBenignOrphanDropReason(orphanReason))
                        _log?.Warn("P2P", $"Orphan dropped: {orphanReason}");
                    _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                }

                return;
            }

            bool liveUnrequestedSideCandidate =
                ingress == BlockIngressKind.LivePush &&
                arrivalKind != BlockArrivalKind.Requested &&
                !isTipExtending;
            if (liveUnrequestedSideCandidate &&
                !IsNearTipCandidate(block.BlockHeight, tipHeightSnapshot))
            {
                RememberTooFarBehindLiveReject(block.BlockHash!);
                _ = _blockDownloadManager.QueueOutOfPlanFallback(prevHash, "live-side-too-far-parent-fallback");
                if (!_blockSyncClient.IsActive &&
                    _shouldRequestMissingHeaderResync() &&
                    ShouldRequestMissingParentResync(prevHash))
                {
                    _requestSyncNow("live-side-too-far-parent");
                }

                _log?.Info(
                    "P2P",
                    $"Live sidechain deferred to request/response path: hash={Hex16(block.BlockHash!)} height={FormatHeightForLog(block.BlockHeight)} tip={tipHeightSnapshot.ToString(CultureInfo.InvariantCulture)}");
                _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                return;
            }

            bool persistedRaw = alreadyHavePayload;
            bool validateAsSidechain = !isTipExtending;
            if (isTipExtending)
            {
                if (!BlockValidator.ValidateNetworkTipBlock(block, out var reason))
                {
                    if (IsTipMovedReason(reason))
                    {
                        validateAsSidechain = true;
                        _log?.Info("P2P", $"Tip moved during validation; retrying as sidechain: {Hex16(block.BlockHash!)}");
                    }
                    else
                    {
                        PeerFailTracker.ReportFailure(peerKey);
                        _log?.Warn("P2P", $"Block rejected (tip-ext): {reason}");
                        if (persistedRaw)
                        {
                            _markStoredBlockInvalid(
                                block.BlockHash!,
                                BadChainReason.BlockStateInvalid,
                                $"tip-invalid:{reason}");
                        }

                        _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                        return;
                    }
                }
            }

            if (validateAsSidechain)
            {
                if (!BlockValidator.ValidateNetworkSideBlockStateless(block, out var reason))
                {
                    if (IsPreviousBlockMissingReason(reason))
                    {
                        if (_tryStoreOrphan(payload, block, peer, peerKey, ingress, out var orphanReason))
                        {
                            if (IsAlreadyBufferedReason(orphanReason))
                                return;

                            _log?.Info("P2P", $"Sidechain deferred (parent missing): h={FormatHeightForLog(block.BlockHeight)} {Hex16(block.BlockHash!)}");
                            ScheduleMissingParentRecovery(
                                peer,
                                prevHash,
                                ingress,
                                "sidechain-parent",
                                "sidechain-parent-fallback",
                                "sidechain-parent-missing");
                        }
                        else
                        {
                            if (!IsBenignOrphanDropReason(orphanReason))
                                _log?.Warn("P2P", $"Sidechain defer dropped: {orphanReason}");
                            _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                        }

                        return;
                    }

                    PeerFailTracker.ReportFailure(peerKey);
                    _log?.Warn("P2P", $"Block rejected (sidechain): {reason}");
                    if (persistedRaw)
                    {
                        _markStoredBlockInvalid(
                            block.BlockHash!,
                            BadChainReason.BlockStatelessInvalid,
                            $"side-invalid:{reason}");
                    }

                    _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                    return;
                }

                bool bypassAdmission =
                    ingress == BlockIngressKind.LivePush &&
                    prevKnown &&
                    tipHeightSnapshot >= prevHeight &&
                    (tipHeightSnapshot - prevHeight) <= 8UL;
                if (!bypassAdmission && !_passesSidechainAdmission(
                        block,
                        canonicalTipHash: tipHashSnapshot,
                        prevHeight: prevHeight,
                        payloadBytes: payload.Length,
                        out var admissionReason))
                {
                    if (ingress == BlockIngressKind.LivePush &&
                        arrivalKind != BlockArrivalKind.Requested &&
                        IsTooFarBehindAdmissionReason(admissionReason))
                    {
                        RememberTooFarBehindLiveReject(block.BlockHash!);
                    }

                    LogSidechainAdmissionReject(admissionReason);
                    _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                    return;
                }
            }

            bool extendedCanon = false;
            bool stateValidated = false;
            try
            {
                lock (Db.Sync)
                {
                    using var tx = Db.Connection!.BeginTransaction();

                    if (!alreadyHavePayload)
                    {
                        BlockStore.SaveBlock(
                            block,
                            tx,
                            validateAsSidechain
                                ? BlockIndexStore.StatusSideStatelessAccepted
                                : BlockIndexStore.StatusHaveBlockPayload);
                        persistedRaw = true;
                    }

                    if (TryExtendCanonTipNoReorg(block, tx, out _))
                    {
                        extendedCanon = true;
                        BlockIndexStore.SetStatus(block.BlockHash!, BlockIndexStore.StatusCanonicalStateValidated, tx);
                    }
                    else
                    {
                        BlockIndexStore.SetStatus(block.BlockHash!, BlockIndexStore.StatusSideStatelessAccepted, tx);
                    }

                    tx.Commit();
                }
            }
            catch (Exception ex)
            {
                _log?.Warn("P2P", $"Block store failed: {ex.Message}");
                _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                return;
            }

            if (!extendedCanon)
            {
                try
                {
                    bool shouldAttemptAdopt = false;
                    ulong canonTipNow = BlockStore.GetLatestHeight();
                    shouldAttemptAdopt = block.BlockHeight >= canonTipNow;
                    if (!shouldAttemptAdopt && canonTipNow > block.BlockHeight)
                        shouldAttemptAdopt = (canonTipNow - block.BlockHeight) <= 4UL;

                    bool parentPayloadAvailable = BlockIndexStore.HasPayload(prevHash);
                    if (shouldAttemptAdopt && parentPayloadAvailable)
                    {
                        ChainSelector.MaybeAdoptNewTip(block.BlockHash!, _log, _mempool);
                        if (BlockIndexStore.TryGetStatus(block.BlockHash!, out var afterStatus) &&
                            BlockIndexStore.IsValidatedStatus(afterStatus))
                        {
                            stateValidated = true;
                        }
                    }
                }
                catch (Exception ex)
                {
                    _log?.Warn("P2P", $"Tip adoption failed for {Hex16(block.BlockHash!)}: {ex.Message}");
                }
            }
            else
            {
                stateValidated = true;
            }

            if (BlockIndexStore.TryGetBadFlags(block.BlockHash!, out var isBadNow, out var badAncestorNow, out _, null) &&
                (isBadNow || badAncestorNow))
            {
                _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                _blockSyncClient.RequestResync("post-validation-bad");
                return;
            }

            _notifyUiAfterAcceptedBlock();

            PeerFailTracker.ReportSuccess(peerKey);
            if (stateValidated)
            {
                try { _mempool.RemoveIncluded(block); } catch { }
                await _relayValidatedBlockAsync(block, peer, ct).ConfigureAwait(false);
                _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: true);
            }

            await _promoteOrphansForParentAsync(block.BlockHash!, peer, ct).ConfigureAwait(false);
            TryReevaluateBestKnownTipAfterPromotion(block.BlockHash!);
        }

        private void TryReevaluateBestKnownTipAfterPromotion(byte[] acceptedBlockHash)
        {
            if (acceptedBlockHash is not { Length: 32 })
                return;

            try
            {
                if (!BlockIndexStore.TryGetBestKnownTip(out var bestKnownHash, out _, out _))
                    return;
                if (bestKnownHash is not { Length: 32 })
                    return;
                if (BytesEqual32(bestKnownHash, acceptedBlockHash))
                    return;
                if (!BlockIndexStore.HasPayload(bestKnownHash))
                    return;

                var canonTipHash = BlockStore.GetCanonicalHashAtHeight(BlockStore.GetLatestHeight());
                if (canonTipHash is { Length: 32 } && BytesEqual32(canonTipHash, bestKnownHash))
                    return;

                ChainSelector.MaybeAdoptNewTip(bestKnownHash, _log, _mempool);
            }
            catch (Exception ex)
            {
                _log?.Warn("P2P", $"Post-promotion tip reevaluation failed for {Hex16(acceptedBlockHash)}: {ex.Message}");
            }
        }

        private static bool TryExtendCanonTipNoReorg(Block block, SqliteTransaction tx, out ulong newHeight)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            if (tx == null) throw new ArgumentNullException(nameof(tx));

            newHeight = 0;

            var prevHash = block.Header?.PreviousBlockHash;
            if (prevHash is not { Length: 32 })
                return false;

            if (!BlockStore.TryGetLatestHeight(out var tipHeight, tx))
                return false;

            var tipHash = BlockStore.GetCanonicalHashAtHeight(tipHeight, tx);
            if (tipHash is not { Length: 32 })
                return false;

            if (!BytesEqual32(prevHash, tipHash))
                return false;

            newHeight = tipHeight + 1UL;
            block.BlockHeight = newHeight;

            if (block.BlockHash is not { Length: 32 } || IsZero32(block.BlockHash))
                block.BlockHash = block.ComputeBlockHash();

            StateApplier.ApplyBlockWithUndo(block, tx);
            BlockStore.SetCanonicalHashAtHeight(newHeight, block.BlockHash!, tx);
            return true;
        }

        private static bool IsZero32(byte[]? hash)
        {
            if (hash is not { Length: 32 })
                return true;

            for (int i = 0; i < 32; i++)
            {
                if (hash[i] != 0)
                    return false;
            }

            return true;
        }

        private void LogSidechainAdmissionReject(string admissionReason)
        {
            if (!IsTooFarBehindAdmissionReason(admissionReason))
            {
                _log?.Warn("P2P", $"Sidechain block rejected by admission gate: {admissionReason}");
                return;
            }

            lock (_admissionLogGate)
            {
                var now = DateTime.UtcNow;
                _lastTooFarBehindAdmissionReason = admissionReason;

                if ((now - _lastTooFarBehindAdmissionLogUtc) >= TooFarBehindAdmissionLogWindow)
                {
                    if (_tooFarBehindAdmissionSuppressed > 0)
                    {
                        _log?.Warn(
                            "P2P",
                            $"Sidechain block rejected by admission gate: {_lastTooFarBehindAdmissionReason} (+{_tooFarBehindAdmissionSuppressed} similar rejects suppressed)");
                    }
                    else
                    {
                        _log?.Warn("P2P", $"Sidechain block rejected by admission gate: {_lastTooFarBehindAdmissionReason}");
                    }

                    _lastTooFarBehindAdmissionLogUtc = now;
                    _tooFarBehindAdmissionSuppressed = 0;
                }
                else
                {
                    _tooFarBehindAdmissionSuppressed++;
                }
            }
        }

        private static bool IsTooFarBehindAdmissionReason(string admissionReason)
            => !string.IsNullOrWhiteSpace(admissionReason) &&
               admissionReason.StartsWith("candidate chainwork too far behind", StringComparison.Ordinal);

        private bool ShouldDropCachedTooFarBehindLiveBranch(byte[] blockHash, byte[] prevHash)
        {
            lock (_tooFarBehindLiveRejectGate)
            {
                var now = DateTime.UtcNow;
                PruneTooFarBehindLiveRejects_NoLock(now);
                return IsTooFarBehindLiveRejectCached_NoLock(blockHash, now) ||
                       IsTooFarBehindLiveRejectCached_NoLock(prevHash, now);
            }
        }

        private void RememberTooFarBehindLiveReject(byte[] blockHash)
        {
            if (blockHash is not { Length: 32 })
                return;

            lock (_tooFarBehindLiveRejectGate)
            {
                var now = DateTime.UtcNow;
                PruneTooFarBehindLiveRejects_NoLock(now);
                if (_tooFarBehindLiveRejectUntilUtc.Count >= MaxTooFarBehindLiveRejectEntries)
                    _tooFarBehindLiveRejectUntilUtc.Clear();

                _tooFarBehindLiveRejectUntilUtc[Convert.ToHexString(blockHash)] = now + TooFarBehindLiveRejectTtl;
            }
        }

        private bool IsTooFarBehindLiveRejectCached_NoLock(byte[] hash, DateTime nowUtc)
        {
            if (hash is not { Length: 32 })
                return false;

            if (!_tooFarBehindLiveRejectUntilUtc.TryGetValue(Convert.ToHexString(hash), out var untilUtc))
                return false;

            if (untilUtc <= nowUtc)
            {
                _tooFarBehindLiveRejectUntilUtc.Remove(Convert.ToHexString(hash));
                return false;
            }

            return true;
        }

        private void PruneTooFarBehindLiveRejects_NoLock(DateTime nowUtc)
        {
            if (_tooFarBehindLiveRejectUntilUtc.Count == 0)
                return;

            List<string>? expired = null;
            foreach (var kv in _tooFarBehindLiveRejectUntilUtc)
            {
                if (kv.Value > nowUtc)
                    continue;

                expired ??= new List<string>();
                expired.Add(kv.Key);
            }

            if (expired == null)
                return;

            for (int i = 0; i < expired.Count; i++)
                _tooFarBehindLiveRejectUntilUtc.Remove(expired[i]);
        }

        private bool ShouldRequestMissingParentResync(byte[] prevHash)
        {
            if (prevHash is not { Length: 32 })
                return true;

            lock (_missingParentResyncGate)
            {
                var now = DateTime.UtcNow;
                PruneMissingParentResyncs_NoLock(now);
                string key = Convert.ToHexString(prevHash);

                if (_missingParentResyncUntilUtc.TryGetValue(key, out var untilUtc) && untilUtc > now)
                    return false;

                if (_missingParentResyncUntilUtc.Count >= MaxMissingParentResyncEntries)
                    _missingParentResyncUntilUtc.Clear();

                _missingParentResyncUntilUtc[key] = now + MissingParentResyncTtl;
                return true;
            }
        }

        private void PruneMissingParentResyncs_NoLock(DateTime nowUtc)
        {
            if (_missingParentResyncUntilUtc.Count == 0)
                return;

            List<string>? expired = null;
            foreach (var kv in _missingParentResyncUntilUtc)
            {
                if (kv.Value > nowUtc)
                    continue;

                expired ??= new List<string>();
                expired.Add(kv.Key);
            }

            if (expired == null)
                return;

            for (int i = 0; i < expired.Count; i++)
                _missingParentResyncUntilUtc.Remove(expired[i]);
        }

        private void ScheduleMissingParentRecovery(
            PeerSession peer,
            byte[] prevHash,
            BlockIngressKind ingress,
            string parentRequestSource,
            string fallbackSource,
            string syncReason)
        {
            if (_blockSyncClient.IsActive)
            {
                // Keep a bounded side-path alive even during active historical sync so live orphan
                // recovery does not fully stall behind the bulk planner. This still avoids nested
                // resync storms: sender-first direct chase plus an out-of-plan fallback, but no
                // extra RequestSyncNow while bulk sync is already active.
                _ = _blockDownloadManager.QueueOutOfPlanFallback(prevHash, fallbackSource);
                _requestParentFromPeer(peer, prevHash, parentRequestSource);
                return;
            }

            _ = _blockDownloadManager.QueueOutOfPlanFallback(prevHash, fallbackSource);
            _requestParentFromPeer(peer, prevHash, parentRequestSource);
            if (_shouldRequestMissingHeaderResync() &&
                ShouldRequestMissingParentResync(prevHash))
            {
                _requestSyncNow(syncReason);
            }
        }

        private void LogBufferedOrphan(string category, string message)
        {
            lock (_bufferedOrphanLogGate)
            {
                if (!_bufferedOrphanLogsByCategory.TryGetValue(category, out var state))
                {
                    state = new SuppressedInfoLogState();
                    _bufferedOrphanLogsByCategory[category] = state;
                }

                var now = DateTime.UtcNow;
                state.LastMessage = message;
                if ((now - state.LastLogUtc) >= BufferedOrphanLogWindow)
                {
                    if (state.Suppressed > 0)
                        _log?.Info("P2P", $"{state.LastMessage} (+{state.Suppressed} similar events suppressed)");
                    else
                        _log?.Info("P2P", state.LastMessage);

                    state.LastLogUtc = now;
                    state.Suppressed = 0;
                }
                else
                {
                    state.Suppressed++;
                }
            }
        }

        private static bool BytesEqual32(byte[] left, byte[] right)
        {
            if (left.Length != 32 || right.Length != 32)
                return false;

            for (int i = 0; i < 32; i++)
            {
                if (left[i] != right[i])
                    return false;
            }

            return true;
        }

        private static string Hex16(byte[] hash)
        {
            var text = Convert.ToHexString(hash).ToLowerInvariant();
            return text.Length > 16 ? text[..16] + "..." : text;
        }

        private static bool IsPreviousBlockMissingReason(string? reason)
        {
            if (string.IsNullOrWhiteSpace(reason))
                return false;

            return reason.IndexOf("previous block missing", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private static bool IsAlreadyBufferedReason(string? reason)
            => string.Equals(reason, "already buffered", StringComparison.Ordinal);

        private static bool IsBenignOrphanDropReason(string? reason)
            => IsAlreadyBufferedReason(reason) ||
               string.Equals(reason, "peer orphan cooldown active", StringComparison.Ordinal);

        private static string FormatHeightForLog(ulong height)
            => height == 0UL ? "?" : height.ToString(CultureInfo.InvariantCulture);

        private static bool IsNearTipCandidate(ulong candidateHeight, ulong tipHeight)
        {
            if (candidateHeight == 0UL)
                return true;
            if (candidateHeight >= tipHeight)
                return true;

            return (tipHeight - candidateHeight) <= LiveNearTipWindow;
        }

        private static bool IsTipMovedReason(string? reason)
        {
            if (string.IsNullOrWhiteSpace(reason))
                return false;

            return reason.IndexOf("block must extend current canonical tip", StringComparison.OrdinalIgnoreCase) >= 0;
        }
    }
}
