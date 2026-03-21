using System;
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

            if (!_blockSyncClient.IsActive && !headerKnown && _shouldRequestMissingHeaderResync())
                _requestSyncNow("block-out-of-plan-missing-block");

            ulong candidateHeight = block.BlockHeight;
            if (prevKnown)
                candidateHeight = prevHeight + 1UL;

            bool nearTip = candidateHeight >= tipHeight || (tipHeight - candidateHeight) <= 32UL;
            bool armedOutOfPlanFallback = _blockDownloadManager.IsOutOfPlanFallbackHash(block.BlockHash!);
            bool allowOutOfPlan = prevIsTip ||
                                  prevKnown ||
                                  armedOutOfPlanFallback ||
                                  (_blockSyncClient.IsActive && nearTip && headerKnown);
            if (allowOutOfPlan)
                return false;

            if (!headerKnown && !prevKnown)
            {
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

                _log?.Info("P2P", $"{bufferedPrefix}: h={FormatHeightForLog(block.BlockHeight)} {Hex16(block.BlockHash!)}");
                _requestParentFromPeer(peer, prevHash, parentRequestSource);
                _ = _blockDownloadManager.QueueOutOfPlanFallback(prevHash, fallbackSource);
                if (_shouldRequestMissingHeaderResync())
                    _requestSyncNow(syncReason);
            }
            else
            {
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

                if (!_blockSyncClient.IsActive && !headerKnown && _shouldRequestMissingHeaderResync())
                    _requestSyncNow("block-out-of-plan-missing-block");

                ulong candidateHeight = block.BlockHeight;
                if (outOfPlanPrevKnown && prevHeightKnown < ulong.MaxValue)
                    candidateHeight = prevHeightKnown + 1UL;

                bool nearTip = candidateHeight >= tipHeight || (tipHeight - candidateHeight) <= 32UL;
                bool armedOutOfPlanFallback = _blockDownloadManager.IsOutOfPlanFallbackHash(block.BlockHash!);
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

                            _log?.Info("P2P", $"Out-of-plan orphan buffered: h={FormatHeightForLog(block.BlockHeight)} {Hex16(block.BlockHash!)}");
                            _requestParentFromPeer(peer, prevHash, "out-of-plan-parent");
                            _ = _blockDownloadManager.QueueOutOfPlanFallback(prevHash, "out-of-plan-parent-fallback");
                            if (_shouldRequestMissingHeaderResync())
                                _requestSyncNow("out-of-plan-parent-missing");
                        }
                        else
                        {
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

                    _log?.Info("P2P", $"Orphan buffered: h={FormatHeightForLog(block.BlockHeight)} {Hex16(block.BlockHash!)}");
                    _requestParentFromPeer(peer, prevHash, "orphan-parent");
                    _ = _blockDownloadManager.QueueOutOfPlanFallback(prevHash, "orphan-parent-fallback");
                    if (_shouldRequestMissingHeaderResync())
                        _requestSyncNow("orphan-parent-missing");
                }
                else
                {
                    _log?.Warn("P2P", $"Orphan dropped: {orphanReason}");
                    _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                }

                return;
            }

            bool persistedRaw = alreadyHavePayload;
            if (!alreadyHavePayload)
            {
                try
                {
                    PersistRawBlockAsHaveBlock(block);
                    persistedRaw = true;
                }
                catch (Exception ex)
                {
                    _log?.Warn("P2P", $"Block raw persist failed: {ex.Message}");
                    _blockDownloadManager.MarkBlockValidated(block.BlockHash!, valid: false);
                    return;
                }
            }

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
                            _requestParentFromPeer(peer, prevHash, "sidechain-parent");
                            _ = _blockDownloadManager.QueueOutOfPlanFallback(prevHash, "sidechain-parent-fallback");
                            if (_shouldRequestMissingHeaderResync())
                                _requestSyncNow("sidechain-parent-missing");
                        }
                        else
                        {
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
                    _log?.Warn("P2P", $"Sidechain block rejected by admission gate: {admissionReason}");
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

                    if (TryExtendCanonTipNoReorg(block, tx, out _))
                    {
                        extendedCanon = true;
                        BlockIndexStore.SetStatus(block.BlockHash!, BlockIndexStore.StatusCanonicalStateValidated, tx);
                    }
                    else
                    {
                        BlockIndexStore.SetStatus(block.BlockHash!, BlockIndexStore.StatusHaveBlockPayload, tx);
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

        private static void PersistRawBlockAsHaveBlock(Block block)
        {
            lock (Db.Sync)
            {
                using var tx = Db.Connection!.BeginTransaction();
                BlockStore.SaveBlock(block, tx, BlockIndexStore.StatusHaveBlockPayload);
                tx.Commit();
            }
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

        private static string FormatHeightForLog(ulong height)
            => height == 0UL ? "?" : height.ToString(CultureInfo.InvariantCulture);

        private static bool IsTipMovedReason(string? reason)
        {
            if (string.IsNullOrWhiteSpace(reason))
                return false;

            return reason.IndexOf("block must extend current canonical tip", StringComparison.OrdinalIgnoreCase) >= 0;
        }
    }
}
