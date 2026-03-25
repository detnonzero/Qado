using System;
using System.Collections.Generic;
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
    public sealed class BulkSyncRuntime
    {
        private readonly SemaphoreSlim _validationSerializeGate;
        private readonly object _stateGate = new();
        private readonly MempoolManager _mempool;
        private readonly Action _notifyUiAfterCanonicalChange;
        private readonly ILogSink? _log;
        private BlockSyncBatchContext? _activeBatch;

        public BulkSyncRuntime(
            SemaphoreSlim validationSerializeGate,
            MempoolManager mempool,
            Action notifyUiAfterCanonicalChange,
            ILogSink? log = null)
        {
            _validationSerializeGate = validationSerializeGate ?? throw new ArgumentNullException(nameof(validationSerializeGate));
            _mempool = mempool ?? throw new ArgumentNullException(nameof(mempool));
            _notifyUiAfterCanonicalChange = notifyUiAfterCanonicalChange ?? throw new ArgumentNullException(nameof(notifyUiAfterCanonicalChange));
            _log = log;
        }

        public async Task<BlockSyncPrepareResult> PrepareBatchAsync(
            byte[] startHash,
            ulong startHeight,
            UInt128 advertisedTipChainwork,
            PeerSession peer,
            CancellationToken ct)
        {
            if (peer == null)
                return new BlockSyncPrepareResult(false, "peer missing");
            if (startHash is not { Length: 32 })
                return new BlockSyncPrepareResult(false, "invalid start hash");
            if (startHeight == 0)
                return new BlockSyncPrepareResult(false, "invalid start height");
            if (advertisedTipChainwork == 0)
                return new BlockSyncPrepareResult(false, "peer tip missing chainwork");

            bool acquiredValidationGate = false;

            try
            {
                BlockSyncBatchContext? continuationBatch;
                lock (_stateGate)
                    continuationBatch = _activeBatch;

                if (continuationBatch == null)
                {
                    await _validationSerializeGate.WaitAsync(ct).ConfigureAwait(false);
                    acquiredValidationGate = true;
                }

                lock (_stateGate)
                    continuationBatch = _activeBatch;

                if (continuationBatch != null)
                {
                    if (acquiredValidationGate)
                    {
                        _validationSerializeGate.Release();
                        acquiredValidationGate = false;
                    }

                    return PrepareContinuationBatch(
                        continuationBatch,
                        startHash,
                        startHeight,
                        advertisedTipChainwork,
                        peer);
                }

                ulong forkHeight = startHeight - 1UL;
                byte[]? localForkHash;
                Block? forkBlock;
                lock (Db.Sync)
                {
                    var currentTipHash = BlockStore.GetCanonicalHashAtHeight(BlockStore.GetLatestHeight()) ?? Array.Empty<byte>();
                    UInt128 currentTipWork = currentTipHash is { Length: 32 } && !IsZero32(currentTipHash)
                        ? BlockIndexStore.GetChainwork(currentTipHash)
                        : 0;
                    if (currentTipWork != 0 && advertisedTipChainwork <= currentTipWork)
                    {
                        return new BlockSyncPrepareResult(
                            false,
                            $"peer tip chainwork not better than local chain (peer={advertisedTipChainwork}, local={currentTipWork})");
                    }

                    localForkHash = BlockStore.GetCanonicalHashAtHeight(forkHeight);
                    forkBlock = BlockStore.GetBlockByHeight(forkHeight);
                }

                if (localForkHash is not { Length: 32 })
                    return new BlockSyncPrepareResult(false, $"missing local forkpoint at height {forkHeight}");
                if (forkBlock?.BlockHash is not { Length: 32 })
                    return new BlockSyncPrepareResult(false, $"missing local forkpoint payload at height {forkHeight}");
                if (!BytesEqual32(localForkHash, startHash))
                    return new BlockSyncPrepareResult(false, "forkpoint no longer matches local canonical chain");

                var batch = new BlockSyncBatchContext
                {
                    PeerKey = peer.SessionKey,
                    ForkHash = (byte[])startHash.Clone(),
                    ForkHeight = forkHeight,
                    AdvertisedTipChainwork = advertisedTipChainwork
                };
                batch.History.Seed(forkBlock);
                lock (_stateGate)
                    _activeBatch = batch;

                acquiredValidationGate = false;
                return new BlockSyncPrepareResult(true, string.Empty);
            }
            catch (Exception ex)
            {
                return new BlockSyncPrepareResult(false, ex.Message);
            }
            finally
            {
                if (acquiredValidationGate)
                    _validationSerializeGate.Release();
            }
        }

        public Task<BlockSyncChunkCommitResult> CommitChunkAsync(
            IReadOnlyList<byte[]> payloads,
            ulong expectedHeight,
            byte[] expectedPrevHash,
            PeerSession peer,
            CancellationToken ct)
        {
            if (peer == null)
                return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), "peer missing"));
            if (payloads == null || payloads.Count == 0 || payloads.Count > BlockSyncProtocol.ChunkBlocks)
                return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), "invalid chunk payload count"));
            if (expectedPrevHash is not { Length: 32 })
                return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), "invalid expected prev hash"));

            BlockSyncBatchContext? batch;
            lock (_stateGate)
                batch = _activeBatch;
            if (batch == null)
                return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), "no active batch"));
            if (!string.Equals(batch.PeerKey, peer.SessionKey, StringComparison.Ordinal))
                return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), "batch peer mismatch"));

            var blocks = new List<Block>(payloads.Count);
            ulong nextHeight = expectedHeight;
            byte[] nextPrevHash = (byte[])expectedPrevHash.Clone();

            for (int i = 0; i < payloads.Count; i++)
            {
                if (ct.IsCancellationRequested)
                    return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), "operation cancelled"));

                var payload = payloads[i];
                if (payload == null || payload.Length == 0 || payload.Length > BlockSyncProtocol.MaxSerializedBlockBytes)
                    return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), "invalid block payload size"));

                Block block;
                try
                {
                    block = BlockBinarySerializer.Read(payload);
                }
                catch (Exception ex)
                {
                    return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), $"block decode failed: {ex.Message}"));
                }

                if (block.Header?.PreviousBlockHash is not { Length: 32 })
                    return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), "block missing prev hash"));

                block.BlockHeight = nextHeight;
                if (block.BlockHash is not { Length: 32 } || IsZero32(block.BlockHash))
                    block.BlockHash = block.ComputeBlockHash();

                if (!BytesEqual32(block.Header.PreviousBlockHash, nextPrevHash))
                    return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), "prev hash mismatch"));

                if (BlockIndexStore.IsBadOrHasBadAncestor(block.BlockHash!))
                    return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), "block is marked bad"));

                blocks.Add(block);
                nextPrevHash = (byte[])block.BlockHash!.Clone();
                nextHeight++;
            }

            try
            {
                var workingBatch = new BlockSyncBatchContext
                {
                    PeerKey = batch.PeerKey,
                    ForkHash = (byte[])batch.ForkHash.Clone(),
                    ForkHeight = batch.ForkHeight,
                    AdvertisedTipChainwork = batch.AdvertisedTipChainwork,
                    DidRollback = batch.DidRollback
                };
                if (batch.RolledBackCanonicalBlocksDesc.Count > 0)
                    workingBatch.RolledBackCanonicalBlocksDesc.AddRange(batch.RolledBackCanonicalBlocksDesc);

                var workingHistory = batch.History.CreateWorkingCopy();

                lock (Db.Sync)
                {
                    using var tx = Db.Connection!.BeginTransaction();

                    if (!EnsureBlockSyncTipReady(workingBatch, expectedHeight, expectedPrevHash, tx, out var prepReason))
                    {
                        tx.Rollback();
                        return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), prepReason));
                    }

                    var tipBlock = workingHistory.EnsureTip(expectedHeight - 1UL, tx);
                    if (tipBlock?.BlockHash is not { Length: 32 } || !BytesEqual32(tipBlock.BlockHash, expectedPrevHash))
                    {
                        tx.Rollback();
                        return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), "failed to align validation tip"));
                    }

                    for (int i = 0; i < blocks.Count; i++)
                    {
                        if (ct.IsCancellationRequested)
                        {
                            tx.Rollback();
                            return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), "operation cancelled"));
                        }

                        var block = blocks[i];
                        if (!BlockValidator.ValidateNetworkTipBlock(
                                block,
                                tipBlock,
                                h => workingHistory.GetBlockAtHeight(h, tx),
                                out var reason,
                                tx))
                        {
                            tx.Rollback();
                            return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), reason));
                        }

                        BlockStore.SaveBlock(block, tx, BlockIndexStore.StatusCanonicalStateValidated);
                        StateApplier.ApplyBlockWithUndo(block, tx);
                        BlockStore.SetCanonicalHashAtHeight(block.BlockHeight, block.BlockHash!, tx);
                        BlockIndexStore.SetStatus(block.BlockHash!, BlockIndexStore.StatusCanonicalStateValidated, tx);

                        workingHistory.NoteCommitted(block);
                        tipBlock = block;
                    }

                    tx.Commit();
                }

                batch.DidRollback = workingBatch.DidRollback;
                batch.RolledBackCanonicalBlocksDesc.Clear();
                if (workingBatch.RolledBackCanonicalBlocksDesc.Count > 0)
                    batch.RolledBackCanonicalBlocksDesc.AddRange(workingBatch.RolledBackCanonicalBlocksDesc);
                batch.History.ReplaceWith(workingHistory);
                batch.AppliedBlocksAsc.AddRange(blocks);

                return Task.FromResult(new BlockSyncChunkCommitResult(true, (byte[])blocks[^1].BlockHash!.Clone(), string.Empty));
            }
            catch (Exception ex)
            {
                return Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), ex.Message));
            }
        }

        public Task CompleteBatchAsync(PeerSession peer, BlocksEndStatus status, CancellationToken ct)
        {
            if (status == BlocksEndStatus.MoreAvailable)
                return Task.CompletedTask;

            FinalizeBatch("tip-reached");
            return Task.CompletedTask;
        }

        public Task AbortBatchAsync(PeerSession? peer, string reason, CancellationToken ct)
        {
            FinalizeBatch($"abort:{reason}");
            return Task.CompletedTask;
        }

        private bool EnsureBlockSyncTipReady(
            BlockSyncBatchContext batch,
            ulong expectedHeight,
            byte[] expectedPrevHash,
            SqliteTransaction tx,
            out string reason)
        {
            reason = "OK";

            if (expectedPrevHash is not { Length: 32 })
            {
                reason = "invalid expected prev hash";
                return false;
            }

            if (!BlockStore.TryGetLatestHeight(out var currentTipHeight, tx))
            {
                reason = "local tip unknown";
                return false;
            }

            var currentTipHash = BlockStore.GetCanonicalHashAtHeight(currentTipHeight, tx);
            if (currentTipHash is not { Length: 32 })
            {
                reason = "local tip unknown";
                return false;
            }

            if (batch.AdvertisedTipChainwork == 0)
            {
                reason = "peer tip missing chainwork";
                return false;
            }

            UInt128 currentTipWork = BlockIndexStore.GetChainwork(currentTipHash, tx);
            if (currentTipWork != 0 && batch.AdvertisedTipChainwork <= currentTipWork)
            {
                reason = $"peer tip chainwork not better than local chain (peer={batch.AdvertisedTipChainwork}, local={currentTipWork})";
                return false;
            }

            if (BytesEqual32(currentTipHash, expectedPrevHash))
                return true;

            if (batch.DidRollback)
            {
                reason = "local tip changed unexpectedly during block sync";
                return false;
            }

            if (expectedHeight == 0)
            {
                reason = "invalid expected height";
                return false;
            }

            ulong forkHeight = expectedHeight - 1UL;
            var forkHash = BlockStore.GetCanonicalHashAtHeight(forkHeight, tx);
            if (forkHash is not { Length: 32 } || !BytesEqual32(forkHash, expectedPrevHash))
            {
                reason = "forkpoint no longer canonical";
                return false;
            }

            if (currentTipHeight < expectedHeight)
            {
                reason = "local tip is behind expected forkpoint";
                return false;
            }

            batch.RolledBackCanonicalBlocksDesc.Clear();

            for (ulong height = currentTipHeight; ; height--)
            {
                if (height < expectedHeight)
                    break;

                var oldHash = BlockStore.GetCanonicalHashAtHeight(height, tx);
                if (oldHash is not { Length: 32 })
                {
                    reason = $"missing canonical hash at height {height}";
                    return false;
                }

                var oldBlock = BlockStore.GetBlockByHash(oldHash, tx);
                if (oldBlock == null)
                {
                    reason = $"missing canonical block payload at height {height}";
                    return false;
                }

                oldBlock.BlockHash = (byte[])oldHash.Clone();
                oldBlock.BlockHeight = height;
                batch.RolledBackCanonicalBlocksDesc.Add(oldBlock);

                if (height == expectedHeight)
                    break;
            }

            for (int i = 0; i < batch.RolledBackCanonicalBlocksDesc.Count; i++)
            {
                var oldBlock = batch.RolledBackCanonicalBlocksDesc[i];
                StateUndoStore.RollbackBlock(oldBlock.BlockHash!, tx);
                BlockIndexStore.SetStatus(oldBlock.BlockHash!, BlockIndexStore.StatusHaveBlockPayload, tx);
            }

            BlockStore.DeleteCanonicalFromHeight(expectedHeight, tx);
            batch.DidRollback = true;

            var alignedTipHash = BlockStore.GetCanonicalHashAtHeight(forkHeight, tx);
            if (alignedTipHash is not { Length: 32 } || !BytesEqual32(alignedTipHash, expectedPrevHash))
            {
                reason = "failed to align canonical tip to forkpoint";
                return false;
            }

            return true;
        }

        private void FinalizeBatch(string context)
        {
            BlockSyncBatchContext? batch;
            lock (_stateGate)
            {
                batch = _activeBatch;
                _activeBatch = null;
            }

            try
            {
                if (batch == null)
                    return;

                bool restoredAbortedReorg = false;
                if (batch.DidRollback && context.StartsWith("abort:", StringComparison.Ordinal))
                    restoredAbortedReorg = TryRestoreAbortedReorg(batch, context);

                if (batch.DidRollback && !restoredAbortedReorg)
                {
                    try
                    {
                        var recon = _mempool.ReconcileAfterReorg(batch.RolledBackCanonicalBlocksDesc, batch.AppliedBlocksAsc);
                        _log?.Info(
                            "Sync",
                            $"Block sync reorg reconcile ({context}): removedIncluded={recon.RemovedIncluded}, requeued={recon.Requeued}, rejected={recon.Rejected}, purged={recon.PurgedInvalid}");
                    }
                    catch (Exception ex)
                    {
                        _log?.Warn("Sync", $"Block sync mempool reconcile failed ({context}): {ex.Message}");
                    }
                }
                else if (!batch.DidRollback)
                {
                    for (int i = 0; i < batch.AppliedBlocksAsc.Count; i++)
                    {
                        try { _mempool.RemoveIncluded(batch.AppliedBlocksAsc[i]); } catch { }
                    }
                }

                if (batch.DidRollback || batch.AppliedBlocksAsc.Count > 0)
                    _notifyUiAfterCanonicalChange();
            }
            finally
            {
                if (batch != null)
                    _validationSerializeGate.Release();
            }
        }

        private bool TryRestoreAbortedReorg(BlockSyncBatchContext batch, string context)
        {
            if (batch.RolledBackCanonicalBlocksDesc.Count == 0)
                return false;

            try
            {
                lock (Db.Sync)
                {
                    using var tx = Db.Connection!.BeginTransaction();

                    for (int i = batch.AppliedBlocksAsc.Count - 1; i >= 0; i--)
                    {
                        var applied = batch.AppliedBlocksAsc[i];
                        if (applied?.BlockHash is not { Length: 32 })
                            throw new InvalidOperationException("applied sync block missing hash during abort restore");

                        StateUndoStore.RollbackBlock(applied.BlockHash, tx);
                        BlockIndexStore.SetStatus(applied.BlockHash, BlockIndexStore.StatusHaveBlockPayload, tx);
                    }

                    ulong restoreFromHeight = batch.RolledBackCanonicalBlocksDesc[^1].BlockHeight;
                    if (batch.AppliedBlocksAsc.Count > 0 && batch.AppliedBlocksAsc[0].BlockHeight < restoreFromHeight)
                        restoreFromHeight = batch.AppliedBlocksAsc[0].BlockHeight;

                    BlockStore.DeleteCanonicalFromHeight(restoreFromHeight, tx);

                    for (int i = batch.RolledBackCanonicalBlocksDesc.Count - 1; i >= 0; i--)
                    {
                        var oldBlock = batch.RolledBackCanonicalBlocksDesc[i];
                        if (oldBlock?.BlockHash is not { Length: 32 })
                            throw new InvalidOperationException("rolled-back canonical block missing hash during abort restore");

                        StateApplier.ApplyBlockWithUndo(oldBlock, tx);
                        BlockStore.SetCanonicalHashAtHeight(oldBlock.BlockHeight, oldBlock.BlockHash, tx);
                        BlockIndexStore.SetStatus(oldBlock.BlockHash, BlockIndexStore.StatusCanonicalStateValidated, tx);
                    }

                    tx.Commit();
                }

                _log?.Info(
                    "Sync",
                    $"Block sync abort restored prior canonical chain ({context}): reapplied={batch.RolledBackCanonicalBlocksDesc.Count}, reverted={batch.AppliedBlocksAsc.Count}");
                return true;
            }
            catch (Exception ex)
            {
                _log?.Warn("Sync", $"Block sync abort restore failed ({context}): {ex.Message}");
                return false;
            }
        }

        private BlockSyncPrepareResult PrepareContinuationBatch(
            BlockSyncBatchContext batch,
            byte[] startHash,
            ulong startHeight,
            UInt128 advertisedTipChainwork,
            PeerSession peer)
        {
            if (!string.Equals(batch.PeerKey, peer.SessionKey, StringComparison.Ordinal))
                return new BlockSyncPrepareResult(false, "another batch is already active");

            ulong forkHeight = startHeight - 1UL;
            byte[]? localForkHash;
            Block? forkBlock;
            lock (Db.Sync)
            {
                var currentTipHeight = BlockStore.GetLatestHeight();
                var currentTipHash = BlockStore.GetCanonicalHashAtHeight(currentTipHeight);
                if (currentTipHash is not { Length: 32 })
                    return new BlockSyncPrepareResult(false, "local tip unknown");
                if (currentTipHeight != forkHeight || !BytesEqual32(currentTipHash, startHash))
                {
                    return new BlockSyncPrepareResult(
                        false,
                        $"continuation forkpoint no longer matches local canonical tip (local={currentTipHeight}:{Hex16(currentTipHash)}, expected={forkHeight}:{Hex16(startHash)})");
                }

                localForkHash = currentTipHash;
                forkBlock = BlockStore.GetBlockByHeight(currentTipHeight);
            }

            if (localForkHash is not { Length: 32 })
                return new BlockSyncPrepareResult(false, $"missing continuation forkpoint at height {forkHeight}");
            if (forkBlock?.BlockHash is not { Length: 32 })
                return new BlockSyncPrepareResult(false, $"missing continuation forkpoint payload at height {forkHeight}");

            if (advertisedTipChainwork > batch.AdvertisedTipChainwork)
                batch.AdvertisedTipChainwork = advertisedTipChainwork;

            batch.History.Seed(forkBlock);
            return new BlockSyncPrepareResult(true, string.Empty);
        }

        private sealed class BlockSyncBatchContext
        {
            public string PeerKey { get; init; } = string.Empty;
            public byte[] ForkHash { get; init; } = Array.Empty<byte>();
            public ulong ForkHeight { get; init; }
            public UInt128 AdvertisedTipChainwork { get; set; }
            public bool DidRollback { get; set; }
            public List<Block> RolledBackCanonicalBlocksDesc { get; } = new();
            public List<Block> AppliedBlocksAsc { get; } = new();
            public BlockSyncHistoryCache History { get; } = new();
        }

        private sealed class BlockSyncHistoryCache
        {
            private const ulong MaxDepth = 384;
            private readonly SortedDictionary<ulong, Block> _byHeight = new();

            public ulong TipHeight { get; private set; }
            public byte[] TipHash { get; private set; } = Array.Empty<byte>();

            public void Seed(Block tipBlock)
            {
                if (tipBlock == null)
                    throw new ArgumentNullException(nameof(tipBlock));
                if (tipBlock.BlockHash is not { Length: 32 })
                    throw new ArgumentException("tipBlock.BlockHash missing", nameof(tipBlock));

                _byHeight.Clear();
                TipHeight = tipBlock.BlockHeight;
                TipHash = (byte[])tipBlock.BlockHash.Clone();
                Remember(tipBlock);
                Prune();
            }

            public BlockSyncHistoryCache CreateWorkingCopy()
            {
                var copy = new BlockSyncHistoryCache
                {
                    TipHeight = TipHeight,
                    TipHash = TipHash is { Length: 32 } ? (byte[])TipHash.Clone() : Array.Empty<byte>()
                };

                foreach (var kv in _byHeight)
                    copy._byHeight[kv.Key] = kv.Value;

                return copy;
            }

            public void ReplaceWith(BlockSyncHistoryCache source)
            {
                if (source == null)
                    throw new ArgumentNullException(nameof(source));

                _byHeight.Clear();
                foreach (var kv in source._byHeight)
                    _byHeight[kv.Key] = kv.Value;

                TipHeight = source.TipHeight;
                TipHash = source.TipHash is { Length: 32 } ? (byte[])source.TipHash.Clone() : Array.Empty<byte>();
            }

            public Block? EnsureTip(ulong tipHeight, SqliteTransaction tx)
            {
                RemoveAbove(tipHeight);
                TipHeight = tipHeight;

                var tipBlock = GetBlockAtHeight(tipHeight, tx);
                if (tipBlock?.BlockHash is { Length: 32 })
                    TipHash = (byte[])tipBlock.BlockHash.Clone();
                else
                    TipHash = Array.Empty<byte>();

                Prune();
                return tipBlock;
            }

            public Block? GetBlockAtHeight(ulong height, SqliteTransaction tx)
            {
                if (_byHeight.TryGetValue(height, out var cached))
                    return cached;

                var block = BlockStore.GetBlockByHeight(height, tx);
                if (block?.BlockHash is not { Length: 32 })
                    return null;

                Remember(block);
                Prune();
                return block;
            }

            public void NoteCommitted(Block block)
            {
                if (block == null)
                    throw new ArgumentNullException(nameof(block));
                if (block.BlockHash is not { Length: 32 })
                    throw new ArgumentException("block.BlockHash missing", nameof(block));

                TipHeight = block.BlockHeight;
                TipHash = (byte[])block.BlockHash.Clone();
                Remember(block);
                Prune();
            }

            private void Remember(Block block)
            {
                _byHeight[block.BlockHeight] = block;
            }

            private void RemoveAbove(ulong height)
            {
                if (_byHeight.Count == 0)
                    return;

                var remove = new List<ulong>();
                foreach (var kv in _byHeight)
                {
                    if (kv.Key > height)
                        remove.Add(kv.Key);
                }

                for (int i = 0; i < remove.Count; i++)
                    _byHeight.Remove(remove[i]);
            }

            private void Prune()
            {
                RemoveAbove(TipHeight);

                ulong keepFrom = TipHeight > MaxDepth ? TipHeight - MaxDepth : 0;
                if (_byHeight.Count == 0)
                    return;

                var remove = new List<ulong>();
                foreach (var kv in _byHeight)
                {
                    if (kv.Key < keepFrom)
                        remove.Add(kv.Key);
                }

                for (int i = 0; i < remove.Count; i++)
                    _byHeight.Remove(remove[i]);
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
            if (hash is not { Length: 32 })
                return "unknown";

            var hex = Convert.ToHexString(hash).ToLowerInvariant();
            return hex.Length > 16 ? hex[..16] : hex;
        }
    }
}
