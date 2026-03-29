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
        private const int ChainWindowSize = 256;
        private readonly SemaphoreSlim _validationSerializeGate;
        private readonly MempoolManager _mempool;
        private readonly Action _notifyUiAfterCanonicalChange;
        private readonly ILogSink? _log;
        private readonly object _rollingChainWindowGate = new();
        private Dictionary<ulong, Block>? _rollingChainWindow;
        private byte[] _rollingChainTipHash = Array.Empty<byte>();
        private ulong _rollingChainTipHeight;

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

        public Task<BlockSyncPrepareResult> PrepareBatchAsync(
            byte[] startHash,
            ulong startHeight,
            UInt128 advertisedTipChainwork,
            PeerSession peer,
            CancellationToken ct)
        {
            if (peer == null)
                return Task.FromResult(new BlockSyncPrepareResult(false, "peer missing"));
            if (startHash is not { Length: 32 })
                return Task.FromResult(new BlockSyncPrepareResult(false, "invalid start hash"));
            if (startHeight == 0)
                return Task.FromResult(new BlockSyncPrepareResult(false, "invalid start height"));
            if (advertisedTipChainwork == 0)
                return Task.FromResult(new BlockSyncPrepareResult(false, "peer tip missing chainwork"));

            try
            {
                lock (Db.Sync)
                {
                    if (!BlockIndexStore.TryGetMeta(startHash, out var knownHeight, out _, out _, null))
                        return Task.FromResult(new BlockSyncPrepareResult(false, "start hash unknown"));

                    if (knownHeight + 1UL != startHeight)
                    {
                        return Task.FromResult(new BlockSyncPrepareResult(
                            false,
                            $"start height mismatch (known={knownHeight}, expectedPrev={startHeight - 1UL})"));
                    }

                    var currentTipHash = BlockStore.GetCanonicalHashAtHeight(BlockStore.GetLatestHeight()) ?? Array.Empty<byte>();
                    UInt128 currentTipWork = currentTipHash is { Length: 32 } && !IsZero32(currentTipHash)
                        ? BlockIndexStore.GetChainwork(currentTipHash)
                        : 0;
                    if (currentTipWork != 0 && advertisedTipChainwork <= currentTipWork)
                    {
                        return Task.FromResult(new BlockSyncPrepareResult(
                            false,
                            $"peer tip chainwork not better than local chain (peer={advertisedTipChainwork}, local={currentTipWork})"));
                    }
                }

                return Task.FromResult(new BlockSyncPrepareResult(true, string.Empty));
            }
            catch (Exception ex)
            {
                return Task.FromResult(new BlockSyncPrepareResult(false, ex.Message));
            }
        }

        public async Task<BlockSyncCommitResult> CommitBlocksAsync(
            IReadOnlyList<byte[]> payloads,
            ulong expectedHeight,
            byte[] expectedPrevHash,
            PeerSession peer,
            CancellationToken ct)
        {
            if (peer == null)
                return new BlockSyncCommitResult(false, Array.Empty<byte>(), "peer missing");
            if (payloads == null || payloads.Count == 0 || payloads.Count > BlockSyncProtocol.BatchMaxBlocks)
                return new BlockSyncCommitResult(false, Array.Empty<byte>(), "invalid batch payload count");
            if (expectedPrevHash is not { Length: 32 })
                return new BlockSyncCommitResult(false, Array.Empty<byte>(), "invalid expected prev hash");

            await _validationSerializeGate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                byte[] lastHash;
                var committedBlocks = new List<Block>(payloads.Count);
                lock (Db.Sync)
                {
                    using var tx = Db.Connection!.BeginTransaction();

                    if (!BlockIndexStore.TryGetMeta(expectedPrevHash, out var prevHeight, out _, out _, tx))
                    {
                        InvalidateRollingChainWindow();
                        tx.Rollback();
                        return new BlockSyncCommitResult(false, Array.Empty<byte>(), "expected prev hash unknown");
                    }

                    if (prevHeight + 1UL != expectedHeight)
                    {
                        InvalidateRollingChainWindow();
                        tx.Rollback();
                        return new BlockSyncCommitResult(false, Array.Empty<byte>(), "expected height mismatch");
                    }

                    var chainWindow = ResolveOrBuildChainWindow(expectedPrevHash, prevHeight, tx);
                    if (chainWindow == null)
                    {
                        InvalidateRollingChainWindow();
                        tx.Rollback();
                        return new BlockSyncCommitResult(false, Array.Empty<byte>(), "expected prev block payload missing");
                    }
                    Block? GetChainBlockByHeight(ulong height)
                    {
                        if (chainWindow.TryGetValue(height, out var known))
                            return known;

                        return null;
                    }

                    byte[] nextPrevHash = (byte[])expectedPrevHash.Clone();
                    ulong nextHeight = expectedHeight;

                    for (int i = 0; i < payloads.Count; i++)
                    {
                        if (ct.IsCancellationRequested)
                        {
                            InvalidateRollingChainWindow();
                            tx.Rollback();
                            return new BlockSyncCommitResult(false, Array.Empty<byte>(), "operation cancelled");
                        }

                        var payload = payloads[i];
                        if (payload == null || payload.Length == 0 || payload.Length > BlockSyncProtocol.MaxSerializedBlockBytes)
                        {
                            InvalidateRollingChainWindow();
                            tx.Rollback();
                            return new BlockSyncCommitResult(
                                false,
                                Array.Empty<byte>(),
                                $"invalid block payload size at height {nextHeight} (index={i}, bytes={payload?.Length ?? 0})");
                        }

                        Block block;
                        try
                        {
                            block = BlockBinarySerializer.Read(payload);
                        }
                        catch (Exception ex)
                        {
                            InvalidateRollingChainWindow();
                            tx.Rollback();
                            return new BlockSyncCommitResult(
                                false,
                                Array.Empty<byte>(),
                                $"block decode failed at height {nextHeight} (index={i}): {ex.Message}");
                        }

                        if (block.Header?.PreviousBlockHash is not { Length: 32 })
                        {
                            InvalidateRollingChainWindow();
                            tx.Rollback();
                            return new BlockSyncCommitResult(
                                false,
                                Array.Empty<byte>(),
                                $"block missing prev hash at height {nextHeight} (index={i})");
                        }

                        block.BlockHeight = nextHeight;
                        if (block.BlockHash is not { Length: 32 } || IsZero32(block.BlockHash))
                            block.BlockHash = block.ComputeBlockHash();

                        if (!BytesEqual32(block.Header.PreviousBlockHash, nextPrevHash))
                        {
                            InvalidateRollingChainWindow();
                            tx.Rollback();
                            return new BlockSyncCommitResult(
                                false,
                                Array.Empty<byte>(),
                                $"prev hash mismatch at height {nextHeight} (index={i}, block={Hex16(block.BlockHash!)}, expectedPrev={Hex16(nextPrevHash)}, actualPrev={Hex16(block.Header.PreviousBlockHash)})");
                        }

                        if (BlockIndexStore.IsBadOrHasBadAncestor(block.BlockHash!, tx))
                        {
                            InvalidateRollingChainWindow();
                            tx.Rollback();
                            return new BlockSyncCommitResult(
                                false,
                                Array.Empty<byte>(),
                                $"block is marked bad at height {nextHeight} ({Hex16(block.BlockHash!)})");
                        }

                        if (!BlockValidator.ValidateNetworkSideBlockStateless(block, GetChainBlockByHeight, out var reason, tx))
                        {
                            InvalidateRollingChainWindow();
                            tx.Rollback();
                            return new BlockSyncCommitResult(
                                false,
                                Array.Empty<byte>(),
                                $"height {nextHeight} block {Hex16(block.BlockHash!)} invalid: {reason}");
                        }

                        BlockStore.SaveBlock(block, tx, BlockIndexStore.StatusHaveBlockPayload);
                        committedBlocks.Add(block);
                        AdvanceRollingChainWindow(chainWindow, block);
                        nextPrevHash = (byte[])block.BlockHash!.Clone();
                        nextHeight++;
                    }

                    tx.Commit();
                    lastHash = nextPrevHash;
                }

                bool adopted = TryAdoptCommittedTip(lastHash, committedBlocks);
                if (adopted)
                    _notifyUiAfterCanonicalChange();

                return new BlockSyncCommitResult(true, lastHash, string.Empty);
            }
            catch (Exception ex)
            {
                InvalidateRollingChainWindow();
                return new BlockSyncCommitResult(false, Array.Empty<byte>(), ex.Message);
            }
            finally
            {
                _validationSerializeGate.Release();
            }
        }

        public Task CompleteBatchAsync(PeerSession peer, BlocksEndStatus status, CancellationToken ct)
            => Task.CompletedTask;

        public Task AbortBatchAsync(PeerSession? peer, string reason, CancellationToken ct)
            => Task.CompletedTask;

        private bool TryAdoptCommittedTip(byte[] candidateTipHash, IReadOnlyList<Block>? committedBlocksAsc)
        {
            if (candidateTipHash is not { Length: 32 })
                return false;

            try
            {
                int beforeStatus = 0;
                _ = BlockIndexStore.TryGetStatus(candidateTipHash, out beforeStatus);
                if (committedBlocksAsc is { Count: > 0 })
                    ChainSelector.MaybeAdoptNewTip(candidateTipHash, committedBlocksAsc, _log, _mempool);
                else
                    ChainSelector.MaybeAdoptNewTip(candidateTipHash, _log, _mempool);
                if (!BlockIndexStore.TryGetStatus(candidateTipHash, out var afterStatus))
                    return false;

                return !BlockIndexStore.IsValidatedStatus(beforeStatus) &&
                       BlockIndexStore.IsValidatedStatus(afterStatus);
            }
            catch (Exception ex)
            {
                _log?.Warn("Sync", $"Batch adoption failed for {Hex16(candidateTipHash)}: {ex.Message}");
                return false;
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

        private Dictionary<ulong, Block>? ResolveOrBuildChainWindow(byte[] expectedPrevHash, ulong expectedPrevHeight, SqliteTransaction tx)
        {
            lock (_rollingChainWindowGate)
            {
                if (_rollingChainWindow != null &&
                    _rollingChainTipHash is { Length: 32 } &&
                    BytesEqual32(_rollingChainTipHash, expectedPrevHash) &&
                    _rollingChainTipHeight == expectedPrevHeight &&
                    _rollingChainWindow.TryGetValue(expectedPrevHeight, out var cachedTip) &&
                    cachedTip?.BlockHash is { Length: 32 } &&
                    BytesEqual32(cachedTip.BlockHash, expectedPrevHash))
                {
                    return _rollingChainWindow;
                }
            }

            var expectedPrevBlock = BlockStore.GetBlockByHash(expectedPrevHash, tx);
            if (expectedPrevBlock == null)
                return null;

            var rebuilt = BuildChainWindow(expectedPrevBlock, tx);
            lock (_rollingChainWindowGate)
            {
                _rollingChainWindow = rebuilt;
                _rollingChainTipHash = (byte[])expectedPrevHash.Clone();
                _rollingChainTipHeight = expectedPrevHeight;
            }

            return rebuilt;
        }

        private void AdvanceRollingChainWindow(Dictionary<ulong, Block> chainWindow, Block block)
        {
            if (chainWindow == null)
                return;

            chainWindow[block.BlockHeight] = block;
            if (block.BlockHeight >= ChainWindowSize)
                chainWindow.Remove(block.BlockHeight - (ulong)ChainWindowSize);

            lock (_rollingChainWindowGate)
            {
                if (!ReferenceEquals(_rollingChainWindow, chainWindow))
                {
                    _rollingChainWindow = chainWindow;
                }

                _rollingChainTipHash = block.BlockHash is { Length: 32 }
                    ? (byte[])block.BlockHash.Clone()
                    : Array.Empty<byte>();
                _rollingChainTipHeight = block.BlockHeight;
            }
        }

        private void InvalidateRollingChainWindow()
        {
            lock (_rollingChainWindowGate)
            {
                _rollingChainWindow = null;
                _rollingChainTipHash = Array.Empty<byte>();
                _rollingChainTipHeight = 0;
            }
        }

        private static Dictionary<ulong, Block> BuildChainWindow(Block tip, SqliteTransaction tx)
        {
            var window = new Dictionary<ulong, Block>(capacity: ChainWindowSize);
            Block? current = tip;

            for (int i = 0; i < ChainWindowSize && current != null; i++)
            {
                window[current.BlockHeight] = current;

                if (current.BlockHeight == 0)
                    break;

                var prevHash = current.Header?.PreviousBlockHash;
                if (prevHash is not { Length: 32 })
                    break;

                current = BlockStore.GetBlockByHash(prevHash, tx);
            }

            return window;
        }
    }
}
