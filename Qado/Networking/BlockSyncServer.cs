using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Qado.Logging;
using Qado.Storage;

namespace Qado.Networking
{
    public static class BlockSyncServer
    {
        public static async Task HandleGetBlocksByLocatorAsync(
            byte[] payload,
            PeerSession peer,
            Func<PeerSession, MsgType, byte[], CancellationToken, Task> sendFrameAsync,
            ILogSink? log,
            CancellationToken ct)
        {
            if (peer == null) throw new ArgumentNullException(nameof(peer));
            if (sendFrameAsync == null) throw new ArgumentNullException(nameof(sendFrameAsync));

            if (!BlockSyncProtocol.TryParseGetBlocksByLocator(payload, out var locatorHashes, out var maxBlocks))
                return;

            if (!TryFindForkPoint(locatorHashes, out var forkHash, out var forkHeight))
            {
                await sendFrameAsync(peer, MsgType.NoCommonAncestor, Array.Empty<byte>(), ct).ConfigureAwait(false);
                return;
            }

            await StreamCanonicalChildrenAsync(peer, forkHash, forkHeight, maxBlocks, sendFrameAsync, log, ct).ConfigureAwait(false);
        }

        public static async Task HandleGetBlocksFromAsync(
            byte[] payload,
            PeerSession peer,
            Func<PeerSession, MsgType, byte[], CancellationToken, Task> sendFrameAsync,
            ILogSink? log,
            CancellationToken ct)
        {
            if (peer == null) throw new ArgumentNullException(nameof(peer));
            if (sendFrameAsync == null) throw new ArgumentNullException(nameof(sendFrameAsync));

            if (!BlockSyncProtocol.TryParseGetBlocksFrom(payload, out var fromHash, out var maxBlocks))
                return;

            if (!TryGetCanonicalHeight(fromHash, out var forkHeight))
            {
                await sendFrameAsync(peer, MsgType.NoCommonAncestor, Array.Empty<byte>(), ct).ConfigureAwait(false);
                return;
            }

            await StreamCanonicalChildrenAsync(peer, fromHash, forkHeight, maxBlocks, sendFrameAsync, log, ct).ConfigureAwait(false);
        }

        public static bool TryFindForkPoint(IReadOnlyList<byte[]> locatorHashes, out byte[] forkHash, out ulong forkHeight)
        {
            forkHash = Array.Empty<byte>();
            forkHeight = 0;

            if (locatorHashes == null || locatorHashes.Count == 0)
                return false;

            for (int i = 0; i < locatorHashes.Count; i++)
            {
                var hash = locatorHashes[i];
                if (!TryGetCanonicalHeight(hash, out var height))
                    continue;

                forkHash = (byte[])hash.Clone();
                forkHeight = height;
                return true;
            }

            return false;
        }

        private static async Task StreamCanonicalChildrenAsync(
            PeerSession peer,
            byte[] forkHash,
            ulong forkHeight,
            int maxBlocks,
            Func<PeerSession, MsgType, byte[], CancellationToken, Task> sendFrameAsync,
            ILogSink? log,
            CancellationToken ct)
        {
            ulong tipHeight = BlockStore.GetLatestHeight();
            int totalBlocks = 0;
            if (tipHeight > forkHeight)
            {
                ulong available = tipHeight - forkHeight;
                totalBlocks = (int)Math.Min(available, (ulong)Math.Min(maxBlocks, BlockSyncProtocol.BatchMaxBlocks));
            }

            ulong startHeight = forkHeight + 1UL;
            Guid batchId = Guid.NewGuid();
            await sendFrameAsync(
                peer,
                MsgType.BlocksBatchStart,
                SmallNetSyncProtocol.BuildBlocksBatchStart(new SmallNetBlocksBatchStartFrame(
                    batchId,
                    forkHash,
                    forkHeight,
                    totalBlocks,
                    BlockStore.GetCanonicalHashAtHeight(tipHeight) ?? new byte[32],
                    TryGetTipChainwork(tipHeight))),
                ct).ConfigureAwait(false);

            int sentBlocks = 0;
            ulong currentHeight = startHeight;
            while (sentBlocks < totalBlocks)
            {
                int chunkTake = Math.Min(BlockSyncProtocol.ChunkBlocks, totalBlocks - sentBlocks);
                var chunk = new List<byte[]>(chunkTake);
                ulong firstHeight = currentHeight;

                for (int i = 0; i < chunkTake; i++)
                {
                    if (!BlockStore.TryGetSerializedCanonicalBlockAtHeight(currentHeight, out var blockBlob))
                    {
                        log?.Warn("Sync", $"BlockSyncServer missing canonical payload at height {currentHeight}; terminating stream early.");
                        await sendFrameAsync(
                                peer,
                                MsgType.BlocksBatchEnd,
                                SmallNetSyncProtocol.BuildBlocksBatchEnd(new SmallNetBlocksBatchEndFrame(
                                    batchId,
                                    forkHash,
                                    forkHeight,
                                    MoreAvailable: true)),
                                ct)
                            .ConfigureAwait(false);
                        return;
                    }

                    chunk.Add(blockBlob);
                    currentHeight++;
                }

                await sendFrameAsync(
                    peer,
                    MsgType.BlocksChunk,
                    SmallNetSyncProtocol.BuildBlocksChunk(firstHeight, chunk),
                    ct).ConfigureAwait(false);

                sentBlocks += chunk.Count;
            }

            var status = (forkHeight + (ulong)totalBlocks) >= tipHeight
                ? BlocksEndStatus.TipReached
                : BlocksEndStatus.MoreAvailable;

            byte[] lastHash = totalBlocks > 0
                ? BlockStore.GetCanonicalHashAtHeight(forkHeight + (ulong)totalBlocks) ?? forkHash
                : forkHash;
            await sendFrameAsync(
                peer,
                MsgType.BlocksBatchEnd,
                SmallNetSyncProtocol.BuildBlocksBatchEnd(new SmallNetBlocksBatchEndFrame(
                    batchId,
                    lastHash,
                    forkHeight + (ulong)totalBlocks,
                    status == BlocksEndStatus.MoreAvailable)),
                ct).ConfigureAwait(false);
        }

        private static UInt128 TryGetTipChainwork(ulong tipHeight)
        {
            var tipHash = BlockStore.GetCanonicalHashAtHeight(tipHeight);
            if (tipHash is not { Length: 32 })
                return 0;

            return BlockIndexStore.GetChainwork(tipHash);
        }

        private static bool TryGetCanonicalHeight(byte[] hash, out ulong height)
        {
            height = 0;
            if (hash is not { Length: 32 })
                return false;

            if (!BlockIndexStore.TryGetMeta(hash, out var candidateHeight, out _, out _))
                return false;

            var canonHash = BlockStore.GetCanonicalHashAtHeight(candidateHeight);
            if (canonHash is not { Length: 32 })
                return false;

            if (!BytesEqual32(canonHash, hash))
                return false;

            height = candidateHeight;
            return true;
        }

        private static bool BytesEqual32(byte[] a, byte[] b)
        {
            if (a is not { Length: 32 } || b is not { Length: 32 })
                return false;

            int diff = 0;
            for (int i = 0; i < 32; i++) diff |= a[i] ^ b[i];
            return diff == 0;
        }
    }
}
