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
            bool useBatchData = peer.Supports(HandshakeCapabilities.BlocksBatchData);
            bool useExtendedWindow = useBatchData && peer.Supports(HandshakeCapabilities.ExtendedSyncWindow);
            bool usePreview = useBatchData && peer.Supports(HandshakeCapabilities.SyncWindowPreview);
            int maxWindowBlocks = useExtendedWindow
                ? BlockSyncProtocol.ExtendedSyncWindowBlocks
                : BlockSyncProtocol.BatchMaxBlocks;
            int requestedBlocks = 0;
            if (tipHeight > forkHeight)
            {
                ulong available = tipHeight - forkHeight;
                requestedBlocks = (int)Math.Min(available, (ulong)Math.Min(maxBlocks, maxWindowBlocks));
            }

            ulong startHeight = forkHeight + 1UL;
            bool rangeComplete = true;
            ulong firstMissingHeight = startHeight;
            List<(ulong Height, byte[] Hash, byte[] Payload)> blocks = new();
            if (requestedBlocks > 0)
            {
                rangeComplete = BlockStore.TryGetSerializedCanonicalBlocksRange(
                    startHeight,
                    requestedBlocks,
                    out blocks,
                    out firstMissingHeight);
                if (!rangeComplete)
                {
                    log?.Warn(
                        "Sync",
                        $"BlockSyncServer canonical range read stopped at height {firstMissingHeight}; requested={requestedBlocks}, availablePayloads={blocks.Count}.");
                }
            }

            int totalBlocks = blocks.Count;
            Guid batchId = Guid.NewGuid();
            ulong lastHeight = totalBlocks > 0 ? blocks[^1].Height : forkHeight;
            byte[] lastHash = totalBlocks > 0 ? (byte[])blocks[^1].Hash.Clone() : forkHash;
            if (!BlockStore.TryGetCanonicalHashAndChainworkAtHeight(tipHeight, out var tipHash, out var tipChainwork))
            {
                tipHash = new byte[32];
                tipChainwork = 0;
            }
            await sendFrameAsync(
                peer,
                MsgType.BlocksBatchStart,
                SmallNetSyncProtocol.BuildBlocksBatchStart(new SmallNetBlocksBatchStartFrame(
                    batchId,
                    forkHash,
                    forkHeight,
                    totalBlocks,
                    tipHash,
                    tipChainwork,
                    usePreview ? lastHash : null,
                    usePreview ? lastHeight : 0UL)),
                ct).ConfigureAwait(false);

            int sentBlocks = 0;
            while (sentBlocks < totalBlocks)
            {
                int take = Math.Min(
                    useBatchData ? BlockSyncProtocol.BatchMaxBlocks : BlockSyncProtocol.LegacyChunkBlocks,
                    totalBlocks - sentBlocks);
                var payloads = new List<byte[]>(take);
                ulong firstHeight = blocks[sentBlocks].Height;
                for (int i = 0; i < take; i++)
                    payloads.Add(blocks[sentBlocks + i].Payload);

                await sendFrameAsync(
                    peer,
                    useBatchData ? MsgType.BlocksBatchData : MsgType.BlocksChunk,
                    useBatchData
                        ? SmallNetSyncProtocol.BuildBlocksBatchData(firstHeight, payloads)
                        : SmallNetSyncProtocol.BuildBlocksChunk(firstHeight, payloads),
                    ct).ConfigureAwait(false);

                sentBlocks += payloads.Count;
            }

            var status = rangeComplete && lastHeight >= tipHeight
                ? BlocksEndStatus.TipReached
                : BlocksEndStatus.MoreAvailable;

            await sendFrameAsync(
                peer,
                MsgType.BlocksBatchEnd,
                SmallNetSyncProtocol.BuildBlocksBatchEnd(new SmallNetBlocksBatchEndFrame(
                    batchId,
                    lastHash,
                    lastHeight,
                    status == BlocksEndStatus.MoreAvailable)),
                ct).ConfigureAwait(false);
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
