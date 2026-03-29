using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using Qado.Utils;

namespace Qado.Networking
{
    public readonly record struct SmallNetHelloFrame(
        byte ProtocolVersion,
        byte[] NodeId,
        byte[] TipHash,
        ulong TipHeight,
        UInt128 TipChainwork,
        byte[]? StateDigest);

    public readonly record struct SmallNetTipStateFrame(
        ulong Height,
        byte[] TipHash,
        UInt128 Chainwork,
        byte[]? StateDigest,
        IReadOnlyList<byte[]> RecentCanonical);

    public readonly record struct SmallNetGetAncestorPackFrame(byte[] StartHash, int MaxBlocks);

    public readonly record struct SmallNetAncestorPackFrame(IReadOnlyList<byte[]> Blocks);

    public readonly record struct SmallNetBlocksBatchStartFrame(
        Guid BatchId,
        byte[] ForkHash,
        ulong ForkHeight,
        int TotalBlocks,
        byte[] TipHash,
        UInt128 TipChainwork,
        byte[]? PreviewLastHash = null,
        ulong PreviewLastHeight = 0);

    public readonly record struct SmallNetBlocksBatchEndFrame(
        Guid BatchId,
        byte[] LastHash,
        ulong LastHeight,
        bool MoreAvailable);

    public static class SmallNetSyncProtocol
    {
        public const byte CurrentProtocolVersion = 2;
        public const int BatchMaxBlocks = BlockSyncProtocol.BatchMaxBlocks;
        public const int ExtendedSyncWindowBlocks = BlockSyncProtocol.ExtendedSyncWindowBlocks;
        public const int LegacyChunkBlocks = BlockSyncProtocol.LegacyChunkBlocks;
        public const int MaxAncestorPackBlocks = 16;
        public const int MaxRecentCanonicalHashes = 16;
        public const int StateDigestBytes = 32;
        public const int NodeIdBytes = 32;
        public const int HashBytes = 32;
        public const int GuidBytes = 16;
        public const int MaxSerializedBlockBytes = BlockSyncProtocol.MaxSerializedBlockBytes;
        public static readonly int MaxAncestorPackPayloadBytes =
            4 + (MaxAncestorPackBlocks * (4 + MaxSerializedBlockBytes));
        public static readonly int MaxTipStatePayloadBytes =
            8 + HashBytes + 16 + 1 + StateDigestBytes + 1 + (MaxRecentCanonicalHashes * HashBytes);

        public static byte[] BuildHello(SmallNetHelloFrame frame)
        {
            if (frame.NodeId is not { Length: NodeIdBytes })
                throw new ArgumentException("node id must be 32 bytes", nameof(frame));
            if (frame.TipHash is not { Length: HashBytes })
                throw new ArgumentException("tip hash must be 32 bytes", nameof(frame));
            if (frame.StateDigest != null && frame.StateDigest.Length != StateDigestBytes)
                throw new ArgumentException("state digest must be 32 bytes", nameof(frame));

            var payload = new byte[1 + NodeIdBytes + 8 + HashBytes + 16 + 1 + (frame.StateDigest?.Length ?? 0)];
            payload[0] = frame.ProtocolVersion;
            Buffer.BlockCopy(frame.NodeId, 0, payload, 1, NodeIdBytes);
            BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(1 + NodeIdBytes, 8), frame.TipHeight);
            Buffer.BlockCopy(frame.TipHash, 0, payload, 1 + NodeIdBytes + 8, HashBytes);
            U128.WriteBE(payload.AsSpan(1 + NodeIdBytes + 8 + HashBytes, 16), frame.TipChainwork);
            payload[1 + NodeIdBytes + 8 + HashBytes + 16] = frame.StateDigest == null ? (byte)0 : (byte)1;
            if (frame.StateDigest != null)
            {
                Buffer.BlockCopy(
                    frame.StateDigest,
                    0,
                    payload,
                    1 + NodeIdBytes + 8 + HashBytes + 16 + 1,
                    StateDigestBytes);
            }

            return payload;
        }

        public static bool TryParseHello(byte[] payload, out SmallNetHelloFrame frame)
        {
            frame = default;
            if (payload == null || payload.Length < 1 + NodeIdBytes + 8 + HashBytes + 16 + 1)
                return false;

            int offset = 0;
            byte protocolVersion = payload[offset++];
            byte[] nodeId = payload.AsSpan(offset, NodeIdBytes).ToArray();
            offset += NodeIdBytes;
            ulong tipHeight = BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(offset, 8));
            offset += 8;
            byte[] tipHash = payload.AsSpan(offset, HashBytes).ToArray();
            offset += HashBytes;
            UInt128 tipChainwork = U128.ReadBE(payload.AsSpan(offset, 16));
            offset += 16;
            bool hasStateDigest = payload[offset++] != 0;

            byte[]? stateDigest = null;
            if (hasStateDigest)
            {
                if (payload.Length != offset + StateDigestBytes)
                    return false;
                stateDigest = payload.AsSpan(offset, StateDigestBytes).ToArray();
                offset += StateDigestBytes;
            }

            if (offset != payload.Length)
                return false;

            frame = new SmallNetHelloFrame(protocolVersion, nodeId, tipHash, tipHeight, tipChainwork, stateDigest);
            return true;
        }

        public static byte[] BuildTipState(SmallNetTipStateFrame frame)
        {
            if (frame.TipHash is not { Length: HashBytes })
                throw new ArgumentException("tip hash must be 32 bytes", nameof(frame));
            if (frame.StateDigest != null && frame.StateDigest.Length != StateDigestBytes)
                throw new ArgumentException("state digest must be 32 bytes", nameof(frame));

            int recentCount = Math.Min(frame.RecentCanonical?.Count ?? 0, MaxRecentCanonicalHashes);
            var payload = new byte[8 + HashBytes + 16 + 1 + (frame.StateDigest?.Length ?? 0) + 1 + (recentCount * HashBytes)];
            int offset = 0;
            BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(offset, 8), frame.Height);
            offset += 8;
            Buffer.BlockCopy(frame.TipHash, 0, payload, offset, HashBytes);
            offset += HashBytes;
            U128.WriteBE(payload.AsSpan(offset, 16), frame.Chainwork);
            offset += 16;
            payload[offset++] = frame.StateDigest == null ? (byte)0 : (byte)1;
            if (frame.StateDigest != null)
            {
                Buffer.BlockCopy(frame.StateDigest, 0, payload, offset, StateDigestBytes);
                offset += StateDigestBytes;
            }

            payload[offset++] = (byte)recentCount;
            for (int i = 0; i < recentCount; i++)
            {
                var hash = frame.RecentCanonical![i];
                if (hash is not { Length: HashBytes })
                    throw new ArgumentException("recent canonical hash must be 32 bytes", nameof(frame));

                Buffer.BlockCopy(hash, 0, payload, offset, HashBytes);
                offset += HashBytes;
            }

            return payload;
        }

        public static bool TryParseTipState(byte[] payload, out SmallNetTipStateFrame frame)
        {
            frame = default;
            if (payload == null || payload.Length < 8 + HashBytes + 16 + 2)
                return false;

            int offset = 0;
            ulong height = BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(offset, 8));
            offset += 8;
            byte[] tipHash = payload.AsSpan(offset, HashBytes).ToArray();
            offset += HashBytes;
            UInt128 chainwork = U128.ReadBE(payload.AsSpan(offset, 16));
            offset += 16;
            bool hasStateDigest = payload[offset++] != 0;

            byte[]? stateDigest = null;
            if (hasStateDigest)
            {
                if (offset + StateDigestBytes + 1 > payload.Length)
                    return false;
                stateDigest = payload.AsSpan(offset, StateDigestBytes).ToArray();
                offset += StateDigestBytes;
            }

            int recentCount = payload[offset++];
            if (recentCount < 0 || recentCount > MaxRecentCanonicalHashes)
                return false;
            if (offset + (recentCount * HashBytes) != payload.Length)
                return false;

            var recent = new List<byte[]>(recentCount);
            for (int i = 0; i < recentCount; i++)
            {
                recent.Add(payload.AsSpan(offset, HashBytes).ToArray());
                offset += HashBytes;
            }

            frame = new SmallNetTipStateFrame(height, tipHash, chainwork, stateDigest, recent);
            return true;
        }

        public static byte[] BuildGetAncestorPack(byte[] startHash, int maxBlocks = MaxAncestorPackBlocks)
        {
            if (startHash is not { Length: HashBytes })
                throw new ArgumentException("start hash must be 32 bytes", nameof(startHash));

            int normalized = NormalizeAncestorRequest(maxBlocks);
            var payload = new byte[HashBytes + 4];
            Buffer.BlockCopy(startHash, 0, payload, 0, HashBytes);
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(HashBytes, 4), (uint)normalized);
            return payload;
        }

        public static bool TryParseGetAncestorPack(byte[] payload, out SmallNetGetAncestorPackFrame frame)
        {
            frame = default;
            if (payload == null || payload.Length != HashBytes + 4)
                return false;

            byte[] startHash = payload.AsSpan(0, HashBytes).ToArray();
            uint rawMax = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(HashBytes, 4));
            if (rawMax == 0 || rawMax > MaxAncestorPackBlocks)
                return false;

            frame = new SmallNetGetAncestorPackFrame(startHash, (int)rawMax);
            return true;
        }

        public static byte[] BuildAncestorPack(IReadOnlyList<byte[]> blocks)
        {
            blocks ??= Array.Empty<byte[]>();
            if (blocks.Count == 0 || blocks.Count > MaxAncestorPackBlocks)
                throw new ArgumentOutOfRangeException(nameof(blocks));

            int totalBytes = 4;
            for (int i = 0; i < blocks.Count; i++)
            {
                var block = blocks[i];
                if (block == null || block.Length == 0 || block.Length > MaxSerializedBlockBytes)
                    throw new ArgumentException("invalid ancestor pack block", nameof(blocks));
                totalBytes += 4 + block.Length;
            }

            var payload = new byte[totalBytes];
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), (uint)blocks.Count);
            int offset = 4;
            for (int i = 0; i < blocks.Count; i++)
            {
                var block = blocks[i];
                BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(offset, 4), (uint)block.Length);
                offset += 4;
                Buffer.BlockCopy(block, 0, payload, offset, block.Length);
                offset += block.Length;
            }

            return payload;
        }

        public static bool TryParseAncestorPack(byte[] payload, out SmallNetAncestorPackFrame frame)
        {
            frame = default;
            if (payload == null || payload.Length < 8)
                return false;

            uint count = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(0, 4));
            if (count == 0 || count > MaxAncestorPackBlocks)
                return false;

            int offset = 4;
            var blocks = new List<byte[]>((int)count);
            for (int i = 0; i < count; i++)
            {
                if (offset + 4 > payload.Length)
                    return false;

                uint len = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(offset, 4));
                offset += 4;
                if (len == 0 || len > MaxSerializedBlockBytes || offset + len > payload.Length)
                    return false;

                blocks.Add(payload.AsSpan(offset, (int)len).ToArray());
                offset += (int)len;
            }

            if (offset != payload.Length)
                return false;

            frame = new SmallNetAncestorPackFrame(blocks);
            return true;
        }

        public static byte[] BuildBlocksBatchStart(SmallNetBlocksBatchStartFrame frame)
        {
            if (frame.ForkHash is not { Length: HashBytes })
                throw new ArgumentException("fork hash must be 32 bytes", nameof(frame));
            if (frame.TipHash is not { Length: HashBytes })
                throw new ArgumentException("tip hash must be 32 bytes", nameof(frame));
            if (frame.TotalBlocks < 0 || frame.TotalBlocks > ExtendedSyncWindowBlocks)
                throw new ArgumentOutOfRangeException(nameof(frame));
            if (frame.PreviewLastHash != null && frame.PreviewLastHash.Length != HashBytes)
                throw new ArgumentException("preview last hash must be 32 bytes", nameof(frame));

            bool includePreview = frame.PreviewLastHash is { Length: HashBytes };
            var payload = new byte[
                GuidBytes + HashBytes + 8 + 4 + HashBytes + 16 +
                (includePreview ? HashBytes + 8 : 0)];
            frame.BatchId.TryWriteBytes(payload.AsSpan(0, GuidBytes));
            Buffer.BlockCopy(frame.ForkHash, 0, payload, GuidBytes, HashBytes);
            BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(GuidBytes + HashBytes, 8), frame.ForkHeight);
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(GuidBytes + HashBytes + 8, 4), (uint)frame.TotalBlocks);
            Buffer.BlockCopy(frame.TipHash, 0, payload, GuidBytes + HashBytes + 8 + 4, HashBytes);
            U128.WriteBE(payload.AsSpan(GuidBytes + HashBytes + 8 + 4 + HashBytes, 16), frame.TipChainwork);

            if (includePreview)
            {
                int offset = GuidBytes + HashBytes + 8 + 4 + HashBytes + 16;
                Buffer.BlockCopy(frame.PreviewLastHash!, 0, payload, offset, HashBytes);
                BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(offset + HashBytes, 8), frame.PreviewLastHeight);
            }

            return payload;
        }

        public static bool TryParseBlocksBatchStart(byte[] payload, out SmallNetBlocksBatchStartFrame frame)
        {
            frame = default;
            int baseLength = GuidBytes + HashBytes + 8 + 4 + HashBytes + 16;
            int previewLength = baseLength + HashBytes + 8;
            if (payload == null || (payload.Length != baseLength && payload.Length != previewLength))
                return false;

            Guid batchId = new(payload.AsSpan(0, GuidBytes));
            byte[] forkHash = payload.AsSpan(GuidBytes, HashBytes).ToArray();
            ulong forkHeight = BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(GuidBytes + HashBytes, 8));
            uint totalBlocks = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(GuidBytes + HashBytes + 8, 4));
            if (totalBlocks > ExtendedSyncWindowBlocks)
                return false;

            byte[] tipHash = payload.AsSpan(GuidBytes + HashBytes + 8 + 4, HashBytes).ToArray();
            UInt128 tipChainwork = U128.ReadBE(payload.AsSpan(GuidBytes + HashBytes + 8 + 4 + HashBytes, 16));
            byte[]? previewLastHash = null;
            ulong previewLastHeight = 0;
            if (payload.Length == previewLength)
            {
                int offset = baseLength;
                previewLastHash = payload.AsSpan(offset, HashBytes).ToArray();
                previewLastHeight = BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(offset + HashBytes, 8));
            }

            frame = new SmallNetBlocksBatchStartFrame(
                batchId,
                forkHash,
                forkHeight,
                (int)totalBlocks,
                tipHash,
                tipChainwork,
                previewLastHash,
                previewLastHeight);
            return true;
        }

        public static byte[] BuildBlocksBatchData(ulong firstHeight, IReadOnlyList<byte[]> blocks)
            => BlockSyncProtocol.BuildBlockBatch(firstHeight, blocks);

        public static bool TryParseBlocksBatchData(byte[] payload, out BlockBatchFrame frame)
            => BlockSyncProtocol.TryParseBlockBatch(payload, out frame);

        public static byte[] BuildBlocksChunk(ulong firstHeight, IReadOnlyList<byte[]> blocks)
            => BlockSyncProtocol.BuildBlockChunk(firstHeight, blocks);

        public static bool TryParseBlocksChunk(byte[] payload, out BlockChunkFrame frame)
            => BlockSyncProtocol.TryParseBlockChunk(payload, out frame);

        public static byte[] BuildBlocksBatchEnd(SmallNetBlocksBatchEndFrame frame)
        {
            if (frame.LastHash is not { Length: HashBytes })
                throw new ArgumentException("last hash must be 32 bytes", nameof(frame));

            var payload = new byte[GuidBytes + HashBytes + 8 + 1];
            frame.BatchId.TryWriteBytes(payload.AsSpan(0, GuidBytes));
            Buffer.BlockCopy(frame.LastHash, 0, payload, GuidBytes, HashBytes);
            BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(GuidBytes + HashBytes, 8), frame.LastHeight);
            payload[GuidBytes + HashBytes + 8] = frame.MoreAvailable ? (byte)1 : (byte)0;
            return payload;
        }

        public static bool TryParseBlocksBatchEnd(byte[] payload, out SmallNetBlocksBatchEndFrame frame)
        {
            frame = default;
            if (payload == null || payload.Length != GuidBytes + HashBytes + 8 + 1)
                return false;

            Guid batchId = new(payload.AsSpan(0, GuidBytes));
            byte[] lastHash = payload.AsSpan(GuidBytes, HashBytes).ToArray();
            ulong lastHeight = BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(GuidBytes + HashBytes, 8));
            bool moreAvailable = payload[GuidBytes + HashBytes + 8] != 0;
            frame = new SmallNetBlocksBatchEndFrame(batchId, lastHash, lastHeight, moreAvailable);
            return true;
        }

        private static int NormalizeAncestorRequest(int maxBlocks)
        {
            if (maxBlocks <= 0)
                return MaxAncestorPackBlocks;
            return Math.Min(maxBlocks, MaxAncestorPackBlocks);
        }
    }
}
