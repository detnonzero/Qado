using System;
using System.Buffers.Binary;
using System.Collections.Generic;

namespace Qado.Networking
{
    public enum BlocksEndStatus : byte
    {
        TipReached = 0,
        MoreAvailable = 1
    }

    public readonly record struct BlockChunkFrame(ulong FirstHeight, IReadOnlyList<byte[]> Blocks);

    public static class BlockSyncProtocol
    {
        public const int BatchMaxBlocks = 4096;
        public const int ChunkBlocks = 64;
        public const int MaxLocatorHashes = 64;
        public const int MaxSerializedBlockBytes = 64 * 1024;
        public static readonly int MaxBlockChunkPayloadBytes =
            8 + 4 + (ChunkBlocks * (4 + MaxSerializedBlockBytes));
        public static readonly int MaxFramePayloadBytes = Math.Max(
            MaxBlockChunkPayloadBytes,
            4 + (MaxLocatorHashes * 32) + 4);

        public static byte[] BuildGetBlocksByLocator(IReadOnlyList<byte[]> locatorHashes, int maxBlocks = BatchMaxBlocks)
        {
            locatorHashes ??= Array.Empty<byte[]>();
            int count = Math.Min(locatorHashes.Count, MaxLocatorHashes);
            int normalizedMax = NormalizeMaxBlocks(maxBlocks);
            var payload = new byte[4 + (count * 32) + 4];
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), (uint)count);

            int offset = 4;
            for (int i = 0; i < count; i++)
            {
                var hash = locatorHashes[i];
                if (hash is not { Length: 32 })
                    throw new ArgumentException("locator hash must be 32 bytes", nameof(locatorHashes));

                hash.CopyTo(payload, offset);
                offset += 32;
            }

            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(offset, 4), (uint)normalizedMax);
            return payload;
        }

        public static bool TryParseGetBlocksByLocator(
            byte[] payload,
            out List<byte[]> locatorHashes,
            out int maxBlocks)
        {
            locatorHashes = new List<byte[]>();
            maxBlocks = BatchMaxBlocks;

            if (payload == null || payload.Length < 8)
                return false;

            uint count = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(0, 4));
            if (count > MaxLocatorHashes)
                return false;

            int needed = 4 + checked((int)count * 32) + 4;
            if (payload.Length != needed)
                return false;

            int offset = 4;
            for (int i = 0; i < count; i++)
            {
                locatorHashes.Add(payload.AsSpan(offset, 32).ToArray());
                offset += 32;
            }

            uint rawMax = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(offset, 4));
            if (rawMax == 0)
                return false;

            maxBlocks = NormalizeMaxBlocks((int)rawMax);
            return true;
        }

        public static byte[] BuildGetBlocksFrom(byte[] fromHash, int maxBlocks = BatchMaxBlocks)
        {
            if (fromHash is not { Length: 32 })
                throw new ArgumentException("fromHash must be 32 bytes", nameof(fromHash));

            int normalizedMax = NormalizeMaxBlocks(maxBlocks);
            var payload = new byte[32 + 4];
            fromHash.CopyTo(payload, 0);
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(32, 4), (uint)normalizedMax);
            return payload;
        }

        public static bool TryParseGetBlocksFrom(byte[] payload, out byte[] fromHash, out int maxBlocks)
        {
            fromHash = Array.Empty<byte>();
            maxBlocks = BatchMaxBlocks;

            if (payload == null || payload.Length != 36)
                return false;

            fromHash = payload.AsSpan(0, 32).ToArray();
            uint rawMax = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(32, 4));
            if (rawMax == 0)
                return false;

            maxBlocks = NormalizeMaxBlocks((int)rawMax);
            return true;
        }

        public static byte[] BuildBlockChunk(ulong firstHeight, IReadOnlyList<byte[]> blocks)
        {
            blocks ??= Array.Empty<byte[]>();
            if (blocks.Count == 0 || blocks.Count > ChunkBlocks)
                throw new ArgumentOutOfRangeException(nameof(blocks));

            int totalSize = 8 + 4;
            for (int i = 0; i < blocks.Count; i++)
            {
                var block = blocks[i];
                if (block == null || block.Length == 0 || block.Length > MaxSerializedBlockBytes)
                    throw new ArgumentException("invalid block blob length", nameof(blocks));

                checked
                {
                    totalSize += 4;
                    totalSize += block.Length;
                }
            }

            var payload = new byte[totalSize];
            BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(0, 8), firstHeight);
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(8, 4), (uint)blocks.Count);

            int offset = 12;
            for (int i = 0; i < blocks.Count; i++)
            {
                var block = blocks[i];
                BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(offset, 4), (uint)block.Length);
                offset += 4;
                block.CopyTo(payload, offset);
                offset += block.Length;
            }

            return payload;
        }

        public static bool TryParseBlockChunk(byte[] payload, out BlockChunkFrame frame)
        {
            frame = default;
            if (payload == null || payload.Length < 12)
                return false;

            ulong firstHeight = BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(0, 8));
            uint count = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(8, 4));
            if (count == 0 || count > ChunkBlocks)
                return false;

            var blocks = new List<byte[]>((int)count);
            int offset = 12;
            for (int i = 0; i < count; i++)
            {
                if (offset + 4 > payload.Length)
                    return false;

                uint len = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(offset, 4));
                offset += 4;
                if (len == 0 || len > MaxSerializedBlockBytes)
                    return false;
                if (offset + len > payload.Length)
                    return false;

                blocks.Add(payload.AsSpan(offset, (int)len).ToArray());
                offset += (int)len;
            }

            if (offset != payload.Length)
                return false;

            frame = new BlockChunkFrame(firstHeight, blocks);
            return true;
        }

        public static List<byte[]> BuildChunkPayloads(IReadOnlyList<byte[]> blocks, ulong startHeight = 0)
        {
            blocks ??= Array.Empty<byte[]>();
            var payloads = new List<byte[]>((blocks.Count + ChunkBlocks - 1) / ChunkBlocks);
            int offset = 0;
            ulong height = startHeight;

            while (offset < blocks.Count)
            {
                int take = Math.Min(ChunkBlocks, blocks.Count - offset);
                var slice = new byte[take][];
                for (int i = 0; i < take; i++)
                    slice[i] = blocks[offset + i];

                payloads.Add(BuildBlockChunk(height, slice));
                height += (ulong)take;
                offset += take;
            }

            return payloads;
        }

        private static int NormalizeMaxBlocks(int maxBlocks)
        {
            if (maxBlocks <= 0)
                return BatchMaxBlocks;
            return Math.Min(maxBlocks, BatchMaxBlocks);
        }
    }
}
