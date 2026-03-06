using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using Qado.Utils;

namespace Qado.Networking
{
    public enum BlocksEndStatus : byte
    {
        TipReached = 0,
        MoreAvailable = 1
    }

    public readonly record struct BlocksBeginFrame(byte[] StartHash, ulong StartHeight, int TotalBlocks);

    public readonly record struct BlockChunkFrame(ulong FirstHeight, IReadOnlyList<byte[]> Blocks);

    public readonly record struct TipFrame(ulong Height, byte[] Hash, UInt128 Chainwork);

    public static class BlockSyncProtocol
    {
        public const int BatchMaxBlocks = 1440;
        public const int ChunkBlocks = 64;
        public const int MaxLocatorHashes = 64;
        public const int MaxSerializedBlockBytes = 64 * 1024;
        public const int TipPayloadBytesLegacy = 8 + 32;
        public const int TipPayloadBytes = TipPayloadBytesLegacy + 16;
        public static readonly int MaxBlockChunkPayloadBytes =
            8 + 4 + (ChunkBlocks * (4 + MaxSerializedBlockBytes));
        public static readonly int MaxFramePayloadBytes = Math.Max(
            MaxBlockChunkPayloadBytes,
            Math.Max(
                4 + (MaxLocatorHashes * 32) + 4,
                Math.Max(32 + 4, TipPayloadBytes)));

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

        public static byte[] BuildBlocksBegin(byte[] startHash, ulong startHeight, int totalBlocks)
        {
            if (startHash is not { Length: 32 })
                throw new ArgumentException("startHash must be 32 bytes", nameof(startHash));
            if (totalBlocks < 0 || totalBlocks > BatchMaxBlocks)
                throw new ArgumentOutOfRangeException(nameof(totalBlocks));

            var payload = new byte[32 + 8 + 4];
            startHash.CopyTo(payload, 0);
            BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(32, 8), startHeight);
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(40, 4), (uint)totalBlocks);
            return payload;
        }

        public static bool TryParseBlocksBegin(byte[] payload, out BlocksBeginFrame frame)
        {
            frame = default;
            if (payload == null || payload.Length != 44)
                return false;

            byte[] startHash = payload.AsSpan(0, 32).ToArray();
            ulong startHeight = BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(32, 8));
            uint totalBlocks = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(40, 4));
            if (totalBlocks > BatchMaxBlocks)
                return false;

            frame = new BlocksBeginFrame(startHash, startHeight, (int)totalBlocks);
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

        public static byte[] BuildBlocksEnd(BlocksEndStatus status)
            => new[] { (byte)status };

        public static bool TryParseBlocksEnd(byte[] payload, out BlocksEndStatus status)
        {
            status = BlocksEndStatus.TipReached;
            if (payload == null || payload.Length != 1)
                return false;

            if (!Enum.IsDefined(typeof(BlocksEndStatus), payload[0]))
                return false;

            status = (BlocksEndStatus)payload[0];
            return true;
        }

        public static byte[] BuildTipPayload(ulong height, byte[] hash, UInt128 chainwork)
        {
            if (hash is not { Length: 32 })
                throw new ArgumentException("tip hash must be 32 bytes", nameof(hash));

            var payload = new byte[TipPayloadBytes];
            BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(0, 8), height);
            hash.CopyTo(payload, 8);
            U128.WriteBE(payload.AsSpan(TipPayloadBytesLegacy, 16), chainwork);
            return payload;
        }

        public static bool TryParseTipPayload(byte[] payload, out TipFrame frame)
        {
            frame = default;
            if (payload == null || (payload.Length != TipPayloadBytesLegacy && payload.Length != TipPayloadBytes))
                return false;

            ulong height = BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(0, 8));
            byte[] hash = payload.AsSpan(8, 32).ToArray();
            UInt128 chainwork = payload.Length == TipPayloadBytes
                ? U128.ReadBE(payload.AsSpan(TipPayloadBytesLegacy, 16))
                : 0;
            frame = new TipFrame(height, hash, chainwork);
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
