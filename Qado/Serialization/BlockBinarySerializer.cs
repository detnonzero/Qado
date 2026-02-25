using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using Qado.Blockchain;

namespace Qado.Serialization
{
    public static class BlockBinarySerializer
    {
        public const int HeaderSize = ConsensusRules.BlockHeaderSizeBytes;

        public const int MaxBlockSize = ConsensusRules.MaxBlockSizeBytes;
        public const int MaxTxCount = ConsensusRules.MaxTransactionsPerBlock;

        public const int MaxSingleTxSize = ConsensusRules.MaxTransactionSizeBytes;

        public static int GetSize(Block block)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));

            int n = block.Transactions?.Count ?? 0;
            if (n < 0 || n > MaxTxCount)
                throw new ArgumentOutOfRangeException(nameof(block), $"Too many txs: {n} (max {MaxTxCount})");

            int size = HeaderSize;

            for (int i = 0; i < n; i++)
            {
                var tx = block.Transactions![i];
                int txSize = TxBinarySerializer.GetSize(tx);

                if (txSize <= 0) throw new ArgumentException("Invalid tx size.", nameof(block));
                if (txSize > MaxSingleTxSize) throw new ArgumentOutOfRangeException(nameof(block), $"Tx too large: {txSize} bytes (max {MaxSingleTxSize})");

                checked
                {
                    size += 4;      // u32 length
                    size += txSize; // tx payload
                }
            }

            if (size > MaxBlockSize)
                throw new ArgumentOutOfRangeException(nameof(block), $"Block too large: {size} bytes (max {MaxBlockSize})");

            return size;
        }

        public static int Write(Span<byte> dst, Block block)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));

            int needed = GetSize(block);
            if (dst.Length < needed)
                throw new ArgumentException("Destination buffer too small.", nameof(dst));

            var h = block.Header ?? throw new ArgumentException("Block.Header is null.", nameof(block));

            Validate32(h.Miner, nameof(h.Miner));
            Validate32(h.PreviousBlockHash, nameof(h.PreviousBlockHash));
            Validate32(h.MerkleRoot, nameof(h.MerkleRoot));
            Validate32(h.Target, nameof(h.Target));
            if (!Difficulty.IsValidTarget(h.Target))
                throw new ArgumentException("Block header target out of consensus range.", nameof(block));

            int txCount = block.Transactions?.Count ?? 0;
            if (txCount < 0 || txCount > MaxTxCount)
                throw new ArgumentOutOfRangeException(nameof(block), $"Too many txs: {txCount} (max {MaxTxCount})");

            int o = 0;

            dst[o++] = h.Version;

            h.PreviousBlockHash!.AsSpan().CopyTo(dst.Slice(o, 32)); o += 32;
            h.MerkleRoot!.AsSpan().CopyTo(dst.Slice(o, 32)); o += 32;

            BinaryPrimitives.WriteUInt64BigEndian(dst.Slice(o, 8), h.Timestamp); o += 8;

            h.Target!.AsSpan().CopyTo(dst.Slice(o, 32)); o += 32;

            BinaryPrimitives.WriteUInt32BigEndian(dst.Slice(o, 4), h.Nonce); o += 4;

            h.Miner!.AsSpan().CopyTo(dst.Slice(o, 32)); o += 32;

            BinaryPrimitives.WriteUInt32BigEndian(dst.Slice(o, 4), (uint)txCount); o += 4;

            for (int i = 0; i < txCount; i++)
            {
                var tx = block.Transactions![i];
                int txSize = TxBinarySerializer.GetSize(tx);

                if (txSize <= 0) throw new ArgumentException("Invalid tx size.", nameof(block));
                if (txSize > MaxSingleTxSize) throw new ArgumentOutOfRangeException(nameof(block), $"Tx too large: {txSize} bytes (max {MaxSingleTxSize})");

                BinaryPrimitives.WriteUInt32BigEndian(dst.Slice(o, 4), (uint)txSize); o += 4;
                _ = TxBinarySerializer.Write(dst.Slice(o, txSize), tx);
                o += txSize;
            }

            if (o != needed)
                throw new InvalidOperationException($"BlockBinarySerializer.Write wrote {o} bytes, expected {needed}.");

            return o;
        }

        public static Block Read(ReadOnlySpan<byte> src)
        {
            if (src.Length < HeaderSize)
                throw new ArgumentException("Buffer too small for block header.", nameof(src));

            if (src.Length > MaxBlockSize)
                throw new ArgumentException($"Block buffer too large: {src.Length} bytes (max {MaxBlockSize})", nameof(src));

            int o = 0;

            byte ver = src[o++];

            byte[] prev = src.Slice(o, 32).ToArray(); o += 32;
            byte[] merkle = src.Slice(o, 32).ToArray(); o += 32;

            ulong ts = BinaryPrimitives.ReadUInt64BigEndian(src.Slice(o, 8)); o += 8;

            byte[] target = src.Slice(o, 32).ToArray(); o += 32;
            if (!Difficulty.IsValidTarget(target))
                throw new ArgumentException("Block header target out of consensus range.", nameof(src));

            uint nonce = BinaryPrimitives.ReadUInt32BigEndian(src.Slice(o, 4)); o += 4;

            byte[] miner = src.Slice(o, 32).ToArray(); o += 32;

            uint count = BinaryPrimitives.ReadUInt32BigEndian(src.Slice(o, 4)); o += 4;

            if (count > MaxTxCount)
                throw new ArgumentException($"Too many txs in block: {count} (max {MaxTxCount})", nameof(src));

            var header = new BlockHeader
            {
                Version = ver,
                PreviousBlockHash = prev,
                MerkleRoot = merkle,
                Timestamp = ts,
                Target = target,
                Nonce = nonce,
                Miner = miner
            };

            var list = new List<Transaction>((int)count);

            for (uint i = 0; i < count; i++)
            {
                if (src.Length < o + 4)
                    throw new ArgumentException("Buffer truncated at tx length.", nameof(src));

                uint txLen = BinaryPrimitives.ReadUInt32BigEndian(src.Slice(o, 4));
                o += 4;

                if (txLen == 0)
                    throw new ArgumentException("Invalid tx length (0).", nameof(src));

                if (txLen > MaxSingleTxSize)
                    throw new ArgumentException($"Tx too large: {txLen} bytes (max {MaxSingleTxSize}).", nameof(src));

                if (txLen > (uint)(src.Length - o))
                    throw new ArgumentException("Buffer truncated in tx payload.", nameof(src));

                var tx = TxBinarySerializer.Read(src.Slice(o, (int)txLen));
                o += (int)txLen;

                list.Add(tx);
            }

            if (o != src.Length)
                throw new ArgumentException($"Trailing bytes after block payload: {src.Length - o}.", nameof(src));

            return new Block
            {
                Header = header,
                Transactions = list,
                BlockHeight = 0,
                BlockHash = new byte[32] // set and validated by the storage layer
            };
        }

        private static void Validate32(byte[]? v, string name)
        {
            if (v is not { Length: 32 })
                throw new ArgumentException($"{name} must be 32 bytes.");
        }
    }
}

