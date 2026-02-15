using System;
using System.Buffers.Binary;
using System.Numerics;

namespace Qado.Blockchain
{
    public sealed class BlockHeader
    {
        public const int HashSize = 32;

        public const int PowHeaderSize = 1 + 32 + 32 + 8 + 32 + 4 + 32;

        public byte Version { get; set; } = 1;

        public byte[] PreviousBlockHash
        {
            get => _previousBlockHash;
            set => _previousBlockHash = Ensure32(value, nameof(PreviousBlockHash));
        }

        public byte[] MerkleRoot
        {
            get => _merkleRoot;
            set => _merkleRoot = Ensure32(value, nameof(MerkleRoot));
        }

        public ulong Timestamp { get; set; }

        public byte[] Target
        {
            get => _target;
            set => _target = Ensure32(value, nameof(Target));
        }

        public uint Nonce { get; set; }

        public byte[] Miner
        {
            get => _miner;
            set => _miner = Ensure32(value, nameof(Miner));
        }

        public BigInteger DifficultyValue
        {
            get => global::Qado.Blockchain.Difficulty.TargetToDifficulty(_target);
            set => _target = Ensure32(global::Qado.Blockchain.Difficulty.DifficultyToTarget(value), nameof(Target));
        }

        public BigInteger Difficulty
        {
            get => DifficultyValue;
            set => DifficultyValue = value;
        }

        private byte[] _previousBlockHash;
        private byte[] _merkleRoot;
        private byte[] _target;
        private byte[] _miner;

        public BlockHeader()
        {
            Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            _previousBlockHash = new byte[HashSize]; // initialized to zero; set before hashing
            _merkleRoot = new byte[HashSize];        // initialized to zero; set before hashing
            _miner = new byte[HashSize];             // initialized to zero; set before hashing

            _target = Ensure32(global::Qado.Blockchain.Difficulty.PowLimit.ToArray(), nameof(Target));
        }

        public byte[] ToHashBytes() => ToHashBytesWithNonce(Nonce);

        public byte[] ToHashBytesWithNonce(uint nonceCandidate)
        {
            var targetClamped = global::Qado.Blockchain.Difficulty.ClampTarget(_target);
            if (targetClamped is null || targetClamped.Length != HashSize)
                throw new InvalidOperationException("ClampTarget(Target) must return 32 bytes.");

            var buf = new byte[PowHeaderSize];
            int o = 0;

            buf[o++] = Version;

            _previousBlockHash.AsSpan(0, 32).CopyTo(buf.AsSpan(o, 32)); o += 32;
            _merkleRoot.AsSpan(0, 32).CopyTo(buf.AsSpan(o, 32)); o += 32;

            BinaryPrimitives.WriteUInt64BigEndian(buf.AsSpan(o, 8), Timestamp); o += 8;

            targetClamped.AsSpan(0, 32).CopyTo(buf.AsSpan(o, 32)); o += 32;

            BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(o, 4), nonceCandidate); o += 4;

            _miner.AsSpan(0, 32).CopyTo(buf.AsSpan(o, 32)); o += 32;

            return buf;
        }

        private static byte[] Ensure32(byte[]? v, string name)
        {
            if (v is null)
                throw new ArgumentNullException(name);

            if (v.Length != HashSize)
                throw new ArgumentOutOfRangeException(name, v.Length, $"{name} must be {HashSize} bytes.");

            return v;
        }
    }
}

