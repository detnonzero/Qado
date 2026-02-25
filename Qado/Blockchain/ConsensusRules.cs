using System;

namespace Qado.Blockchain
{
    internal static class ConsensusRules
    {
        public const int PowMemoryKb = 4; // 4 KiB
        public const int PowIterations = 1;
        public const int PowParallelism = 1;

        public const int MaxTransactionsPerBlock = 100;

        public const byte MerkleLeafDomainTag = 0x00;
        public const byte MerkleNodeDomainTag = 0x01;

        public const int BlockHeaderSizeBytes = BlockHeader.PowHeaderSize + 4;

        public const int TxLengthPrefixBytes = 4;

        public const int TargetMaxBlockSizeBytes = 18045;

        public const int MaxTransactionSizeBytes =
            (TargetMaxBlockSizeBytes - BlockHeaderSizeBytes - (MaxTransactionsPerBlock * TxLengthPrefixBytes))
            / MaxTransactionsPerBlock;

        public const int MaxBlockSizeBytes =
            BlockHeaderSizeBytes + (MaxTransactionsPerBlock * (TxLengthPrefixBytes + MaxTransactionSizeBytes));

        static ConsensusRules()
        {
            if (MaxTransactionsPerBlock <= 0)
                throw new InvalidOperationException("MaxTransactionsPerBlock must be > 0.");
            if (MaxTransactionSizeBytes <= 0)
                throw new InvalidOperationException("Derived MaxTransactionSizeBytes must be > 0.");
            if (MaxBlockSizeBytes > TargetMaxBlockSizeBytes)
                throw new InvalidOperationException("Derived MaxBlockSizeBytes exceeds target ceiling.");
            if (MerkleLeafDomainTag == MerkleNodeDomainTag)
                throw new InvalidOperationException("Merkle domain tags must be distinct.");
        }
    }
}

