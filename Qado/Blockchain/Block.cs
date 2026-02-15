using System;
using System.Collections.Generic;
using Qado.Networking;
using Qado.Utils;

namespace Qado.Blockchain
{
    public sealed class Block
    {
        public ulong BlockHeight { get; set; }

        public required BlockHeader Header { get; set; }

        public byte[] BlockHash
        {
            get => _blockHash;
            set => _blockHash = Ensure32(value, nameof(BlockHash));
        }

        public byte[] Signature { get; set; } = Array.Empty<byte>();

        public List<Transaction> Transactions
        {
            get => _transactions;
            set => _transactions = value ?? throw new ArgumentNullException(nameof(Transactions));
        }

        private byte[] _blockHash = new byte[32];
        private List<Transaction> _transactions = new();

        public byte[] ComputeBlockHash()
        {
            if (Header is null) throw new InvalidOperationException("Header is required.");

            var headerBytes = Header.ToHashBytes();

            return Argon2Util.ComputeHash(
                headerBytes,
                memoryKb: ConsensusRules.PowMemoryKb,
                iterations: ConsensusRules.PowIterations,
                parallelism: ConsensusRules.PowParallelism);
        }

        public void RecomputeAndSetBlockHash()
            => BlockHash = ComputeBlockHash();

        public byte[] ComputeMerkleRoot()
            => MerkleUtil.ComputeMerkleRootFromTransactions(_transactions);

        public void RecomputeAndSetMerkleRoot()
            => Header.MerkleRoot = ComputeMerkleRoot();

        public ulong CalculateTotalFees()
        {
            ulong sum = 0;

            for (int i = 0; i < _transactions.Count; i++)
            {
                var tx = _transactions[i];
                if (tx == null) continue;

                if (tx.IsCoinbase) continue;

                checked { sum += tx.Fee; }
            }

            return sum;
        }

        public void InsertCoinbaseTransaction(ulong coinbaseAmount, uint chainId = NetworkParams.ChainId)
        {
            if (Header is null) throw new InvalidOperationException("Header is required.");

            if (Header.Miner is not { Length: 32 })
                throw new InvalidOperationException("Header.Miner must be 32 bytes.");

            if (_transactions.Count > 0 && _transactions[0]?.IsCoinbase == true)
                throw new InvalidOperationException("Coinbase already present at index 0.");

            for (int i = 0; i < _transactions.Count; i++)
            {
                if (_transactions[i]?.IsCoinbase == true)
                    throw new InvalidOperationException("Coinbase must only appear at index 0.");
            }

            var coinbaseTx = new Transaction
            {
                ChainId = chainId,
                Sender = new byte[32],                 // zero sender marks coinbase
                Recipient = (byte[])Header.Miner.Clone(),
                Amount = coinbaseAmount,
                Fee = 0,
                TxNonce = 0,
                Signature = Array.Empty<byte>()
            };

            coinbaseTx.ValidateBasicOrThrow();

            _transactions.Insert(0, coinbaseTx);
        }

        public void ValidateBasicOrThrow()
        {
            if (Header is null) throw new InvalidOperationException("Header is required.");

            Ensure32(Header.PreviousBlockHash, "Header.PreviousBlockHash");
            Ensure32(Header.MerkleRoot, "Header.MerkleRoot");
            Ensure32(Header.Target, "Header.Target");
            Ensure32(Header.Miner, "Header.Miner");

            if (_transactions is null) throw new InvalidOperationException("Transactions is required.");
            if (_transactions.Count == 0) throw new InvalidOperationException("Block must contain at least the coinbase transaction.");
        }

        private static byte[] Ensure32(byte[]? v, string name)
        {
            if (v is null) throw new ArgumentNullException(name);
            if (v.Length != 32) throw new ArgumentOutOfRangeException(name, v.Length, $"{name} must be 32 bytes.");
            return v;
        }
    }
}

