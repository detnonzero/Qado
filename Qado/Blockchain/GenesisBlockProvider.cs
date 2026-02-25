using System;
using System.Collections.Generic;
using Qado.Logging;
using Qado.Networking;
using Qado.Storage;

namespace Qado.Blockchain
{
    public static class GenesisBlockProvider
    {
        private static readonly byte[] GENESIS_MINER =
            Convert.FromHexString(NetworkParams.GenesisMinerHex);

        private const ulong GENESIS_TIMESTAMP = NetworkParams.GenesisTimestamp;

        private static readonly byte[] GENESIS_TARGET = Convert.FromHexString(NetworkParams.GenesisTargetHex);

        private const uint GENESIS_NONCE = NetworkParams.GenesisNonce;
        private const uint GENESIS_CHAIN_ID = NetworkParams.ChainId;

        private static readonly ulong GENESIS_COINBASE_AMOUNT = RewardCalculator.GetBlockSubsidy(0);

        private static Transaction CreateGenesisCoinbase()
            => new Transaction
            {
                ChainId = GENESIS_CHAIN_ID,
                Sender = new byte[32],                 // coinbase sender is zero address
                Recipient = (byte[])GENESIS_MINER.Clone(),
                Amount = GENESIS_COINBASE_AMOUNT,
                Fee = 0UL,
                TxNonce = 0UL,
                Signature = Array.Empty<byte>()        // coinbase is unsigned
            };

        public static Block GetGenesisBlock()
        {
            var header = new BlockHeader
            {
                Version = 1,
                PreviousBlockHash = new byte[32],
                MerkleRoot = new byte[32],
                Timestamp = GENESIS_TIMESTAMP,
                Target = (byte[])GENESIS_TARGET.Clone(),
                Nonce = GENESIS_NONCE,
                Miner = (byte[])GENESIS_MINER.Clone()
            };

            var block = new Block
            {
                BlockHeight = 0,
                Header = header,
                Transactions = new List<Transaction> { CreateGenesisCoinbase() },
                BlockHash = new byte[32]
            };

            block.RecomputeAndSetMerkleRoot();
            block.RecomputeAndSetBlockHash();
            return block;
        }

        public static bool ValidateGenesisBlock(Block block, out string reason)
        {
            reason = "OK";

            if (block is null) { reason = "Block is null"; return false; }
            if (block.BlockHeight != 0) { reason = "height != 0"; return false; }
            if (block.Header is null) { reason = "header missing"; return false; }
            if (block.Transactions is null || block.Transactions.Count == 0) { reason = "no transactions"; return false; }

            var header = block.Header;

            if (header.MerkleRoot is not { Length: 32 }) { reason = "merkle malformed"; return false; }
            var expectedRoot = block.ComputeMerkleRoot();
            if (expectedRoot is not { Length: 32 }) { reason = "merkle compute failed"; return false; }
            if (!BytesEqual32(expectedRoot, header.MerkleRoot)) { reason = "merkle mismatch"; return false; }

            var expectedHash = block.ComputeBlockHash();
            if (expectedHash is not { Length: 32 }) { reason = "pow hash invalid"; return false; }
            if (block.BlockHash is not { Length: 32 }) { reason = "blockHash malformed"; return false; }
            if (!BytesEqual32(expectedHash, block.BlockHash)) { reason = "blockHash mismatch"; return false; }

            if (header.Target is not { Length: 32 }) { reason = "target malformed"; return false; }
            if (!Difficulty.IsValidTarget(header.Target)) { reason = "target out of range"; return false; }
            if (!BytesEqual32(header.Target, GENESIS_TARGET)) { reason = "target mismatch"; return false; }

            if (header.Nonce != GENESIS_NONCE) { reason = "nonce mismatch"; return false; }
            if (header.PreviousBlockHash is not { Length: 32 } || !IsZero32(header.PreviousBlockHash))
            {
                reason = "prevHash mismatch";
                return false;
            }
            if (header.Miner is not { Length: 32 }) { reason = "miner malformed"; return false; }
            if (!BytesEqual32(header.Miner, GENESIS_MINER)) { reason = "miner mismatch"; return false; }

            var coinbase = block.Transactions[0];
            if (coinbase is null) { reason = "coinbase missing"; return false; }

            if (!TransactionValidator.IsCoinbase(coinbase)) { reason = "coinbase invalid"; return false; }
            if (coinbase.Recipient is not { Length: 32 } || !BytesEqual32(coinbase.Recipient, header.Miner))
            {
                reason = "coinbase recipient mismatch";
                return false;
            }

            if (coinbase.Fee != 0UL) { reason = "coinbase fee must be 0"; return false; }
            if (coinbase.Amount != GENESIS_COINBASE_AMOUNT) { reason = "coinbase amount mismatch"; return false; }
            if (coinbase.TxNonce != 0UL) { reason = "coinbase nonce must be 0"; return false; }
            if (coinbase.Signature is not null && coinbase.Signature.Length != 0) { reason = "coinbase must be unsigned"; return false; }
            if (coinbase.ChainId != GENESIS_CHAIN_ID) { reason = "coinbase chainId mismatch"; return false; }

            if (coinbase.Sender is not { Length: 32 } || !IsZero32(coinbase.Sender))
            {
                reason = "coinbase sender must be zero";
                return false;
            }

            return true;
        }

        public static void EnsureGenesisBlockStored(ILogSink? log = null)
        {
            if (Db.Connection == null)
                throw new InvalidOperationException("Db.Connection is null (Db.Initialize must run before genesis).");

            var genesis = GetGenesisBlock();

            if (!ValidateGenesisBlock(genesis, out var expectedReason))
            {
                log?.Error("Genesis", $"Expected genesis validation failed: {expectedReason}");
                throw new InvalidOperationException($"Expected genesis invalid: {expectedReason}");
            }

            var canon0 = BlockStore.GetCanonicalHashAtHeight(0);
            if (canon0 is { Length: 32 })
            {
                if (!BytesEqual32(canon0, genesis.BlockHash!))
                    throw new InvalidOperationException("Existing canonical genesis hash does not match expected genesis.");

                var stored = BlockStore.GetBlockByHash(canon0);
                if (stored == null)
                    throw new InvalidOperationException("Stored genesis payload missing.");
                if (!ValidateGenesisBlock(stored, out var storedReason))
                    throw new InvalidOperationException($"Stored genesis payload invalid: {storedReason}");

                log?.Info("Genesis", "Genesis block already present and validated.");
                return;
            }

            lock (Db.Sync)
            {
                using var tx = Db.Connection.BeginTransaction();

                var canon0Again = BlockStore.GetCanonicalHashAtHeight(0, tx);
                if (canon0Again is { Length: 32 })
                {
                    if (!BytesEqual32(canon0Again, genesis.BlockHash!))
                    {
                        tx.Rollback();
                        throw new InvalidOperationException("Canonical genesis hash mismatch (rechecked in tx).");
                    }

                    var stored = BlockStore.GetBlockByHash(canon0Again);
                    if (stored == null)
                    {
                        tx.Rollback();
                        throw new InvalidOperationException("Stored genesis payload missing (rechecked in tx).");
                    }

                    if (!ValidateGenesisBlock(stored, out var storedReason))
                    {
                        tx.Rollback();
                        throw new InvalidOperationException($"Stored genesis payload invalid (rechecked in tx): {storedReason}");
                    }

                    tx.Rollback();
                    log?.Info("Genesis", "Genesis block already present and validated (rechecked in tx).");
                    return;
                }

                BlockStore.SaveBlock(genesis, tx);

                BlockStore.SetCanonicalHashAtHeight(0, genesis.BlockHash!, tx);

                MetaStore.Set("LatestBlockHash", Hex(genesis.BlockHash!), tx);
                MetaStore.Set("LatestHeight", "0", tx);

                tx.Commit();
            }

            log?.Info("Genesis", $"Genesis inserted. hash={Hex(genesis.BlockHash)}");
        }

        private static bool BytesEqual32(byte[] a, byte[] b)
        {
            if (a is null || b is null) return false;
            if (a.Length != 32 || b.Length != 32) return false;
            int diff = 0;
            for (int i = 0; i < 32; i++) diff |= a[i] ^ b[i];
            return diff == 0;
        }

        private static bool IsZero32(byte[] a)
        {
            if (a is not { Length: 32 }) return false;
            for (int i = 0; i < 32; i++) if (a[i] != 0) return false;
            return true;
        }

        private static string Hex(byte[] b) => Convert.ToHexString(b).ToLowerInvariant();
    }
}

