using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using Qado.Logging;
using Qado.Mempool;
using Qado.Storage;

namespace Qado.Blockchain
{
    public static class BlockInsertSafeGuard
    {
        public static bool TryInsertBlock(Block block, out string error, ILogSink? log = null)
        {
            error = string.Empty;

            if (block is null) { error = "Block is null"; log?.Warn("Insert", error); return false; }
            if (block.Header is null) { error = "Missing header"; log?.Warn("Insert", error); return false; }

            if (block.Header.Miner is null || block.Header.Miner.Length != 32) { error = "Invalid miner length"; log?.Warn("Insert", error); return false; }
            if (block.Header.PreviousBlockHash is null || block.Header.PreviousBlockHash.Length != 32) { error = "Invalid prev hash length"; log?.Warn("Insert", error); return false; }
            if (block.Header.Target is null || block.Header.Target.Length != 32) { error = "Invalid target length"; log?.Warn("Insert", error); return false; }
            if (block.Header.MerkleRoot is null || block.Header.MerkleRoot.Length != 32) { error = "Invalid merkle root length"; log?.Warn("Insert", error); return false; }

            if (block.Transactions is null || block.Transactions.Count == 0)
            {
                error = "No transactions";
                log?.Warn("Insert", error);
                return false;
            }

            if (block.BlockHash is not { Length: 32 } || IsZero32(block.BlockHash))
                block.BlockHash = block.ComputeBlockHash();

            if (BlockIndexStore.GetLocation(block.BlockHash) != null)
            {
                error = "Block already known";
                log?.Warn("Insert", error);
                return false;
            }

            if (block.BlockHeight == 0)
            {
                if (!IsZero32(block.Header.PreviousBlockHash))
                {
                    error = "Genesis must have PreviousBlockHash = 0x00..00";
                    log?.Warn("Insert", error);
                    return false;
                }
            }
            else
            {
                if (!BlockIndexStore.TryGetMeta(block.Header.PreviousBlockHash, out var prevHeight, out _, out _))
                {
                    error = "Previous block missing";
                    log?.Warn("Insert", error);
                    return false;
                }

                if (block.BlockHeight != prevHeight + 1)
                {
                    error = $"Height mismatch (have {block.BlockHeight}, prev is {prevHeight})";
                    log?.Warn("Insert", error);
                    return false;
                }
            }

            if (!BlockValidator.ValidateNetworkBlock(block, out var reason))
            {
                error = $"Block validation failed: {reason}";
                log?.Warn("Insert", error);
                return false;
            }

            try
            {
                lock (Db.Sync)
                {
                    using var sqlTx = Db.Connection.BeginTransaction();

                    BlockStore.SaveBlock(block, sqlTx);

                    var txOffsets = ComputeTxOffsets(block);
                    UpsertTxIndex(block, txOffsets, sqlTx);

                    sqlTx.Commit();
                }

                log?.Info("Insert", $"Inserted block h={block.BlockHeight} hash={Hex(block.BlockHash, 16)}…");
                return true;
            }
            catch (SqliteException ex)
            {
                error = $"Persist failed (sqlite): {ex.Message}";
                log?.Warn("Insert", error);
                return false;
            }
            catch (Exception ex)
            {
                error = $"Persist failed: {ex.Message}";
                log?.Error("Insert", error);
                return false;
            }
        }

        public static void RequeueOldBlockTxs(Block oldBlock, Block newBlock, MempoolManager mempool, ILogSink? log = null)
        {
            if (oldBlock?.Transactions is null || newBlock?.Transactions is null) return;

            var newTxIds = new HashSet<string>(StringComparer.Ordinal);
            for (int i = 1; i < newBlock.Transactions.Count; i++)
            {
                var tx = newBlock.Transactions[i];
                if (tx is null) continue;
                newTxIds.Add(Hex(tx.ComputeTransactionHash()));
            }

            for (int i = 1; i < oldBlock.Transactions.Count; i++)
            {
                var tx = oldBlock.Transactions[i];
                if (tx is null) continue;

                var txIdHex = Hex(tx.ComputeTransactionHash());
                if (newTxIds.Contains(txIdHex))
                    continue;

                string senderHex = Hex(tx.Sender);
                if (string.IsNullOrEmpty(senderHex)) continue;

                ulong confirmedNonce = StateStore.GetNonceU64(senderHex);

                if (tx.TxNonce <= confirmedNonce)
                    continue;

                try
                {
                    tx.ValidateBasicOrThrow();
                }
                catch
                {
                    continue;
                }

                if (mempool.TryAdd(tx))
                    log?.Info("Reorg", $"Requeued TX nonce={tx.TxNonce} from {Short(senderHex, 8)}");
            }
        }


        private static void UpsertTxIndex(Block block, (byte[] txid, int offset, int size)[] txOffsets, SqliteTransaction tx)
        {
            if (block.BlockHash is not { Length: 32 })
                throw new InvalidOperationException("BlockHash must be 32 bytes before indexing.");

            using var cmd = Db.Connection.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = @"
INSERT INTO tx_index(txid, block_hash, height, offset, size)
VALUES($txid, $bh, $h, $off, $sz)
ON CONFLICT(txid, block_hash) DO UPDATE SET
  height = excluded.height,
  offset = excluded.offset,
  size   = excluded.size;";

            var bh = block.BlockHash;
            var h = checked((long)block.BlockHeight);

            foreach (var (txid, off, sz) in txOffsets)
            {
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("$txid", txid);
                cmd.Parameters.AddWithValue("$bh", bh);
                cmd.Parameters.AddWithValue("$h", h);
                cmd.Parameters.AddWithValue("$off", off);
                cmd.Parameters.AddWithValue("$sz", sz);
                cmd.ExecuteNonQuery();
            }
        }

        private static (byte[] txid, int offset, int size)[] ComputeTxOffsets(Block block)
        {
            if (block.Transactions is null) return Array.Empty<(byte[] txid, int offset, int size)>();

            int headerSize = Qado.Serialization.BlockBinarySerializer.HeaderSize;
            if (headerSize <= 0)
                throw new InvalidOperationException("BlockBinarySerializer.HeaderSize must be > 0.");

            int o = headerSize;

            var list = new (byte[] txid, int offset, int size)[block.Transactions.Count];

            for (int i = 0; i < block.Transactions.Count; i++)
            {
                var tx = block.Transactions[i] ?? throw new InvalidOperationException($"Transactions[{i}] is null.");

                int txSize = Qado.Serialization.TxBinarySerializer.GetSize(tx);
                if (txSize < 0) throw new InvalidOperationException("TxBinarySerializer.GetSize returned negative size.");

                int lenFieldPos = o;
                checked
                {
                    int txStart = lenFieldPos + 4;
                    list[i] = (tx.ComputeTransactionHash(), txStart, txSize);
                    o = txStart + txSize;
                }
            }

            return list;
        }


        private static bool IsZero32(byte[]? h)
        {
            if (h is not { Length: 32 }) return false;
            for (int i = 0; i < 32; i++) if (h[i] != 0) return false;
            return true;
        }

        private static string Hex(byte[] data)
            => data is null ? string.Empty : Convert.ToHexString(data).ToLowerInvariant();

        private static string Hex(byte[] data, int takeChars)
        {
            if (data is null) return string.Empty;
            var hex = Convert.ToHexString(data).ToLowerInvariant();
            return Short(hex, takeChars);
        }

        private static string Short(string s, int takeChars)
        {
            if (string.IsNullOrEmpty(s) || takeChars <= 0) return string.Empty;
            return s.Length > takeChars ? s[..takeChars] : s;
        }
    }
}

