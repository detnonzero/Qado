using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;
using Qado.Blockchain;

namespace Qado.Storage
{
    public static class StateApplier
    {
        public static void ApplyBlockWithUndo(Block block, SqliteTransaction tx)
        {
            if (block is null) throw new ArgumentNullException(nameof(block));
            if (tx is null) throw new ArgumentNullException(nameof(tx));
            if (block.BlockHash is not { Length: 32 }) throw new ArgumentException("block.BlockHash missing", nameof(block));
            if (!BlockValidator.ValidateNetworkSideBlockStateless(block, out var reason))
                throw new InvalidOperationException($"Block stateless validation failed: {reason}");

            StateUndoStore.EnsureSchema(tx);

            var touched = new Dictionary<string, (byte[] addr, bool existed, ulong bal, ulong nonce)>(StringComparer.Ordinal);

            foreach (var t in block.Transactions)
            {
                if (TransactionValidator.IsCoinbase(t))
                {
                    AddTouched(t.Recipient, tx, touched);
                }
                else
                {
                    AddTouched(t.Sender, tx, touched);
                    AddTouched(t.Recipient, tx, touched);
                }
            }

            var undo = new List<StateUndoStore.UndoEntry>(touched.Count);
            foreach (var kv in touched)
            {
                var v = kv.Value;
                undo.Add(new StateUndoStore.UndoEntry(
                    addr: (byte[])v.addr.Clone(),
                    existed: v.existed,
                    balance: v.bal,
                    nonce: v.nonce
                ));
            }
            undo.Sort((a, b) => Compare32(a.Addr, b.Addr));

            StateUndoStore.Put(block.BlockHash, undo, tx);

            ApplyBlock(block, tx);

            for (int i = 0; i < block.Transactions.Count; i++)
            {
                var t = block.Transactions[i];
                byte[] txid = t.ComputeTransactionHash();
                if (txid is { Length: 32 })
                {
                    TxIndexStore.Insert(
                        txid: txid,
                        blockHash: block.BlockHash,
                        height: block.BlockHeight,
                        offset: i,
                        size: 0,
                        tx: tx);
                }
            }
        }

        public static void ApplyBlock(Block block, SqliteTransaction tx)
        {
            if (block is null) throw new ArgumentNullException(nameof(block));
            if (tx is null) throw new ArgumentNullException(nameof(tx));
            if (block.Transactions is null || block.Transactions.Count == 0)
                throw new InvalidOperationException("Block has no transactions.");

            var txs = block.Transactions;
            var coinbase = txs[0];
            if (!TransactionValidator.IsCoinbase(coinbase))
                throw new InvalidOperationException("TX[0] must be coinbase.");

            if (!TransactionValidator.ValidateBasic(coinbase, out var cbReason))
                throw new InvalidOperationException($"Coinbase invalid: {cbReason}");

            if (block.Header?.Miner is not { Length: 32 })
                throw new InvalidOperationException("Header miner is invalid.");

            if (!BytesEqual32(coinbase.Recipient, block.Header.Miner))
                throw new InvalidOperationException("Coinbase recipient must equal header miner.");

            ulong totalFees = 0UL;
            for (int i = 1; i < txs.Count; i++)
            {
                var t = txs[i];
                if (TransactionValidator.IsCoinbase(t))
                    throw new InvalidOperationException($"TX[{i}] must not be coinbase.");

                if (!TransactionValidator.ValidateBasic(t, out var reason))
                    throw new InvalidOperationException($"TX[{i}] invalid: {reason}");

                if (!TryAddU64(totalFees, t.Fee, out totalFees))
                    throw new OverflowException("Fee sum overflow.");
            }

            ulong subsidy = RewardCalculator.GetBlockSubsidy(block.BlockHeight);
            if (!TryAddU64(subsidy, totalFees, out ulong expectedCoinbase))
                throw new OverflowException("Coinbase amount overflow.");

            if (coinbase.Amount != expectedCoinbase)
                throw new InvalidOperationException("Coinbase amount mismatch.");

            for (int i = 0; i < txs.Count; i++)
            {
                var t = txs[i];

                if (i == 0)
                {
                    var recipHex = ToHex(t.Recipient);
                    var (bal, nonce) = GetAccount(recipHex, tx);

                    if (ulong.MaxValue - bal < t.Amount)
                        throw new OverflowException("balance overflow (coinbase)");

                    bal += t.Amount;
                    SetAccount(recipHex, bal, nonce, tx);
                    continue;
                }

                var senderHex = ToHex(t.Sender);
                var recipHex2 = ToHex(t.Recipient);

                var (sBal, sNonce) = GetAccount(senderHex, tx);
                var (rBal, rNonce) = GetAccount(recipHex2, tx);

                if (!TryAddU64(t.Amount, t.Fee, out ulong totalCost))
                    throw new OverflowException("cost overflow");

                if (sBal < totalCost)
                    throw new OverflowException("balance underflow");

                if (!TryAddU64(sNonce, 1UL, out var expectedNonce) || t.TxNonce != expectedNonce)
                    throw new InvalidOperationException($"Nonce mismatch for sender {senderHex}: expected {expectedNonce}, got {t.TxNonce}");

                sBal -= totalCost;
                sNonce = t.TxNonce;

                if (ulong.MaxValue - rBal < t.Amount)
                    throw new OverflowException("recipient balance overflow");

                rBal += t.Amount;

                SetAccount(senderHex, sBal, sNonce, tx);
                SetAccount(recipHex2, rBal, rNonce, tx);
            }
        }


        private static void AddTouched(byte[] addr32, SqliteTransaction tx, Dictionary<string, (byte[] addr, bool existed, ulong bal, ulong nonce)> touched)
        {
            var key = ToHex(addr32);
            if (touched.ContainsKey(key)) return;

            var (ex, bal, nonce) = GetAccountRaw(addr32, tx);
            touched[key] = ((byte[])addr32.Clone(), ex, bal, nonce);
        }

        private static (ulong balance, ulong nonce) GetAccount(string addrHex, SqliteTransaction tx)
        {
            var (existed, bal, nonce) = GetAccountRaw(Convert.FromHexString(addrHex), tx);
            return existed ? (bal, nonce) : (0UL, 0UL);
        }

        private static (bool existed, ulong balance, ulong nonce) GetAccountRaw(byte[] addr32, SqliteTransaction tx)
        {
            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "SELECT balance, nonce FROM accounts WHERE addr=$a;";
            cmd.Parameters.AddWithValue("$a", addr32);

            using var r = cmd.ExecuteReader();
            if (!r.Read())
                return (false, 0UL, 0UL);

            ulong bal = StateStore.ReadU64Blob(r.GetValue(0));
            ulong nonce = StateStore.ReadU64IntegerOrBlob(r.GetValue(1));
            return (true, bal, nonce);
        }

        private static void SetAccount(string addrHex, ulong balance, ulong nonce, SqliteTransaction tx)
        {
            if (nonce > (ulong)long.MaxValue)
                throw new OverflowException($"Nonce {nonce} exceeds SQLite INTEGER range.");

            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = @"
INSERT INTO accounts(addr, balance, nonce) VALUES($a, $b, $n)
ON CONFLICT(addr) DO UPDATE SET balance=excluded.balance, nonce=excluded.nonce;";
            cmd.Parameters.AddWithValue("$a", Convert.FromHexString(addrHex));
            cmd.Parameters.AddWithValue("$b", StateStore.U64ToBlob(balance));
            cmd.Parameters.AddWithValue("$n", (long)nonce);
            cmd.ExecuteNonQuery();
        }

        private static string ToHex(byte[] addr) => Convert.ToHexString(addr).ToLowerInvariant();

        private static int Compare32(byte[] a, byte[] b)
        {
            for (int i = 0; i < 32; i++)
            {
                int d = a[i].CompareTo(b[i]);
                if (d != 0) return d;
            }
            return 0;
        }

        private static bool TryAddU64(ulong a, ulong b, out ulong sum)
        {
            if (ulong.MaxValue - a < b)
            {
                sum = 0;
                return false;
            }
            sum = a + b;
            return true;
        }

        private static bool BytesEqual32(byte[] a, byte[] b)
        {
            if (a is not { Length: 32 } || b is not { Length: 32 }) return false;
            int diff = 0;
            for (int i = 0; i < 32; i++) diff |= a[i] ^ b[i];
            return diff == 0;
        }
    }
}

