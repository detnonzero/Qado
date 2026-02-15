using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace Qado.Storage
{
    public static class StateUndoStore
    {
        public static void EnsureSchema(SqliteTransaction? tx = null)
        {
            const string sql = @"
CREATE TABLE IF NOT EXISTS state_undo(
  block_hash BLOB(32) PRIMARY KEY,
  payload    BLOB NOT NULL
) WITHOUT ROWID;";

            if (tx != null)
            {
                using var cmd = tx.Connection!.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
                return;
            }

            lock (Db.Sync)
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }
        }

        public static void Put(byte[] blockHash, IReadOnlyList<UndoEntry> entries, SqliteTransaction tx)
        {
            if (blockHash is not { Length: 32 }) throw new ArgumentException("blockHash must be 32 bytes", nameof(blockHash));
            if (tx is null) throw new ArgumentNullException(nameof(tx));

            EnsureSchema(tx);

            byte[] payload = Encode(entries);

            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = @"
INSERT INTO state_undo(block_hash, payload)
VALUES($h, $p)
ON CONFLICT(block_hash) DO UPDATE SET payload=excluded.payload;";
            cmd.Parameters.AddWithValue("$h", blockHash);
            cmd.Parameters.AddWithValue("$p", payload);
            cmd.ExecuteNonQuery();
        }

        public static bool TryGet(byte[] blockHash, SqliteTransaction tx, out List<UndoEntry> entries)
        {
            if (blockHash is not { Length: 32 }) throw new ArgumentException("blockHash must be 32 bytes", nameof(blockHash));
            if (tx is null) throw new ArgumentNullException(nameof(tx));

            EnsureSchema(tx);

            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "SELECT payload FROM state_undo WHERE block_hash=$h LIMIT 1;";
            cmd.Parameters.AddWithValue("$h", blockHash);

            var v = cmd.ExecuteScalar() as byte[];
            if (v == null || v.Length < 4)
            {
                entries = new List<UndoEntry>();
                return false;
            }

            entries = Decode(v);
            return true;
        }

        public static void Delete(byte[] blockHash, SqliteTransaction tx)
        {
            if (blockHash is not { Length: 32 }) throw new ArgumentException("blockHash must be 32 bytes", nameof(blockHash));
            if (tx is null) throw new ArgumentNullException(nameof(tx));

            EnsureSchema(tx);

            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "DELETE FROM state_undo WHERE block_hash=$h;";
            cmd.Parameters.AddWithValue("$h", blockHash);
            cmd.ExecuteNonQuery();
        }

        public static void RollbackBlock(byte[] blockHash, SqliteTransaction tx)
        {
            if (blockHash is not { Length: 32 }) throw new ArgumentException("blockHash must be 32 bytes", nameof(blockHash));
            if (tx is null) throw new ArgumentNullException(nameof(tx));

            if (!TryGet(blockHash, tx, out var entries))
                throw new InvalidOperationException("state_undo missing for block");

            for (int i = 0; i < entries.Count; i++)
            {
                var e = entries[i];

                if (!e.Existed)
                {
                    DeleteAccount(e.Addr, tx);
                }
                else
                {
                    SetAccount(e.Addr, e.Balance, e.Nonce, tx);
                }
            }

            Delete(blockHash, tx);
        }


        private static void DeleteAccount(byte[] addr, SqliteTransaction tx)
        {
            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "DELETE FROM accounts WHERE addr=$a;";
            cmd.Parameters.AddWithValue("$a", addr);
            cmd.ExecuteNonQuery();
        }

        private static void SetAccount(byte[] addr, ulong balance, ulong nonce, SqliteTransaction tx)
        {
            if (nonce > (ulong)long.MaxValue)
                throw new OverflowException($"Nonce {nonce} exceeds SQLite INTEGER range.");

            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = @"
INSERT INTO accounts(addr, balance, nonce) VALUES($a, $b, $n)
ON CONFLICT(addr) DO UPDATE SET balance=excluded.balance, nonce=excluded.nonce;";
            cmd.Parameters.AddWithValue("$a", addr);
            cmd.Parameters.AddWithValue("$b", StateStore.U64ToBlob(balance));
            cmd.Parameters.AddWithValue("$n", (long)nonce);
            cmd.ExecuteNonQuery();
        }


        private static byte[] Encode(IReadOnlyList<UndoEntry> entries)
        {
            int count = entries?.Count ?? 0;
            const int entrySize = 32 + 1 + 8 + 8; // 49 bytes

            int total = checked(4 + (count * entrySize));
            var buf = new byte[total];
            var span = buf.AsSpan();

            BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0, 4), (uint)count);

            int off = 4;
            for (int i = 0; i < count; i++)
            {
                var e = entries![i];
                if (e.Addr is not { Length: 32 })
                    throw new ArgumentException("UndoEntry.Addr must be 32 bytes");

                e.Addr.AsSpan().CopyTo(span.Slice(off, 32));
                off += 32;

                span[off++] = e.Existed ? (byte)1 : (byte)0;

                BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(off, 8), e.Balance);
                off += 8;

                BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(off, 8), e.Nonce);
                off += 8;
            }

            return buf;
        }

        private static List<UndoEntry> Decode(byte[] payload)
        {
            var span = payload.AsSpan();
            if (span.Length < 4) throw new FormatException("payload too small");

            uint countU = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(0, 4));
            if (countU > 5_000_000) throw new FormatException("undo count insane");

            int count = (int)countU;
            const int entrySize = 49;
            int expected = checked(4 + (count * entrySize));
            if (span.Length < expected) throw new FormatException("payload truncated");

            var list = new List<UndoEntry>(count);

            int off = 4;
            for (int i = 0; i < count; i++)
            {
                var addr = span.Slice(off, 32).ToArray();
                off += 32;

                bool existed = span[off++] != 0;

                ulong bal = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(off, 8));
                off += 8;

                ulong nonce = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(off, 8));
                off += 8;

                list.Add(new UndoEntry(addr, existed, bal, nonce));
            }

            return list;
        }

        public readonly struct UndoEntry
        {
            public readonly byte[] Addr;   // 32-byte account address
            public readonly bool Existed;
            public readonly ulong Balance;
            public readonly ulong Nonce;

            public UndoEntry(byte[] addr, bool existed, ulong balance, ulong nonce)
            {
                Addr = addr;
                Existed = existed;
                Balance = balance;
                Nonce = nonce;
            }
        }
    }
}

