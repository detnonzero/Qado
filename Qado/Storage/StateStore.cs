using System;
using System.Buffers.Binary;
using Microsoft.Data.Sqlite;

namespace Qado.Storage
{
    public static class StateStore
    {
        public static ulong GetBalance(string addrHex, SqliteTransaction? tx = null)
        {
            var addr = Convert.FromHexString(addrHex);

            const string sql = "SELECT balance FROM accounts WHERE addr=$a;";

            if (tx != null)
            {
                using var cmd = tx.Connection!.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$a", addr);
                return ReadU64Blob(cmd.ExecuteScalar());
            }

            lock (Db.Sync)
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$a", addr);
                return ReadU64Blob(cmd.ExecuteScalar());
            }
        }

        public static ulong GetNonce(string addrHex, SqliteTransaction? tx = null)
        {
            var addr = Convert.FromHexString(addrHex);

            const string sql = "SELECT nonce FROM accounts WHERE addr=$a;";

            if (tx != null)
            {
                using var cmd = tx.Connection!.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$a", addr);
                return ReadU64IntegerOrBlob(cmd.ExecuteScalar());
            }

            lock (Db.Sync)
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$a", addr);
                return ReadU64IntegerOrBlob(cmd.ExecuteScalar());
            }
        }

        public static void Set(string addrHex, ulong balance, ulong nonce, SqliteTransaction? tx = null)
        {
            var addr = Convert.FromHexString(addrHex);
            var balBlob = U64ToBlobLE(balance);
            long nonceI64 = ToSqliteInt64Checked(nonce);

            const string sql = @"
INSERT INTO accounts(addr,balance,nonce) VALUES($a,$b,$n)
ON CONFLICT(addr) DO UPDATE SET balance=excluded.balance, nonce=excluded.nonce;";

            if (tx != null)
            {
                using var cmd = tx.Connection!.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$a", addr);
                cmd.Parameters.AddWithValue("$b", balBlob);
                cmd.Parameters.AddWithValue("$n", nonceI64);
                cmd.ExecuteNonQuery();
                return;
            }

            lock (Db.Sync)
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$a", addr);
                cmd.Parameters.AddWithValue("$b", balBlob);
                cmd.Parameters.AddWithValue("$n", nonceI64);
                cmd.ExecuteNonQuery();
            }
        }

        public static ulong GetBalanceU64(string addrHex) => GetBalance(addrHex);
        public static ulong GetNonceU64(string addrHex) => GetNonce(addrHex);


        private static byte[] U64ToBlobLE(ulong v)
        {
            var b = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(b, v);
            return b;
        }

        private static long ToSqliteInt64Checked(ulong v)
        {
            if (v > (ulong)long.MaxValue)
                throw new OverflowException($"Value {v} exceeds SQLite INTEGER range.");
            return (long)v;
        }


        internal static byte[] U64ToBlob(ulong v) => U64ToBlobLE(v);

        internal static ulong ReadU64Blob(object? scalar)
        {
            if (scalar is null || scalar is DBNull) return 0UL;

            if (scalar is byte[] b)
            {
                if (b.Length != 8) return 0UL;
                return BinaryPrimitives.ReadUInt64LittleEndian(b);
            }

            if (scalar is long l)
                return l < 0 ? 0UL : (ulong)l;

            return 0UL;
        }

        internal static ulong ReadU64IntegerOrBlob(object? scalar)
        {
            if (scalar is null || scalar is DBNull) return 0UL;

            if (scalar is long l)
                return l < 0 ? 0UL : (ulong)l;

            if (scalar is byte[] b)
            {
                if (b.Length != 8) return 0UL;
                return BinaryPrimitives.ReadUInt64LittleEndian(b);
            }

            return 0UL;
        }
    }
}

