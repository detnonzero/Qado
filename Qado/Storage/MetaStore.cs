using System;
using Microsoft.Data.Sqlite;

namespace Qado.Storage
{
    public static class MetaStore
    {
        public static void Set(string key, string value, SqliteTransaction? tx = null)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("key is null/empty", nameof(key));

            value ??= string.Empty;

            const string sql = @"
INSERT INTO meta(key,value) VALUES($k,$v)
ON CONFLICT(key) DO UPDATE SET value=excluded.value;";

            if (tx != null)
            {
                using var cmd = tx.Connection!.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$k", key);
                cmd.Parameters.AddWithValue("$v", value);
                cmd.ExecuteNonQuery();
                return;
            }

            lock (Db.Sync)
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$k", key);
                cmd.Parameters.AddWithValue("$v", value);
                cmd.ExecuteNonQuery();
            }
        }

        public static string? Get(string key, SqliteTransaction? tx = null)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("key is null/empty", nameof(key));

            const string sql = "SELECT value FROM meta WHERE key=$k LIMIT 1;";

            if (tx != null)
            {
                using var cmd = tx.Connection!.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$k", key);
                var v = cmd.ExecuteScalar();
                if (v == null || v is DBNull) return null;
                return v as string;
            }

            lock (Db.Sync)
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$k", key);
                var v = cmd.ExecuteScalar();
                if (v == null || v is DBNull) return null;
                return v as string;
            }
        }
    }
}

