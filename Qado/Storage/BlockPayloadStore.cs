using System;
using Microsoft.Data.Sqlite;

namespace Qado.Storage
{
    internal static class BlockPayloadStore
    {
        private static SqliteConnection Conn =>
            Db.Connection ?? throw new InvalidOperationException("Db not initialized");

        public static void Upsert(byte[] hash, byte[] payload, SqliteTransaction? tx = null)
        {
            if (hash is not { Length: 32 }) throw new ArgumentException("hash must be 32 bytes", nameof(hash));
            if (payload == null || payload.Length == 0) throw new ArgumentException("payload must not be empty", nameof(payload));

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = @"
INSERT INTO block_payloads(hash, payload)
VALUES($h, $payload)
ON CONFLICT(hash) DO UPDATE SET
  payload = excluded.payload;";
                cmd.Parameters.AddWithValue("$h", hash);
                cmd.Parameters.AddWithValue("$payload", payload);
                cmd.ExecuteNonQuery();
            }
        }

        public static bool TryGet(byte[] hash, out byte[] payload, SqliteTransaction? tx = null)
        {
            payload = Array.Empty<byte>();
            if (hash is not { Length: 32 })
                return false;

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "SELECT payload FROM block_payloads WHERE hash=$h LIMIT 1;";
                cmd.Parameters.AddWithValue("$h", hash);
                var v = cmd.ExecuteScalar() as byte[];
                if (v is not { Length: > 0 })
                    return false;

                payload = (byte[])v.Clone();
                return true;
            }
        }

        public static void Delete(byte[] hash, SqliteTransaction? tx = null)
        {
            if (hash is not { Length: 32 }) throw new ArgumentException("hash must be 32 bytes", nameof(hash));

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "DELETE FROM block_payloads WHERE hash=$h;";
                cmd.Parameters.AddWithValue("$h", hash);
                cmd.ExecuteNonQuery();
            }
        }
    }
}
