using System;
using Microsoft.Data.Sqlite;
using Qado.Utils;

namespace Qado.Storage
{
    public static class BlockIndexStore
    {
        private static SqliteConnection Conn =>
            Db.Connection ?? throw new InvalidOperationException("Db not initialized");

        public static void Upsert(
            byte[] hash,
            byte[] prevHash,
            ulong height,
            ulong ts,
            byte[] target32,
            byte[] miner32,
            UInt128 chainwork,
            int fileId,
            long recordOffset,
            int recordSize,
            int statusFlags,
            SqliteTransaction? tx = null)
        {
            if (hash is not { Length: 32 }) throw new ArgumentException("hash must be 32 bytes", nameof(hash));
            if (prevHash is not { Length: 32 }) throw new ArgumentException("prevHash must be 32 bytes", nameof(prevHash));
            if (target32 is not { Length: 32 }) throw new ArgumentException("target32 must be 32 bytes", nameof(target32));
            if (miner32 is not { Length: 32 }) throw new ArgumentException("miner32 must be 32 bytes", nameof(miner32));

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = @"
INSERT INTO block_index(
  hash, prev_hash, height, ts, target, miner, chainwork,
  file_id, file_offset, file_size, status
) VALUES(
  $h, $p, $ht, $ts, $t, $m, $cw,
  $fid, $off, $sz, $st
)
ON CONFLICT(hash) DO UPDATE SET
  prev_hash   = excluded.prev_hash,
  height      = excluded.height,
  ts          = excluded.ts,
  target      = excluded.target,
  miner       = excluded.miner,
  chainwork   = excluded.chainwork,
  file_id     = excluded.file_id,
  file_offset = excluded.file_offset,
  file_size   = excluded.file_size,
  status      = excluded.status;
";
                cmd.Parameters.AddWithValue("$h", hash);
                cmd.Parameters.AddWithValue("$p", prevHash);
                cmd.Parameters.AddWithValue("$ht", (long)height);
                cmd.Parameters.AddWithValue("$ts", (long)ts);
                cmd.Parameters.AddWithValue("$t", target32);
                cmd.Parameters.AddWithValue("$m", miner32);

                Span<byte> cw = stackalloc byte[16];
                U128.WriteBE(cw, chainwork);
                cmd.Parameters.AddWithValue("$cw", cw.ToArray());

                cmd.Parameters.AddWithValue("$fid", fileId);
                cmd.Parameters.AddWithValue("$off", recordOffset);
                cmd.Parameters.AddWithValue("$sz", recordSize);
                cmd.Parameters.AddWithValue("$st", statusFlags);

                cmd.ExecuteNonQuery();
            }
        }

        public static (int fileId, long recordOffset, int recordSize)? GetLocation(byte[] hash)
        {
            if (hash is not { Length: 32 }) return null;

            lock (Db.Sync)
            {
                using var cmd = Conn.CreateCommand();
                cmd.CommandText = "SELECT file_id, file_offset, file_size FROM block_index WHERE hash=$h LIMIT 1;";
                cmd.Parameters.AddWithValue("$h", hash);
                using var r = cmd.ExecuteReader();
                if (!r.Read()) return null;
                return (r.GetInt32(0), r.GetInt64(1), r.GetInt32(2));
            }
        }

        public static UInt128 GetChainwork(byte[] hash, SqliteTransaction? tx = null)
        {
            if (hash is not { Length: 32 }) return 0;

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "SELECT chainwork FROM block_index WHERE hash=$h LIMIT 1;";
                cmd.Parameters.AddWithValue("$h", hash);
                var v = cmd.ExecuteScalar() as byte[];
                return (v is { Length: 16 }) ? U128.ReadBE(v) : 0;
            }
        }

        public static bool TryGetMeta(byte[] hash, out ulong height, out byte[] prevHash, out UInt128 chainwork)
        {
            height = 0;
            prevHash = new byte[32];
            chainwork = 0;

            if (hash is not { Length: 32 }) return false;

            lock (Db.Sync)
            {
                using var cmd = Conn.CreateCommand();
                cmd.CommandText = "SELECT height, prev_hash, chainwork FROM block_index WHERE hash=$h LIMIT 1;";
                cmd.Parameters.AddWithValue("$h", hash);

                using var r = cmd.ExecuteReader();
                if (!r.Read()) return false;

                var h = r.GetInt64(0);
                var prev = (byte[])r[1];
                var cw = r.IsDBNull(2) ? null : (byte[])r[2];

                height = h >= 0 ? (ulong)h : 0UL;
                prevHash = prev is { Length: 32 } ? prev : new byte[32];
                chainwork = cw is { Length: 16 } ? U128.ReadBE(cw) : 0;
                return true;
            }
        }

        public static ulong GetMaxHeight()
        {
            lock (Db.Sync)
            {
                using var cmd = Conn.CreateCommand();
                cmd.CommandText = "SELECT IFNULL(MAX(height), 0) FROM block_index;";
                var v = cmd.ExecuteScalar();
                return v is long l ? (ulong)l : 0UL;
            }
        }
    }
}

