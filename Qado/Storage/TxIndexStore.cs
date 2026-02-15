using System;
using Microsoft.Data.Sqlite;

namespace Qado.Storage
{
    public static class TxIndexStore
    {
        public static void Insert(byte[] txid, byte[] blockHash, ulong height, int offset, int size, SqliteTransaction? tx = null)
        {
            if (txid is not { Length: 32 }) throw new ArgumentException("txid must be 32 bytes", nameof(txid));
            if (blockHash is not { Length: 32 }) throw new ArgumentException("blockHash must be 32 bytes", nameof(blockHash));
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset));
            if (size < 0) throw new ArgumentOutOfRangeException(nameof(size));

            const string sql = @"
INSERT OR REPLACE INTO tx_index(txid, block_hash, height, offset, size)
VALUES($i,$bh,$h,$o,$s);";

            if (tx != null)
            {
                using var cmd = tx.Connection!.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$i", txid);
                cmd.Parameters.AddWithValue("$bh", blockHash);
                cmd.Parameters.AddWithValue("$h", (long)height);
                cmd.Parameters.AddWithValue("$o", offset);
                cmd.Parameters.AddWithValue("$s", size);
                cmd.ExecuteNonQuery();
                return;
            }

            lock (Db.Sync)
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$i", txid);
                cmd.Parameters.AddWithValue("$bh", blockHash);
                cmd.Parameters.AddWithValue("$h", (long)height);
                cmd.Parameters.AddWithValue("$o", offset);
                cmd.Parameters.AddWithValue("$s", size);
                cmd.ExecuteNonQuery();
            }
        }

        public static (byte[] blockHash, ulong height, int offset, int size)? Get(byte[] txid, SqliteTransaction? tx = null)
        {
            if (txid is not { Length: 32 }) throw new ArgumentException("txid must be 32 bytes", nameof(txid));

            const string sql = @"
SELECT t.block_hash, t.height, t.offset, t.size
FROM tx_index t
LEFT JOIN canon c
  ON c.height = t.height
 AND c.hash   = t.block_hash
WHERE t.txid = $i
ORDER BY
  CASE WHEN c.hash IS NOT NULL THEN 1 ELSE 0 END DESC,
  t.height DESC,
  t.block_hash ASC
LIMIT 1;";

            if (tx != null)
            {
                using var cmd = tx.Connection!.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$i", txid);

                using var r = cmd.ExecuteReader();
                if (!r.Read()) return null;

                var bh = (byte[])r[0];
                var h = (ulong)r.GetInt64(1);
                var off = r.GetInt32(2);
                var size = r.GetInt32(3);
                return (bh, h, off, size);
            }

            lock (Db.Sync)
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$i", txid);

                using var r = cmd.ExecuteReader();
                if (!r.Read()) return null;

                var bh = (byte[])r[0];
                var h = (ulong)r.GetInt64(1);
                var off = r.GetInt32(2);
                var size = r.GetInt32(3);
                return (bh, h, off, size);
            }
        }

        public static void DeleteFromHeight(ulong fromHeight, SqliteTransaction tx)
        {
            if (tx is null) throw new ArgumentNullException(nameof(tx));

            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "DELETE FROM tx_index WHERE height >= $h;";
            cmd.Parameters.AddWithValue("$h", (long)fromHeight);
            cmd.ExecuteNonQuery();
        }
    }
}

