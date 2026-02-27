using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;
using Qado.Utils;

namespace Qado.Storage
{
    public static class BlockIndexStore
    {
        public const int BadReasonNone = 0;
        public const int StatusHeaderOnly = 10;
        public const int StatusHaveBlockPayload = 11;
        public const int StatusCanonicalStateValidated = 1;
        public const int StatusSideStatelessAccepted = 2;
        public const int StatusSideStateInvalid = 3;

        public readonly record struct HeaderRecord(
            byte[] Hash,
            byte[] PrevHash,
            ulong Height,
            ulong Timestamp,
            byte[] Target,
            byte[] Miner,
            UInt128 Chainwork,
            int Status,
            bool IsBad,
            bool BadAncestor,
            int BadReason,
            int FileId,
            long RecordOffset,
            int RecordSize);

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
                if (r.FieldCount < 3)
                    return null;

                int fileId;
                long offset;
                int size;
                try
                {
                    fileId = r.GetInt32(0);
                    offset = r.GetInt64(1);
                    size = r.GetInt32(2);
                }
                catch
                {
                    return null;
                }

                if (fileId < 0 || offset < 0 || size <= 0)
                    return null;

                return (fileId, offset, size);
            }
        }

        public static bool ContainsHash(byte[] hash, SqliteTransaction? tx = null)
        {
            if (hash is not { Length: 32 }) return false;

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "SELECT 1 FROM block_index WHERE hash=$h LIMIT 1;";
                cmd.Parameters.AddWithValue("$h", hash);
                var v = cmd.ExecuteScalar();
                return v != null && v is not DBNull;
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

        public static bool TryGetStatus(byte[] hash, out int status, SqliteTransaction? tx = null)
        {
            status = 0;
            if (hash is not { Length: 32 }) return false;

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "SELECT status FROM block_index WHERE hash=$h LIMIT 1;";
                cmd.Parameters.AddWithValue("$h", hash);
                var v = cmd.ExecuteScalar();
                if (v is not long l) return false;
                status = (int)l;
                return true;
            }
        }

        public static bool TryGetHeaderRecord(byte[] hash, out HeaderRecord record, SqliteTransaction? tx = null)
        {
            record = default;
            if (hash is not { Length: 32 }) return false;

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = @"
SELECT prev_hash, height, ts, target, miner, chainwork, status, is_bad, bad_ancestor, bad_reason, file_id, file_offset, file_size
FROM block_index
WHERE hash=$h
LIMIT 1;";
                cmd.Parameters.AddWithValue("$h", hash);
                using var r = cmd.ExecuteReader();
                if (!r.Read()) return false;

                var prev = r[0] as byte[];
                long heightRaw = r.GetInt64(1);
                long tsRaw = r.GetInt64(2);
                var target = r[3] as byte[];
                var miner = r[4] as byte[];
                var cwBytes = r[5] as byte[];
                long statusRaw = r.GetInt64(6);
                long isBadRaw = r.GetInt64(7);
                long badAncestorRaw = r.GetInt64(8);
                long badReasonRaw = r.GetInt64(9);
                int fileId = r.GetInt32(10);
                long recordOffset = r.GetInt64(11);
                int recordSize = r.GetInt32(12);

                if (prev is not { Length: 32 } ||
                    target is not { Length: 32 } ||
                    miner is not { Length: 32 } ||
                    cwBytes is not { Length: 16 } ||
                    heightRaw < 0 ||
                    tsRaw < 0)
                {
                    return false;
                }

                record = new HeaderRecord(
                    Hash: (byte[])hash.Clone(),
                    PrevHash: (byte[])prev.Clone(),
                    Height: (ulong)heightRaw,
                    Timestamp: (ulong)tsRaw,
                    Target: (byte[])target.Clone(),
                    Miner: (byte[])miner.Clone(),
                    Chainwork: U128.ReadBE(cwBytes),
                    Status: (int)statusRaw,
                    IsBad: isBadRaw != 0,
                    BadAncestor: badAncestorRaw != 0,
                    BadReason: (int)badReasonRaw,
                    FileId: fileId,
                    RecordOffset: recordOffset,
                    RecordSize: recordSize);

                return true;
            }
        }

        public static bool TryGetBestHeaderHash(out byte[] hash, SqliteTransaction? tx = null)
        {
            hash = Array.Empty<byte>();
            return TryGetBestHeaderTip(out hash, out _, out _, tx);
        }

        public static bool TryGetBestHeaderTip(out byte[] hash, out ulong height, out UInt128 chainwork, SqliteTransaction? tx = null)
        {
            hash = Array.Empty<byte>();
            height = 0;
            chainwork = 0;

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = @"
SELECT hash, height, chainwork
FROM block_index
WHERE is_bad = 0
  AND bad_ancestor = 0
ORDER BY chainwork DESC, hash ASC
LIMIT 1;";
                using var r = cmd.ExecuteReader();
                if (!r.Read())
                    return false;

                var h = r[0] as byte[];
                long heightRaw = r.GetInt64(1);
                var cwBytes = r[2] as byte[];
                if (h is not { Length: 32 } || heightRaw < 0 || cwBytes is not { Length: 16 })
                    return false;

                hash = (byte[])h.Clone();
                height = (ulong)heightRaw;
                chainwork = U128.ReadBE(cwBytes);
                return true;
            }
        }

        public static bool TryGetBestValidatedTip(out byte[] hash, out ulong height, out UInt128 chainwork, SqliteTransaction? tx = null)
        {
            hash = Array.Empty<byte>();
            height = 0;
            chainwork = 0;

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = @"
SELECT c.hash, c.height, bi.chainwork
FROM canon c
JOIN block_index bi ON bi.hash = c.hash
WHERE bi.is_bad = 0
  AND bi.bad_ancestor = 0
ORDER BY c.height DESC
LIMIT 1;";
                using var r = cmd.ExecuteReader();
                if (!r.Read())
                    return false;

                var h = r[0] as byte[];
                long heightRaw = r.GetInt64(1);
                var cwBytes = r[2] as byte[];
                if (h is not { Length: 32 } || heightRaw < 0 || cwBytes is not { Length: 16 })
                    return false;

                hash = (byte[])h.Clone();
                height = (ulong)heightRaw;
                chainwork = U128.ReadBE(cwBytes);
                return true;
            }
        }

        public static void UpsertHeaderBytes(byte[] hash, byte[] headerBytes, SqliteTransaction? tx = null)
        {
            if (hash is not { Length: 32 }) throw new ArgumentException("hash must be 32 bytes", nameof(hash));
            if (headerBytes == null || headerBytes.Length == 0) throw new ArgumentException("headerBytes must not be empty", nameof(headerBytes));

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = @"
INSERT INTO header_store(hash, header)
VALUES($h, $hdr)
ON CONFLICT(hash) DO UPDATE SET
  header = excluded.header;";
                cmd.Parameters.AddWithValue("$h", hash);
                cmd.Parameters.AddWithValue("$hdr", headerBytes);
                cmd.ExecuteNonQuery();
            }
        }

        public static bool TryGetHeaderBytes(byte[] hash, out byte[] headerBytes, SqliteTransaction? tx = null)
        {
            headerBytes = Array.Empty<byte>();
            if (hash is not { Length: 32 }) return false;

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "SELECT header FROM header_store WHERE hash=$h LIMIT 1;";
                cmd.Parameters.AddWithValue("$h", hash);
                var v = cmd.ExecuteScalar() as byte[];
                if (v is not { Length: > 0 })
                    return false;

                headerBytes = (byte[])v.Clone();
                return true;
            }
        }

        public static bool TryGetBadFlags(
            byte[] hash,
            out bool isBad,
            out bool badAncestor,
            out int badReason,
            SqliteTransaction? tx = null)
        {
            isBad = false;
            badAncestor = false;
            badReason = BadReasonNone;
            if (hash is not { Length: 32 }) return false;

            lock (Db.Sync)
            {
                try
                {
                    using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                    cmd.Transaction = tx;
                    cmd.CommandText = @"
SELECT is_bad, bad_ancestor, bad_reason
FROM block_index
WHERE hash=$h
LIMIT 1;";
                    cmd.Parameters.AddWithValue("$h", hash);
                    using var r = cmd.ExecuteReader();
                    if (!r.Read())
                        return false;

                    isBad = r.GetInt64(0) != 0;
                    badAncestor = r.GetInt64(1) != 0;
                    badReason = (int)r.GetInt64(2);
                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }

        public static bool IsBadOrHasBadAncestor(byte[] hash, SqliteTransaction? tx = null)
        {
            if (!TryGetBadFlags(hash, out var isBad, out var badAncestor, out _, tx))
                return false;
            return isBad || badAncestor;
        }

        public static int MarkBadAndDescendants(byte[] rootHash, int badReason, SqliteTransaction? tx = null)
        {
            if (rootHash is not { Length: 32 })
                return 0;

            if (badReason < 0)
                badReason = BadReasonNone;

            lock (Db.Sync)
            {
                bool ownTx = tx == null;
                using var localTx = ownTx ? Conn.BeginTransaction() : null;
                var workTx = tx ?? localTx!;
                var conn = workTx.Connection ?? Conn;

                if (!ContainsHash(rootHash, workTx))
                {
                    if (ownTx)
                        localTx!.Rollback();
                    return 0;
                }

                int affected = 0;

                using (var rootCmd = conn.CreateCommand())
                {
                    rootCmd.Transaction = workTx;
                    rootCmd.CommandText = @"
UPDATE block_index
SET is_bad = 1,
    bad_reason = CASE WHEN bad_reason = 0 THEN $reason ELSE bad_reason END,
    status = $invalidStatus
WHERE hash = $h;";
                    rootCmd.Parameters.AddWithValue("$reason", badReason);
                    rootCmd.Parameters.AddWithValue("$invalidStatus", StatusSideStateInvalid);
                    rootCmd.Parameters.AddWithValue("$h", rootHash);
                    affected += rootCmd.ExecuteNonQuery();
                }

                var queue = new Queue<byte[]>();
                var seen = new HashSet<string>(StringComparer.Ordinal);
                queue.Enqueue((byte[])rootHash.Clone());
                seen.Add(Convert.ToHexString(rootHash).ToLowerInvariant());

                while (queue.Count > 0)
                {
                    var parent = queue.Dequeue();
                    var children = new List<byte[]>();

                    using (var q = conn.CreateCommand())
                    {
                        q.Transaction = workTx;
                        q.CommandText = "SELECT hash FROM block_index WHERE prev_hash=$p;";
                        q.Parameters.AddWithValue("$p", parent);
                        using var r = q.ExecuteReader();
                        while (r.Read())
                        {
                            if (r[0] is byte[] h && h.Length == 32)
                                children.Add(h);
                        }
                    }

                    for (int i = 0; i < children.Count; i++)
                    {
                        var child = children[i];
                        string key = Convert.ToHexString(child).ToLowerInvariant();
                        if (!seen.Add(key))
                            continue;

                        using var up = conn.CreateCommand();
                        up.Transaction = workTx;
                        up.CommandText = @"
UPDATE block_index
SET bad_ancestor = 1
WHERE hash = $h
  AND bad_ancestor = 0;";
                        up.Parameters.AddWithValue("$h", child);
                        affected += up.ExecuteNonQuery();

                        queue.Enqueue((byte[])child.Clone());
                    }
                }

                if (ownTx)
                    localTx!.Commit();

                return affected;
            }
        }

        public static int MarkBadSelf(byte[] hash, int badReason, SqliteTransaction? tx = null)
        {
            if (hash is not { Length: 32 })
                return 0;

            if (badReason < 0)
                badReason = BadReasonNone;

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = @"
UPDATE block_index
SET is_bad = 1,
    bad_reason = CASE WHEN bad_reason = 0 THEN $reason ELSE bad_reason END,
    status = $invalidStatus
WHERE hash = $h;";
                cmd.Parameters.AddWithValue("$reason", badReason);
                cmd.Parameters.AddWithValue("$invalidStatus", StatusSideStateInvalid);
                cmd.Parameters.AddWithValue("$h", hash);
                return cmd.ExecuteNonQuery();
            }
        }

        public static int ClearBadFlagsForNonCanonical(SqliteTransaction? tx = null)
        {
            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = @"
UPDATE block_index
SET is_bad = 0,
    bad_ancestor = 0,
    bad_reason = 0
WHERE (is_bad <> 0 OR bad_ancestor <> 0)
  AND hash NOT IN (SELECT hash FROM canon);";
                return cmd.ExecuteNonQuery();
            }
        }

        public static void SetStatus(byte[] hash, int statusFlags, SqliteTransaction? tx = null)
        {
            if (hash is not { Length: 32 }) throw new ArgumentException("hash must be 32 bytes", nameof(hash));

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "UPDATE block_index SET status=$st WHERE hash=$h;";
                cmd.Parameters.AddWithValue("$st", statusFlags);
                cmd.Parameters.AddWithValue("$h", hash);
                cmd.ExecuteNonQuery();
            }
        }

        public static int CountChildren(byte[] prevHash, SqliteTransaction? tx = null)
        {
            if (prevHash is not { Length: 32 }) return 0;

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "SELECT COUNT(1) FROM block_index WHERE prev_hash=$p;";
                cmd.Parameters.AddWithValue("$p", prevHash);
                var v = cmd.ExecuteScalar();
                return v is long l ? (int)l : 0;
            }
        }

        public static (long count, long totalBytes) GetStatusStats(int statusFlags, SqliteTransaction? tx = null)
        {
            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = @"
SELECT COUNT(1), IFNULL(SUM(file_size), 0)
FROM block_index
WHERE status = $st;";
                cmd.Parameters.AddWithValue("$st", statusFlags);

                using var r = cmd.ExecuteReader();
                if (!r.Read()) return (0, 0);

                long count = r.IsDBNull(0) ? 0 : r.GetInt64(0);
                long totalBytes = r.IsDBNull(1) ? 0 : r.GetInt64(1);
                return (count, totalBytes);
            }
        }

        public static (long count, long totalBytes) GetNonCanonicalPayloadStats(SqliteTransaction? tx = null)
        {
            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = @"
SELECT COUNT(1), IFNULL(SUM(bi.file_size), 0)
FROM block_index bi
LEFT JOIN canon c ON c.hash = bi.hash
WHERE c.hash IS NULL
  AND bi.status = $st
  AND bi.is_bad = 0
  AND bi.bad_ancestor = 0;";
                cmd.Parameters.AddWithValue("$st", StatusHaveBlockPayload);

                using var r = cmd.ExecuteReader();
                if (!r.Read()) return (0, 0);

                long count = r.IsDBNull(0) ? 0 : r.GetInt64(0);
                long totalBytes = r.IsDBNull(1) ? 0 : r.GetInt64(1);
                return (count, totalBytes);
            }
        }

        public static bool IsValidatedStatus(int statusFlags)
            => statusFlags == StatusCanonicalStateValidated;

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

