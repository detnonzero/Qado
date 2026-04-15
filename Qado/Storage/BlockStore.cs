using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Numerics;
using Microsoft.Data.Sqlite;
using Qado.Blockchain;
using Qado.Serialization;
using Qado.Utils;

namespace Qado.Storage
{
    public static class BlockStore
    {
        private static SqliteConnection Conn =>
            Db.Connection ?? throw new InvalidOperationException("Db not initialized");


        public static void SaveBlock(
            Block block,
            SqliteTransaction? tx = null,
            int statusFlags = BlockIndexStore.StatusCanonicalStateValidated)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            if (block.Header == null) throw new ArgumentException("block.Header is null", nameof(block));

            lock (Db.Sync)
            {
                SaveBlock_NoLock(block, tx, statusFlags);
            }
        }

        private static void SaveBlock_NoLock(Block block, SqliteTransaction? tx, int statusFlags)
        {
            EnsureCanonicalBlockHash(block);

            int payloadSize = BlockBinarySerializer.GetSize(block);
            var payload = new byte[payloadSize];
            _ = BlockBinarySerializer.Write(payload, block);
            BlockPayloadStore.Upsert(block.BlockHash!, payload, tx);

            UInt128 parentWork = 0;
            var prev = block.Header!.PreviousBlockHash;
            if (prev is { Length: 32 })
                parentWork = BlockIndexStore.GetChainwork(prev, tx);

            if (!Difficulty.IsValidTarget(block.Header.Target))
                throw new InvalidOperationException("Block header target out of consensus range.");

            var target = (byte[])block.Header.Target.Clone();
            BigInteger diff = Difficulty.TargetToDifficulty(target);
            UInt128 delta = BigToUInt128Sat(diff);
            UInt128 cw = ChainworkUtil.Add(parentWork, delta);

            if (statusFlags <= 0)
                statusFlags = BlockIndexStore.StatusCanonicalStateValidated;

            BlockIndexStore.Upsert(
                hash: block.BlockHash!,
                prevHash: block.Header.PreviousBlockHash ?? new byte[32],
                height: block.BlockHeight,
                ts: block.Header.Timestamp,
                target32: target,
                miner32: block.Header.Miner,
                chainwork: cw,
                statusFlags: statusFlags,
                tx: tx);
        }

        public static Block? GetBlockByHash(byte[] hash, SqliteTransaction? tx = null)
        {
            if (hash is not { Length: 32 }) throw new ArgumentException("hash must be 32 bytes", nameof(hash));

            if (!BlockIndexStore.ContainsHash(hash, tx))
                return null;

            if (!BlockPayloadStore.TryGet(hash, out var payload, tx))
            {
                TryMarkPayloadMissing(hash, tx);
                return null;
            }

            Block block;
            try
            {
                block = BlockBinarySerializer.Read(payload);
            }
            catch
            {
                TryMarkPayloadMissing(hash, tx);
                return null;
            }
            block.BlockHash = (byte[])hash.Clone();

            if (BlockIndexStore.TryGetMeta(hash, out var height, out _, out _, tx))
                block.BlockHeight = height;

            return block;
        }

        public static bool TryGetSerializedBlockByHash(byte[] hash, out byte[] payload, SqliteTransaction? tx = null)
        {
            payload = Array.Empty<byte>();
            if (hash is not { Length: 32 })
                return false;

            if (!BlockIndexStore.ContainsHash(hash, tx))
                return false;

            if (!BlockPayloadStore.TryGet(hash, out payload, tx))
            {
                TryMarkPayloadMissing(hash, tx);
                payload = Array.Empty<byte>();
                return false;
            }

            return true;
        }

        public static bool TryGetSerializedCanonicalBlockAtHeight(ulong height, out byte[] payload, SqliteTransaction? tx = null)
        {
            payload = Array.Empty<byte>();
            byte[]? hash = GetCanonicalHashAtHeight(height, tx);
            if (hash is not { Length: 32 })
                return false;

            return TryGetSerializedBlockByHash(hash, out payload, tx);
        }

        public static bool TryGetSerializedCanonicalBlocksRange(
            ulong startHeight,
            int maxBlocks,
            out List<(ulong Height, byte[] Hash, byte[] Payload)> blocks,
            out ulong firstMissingHeight,
            SqliteTransaction? tx = null)
        {
            blocks = new List<(ulong Height, byte[] Hash, byte[] Payload)>(Math.Max(0, maxBlocks));
            firstMissingHeight = startHeight;
            if (maxBlocks <= 0)
                return true;
            if (startHeight > (ulong)long.MaxValue)
                return false;
            if ((ulong)maxBlocks > (ulong)long.MaxValue - startHeight)
                return false;

            ulong endHeightExclusive = startHeight + (ulong)maxBlocks;
            ulong expectedHeight = startHeight;

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = @"
SELECT c.height, c.hash, bp.payload
FROM canon c
JOIN block_index bi ON bi.hash = c.hash
JOIN block_payloads bp ON bp.hash = c.hash
WHERE c.height >= $start AND c.height < $end
ORDER BY c.height ASC
LIMIT $limit;";
                cmd.Parameters.AddWithValue("$start", (long)startHeight);
                cmd.Parameters.AddWithValue("$end", (long)endHeightExclusive);
                cmd.Parameters.AddWithValue("$limit", maxBlocks);

                using var r = cmd.ExecuteReader();
                while (r.Read())
                {
                    long rawHeight = r.GetInt64(0);
                    if (rawHeight < 0)
                        return false;

                    ulong height = (ulong)rawHeight;
                    if (height != expectedHeight)
                    {
                        firstMissingHeight = expectedHeight;
                        return false;
                    }

                    var hash = r[1] as byte[];
                    var payload = r[2] as byte[];
                    if (hash is not { Length: 32 } || payload is not { Length: > 0 })
                    {
                        firstMissingHeight = expectedHeight;
                        return false;
                    }

                    blocks.Add((height, hash, payload));
                    expectedHeight++;
                }
            }

            firstMissingHeight = expectedHeight;
            return blocks.Count == maxBlocks;
        }

        private static void TryMarkPayloadMissing(byte[] hash, SqliteTransaction? tx = null)
        {
            try { BlockPayloadStore.Delete(hash, tx); } catch { }
            try { BlockIndexStore.MarkPayloadMissing(hash, tx); } catch { }
        }

        public static Block? GetBlockByHeight(ulong height, SqliteTransaction? tx = null)
        {
            byte[]? h = GetCanonicalHashAtHeight(height, tx);
            if (h == null) return null;

            var b = GetBlockByHash(h, tx);
            if (b != null) b.BlockHeight = height;
            return b;
        }

        public static Block? GetLatestBlock(SqliteTransaction? tx = null)
        {
            if (!TryGetLatestHeight(out var h, tx)) return null;
            return GetBlockByHeight(h, tx);
        }

        public static byte[]? GetCanonicalHashAtHeight(ulong height, SqliteTransaction? tx = null)
        {
            lock (Db.Sync)
            {
                try
                {
                    using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                    cmd.Transaction = tx;
                    cmd.CommandText = "SELECT hash FROM canon WHERE height=$h LIMIT 1;";
                    cmd.Parameters.AddWithValue("$h", (long)height);
                    var v = cmd.ExecuteScalar() as byte[];
                    return v is { Length: 32 } ? v : null;
                }
                catch
                {
                    return null;
                }
            }
        }

        public static bool TryGetCanonicalHashAndChainworkAtHeight(
            ulong height,
            out byte[] hash,
            out UInt128 chainwork,
            SqliteTransaction? tx = null)
        {
            hash = Array.Empty<byte>();
            chainwork = 0;
            if (height > (ulong)long.MaxValue)
                return false;

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = @"
SELECT c.hash, bi.chainwork
FROM canon c
JOIN block_index bi ON bi.hash = c.hash
WHERE c.height = $h
LIMIT 1;";
                cmd.Parameters.AddWithValue("$h", (long)height);

                using var r = cmd.ExecuteReader();
                if (!r.Read())
                    return false;

                var rawHash = r[0] as byte[];
                var rawChainwork = r[1] as byte[];
                if (rawHash is not { Length: 32 } || rawChainwork is not { Length: 16 })
                    return false;

                hash = rawHash;
                chainwork = U128.ReadBE(rawChainwork);
                return true;
            }
        }

        public static void SetCanonicalHashAtHeight(ulong height, byte[] hash, SqliteTransaction? tx = null)
        {
            if (hash is not { Length: 32 }) throw new ArgumentException("hash must be 32 bytes", nameof(hash));

            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = @"
INSERT INTO canon(height, hash)
VALUES($h, $hash)
ON CONFLICT(height) DO UPDATE SET hash = excluded.hash;";
                cmd.Parameters.AddWithValue("$h", (long)height);
                cmd.Parameters.AddWithValue("$hash", hash);
                cmd.ExecuteNonQuery();
            }
        }

        public static bool TryGetLatestHeight(out ulong height, SqliteTransaction? tx = null)
        {
            lock (Db.Sync)
            {
                try
                {
                    using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                    cmd.Transaction = tx;
                    cmd.CommandText = "SELECT MAX(height) FROM canon;";
                    var v = cmd.ExecuteScalar();
                    if (v == null || v is DBNull)
                    {
                        height = 0;
                        return false;
                    }

                    long l = Convert.ToInt64(v);
                    if (l < 0)
                    {
                        height = 0;
                        return false;
                    }

                    height = (ulong)l;
                    return true;
                }
                catch
                {
                    height = 0;
                    return false;
                }
            }
        }

        public static ulong GetLatestHeight(SqliteTransaction? tx = null)
            => TryGetLatestHeight(out var h, tx) ? h : 0UL;

        public static UInt128 GetChainworkByHash(byte[] hash, SqliteTransaction? tx = null)
            => BlockIndexStore.GetChainwork(hash, tx);

        public static void DeleteCanonicalFromHeight(ulong fromHeight, SqliteTransaction tx)
        {
            if (tx == null) throw new ArgumentNullException(nameof(tx));

            lock (Db.Sync)
            {
                using var delCanon = tx.Connection!.CreateCommand();
                delCanon.Transaction = tx;
                delCanon.CommandText = "DELETE FROM canon WHERE height >= $h;";
                delCanon.Parameters.AddWithValue("$h", (long)fromHeight);
                delCanon.ExecuteNonQuery();

                using var delTxIdx = tx.Connection!.CreateCommand();
                delTxIdx.Transaction = tx;
                delTxIdx.CommandText = "DELETE FROM tx_index WHERE height >= $h;";
                delTxIdx.Parameters.AddWithValue("$h", (long)fromHeight);
                delTxIdx.ExecuteNonQuery();
            }
        }

        public static void DeleteBlocksFromHeight(ulong fromHeight, SqliteTransaction? tx = null)
        {
            lock (Db.Sync)
            {
                if (tx == null)
                {
                    using var localTx = Conn.BeginTransaction();
                    DeleteBlocksFromHeight_NoLock(fromHeight, localTx);
                    localTx.Commit();
                    return;
                }

                DeleteBlocksFromHeight_NoLock(fromHeight, tx);
            }
        }

        private static void DeleteBlocksFromHeight_NoLock(ulong fromHeight, SqliteTransaction tx)
        {
            using (var delPayload = tx.Connection!.CreateCommand())
            {
                delPayload.Transaction = tx;
                delPayload.CommandText = @"
DELETE FROM block_payloads
WHERE hash IN (SELECT hash FROM block_index WHERE height >= $h);";
                delPayload.Parameters.AddWithValue("$h", (long)fromHeight);
                delPayload.ExecuteNonQuery();
            }

            try
            {
                using var delUndo = tx.Connection!.CreateCommand();
                delUndo.Transaction = tx;
                delUndo.CommandText = @"
DELETE FROM state_undo
WHERE block_hash IN (SELECT hash FROM block_index WHERE height >= $h);";
                delUndo.Parameters.AddWithValue("$h", (long)fromHeight);
                delUndo.ExecuteNonQuery();
            }
            catch { }

            using (var delCanon = tx.Connection!.CreateCommand())
            {
                delCanon.Transaction = tx;
                delCanon.CommandText = "DELETE FROM canon WHERE height >= $h;";
                delCanon.Parameters.AddWithValue("$h", (long)fromHeight);
                delCanon.ExecuteNonQuery();
            }

            using (var delTxIdx = tx.Connection!.CreateCommand())
            {
                delTxIdx.Transaction = tx;
                delTxIdx.CommandText = "DELETE FROM tx_index WHERE height >= $h;";
                delTxIdx.Parameters.AddWithValue("$h", (long)fromHeight);
                delTxIdx.ExecuteNonQuery();
            }

            using (var delIdx = tx.Connection!.CreateCommand())
            {
                delIdx.Transaction = tx;
                delIdx.CommandText = "DELETE FROM block_index WHERE height >= $h;";
                delIdx.Parameters.AddWithValue("$h", (long)fromHeight);
                delIdx.ExecuteNonQuery();
            }

        }

        public static void AdoptCanonicalRange(ulong fromHeight, List<(ulong Height, byte[] Hash)> mapping)
        {
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));
            if (mapping.Count == 0) return;

            lock (Db.Sync)
            {
                using var tx = Conn.BeginTransaction();

                DeleteCanonicalFromHeight(fromHeight, tx);

                using var up = tx.Connection!.CreateCommand();
                up.Transaction = tx;
                up.CommandText = @"
INSERT INTO canon(height, hash)
VALUES($h, $hash)
ON CONFLICT(height) DO UPDATE SET hash = excluded.hash;";

                var pH = up.CreateParameter(); pH.ParameterName = "$h"; up.Parameters.Add(pH);
                var pHash = up.CreateParameter(); pHash.ParameterName = "$hash"; up.Parameters.Add(pHash);

                for (int i = 0; i < mapping.Count; i++)
                {
                    var (h, hash) = mapping[i];
                    if (hash is not { Length: 32 }) throw new ArgumentException("mapping hash must be 32 bytes", nameof(mapping));
                    if (h < fromHeight) throw new ArgumentException("mapping contains height below fromHeight", nameof(mapping));

                    pH.Value = (long)h;
                    pHash.Value = hash;
                    up.ExecuteNonQuery();
                }

                tx.Commit();
            }
        }

        public static bool TryExtendCanonTip(Block block, SqliteTransaction tx, out ulong newHeight)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            if (tx == null) throw new ArgumentNullException(nameof(tx));
            if (block.Header == null) throw new ArgumentException("block.Header is null", nameof(block));

            newHeight = 0;

            ulong tipH = GetLatestHeight(tx);
            var tipHash = GetCanonicalHashAtHeight(tipH, tx);
            if (tipHash is not { Length: 32 }) return false;

            var prev = block.Header.PreviousBlockHash;
            if (prev is not { Length: 32 }) return false;

            if (!BytesEqual(prev, tipHash)) return false;

            EnsureCanonicalBlockHash(block);

            newHeight = tipH + 1;
            block.BlockHeight = newHeight;

            SetCanonicalHashAtHeight(newHeight, block.BlockHash!, tx);

            return true;
        }


        private static void EnsureCanonicalBlockHash(Block block)
        {
            if (block.BlockHash is { Length: 32 } && !IsZero32(block.BlockHash))
                return;

            block.BlockHash = block.ComputeBlockHash();
        }

        private static bool IsZero32(byte[] h)
        {
            for (int i = 0; i < 32; i++) if (h[i] != 0) return false;
            return true;
        }

        private static bool BytesEqual(byte[] a, byte[] b)
        {
            if (a.Length != b.Length) return false;
            for (int i = 0; i < a.Length; i++) if (a[i] != b[i]) return false;
            return true;
        }

        private static UInt128 BigToUInt128Sat(BigInteger v)
        {
            if (v <= 0) return 0;

            BigInteger max = (BigInteger.One << 128) - 1;
            if (v >= max) return UInt128.MaxValue;

            byte[] raw = v.ToByteArray(isUnsigned: true, isBigEndian: true);

            Span<byte> buf = stackalloc byte[16];
            buf.Clear();
            raw.AsSpan().CopyTo(buf.Slice(16 - raw.Length));

            ulong hi = BinaryPrimitives.ReadUInt64BigEndian(buf.Slice(0, 8));
            ulong lo = BinaryPrimitives.ReadUInt64BigEndian(buf.Slice(8, 8));
            return ((UInt128)hi << 64) | lo;
        }
    }
}

