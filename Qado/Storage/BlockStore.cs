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


        public static void SaveBlock(Block block, SqliteTransaction? tx = null)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            if (block.Header == null) throw new ArgumentException("block.Header is null", nameof(block));

            lock (Db.Sync)
            {
                SaveBlock_NoLock(block, tx);
            }
        }

        private static void SaveBlock_NoLock(Block block, SqliteTransaction? tx)
        {
            EnsureCanonicalBlockHash(block);

            int payloadSize = BlockBinarySerializer.GetSize(block);
            var payload = new byte[payloadSize];
            _ = BlockBinarySerializer.Write(payload, block);

            var (recOff, recSize, _) = BlockLog.Append(payload, block.BlockHash!);

            UInt128 parentWork = 0;
            var prev = block.Header!.PreviousBlockHash;
            if (prev is { Length: 32 })
                parentWork = BlockIndexStore.GetChainwork(prev, tx);

            var target = Difficulty.ClampTarget(block.Header.Target);
            BigInteger diff = Difficulty.TargetToDifficulty(target);
            UInt128 delta = BigToUInt128Sat(diff);
            UInt128 cw = parentWork + (delta == 0 ? 1 : delta);

            const int fileId = 0;
            const int status = 1; // status: known/valid

            BlockIndexStore.Upsert(
                hash: block.BlockHash!,
                prevHash: block.Header.PreviousBlockHash ?? new byte[32],
                height: block.BlockHeight,
                ts: block.Header.Timestamp,
                target32: target,
                miner32: block.Header.Miner,
                chainwork: cw,
                fileId: fileId,
                recordOffset: recOff,
                recordSize: recSize,
                statusFlags: status,
                tx: tx);
        }

        public static Block? GetBlockByHash(byte[] hash)
        {
            if (hash is not { Length: 32 }) throw new ArgumentException("hash must be 32 bytes", nameof(hash));

            var loc = BlockIndexStore.GetLocation(hash);
            if (loc == null) return null;

            var (_, recordOffset, _) = loc.Value;

            if (!BlockLog.TryReadPayload(recordOffset, out var payload, out var hdrHash, out _))
                return null;

            if (!BytesEqual(hash, hdrHash)) return null;

            var block = BlockBinarySerializer.Read(payload);
            block.BlockHash = (byte[])hash.Clone();

            if (BlockIndexStore.TryGetMeta(hash, out var height, out _, out _))
                block.BlockHeight = height;

            return block;
        }

        public static Block? GetBlockByHeight(ulong height)
        {
            byte[]? h = GetCanonicalHashAtHeight(height);
            if (h == null) return null;

            var b = GetBlockByHash(h);
            if (b != null) b.BlockHeight = height;
            return b;
        }

        public static Block? GetLatestBlock()
        {
            if (!TryGetLatestHeight(out var h)) return null;
            return GetBlockByHeight(h);
        }

        public static byte[]? GetCanonicalHashAtHeight(ulong height, SqliteTransaction? tx = null)
        {
            lock (Db.Sync)
            {
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "SELECT hash FROM canon WHERE height=$h LIMIT 1;";
                cmd.Parameters.AddWithValue("$h", (long)height);
                var v = cmd.ExecuteScalar() as byte[];
                return v is { Length: 32 } ? v : null;
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
                using var cmd = (tx?.Connection ?? Conn).CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "SELECT MAX(height) FROM canon;";
                var v = cmd.ExecuteScalar();
                if (v == null || v is DBNull)
                {
                    height = 0;
                    return false;
                }

                long l = (long)v;
                if (l < 0) { height = 0; return false; }

                height = (ulong)l;
                return true;
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

            if (fromHeight == 0)
            {
                MetaStore.Set("LatestHeight", "0", tx);
                MetaStore.Set("LatestBlockHash", "", tx);
            }
            else
            {
                ulong newTipH = fromHeight - 1;
                var newTipHash = GetCanonicalHashAtHeight(newTipH, tx);

                MetaStore.Set("LatestHeight", newTipH.ToString(), tx);
                MetaStore.Set("LatestBlockHash",
                    newTipHash is { Length: 32 } ? Convert.ToHexString(newTipHash).ToLowerInvariant() : "",
                    tx);
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
            MetaStore.Set("LatestBlockHash", Convert.ToHexString(block.BlockHash!).ToLowerInvariant(), tx);
            MetaStore.Set("LatestHeight", newHeight.ToString(), tx);

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

