using System;
using Microsoft.Data.Sqlite;
using Qado.Blockchain;
using Qado.Logging;
using Qado.Serialization;

namespace Qado.Storage
{
    public static class StateRebuilder
    {
        public static void RebuildToTip(byte[] tipHash, ILogSink? log = null)
        {
            if (tipHash is not { Length: 32 })
                throw new ArgumentException("tipHash must be 32 bytes", nameof(tipHash));

            lock (Db.Sync)
            {
                using var tx = Db.Connection.BeginTransaction();

                if (!TryGetHeightFromBlockIndex(tipHash, tx, out ulong tipHeight))
                    throw new InvalidOperationException("tipHash unknown in block_index.");

                var canonTip = GetCanonHashAtHeight(tipHeight, tx);
                if (canonTip is not { Length: 32 } || !BytesEqual(canonTip, tipHash))
                    throw new InvalidOperationException("canon does not match requested tipHash; aborting rebuild.");

                Exec(tx, "DELETE FROM accounts;");

                for (ulong h = 0; h <= tipHeight; h++)
                {
                    var hash = GetCanonHashAtHeight(h, tx);
                    if (hash is not { Length: 32 })
                        throw new InvalidOperationException($"canon missing at height {h}.");

                    if (!TryGetPayload(hash, tx, out var payload))
                        throw new InvalidOperationException($"block payload missing for canon hash at height {h}.");

                    var blk = BlockBinarySerializer.Read(payload);
                    blk.BlockHeight = h;
                    blk.BlockHash = (byte[])hash.Clone();

                    StateApplier.ApplyBlock(blk, tx);
                }

                tx.Commit();
            }

            log?.Info("State", $"State rebuild to tip {Convert.ToHexString(tipHash).ToLowerInvariant()} done.");
        }

        private static byte[]? GetCanonHashAtHeight(ulong height, SqliteTransaction tx)
        {
            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "SELECT hash FROM canon WHERE height=$h LIMIT 1;";
            cmd.Parameters.AddWithValue("$h", (long)height);
            var v = cmd.ExecuteScalar() as byte[];
            return v is { Length: 32 } ? v : null;
        }

        private static bool TryGetHeightFromBlockIndex(byte[] hash, SqliteTransaction tx, out ulong height)
        {
            height = 0;

            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "SELECT height FROM block_index WHERE hash=$h LIMIT 1;";
            cmd.Parameters.AddWithValue("$h", hash);

            var v = cmd.ExecuteScalar();
            if (v is not long l || l < 0) return false;
            height = (ulong)l;
            return true;
        }

        private static bool TryGetPayload(byte[] hash, SqliteTransaction tx, out byte[] payload)
        {
            payload = Array.Empty<byte>();

            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "SELECT payload FROM block_payloads WHERE hash=$h LIMIT 1;";
            cmd.Parameters.AddWithValue("$h", hash);

            var v = cmd.ExecuteScalar() as byte[];
            if (v is not { Length: > 0 })
                return false;

            payload = (byte[])v.Clone();
            return true;
        }

        private static void Exec(SqliteTransaction tx, string sql)
        {
            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        private static bool BytesEqual(byte[] a, byte[] b)
        {
            if (a.Length != b.Length) return false;
            for (int i = 0; i < a.Length; i++)
                if (a[i] != b[i]) return false;
            return true;
        }

    }
}

