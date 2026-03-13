using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace Qado.Storage
{
    internal static class Schema
    {
        public static void Ensure()
        {
            using var tx = Db.Connection.BeginTransaction();

            EnsureBaseTables(tx);
            EnsureChainTables(tx);

            ValidateEndgameSchemaOrThrow(tx);

            tx.Commit();
        }

        private static void EnsureBaseTables(SqliteTransaction tx)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS accounts(
  addr    BLOB(32) PRIMARY KEY,
  balance BLOB(8)  NOT NULL, -- ulong (u64) as raw 8 bytes (LE)
  nonce   BLOB(8)  NOT NULL  -- ulong (u64) as raw 8 bytes (LE)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS peers(
  id        BLOB(32) PRIMARY KEY,
  ip        TEXT NOT NULL,
  port      INTEGER NOT NULL,
  last_seen INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_peers_last_seen    ON peers(last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_peers_ip_last_seen ON peers(ip, last_seen DESC);

CREATE TABLE IF NOT EXISTS peer_announced(
  id          BLOB(32) PRIMARY KEY,
  announced_at INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_peer_announced_at ON peer_announced(announced_at);

CREATE TABLE IF NOT EXISTS peer_quarantine(
  ip       TEXT NOT NULL,
  port     INTEGER NOT NULL,
  until_ts INTEGER NOT NULL,
  reason   TEXT,
  PRIMARY KEY(ip, port)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_peer_quarantine_until ON peer_quarantine(until_ts);

CREATE TABLE IF NOT EXISTS keys(
  pub  BLOB(32) PRIMARY KEY,
  priv BLOB NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS meta(
  key   TEXT PRIMARY KEY,
  value TEXT
);
";
            cmd.ExecuteNonQuery();
        }

        private static void EnsureChainTables(SqliteTransaction tx)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS block_index(
  hash        BLOB(32) PRIMARY KEY,
  prev_hash   BLOB(32) NOT NULL,
  height      INTEGER NOT NULL,
  ts          INTEGER NOT NULL,
  target      BLOB(32) NOT NULL,
  miner       BLOB(32) NOT NULL,
  chainwork   BLOB(16) NOT NULL,
  status      INTEGER NOT NULL,
  is_bad      INTEGER NOT NULL DEFAULT 0,
  bad_reason  INTEGER NOT NULL DEFAULT 0,
  bad_ancestor INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS block_payloads(
  hash    BLOB(32) PRIMARY KEY,
  payload BLOB NOT NULL
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_bi_height ON block_index(height);
CREATE INDEX IF NOT EXISTS idx_bi_prev   ON block_index(prev_hash);
CREATE INDEX IF NOT EXISTS idx_parent    ON block_index(prev_hash);
CREATE INDEX IF NOT EXISTS idx_bi_status ON block_index(status);
CREATE INDEX IF NOT EXISTS idx_bi_chainwork ON block_index(chainwork DESC);
CREATE INDEX IF NOT EXISTS idx_bi_bad ON block_index(is_bad, bad_ancestor);

CREATE TABLE IF NOT EXISTS canon(
  height INTEGER PRIMARY KEY,
  hash   BLOB(32) NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS tx_index(
  txid       BLOB(32) NOT NULL,
  block_hash BLOB(32) NOT NULL,
  height     INTEGER NOT NULL,
  offset     INTEGER NOT NULL,
  size       INTEGER NOT NULL,
  PRIMARY KEY(txid, block_hash)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_tx_height ON tx_index(height);
CREATE INDEX IF NOT EXISTS idx_tx_txid   ON tx_index(txid);
";
            cmd.ExecuteNonQuery();
        }

        private static void ValidateEndgameSchemaOrThrow(SqliteTransaction tx)
        {
            RequireColumns(tx, "accounts", new[] { "addr", "balance", "nonce" });
            RequireColumnTypePrefix(tx, "accounts", "addr", "BLOB");
            RequireColumnTypePrefix(tx, "accounts", "balance", "BLOB");
            RequireColumnTypePrefix(tx, "accounts", "nonce", "BLOB");

            RequireColumns(tx, "block_index", new[]
            {
                "hash","prev_hash","height","ts","target","miner","chainwork","status","is_bad","bad_reason","bad_ancestor"
            });
            RequireColumns(tx, "block_payloads", new[] { "hash", "payload" });

            RequireColumns(tx, "canon", new[] { "height", "hash" });
            RequireColumns(tx, "meta", new[] { "key", "value" });
            RequireColumns(tx, "peer_announced", new[] { "id", "announced_at" });
            RequireColumns(tx, "peer_quarantine", new[] { "ip", "port", "until_ts", "reason" });

            RequireColumns(tx, "tx_index", new[] { "txid", "block_hash", "height", "offset", "size" });

        }

        private static void RequireColumns(SqliteTransaction tx, string table, IEnumerable<string> required)
        {
            var existing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            using (var cmd = Db.Connection.CreateCommand())
            {
                cmd.Transaction = tx;
                cmd.CommandText = $"PRAGMA table_info({table});";
                using var r = cmd.ExecuteReader();
                while (r.Read())
                {
                    var name = r.GetString(1);
                    existing.Add(name);
                }
            }

            foreach (var col in required)
            {
                if (!existing.Contains(col))
                {
                    throw new InvalidOperationException(
                        $"SQLite schema mismatch: table '{table}' missing column '{col}'. " +
                        $"No migrations are supported. Delete the database file and restart.");
                }
            }
        }

        private static void RequireColumnTypePrefix(SqliteTransaction tx, string table, string column, string expectedPrefix)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = $"PRAGMA table_info({table});";

            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                var name = r.GetString(1);
                if (!string.Equals(name, column, StringComparison.OrdinalIgnoreCase))
                    continue;

                string declaredType = r.IsDBNull(2) ? string.Empty : r.GetString(2);
                if (declaredType.StartsWith(expectedPrefix, StringComparison.OrdinalIgnoreCase))
                    return;

                throw new InvalidOperationException(
                    $"SQLite schema mismatch: table '{table}' column '{column}' has type '{declaredType}', " +
                    $"expected '{expectedPrefix}'. No migrations are supported. Delete the database file and restart.");
            }

            throw new InvalidOperationException(
                $"SQLite schema mismatch: table '{table}' missing column '{column}'. " +
                $"No migrations are supported. Delete the database file and restart.");
        }
    }
}

