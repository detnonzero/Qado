using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;
using Qado.Networking;

namespace Qado.Storage
{
    public static class PeerStore
    {
        public static event Action? PeerListChanged;

        public static readonly int MaxPortsPerIp = ReadIntEnv("QADO_PEERS_MAX_PORTS_PER_IP", fallback: 4, min: 1, max: 32);
        public static readonly int MaxPeersTotal = ReadIntEnv("QADO_PEERS_MAX_TOTAL", fallback: 5_000, min: 500, max: 200_000);
        public static readonly ulong PeerTtlSeconds =
            (ulong)ReadIntEnv("QADO_PEERS_TTL_DAYS", fallback: 21, min: 1, max: 365) * 24UL * 60UL * 60UL;

        public static void Upsert(byte[] id, string ip, int port, ulong lastSeen, byte[]? pubkey, SqliteTransaction? tx = null)
        {
            if (id is not { Length: 32 }) throw new ArgumentException("id must be 32 bytes", nameof(id));
            if (string.IsNullOrWhiteSpace(ip)) throw new ArgumentNullException(nameof(ip));
            if (port <= 0 || port > 65535) throw new ArgumentOutOfRangeException(nameof(port));

            if (tx != null)
            {
                UpsertCore(tx.Connection!, tx, id, ip, port, lastSeen, pubkey);
                return;
            }

            lock (Db.Sync)
            {
                UpsertCore(Db.Connection, null, id, ip, port, lastSeen, pubkey);
            }

            RaisePeerListChanged();
        }

        public static void UpsertByEndpoint(string ip, int port, ulong lastSeen, SqliteTransaction? tx = null)
            => UpsertByEndpoint(ip, port, lastSeen, networkId: GenesisConfig.NetworkId, tx);

        public static void UpsertByEndpoint(string ip, int port, ulong lastSeen, byte networkId, SqliteTransaction? tx = null)
        {
            if (string.IsNullOrWhiteSpace(ip) || port <= 0) return;
            if (SelfPeerGuard.IsSelf(ip, port)) return;
            byte[] id = ComputeEndpointId(networkId, ip, port);

            if (tx != null)
            {
                UpsertCore(tx.Connection!, tx, id, ip, port, lastSeen, pubkey: null);
                EnforcePerIpPortCapCore(tx.Connection!, tx, ip);
                return;
            }

            lock (Db.Sync)
            {
                UpsertCore(Db.Connection, null, id, ip, port, lastSeen, pubkey: null);
                EnforcePerIpPortCapCore(Db.Connection, null, ip);
            }

            RaisePeerListChanged();
        }

        public static void MarkSeen(string ip, int port, ulong lastSeen, SqliteTransaction? tx = null)
            => UpsertByEndpoint(ip, port, lastSeen, networkId: GenesisConfig.NetworkId, tx);

        public static void MarkSeen(string ip, int port, ulong lastSeen, byte networkId, SqliteTransaction? tx = null)
            => UpsertByEndpoint(ip, port, lastSeen, networkId, tx);

        public static void PruneAndEnforceLimits(ulong? nowUnix = null, SqliteTransaction? tx = null)
        {
            ulong now = nowUnix ?? (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            long cutoff = now > PeerTtlSeconds ? (long)(now - PeerTtlSeconds) : 0L;

            if (tx != null)
            {
                PruneAndEnforceLimitsCore(tx.Connection!, tx, cutoff);
                return;
            }

            lock (Db.Sync)
            {
                PruneAndEnforceLimitsCore(Db.Connection, null, cutoff);
            }

            RaisePeerListChanged();
        }

        public static List<(string ip, int port, ulong lastSeen)> GetRecentPeers(int limit = 64)
        {
            if (limit <= 0) limit = 64;

            lock (Db.Sync)
            {
                var list = new List<(string ip, int port, ulong lastSeen)>(limit);

                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = "SELECT ip, port, last_seen FROM peers ORDER BY last_seen DESC LIMIT $n;";
                cmd.Parameters.AddWithValue("$n", limit);

                using var r = cmd.ExecuteReader();
                while (r.Read())
                {
                    string ip = r.GetString(0);
                    int port = r.GetInt32(1);
                    long ts = r.GetInt64(2);
                    list.Add((ip, port, ts < 0 ? 0UL : (ulong)ts));
                }

                return list;
            }
        }

        private static void UpsertCore(
            SqliteConnection conn,
            SqliteTransaction? tx,
            byte[] id,
            string ip,
            int port,
            ulong lastSeen,
            byte[]? pubkey)
        {
            const string sql = @"
INSERT INTO peers(id,ip,port,last_seen,pubkey)
VALUES($i,$ip,$p,$t,$k)
ON CONFLICT(id) DO UPDATE SET
  ip=excluded.ip,
  port=excluded.port,
  last_seen=excluded.last_seen,
  pubkey=excluded.pubkey;";

            long ts = lastSeen <= long.MaxValue ? (long)lastSeen : long.MaxValue;

            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("$i", id);
            cmd.Parameters.AddWithValue("$ip", ip);
            cmd.Parameters.AddWithValue("$p", port);
            cmd.Parameters.AddWithValue("$t", ts);
            cmd.Parameters.AddWithValue("$k", (object?)pubkey ?? DBNull.Value);
            cmd.ExecuteNonQuery();
        }

        private static void EnforcePerIpPortCapCore(SqliteConnection conn, SqliteTransaction? tx, string ip)
        {
            const string sql = @"
DELETE FROM peers
WHERE ip = $ip
  AND id NOT IN (
    SELECT id
    FROM peers
    WHERE ip = $ip
    ORDER BY last_seen DESC, port ASC
    LIMIT $max_ports
  );";

            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("$ip", ip);
            cmd.Parameters.AddWithValue("$max_ports", MaxPortsPerIp);
            cmd.ExecuteNonQuery();
        }

        private static void PruneAndEnforceLimitsCore(SqliteConnection conn, SqliteTransaction? tx, long cutoff)
        {
            const string delExpiredSql = "DELETE FROM peers WHERE last_seen < $cutoff;";
            const string trimGlobalSql = @"
DELETE FROM peers
WHERE id NOT IN (
  SELECT id
  FROM peers
  ORDER BY last_seen DESC, ip ASC, port ASC
  LIMIT $max_total
);";

            using (var delExpired = conn.CreateCommand())
            {
                delExpired.Transaction = tx;
                delExpired.CommandText = delExpiredSql;
                delExpired.Parameters.AddWithValue("$cutoff", cutoff);
                delExpired.ExecuteNonQuery();
            }

            using (var trimGlobal = conn.CreateCommand())
            {
                trimGlobal.Transaction = tx;
                trimGlobal.CommandText = trimGlobalSql;
                trimGlobal.Parameters.AddWithValue("$max_total", MaxPeersTotal);
                trimGlobal.ExecuteNonQuery();
            }
        }

        private static byte[] ComputeEndpointId(byte networkId, string ip, int port)
        {
            var s = $"v1|{networkId}|{ip}:{port}";
            return SHA256.HashData(Encoding.UTF8.GetBytes(s));
        }

        private static int ReadIntEnv(string name, int fallback, int min, int max)
        {
            var raw = Environment.GetEnvironmentVariable(name);
            if (string.IsNullOrWhiteSpace(raw))
                return fallback;

            if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
                return fallback;

            if (parsed < min) return min;
            if (parsed > max) return max;
            return parsed;
        }

        private static void RaisePeerListChanged()
        {
            try { PeerListChanged?.Invoke(); } catch { }
        }
    }
}

