using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
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
            if (!IsPublicRoutableIPv4Literal(ip)) return;
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

            DeleteNonPublicPeersCore(conn, tx);

            using (var trimGlobal = conn.CreateCommand())
            {
                trimGlobal.Transaction = tx;
                trimGlobal.CommandText = trimGlobalSql;
                trimGlobal.Parameters.AddWithValue("$max_total", MaxPeersTotal);
                trimGlobal.ExecuteNonQuery();
            }
        }

        private static void DeleteNonPublicPeersCore(SqliteConnection conn, SqliteTransaction? tx)
        {
            var deleteIds = new List<byte[]>();

            using (var select = conn.CreateCommand())
            {
                select.Transaction = tx;
                select.CommandText = "SELECT id, ip FROM peers;";

                using var r = select.ExecuteReader();
                while (r.Read())
                {
                    byte[] id = (byte[])r[0];
                    string ip = r.GetString(1);
                    if (!IsPublicRoutableIPv4Literal(ip))
                        deleteIds.Add(id);
                }
            }

            if (deleteIds.Count == 0)
                return;

            using var del = conn.CreateCommand();
            del.Transaction = tx;
            del.CommandText = "DELETE FROM peers WHERE id = $id;";
            var pId = del.CreateParameter();
            pId.ParameterName = "$id";
            del.Parameters.Add(pId);

            for (int i = 0; i < deleteIds.Count; i++)
            {
                pId.Value = deleteIds[i];
                del.ExecuteNonQuery();
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

        private static bool IsPublicRoutableIPv4Literal(string ip)
        {
            if (string.IsNullOrWhiteSpace(ip)) return false;

            string host = NormalizeHost(ip);
            if (!IPAddress.TryParse(host, out var a)) return false;
            if (a.AddressFamily != AddressFamily.InterNetwork) return false;

            var b = a.GetAddressBytes();
            if (b.Length != 4) return false;

            if (b[0] == 0) return false; // 0.0.0.0/8
            if (b[0] == 10) return false; // RFC1918
            if (b[0] == 100 && b[1] >= 64 && b[1] <= 127) return false; // CGNAT 100.64/10
            if (b[0] == 127) return false; // loopback
            if (b[0] == 169 && b[1] == 254) return false; // link-local
            if (b[0] == 172 && b[1] >= 16 && b[1] <= 31) return false; // RFC1918
            if (b[0] == 192 && b[1] == 168) return false; // RFC1918
            if (b[0] == 198 && (b[1] == 18 || b[1] == 19)) return false; // benchmark net
            if (b[0] >= 224) return false; // multicast + reserved

            return true;
        }

        private static string NormalizeHost(string value)
        {
            if (string.IsNullOrWhiteSpace(value)) return string.Empty;

            string s = value.Trim().ToLowerInvariant();

            if (s.StartsWith("[", StringComparison.Ordinal))
            {
                int close = s.IndexOf(']');
                if (close > 1)
                    s = s.Substring(1, close - 1);
            }
            else
            {
                int firstColon = s.IndexOf(':');
                int lastColon = s.LastIndexOf(':');
                if (firstColon > 0 && firstColon == lastColon)
                {
                    string tail = s[(firstColon + 1)..];
                    if (int.TryParse(tail, out _))
                        s = s[..firstColon];
                }
            }

            if (s.StartsWith("::ffff:", StringComparison.Ordinal))
                s = s[7..];

            return s;
        }

        private static void RaisePeerListChanged()
        {
            try { PeerListChanged?.Invoke(); } catch { }
        }
    }
}

