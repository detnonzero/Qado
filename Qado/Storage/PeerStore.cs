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
            (ulong)ReadIntEnv("QADO_PEERS_TTL_DAYS", fallback: 1, min: 1, max: 365) * 24UL * 60UL * 60UL;
        private const string QuarantineReasonTtlExpired = "ttl-expired";

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
            => UpsertByEndpointInternal(ip, port, lastSeen, networkId: GenesisConfig.NetworkId, bypassQuarantine: false, tx);

        public static void UpsertByEndpoint(string ip, int port, ulong lastSeen, byte networkId, SqliteTransaction? tx = null)
            => UpsertByEndpointInternal(ip, port, lastSeen, networkId, bypassQuarantine: false, tx);

        private static void UpsertByEndpointInternal(
            string ip,
            int port,
            ulong lastSeen,
            byte networkId,
            bool bypassQuarantine,
            SqliteTransaction? tx = null)
        {
            if (string.IsNullOrWhiteSpace(ip) || port <= 0) return;
            if (!IsPublicRoutableIPv4Literal(ip)) return;
            if (SelfPeerGuard.IsSelf(ip, port)) return;

            string ipNorm = NormalizeHost(ip);
            byte[] id = ComputeEndpointId(networkId, ipNorm, port);
            long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            if (tx != null)
            {
                var conn = tx.Connection!;

                if (!bypassQuarantine && IsEndpointQuarantinedCore(conn, tx, ipNorm, port, now))
                    return;

                if (bypassQuarantine)
                    ClearEndpointQuarantineCore(conn, tx, ipNorm, port);

                UpsertCore(conn, tx, id, ipNorm, port, lastSeen, pubkey: null);
                if (lastSeen > 0)
                    DeleteAnnouncedAtCore(conn, tx, id);

                EnforcePerIpPortCapCore(conn, tx, ipNorm);
                return;
            }

            lock (Db.Sync)
            {
                if (!bypassQuarantine && IsEndpointQuarantinedCore(Db.Connection, null, ipNorm, port, now))
                    return;

                if (bypassQuarantine)
                    ClearEndpointQuarantineCore(Db.Connection, null, ipNorm, port);

                UpsertCore(Db.Connection, null, id, ipNorm, port, lastSeen, pubkey: null);
                if (lastSeen > 0)
                    DeleteAnnouncedAtCore(Db.Connection, null, id);

                EnforcePerIpPortCapCore(Db.Connection, null, ipNorm);
            }

            RaisePeerListChanged();
        }

        public static void MarkSeen(string ip, int port, ulong lastSeen, SqliteTransaction? tx = null)
            => UpsertByEndpointInternal(ip, port, lastSeen, networkId: GenesisConfig.NetworkId, bypassQuarantine: true, tx);

        public static void MarkSeen(string ip, int port, ulong lastSeen, byte networkId, SqliteTransaction? tx = null)
            => UpsertByEndpointInternal(ip, port, lastSeen, networkId, bypassQuarantine: true, tx);

        // Announce-only insert from PEX. Does not update last_seen for existing entries.
        public static bool AnnouncePeer(string ip, int port, SqliteTransaction? tx = null)
            => AnnouncePeer(ip, port, networkId: GenesisConfig.NetworkId, tx);

        // Returns true only when a new peer row was created.
        public static bool AnnouncePeer(string ip, int port, byte networkId, SqliteTransaction? tx = null)
        {
            if (string.IsNullOrWhiteSpace(ip) || port <= 0) return false;
            if (!IsPublicRoutableIPv4Literal(ip)) return false;
            if (SelfPeerGuard.IsSelf(ip, port)) return false;

            string ipNorm = NormalizeHost(ip);
            byte[] id = ComputeEndpointId(networkId, ipNorm, port);
            long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            if (tx != null)
            {
                var conn = tx.Connection!;
                if (IsEndpointQuarantinedCore(conn, tx, ipNorm, port, now))
                    return false;

                bool inserted = InsertAnnouncedIfMissingCore(conn, tx, id, ipNorm, port);
                if (inserted)
                    UpsertAnnouncedAtCore(conn, tx, id, now);

                EnforcePerIpPortCapCore(conn, tx, ipNorm);
                return inserted;
            }

            bool insertedNow;
            lock (Db.Sync)
            {
                if (IsEndpointQuarantinedCore(Db.Connection, null, ipNorm, port, now))
                    return false;

                insertedNow = InsertAnnouncedIfMissingCore(Db.Connection, null, id, ipNorm, port);
                if (insertedNow)
                    UpsertAnnouncedAtCore(Db.Connection, null, id, now);

                EnforcePerIpPortCapCore(Db.Connection, null, ipNorm);
            }

            if (insertedNow)
                RaisePeerListChanged();

            return insertedNow;
        }

        public static void PruneAndEnforceLimits(ulong? nowUnix = null, SqliteTransaction? tx = null)
        {
            ulong now = nowUnix ?? (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            long cutoff = now > PeerTtlSeconds ? (long)(now - PeerTtlSeconds) : 0L;
            long nowTs = now <= long.MaxValue ? (long)now : long.MaxValue;

            if (tx != null)
            {
                PruneAndEnforceLimitsCore(tx.Connection!, tx, cutoff, nowTs);
                return;
            }

            lock (Db.Sync)
            {
                PruneAndEnforceLimitsCore(Db.Connection, null, cutoff, nowTs);
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

        private static bool InsertAnnouncedIfMissingCore(
            SqliteConnection conn,
            SqliteTransaction? tx,
            byte[] id,
            string ip,
            int port)
        {
            const string sql = @"
INSERT INTO peers(id,ip,port,last_seen,pubkey)
VALUES($i,$ip,$p,0,NULL)
ON CONFLICT(id) DO NOTHING;";

            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("$i", id);
            cmd.Parameters.AddWithValue("$ip", ip);
            cmd.Parameters.AddWithValue("$p", port);
            return cmd.ExecuteNonQuery() > 0;
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

        private static void PruneAndEnforceLimitsCore(SqliteConnection conn, SqliteTransaction? tx, long cutoff, long nowTs)
        {
            const string trimGlobalSql = @"
DELETE FROM peers
WHERE id NOT IN (
  SELECT id
  FROM peers
  ORDER BY last_seen DESC, ip ASC, port ASC
  LIMIT $max_total
);";

            UpsertMissingAnnouncedAtNoLock(conn, tx, nowTs);
            PruneQuarantineExpiredCore(conn, tx, nowTs);
            DeleteExpiredPeersAndQuarantineCore(conn, tx, cutoff, nowTs);

            DeleteNonPublicPeersCore(conn, tx);

            using (var trimGlobal = conn.CreateCommand())
            {
                trimGlobal.Transaction = tx;
                trimGlobal.CommandText = trimGlobalSql;
                trimGlobal.Parameters.AddWithValue("$max_total", MaxPeersTotal);
                trimGlobal.ExecuteNonQuery();
            }

            CleanupAnnouncedOrphansCore(conn, tx);
        }

        private static void UpsertMissingAnnouncedAtNoLock(SqliteConnection conn, SqliteTransaction? tx, long nowTs)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = @"
INSERT OR IGNORE INTO peer_announced(id, announced_at)
SELECT p.id, $now
FROM peers p
LEFT JOIN peer_announced a ON a.id = p.id
WHERE p.last_seen = 0
  AND a.id IS NULL;";
            cmd.Parameters.AddWithValue("$now", nowTs);
            cmd.ExecuteNonQuery();
        }

        private static void DeleteExpiredPeersAndQuarantineCore(SqliteConnection conn, SqliteTransaction? tx, long cutoff, long nowTs)
        {
            var expired = new List<(byte[] id, string ip, int port)>();

            using (var sel = conn.CreateCommand())
            {
                sel.Transaction = tx;
                sel.CommandText = @"
SELECT p.id, p.ip, p.port
FROM peers p
LEFT JOIN peer_announced a
  ON a.id = p.id
WHERE
    (p.last_seen > 0 AND p.last_seen < $cutoff)
 OR (p.last_seen = 0 AND IFNULL(a.announced_at, 0) < $cutoff);";
                sel.Parameters.AddWithValue("$cutoff", cutoff);

                using var r = sel.ExecuteReader();
                while (r.Read())
                {
                    var id = (byte[])r[0];
                    var ip = r.GetString(1);
                    int port = r.GetInt32(2);
                    expired.Add((id, ip, port));
                }
            }

            if (expired.Count == 0)
                return;

            long untilTs = ComputeQuarantineUntil(nowTs);
            for (int i = 0; i < expired.Count; i++)
                UpsertEndpointQuarantineCore(conn, tx, expired[i].ip, expired[i].port, untilTs, QuarantineReasonTtlExpired);

            using var delPeer = conn.CreateCommand();
            delPeer.Transaction = tx;
            delPeer.CommandText = "DELETE FROM peers WHERE id = $id;";
            var pPeerId = delPeer.CreateParameter();
            pPeerId.ParameterName = "$id";
            delPeer.Parameters.Add(pPeerId);

            using var delAnn = conn.CreateCommand();
            delAnn.Transaction = tx;
            delAnn.CommandText = "DELETE FROM peer_announced WHERE id = $id;";
            var pAnnId = delAnn.CreateParameter();
            pAnnId.ParameterName = "$id";
            delAnn.Parameters.Add(pAnnId);

            for (int i = 0; i < expired.Count; i++)
            {
                pPeerId.Value = expired[i].id;
                delPeer.ExecuteNonQuery();

                pAnnId.Value = expired[i].id;
                delAnn.ExecuteNonQuery();
            }
        }

        private static bool IsEndpointQuarantinedCore(SqliteConnection conn, SqliteTransaction? tx, string ip, int port, long nowTs)
        {
            PruneQuarantineExpiredCore(conn, tx, nowTs);

            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = @"
SELECT until_ts
FROM peer_quarantine
WHERE ip = $ip AND port = $p
LIMIT 1;";
            cmd.Parameters.AddWithValue("$ip", ip);
            cmd.Parameters.AddWithValue("$p", port);
            var v = cmd.ExecuteScalar();
            if (v is not long untilTs)
                return false;

            if (untilTs <= nowTs)
            {
                ClearEndpointQuarantineCore(conn, tx, ip, port);
                return false;
            }

            return true;
        }

        private static void PruneQuarantineExpiredCore(SqliteConnection conn, SqliteTransaction? tx, long nowTs)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "DELETE FROM peer_quarantine WHERE until_ts <= $now;";
            cmd.Parameters.AddWithValue("$now", nowTs);
            cmd.ExecuteNonQuery();
        }

        private static void UpsertEndpointQuarantineCore(
            SqliteConnection conn,
            SqliteTransaction? tx,
            string ip,
            int port,
            long untilTs,
            string reason)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = @"
INSERT INTO peer_quarantine(ip, port, until_ts, reason)
VALUES($ip, $p, $u, $r)
ON CONFLICT(ip, port) DO UPDATE SET
  until_ts = excluded.until_ts,
  reason = excluded.reason;";
            cmd.Parameters.AddWithValue("$ip", ip);
            cmd.Parameters.AddWithValue("$p", port);
            cmd.Parameters.AddWithValue("$u", untilTs);
            cmd.Parameters.AddWithValue("$r", reason);
            cmd.ExecuteNonQuery();
        }

        private static void ClearEndpointQuarantineCore(SqliteConnection conn, SqliteTransaction? tx, string ip, int port)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "DELETE FROM peer_quarantine WHERE ip = $ip AND port = $p;";
            cmd.Parameters.AddWithValue("$ip", ip);
            cmd.Parameters.AddWithValue("$p", port);
            cmd.ExecuteNonQuery();
        }

        private static void UpsertAnnouncedAtCore(SqliteConnection conn, SqliteTransaction? tx, byte[] id, long nowTs)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = @"
INSERT INTO peer_announced(id, announced_at)
VALUES($id, $ts)
ON CONFLICT(id) DO UPDATE SET announced_at = excluded.announced_at;";
            cmd.Parameters.AddWithValue("$id", id);
            cmd.Parameters.AddWithValue("$ts", nowTs);
            cmd.ExecuteNonQuery();
        }

        private static void DeleteAnnouncedAtCore(SqliteConnection conn, SqliteTransaction? tx, byte[] id)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "DELETE FROM peer_announced WHERE id = $id;";
            cmd.Parameters.AddWithValue("$id", id);
            cmd.ExecuteNonQuery();
        }

        private static void CleanupAnnouncedOrphansCore(SqliteConnection conn, SqliteTransaction? tx)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "DELETE FROM peer_announced WHERE id NOT IN (SELECT id FROM peers);";
            cmd.ExecuteNonQuery();
        }

        private static long ComputeQuarantineUntil(long nowTs)
        {
            ulong ttl = PeerTtlSeconds;
            if (ttl > (ulong)long.MaxValue)
                return long.MaxValue;

            long ttlI64 = (long)ttl;
            if (nowTs > long.MaxValue - ttlI64)
                return long.MaxValue;

            return nowTs + ttlI64;
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

