using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Qado.Logging;
using Qado.Storage;

namespace Qado.Networking
{
    public static class PeerDiscovery
    {
        private const int MaxPeersInPayload = 256;        // hard cap for remote payload parsing
        private const int MaxIpStringBytes = 64;          // upper bound for IPv4 literals and malformed inputs
        private const int DefaultP2PPort = P2PNode.DefaultPort;
        private static readonly object PexLogGate = new();
        private static readonly TimeSpan PexLogWindow = TimeSpan.FromSeconds(10);
        private static DateTime _lastPexSentLogUtc = DateTime.MinValue;
        private static DateTime _lastPexReceivedLogUtc = DateTime.MinValue;
        private static int _pexSentSuppressed;
        private static int _pexReceivedSuppressed;

        public static async Task HandleGetPeersAsync(NetworkStream ns, ILogSink? log, CancellationToken ct)
        {
            var payload = BuildPeersPayload(maxPeers: 64);
            await P2PNode.WriteFrame(ns, MsgType.Peers, payload, ct).ConfigureAwait(false);
            LogPexSent(log, GetCountFromPayload(payload));
        }

        public static void HandlePeersPayload(byte[] payload, ILogSink? log)
        {
            if (payload == null || payload.Length < 2) return;

            try
            {
                int idx = 0;
                ushort declared = BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(idx, 2));
                idx += 2;

                int count = Math.Min(declared, (ushort)MaxPeersInPayload);

                int added = 0;

                for (int i = 0; i < count; i++)
                {
                    if (idx >= payload.Length) break;

                    byte ipLen = payload[idx++];
                    if (ipLen == 0 || ipLen > MaxIpStringBytes) { if (!Skip(payload, ref idx, ipLen + 2)) break; continue; }

                    if (idx + ipLen + 2 > payload.Length) break;

                    string ip = Encoding.UTF8.GetString(payload, idx, ipLen);
                    idx += ipLen;

                    ushort portU16 = BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(idx, 2));
                    idx += 2;

                    int port = portU16;
                    if (port <= 0) port = DefaultP2PPort;

                    if (!IsPublicRoutableIPv4Literal(ip)) continue;
                    if (port is < 1 or > 65535) continue;
                    if (SelfPeerGuard.IsSelf(ip, port)) continue;
                    if (IsConfiguredSeed(ip, port)) continue;

                    try { P2PNode.Instance?.NotePeerPublicClaim(ip, port); } catch { }

                    try
                    {
                        if (PeerStore.AnnouncePeer(ip, port, GenesisConfig.NetworkId))
                            added++;
                    }
                    catch
                    {
                    }
                }

                if (added > 0)
                    LogPexReceived(log, added);
            }
            catch (Exception ex)
            {
                log?.Warn("PEX", $"Failed to parse Peers payload: {ex.Message}");
            }
        }

        public static byte[] BuildPeersPayload(int maxPeers = 64)
        {
            if (maxPeers <= 0) return MakeEmpty();

            var peers = P2PNode.Instance?.GetPeerCandidatesForPex(maxPeers, unverifiedPercent: 20)
                        ?? new List<(string ip, int port)>();
            var chunks = new List<byte[]>(peers.Count);

            ushort count = 0;
            foreach (var (ip, port) in peers)
            {
                if (!IsPublicRoutableIPv4Literal(ip)) continue;

                int p = port;
                if (p <= 0) p = DefaultP2PPort;
                if (p is < 1 or > 65535) continue;
                if (SelfPeerGuard.IsSelf(ip, p)) continue;
                if (IsConfiguredSeed(ip, p)) continue;

                var ipBytes = Encoding.UTF8.GetBytes(ip);
                if (ipBytes.Length == 0 || ipBytes.Length > byte.MaxValue) continue;

                var buf = new byte[1 + ipBytes.Length + 2];
                buf[0] = (byte)ipBytes.Length;
                Buffer.BlockCopy(ipBytes, 0, buf, 1, ipBytes.Length);
                BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(1 + ipBytes.Length, 2), (ushort)p);

                chunks.Add(buf);
                count++;
            }

            if (count == 0) return MakeEmpty();

            int len = 2;
            for (int i = 0; i < chunks.Count; i++) len += chunks[i].Length;

            var payload = new byte[len];
            BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(0, 2), count);

            int off = 2;
            for (int i = 0; i < chunks.Count; i++)
            {
                var c = chunks[i];
                Buffer.BlockCopy(c, 0, payload, off, c.Length);
                off += c.Length;
            }

            return payload;
        }


        private static byte[] MakeEmpty()
        {
            var b = new byte[2];
            BinaryPrimitives.WriteUInt16LittleEndian(b, 0);
            return b;
        }

        private static ushort GetCountFromPayload(byte[] payload)
        {
            if (payload == null || payload.Length < 2) return 0;
            return BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(0, 2));
        }

        private static bool IsPublicRoutableIPv4Literal(string ip)
        {
            if (string.IsNullOrWhiteSpace(ip)) return false;
            if (!IPAddress.TryParse(ip, out var a)) return false;
            if (a.AddressFamily != AddressFamily.InterNetwork) return false;

            var b = a.GetAddressBytes();
            if (b.Length != 4) return false;

            // Exclude non-routable and special-purpose ranges.
            if (b[0] == 0) return false; // 0.0.0.0/8
            if (b[0] == 10) return false; // RFC1918
            if (b[0] == 100 && b[1] >= 64 && b[1] <= 127) return false; // CGNAT 100.64/10
            if (b[0] == 127) return false; // loopback
            if (b[0] == 169 && b[1] == 254) return false; // link-local
            if (b[0] == 172 && b[1] >= 16 && b[1] <= 31) return false; // RFC1918
            if (b[0] == 192 && b[1] == 168) return false; // RFC1918
            if (b[0] == 198 && (b[1] == 18 || b[1] == 19)) return false; // benchmark net
            if (b[0] >= 224) return false; // multicast + reserved (incl. broadcast)

            return true;
        }

        private static bool IsConfiguredSeed(string ip, int port)
        {
            if (port != GenesisConfig.P2PPort) return false;

            var seed = NormalizeHost(GenesisConfig.GenesisHost);
            if (seed.Length == 0) return false;

            return string.Equals(NormalizeHost(ip), seed, StringComparison.OrdinalIgnoreCase);
        }

        private static string NormalizeHost(string value)
        {
            if (string.IsNullOrWhiteSpace(value)) return string.Empty;
            var s = value.Trim().ToLowerInvariant();
            return s.StartsWith("::ffff:", StringComparison.Ordinal) ? s[7..] : s;
        }

        private static bool Skip(byte[] payload, ref int idx, int bytes)
        {
            long next = (long)idx + bytes;
            if (next > payload.Length) return false;
            idx = (int)next;
            return true;
        }

        private static void LogPexSent(ILogSink? log, int count)
        {
            LogPex(log, count, isSent: true);
        }

        private static void LogPexReceived(ILogSink? log, int count)
        {
            LogPex(log, count, isSent: false);
        }

        private static void LogPex(ILogSink? log, int count, bool isSent)
        {
            lock (PexLogGate)
            {
                var now = DateTime.UtcNow;

                if (isSent)
                {
                    if ((now - _lastPexSentLogUtc) >= PexLogWindow)
                    {
                        if (_pexSentSuppressed > 0)
                            log?.Info("PEX", $"Sent {count} peers. (+{_pexSentSuppressed} events suppressed)");
                        else
                            log?.Info("PEX", $"Sent {count} peers.");

                        _lastPexSentLogUtc = now;
                        _pexSentSuppressed = 0;
                    }
                    else
                    {
                        _pexSentSuppressed++;
                    }

                    return;
                }

                if ((now - _lastPexReceivedLogUtc) >= PexLogWindow)
                {
                    if (_pexReceivedSuppressed > 0)
                        log?.Info("PEX", $"Received {count} peer(s). (+{_pexReceivedSuppressed} events suppressed)");
                    else
                        log?.Info("PEX", $"Received {count} peer(s).");

                    _lastPexReceivedLogUtc = now;
                    _pexReceivedSuppressed = 0;
                }
                else
                {
                    _pexReceivedSuppressed++;
                }
            }
        }
    }
}

