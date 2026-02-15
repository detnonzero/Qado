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

        public static async Task HandleGetPeersAsync(NetworkStream ns, ILogSink? log, CancellationToken ct)
        {
            var payload = BuildPeersPayload(maxPeers: 64);
            await P2PNode.WriteFrame(ns, MsgType.Peers, payload, ct).ConfigureAwait(false);
            log?.Info("PEX", $"Sent {GetCountFromPayload(payload)} peers.");
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

                ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
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

                    if (!IsValidIPv4Literal(ip)) continue;
                    if (port is < 1 or > 65535) continue;
                    if (SelfPeerGuard.IsSelf(ip, port)) continue;

                    try
                    {
                        PeerStore.MarkSeen(ip, port, now, GenesisConfig.NetworkId);
                        added++;
                    }
                    catch
                    {
                    }
                }

                if (added > 0)
                    log?.Info("PEX", $"Received {added} peer(s).");
            }
            catch (Exception ex)
            {
                log?.Warn("PEX", $"Failed to parse Peers payload: {ex.Message}");
            }
        }

        public static byte[] BuildPeersPayload(int maxPeers = 64)
        {
            if (maxPeers <= 0) return MakeEmpty();

            var peers = PeerStore.GetRecentPeers(maxPeers);
            var chunks = new List<byte[]>(peers.Count);

            ushort count = 0;
            foreach (var (ip, port, _) in peers)
            {
                if (!IsValidIPv4Literal(ip)) continue;

                int p = port;
                if (p <= 0) p = DefaultP2PPort;
                if (p is < 1 or > 65535) continue;

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

        private static bool IsValidIPv4Literal(string ip)
        {
            if (string.IsNullOrWhiteSpace(ip)) return false;
            if (!IPAddress.TryParse(ip, out var a)) return false;
            return a.AddressFamily == AddressFamily.InterNetwork;
        }

        private static bool Skip(byte[] payload, ref int idx, int bytes)
        {
            long next = (long)idx + bytes;
            if (next > payload.Length) return false;
            idx = (int)next;
            return true;
        }
    }
}

