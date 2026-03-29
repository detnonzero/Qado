using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;

namespace Qado.Networking
{
    internal static class PeerAddress
    {
        public static string NormalizeHost(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return string.Empty;

            string s = value.Trim();
            if (TrySplitHostPort(s, out var host, out _))
                s = host;

            int zone = s.IndexOf('%');
            if (zone > 0)
                s = s[..zone];

            s = s.Trim().ToLowerInvariant();
            if (s.StartsWith("[", StringComparison.Ordinal) && s.EndsWith("]", StringComparison.Ordinal) && s.Length > 2)
                s = s[1..^1];

            if (!IPAddress.TryParse(s, out var addr))
                return s;

            if (addr.IsIPv4MappedToIPv6)
                addr = addr.MapToIPv4();

            return addr.ToString().ToLowerInvariant();
        }

        public static bool TryParseIp(string? value, out IPAddress address)
        {
            string host = NormalizeHost(value);
            if (!IPAddress.TryParse(host, out address!))
                return false;

            if (address.IsIPv4MappedToIPv6)
                address = address.MapToIPv4();

            return true;
        }

        public static bool TrySplitHostPort(string? endpoint, out string host, out int port)
        {
            host = string.Empty;
            port = 0;

            if (string.IsNullOrWhiteSpace(endpoint))
                return false;

            string s = endpoint.Trim();
            if (s.StartsWith("[", StringComparison.Ordinal))
            {
                int close = s.IndexOf(']');
                if (close <= 1)
                    return false;

                host = s.Substring(1, close - 1);
                if (close + 1 >= s.Length || s[close + 1] != ':')
                    return true;

                if (!int.TryParse(s[(close + 2)..], out port))
                    return false;

                return port > 0 && port <= 65535;
            }

            int firstColon = s.IndexOf(':');
            int lastColon = s.LastIndexOf(':');
            if (firstColon > 0 && firstColon == lastColon)
            {
                host = s[..lastColon];
                if (!int.TryParse(s[(lastColon + 1)..], out port))
                    return false;

                return port > 0 && port <= 65535;
            }

            host = s;
            return true;
        }

        public static bool TryBuildEndpointKey(string? hostOrIp, int port, out string key)
        {
            key = string.Empty;
            if (port <= 0 || port > 65535)
                return false;

            string host = NormalizeHost(hostOrIp);
            if (host.Length == 0)
                return false;

            key = $"{host}|{port}";
            return true;
        }

        public static bool TryParseEndpointKey(string? key, out string host, out int port)
        {
            host = string.Empty;
            port = 0;

            if (string.IsNullOrWhiteSpace(key))
                return false;

            int sep = key.LastIndexOf('|');
            if (sep <= 0 || sep >= key.Length - 1)
                return false;

            host = NormalizeHost(key[..sep]);
            if (host.Length == 0)
                return false;

            if (!int.TryParse(key[(sep + 1)..], out port))
                return false;

            return port > 0 && port <= 65535;
        }

        public static bool IsPublicRoutable(string? hostOrIp)
            => TryParseIp(hostOrIp, out var address) && IsPublicRoutable(address);

        public static bool IsPublicRoutableIPv4(string? hostOrIp)
            => TryParseIp(hostOrIp, out var address) &&
               address.AddressFamily == AddressFamily.InterNetwork &&
               IsPublicRoutable(address);

        public static bool IsPublicRoutableIPv6(string? hostOrIp)
            => TryParseIp(hostOrIp, out var address) &&
               address.AddressFamily == AddressFamily.InterNetworkV6 &&
               IsPublicRoutable(address);

        public static bool IsPublicRoutable(IPAddress address)
        {
            if (address == null)
                return false;

            if (address.IsIPv4MappedToIPv6)
                address = address.MapToIPv4();

            if (IPAddress.Any.Equals(address) ||
                IPAddress.None.Equals(address) ||
                IPAddress.IPv6Any.Equals(address) ||
                IPAddress.IPv6None.Equals(address) ||
                IPAddress.IsLoopback(address))
            {
                return false;
            }

            if (address.AddressFamily == AddressFamily.InterNetwork)
                return IsPublicRoutableIPv4(address);

            if (address.AddressFamily != AddressFamily.InterNetworkV6)
                return false;

            if (address.IsIPv6LinkLocal || address.IsIPv6Multicast || address.IsIPv6SiteLocal || address.Equals(IPAddress.IPv6Loopback))
                return false;

            var bytes = address.GetAddressBytes();
            if (bytes.Length != 16)
                return false;

            if ((bytes[0] & 0xFE) == 0xFC)
                return false;

            if (IsAllZero(bytes))
                return false;

            return true;
        }

        public static IReadOnlyList<IPAddress> OrderDialAddresses(IEnumerable<IPAddress> addresses)
        {
            var ipv4 = new List<IPAddress>();
            var ipv6 = new List<IPAddress>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var raw in addresses)
            {
                if (raw == null)
                    continue;

                var address = raw.IsIPv4MappedToIPv6 ? raw.MapToIPv4() : raw;
                string key = address.ToString();
                if (!seen.Add(key))
                    continue;

                if (address.AddressFamily == AddressFamily.InterNetworkV6)
                    ipv6.Add(address);
                else if (address.AddressFamily == AddressFamily.InterNetwork)
                    ipv4.Add(address);
            }

            bool preferIpv6 = ipv6.Count > 0 && (ipv4.Count == 0 || Random.Shared.Next(2) == 0);
            var ordered = new List<IPAddress>(ipv4.Count + ipv6.Count);
            Interleave(preferIpv6 ? ipv6 : ipv4, preferIpv6 ? ipv4 : ipv6, ordered);
            return ordered;
        }

        private static void Interleave(List<IPAddress> primary, List<IPAddress> secondary, List<IPAddress> target)
        {
            int max = Math.Max(primary.Count, secondary.Count);
            for (int i = 0; i < max; i++)
            {
                if (i < primary.Count)
                    target.Add(primary[i]);
                if (i < secondary.Count)
                    target.Add(secondary[i]);
            }
        }

        private static bool IsPublicRoutableIPv4(IPAddress address)
        {
            var bytes = address.GetAddressBytes();
            if (bytes.Length != 4)
                return false;

            if (bytes[0] == 0) return false;
            if (bytes[0] == 10) return false;
            if (bytes[0] == 100 && bytes[1] >= 64 && bytes[1] <= 127) return false;
            if (bytes[0] == 127) return false;
            if (bytes[0] == 169 && bytes[1] == 254) return false;
            if (bytes[0] == 172 && bytes[1] >= 16 && bytes[1] <= 31) return false;
            if (bytes[0] == 192 && bytes[1] == 168) return false;
            if (bytes[0] == 198 && (bytes[1] == 18 || bytes[1] == 19)) return false;
            if (bytes[0] >= 224) return false;

            return true;
        }

        private static bool IsAllZero(byte[] bytes)
        {
            for (int i = 0; i < bytes.Length; i++)
            {
                if (bytes[i] != 0)
                    return false;
            }

            return true;
        }
    }
}
