using System;
using System.Net;
using System.Net.Sockets;

namespace Qado.Networking
{
    internal static class EndpointLogFormatter
    {
        public static string FormatHostPort(string? hostOrIp, int port)
        {
            string host = FormatHost(hostOrIp);
            if (port > 0 && port <= 65535)
                return $"{host}:{port}";

            return host;
        }

        public static string FormatHost(string? hostOrIp)
        {
            string host = NormalizeHost(hostOrIp);
            if (host.Length == 0)
                return "<unknown>";

            if (!IPAddress.TryParse(host, out var addr))
                return host;

            if (addr.AddressFamily != AddressFamily.InterNetwork)
                return "<non-ipv4>";

            return IsPublicRoutableIPv4(addr) ? addr.ToString() : "<non-public-ipv4>";
        }

        public static string FormatEndpoint(string? endpoint)
        {
            if (string.IsNullOrWhiteSpace(endpoint))
                return "<unknown-endpoint>";

            if (!TrySplitHostPort(endpoint, out var host, out var port))
                return FormatHost(endpoint);

            return FormatHostPort(host, port);
        }

        private static bool TrySplitHostPort(string endpoint, out string host, out int port)
        {
            host = endpoint ?? string.Empty;
            port = 0;

            if (string.IsNullOrWhiteSpace(endpoint))
                return false;

            string s = endpoint.Trim();

            if (s.StartsWith("[", StringComparison.Ordinal))
            {
                int close = s.IndexOf(']');
                if (close > 1)
                {
                    host = s.Substring(1, close - 1);
                    if (close + 2 <= s.Length && s[close + 1] == ':' &&
                        int.TryParse(s[(close + 2)..], out int p) &&
                        p > 0 && p <= 65535)
                    {
                        port = p;
                    }
                    return true;
                }
            }

            int firstColon = s.IndexOf(':');
            int lastColon = s.LastIndexOf(':');
            if (firstColon > 0 && firstColon == lastColon)
            {
                string possiblePort = s[(lastColon + 1)..];
                if (int.TryParse(possiblePort, out int p) && p > 0 && p <= 65535)
                {
                    host = s[..lastColon];
                    port = p;
                    return true;
                }
            }

            host = s;
            return false;
        }

        private static string NormalizeHost(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return string.Empty;

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

        private static bool IsPublicRoutableIPv4(IPAddress addr)
        {
            var b = addr.GetAddressBytes();
            if (b.Length != 4) return false;

            if (b[0] == 0) return false;
            if (b[0] == 10) return false;
            if (b[0] == 100 && b[1] >= 64 && b[1] <= 127) return false;
            if (b[0] == 127) return false;
            if (b[0] == 169 && b[1] == 254) return false;
            if (b[0] == 172 && b[1] >= 16 && b[1] <= 31) return false;
            if (b[0] == 192 && b[1] == 168) return false;
            if (b[0] == 198 && (b[1] == 18 || b[1] == 19)) return false;
            if (b[0] >= 224) return false;

            return true;
        }
    }
}
