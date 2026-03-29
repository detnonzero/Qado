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
            {
                if (host.IndexOf(':') >= 0 && !host.StartsWith("[", StringComparison.Ordinal))
                    return $"[{host}]:{port}";

                return $"{host}:{port}";
            }

            return host;
        }

        public static string FormatHost(string? hostOrIp)
        {
            string host = NormalizeHost(hostOrIp);
            if (host.Length == 0)
                return "<unknown>";

            if (!IPAddress.TryParse(host, out var addr))
                return host;

            return PeerAddress.IsPublicRoutable(addr)
                ? addr.ToString().ToLowerInvariant()
                : "<non-public-ip>";
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
            => PeerAddress.NormalizeHost(value);
    }
}
