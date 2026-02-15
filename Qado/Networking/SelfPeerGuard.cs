using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;

namespace Qado.Networking
{
    public static class SelfPeerGuard
    {
        private static readonly object Sync = new();
        private static readonly HashSet<string> SelfIps = new(StringComparer.OrdinalIgnoreCase);

        private static bool _initialized;
        private static int _listenPort = GenesisConfig.P2PPort;

        public static void InitializeAtStartup(int listenPort)
        {
            lock (Sync)
            {
                if (_initialized) return;

                _listenPort = listenPort > 0 ? listenPort : GenesisConfig.P2PPort;
                SeedLocalIps_NoThrow();
                SeedPublicIp_NoThrow();
                _initialized = true;
            }
        }

        public static bool IsSelf(string? hostOrIp, int port)
        {
            if (string.IsNullOrWhiteSpace(hostOrIp)) return false;
            EnsureInitialized();

            if (port > 0 && _listenPort > 0 && port != _listenPort)
                return false;

            string normalized = NormalizeHost(hostOrIp);
            if (normalized.Length == 0) return false;

            return SelfIps.Contains(normalized);
        }

        private static void EnsureInitialized()
        {
            if (_initialized) return;
            InitializeAtStartup(GenesisConfig.P2PPort);
        }

        private static void SeedLocalIps_NoThrow()
        {
            try
            {
                SelfIps.Add("localhost");
                SelfIps.Add("127.0.0.1");
                SelfIps.Add("::1");

                foreach (var addr in Dns.GetHostAddresses(Dns.GetHostName()))
                {
                    string normalized = NormalizeHost(addr.ToString());
                    if (normalized.Length > 0)
                        SelfIps.Add(normalized);
                }
            }
            catch
            {
            }
        }

        private static void SeedPublicIp_NoThrow()
        {
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(6));
                string? publicIp = IpHelper.GetPublicIpAsync(cts.Token).GetAwaiter().GetResult();
                string normalized = NormalizeHost(publicIp);
                if (normalized.Length > 0)
                    SelfIps.Add(normalized);
            }
            catch
            {
            }
        }

        private static string NormalizeHost(string? value)
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
    }
}
