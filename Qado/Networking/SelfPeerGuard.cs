using System;
using System.Collections.Generic;
using System.Net;

namespace Qado.Networking
{
    public static class SelfPeerGuard
    {
        private static readonly object Sync = new();
        private static readonly HashSet<string> SelfHosts = new(StringComparer.OrdinalIgnoreCase);
        private static readonly HashSet<string> VerifiedSelfEndpoints = new(StringComparer.OrdinalIgnoreCase);

        private static bool _initialized;
        private static int _listenPort = GenesisConfig.P2PPort;

        public static void InitializeAtStartup(int listenPort)
        {
            lock (Sync)
            {
                if (_initialized) return;

                _listenPort = listenPort > 0 ? listenPort : GenesisConfig.P2PPort;
                SeedLocalHosts_NoThrow();
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

            lock (Sync)
            {
                if (SelfHosts.Contains(normalized))
                    return true;

                return TryBuildEndpointKey(normalized, port, out var endpointKey) &&
                    VerifiedSelfEndpoints.Contains(endpointKey);
            }
        }

        public static void RememberSelf(string? hostOrIp, int port)
        {
            if (string.IsNullOrWhiteSpace(hostOrIp)) return;
            EnsureInitialized();

            if (port > 0 && _listenPort > 0 && port != _listenPort)
                return;

            string normalized = NormalizeHost(hostOrIp);
            if (normalized.Length == 0) return;
            if (!TryBuildEndpointKey(normalized, port, out var endpointKey))
                return;

            lock (Sync)
            {
                VerifiedSelfEndpoints.Add(endpointKey);
            }
        }

        private static void EnsureInitialized()
        {
            if (_initialized) return;
            InitializeAtStartup(GenesisConfig.P2PPort);
        }

        private static void SeedLocalHosts_NoThrow()
        {
            try
            {
                SelfHosts.Add("localhost");
                SelfHosts.Add("127.0.0.1");
                SelfHosts.Add("::1");

                string hostName = NormalizeHost(Dns.GetHostName());
                if (hostName.Length > 0)
                    SelfHosts.Add(hostName);

                foreach (var addr in Dns.GetHostAddresses(Dns.GetHostName()))
                {
                    string normalized = NormalizeHost(addr.ToString());
                    if (normalized.Length > 0)
                        SelfHosts.Add(normalized);
                }
            }
            catch
            {
            }
        }

        private static bool TryBuildEndpointKey(string normalizedHost, int port, out string key)
            => PeerAddress.TryBuildEndpointKey(normalizedHost, port, out key);

        private static string NormalizeHost(string? value)
            => PeerAddress.NormalizeHost(value);
    }
}
