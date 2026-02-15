using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Qado.Logging;

namespace Qado.Networking
{
    public static class GenesisDetector
    {
        public static async Task<bool> IsSelfGenesisAsync(ILogSink? log = null, CancellationToken ct = default)
        {
            try
            {
                var myIp = await IpHelper.GetPublicIpAsync(ct).ConfigureAwait(false);
                if (string.IsNullOrWhiteSpace(myIp))
                {
                    log?.Warn("Genesis", "Could not determine own public IP.");
                    return false;
                }

#if NET8_0_OR_GREATER
                IPAddress[] records = await Dns.GetHostAddressesAsync(GenesisConfig.GenesisHost, ct).ConfigureAwait(false);
#else
                IPAddress[] records = await Dns.GetHostAddressesAsync(GenesisConfig.GenesisHost).ConfigureAwait(false);
#endif
                var v4 = records
                    .Where(a => a.AddressFamily == AddressFamily.InterNetwork)
                    .Select(a => a.ToString())
                    .ToArray();

                bool match = v4.Any(ip => string.Equals(ip, myIp, StringComparison.OrdinalIgnoreCase));

                log?.Info("Genesis", match
                    ? $"This instance IS the genesis node (self={myIp})."
                    : $"This instance is NOT the genesis node (self={myIp}, seedV4={string.Join(",", v4)})");

                return match;
            }
            catch (Exception ex)
            {
                log?.Warn("Genesis", $"Self-check failed: {ex.Message}");
                return false;
            }
        }

        public static async Task<(string ip, int port)[]> GetGenesisEndpointsAsync(ILogSink? log = null, CancellationToken ct = default)
        {
            try
            {
#if NET8_0_OR_GREATER
                IPAddress[] records = await Dns.GetHostAddressesAsync(GenesisConfig.GenesisHost, ct).ConfigureAwait(false);
#else
                IPAddress[] records = await Dns.GetHostAddressesAsync(GenesisConfig.GenesisHost).ConfigureAwait(false);
#endif
                var list = records
                    .Where(a => a.AddressFamily == AddressFamily.InterNetwork) // IPv4 only
                    .Select(a => (a.ToString(), GenesisConfig.P2PPort))
                    .ToArray();

                if (list.Length == 0)
                    log?.Warn("Genesis", "No IPv4 A-records for genesis host found.");

                return list;
            }
            catch (Exception ex)
            {
                log?.Warn("Genesis", $"DNS resolution for {GenesisConfig.GenesisHost} failed: {ex.Message}");
                return Array.Empty<(string ip, int port)>();
            }
        }
    }
}

