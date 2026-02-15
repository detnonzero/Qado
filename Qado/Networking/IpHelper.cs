using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Qado.Networking
{
    public static class IpHelper
    {
        private static readonly HttpClient _http = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(6)
        };

        private static string? _cachedIp;
        private static long _cachedAtUnix;

        public static async Task<string?> GetPublicIpAsync(CancellationToken ct = default)
        {
            long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            if (!string.IsNullOrWhiteSpace(_cachedIp) && (now - _cachedAtUnix) < 600)
                return _cachedIp;

            try
            {
                using var req = new HttpRequestMessage(HttpMethod.Get, "https://api.ipify.org");
                req.Headers.UserAgent.ParseAdd("QadoNode/1.0");

#if NET8_0_OR_GREATER
                using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
                resp.EnsureSuccessStatusCode();
                var ip = (await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false)).Trim();
#else
                using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
                resp.EnsureSuccessStatusCode();
                var ip = (await resp.Content.ReadAsStringAsync().ConfigureAwait(false)).Trim();
#endif
                if (string.IsNullOrWhiteSpace(ip)) return null;

                _cachedIp = ip;
                _cachedAtUnix = now;
                return ip;
            }
            catch
            {
                return null;
            }
        }

        public static bool IsSelf(string url, string? publicIp)
        {
            if (string.IsNullOrWhiteSpace(url) || string.IsNullOrWhiteSpace(publicIp))
                return false;

            return url.Contains(publicIp, StringComparison.OrdinalIgnoreCase);
        }
    }
}

