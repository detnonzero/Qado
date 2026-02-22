using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Qado.Networking
{
    public static class PeerFailTracker
    {
        private const int MaxFails = 3;
        private static readonly TimeSpan BanThresholdWindow = TimeSpan.FromMinutes(30);
        private static readonly string SeedNeverBanKey = Normalize(GenesisConfig.GenesisHost);

        private static readonly ConcurrentDictionary<string, int> _failCounts = new(StringComparer.Ordinal);

        private static readonly ConcurrentDictionary<string, DateTime> _lastFailUtc = new(StringComparer.Ordinal);

        private static DateTime _lastPruneUtc = DateTime.MinValue;
        private static readonly TimeSpan PruneInterval = TimeSpan.FromMinutes(5);

        public static void ReportFailure(string address)
        {
            address = Normalize(address);
            if (address.Length == 0) return;
            if (IsNeverBan(address))
            {
                Reset(address);
                return;
            }

            _failCounts.AddOrUpdate(address, 1, (_, n) => n + 1);
            _lastFailUtc[address] = DateTime.UtcNow;

            TryPruneExpired_NoThrow();
        }

        public static void ReportSuccess(string address)
        {
            Reset(address);
        }

        public static void Reset(string address)
        {
            address = Normalize(address);
            if (address.Length == 0) return;

            _failCounts.TryRemove(address, out _);
            _lastFailUtc.TryRemove(address, out _);
        }

        public static bool ShouldBan(string address)
        {
            address = Normalize(address);
            if (address.Length == 0) return false;
            if (IsNeverBan(address))
            {
                Reset(address);
                return false;
            }

            if (_failCounts.TryGetValue(address, out int count) &&
                _lastFailUtc.TryGetValue(address, out DateTime last) &&
                (DateTime.UtcNow - last) < BanThresholdWindow)
            {
                return count >= MaxFails;
            }

            Reset(address);
            return false;
        }

        public static int GetFailCount(string address)
        {
            address = Normalize(address);
            if (address.Length == 0) return 0;
            if (IsNeverBan(address)) return 0;
            return _failCounts.TryGetValue(address, out var n) ? n : 0;
        }

        private static string Normalize(string address)
        {
            if (string.IsNullOrWhiteSpace(address)) return string.Empty;
            string s = address.Trim().ToLowerInvariant();

            // Remove optional [host]:port brackets.
            if (s.StartsWith("[", StringComparison.Ordinal))
            {
                int close = s.IndexOf(']');
                if (close > 1)
                    s = s.Substring(1, close - 1);
            }
            else
            {
                // Remove optional :port for IPv4/host literals.
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

        private static bool IsNeverBan(string address)
        {
            if (SeedNeverBanKey.Length == 0) return false;
            return string.Equals(address, SeedNeverBanKey, StringComparison.Ordinal);
        }

        private static void TryPruneExpired_NoThrow()
        {
            try
            {
                var now = DateTime.UtcNow;
                if ((now - _lastPruneUtc) < PruneInterval) return;
                _lastPruneUtc = now;

                var keys = new List<string>(_lastFailUtc.Keys);

                foreach (var k in keys)
                {
                    if (_lastFailUtc.TryGetValue(k, out var last))
                    {
                        if ((now - last) >= BanThresholdWindow)
                            Reset(k);
                    }
                    else
                    {
                        _failCounts.TryRemove(k, out _);
                    }
                }
            }
            catch
            {
            }
        }
    }
}

