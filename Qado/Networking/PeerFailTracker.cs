using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Qado.Networking
{
    public static class PeerFailTracker
    {
        private const int MaxFails = 3;
        private static readonly TimeSpan BanThresholdWindow = TimeSpan.FromMinutes(30);

        private static readonly ConcurrentDictionary<string, int> _failCounts = new(StringComparer.Ordinal);

        private static readonly ConcurrentDictionary<string, DateTime> _lastFailUtc = new(StringComparer.Ordinal);

        private static DateTime _lastPruneUtc = DateTime.MinValue;
        private static readonly TimeSpan PruneInterval = TimeSpan.FromMinutes(5);

        public static void ReportFailure(string address)
        {
            address = Normalize(address);
            if (address.Length == 0) return;

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
            return _failCounts.TryGetValue(address, out var n) ? n : 0;
        }

        private static string Normalize(string address)
        {
            if (string.IsNullOrWhiteSpace(address)) return string.Empty;
            return address.Trim().ToLowerInvariant();
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

