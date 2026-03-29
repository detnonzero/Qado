using System;
using System.Collections.Generic;

namespace Qado.Networking
{
    public enum PeerDialClass
    {
        Unverified = 0,
        RecentlySeen = 1,
        Stale = 2
    }

    public static class PeerDialPolicy
    {
        // Mainnet-safe defaults:
        // - unverified: one attempt, then long cooldown
        // - recently seen: exponential backoff
        // - stale: very sparse retries
        private static readonly TimeSpan RecentSeenWindow = TimeSpan.FromHours(3);
        private static readonly TimeSpan UnverifiedCooldown = TimeSpan.FromHours(8);
        private static readonly TimeSpan StaleCooldown = TimeSpan.FromHours(12);
        private static readonly TimeSpan RecentBackoffBase = TimeSpan.FromMinutes(1);
        private static readonly TimeSpan RecentBackoffCap = TimeSpan.FromMinutes(30);
        private const int RecentBackoffMaxExp = 8;

        private static readonly TimeSpan StateRetention = TimeSpan.FromDays(3);
        private static readonly TimeSpan PruneInterval = TimeSpan.FromMinutes(5);
        private const int MaxStateEntries = 8192;

        private static readonly object Gate = new();
        private static readonly Dictionary<string, DialState> StateByPeer = new(StringComparer.Ordinal);
        private static DateTime _lastPruneUtc = DateTime.MinValue;

        private sealed class DialState
        {
            public DateTime NextUnverifiedUtc = DateTime.MinValue;
            public DateTime NextRecentUtc = DateTime.MinValue;
            public DateTime NextStaleUtc = DateTime.MinValue;
            public int RecentFailureStreak;
            public DateTime LastTouchedUtc = DateTime.UtcNow;
        }

        public static bool ShouldAttempt(
            string host,
            int port,
            ulong lastSeenUnix,
            out PeerDialClass dialClass,
            out TimeSpan retryIn)
        {
            dialClass = Classify(lastSeenUnix, DateTime.UtcNow);
            retryIn = TimeSpan.Zero;

            string key = BuildKey(host, port);
            if (key.Length == 0)
                return false;

            lock (Gate)
            {
                var now = DateTime.UtcNow;
                TryPrune_NoThrow(now);

                if (!StateByPeer.TryGetValue(key, out var state))
                    return true;

                state.LastTouchedUtc = now;

                DateTime nextAllowedUtc = dialClass switch
                {
                    PeerDialClass.Unverified => state.NextUnverifiedUtc,
                    PeerDialClass.RecentlySeen => state.NextRecentUtc,
                    _ => state.NextStaleUtc
                };

                if (nextAllowedUtc <= now)
                    return true;

                retryIn = nextAllowedUtc - now;
                return false;
            }
        }

        public static void ReportFailure(string host, int port, PeerDialClass dialClass)
        {
            string key = BuildKey(host, port);
            if (key.Length == 0)
                return;

            lock (Gate)
            {
                var now = DateTime.UtcNow;
                if (!StateByPeer.TryGetValue(key, out var state))
                {
                    state = new DialState();
                    StateByPeer[key] = state;
                }

                switch (dialClass)
                {
                    case PeerDialClass.Unverified:
                        state.NextUnverifiedUtc = now + UnverifiedCooldown;
                        break;

                    case PeerDialClass.Stale:
                        state.NextStaleUtc = now + StaleCooldown;
                        break;

                    default:
                        state.RecentFailureStreak = Math.Min(state.RecentFailureStreak + 1, RecentBackoffMaxExp + 8);
                        state.NextRecentUtc = now + ComputeRecentBackoff(state.RecentFailureStreak);
                        break;
                }

                state.LastTouchedUtc = now;
                TryPrune_NoThrow(now);
            }
        }

        public static void ReportSuccess(string host, int port)
        {
            string key = BuildKey(host, port);
            if (key.Length == 0)
                return;

            lock (Gate)
            {
                StateByPeer.Remove(key);
            }
        }

        private static TimeSpan ComputeRecentBackoff(int failureStreak)
        {
            int exp = Math.Clamp(failureStreak - 1, 0, RecentBackoffMaxExp);
            long factor = 1L << exp;
            long ticks = RecentBackoffBase.Ticks * factor;
            if (ticks < 0 || ticks > RecentBackoffCap.Ticks)
                ticks = RecentBackoffCap.Ticks;
            return new TimeSpan(ticks);
        }

        private static PeerDialClass Classify(ulong lastSeenUnix, DateTime nowUtc)
        {
            if (lastSeenUnix == 0)
                return PeerDialClass.Unverified;

            var seenUtc = FromUnixSecondsSafe(lastSeenUnix);
            if (seenUtc == DateTime.MinValue)
                return PeerDialClass.Unverified;

            var age = nowUtc - seenUtc;
            return age <= RecentSeenWindow
                ? PeerDialClass.RecentlySeen
                : PeerDialClass.Stale;
        }

        private static DateTime FromUnixSecondsSafe(ulong value)
        {
            if (value == 0)
                return DateTime.MinValue;

            long ts = value > (ulong)long.MaxValue ? long.MaxValue : (long)value;

            try
            {
                return DateTimeOffset.FromUnixTimeSeconds(ts).UtcDateTime;
            }
            catch
            {
                return DateTime.MinValue;
            }
        }

        private static string BuildKey(string host, int port)
        {
            if (string.IsNullOrWhiteSpace(host))
                return string.Empty;

            int p = port > 0 && port <= 65535 ? port : P2PNode.DefaultPort;
            if (p <= 0 || p > 65535)
                return string.Empty;

            string h = NormalizeHost(host);
            if (h.Length == 0)
                return string.Empty;

            return $"{h}:{p}";
        }

        private static string NormalizeHost(string host)
            => PeerAddress.NormalizeHost(host);

        private static void TryPrune_NoThrow(DateTime nowUtc)
        {
            try
            {
                if ((nowUtc - _lastPruneUtc) < PruneInterval && StateByPeer.Count <= MaxStateEntries)
                    return;

                _lastPruneUtc = nowUtc;
                if (StateByPeer.Count == 0)
                    return;

                var cutoff = nowUtc - StateRetention;
                var stale = new List<string>();

                foreach (var kv in StateByPeer)
                {
                    var s = kv.Value;
                    bool fullyExpired =
                        s.LastTouchedUtc < cutoff &&
                        s.NextUnverifiedUtc <= nowUtc &&
                        s.NextRecentUtc <= nowUtc &&
                        s.NextStaleUtc <= nowUtc;

                    if (fullyExpired)
                        stale.Add(kv.Key);
                }

                for (int i = 0; i < stale.Count; i++)
                    StateByPeer.Remove(stale[i]);

                if (StateByPeer.Count <= MaxStateEntries)
                    return;

                var ordered = new List<KeyValuePair<string, DialState>>(StateByPeer);
                ordered.Sort((a, b) => a.Value.LastTouchedUtc.CompareTo(b.Value.LastTouchedUtc));
                int overflow = StateByPeer.Count - MaxStateEntries;

                for (int i = 0; i < overflow && i < ordered.Count; i++)
                    StateByPeer.Remove(ordered[i].Key);
            }
            catch
            {
            }
        }
    }
}
