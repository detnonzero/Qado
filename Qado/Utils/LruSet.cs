using System;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace Qado.Utils
{
    public sealed class LruSet
    {
        private readonly int _capacity;
        private readonly TimeSpan _ttl;
        private readonly ConcurrentDictionary<string, long> _seen = new(); // key -> last-seen unix seconds
        private readonly ConcurrentQueue<string> _fifo = new();

        public LruSet(int capacity, TimeSpan ttl)
        {
            if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity));
            _capacity = capacity;
            _ttl = ttl <= TimeSpan.Zero ? TimeSpan.FromMinutes(10) : ttl;
        }

        public bool Seen(string key)
        {
            PurgeExpired();
            return _seen.ContainsKey(key);
        }

        public bool TryAdd(string key)
        {
            PurgeExpired();

            long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            if (_seen.TryGetValue(key, out var ts))
            {
                if (now - ts <= (long)_ttl.TotalSeconds) return false;
                _seen[key] = now;
                _fifo.Enqueue(key);
                return true;
            }

            _seen[key] = now;
            _fifo.Enqueue(key);
            EvictOverflow();
            return true;
        }

        public bool TryAdd(byte[] bytes) => TryAdd(Convert.ToHexString(bytes).ToLowerInvariant());

        public bool TryAddHashOf(byte[] data)
        {
            var h = SHA256.HashData(data);
            return TryAdd(h);
        }

        private void PurgeExpired()
        {
            long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            foreach (var kv in _seen)
            {
                if (now - kv.Value > (long)_ttl.TotalSeconds)
                    _seen.TryRemove(kv.Key, out _);
            }
        }

        private void EvictOverflow()
        {
            while (_seen.Count > _capacity && _fifo.TryDequeue(out var oldKey))
            {
                _seen.TryRemove(oldKey, out _);
            }
        }
    }
}

