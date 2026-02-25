using System;
using System.Collections.Generic;
using System.Linq;
using Qado.Blockchain;

namespace Qado.Mempool
{
    public sealed class PendingTransactionBuffer
    {
        private readonly object _sync = new();

        private readonly Dictionary<string, SortedList<ulong, Transaction>> _buffer = new(StringComparer.Ordinal);

        private readonly HashSet<string> _lockedSenders = new(StringComparer.Ordinal);

        private readonly Func<string, ulong> _getConfirmedNonce;
        private readonly Func<string, ulong> _getBalance;

        private const int DefaultMaxTxAgeSeconds = 3600;

        private readonly Dictionary<string, long> _txIdTimestamps = new(StringComparer.Ordinal);

        private int _count;

        public PendingTransactionBuffer(Func<string, ulong> getConfirmedNonce, Func<string, ulong> getBalance)
        {
            _getConfirmedNonce = getConfirmedNonce ?? throw new ArgumentNullException(nameof(getConfirmedNonce));
            _getBalance = getBalance ?? throw new ArgumentNullException(nameof(getBalance));
        }

        public int Count { get { lock (_sync) return _count; } }

        public IEnumerable<Transaction> GetAll() => GetAllSnapshot();

        public List<Transaction> GetAllSnapshot()
        {
            lock (_sync)
                return _buffer.Values.SelectMany(v => v.Values).ToList();
        }

        public List<string> GetAllSenderKeysSnapshot()
        {
            lock (_sync)
                return _buffer.Keys.ToList();
        }

        public int GetCountForSender(string senderHex)
        {
            senderHex = Norm(senderHex);
            lock (_sync)
                return _buffer.TryGetValue(senderHex, out var list) ? list.Count : 0;
        }

        public bool Contains(Transaction tx)
        {
            if (tx is null) return false;

            byte[] txidBytes;
            try
            {
                txidBytes = tx.ComputeTransactionHash();
            }
            catch
            {
                return false;
            }

            string txIdHex = Hex(txidBytes);
            lock (_sync)
                return _txIdTimestamps.ContainsKey(txIdHex);
        }

        public bool Add(Transaction tx)
        {
            if (tx is null) return false;
            if (TransactionValidator.IsCoinbase(tx)) return false;
            if (tx.Sender is null || tx.Sender.Length != 32) return false;

            string senderHex = Hex(tx.Sender);
            byte[] txidBytes = tx.ComputeTransactionHash();
            string txIdHex = Hex(txidBytes);

            lock (_sync)
            {
                if (_txIdTimestamps.ContainsKey(txIdHex)) return false;

                ulong confirmedNonce = _getConfirmedNonce(senderHex);
                if (tx.TxNonce <= confirmedNonce) return false;

                if (!_buffer.TryGetValue(senderHex, out var list))
                {
                    list = new SortedList<ulong, Transaction>();
                    _buffer[senderHex] = list;
                }

                if (list.ContainsKey(tx.TxNonce)) return false;

                ulong runningBalance = _getBalance(senderHex);

                if (!TryAddU64(confirmedNonce, 1UL, out ulong expectedNonce))
                    return false;

                foreach (var kvp in list)
                {
                    if (kvp.Key != expectedNonce) return false;

                    if (!TryCost(kvp.Value.Amount, kvp.Value.Fee, out var totalCost)) return false;
                    if (totalCost > runningBalance) return false;

                    runningBalance -= totalCost;

                    if (!TryAddU64(expectedNonce, 1UL, out expectedNonce))
                        return false;
                }

                if (tx.TxNonce != expectedNonce) return false;

                if (!TryCost(tx.Amount, tx.Fee, out var thisCost)) return false;
                if (thisCost > runningBalance) return false;

                _txIdTimestamps[txIdHex] = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                list[tx.TxNonce] = tx;
                _count++;

                return true;
            }
        }


        public readonly struct SenderLease : IDisposable
        {
            private readonly PendingTransactionBuffer _owner;
            private readonly string _senderHex;
            private readonly bool _acquired;

            internal SenderLease(PendingTransactionBuffer owner, string senderHex, bool acquired)
            {
                _owner = owner;
                _senderHex = senderHex;
                _acquired = acquired;
            }

            public bool Acquired => _acquired;

            public void Dispose()
            {
                if (!_acquired) return;
                _owner.Unlock(_senderHex);
            }
        }

        public SenderLease TryLockSender(string senderHex)
        {
            senderHex = Norm(senderHex);

            lock (_sync)
            {
                if (_lockedSenders.Contains(senderHex))
                    return new SenderLease(this, senderHex, acquired: false);

                _lockedSenders.Add(senderHex);
                return new SenderLease(this, senderHex, acquired: true);
            }
        }

        public List<Transaction> GetReadyTransactions(string senderHex)
        {
            senderHex = Norm(senderHex);

            lock (_sync)
            {
                if (!_buffer.TryGetValue(senderHex, out var list)) return new();

                ulong confirmedNonce = _getConfirmedNonce(senderHex);
                var ready = new List<Transaction>();

                if (!TryAddU64(confirmedNonce, 1UL, out ulong expected))
                    return ready;

                ulong runningBalance = _getBalance(senderHex);

                while (list.TryGetValue(expected, out var tx))
                {
                    if (!TryCost(tx.Amount, tx.Fee, out var cost)) break;
                    if (cost > runningBalance) break;

                    runningBalance -= cost;
                    ready.Add(tx);

                    if (!TryAddU64(expected, 1UL, out expected))
                        break;
                }

                return ready;
            }
        }

        public void RemoveProcessed(string senderHex, ulong upToNonce)
        {
            senderHex = Norm(senderHex);

            lock (_sync)
            {
                if (!_buffer.TryGetValue(senderHex, out var list)) return;

                var toRemove = list.Keys.Where(n => n <= upToNonce).ToList();
                foreach (var nonce in toRemove)
                {
                    RemoveExact_NoLock(senderHex, list, nonce);
                }

                if (list.Count == 0) _buffer.Remove(senderHex);
            }
        }

        public bool Remove(Transaction tx)
        {
            if (tx?.Sender is null || tx.Sender.Length != 32) return false;

            string senderHex = Hex(tx.Sender);

            lock (_sync)
            {
                if (!_buffer.TryGetValue(senderHex, out var list)) return false;
                if (!list.ContainsKey(tx.TxNonce)) return false;

                RemoveTailFromNonce_NoLock(senderHex, list, tx.TxNonce);
                if (list.Count == 0) _buffer.Remove(senderHex);
                return true;
            }
        }

        public int PurgeInvalid()
        {
            int removed = 0;

            lock (_sync)
            {
                foreach (var sender in _buffer.Keys.ToList())
                {
                    if (!_buffer.TryGetValue(sender, out var list)) continue;

                    ulong confirmedNonce = _getConfirmedNonce(sender);

                    var toRemove = list.Keys.Where(nonce => nonce <= confirmedNonce).ToList();
                    foreach (var nonce in toRemove)
                    {
                        RemoveExact_NoLock(sender, list, nonce);
                        removed++;
                    }

                    if (list.Count == 0) _buffer.Remove(sender);
                }
            }

            return removed;
        }

        public ulong? GetHighestPendingNonce(string senderHex)
        {
            senderHex = Norm(senderHex);
            lock (_sync)
            {
                if (!_buffer.TryGetValue(senderHex, out var list) || list.Count == 0) return null;
                return list.Keys.Max();
            }
        }

        public int EvictStaleAndLowFee(int maxAgeSeconds = DefaultMaxTxAgeSeconds, int maxCount = 200_000)
        {
            int removed = 0;
            long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            lock (_sync)
            {
                foreach (var sender in _buffer.Keys.ToList())
                {
                    if (!_buffer.TryGetValue(sender, out var list)) continue;

                    ulong? firstExpired = null;

                    foreach (var kvp in list)
                    {
                        var tx = kvp.Value;
                        var txid = Hex(tx.ComputeTransactionHash());

                        if (_txIdTimestamps.TryGetValue(txid, out var createdAt) &&
                            (now - createdAt > maxAgeSeconds))
                        {
                            firstExpired = kvp.Key;
                            break;
                        }
                    }

                    if (firstExpired.HasValue)
                    {
                        int before = list.Count;
                        RemoveTailFromNonce_NoLock(sender, list, firstExpired.Value);
                        removed += (before - list.Count);

                        if (list.Count == 0) _buffer.Remove(sender);
                    }
                }

                while (_count > maxCount)
                {
                    if (!EvictWorstTailTx_NoLock(out int evictedNow)) break;
                    removed += evictedNow;
                }
            }

            return removed;
        }

        public List<Transaction> GetAllReadyTransactionsSortedByFee()
        {
            lock (_sync)
            {
                // Build nonce-safe ready chains per sender first.
                var readyBySender = new Dictionary<string, Queue<Transaction>>(StringComparer.Ordinal);

                foreach (var (senderHex, txList) in _buffer)
                {
                    ulong confirmedNonce = _getConfirmedNonce(senderHex);
                    ulong runningBalance = _getBalance(senderHex);

                    if (!TryAddU64(confirmedNonce, 1UL, out ulong expectedNonce))
                        continue;

                    var chain = new Queue<Transaction>();
                    while (txList.TryGetValue(expectedNonce, out var tx))
                    {
                        if (!TryCost(tx.Amount, tx.Fee, out var cost)) break;
                        if (cost > runningBalance) break;

                        runningBalance -= cost;
                        chain.Enqueue(tx);

                        if (!TryAddU64(expectedNonce, 1UL, out expectedNonce))
                            break;
                    }

                    if (chain.Count > 0)
                        readyBySender[senderHex] = chain;
                }

                if (readyBySender.Count == 0)
                    return new List<Transaction>();

                // Merge sender chains by fee while preserving strict per-sender nonce order.
                var senderOrder = readyBySender.Keys.OrderBy(k => k, StringComparer.Ordinal).ToList();
                int total = 0;
                for (int i = 0; i < senderOrder.Count; i++)
                    total += readyBySender[senderOrder[i]].Count;

                var merged = new List<Transaction>(total);
                while (true)
                {
                    string? bestSender = null;
                    Transaction? bestTx = null;

                    for (int i = 0; i < senderOrder.Count; i++)
                    {
                        string sender = senderOrder[i];
                        var queue = readyBySender[sender];
                        if (queue.Count == 0)
                            continue;

                        var candidate = queue.Peek();
                        if (bestTx == null)
                        {
                            bestTx = candidate;
                            bestSender = sender;
                            continue;
                        }

                        if (candidate.Fee > bestTx.Fee)
                        {
                            bestTx = candidate;
                            bestSender = sender;
                            continue;
                        }

                        if (candidate.Fee == bestTx.Fee)
                        {
                            if (candidate.TxNonce < bestTx.TxNonce)
                            {
                                bestTx = candidate;
                                bestSender = sender;
                                continue;
                            }

                            if (candidate.TxNonce == bestTx.TxNonce &&
                                string.CompareOrdinal(sender, bestSender) < 0)
                            {
                                bestTx = candidate;
                                bestSender = sender;
                            }
                        }
                    }

                    if (bestSender == null)
                        break;

                    merged.Add(readyBySender[bestSender].Dequeue());
                }

                return merged;
            }
        }


        private void Unlock(string senderHex)
        {
            senderHex = Norm(senderHex);
            lock (_sync) { _lockedSenders.Remove(senderHex); }
        }


        private void RemoveExact_NoLock(string senderHex, SortedList<ulong, Transaction> list, ulong nonce)
        {
            if (!list.TryGetValue(nonce, out var tx)) return;

            string txId = Hex(tx.ComputeTransactionHash());
            _txIdTimestamps.Remove(txId);

            if (list.Remove(nonce))
                _count--;
        }

        private void RemoveTailFromNonce_NoLock(string senderHex, SortedList<ulong, Transaction> list, ulong fromNonce)
        {
            var keys = list.Keys.Where(n => n >= fromNonce).ToList();
            foreach (var n in keys)
                RemoveExact_NoLock(senderHex, list, n);
        }

        private bool EvictWorstTailTx_NoLock(out int removed)
        {
            removed = 0;

            Transaction? worst = null;
            string? worstSender = null;
            ulong worstNonce = 0;

            foreach (var (sender, list) in _buffer)
            {
                if (list.Count == 0) continue;

                ulong tailNonce = list.Keys[list.Count - 1];
                var tailTx = list.Values[list.Count - 1];

                if (worst == null || IsWorse(tailTx, worst))
                {
                    worst = tailTx;
                    worstSender = sender;
                    worstNonce = tailNonce;
                }
            }

            if (worst == null || worstSender == null) return false;

            if (_buffer.TryGetValue(worstSender, out var l))
            {
                int before = l.Count;
                RemoveExact_NoLock(worstSender, l, worstNonce);
                if (l.Count == 0) _buffer.Remove(worstSender);
                removed = (before - l.Count);
                return removed > 0;
            }

            return false;
        }

        private static bool IsWorse(Transaction a, Transaction b)
        {
            if (a.Fee != b.Fee) return a.Fee < b.Fee;
            return a.TxNonce > b.TxNonce;
        }


        private static string Norm(string s) => (s ?? "").Trim().ToLowerInvariant();

        private static bool TryCost(ulong amount, ulong fee, out ulong cost)
        {
            if (ulong.MaxValue - amount < fee)
            {
                cost = 0;
                return false;
            }
            cost = amount + fee;
            return true;
        }

        private static bool TryAddU64(ulong a, ulong b, out ulong sum)
        {
            if (ulong.MaxValue - a < b)
            {
                sum = 0;
                return false;
            }
            sum = a + b;
            return true;
        }

        private static string Hex(byte[] data) => Convert.ToHexString(data).ToLowerInvariant();
    }
}

