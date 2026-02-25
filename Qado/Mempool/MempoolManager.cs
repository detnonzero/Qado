using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Qado.Blockchain;
using Qado.Logging;

namespace Qado.Mempool
{
    public sealed class MempoolManager
    {
        private const int MaxMempoolTxCount = 50_000;

        private readonly object _gate = new();

        private readonly PendingTransactionBuffer _buffer;
        private readonly Func<string, ulong> _getBalanceHex;
        private readonly Func<string, ulong> _getConfirmedNonceHex;
        private readonly ILogSink? _log;

        private static readonly ConcurrentDictionary<string, int> _pendingBySender = new(StringComparer.Ordinal);

        public readonly struct ReorgReconcileResult
        {
            public readonly int RemovedIncluded;
            public readonly int Requeued;
            public readonly int Rejected;
            public readonly int PurgedInvalid;
            public readonly IReadOnlyList<Transaction> RequeuedTransactions;

            public ReorgReconcileResult(
                int removedIncluded,
                int requeued,
                int rejected,
                int purgedInvalid,
                IReadOnlyList<Transaction>? requeuedTransactions = null)
            {
                RemovedIncluded = removedIncluded;
                Requeued = requeued;
                Rejected = rejected;
                PurgedInvalid = purgedInvalid;
                RequeuedTransactions = requeuedTransactions ?? Array.Empty<Transaction>();
            }
        }

        public MempoolManager(
            Func<string, ulong> getBalanceHex,
            Func<string, ulong> getConfirmedNonceHex,
            ILogSink? log = null)
        {
            _getBalanceHex = getBalanceHex ?? throw new ArgumentNullException(nameof(getBalanceHex));
            _getConfirmedNonceHex = getConfirmedNonceHex ?? throw new ArgumentNullException(nameof(getConfirmedNonceHex));
            _log = log;

            _buffer = new PendingTransactionBuffer(_getConfirmedNonceHex, _getBalanceHex);
        }

        public PendingTransactionBuffer GetBuffer() => _buffer;

        public int Count
        {
            get { lock (_gate) return _buffer.Count; }
        }

        public static int PendingCount(string senderHex)
        {
            senderHex = (senderHex ?? "").ToLowerInvariant();
            return _pendingBySender.TryGetValue(senderHex, out var n) ? n : 0;
        }

        public bool TryAdd(Transaction tx)
        {
            if (tx is null) return false;

            if (TransactionValidator.IsCoinbase(tx))
            {
                _log?.Warn("Mempool", "Rejected coinbase transaction.");
                return false;
            }

            if (!TransactionValidator.ValidateForMempool(tx, out var reason))
            {
                _log?.Warn("Mempool", $"Transaction rejected: {reason}");
                return false;
            }

            if (tx.Sender is not { Length: 32 } || tx.Recipient is not { Length: 32 })
            {
                _log?.Warn("Mempool", "TX endpoints malformed.");
                return false;
            }

            string senderHex = Convert.ToHexString(tx.Sender).ToLowerInvariant();
            string recipHex = Convert.ToHexString(tx.Recipient).ToLowerInvariant();

            lock (_gate)
            {
                bool poolWasFull = _buffer.Count >= MaxMempoolTxCount;

                bool added = _buffer.Add(tx);
                if (!added)
                {
                    _log?.Warn("Mempool", "Transaction not added to buffer (nonce ordering/balance/duplicate).");
                    ResyncSenderCount_NoThrow(senderHex);
                    return false;
                }

                int replacementEvicted = 0;
                if (poolWasFull)
                {
                    replacementEvicted = _buffer.EvictStaleAndLowFee(maxAgeSeconds: int.MaxValue, maxCount: MaxMempoolTxCount);
                    bool incomingKept = _buffer.Contains(tx);
                    if (!incomingKept)
                    {
                        _log?.Warn("Mempool", "Mempool full: incoming transaction is not competitive enough.");
                        ResyncAllCounts_NoThrow();
                        return false;
                    }

                    if (replacementEvicted > 0)
                        ResyncAllCounts_NoThrow();
                }

                ResyncSenderCount_NoThrow(senderHex);

                if (replacementEvicted > 0)
                {
                    _log?.Info("Mempool",
                        $"Mempool replacement: evicted={replacementEvicted} to admit nonce={tx.TxNonce} fee={tx.Fee}");
                }

                _log?.Info("Mempool",
                    $"TX added: {Short(senderHex)} → {Short(recipHex)} | Nonce={tx.TxNonce} | Fee={tx.Fee}");
                return true;
            }
        }

        public List<Transaction> GetReadyForSender(string senderHex)
        {
            senderHex = (senderHex ?? "").ToLowerInvariant();
            lock (_gate) return _buffer.GetReadyTransactions(senderHex);
        }

        public List<Transaction> GetAll()
        {
            lock (_gate) return _buffer.GetAll().ToList();
        }

        public void ConfirmUpTo(string senderHex, ulong nonce)
        {
            senderHex = (senderHex ?? "").ToLowerInvariant();
            lock (_gate)
            {
                _buffer.RemoveProcessed(senderHex, nonce);
                ResyncSenderCount_NoThrow(senderHex);
            }
        }

        public int EvictStaleAndLowFee(int maxAgeSeconds = 3600, int maxCount = MaxMempoolTxCount)
        {
            lock (_gate)
            {
                int evicted = _buffer.EvictStaleAndLowFee(maxAgeSeconds, maxCount);
                if (evicted > 0)
                {
                    _log?.Info("Mempool", $"Evicted {evicted} stale/low-fee transactions.");
                    ResyncAllCounts_NoThrow();
                }
                return evicted;
            }
        }

        public void RemoveIncluded(Block block)
        {
            if (block?.Transactions == null) return;

            var maxNonceBySender = CollectIncludedMaxNonceBySender(block);
            int removed = 0;
            HashSet<string> touched = new(StringComparer.Ordinal);

            lock (_gate)
            {
                removed = RemoveIncludedBySenderMaxNonceNoLock(maxNonceBySender, touched);

                foreach (var s in touched)
                    ResyncSenderCount_NoThrow(s);
            }

            if (removed > 0)
                _log?.Info("Mempool", $"Removed {removed} included transaction(s).");
        }

        public int PurgeInvalid()
        {
            lock (_gate)
            {
                int purged = _buffer.PurgeInvalid();
                if (purged > 0)
                {
                    _log?.Warn("Mempool", $"Purged {purged} invalid transaction(s).");
                    ResyncAllCounts_NoThrow();
                }
                return purged;
            }
        }

        public ReorgReconcileResult ReconcileAfterReorg(
            IReadOnlyList<Block>? oldCanonicalBlocksDesc,
            IReadOnlyList<Block>? newCanonicalBlocksAsc)
        {
            oldCanonicalBlocksDesc ??= Array.Empty<Block>();
            newCanonicalBlocksAsc ??= Array.Empty<Block>();

            var newTxIds = new HashSet<string>(StringComparer.Ordinal);
            for (int i = 0; i < newCanonicalBlocksAsc.Count; i++)
            {
                var b = newCanonicalBlocksAsc[i];
                if (b?.Transactions == null) continue;

                for (int t = 1; t < b.Transactions.Count; t++)
                {
                    var tx = b.Transactions[t];
                    if (tx == null) continue;
                    newTxIds.Add(Hex(tx.ComputeTransactionHash()));
                }
            }

            var candidates = new List<Transaction>(256);
            for (int i = 0; i < oldCanonicalBlocksDesc.Count; i++)
            {
                var b = oldCanonicalBlocksDesc[i];
                if (b?.Transactions == null) continue;

                for (int t = 1; t < b.Transactions.Count; t++)
                {
                    var tx = b.Transactions[t];
                    if (tx == null) continue;

                    var txId = Hex(tx.ComputeTransactionHash());
                    if (!newTxIds.Contains(txId))
                        candidates.Add(tx);
                }
            }

            candidates.Sort(static (a, b) =>
            {
                string sa = Convert.ToHexString(a.Sender).ToLowerInvariant();
                string sb = Convert.ToHexString(b.Sender).ToLowerInvariant();
                int sc = string.CompareOrdinal(sa, sb);
                if (sc != 0) return sc;
                return a.TxNonce.CompareTo(b.TxNonce);
            });

            int removedIncluded = 0;
            var touched = new HashSet<string>(StringComparer.Ordinal);
            lock (_gate)
            {
                for (int i = 0; i < newCanonicalBlocksAsc.Count; i++)
                    removedIncluded += RemoveIncludedNoLog(newCanonicalBlocksAsc[i], touched);

                foreach (var sender in touched)
                    ResyncSenderCount_NoThrow(sender);
            }

            int requeued = 0;
            int rejected = 0;
            var requeuedTxs = new List<Transaction>(Math.Min(candidates.Count, 2048));
            for (int i = 0; i < candidates.Count; i++)
            {
                if (TryAddReorgCandidateNoLog(candidates[i]))
                {
                    requeued++;
                    requeuedTxs.Add(candidates[i]);
                }
                else
                    rejected++;
            }

            int purgedInvalid = PurgeInvalid();

            if (removedIncluded > 0 || requeued > 0 || rejected > 0 || purgedInvalid > 0)
            {
                _log?.Info("Mempool",
                    $"Reorg reconcile: removedIncluded={removedIncluded}, requeued={requeued}, rejected={rejected}, purged={purgedInvalid}");
            }

            return new ReorgReconcileResult(removedIncluded, requeued, rejected, purgedInvalid, requeuedTxs);
        }


        private void ResyncSenderCount_NoThrow(string senderHex)
        {
            try
            {
                senderHex = (senderHex ?? "").ToLowerInvariant();
                _pendingBySender[senderHex] = _buffer.GetCountForSender(senderHex);
            }
            catch { }
        }

        private void ResyncAllCounts_NoThrow()
        {
            try
            {
                var senders = _buffer.GetAllSenderKeysSnapshot();
                foreach (var s in senders)
                    _pendingBySender[s] = _buffer.GetCountForSender(s);

                foreach (var key in _pendingBySender.Keys.ToList())
                    if (!senders.Contains(key))
                        _pendingBySender.TryRemove(key, out _);
            }
            catch { }
        }

        private int RemoveIncludedNoLog(Block block, HashSet<string> touchedSenders)
        {
            if (block?.Transactions == null) return 0;

            var maxNonceBySender = CollectIncludedMaxNonceBySender(block);
            return RemoveIncludedBySenderMaxNonceNoLock(maxNonceBySender, touchedSenders);
        }

        private int RemoveIncludedBySenderMaxNonceNoLock(
            Dictionary<string, ulong> maxNonceBySender,
            HashSet<string> touchedSenders)
        {
            if (maxNonceBySender == null || maxNonceBySender.Count == 0)
                return 0;

            int removed = 0;

            foreach (var kv in maxNonceBySender)
            {
                string senderHex = kv.Key;
                ulong maxIncludedNonce = kv.Value;

                int before = _buffer.GetCountForSender(senderHex);
                if (before <= 0)
                    continue;

                _buffer.RemoveProcessed(senderHex, maxIncludedNonce);

                int after = _buffer.GetCountForSender(senderHex);
                if (after >= before)
                    continue;

                removed += before - after;
                touchedSenders.Add(senderHex);
            }

            return removed;
        }

        private static Dictionary<string, ulong> CollectIncludedMaxNonceBySender(Block block)
        {
            var maxNonceBySender = new Dictionary<string, ulong>(StringComparer.Ordinal);
            if (block?.Transactions == null)
                return maxNonceBySender;

            for (int i = 0; i < block.Transactions.Count; i++)
            {
                var tx = block.Transactions[i];
                if (tx == null) continue;
                if (TransactionValidator.IsCoinbase(tx)) continue;
                if (tx.Sender is not { Length: 32 }) continue;

                string senderHex = Convert.ToHexString(tx.Sender).ToLowerInvariant();
                if (!maxNonceBySender.TryGetValue(senderHex, out var currentMax) || tx.TxNonce > currentMax)
                    maxNonceBySender[senderHex] = tx.TxNonce;
            }

            return maxNonceBySender;
        }

        private bool TryAddReorgCandidateNoLog(Transaction tx)
        {
            if (tx is null) return false;
            if (TransactionValidator.IsCoinbase(tx)) return false;
            if (!TransactionValidator.ValidateBasic(tx, out _)) return false;
            if (tx.Sender is not { Length: 32 }) return false;

            string senderHex = Convert.ToHexString(tx.Sender).ToLowerInvariant();

            lock (_gate)
            {
                if (_buffer.Count >= MaxMempoolTxCount)
                {
                    ResyncSenderCount_NoThrow(senderHex);
                    return false;
                }

                bool added = _buffer.Add(tx);
                ResyncSenderCount_NoThrow(senderHex);
                return added;
            }
        }

        private static string Hex(byte[] data) => Convert.ToHexString(data).ToLowerInvariant();

        private static string Short(string hex, int head = 6, int tail = 6)
            => string.IsNullOrEmpty(hex) || hex.Length <= head + tail ? hex : $"{hex[..head]}…{hex[^tail..]}";
    }
}

