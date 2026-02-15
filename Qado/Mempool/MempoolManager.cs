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
                if (_buffer.Count >= MaxMempoolTxCount)
                {
                    _log?.Warn("Mempool", "Mempool full.");
                    return false;
                }

                bool added = _buffer.Add(tx);
                if (!added)
                {
                    _log?.Warn("Mempool", "Transaction not added to buffer (nonce ordering/balance/duplicate).");
                    ResyncSenderCount_NoThrow(senderHex);
                    return false;
                }

                ResyncSenderCount_NoThrow(senderHex);

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

            int removed = 0;
            HashSet<string> touched = new(StringComparer.Ordinal);

            lock (_gate)
            {
                foreach (var tx in block.Transactions)
                {
                    if (tx == null) continue;
                    if (TransactionValidator.IsCoinbase(tx)) continue;
                    if (tx.Sender is not { Length: 32 }) continue;

                    string senderHex = Convert.ToHexString(tx.Sender).ToLowerInvariant();

                    if (_buffer.Remove(tx))
                    {
                        removed++;
                        touched.Add(senderHex);
                    }
                }

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

        private static string Short(string hex, int head = 6, int tail = 6)
            => string.IsNullOrEmpty(hex) || hex.Length <= head + tail ? hex : $"{hex[..head]}…{hex[^tail..]}";
    }
}

