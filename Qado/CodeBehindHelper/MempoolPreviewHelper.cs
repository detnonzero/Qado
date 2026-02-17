using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Windows;
using Qado.Blockchain;
using Qado.Mempool;
using Qado.Utils;

namespace Qado.CodeBehindHelper
{
    public static class MempoolPreviewHelper
    {
        private static Timer? _mempoolUiTimer;
        private static int _isTickRunning; // 0 = idle, 1 = running

        public static void StartMempoolUiUpdater(
            MempoolManager mempool,
            ObservableCollection<MainWindow.MempoolRow> targetCollection,
            Func<bool>? isMiningActive = null,
            Action? onUiTick = null)
        {
            if (mempool == null) throw new ArgumentNullException(nameof(mempool));
            if (targetCollection == null) throw new ArgumentNullException(nameof(targetCollection));

            StopMempoolUiUpdater(); // ensure only one timer instance

            _mempoolUiTimer = new Timer(_ =>
            {
                if (Interlocked.Exchange(ref _isTickRunning, 1) == 1)
                    return;

                try
                {
                    _ = isMiningActive?.Invoke(); // compatibility hook

                    var txs = SnapshotTopMempoolTxs(mempool, limit: 100);

                    var app = Application.Current;
                    if (app == null) return;
                    var dispatcher = app.Dispatcher;
                    if (dispatcher == null || dispatcher.HasShutdownStarted || dispatcher.HasShutdownFinished)
                        return;

                    dispatcher.BeginInvoke(new Action(() =>
                    {
                        targetCollection.Clear();

                        for (int i = 0; i < txs.Count; i++)
                        {
                            var tx = txs[i];
                            targetCollection.Add(new MainWindow.MempoolRow
                            {
                                From = ToHex(tx.Sender),
                                To = ToHex(tx.Recipient),
                                AmountQado = QadoAmountParser.FormatNanoToQado(tx.Amount) + " QADO",
                                FeeQado = QadoAmountParser.FormatNanoToQado(tx.Fee) + " QADO",
                                Nonce = tx.TxNonce
                            });
                        }

                        try { onUiTick?.Invoke(); } catch { }
                    }));
                }
                catch
                {
                }
                finally
                {
                    Volatile.Write(ref _isTickRunning, 0);
                }
            },
            state: null,
            dueTime: TimeSpan.Zero,
            period: TimeSpan.FromSeconds(3));
        }

        public static void StopMempoolUiUpdater()
        {
            _mempoolUiTimer?.Dispose();
            _mempoolUiTimer = null;
            Volatile.Write(ref _isTickRunning, 0);
        }

        private static List<Transaction> SnapshotTopMempoolTxs(MempoolManager mempool, int limit)
        {
            List<Transaction> all;

            try
            {
                all = mempool.GetAll();
            }
            catch
            {
                return new List<Transaction>(0);
            }

            if (all == null || all.Count == 0)
                return new List<Transaction>(0);

            var list = new List<Transaction>(Math.Min(limit, all.Count));

            for (int i = 0; i < all.Count; i++)
            {
                var tx = all[i];
                if (tx == null) continue;
                if (TransactionValidator.IsCoinbase(tx)) continue;
                list.Add(tx);
            }

            if (list.Count == 0) return list;

            list.Sort(static (a, b) =>
            {
                int c = b.Fee.CompareTo(a.Fee);
                if (c != 0) return c;
                return a.TxNonce.CompareTo(b.TxNonce);
            });

            if (list.Count > limit)
                list.RemoveRange(limit, list.Count - limit);

            return list;
        }

        private static string ToHex(byte[]? value)
            => value == null || value.Length == 0 ? "" : Convert.ToHexString(value).ToLowerInvariant();
    }
}
