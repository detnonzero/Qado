using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Threading;

namespace Qado.Logging
{
    public sealed class GuiLogSink : ILogSink, IDisposable
    {
        private readonly Action<string> _logCallback;
        private readonly SynchronizationContext? _uiContext;
        private readonly bool _useUtc;

        private readonly ConcurrentQueue<string> _queue = new();
        private int _scheduled; // 0 = idle, 1 = scheduled
        private int _count;     // approximate queue length
        private volatile bool _disposed;

        private const int MaxQueuedLines = 5000;
        private const int MaxDrainPerTick = 250;

        public GuiLogSink(Action<string> logCallback, bool useUtcTimestamps = true)
        {
            _logCallback = logCallback ?? throw new ArgumentNullException(nameof(logCallback));
            _uiContext = SynchronizationContext.Current;
            _useUtc = useUtcTimestamps;
        }

        public void Dispose()
        {
            _disposed = true;
            while (_queue.TryDequeue(out _)) { }
            Interlocked.Exchange(ref _count, 0);
            Interlocked.Exchange(ref _scheduled, 0);
        }

        public void Info(string category, string message) => Enqueue("INFO", category, message);
        public void Warn(string category, string message) => Enqueue("WARN", category, message);
        public void Error(string category, string message) => Enqueue("ERROR", category, message);

        public void Error(string category, string message, Exception ex)
            => Error(category, $"{message} ({ex.GetType().Name}: {ex.Message})");


        private void Enqueue(string level, string category, string message)
        {
            if (_disposed) return;

            category ??= "";
            message ??= "";

            var now = _useUtc ? DateTime.UtcNow : DateTime.Now;
            var ts = now.ToString("HH:mm:ss", CultureInfo.InvariantCulture);
            var line = $"[{ts}] [{level}] [{category}] {message}";

            _queue.Enqueue(line);
            int newCount = Interlocked.Increment(ref _count);

            if (newCount > MaxQueuedLines)
            {
                while (Volatile.Read(ref _count) > MaxQueuedLines && _queue.TryDequeue(out _))
                    Interlocked.Decrement(ref _count);
            }

            TryScheduleDrain();
        }

        private void TryScheduleDrain()
        {
            if (_disposed) return;

            if (Interlocked.Exchange(ref _scheduled, 1) != 0)
                return;

            try
            {
                if (_uiContext != null)
                {
                    _uiContext.Post(static state => ((GuiLogSink)state!).Drain(), this);
                }
                else
                {
                    ThreadPool.QueueUserWorkItem(static state => ((GuiLogSink)state!).Drain(), this);
                }
            }
            catch
            {
                Interlocked.Exchange(ref _scheduled, 0);
            }
        }

        private void Drain()
        {
            if (_disposed)
            {
                Interlocked.Exchange(ref _scheduled, 0);
                return;
            }

            try
            {
                int drained = 0;

                while (drained < MaxDrainPerTick && _queue.TryDequeue(out var line))
                {
                    Interlocked.Decrement(ref _count);
                    drained++;

                    try { _logCallback(line); }
                    catch { /* ignore callback errors */ }
                }
            }
            catch
            {
            }
            finally
            {
                Interlocked.Exchange(ref _scheduled, 0);

                if (!_disposed && !_queue.IsEmpty)
                    TryScheduleDrain();
            }
        }
    }
}

