using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Qado.Logging;

namespace Qado.Networking
{
    public enum BlockIngressKind
    {
        LivePush = 0,
        SyncPlan = 1,
        Recovery = 2
    }

    public readonly record struct ValidationWorkItem(
        byte[] Payload,
        PeerSession Peer,
        BlockIngressKind Ingress = BlockIngressKind.SyncPlan,
        bool? EnforceRateLimitOverride = null);

    public sealed class ValidationWorker : IDisposable
    {
        private const int DefaultQueueCapacity = 4096;

        private readonly Func<ValidationWorkItem, CancellationToken, Task> _processor;
        private readonly ILogSink? _log;
        private readonly ConcurrentQueue<ValidationWorkItem> _highPriorityQueue = new();
        private readonly ConcurrentQueue<ValidationWorkItem> _normalQueue = new();
        private readonly SemaphoreSlim _signal = new(0);
        private readonly List<Task> _workers = new();
        private readonly CancellationTokenSource _disposeCts = new();
        private readonly int _queueCapacity;
        private int _queuedCount;
        private int _started;

        public ValidationWorker(
            Func<ValidationWorkItem, CancellationToken, Task> processor,
            ILogSink? log = null,
            int queueCapacity = DefaultQueueCapacity)
        {
            _processor = processor ?? throw new ArgumentNullException(nameof(processor));
            _log = log;

            if (queueCapacity < 256)
                queueCapacity = 256;
            _queueCapacity = queueCapacity;
        }

        public void Start(int workerCount, CancellationToken ct)
        {
            if (Interlocked.Exchange(ref _started, 1) != 0)
                return;

            if (workerCount <= 0)
                workerCount = 1;

            for (int i = 0; i < workerCount; i++)
            {
                _workers.Add(Task.Run(async () =>
                {
                    using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _disposeCts.Token);
                    var token = linked.Token;

                    try
                    {
                        while (!token.IsCancellationRequested)
                        {
                            await _signal.WaitAsync(token).ConfigureAwait(false);

                            if (!TryDequeue(out var item))
                                continue;

                            try
                            {
                                await _processor(item, token).ConfigureAwait(false);
                            }
                            catch (OperationCanceledException) when (token.IsCancellationRequested)
                            {
                                return;
                            }
                            catch (Exception ex)
                            {
                                string peer = item.Peer?.RemoteEndpoint ?? "unknown";
                                int payloadLen = item.Payload?.Length ?? 0;
                                _log?.Warn("Validation", $"Worker error for {peer} ingress={item.Ingress} payload={payloadLen}: {ex}");
                            }
                        }
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                    }
                }, CancellationToken.None));
            }
        }

        public bool Enqueue(in ValidationWorkItem item)
        {
            if (item.Payload == null || item.Payload.Length == 0)
                return false;

            while (true)
            {
                int observed = Volatile.Read(ref _queuedCount);
                if (observed >= _queueCapacity)
                    return false;

                if (Interlocked.CompareExchange(ref _queuedCount, observed + 1, observed) != observed)
                    continue;

                if (IsHighPriority(item.Ingress))
                    _highPriorityQueue.Enqueue(item);
                else
                    _normalQueue.Enqueue(item);

                try { _signal.Release(); } catch (SemaphoreFullException) { }
                return true;
            }
        }

        public void Dispose()
        {
            _disposeCts.Cancel();

            try { Task.WaitAll(_workers.ToArray(), TimeSpan.FromSeconds(2)); } catch { }
            try { _signal.Dispose(); } catch { }
            try { _disposeCts.Dispose(); } catch { }
        }

        private bool TryDequeue(out ValidationWorkItem item)
        {
            if (_highPriorityQueue.TryDequeue(out item))
            {
                Interlocked.Decrement(ref _queuedCount);
                return true;
            }

            if (_normalQueue.TryDequeue(out item))
            {
                Interlocked.Decrement(ref _queuedCount);
                return true;
            }

            item = default;
            return false;
        }

        private static bool IsHighPriority(BlockIngressKind ingress)
            => ingress == BlockIngressKind.LivePush;
    }
}
