using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Qado.Logging;

namespace Qado.Networking
{
    public readonly record struct ValidatorWorkItem(
        byte[] Payload,
        PeerSession Peer,
        bool? EnforceRateLimitOverride = null);

    public sealed class ValidatorWorker : IDisposable
    {
        private const int DefaultQueueCapacity = 4096;

        private readonly Func<ValidatorWorkItem, CancellationToken, Task> _processor;
        private readonly ILogSink? _log;
        private readonly Channel<ValidatorWorkItem> _queue;
        private readonly List<Task> _workers = new();
        private readonly CancellationTokenSource _disposeCts = new();
        private int _started;

        public ValidatorWorker(
            Func<ValidatorWorkItem, CancellationToken, Task> processor,
            ILogSink? log = null,
            int queueCapacity = DefaultQueueCapacity)
        {
            _processor = processor ?? throw new ArgumentNullException(nameof(processor));
            _log = log;

            if (queueCapacity < 256)
                queueCapacity = 256;

            var opts = new BoundedChannelOptions(queueCapacity)
            {
                SingleReader = false,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            };
            _queue = Channel.CreateBounded<ValidatorWorkItem>(opts);
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
                            if (!await _queue.Reader.WaitToReadAsync(token).ConfigureAwait(false))
                                break;

                            while (_queue.Reader.TryRead(out var item))
                            {
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
                                    _log?.Warn("Validator", $"Worker error: {ex.Message}");
                                }
                            }
                        }
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                    }
                }, CancellationToken.None));
            }
        }

        public bool Enqueue(in ValidatorWorkItem item)
        {
            if (item.Payload == null || item.Payload.Length == 0)
                return false;
            return _queue.Writer.TryWrite(item);
        }

        public void Dispose()
        {
            _disposeCts.Cancel();
            _queue.Writer.TryComplete();

            try { Task.WaitAll(_workers.ToArray(), TimeSpan.FromSeconds(2)); } catch { }
            try { _disposeCts.Dispose(); } catch { }
        }
    }
}
