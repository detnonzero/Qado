using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Qado.Logging;
using Qado.Storage;

namespace Qado.Networking
{
    public sealed class SyncManager : IDisposable
    {
        public const int MaxInflightGlobal = 256;
        public const int MaxInflightPerPeer = 64;
        public const int MaxInvHashes = 50_000;
        public const int MaxGetDataBatch = 192;
        public static readonly TimeSpan RequestedTimeout = TimeSpan.FromSeconds(15);
        public static readonly int MaxHashListPayloadBytes = 4 + (MaxInvHashes * 32);

        private const int MaxInvSeenEntries = 200_000;
        private static readonly TimeSpan InvSeenTtl = TimeSpan.FromMinutes(30);

        private readonly object _gate = new();
        private readonly Queue<byte[]> _missingQueue = new();
        private readonly HashSet<string> _queuedSet = new(StringComparer.Ordinal);
        private readonly Dictionary<string, InflightRequest> _inflightByHash = new(StringComparer.Ordinal);
        private readonly Dictionary<string, HashSet<string>> _inflightByPeer = new(StringComparer.Ordinal);
        private readonly Dictionary<string, DateTime> _invSeenByHash = new(StringComparer.Ordinal);
        private readonly Queue<(string key, DateTime seenUtc)> _invSeenOrder = new();

        private readonly ConcurrentDictionary<string, BlockSyncItemState> _stateByHash = new(StringComparer.Ordinal);

        private readonly Func<IReadOnlyCollection<PeerSession>> _sessionSnapshot;
        private readonly Func<PeerSession, MsgType, byte[], CancellationToken, Task> _sendFrameAsync;
        private readonly Func<byte[], bool> _haveBlock;
        private readonly Func<ValidatorWorkItem, bool> _enqueueValidator;
        private readonly Action<byte[]> _revalidateStoredBlock;
        private readonly ILogSink? _log;

        private readonly SemaphoreSlim _pumpSignal = new(0, 1);
        private readonly CancellationTokenSource _disposeCts = new();
        private int _started;

        private sealed class InflightRequest
        {
            public byte[] Hash = Array.Empty<byte>();
            public string PeerKey = string.Empty;
            public DateTime RequestedUtc;
        }

        public SyncManager(
            Func<IReadOnlyCollection<PeerSession>> sessionSnapshot,
            Func<PeerSession, MsgType, byte[], CancellationToken, Task> sendFrameAsync,
            Func<byte[], bool> haveBlock,
            Func<ValidatorWorkItem, bool> enqueueValidator,
            Action<byte[]> revalidateStoredBlock,
            ILogSink? log = null)
        {
            _sessionSnapshot = sessionSnapshot ?? throw new ArgumentNullException(nameof(sessionSnapshot));
            _sendFrameAsync = sendFrameAsync ?? throw new ArgumentNullException(nameof(sendFrameAsync));
            _haveBlock = haveBlock ?? throw new ArgumentNullException(nameof(haveBlock));
            _enqueueValidator = enqueueValidator ?? throw new ArgumentNullException(nameof(enqueueValidator));
            _revalidateStoredBlock = revalidateStoredBlock ?? throw new ArgumentNullException(nameof(revalidateStoredBlock));
            _log = log;
        }

        public void Start(CancellationToken ct)
        {
            if (Interlocked.Exchange(ref _started, 1) != 0)
                return;

            _ = Task.Run(() => PumpLoopAsync(ct), CancellationToken.None);
            _ = Task.Run(() => RecoverUnvalidatedBlocksAsync(ct), CancellationToken.None);

            RequestPump();
        }

        public bool EnqueueBlockPayload(PeerSession peer, byte[] payload, bool? enforceRateLimitOverride = null)
        {
            if (peer == null || payload == null || payload.Length == 0)
                return false;

            return _enqueueValidator(new ValidatorWorkItem(payload, peer, enforceRateLimitOverride));
        }

        public void OnPeerReady(PeerSession peer)
        {
            if (peer == null) return;
            RequestPump();
        }

        public void OnPeerDisconnected(PeerSession peer)
        {
            if (peer == null) return;

            int requeued = 0;
            string peerKey = NormalizePeerKey(peer.SessionKey);

            lock (_gate)
            {
                if (!_inflightByPeer.TryGetValue(peerKey, out var hashes) || hashes.Count == 0)
                    return;

                var list = new List<string>(hashes);
                for (int i = 0; i < list.Count; i++)
                {
                    string hex = list[i];
                    if (!_inflightByHash.TryGetValue(hex, out var req))
                        continue;

                    _inflightByHash.Remove(hex);
                    RequeueHash_NoLock(req.Hash, hex);
                    requeued++;
                }

                _inflightByPeer.Remove(peerKey);
            }

            if (requeued > 0)
                RequestPump();
        }

        public void OnInv(PeerSession peer, byte[] payload)
        {
            if (payload == null || payload.Length == 0)
                return;

            if (!TryParseHashListPayload(payload, MaxInvHashes, out var hashes))
            {
                _log?.Warn("Sync", "Invalid inv payload ignored.");
                return;
            }

            EnqueueMissingHashes(hashes, source: "inv");
        }

        public void OnHeaders(PeerSession peer, byte[] payload)
        {
            if (payload == null || payload.Length == 0)
                return;

            // Header-first is intentionally minimal here: payload carries header-hashes
            // and follows the same compact hash-list shape as inv.
            if (!TryParseHashListPayload(payload, MaxInvHashes, out var hashes))
            {
                _log?.Warn("Sync", "Invalid headers payload ignored.");
                return;
            }

            EnqueueMissingHashes(hashes, source: "headers");
        }

        public BlockArrivalKind MarkBlockArrived(PeerSession peer, byte[] blockHash)
        {
            if (blockHash is not { Length: 32 })
                return BlockArrivalKind.Unsolicited;

            string hashHex = ToHex(blockHash);

            lock (_gate)
            {
                if (_inflightByHash.TryGetValue(hashHex, out var req))
                {
                    _inflightByHash.Remove(hashHex);

                    if (_inflightByPeer.TryGetValue(req.PeerKey, out var set))
                    {
                        set.Remove(hashHex);
                        if (set.Count == 0)
                            _inflightByPeer.Remove(req.PeerKey);
                    }

                    _queuedSet.Remove(hashHex);
                    _stateByHash[hashHex] = BlockSyncItemState.HaveBlock;
                    return BlockArrivalKind.Requested;
                }

                _queuedSet.Remove(hashHex);
                _stateByHash[hashHex] = BlockSyncItemState.HaveBlock;
                return BlockArrivalKind.Unsolicited;
            }
        }

        public void MarkBlockValidated(byte[] blockHash, bool valid)
        {
            if (blockHash is not { Length: 32 })
                return;

            string hashHex = ToHex(blockHash);
            _stateByHash[hashHex] = valid ? BlockSyncItemState.Validated : BlockSyncItemState.Invalid;
        }

        private async Task PumpLoopAsync(CancellationToken externalCt)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(externalCt, _disposeCts.Token);
            var ct = linked.Token;

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await _pumpSignal.WaitAsync(TimeSpan.FromSeconds(1), ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch
                {
                }

                try
                {
                    await PumpDownloadAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _log?.Warn("Sync", $"Pump error: {ex.Message}");
                }
            }
        }

        private async Task PumpDownloadAsync(CancellationToken ct)
        {
            var peers = _sessionSnapshot();
            if (peers == null || peers.Count == 0)
                return;

            var sendJobs = new List<(PeerSession peer, List<byte[]> hashes)>();
            DateTime now = DateTime.UtcNow;

            lock (_gate)
            {
                RequeueTimedOut_NoLock(now);

                if (_missingQueue.Count == 0)
                    return;

                foreach (var peer in peers)
                {
                    if (peer == null || !peer.HandshakeOk || !peer.Client.Connected)
                        continue;

                    int globalFree = MaxInflightGlobal - _inflightByHash.Count;
                    if (globalFree <= 0)
                        break;

                    string peerKey = NormalizePeerKey(peer.SessionKey);
                    int peerInflight = GetPeerInflightCount_NoLock(peerKey);
                    int peerFree = MaxInflightPerPeer - peerInflight;
                    if (peerFree <= 0)
                        continue;

                    int toTake = Math.Min(MaxGetDataBatch, Math.Min(peerFree, globalFree));
                    if (toTake <= 0)
                        continue;

                    var hashes = TakeQueuedHashes_NoLock(toTake);
                    if (hashes.Count == 0)
                        continue;

                    ReserveInflight_NoLock(hashes, peerKey, now);
                    sendJobs.Add((peer, hashes));

                    if (_missingQueue.Count == 0 || _inflightByHash.Count >= MaxInflightGlobal)
                        break;
                }
            }

            for (int i = 0; i < sendJobs.Count; i++)
            {
                var (peer, hashes) = sendJobs[i];
                if (hashes.Count == 0)
                    continue;

                byte[] payload;
                try
                {
                    payload = BuildHashListPayload(hashes, MaxGetDataBatch);
                }
                catch
                {
                    OnPeerDisconnected(peer);
                    continue;
                }

                try
                {
                    await _sendFrameAsync(peer, MsgType.GetData, payload, ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _log?.Warn("Sync", $"getdata send failed for {peer.RemoteEndpoint}: {ex.Message}");
                    OnPeerDisconnected(peer);
                }
            }

            bool shouldRepump;
            lock (_gate)
            {
                shouldRepump = _missingQueue.Count > 0 && _inflightByHash.Count < MaxInflightGlobal;
            }

            if (shouldRepump)
                RequestPump();
        }

        private void EnqueueMissingHashes(List<byte[]> hashes, string source)
        {
            int added = 0;
            int duplicates = 0;
            DateTime now = DateTime.UtcNow;

            lock (_gate)
            {
                for (int i = 0; i < hashes.Count; i++)
                {
                    var hash = hashes[i];
                    if (hash is not { Length: 32 })
                        continue;

                    string hex = ToHex(hash);
                    if (IsDuplicateInv_NoLock(hex, now))
                    {
                        duplicates++;
                        continue;
                    }

                    if (_haveBlock(hash))
                    {
                        _stateByHash[hex] = BlockSyncItemState.HaveBlock;
                        continue;
                    }

                    if (_stateByHash.TryGetValue(hex, out var state))
                    {
                        if (state == BlockSyncItemState.Requested ||
                            state == BlockSyncItemState.HaveBlock ||
                            state == BlockSyncItemState.Validated ||
                            state == BlockSyncItemState.Invalid)
                            continue;
                    }

                    if (_queuedSet.Add(hex))
                    {
                        _missingQueue.Enqueue((byte[])hash.Clone());
                        _stateByHash[hex] = BlockSyncItemState.Queued;
                        added++;
                    }
                }
            }

            if (added > 0)
                RequestPump();

            if (added > 0 || duplicates > 0)
                _log?.Info("Sync", $"{source}: queued={added}, duplicates={duplicates}");
        }

        private bool IsDuplicateInv_NoLock(string hashHex, DateTime now)
        {
            bool duplicate = _invSeenByHash.TryGetValue(hashHex, out var seenUtc) &&
                             (now - seenUtc) < InvSeenTtl;

            _invSeenByHash[hashHex] = now;
            _invSeenOrder.Enqueue((hashHex, now));

            while (_invSeenOrder.Count > 0)
            {
                var (key, ts) = _invSeenOrder.Peek();
                bool staleByAge = (now - ts) > InvSeenTtl;
                bool staleBySize = _invSeenByHash.Count > MaxInvSeenEntries;

                if (!staleByAge && !staleBySize)
                    break;

                _invSeenOrder.Dequeue();

                if (_invSeenByHash.TryGetValue(key, out var current) && current == ts)
                    _invSeenByHash.Remove(key);
            }

            return duplicate;
        }

        private void RequeueTimedOut_NoLock(DateTime now)
        {
            if (_inflightByHash.Count == 0)
                return;

            var timedOut = new List<string>();
            foreach (var kv in _inflightByHash)
            {
                if ((now - kv.Value.RequestedUtc) > RequestedTimeout)
                    timedOut.Add(kv.Key);
            }

            for (int i = 0; i < timedOut.Count; i++)
            {
                string hex = timedOut[i];
                if (!_inflightByHash.TryGetValue(hex, out var req))
                    continue;

                _inflightByHash.Remove(hex);

                if (_inflightByPeer.TryGetValue(req.PeerKey, out var set))
                {
                    set.Remove(hex);
                    if (set.Count == 0)
                        _inflightByPeer.Remove(req.PeerKey);
                }

                RequeueHash_NoLock(req.Hash, hex);
            }
        }

        private List<byte[]> TakeQueuedHashes_NoLock(int count)
        {
            var result = new List<byte[]>(count);

            while (result.Count < count && _missingQueue.Count > 0)
            {
                var hash = _missingQueue.Dequeue();
                if (hash is not { Length: 32 })
                    continue;

                string hex = ToHex(hash);
                _queuedSet.Remove(hex);

                if (_haveBlock(hash))
                {
                    _stateByHash[hex] = BlockSyncItemState.HaveBlock;
                    continue;
                }

                if (_stateByHash.TryGetValue(hex, out var state))
                {
                    if (state == BlockSyncItemState.Requested ||
                        state == BlockSyncItemState.HaveBlock ||
                        state == BlockSyncItemState.Validated ||
                        state == BlockSyncItemState.Invalid)
                        continue;
                }

                result.Add(hash);
            }

            return result;
        }

        private void ReserveInflight_NoLock(List<byte[]> hashes, string peerKey, DateTime now)
        {
            if (!_inflightByPeer.TryGetValue(peerKey, out var set))
            {
                set = new HashSet<string>(StringComparer.Ordinal);
                _inflightByPeer[peerKey] = set;
            }

            for (int i = 0; i < hashes.Count; i++)
            {
                var hash = hashes[i];
                if (hash is not { Length: 32 })
                    continue;

                string hex = ToHex(hash);
                _inflightByHash[hex] = new InflightRequest
                {
                    Hash = (byte[])hash.Clone(),
                    PeerKey = peerKey,
                    RequestedUtc = now
                };

                set.Add(hex);
                _stateByHash[hex] = BlockSyncItemState.Requested;
            }
        }

        private int GetPeerInflightCount_NoLock(string peerKey)
        {
            if (!_inflightByPeer.TryGetValue(peerKey, out var set))
                return 0;
            return set.Count;
        }

        private void RequeueHash_NoLock(byte[] hash, string hashHex)
        {
            if (hash is not { Length: 32 })
                return;

            if (_haveBlock(hash))
            {
                _stateByHash[hashHex] = BlockSyncItemState.HaveBlock;
                return;
            }

            if (_queuedSet.Add(hashHex))
            {
                _missingQueue.Enqueue((byte[])hash.Clone());
                _stateByHash[hashHex] = BlockSyncItemState.Queued;
            }
        }

        private async Task RecoverUnvalidatedBlocksAsync(CancellationToken externalCt)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(externalCt, _disposeCts.Token);
            var ct = linked.Token;

            List<byte[]> pending = new();

            try
            {
                lock (Db.Sync)
                {
                    using var cmd = Db.Connection!.CreateCommand();
                    cmd.CommandText = @"
SELECT hash
FROM block_index
WHERE status = $st
ORDER BY height ASC
LIMIT 50000;";
                    cmd.Parameters.AddWithValue("$st", BlockIndexStore.StatusSideStatelessAccepted);
                    using var r = cmd.ExecuteReader();
                    while (r.Read())
                    {
                        if (r[0] is byte[] h && h.Length == 32)
                            pending.Add(h);
                    }
                }
            }
            catch (Exception ex)
            {
                _log?.Warn("Sync", $"Recovery scan failed: {ex.Message}");
                return;
            }

            if (pending.Count == 0)
                return;

            _log?.Info("Sync", $"Recovery: revalidating {pending.Count} stored side block(s).");

            for (int i = 0; i < pending.Count; i++)
            {
                if (ct.IsCancellationRequested)
                    return;

                try
                {
                    _revalidateStoredBlock(pending[i]);
                }
                catch
                {
                }

                if ((i % 64) == 63)
                {
                    try { await Task.Delay(1, ct).ConfigureAwait(false); } catch { }
                }
            }
        }

        private void RequestPump()
        {
            try { _pumpSignal.Release(); } catch (SemaphoreFullException) { }
        }

        public static byte[] BuildHashListPayload(IReadOnlyList<byte[]> hashes, int maxHashes = MaxInvHashes)
        {
            if (hashes == null || hashes.Count == 0)
            {
                var empty = new byte[4];
                BinaryPrimitives.WriteUInt32LittleEndian(empty, 0);
                return empty;
            }

            if (maxHashes <= 0)
                maxHashes = 1;

            var valid = new List<byte[]>(Math.Min(maxHashes, hashes.Count));
            for (int i = 0; i < hashes.Count && valid.Count < maxHashes; i++)
            {
                if (hashes[i] is { Length: 32 } h)
                    valid.Add(h);
            }

            var payload = new byte[4 + (valid.Count * 32)];
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), (uint)valid.Count);

            int o = 4;
            for (int i = 0; i < valid.Count; i++)
            {
                valid[i].AsSpan(0, 32).CopyTo(payload.AsSpan(o, 32));
                o += 32;
            }

            return payload;
        }

        public static bool TryParseHashListPayload(byte[] payload, int maxHashes, out List<byte[]> hashes)
        {
            hashes = new List<byte[]>();
            if (payload == null)
                return false;

            // Canonical format: [u32 count][count * 32-byte hashes]
            if (payload.Length >= 4)
            {
                uint declared = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(0, 4));
                if (declared <= int.MaxValue)
                {
                    int count = (int)declared;
                    if (count >= 0 && count <= maxHashes)
                    {
                        int expected = 4 + (count * 32);
                        if (payload.Length == expected)
                        {
                            hashes = new List<byte[]>(count);
                            int o = 4;
                            for (int i = 0; i < count; i++)
                            {
                                hashes.Add(payload.AsSpan(o, 32).ToArray());
                                o += 32;
                            }

                            return true;
                        }
                    }
                }
            }

            // Legacy compatibility format: [count * 32-byte hashes] (without leading count).
            if (payload.Length == 0 || (payload.Length % 32) != 0)
                return false;

            int legacyCount = payload.Length / 32;
            if (legacyCount > maxHashes)
                return false;

            hashes = new List<byte[]>(legacyCount);
            int legacyOffset = 0;
            for (int i = 0; i < legacyCount; i++)
            {
                hashes.Add(payload.AsSpan(legacyOffset, 32).ToArray());
                legacyOffset += 32;
            }

            return true;
        }

        private static string ToHex(byte[] hash)
            => Convert.ToHexString(hash).ToLowerInvariant();

        private static string NormalizePeerKey(string endpoint)
            => (endpoint ?? string.Empty).Trim().ToLowerInvariant();

        public void Dispose()
        {
            _disposeCts.Cancel();
            _pumpSignal.Dispose();
            _disposeCts.Dispose();
        }
    }
}
