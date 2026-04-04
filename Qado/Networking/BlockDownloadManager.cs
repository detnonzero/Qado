using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Qado.Blockchain;
using Qado.Logging;
using Qado.Storage;

namespace Qado.Networking
{
    public sealed class BlockDownloadManager : IDisposable
    {
        public const int MaxInflightGlobal = 4;
        public const int MaxInflightPerPeer = 1;
        public const int RecoveryRequestBatchSize = 1;
        public const int SequentialChunkSize = 128;
        public static readonly TimeSpan RequestedTimeout = TimeSpan.FromSeconds(12);

        private static readonly TimeSpan PeerFailureCooldownBase = TimeSpan.FromSeconds(25);
        private static readonly TimeSpan PeerFailureCooldownCap = TimeSpan.FromMinutes(10);
        private static readonly TimeSpan PeerFailureStreakResetAfter = TimeSpan.FromMinutes(15);
        private static readonly TimeSpan OutOfPlanFallbackTtl = TimeSpan.FromMinutes(2);
        private static readonly TimeSpan OutOfPlanFallbackLogWindow = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan LatencyFreshWindow = TimeSpan.FromMinutes(2);
        private const int MaxOutOfPlanFallbackHashes = 1024;

        private readonly object _gate = new();
        private readonly Queue<byte[]> _missingQueue = new();
        private readonly HashSet<string> _queuedSet = new(StringComparer.Ordinal);
        private readonly Dictionary<string, InflightRequest> _inflightByHash = new(StringComparer.Ordinal);
        private readonly Dictionary<string, HashSet<string>> _inflightByPeer = new(StringComparer.Ordinal);
        private readonly Dictionary<string, DateTime> _peerCooldownUntilUtc = new(StringComparer.Ordinal);
        private readonly Dictionary<string, PeerFailureState> _peerFailureStateByKey = new(StringComparer.Ordinal);
        private readonly Dictionary<string, DateTime> _outOfPlanFallbackUntilUtc = new(StringComparer.Ordinal);
        private readonly object _fallbackLogGate = new();
        private readonly Dictionary<string, SuppressedInfoLogState> _fallbackLogStateBySource = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, BlockSyncItemState> _stateByHash = new(StringComparer.Ordinal);

        private readonly Func<IReadOnlyCollection<PeerSession>> _sessionSnapshot;
        private readonly Func<PeerSession, MsgType, byte[], CancellationToken, Task> _sendFrameAsync;
        private readonly Func<byte[], bool> _haveBlock;
        private readonly Func<ValidationWorkItem, bool> _enqueueValidator;
        private readonly Action<byte[]> _revalidateStoredBlock;
        private readonly ILogSink? _log;

        private readonly SemaphoreSlim _pumpSignal = new(0, 1);
        private readonly CancellationTokenSource _disposeCts = new();
        private int _haveBlockCheckErrorLogged;
        private int _candidateCheckErrorLogged;
        private int _started;
        private string? _activeChunkPeerKey;
        private string? _lastChunkPeerKey;
        private int _activeChunkRequested;

        private sealed class InflightRequest
        {
            public byte[] Hash = Array.Empty<byte>();
            public string PeerKey = string.Empty;
            public DateTime RequestedUtc;
        }

        private sealed class PeerFailureState
        {
            public int FailureStreak;
            public DateTime LastFailureUtc = DateTime.MinValue;
        }

        private sealed class SuppressedInfoLogState
        {
            public DateTime LastLogUtc = DateTime.MinValue;
            public int Suppressed;
            public string LastMessage = string.Empty;
        }

        public BlockDownloadManager(
            Func<IReadOnlyCollection<PeerSession>> sessionSnapshot,
            Func<PeerSession, MsgType, byte[], CancellationToken, Task> sendFrameAsync,
            Func<byte[], bool> haveBlock,
            Func<ValidationWorkItem, bool> enqueueValidator,
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

        public void ResetTransientSyncState(string reason = "manual")
        {
            int requeued = 0;

            lock (_gate)
            {
                if (_inflightByHash.Count > 0)
                {
                    var inflight = new List<InflightRequest>(_inflightByHash.Count);
                    foreach (var kv in _inflightByHash)
                        inflight.Add(kv.Value);

                    _inflightByHash.Clear();
                    _inflightByPeer.Clear();

                    for (int i = 0; i < inflight.Count; i++)
                    {
                        var req = inflight[i];
                        if (req.Hash is not { Length: 32 })
                            continue;

                        RequeueHash_NoLock(req.Hash, ToHex(req.Hash));
                        requeued++;
                    }
                }

                _peerCooldownUntilUtc.Clear();
                _peerFailureStateByKey.Clear();
                _outOfPlanFallbackUntilUtc.Clear();
                _activeChunkPeerKey = null;
                _lastChunkPeerKey = null;
                _activeChunkRequested = 0;
            }

            _log?.Info("Sync", $"Transient sync state reset ({reason}); requeued={requeued}.");
            if (requeued > 0)
                RequestPump();
        }

        public void Start(CancellationToken ct)
        {
            if (Interlocked.Exchange(ref _started, 1) != 0)
                return;

            _ = Task.Run(() => PumpLoopAsync(ct), CancellationToken.None);
            _ = Task.Run(() => RecoverUnvalidatedBlocksAsync(ct), CancellationToken.None);
        }

        public bool EnqueueBlockPayload(
            PeerSession peer,
            byte[] payload,
            BlockIngressKind ingress = BlockIngressKind.SyncPlan,
            bool? enforceRateLimitOverride = null)
        {
            if (peer == null || payload == null || payload.Length == 0)
                return false;

            return _enqueueValidator(new ValidationWorkItem(payload, peer, ingress, enforceRateLimitOverride));
        }

        public void OnPeerReady(PeerSession peer)
        {
            if (peer == null)
                return;

            RequestPump();
        }

        public void OnPeerDisconnected(PeerSession peer)
        {
            if (peer == null) return;

            int requeued = 0;
            string peerKey = NormalizePeerKey(peer.SessionKey);
            bool shouldBackoff = false;

            lock (_gate)
            {
                DateTime now = DateTime.UtcNow;
                if (string.Equals(_activeChunkPeerKey, peerKey, StringComparison.Ordinal))
                {
                    _activeChunkPeerKey = null;
                    _activeChunkRequested = 0;
                    _lastChunkPeerKey = peerKey;
                    shouldBackoff = true;
                }

                if (_inflightByPeer.TryGetValue(peerKey, out var hashes) && hashes.Count > 0)
                {
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
                    if (requeued > 0)
                        shouldBackoff = true;
                }

                if (shouldBackoff)
                    SchedulePeerCooldown_NoLock(peerKey, now);
            }

            if (requeued > 0)
                RequestPump();
        }

        public int QueueOutOfPlanFallback(byte[] hash, string source = "manual-out-of-plan")
        {
            if (hash is not { Length: 32 })
                return 0;

            int armed = 0;
            int queued = 0;

            lock (_gate)
            {
                DateTime now = DateTime.UtcNow;
                PruneOutOfPlanFallback_NoLock(now);
                armed = AddOutOfPlanFallback_NoLock(new[] { hash }, now);
                if (armed > 0)
                {
                    int duplicates = 0;
                    queued = EnqueueMissingHashes_NoLock(new[] { hash }, source, ref duplicates, now);
                }
            }

            if (queued > 0)
                RequestPump();

            if (armed > 0 || queued > 0)
                LogOutOfPlanFallback(source, armed, queued);

            return queued;
        }

        public BlockArrivalKind MarkBlockArrived(PeerSession peer, byte[] blockHash)
        {
            if (blockHash is not { Length: 32 })
                return BlockArrivalKind.Unsolicited;

            string hashHex = ToHex(blockHash);
            bool shouldPump = false;
            BlockArrivalKind arrivalKind;

            lock (_gate)
            {
                _outOfPlanFallbackUntilUtc.Remove(hashHex);

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
                    MarkPeerSuccess_NoLock(req.PeerKey);
                    _stateByHash[hashHex] = BlockSyncItemState.HaveBlock;
                    shouldPump = _missingQueue.Count > 0 && _inflightByHash.Count < MaxInflightGlobal;
                    arrivalKind = BlockArrivalKind.Requested;
                }
                else
                {
                    _queuedSet.Remove(hashHex);
                    _stateByHash[hashHex] = BlockSyncItemState.HaveBlock;
                    arrivalKind = BlockArrivalKind.Unsolicited;
                }
            }
            if (shouldPump)
                RequestPump();

            return arrivalKind;
        }

        public void MarkBlockValidated(byte[] blockHash, bool valid)
        {
            if (blockHash is not { Length: 32 })
                return;

            string hashHex = ToHex(blockHash);
            lock (_gate)
                _outOfPlanFallbackUntilUtc.Remove(hashHex);
            _stateByHash[hashHex] = valid ? BlockSyncItemState.Validated : BlockSyncItemState.Invalid;
        }

        public bool IsOutOfPlanFallbackHash(byte[] hash)
        {
            if (hash is not { Length: 32 })
                return false;

            string hex = ToHex(hash);
            lock (_gate)
            {
                var now = DateTime.UtcNow;
                PruneOutOfPlanFallback_NoLock(now);
                if (!_outOfPlanFallbackUntilUtc.TryGetValue(hex, out var until))
                    return false;

                if (until <= now)
                {
                    _outOfPlanFallbackUntilUtc.Remove(hex);
                    return false;
                }

                return true;
            }
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

            PeerSession? selectedPeer = null;
            string? selectedPeerKey = null;
            List<byte[]>? selectedHashes = null;
            DateTime now = DateTime.UtcNow;
            bool rotated = false;

            lock (_gate)
            {
                var timedOutPeers = RequeueTimedOut_NoLock(now);
                if (timedOutPeers.Count > 0)
                {
                    for (int i = 0; i < timedOutPeers.Count; i++)
                        SchedulePeerCooldown_NoLock(timedOutPeers[i], now);
                }

                if (_missingQueue.Count == 0)
                    return;

                var orderedPeers = BuildOrderedDownloadPeers_NoLock(peers, now);
                if (orderedPeers.Count == 0)
                    return;

                selectedPeer = ResolveActiveChunkPeer_NoLock(orderedPeers, now, allowRotate: true);
                if (selectedPeer == null)
                    return;

                selectedPeerKey = NormalizePeerKey(selectedPeer.SessionKey);

                int peerInflight = GetPeerInflightCount_NoLock(selectedPeerKey);
                int chunkRemaining = SequentialChunkSize - _activeChunkRequested;
                if (chunkRemaining <= 0 && peerInflight <= 0)
                {
                    _lastChunkPeerKey = selectedPeerKey;
                    _activeChunkPeerKey = null;
                    _activeChunkRequested = 0;
                    rotated = true;

                    selectedPeer = ResolveActiveChunkPeer_NoLock(orderedPeers, now, allowRotate: true);
                    if (selectedPeer == null)
                        return;

                    selectedPeerKey = NormalizePeerKey(selectedPeer.SessionKey);
                    peerInflight = GetPeerInflightCount_NoLock(selectedPeerKey);
                    chunkRemaining = SequentialChunkSize - _activeChunkRequested;
                }

                if (chunkRemaining <= 0)
                    return;

                int globalFree = MaxInflightGlobal - _inflightByHash.Count;
                if (globalFree <= 0)
                    return;

                int peerFree = MaxInflightPerPeer - peerInflight;
                if (peerFree <= 0)
                {
                    var alternatePeer = FindAlternatePeerWithCapacity_NoLock(orderedPeers, selectedPeerKey);
                    if (alternatePeer == null)
                        return;

                    selectedPeer = alternatePeer;
                    selectedPeerKey = NormalizePeerKey(selectedPeer.SessionKey);
                    peerInflight = GetPeerInflightCount_NoLock(selectedPeerKey);
                    peerFree = MaxInflightPerPeer - peerInflight;
                    if (peerFree <= 0)
                        return;

                    chunkRemaining = RecoveryRequestBatchSize;
                }

                int toTake = Math.Min(RecoveryRequestBatchSize, Math.Min(peerFree, Math.Min(globalFree, chunkRemaining)));
                if (toTake <= 0)
                    return;

                selectedHashes = TakeQueuedHashes_NoLock(toTake);
                if (selectedHashes.Count == 0)
                    return;

                if (selectedHashes.Count > RecoveryRequestBatchSize)
                {
                    for (int i = RecoveryRequestBatchSize; i < selectedHashes.Count; i++)
                    {
                        var overflowHash = selectedHashes[i];
                        if (overflowHash is { Length: 32 })
                            RequeueHash_NoLock(overflowHash, ToHex(overflowHash));
                    }

                    selectedHashes = selectedHashes.GetRange(0, RecoveryRequestBatchSize);
                }

                ReserveInflight_NoLock(selectedHashes, selectedPeerKey, now);
                if (string.Equals(_activeChunkPeerKey, selectedPeerKey, StringComparison.Ordinal))
                    _activeChunkRequested += selectedHashes.Count;
            }

            if (rotated && selectedPeer != null)
                _log?.Info("Sync", $"Rotated download peer -> {selectedPeer.RemoteEndpoint} (chunk={SequentialChunkSize}).");

            if (selectedPeer == null || selectedHashes == null || selectedHashes.Count == 0)
                return;

            byte[] payload;
            MsgType requestType;
            try
            {
                payload = SmallNetSyncProtocol.BuildGetAncestorPack(selectedHashes[0], 1);
                requestType = MsgType.GetAncestorPack;
            }
            catch
            {
                OnPeerDisconnected(selectedPeer);
                return;
            }

            try
            {
                await _sendFrameAsync(selectedPeer, requestType, payload, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _log?.Warn("Sync", $"{requestType} send failed for {selectedPeer.RemoteEndpoint}: {ex.Message}");
                OnPeerDisconnected(selectedPeer);
            }

            bool shouldRepump;
            lock (_gate)
            {
                shouldRepump = _missingQueue.Count > 0 && _inflightByHash.Count < MaxInflightGlobal;
            }

            if (shouldRepump)
                RequestPump();
        }

        private int EnqueueMissingHashes_NoLock(
            IReadOnlyList<byte[]>? hashes,
            string source,
            ref int duplicates,
            DateTime? nowOverride = null)
        {
            if (hashes == null || hashes.Count == 0)
                return 0;

            int added = 0;
            DateTime now = nowOverride ?? DateTime.UtcNow;

            for (int i = 0; i < hashes.Count; i++)
            {
                var hash = hashes[i];
                if (hash is not { Length: 32 })
                    continue;

                string hex = ToHex(hash);
                if (!IsHashDownloadCandidate_NoLock(hash, hex))
                {
                    continue;
                }

                if (SafeHaveBlock(hash))
                {
                    _stateByHash[hex] = BlockSyncItemState.HaveBlock;
                    continue;
                }

                if (_stateByHash.TryGetValue(hex, out var state))
                {
                    if (state == BlockSyncItemState.Invalid)
                    {
                        _stateByHash.TryRemove(hex, out _);
                    }
                    else if (state == BlockSyncItemState.Requested ||
                             state == BlockSyncItemState.HaveBlock ||
                             state == BlockSyncItemState.Validated)
                        continue;
                }

                if (_queuedSet.Add(hex))
                {
                    _missingQueue.Enqueue((byte[])hash.Clone());
                    _stateByHash[hex] = BlockSyncItemState.Queued;
                    added++;
                }
            }

            return added;
        }

        private int AddOutOfPlanFallback_NoLock(IReadOnlyList<byte[]> hashes, DateTime now)
        {
            if (hashes == null || hashes.Count == 0)
                return 0;

            int armed = 0;
            for (int i = 0; i < hashes.Count; i++)
            {
                var hash = hashes[i];
                if (hash is not { Length: 32 })
                    continue;

                string hex = ToHex(hash);
                if (_stateByHash.TryGetValue(hex, out var state) &&
                    (state == BlockSyncItemState.Requested ||
                     state == BlockSyncItemState.HaveBlock ||
                     state == BlockSyncItemState.Validated))
                    continue;

                if (!_outOfPlanFallbackUntilUtc.ContainsKey(hex) &&
                    _outOfPlanFallbackUntilUtc.Count >= MaxOutOfPlanFallbackHashes)
                    break;

                _outOfPlanFallbackUntilUtc[hex] = now + OutOfPlanFallbackTtl;
                armed++;
            }

            return armed;
        }

        private void PruneOutOfPlanFallback_NoLock(DateTime now)
        {
            if (_outOfPlanFallbackUntilUtc.Count == 0)
                return;

            var remove = new List<string>();
            foreach (var kv in _outOfPlanFallbackUntilUtc)
            {
                if (kv.Value <= now)
                    remove.Add(kv.Key);
            }

            for (int i = 0; i < remove.Count; i++)
                _outOfPlanFallbackUntilUtc.Remove(remove[i]);

            if (_outOfPlanFallbackUntilUtc.Count <= MaxOutOfPlanFallbackHashes)
                return;

            var overflow = new List<KeyValuePair<string, DateTime>>(_outOfPlanFallbackUntilUtc);
            overflow.Sort((a, b) => a.Value.CompareTo(b.Value));
            int extra = _outOfPlanFallbackUntilUtc.Count - MaxOutOfPlanFallbackHashes;
            for (int i = 0; i < extra && i < overflow.Count; i++)
                _outOfPlanFallbackUntilUtc.Remove(overflow[i].Key);
        }

        private List<string> RequeueTimedOut_NoLock(DateTime now)
        {
            var timedOutPeers = new HashSet<string>(StringComparer.Ordinal);
            if (_inflightByHash.Count == 0)
                return new List<string>();

            var timedOut = new List<string>();
            foreach (var kv in _inflightByHash)
            {
                var timeout = RequestedTimeout;
                if ((now - kv.Value.RequestedUtc) > timeout)
                    timedOut.Add(kv.Key);
            }

            for (int i = 0; i < timedOut.Count; i++)
            {
                string hex = timedOut[i];
                if (!_inflightByHash.TryGetValue(hex, out var req))
                    continue;

                _inflightByHash.Remove(hex);
                timedOutPeers.Add(req.PeerKey);

                if (_inflightByPeer.TryGetValue(req.PeerKey, out var set))
                {
                    set.Remove(hex);
                    if (set.Count == 0)
                        _inflightByPeer.Remove(req.PeerKey);
                }

                RequeueHash_NoLock(req.Hash, hex);
            }

            if (_activeChunkPeerKey != null && timedOutPeers.Contains(_activeChunkPeerKey))
            {
                _lastChunkPeerKey = _activeChunkPeerKey;
                _activeChunkPeerKey = null;
                _activeChunkRequested = 0;
            }

            return new List<string>(timedOutPeers);
        }

        private List<PeerSession> BuildOrderedDownloadPeers_NoLock(IReadOnlyCollection<PeerSession> peers, DateTime now)
        {
            var ordered = new List<PeerSession>(peers.Count);
            foreach (var peer in peers)
            {
                if (peer == null || !peer.HandshakeOk)
                    continue;
                if (peer.Client != null && !peer.Client.Connected)
                    continue;

                string peerKey = NormalizePeerKey(peer.SessionKey);
                if (_peerCooldownUntilUtc.TryGetValue(peerKey, out var until))
                {
                    if (until > now)
                        continue;
                    _peerCooldownUntilUtc.Remove(peerKey);
                }

                ordered.Add(peer);
            }

            ordered.Sort((a, b) =>
            {
                int la = GetPeerLatencySortKey(a, now);
                int lb = GetPeerLatencySortKey(b, now);
                int c = la.CompareTo(lb);
                if (c != 0)
                    return c;

                c = a.ConnectedUtc.CompareTo(b.ConnectedUtc);
                if (c != 0)
                    return c;

                return string.CompareOrdinal(NormalizePeerKey(a.SessionKey), NormalizePeerKey(b.SessionKey));
            });
            return ordered;
        }

        private PeerSession? FindAlternatePeerWithCapacity_NoLock(List<PeerSession> orderedPeers, string currentPeerKey)
        {
            if (orderedPeers == null || orderedPeers.Count == 0)
                return null;

            int start = 0;
            if (!string.IsNullOrWhiteSpace(currentPeerKey))
            {
                for (int i = 0; i < orderedPeers.Count; i++)
                {
                    if (string.Equals(NormalizePeerKey(orderedPeers[i].SessionKey), currentPeerKey, StringComparison.Ordinal))
                    {
                        start = (i + 1) % orderedPeers.Count;
                        break;
                    }
                }
            }

            for (int i = 0; i < orderedPeers.Count; i++)
            {
                var candidate = orderedPeers[(start + i) % orderedPeers.Count];
                string key = NormalizePeerKey(candidate.SessionKey);
                if (string.Equals(key, currentPeerKey, StringComparison.Ordinal))
                    continue;
                if (GetPeerInflightCount_NoLock(key) >= MaxInflightPerPeer)
                    continue;

                return candidate;
            }

            return null;
        }

        private static int GetPeerLatencySortKey(PeerSession peer, DateTime now)
        {
            if (peer == null)
                return int.MaxValue;

            if (peer.LastLatencyMs < 0)
                return int.MaxValue;

            if (peer.LastLatencyUpdatedUtc == DateTime.MinValue)
                return int.MaxValue;

            if ((now - peer.LastLatencyUpdatedUtc) > LatencyFreshWindow)
                return int.MaxValue;

            return peer.LastLatencyMs;
        }

        private void SchedulePeerCooldown_NoLock(string peerKey, DateTime now)
        {
            if (string.IsNullOrWhiteSpace(peerKey))
                return;

            if (!_peerFailureStateByKey.TryGetValue(peerKey, out var state))
            {
                state = new PeerFailureState();
                _peerFailureStateByKey[peerKey] = state;
            }

            if (state.LastFailureUtc != DateTime.MinValue &&
                (now - state.LastFailureUtc) > PeerFailureStreakResetAfter)
            {
                state.FailureStreak = 0;
            }

            state.FailureStreak = Math.Min(state.FailureStreak + 1, 32);
            state.LastFailureUtc = now;

            TimeSpan cooldown = ComputePeerFailureCooldown(state.FailureStreak);
            _peerCooldownUntilUtc[peerKey] = now + cooldown;
        }

        private void MarkPeerSuccess_NoLock(string peerKey)
        {
            if (string.IsNullOrWhiteSpace(peerKey))
                return;

            _peerCooldownUntilUtc.Remove(peerKey);
            _peerFailureStateByKey.Remove(peerKey);
        }

        private static TimeSpan ComputePeerFailureCooldown(int failureStreak)
        {
            int exp = Math.Clamp(failureStreak - 1, 0, 8);
            long factor = 1L << exp;
            long ticks = PeerFailureCooldownBase.Ticks * factor;
            if (ticks <= 0 || ticks > PeerFailureCooldownCap.Ticks)
                ticks = PeerFailureCooldownCap.Ticks;
            return new TimeSpan(ticks);
        }

        private void LogOutOfPlanFallback(string source, int armed, int queued)
        {
            string category = string.IsNullOrWhiteSpace(source) ? "fallback" : source.Trim();
            string message = $"{category}: armed={armed}, queued={queued}";

            lock (_fallbackLogGate)
            {
                if (!_fallbackLogStateBySource.TryGetValue(category, out var state))
                {
                    state = new SuppressedInfoLogState();
                    _fallbackLogStateBySource[category] = state;
                }

                var now = DateTime.UtcNow;
                state.LastMessage = message;
                if ((now - state.LastLogUtc) >= OutOfPlanFallbackLogWindow)
                {
                    if (state.Suppressed > 0)
                        _log?.Info("Sync", $"{state.LastMessage} (+{state.Suppressed} similar events suppressed)");
                    else
                        _log?.Info("Sync", state.LastMessage);

                    state.LastLogUtc = now;
                    state.Suppressed = 0;
                }
                else
                {
                    state.Suppressed++;
                }
            }
        }

        private PeerSession? ResolveActiveChunkPeer_NoLock(List<PeerSession> orderedPeers, DateTime now, bool allowRotate)
        {
            if (orderedPeers.Count == 0)
                return null;

            if (!string.IsNullOrWhiteSpace(_activeChunkPeerKey))
            {
                for (int i = 0; i < orderedPeers.Count; i++)
                {
                    var p = orderedPeers[i];
                    if (string.Equals(NormalizePeerKey(p.SessionKey), _activeChunkPeerKey, StringComparison.Ordinal))
                        return p;
                }

                _lastChunkPeerKey = _activeChunkPeerKey;
                _activeChunkPeerKey = null;
                _activeChunkRequested = 0;
            }

            int start = 0;
            if (allowRotate && !string.IsNullOrWhiteSpace(_lastChunkPeerKey))
            {
                for (int i = 0; i < orderedPeers.Count; i++)
                {
                    if (string.Equals(NormalizePeerKey(orderedPeers[i].SessionKey), _lastChunkPeerKey, StringComparison.Ordinal))
                    {
                        start = (i + 1) % orderedPeers.Count;
                        break;
                    }
                }
            }

            for (int i = 0; i < orderedPeers.Count; i++)
            {
                var candidate = orderedPeers[(start + i) % orderedPeers.Count];
                string key = NormalizePeerKey(candidate.SessionKey);
                if (_peerCooldownUntilUtc.TryGetValue(key, out var until) && until > now)
                    continue;

                _activeChunkPeerKey = key;
                _lastChunkPeerKey = key;
                _activeChunkRequested = 0;
                return candidate;
            }

            return null;
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

                if (!IsHashDownloadCandidate_NoLock(hash, hex))
                {
                    continue;
                }

                if (SafeHaveBlock(hash))
                {
                    _stateByHash[hex] = BlockSyncItemState.HaveBlock;
                    continue;
                }

                if (_stateByHash.TryGetValue(hex, out var state))
                {
                    if (state == BlockSyncItemState.Requested ||
                        state == BlockSyncItemState.HaveBlock ||
                        state == BlockSyncItemState.Validated)
                        continue;

                    if (state == BlockSyncItemState.Invalid)
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

            if (!IsHashDownloadCandidate_NoLock(hash, hashHex))
            {
                return;
            }

            if (SafeHaveBlock(hash))
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
  AND is_bad = 0
  AND bad_ancestor = 0
ORDER BY height ASC
LIMIT 50000;";
                    cmd.Parameters.AddWithValue("$st", BlockIndexStore.StatusHaveBlockPayload);
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

            _log?.Info("Sync", $"Recovery: revalidating {pending.Count} stored block(s).");

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

        private bool IsHashDownloadCandidate(byte[] hash)
        {
            if (hash is not { Length: 32 })
                return false;

            string hashHex = ToHex(hash);
            lock (_gate)
                return IsHashDownloadCandidate_NoLock(hash, hashHex);
        }

        private bool IsHashDownloadCandidate_NoLock(byte[] hash, string hashHex)
        {
            if (hash is not { Length: 32 })
                return false;

            var now = DateTime.UtcNow;
            PruneOutOfPlanFallback_NoLock(now);
            if (!_outOfPlanFallbackUntilUtc.TryGetValue(hashHex, out var until) || until <= now)
            {
                return false;
            }

            try
            {
                return !BlockIndexStore.IsBadOrHasBadAncestor(hash);
            }
            catch (Exception ex)
            {
                if (Interlocked.CompareExchange(ref _candidateCheckErrorLogged, 1, 0) == 0)
                    _log?.Warn("Sync", $"download-candidate check failed; suppressing repeats: {ex.Message}");
                return false;
            }
        }

        private bool SafeHaveBlock(byte[] hash)
        {
            try
            {
                return _haveBlock(hash);
            }
            catch (Exception ex)
            {
                if (Interlocked.CompareExchange(ref _haveBlockCheckErrorLogged, 1, 0) == 0)
                    _log?.Warn("Sync", $"have-block check failed; suppressing repeats: {ex.Message}");
                return false;
            }
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
