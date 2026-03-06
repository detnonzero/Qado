using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Qado.Logging;

namespace Qado.Networking
{
    public enum BlockSyncState
    {
        Idle = 0,
        AwaitingBatchBegin = 1,
        ReceivingBatch = 2
    }

    public readonly record struct BlockSyncPrepareResult(bool Success, string Error);

    public readonly record struct BlockSyncCommitResult(bool Success, byte[] BlockHash, string Error);

    public sealed class BlockSyncClient : IDisposable
    {
        public static readonly TimeSpan BatchResponseTimeout = TimeSpan.FromSeconds(20);
        private static readonly TimeSpan PeerCooldown = TimeSpan.FromSeconds(15);

        private readonly object _gate = new();
        private readonly Func<IReadOnlyCollection<PeerSession>> _sessionSnapshot;
        private readonly Func<PeerSession, MsgType, byte[], CancellationToken, Task> _sendFrameAsync;
        private readonly Func<UInt128> _getLocalTipChainwork;
        private readonly Func<byte[]> _getLocalTipHash;
        private readonly Func<byte[], ulong, UInt128, PeerSession, CancellationToken, Task<BlockSyncPrepareResult>> _prepareBatchAsync;
        private readonly Func<byte[], ulong, byte[], PeerSession, CancellationToken, Task<BlockSyncCommitResult>> _commitBlockAsync;
        private readonly Func<PeerSession, BlocksEndStatus, CancellationToken, Task> _completeBatchAsync;
        private readonly Func<PeerSession?, string, CancellationToken, Task> _abortBatchAsync;
        private readonly Action<PeerSession, string>? _penalizePeer;
        private readonly ILogSink? _log;

        private readonly Dictionary<string, TipFrame> _tipByPeerKey = new(StringComparer.Ordinal);
        private readonly Dictionary<string, DateTime> _cooldownUntilByPeerKey = new(StringComparer.Ordinal);
        private readonly CancellationTokenSource _disposeCts = new();

        private BlockSyncState _state;
        private string? _activePeerKey;
        private bool _useLocatorNext = true;
        private byte[] _resumeFromHash = Array.Empty<byte>();
        private ulong _expectedHeight;
        private byte[] _expectedPrevHash = Array.Empty<byte>();
        private int _expectedBlocksInBatch;
        private int _receivedBlocksInBatch;
        private bool _batchPrepared;
        private int _requestSerial;
        private int _activeRequestSerial;

        public BlockSyncClient(
            Func<IReadOnlyCollection<PeerSession>> sessionSnapshot,
            Func<PeerSession, MsgType, byte[], CancellationToken, Task> sendFrameAsync,
            Func<UInt128> getLocalTipChainwork,
            Func<byte[]> getLocalTipHash,
            Func<byte[], ulong, UInt128, PeerSession, CancellationToken, Task<BlockSyncPrepareResult>> prepareBatchAsync,
            Func<byte[], ulong, byte[], PeerSession, CancellationToken, Task<BlockSyncCommitResult>> commitBlockAsync,
            Func<PeerSession, BlocksEndStatus, CancellationToken, Task> completeBatchAsync,
            Func<PeerSession?, string, CancellationToken, Task> abortBatchAsync,
            Action<PeerSession, string>? penalizePeer = null,
            ILogSink? log = null)
        {
            _sessionSnapshot = sessionSnapshot ?? throw new ArgumentNullException(nameof(sessionSnapshot));
            _sendFrameAsync = sendFrameAsync ?? throw new ArgumentNullException(nameof(sendFrameAsync));
            _getLocalTipChainwork = getLocalTipChainwork ?? throw new ArgumentNullException(nameof(getLocalTipChainwork));
            _getLocalTipHash = getLocalTipHash ?? throw new ArgumentNullException(nameof(getLocalTipHash));
            _prepareBatchAsync = prepareBatchAsync ?? throw new ArgumentNullException(nameof(prepareBatchAsync));
            _commitBlockAsync = commitBlockAsync ?? throw new ArgumentNullException(nameof(commitBlockAsync));
            _completeBatchAsync = completeBatchAsync ?? throw new ArgumentNullException(nameof(completeBatchAsync));
            _abortBatchAsync = abortBatchAsync ?? throw new ArgumentNullException(nameof(abortBatchAsync));
            _penalizePeer = penalizePeer;
            _log = log;
        }

        public BlockSyncState State
        {
            get
            {
                lock (_gate)
                    return _state;
            }
        }

        public bool IsActive
        {
            get
            {
                lock (_gate)
                    return _state != BlockSyncState.Idle;
            }
        }

        public async Task OnPeerReadyAsync(PeerSession peer, CancellationToken ct)
        {
            if (peer == null)
                return;

            await RequestTipAsync(peer, ct).ConfigureAwait(false);
        }

        public void RequestResync(string reason = "manual")
        {
            bool shouldAbort;
            string? activePeerKey;

            lock (_gate)
            {
                shouldAbort = _batchPrepared;
                activePeerKey = _activePeerKey;
                _state = BlockSyncState.Idle;
                _activePeerKey = null;
                _useLocatorNext = true;
                _batchPrepared = false;
                _expectedBlocksInBatch = 0;
                _receivedBlocksInBatch = 0;
                _expectedPrevHash = Array.Empty<byte>();
                _expectedHeight = 0;
                _activeRequestSerial = 0;
                _resumeFromHash = SafeCloneHash(_getLocalTipHash());
            }

            if (shouldAbort)
            {
                PeerSession? activePeer = null;
                var peers = _sessionSnapshot();
                foreach (var peer in peers)
                {
                    if (peer != null && string.Equals(peer.SessionKey, activePeerKey, StringComparison.Ordinal))
                    {
                        activePeer = peer;
                        break;
                    }
                }

                _ = _abortBatchAsync(activePeer, $"resync requested ({reason})", CancellationToken.None);
            }

            _log?.Info("Sync", $"Block sync resync requested ({reason}).");
            _ = RequestTipsFromAllPeersAsync(CancellationToken.None);
        }

        public async Task OnTipAsync(PeerSession peer, byte[] payload, CancellationToken ct)
        {
            if (peer == null)
                return;

            if (!BlockSyncProtocol.TryParseTipPayload(payload, out var tip))
                return;

            lock (_gate)
            {
                _tipByPeerKey[peer.SessionKey] = new TipFrame(tip.Height, SafeCloneHash(tip.Hash), tip.Chainwork);
            }

            await TryStartSyncAsync(ct).ConfigureAwait(false);
        }

        public async Task OnBlocksBeginAsync(PeerSession peer, byte[] payload, CancellationToken ct)
        {
            if (peer == null)
                return;

            if (!BlockSyncProtocol.TryParseBlocksBegin(payload, out var frame))
            {
                await HandleBatchFailureAsync(peer, "invalid blocks-begin payload", forceLocator: true, ct).ConfigureAwait(false);
                return;
            }

            bool shouldPrepare;
            TipFrame advertisedTip = default;
            UInt128 advertisedTipChainwork = 0;
            lock (_gate)
            {
                shouldPrepare =
                    _state == BlockSyncState.AwaitingBatchBegin &&
                    string.Equals(_activePeerKey, peer.SessionKey, StringComparison.Ordinal) &&
                    _tipByPeerKey.TryGetValue(peer.SessionKey, out advertisedTip);
                if (shouldPrepare)
                    advertisedTipChainwork = advertisedTip.Chainwork;
            }

            if (!shouldPrepare)
                return;

            var prep = await _prepareBatchAsync(frame.StartHash, frame.StartHeight, advertisedTipChainwork, peer, ct).ConfigureAwait(false);
            if (!prep.Success)
            {
                await HandleBatchFailureAsync(peer, $"batch prepare failed: {prep.Error}", forceLocator: true, ct).ConfigureAwait(false);
                return;
            }

            lock (_gate)
            {
                if (_state != BlockSyncState.AwaitingBatchBegin ||
                    !string.Equals(_activePeerKey, peer.SessionKey, StringComparison.Ordinal))
                {
                    return;
                }

                _batchPrepared = true;
                _state = BlockSyncState.ReceivingBatch;
                _expectedPrevHash = SafeCloneHash(frame.StartHash);
                _expectedHeight = frame.StartHeight;
                _expectedBlocksInBatch = frame.TotalBlocks;
                _receivedBlocksInBatch = 0;
                ArmTimeout_NoLock(peer.SessionKey);
            }
        }

        public async Task OnBlockChunkAsync(PeerSession peer, byte[] payload, CancellationToken ct)
        {
            if (peer == null)
                return;

            if (!BlockSyncProtocol.TryParseBlockChunk(payload, out var frame))
            {
                await HandleBatchFailureAsync(peer, "invalid block-chunk payload", forceLocator: true, ct).ConfigureAwait(false);
                return;
            }

            ulong expectedHeight;
            byte[] expectedPrevHash;
            lock (_gate)
            {
                if (_state != BlockSyncState.ReceivingBatch ||
                    !string.Equals(_activePeerKey, peer.SessionKey, StringComparison.Ordinal))
                {
                    return;
                }

                expectedHeight = _expectedHeight;
                expectedPrevHash = SafeCloneHash(_expectedPrevHash);
                if (frame.FirstHeight != expectedHeight)
                {
                    _ = HandleBatchFailureAsync(peer, "chunk first_height mismatch", forceLocator: true, ct);
                    return;
                }

                if (_receivedBlocksInBatch + frame.Blocks.Count > _expectedBlocksInBatch)
                {
                    _ = HandleBatchFailureAsync(peer, "chunk exceeds declared batch size", forceLocator: true, ct);
                    return;
                }
            }

            for (int i = 0; i < frame.Blocks.Count; i++)
            {
                var commit = await _commitBlockAsync(
                    frame.Blocks[i],
                    expectedHeight,
                    expectedPrevHash,
                    peer,
                    ct).ConfigureAwait(false);

                if (!commit.Success || commit.BlockHash is not { Length: 32 })
                {
                    await HandleBatchFailureAsync(peer, $"commit failed: {commit.Error}", forceLocator: true, ct).ConfigureAwait(false);
                    return;
                }

                expectedPrevHash = SafeCloneHash(commit.BlockHash);
                expectedHeight++;
            }

            lock (_gate)
            {
                if (_state != BlockSyncState.ReceivingBatch ||
                    !string.Equals(_activePeerKey, peer.SessionKey, StringComparison.Ordinal))
                {
                    return;
                }

                _expectedPrevHash = expectedPrevHash;
                _expectedHeight = expectedHeight;
                _receivedBlocksInBatch += frame.Blocks.Count;
                _resumeFromHash = SafeCloneHash(expectedPrevHash);
                ArmTimeout_NoLock(peer.SessionKey);
            }
        }

        public async Task OnBlocksEndAsync(PeerSession peer, byte[] payload, CancellationToken ct)
        {
            if (peer == null)
                return;

            if (!BlockSyncProtocol.TryParseBlocksEnd(payload, out var status))
            {
                await HandleBatchFailureAsync(peer, "invalid blocks-end payload", forceLocator: true, ct).ConfigureAwait(false);
                return;
            }

            bool isActive;
            int received;
            int expected;
            lock (_gate)
            {
                isActive =
                    _state == BlockSyncState.ReceivingBatch &&
                    string.Equals(_activePeerKey, peer.SessionKey, StringComparison.Ordinal);
                received = _receivedBlocksInBatch;
                expected = _expectedBlocksInBatch;
            }

            if (!isActive)
                return;

            if (received != expected)
            {
                await HandleBatchFailureAsync(peer, "batch ended before declared block count arrived", forceLocator: true, ct).ConfigureAwait(false);
                return;
            }

            await _completeBatchAsync(peer, status, ct).ConfigureAwait(false);

            if (status == BlocksEndStatus.TipReached)
            {
                lock (_gate)
                {
                    _state = BlockSyncState.Idle;
                    _activePeerKey = null;
                    _useLocatorNext = false;
                    _batchPrepared = false;
                    _expectedBlocksInBatch = 0;
                    _receivedBlocksInBatch = 0;
                    _expectedHeight = 0;
                    _expectedPrevHash = Array.Empty<byte>();
                    _activeRequestSerial = 0;
                }

                _log?.Info("Sync", "Block sync reached remote tip.");
                return;
            }

            await SendGetBlocksFromAsync(peer, SafeCloneHash(_resumeFromHash), ct).ConfigureAwait(false);
        }

        public async Task OnNoCommonAncestorAsync(PeerSession peer, CancellationToken ct)
        {
            if (peer == null)
                return;

            await HandleBatchFailureAsync(peer, "peer reported no common ancestor", forceLocator: true, ct).ConfigureAwait(false);
        }

        public async Task OnPeerDisconnectedAsync(PeerSession peer, CancellationToken ct)
        {
            if (peer == null)
                return;

            bool wasActive;
            lock (_gate)
            {
                _tipByPeerKey.Remove(peer.SessionKey);
                wasActive = string.Equals(_activePeerKey, peer.SessionKey, StringComparison.Ordinal) &&
                            _state != BlockSyncState.Idle;
            }

            if (!wasActive)
                return;

            await ResetAfterFailureAsync(peer, "peer disconnected mid-batch", forceLocator: !_batchPrepared, penalize: false, ct)
                .ConfigureAwait(false);
            await TryStartSyncAsync(ct).ConfigureAwait(false);
        }

        public void Dispose()
        {
            try { _disposeCts.Cancel(); } catch { }
            try { _disposeCts.Dispose(); } catch { }
        }

        private async Task TryStartSyncAsync(CancellationToken ct)
        {
            PeerSession? selectedPeer;
            bool useLocator;
            byte[] fromHash;

            lock (_gate)
            {
                if (_state != BlockSyncState.Idle)
                    return;

                PruneCooldown_NoLock();

                selectedPeer = SelectBestPeer_NoLock();
                if (selectedPeer == null)
                    return;

                _activePeerKey = selectedPeer.SessionKey;
                _state = BlockSyncState.AwaitingBatchBegin;
                _batchPrepared = false;
                _expectedBlocksInBatch = 0;
                _receivedBlocksInBatch = 0;
                _expectedHeight = 0;
                _expectedPrevHash = Array.Empty<byte>();
                useLocator = _useLocatorNext || _resumeFromHash is not { Length: 32 };
                fromHash = SafeCloneHash(_resumeFromHash is { Length: 32 } ? _resumeFromHash : _getLocalTipHash());
            }

            if (useLocator)
            {
                var locator = BlockLocatorBuilder.BuildCanonicalLocator();
                await SendRequestAsync(
                    selectedPeer,
                    MsgType.GetBlocksByLocator,
                    BlockSyncProtocol.BuildGetBlocksByLocator(locator),
                    ct).ConfigureAwait(false);
                _log?.Info("Sync", $"Block sync requesting locator batch from {selectedPeer.RemoteEndpoint}.");
            }
            else
            {
                await SendGetBlocksFromAsync(selectedPeer, fromHash, ct).ConfigureAwait(false);
                _log?.Info("Sync", $"Block sync continuing from {ShortHash(fromHash)} via {selectedPeer.RemoteEndpoint}.");
            }
        }

        private async Task SendGetBlocksFromAsync(PeerSession peer, byte[] fromHash, CancellationToken ct)
        {
            await SendRequestAsync(
                peer,
                MsgType.GetBlocksFrom,
                BlockSyncProtocol.BuildGetBlocksFrom(fromHash),
                ct).ConfigureAwait(false);

            lock (_gate)
            {
                _state = BlockSyncState.AwaitingBatchBegin;
                _batchPrepared = false;
                _expectedBlocksInBatch = 0;
                _receivedBlocksInBatch = 0;
                _expectedHeight = 0;
                _expectedPrevHash = Array.Empty<byte>();
            }
        }

        private async Task SendRequestAsync(PeerSession peer, MsgType type, byte[] payload, CancellationToken ct)
        {
            await _sendFrameAsync(peer, type, payload, ct).ConfigureAwait(false);

            lock (_gate)
            {
                ArmTimeout_NoLock(peer.SessionKey);
            }
        }

        private async Task HandleBatchFailureAsync(PeerSession peer, string reason, bool forceLocator, CancellationToken ct)
        {
            await ResetAfterFailureAsync(peer, reason, forceLocator, penalize: true, ct).ConfigureAwait(false);
            await TryStartSyncAsync(ct).ConfigureAwait(false);
        }

        private async Task ResetAfterFailureAsync(
            PeerSession? peer,
            string reason,
            bool forceLocator,
            bool penalize,
            CancellationToken ct)
        {
            bool shouldAbort;
            lock (_gate)
            {
                shouldAbort = _batchPrepared;
            }

            if (shouldAbort)
                await _abortBatchAsync(peer, reason, ct).ConfigureAwait(false);

            lock (_gate)
            {
                if (peer != null)
                    _cooldownUntilByPeerKey[peer.SessionKey] = DateTime.UtcNow + PeerCooldown;

                _state = BlockSyncState.Idle;
                _activePeerKey = null;
                _useLocatorNext = forceLocator;
                _batchPrepared = false;
                _expectedBlocksInBatch = 0;
                _receivedBlocksInBatch = 0;
                _expectedHeight = 0;
                _expectedPrevHash = Array.Empty<byte>();
                _activeRequestSerial = 0;
                _resumeFromHash = SafeCloneHash(_getLocalTipHash());
            }

            if (peer != null && penalize)
                _penalizePeer?.Invoke(peer, reason);

            _log?.Warn("Sync", $"Block sync reset: {reason}");
            await RequestTipsFromAllPeersAsync(ct).ConfigureAwait(false);
        }

        private async Task RequestTipsFromAllPeersAsync(CancellationToken ct)
        {
            var peers = _sessionSnapshot();
            foreach (var peer in peers)
            {
                if (peer == null || !peer.HandshakeOk)
                    continue;

                await RequestTipAsync(peer, ct).ConfigureAwait(false);
            }
        }

        private async Task RequestTipAsync(PeerSession peer, CancellationToken ct)
        {
            try
            {
                await _sendFrameAsync(peer, MsgType.GetTip, Array.Empty<byte>(), ct).ConfigureAwait(false);
            }
            catch
            {
            }
        }

        private void ArmTimeout_NoLock(string peerKey)
        {
            int serial = ++_requestSerial;
            _activeRequestSerial = serial;
            _ = MonitorTimeoutAsync(peerKey, serial);
        }

        private async Task MonitorTimeoutAsync(string peerKey, int serial)
        {
            try
            {
                await Task.Delay(BatchResponseTimeout, _disposeCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            PeerSession? peer = null;
            bool shouldReset = false;
            bool forceLocator = true;

            lock (_gate)
            {
                if (_activeRequestSerial != serial)
                    return;
                if (_state == BlockSyncState.Idle || !string.Equals(_activePeerKey, peerKey, StringComparison.Ordinal))
                    return;

                shouldReset = true;
                forceLocator = !_batchPrepared;
            }

            if (!shouldReset)
                return;

            var peers = _sessionSnapshot();
            foreach (var candidate in peers)
            {
                if (candidate != null && string.Equals(candidate.SessionKey, peerKey, StringComparison.Ordinal))
                {
                    peer = candidate;
                    break;
                }
            }

            await ResetAfterFailureAsync(peer, "batch timeout", forceLocator, penalize: true, CancellationToken.None)
                .ConfigureAwait(false);
            await TryStartSyncAsync(CancellationToken.None).ConfigureAwait(false);
        }

        private PeerSession? SelectBestPeer_NoLock()
        {
            var peers = _sessionSnapshot();
            PeerSession? best = null;
            UInt128 bestChainwork = 0;
            ulong bestHeight = 0;
            UInt128 localChainwork = _getLocalTipChainwork();

            foreach (var peer in peers)
            {
                if (peer == null || !peer.HandshakeOk)
                    continue;

                string key = peer.SessionKey;
                if (_cooldownUntilByPeerKey.TryGetValue(key, out var until) && until > DateTime.UtcNow)
                    continue;

                if (!_tipByPeerKey.TryGetValue(key, out var tip))
                    continue;

                if (tip.Chainwork == 0 || tip.Chainwork <= localChainwork)
                    continue;

                if (best == null ||
                    tip.Chainwork > bestChainwork ||
                    (tip.Chainwork == bestChainwork && tip.Height > bestHeight))
                {
                    best = peer;
                    bestChainwork = tip.Chainwork;
                    bestHeight = tip.Height;
                }
            }

            return best;
        }

        private void PruneCooldown_NoLock()
        {
            if (_cooldownUntilByPeerKey.Count == 0)
                return;

            var now = DateTime.UtcNow;
            var stale = new List<string>();
            foreach (var kv in _cooldownUntilByPeerKey)
            {
                if (kv.Value <= now)
                    stale.Add(kv.Key);
            }

            for (int i = 0; i < stale.Count; i++)
                _cooldownUntilByPeerKey.Remove(stale[i]);
        }

        private static byte[] SafeCloneHash(byte[]? hash)
        {
            if (hash is not { Length: 32 })
                return Array.Empty<byte>();
            return (byte[])hash.Clone();
        }

        private static string ShortHash(byte[] hash)
        {
            if (hash is not { Length: 32 })
                return "unknown";
            var hex = Convert.ToHexString(hash).ToLowerInvariant();
            return hex[..16];
        }
    }
}
