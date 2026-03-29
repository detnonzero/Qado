using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Qado.Logging;

namespace Qado.Networking
{
    public sealed class SmallNetPeerFlow
    {
        private readonly byte[] _nodeId;
        private readonly SmallNetPeerDirectory _peers;
        private readonly SmallNetCoordinator _coordinator;
        private readonly BlockSyncClient _blockSyncClient;
        private readonly Func<SmallNetLocalChainView> _getLocalChainView;
        private readonly Func<ulong, byte[]?> _getCanonicalHashAtHeight;
        private readonly Func<ulong, byte[]?> _getCanonicalPayloadAtHeight;
        private readonly Func<PeerSession, byte[], bool> _shouldPushBlockToPeer;
        private readonly Func<PeerSession, MsgType, byte[], CancellationToken, Task> _sendFrameAsync;
        private readonly Action<PeerSession, string> _dropSession;
        private readonly ILogSink? _log;

        public SmallNetPeerFlow(
            byte[] nodeId,
            SmallNetPeerDirectory peers,
            SmallNetCoordinator coordinator,
            BlockSyncClient blockSyncClient,
            Func<SmallNetLocalChainView> getLocalChainView,
            Func<ulong, byte[]?> getCanonicalHashAtHeight,
            Func<ulong, byte[]?> getCanonicalPayloadAtHeight,
            Func<PeerSession, byte[], bool> shouldPushBlockToPeer,
            Func<PeerSession, MsgType, byte[], CancellationToken, Task> sendFrameAsync,
            Action<PeerSession, string> dropSession,
            ILogSink? log = null)
        {
            _nodeId = nodeId is { Length: SmallNetSyncProtocol.NodeIdBytes }
                ? (byte[])nodeId.Clone()
                : throw new ArgumentException("node id must be 32 bytes", nameof(nodeId));
            _peers = peers ?? throw new ArgumentNullException(nameof(peers));
            _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
            _blockSyncClient = blockSyncClient ?? throw new ArgumentNullException(nameof(blockSyncClient));
            _getLocalChainView = getLocalChainView ?? throw new ArgumentNullException(nameof(getLocalChainView));
            _getCanonicalHashAtHeight = getCanonicalHashAtHeight ?? throw new ArgumentNullException(nameof(getCanonicalHashAtHeight));
            _getCanonicalPayloadAtHeight = getCanonicalPayloadAtHeight ?? throw new ArgumentNullException(nameof(getCanonicalPayloadAtHeight));
            _shouldPushBlockToPeer = shouldPushBlockToPeer ?? throw new ArgumentNullException(nameof(shouldPushBlockToPeer));
            _sendFrameAsync = sendFrameAsync ?? throw new ArgumentNullException(nameof(sendFrameAsync));
            _dropSession = dropSession ?? throw new ArgumentNullException(nameof(dropSession));
            _log = log;
        }

        public byte[] BuildLocalHelloPayload()
            => BuildHelloPayload(_getLocalChainView());

        public byte[] BuildLocalTipStatePayload()
            => BuildTipStatePayload(_getLocalChainView());

        public Task SendLocalHelloAsync(PeerSession peer, CancellationToken ct)
            => _sendFrameAsync(peer, MsgType.Hello, BuildLocalHelloPayload(), ct);

        public Task SendLocalTipStateAsync(PeerSession peer, CancellationToken ct)
            => _sendFrameAsync(peer, MsgType.TipState, BuildLocalTipStatePayload(), ct);

        public async Task HandleHelloAsync(byte[] payload, PeerSession peer, CancellationToken ct)
        {
            if (!SmallNetSyncProtocol.TryParseHello(payload, out var frame))
            {
                _log?.Warn("P2P", $"Invalid Hello ignored from {EndpointLogFormatter.FormatEndpoint(peer.RemoteEndpoint)}.");
                return;
            }

            if (frame.ProtocolVersion != SmallNetSyncProtocol.CurrentProtocolVersion)
            {
                _dropSession(peer, $"smallnet protocol mismatch: remote={frame.ProtocolVersion}, local={SmallNetSyncProtocol.CurrentProtocolVersion}");
                return;
            }

            if (frame.NodeId is { Length: 32 } && BytesEqual32(frame.NodeId, _nodeId))
            {
                _dropSession(peer, "smallnet hello self-loop");
                return;
            }

            var peerState = _peers.UpdateHello(peer.SessionKey, frame, DateTime.UtcNow);
            var localView = _getLocalChainView();
            await _blockSyncClient.OnTipStateAsync(
                peer,
                new SmallNetTipStateFrame(
                    frame.TipHeight,
                    frame.TipHash,
                    frame.TipChainwork,
                    frame.StateDigest,
                    Array.Empty<byte[]>()),
                ct).ConfigureAwait(false);
            await SendLocalTipStateAsync(peer, ct).ConfigureAwait(false);
            await ExecuteActionsAsync(peer, _coordinator.OnPeerTipState(peerState, localView), ct).ConfigureAwait(false);
        }

        public async Task HandleTipStateAsync(byte[] payload, PeerSession peer, CancellationToken ct)
        {
            if (!SmallNetSyncProtocol.TryParseTipState(payload, out var frame))
            {
                _log?.Warn("P2P", $"Invalid TipState ignored from {EndpointLogFormatter.FormatEndpoint(peer.RemoteEndpoint)}.");
                return;
            }

            var peerState = _peers.UpdateTipState(peer.SessionKey, frame, DateTime.UtcNow);
            var localView = _getLocalChainView();
            await _blockSyncClient.OnTipStateAsync(peer, frame, ct).ConfigureAwait(false);
            await TryPushCanonicalTailToLaggingPeerAsync(peer, frame, localView, ct).ConfigureAwait(false);
            await ExecuteActionsAsync(peer, _coordinator.OnPeerTipState(peerState, localView), ct).ConfigureAwait(false);
        }

        public async Task HandleBlocksBatchStartAsync(byte[] payload, PeerSession peer, CancellationToken ct)
        {
            if (!SmallNetSyncProtocol.TryParseBlocksBatchStart(payload, out var frame))
                return;

            await _blockSyncClient.OnBlocksBatchStartAsync(peer, frame, ct).ConfigureAwait(false);
        }

        public async Task HandleBlocksBatchEndAsync(byte[] payload, PeerSession peer, CancellationToken ct)
        {
            if (!SmallNetSyncProtocol.TryParseBlocksBatchEnd(payload, out var frame))
                return;

            await _blockSyncClient.OnBlocksBatchEndAsync(peer, frame, ct).ConfigureAwait(false);
        }

        private async Task ExecuteActionsAsync(PeerSession peer, IReadOnlyList<SmallNetAction> actions, CancellationToken ct)
        {
            if (peer == null || actions == null || actions.Count == 0)
                return;

            for (int i = 0; i < actions.Count; i++)
            {
                var action = actions[i];
                if (action.Payload == null)
                    continue;

                switch (action.Kind)
                {
                    case SmallNetActionKind.RequestAncestorPack:
                        await _sendFrameAsync(peer, action.MessageType, action.Payload, ct).ConfigureAwait(false);
                        break;

                    case SmallNetActionKind.RequestBlocksFrom:
                    case SmallNetActionKind.RequestBlocksByLocator:
                        if (_blockSyncClient.IsActive)
                            break;

                        await _sendFrameAsync(peer, action.MessageType, action.Payload, ct).ConfigureAwait(false);
                        break;

                    case SmallNetActionKind.None:
                    default:
                        break;
                }
            }
        }

        private byte[] BuildHelloPayload(SmallNetLocalChainView localView)
            => SmallNetSyncProtocol.BuildHello(new SmallNetHelloFrame(
                SmallNetSyncProtocol.CurrentProtocolVersion,
                (byte[])_nodeId.Clone(),
                (byte[])localView.TipHash.Clone(),
                localView.Height,
                localView.Chainwork,
                localView.StateDigest is { Length: > 0 } ? (byte[])localView.StateDigest.Clone() : null));

        private static byte[] BuildTipStatePayload(SmallNetLocalChainView localView)
            => SmallNetSyncProtocol.BuildTipState(new SmallNetTipStateFrame(
                localView.Height,
                (byte[])localView.TipHash.Clone(),
                localView.Chainwork,
                localView.StateDigest is { Length: > 0 } ? (byte[])localView.StateDigest.Clone() : null,
                localView.RecentCanonical));

        private async Task TryPushCanonicalTailToLaggingPeerAsync(
            PeerSession peer,
            SmallNetTipStateFrame remoteTip,
            SmallNetLocalChainView localView,
            CancellationToken ct)
        {
            if (peer == null)
                return;
            if (remoteTip.TipHash is not { Length: 32 } || IsZero32(remoteTip.TipHash))
                return;
            if (localView.TipHash is not { Length: 32 } || IsZero32(localView.TipHash))
                return;
            if (remoteTip.Height >= localView.Height)
                return;

            ulong gap = localView.Height - remoteTip.Height;
            if (gap == 0 || gap > 8UL)
                return;

            var remoteCanonicalHash = _getCanonicalHashAtHeight(remoteTip.Height);
            if (remoteCanonicalHash is not { Length: 32 } || !BytesEqual32(remoteCanonicalHash, remoteTip.TipHash))
                return;

            var payloads = new List<byte[]>((int)gap);
            for (ulong height = remoteTip.Height + 1UL; height <= localView.Height; height++)
            {
                var hash = _getCanonicalHashAtHeight(height);
                if (hash is not { Length: 32 } || !_shouldPushBlockToPeer(peer, hash))
                    continue;

                var payload = _getCanonicalPayloadAtHeight(height);
                if (payload == null || payload.Length == 0)
                    return;

                payloads.Add(payload);
            }

            if (payloads.Count == 0)
                return;

            for (int i = 0; i < payloads.Count; i++)
            {
                if (ct.IsCancellationRequested)
                    return;

                await _sendFrameAsync(peer, MsgType.Block, payloads[i], ct).ConfigureAwait(false);
            }

            await SendLocalTipStateAsync(peer, ct).ConfigureAwait(false);
            _log?.Info(
                "P2P",
                $"Pushed canonical tail to lagging peer {EndpointLogFormatter.FormatEndpoint(peer.RemoteEndpoint)}: startHeight={remoteTip.Height + 1UL}, blocks={payloads.Count}");
        }

        private static bool IsZero32(byte[]? hash)
        {
            if (hash is not { Length: 32 })
                return true;

            for (int i = 0; i < 32; i++)
            {
                if (hash[i] != 0)
                    return false;
            }

            return true;
        }

        private static bool BytesEqual32(byte[] left, byte[] right)
        {
            if (left.Length != 32 || right.Length != 32)
                return false;

            for (int i = 0; i < 32; i++)
            {
                if (left[i] != right[i])
                    return false;
            }

            return true;
        }
    }
}
