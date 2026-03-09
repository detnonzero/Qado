using System;
using System.Collections.Generic;

namespace Qado.Networking
{
    public sealed class SmallNetCoordinator
    {
        public IReadOnlyList<SmallNetAction> OnPeerTipState(
            SmallNetPeerState peerState,
            SmallNetLocalChainView localView,
            int smallGapThreshold = 8)
        {
            if (peerState == null)
                throw new ArgumentNullException(nameof(peerState));

            var decision = peerState.ClassifyGap(localView, smallGapThreshold);
            return BuildActionsForGap(peerState, decision, localView);
        }

        private static IReadOnlyList<SmallNetAction> BuildActionsForGap(
            SmallNetPeerState peerState,
            SmallNetGapDecision decision,
            SmallNetLocalChainView localView)
        {
            switch (decision.Kind)
            {
                case SmallNetGapKind.InSync:
                    return Array.Empty<SmallNetAction>();

                case SmallNetGapKind.LocalAheadSmall:
                    return Array.Empty<SmallNetAction>();

                case SmallNetGapKind.LocalAheadLarge:
                    return Array.Empty<SmallNetAction>();

                case SmallNetGapKind.RemoteAheadSmall:
                    return new[]
                    {
                        new SmallNetAction(
                            SmallNetActionKind.RequestBlocksFrom,
                            MsgType.GetBlocksFrom,
                            BlockSyncProtocol.BuildGetBlocksFrom(localView.TipHash, Math.Max(1, decision.BlockGap)))
                    };

                case SmallNetGapKind.RemoteAheadLarge:
                    return new[]
                    {
                        new SmallNetAction(
                            SmallNetActionKind.RequestBlocksByLocator,
                            MsgType.GetBlocksByLocator,
                            BlockSyncProtocol.BuildGetBlocksByLocator(localView.RecentCanonical, BlockSyncProtocol.BatchMaxBlocks))
                    };

                case SmallNetGapKind.CompetingTip:
                    return new[]
                    {
                        new SmallNetAction(
                            SmallNetActionKind.RequestAncestorPack,
                            MsgType.GetAncestorPack,
                            SmallNetSyncProtocol.BuildGetAncestorPack(peerState.TipHash))
                    };

                default:
                    return Array.Empty<SmallNetAction>();
            }
        }
    }
}
