using System;
using System.Collections.Generic;

namespace Qado.Networking
{
    public enum SmallNetGapKind
    {
        Unknown = 0,
        InSync = 1,
        LocalAheadSmall = 2,
        LocalAheadLarge = 3,
        RemoteAheadSmall = 4,
        RemoteAheadLarge = 5,
        CompetingTip = 6
    }

    public enum SmallNetActionKind
    {
        None = 0,
        RequestAncestorPack = 1,
        RequestBlocksFrom = 2,
        RequestBlocksByLocator = 3
    }

    public readonly record struct SmallNetLocalChainView(
        ulong Height,
        byte[] TipHash,
        UInt128 Chainwork,
        IReadOnlyList<byte[]> RecentCanonical,
        byte[]? StateDigest);

    public readonly record struct SmallNetGapDecision(
        SmallNetGapKind Kind,
        int BlockGap,
        bool SameTip,
        bool LikelyFork);

    public readonly record struct SmallNetAction(
        SmallNetActionKind Kind,
        MsgType MessageType,
        byte[] Payload);
}
