using System;

namespace Qado.Networking
{
    [Flags]
    public enum HandshakeCapabilities : uint
    {
        None = 0,
        DirectTransactions = 1u << 0,
        DirectBlocks = 1u << 1,
        BlocksBatchData = 1u << 2,
        PeerExchangeIpv6 = 1u << 3,
        DualStack = 1u << 4,
        LegacyChunkSync = 1u << 5,
        ExtendedSyncWindow = 1u << 6,
        SyncWindowPreview = 1u << 7
    }
}
