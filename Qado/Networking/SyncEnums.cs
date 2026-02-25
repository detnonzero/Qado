namespace Qado.Networking
{
    public enum HeaderSyncState
    {
        Idle = 0,
        SelectingPeer = 1,
        WaitingHeaders = 2,
        Completed = 3
    }

    public enum BadChainReason
    {
        None = 0,
        BlockStatelessInvalid = 1,
        BlockStateInvalid = 2,
        StoredAsInvalid = 3
    }

    public enum BlockArrivalKind
    {
        Unknown = 0,
        Requested = 1,
        Unsolicited = 2
    }

    public enum BlockSyncItemState
    {
        Queued = 0,
        Requested = 1,
        HaveBlock = 2,
        Validated = 3,
        Invalid = 4
    }
}
