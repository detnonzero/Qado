namespace Qado.Networking
{
    public enum MsgType : byte
    {
        Handshake = 1,

        InvTx = 2,
        Tx = 3,
        Block = 5,

        GetBlock = 6,
        BlockAt = 7,

        GetPeers = 10,
        Peers = 11,

        Ping = 12,
        Pong = 13,

        GetTx = 14,

        Headers = 16,
        GetHeaders = 17,

        GetBlocksByLocator = 18,
        GetBlocksFrom = 19,
        NoCommonAncestor = 23,

        Hello = 24,
        TipState = 25,
        GetAncestorPack = 26,
        AncestorPack = 27,
        BlocksBatchStart = 28,
        BlocksChunk = 29,
        BlocksBatchEnd = 30
    }
}

