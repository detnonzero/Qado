namespace Qado.Networking
{
    public enum MsgType : byte
    {
        Handshake = 1,

        InvTx = 2,
        Tx = 3,
        InvBlock = 4,
        Block = 5,

        GetBlock = 6,
        BlockAt = 7,
        GetTip = 8,
        Tip = 9,

        GetPeers = 10,
        Peers = 11,

        Ping = 12,
        Pong = 13,

        GetTx = 14
    }
}

