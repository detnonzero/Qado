namespace Qado.Networking
{
    public static class NetworkParams
    {
        public static class Mainnet
        {
            public const string Name = "mainnet";
            public const uint ChainId = 1u;
            public const byte NetworkId = 0x01;
            public const int P2PPort = 34000;
            public const int ApiPort = 18080;
            public const string GenesisHost = "genesis.qado.org";
            public const string GenesisMinerHex = "C3B723B07AB5DA5A3D2F5538CABED0D4D5BF025BFD572AB3812F7A75858C0525";
            public const ulong GenesisTimestamp = 1771192800UL;
            public const uint GenesisNonce = 0u;
            public const string GenesisTargetHex = "0000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
            public const string DataDirectoryName = "data-mainnet";
            public const string DbFileName = "qado.db";
            public const string BlockLogFileName = "blocks.dat";
        }

        public static class Testnet
        {
            public const string Name = "testnet";
            public const uint ChainId = 1000u;
            public const byte NetworkId = 0x02;
            public const int P2PPort = 34000;
            public const int ApiPort = 18080;
            public const string GenesisHost = "genesis.qado.org";
            public const string GenesisMinerHex = Mainnet.GenesisMinerHex;
            public const ulong GenesisTimestamp = Mainnet.GenesisTimestamp;
            public const uint GenesisNonce = Mainnet.GenesisNonce;
            public const string GenesisTargetHex = Mainnet.GenesisTargetHex;
            public const string DataDirectoryName = "data-testnet";
            public const string DbFileName = Mainnet.DbFileName;
            public const string BlockLogFileName = Mainnet.BlockLogFileName;
        }

        // Hardcoded profile switch (change only this line).
        private const bool UseTestnet = true;

        // Active network profile (hardcoded, not runtime-switchable).
        public const string Name = UseTestnet ? Testnet.Name : Mainnet.Name;
        public const uint ChainId = UseTestnet ? Testnet.ChainId : Mainnet.ChainId;
        public const byte NetworkId = UseTestnet ? Testnet.NetworkId : Mainnet.NetworkId;
        public const int P2PPort = UseTestnet ? Testnet.P2PPort : Mainnet.P2PPort;
        public const int ApiPort = UseTestnet ? Testnet.ApiPort : Mainnet.ApiPort;
        public const string GenesisHost = UseTestnet ? Testnet.GenesisHost : Mainnet.GenesisHost;
        public const string GenesisMinerHex = UseTestnet ? Testnet.GenesisMinerHex : Mainnet.GenesisMinerHex;
        public const ulong GenesisTimestamp = UseTestnet ? Testnet.GenesisTimestamp : Mainnet.GenesisTimestamp;
        public const uint GenesisNonce = UseTestnet ? Testnet.GenesisNonce : Mainnet.GenesisNonce;
        public const string GenesisTargetHex = UseTestnet ? Testnet.GenesisTargetHex : Mainnet.GenesisTargetHex;
        public const string DataDirectoryName = UseTestnet ? Testnet.DataDirectoryName : Mainnet.DataDirectoryName;
        public const string DbFileName = UseTestnet ? Testnet.DbFileName : Mainnet.DbFileName;
        public const string BlockLogFileName = UseTestnet ? Testnet.BlockLogFileName : Mainnet.BlockLogFileName;
    }
}
