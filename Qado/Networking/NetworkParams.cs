namespace Qado.Networking
{
    public static class NetworkParams
    {
        public static class Mainnet
        {
            public const string Name = "mainnet";
            public const uint ChainId = 11u;
            public const byte NetworkId = 0x30;
            public const int P2PPort = 33000;
            public const int ApiPort = 18080;
            public const string GenesisHost = "212.227.21.183";
            public const string GenesisMinerHex = "3c1e6ea3fdfcf17621b8831031433a29400519899c7d9576c3d00d1e527c33ab";
            public const ulong GenesisTimestamp = 1772757000UL;
            public const ulong GenesisNonce = 0UL;
            public const string GenesisTargetHex = "0000000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
            public const string DataDirectoryName = "data";
            public const string DbFileName = "qado.db";
        }

        public static class Testnet
        {
            public const string Name = "testnet";
            public const uint ChainId = 2000u;
            public const byte NetworkId = 0x20;
            public const int P2PPort = 33001;
            public const int ApiPort = 19080;
            public const string GenesisHost = "212.227.21.183";
            public const string GenesisMinerHex = Mainnet.GenesisMinerHex;
            public const ulong GenesisTimestamp = Mainnet.GenesisTimestamp;
            public const ulong GenesisNonce = Mainnet.GenesisNonce;
            public const string GenesisTargetHex = Mainnet.GenesisTargetHex;
            public const string DataDirectoryName = "data";
            public const string DbFileName = Mainnet.DbFileName;
        }

        // Hardcoded profile switch
        private const bool UseTestnet = false;

        // Active network profile (hardcoded, not runtime-switchable).
        public const string Name = UseTestnet ? Testnet.Name : Mainnet.Name;
        public const uint ChainId = UseTestnet ? Testnet.ChainId : Mainnet.ChainId;
        public const byte NetworkId = UseTestnet ? Testnet.NetworkId : Mainnet.NetworkId;
        public const int P2PPort = UseTestnet ? Testnet.P2PPort : Mainnet.P2PPort;
        public const int ApiPort = UseTestnet ? Testnet.ApiPort : Mainnet.ApiPort;
        public const string GenesisHost = UseTestnet ? Testnet.GenesisHost : Mainnet.GenesisHost;
        public const string GenesisMinerHex = UseTestnet ? Testnet.GenesisMinerHex : Mainnet.GenesisMinerHex;
        public const ulong GenesisTimestamp = UseTestnet ? Testnet.GenesisTimestamp : Mainnet.GenesisTimestamp;
        public const ulong GenesisNonce = UseTestnet ? Testnet.GenesisNonce : Mainnet.GenesisNonce;
        public const string GenesisTargetHex = UseTestnet ? Testnet.GenesisTargetHex : Mainnet.GenesisTargetHex;
        public const string DataDirectoryName = UseTestnet ? Testnet.DataDirectoryName : Mainnet.DataDirectoryName;
        public const string DbFileName = UseTestnet ? Testnet.DbFileName : Mainnet.DbFileName;
    }
}
