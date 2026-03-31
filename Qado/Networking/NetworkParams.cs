namespace Qado.Networking
{
    public static class NetworkParams
    {
        public static class Mainnet
        {
            public const string Name = "mainnet";
            public const uint ChainId = 1u;
            public const byte NetworkId = 0x01;
            public const int P2PPort = 33333;
            public const int ApiPort = 18080;
            public const string GenesisHost = "212.227.21.183";
            public static readonly string[] BootstrapHosts =
            {
                GenesisHost,
                "2a02:2479:b2:4900::1",
                "82.165.121.88",
                "2a02:2479:7b:7a00::1"
            };
            public const string GenesisMinerHex = "e4449f3cce0e9b0db225e11cae2f97594725bee62255f2131310f9625fd26382";
            public const ulong GenesisTimestamp = 1773428400UL;
            public const ulong GenesisNonce = 0UL;
            public const string GenesisTargetHex = "0000000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
            public const string DataDirectoryName = "data";
            public const string DbFileName = "qado.db";
        }

        public static class Testnet
        {
            public const string Name = "testnet";
            public const uint ChainId = 10u;
            public const byte NetworkId = 0x10;
            public const int P2PPort = 34333;
            public const int ApiPort = 19080;
            public const string GenesisHost = "212.227.21.183";
            public static readonly string[] BootstrapHosts =
            {
                GenesisHost
            };
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
        public static readonly string[] BootstrapHosts = UseTestnet ? Testnet.BootstrapHosts : Mainnet.BootstrapHosts;
        public const string GenesisMinerHex = UseTestnet ? Testnet.GenesisMinerHex : Mainnet.GenesisMinerHex;
        public const ulong GenesisTimestamp = UseTestnet ? Testnet.GenesisTimestamp : Mainnet.GenesisTimestamp;
        public const ulong GenesisNonce = UseTestnet ? Testnet.GenesisNonce : Mainnet.GenesisNonce;
        public const string GenesisTargetHex = UseTestnet ? Testnet.GenesisTargetHex : Mainnet.GenesisTargetHex;
        public const string DataDirectoryName = UseTestnet ? Testnet.DataDirectoryName : Mainnet.DataDirectoryName;
        public const string DbFileName = UseTestnet ? Testnet.DbFileName : Mainnet.DbFileName;
    }
}
