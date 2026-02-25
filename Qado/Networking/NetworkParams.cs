namespace Qado.Networking
{
    public static class NetworkParams
    {
        public static class Mainnet
        {
            public const string Name = "mainnet";
            public const uint ChainId = 1u;
            public const byte NetworkId = 0x01;
            public const int P2PPort = 34001;
            public const int ApiPort = 18080;
            public const string GenesisHost = "82.165.63.4";
            public const string GenesisMinerHex = "6A37C7D48887658C233C80ED6D1F8A28753E1F9E209867ABE9278D714033BA80";
            public const ulong GenesisTimestamp = 1772055600UL;
            public const uint GenesisNonce = 0u;
            public const string GenesisTargetHex = "00000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
            public const string DataDirectoryName = "data-mainnet";
            public const string DbFileName = "qado.db";
            public const string BlockLogFileName = "blocks.dat";
        }

        public static class Testnet
        {
            public const string Name = "testnet";
            public const uint ChainId = 1001u;
            public const byte NetworkId = 0x10;
            public const int P2PPort = 34001;
            public const int ApiPort = 19080;
            public const string GenesisHost = "82.165.63.4";
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
