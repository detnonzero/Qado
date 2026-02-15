namespace Qado.Blockchain
{
    public static class RewardCalculator
    {
        public const ulong Coin = 1_000_000_000UL;

        public static ulong GetBlockSubsidy(ulong height)
        {
            if (height == 0)
                return 0UL;

            if (height <= 1_000_000UL) return 20UL * Coin;
            if (height <= 2_000_000UL) return 10UL * Coin;
            if (height <= 3_000_000UL) return 5UL * Coin;
            if (height <= 4_000_000UL) return 2_250_000_000UL; // 2.25 QADO
            if (height <= 5_000_000UL) return 1_250_000_000UL; // 1.25 QADO
            return 1UL * Coin;
        }
    }
}

