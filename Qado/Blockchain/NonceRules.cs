using System;

namespace Qado.Blockchain
{
    public static class NonceRules
    {
        public const ulong MaxAccountNonce = ulong.MaxValue;

        public static bool IsUsableTransactionNonce(ulong txNonce)
            => txNonce >= 1UL;

        public static bool TryGetExpectedNextNonce(ulong senderNonce, out ulong expectedNonce)
        {
            expectedNonce = 0;

            if (senderNonce == ulong.MaxValue)
                return false;

            expectedNonce = checked(senderNonce + 1UL);
            return true;
        }
    }
}
