using System;
using System.Numerics;
using Qado.Storage;

namespace Qado.Blockchain
{
    public static class DifficultyCalculator
    {
        public const int TargetBlockTimeSeconds = 60;

        private const int WindowSolveTimes = 60;

        private const int MaxAdjustFactor = 2;

        public static byte[] GetTargetForHeight(ulong height, Func<ulong, Block?>? getBlock = null)
        {
            getBlock ??= BlockStore.GetBlockByHeight;
            return GetNextTarget(height, getBlock);
        }

        public static byte[] GetNextTarget(ulong nextHeight, Func<ulong, Block?> getBlock)
        {
            if (getBlock is null) throw new ArgumentNullException(nameof(getBlock));

            if (nextHeight == 0)
                return Difficulty.PowLimit.ToArray();

            ulong end = nextHeight - 1;

            var last = getBlock(end);
            if (last?.Header is null)
                return Difficulty.PowLimit.ToArray();

            var lastTarget = Difficulty.ClampTarget(last.Header.Target);
            BigInteger lastDiff = Difficulty.TargetToDifficulty(lastTarget);

            int N = (int)Math.Min((ulong)WindowSolveTimes, end);
            if (N < 3)
                return lastTarget;

            ulong start = end - (ulong)N;

            var b0 = getBlock(start);
            if (b0?.Header is null)
                return lastTarget;

            BigInteger sumWST = BigInteger.Zero; // weighted solve times
            BigInteger sumDiff = BigInteger.Zero;

            ulong prevTs = b0.Header.Timestamp;

            int i = 0; // sample weight 1..N
            for (ulong h = start + 1; h <= end; h++)
            {
                var b = getBlock(h);
                if (b?.Header is null) break;

                i++;

                long solvetime = unchecked((long)b.Header.Timestamp - (long)prevTs);
                prevTs = b.Header.Timestamp;

                if (solvetime <= 0)
                    solvetime = 1;

                sumWST += (BigInteger)i * solvetime;

                var t = Difficulty.ClampTarget(b.Header.Target);
                var d = Difficulty.TargetToDifficulty(t);
                sumDiff += d;
            }

            if (i < 3)
                return lastTarget;

            if (sumWST <= 0 || sumDiff <= 0)
                return lastTarget;

            BigInteger k = (BigInteger)i * (i + 1) / 2;

            BigInteger num = sumDiff * TargetBlockTimeSeconds * k;
            BigInteger den = sumWST * i;
            if (den <= 0)
                return lastTarget;

            BigInteger nextDiff = num / den;

            if (nextDiff <= 0)
                nextDiff = BigInteger.One;

            BigInteger maxUp = lastDiff * MaxAdjustFactor;

            BigInteger maxDown = lastDiff / MaxAdjustFactor;
            if (maxDown <= 0) maxDown = BigInteger.One;

            if (nextDiff > maxUp) nextDiff = maxUp;
            if (nextDiff < maxDown) nextDiff = maxDown;

            return Difficulty.DifficultyToTarget(nextDiff);
        }
    }
}

