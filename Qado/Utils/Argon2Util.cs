using System;
using System.Security.Cryptography;
using Konscious.Security.Cryptography;

namespace Qado.Utils
{
    public static class Argon2Util
    {
        public static byte[] ComputeHash(byte[] input, int memoryKb = 4, int iterations = 1, int parallelism = 1)
        {
            if (input == null) throw new ArgumentNullException(nameof(input));
            if (memoryKb <= 0) throw new ArgumentOutOfRangeException(nameof(memoryKb));
            if (iterations <= 0) throw new ArgumentOutOfRangeException(nameof(iterations));
            if (parallelism <= 0) throw new ArgumentOutOfRangeException(nameof(parallelism));

            byte[] salt = SHA256.HashData(input);

            using var hasher = new Argon2id(input)
            {
                Salt = salt,
                DegreeOfParallelism = parallelism,
                MemorySize = memoryKb,
                Iterations = iterations
            };

            return hasher.GetBytes(32);
        }

        public static bool MeetsTarget(byte[] hash, byte[] target)
            => Qado.Blockchain.Difficulty.Meets(hash, target);

        public static bool MeetsTarget(byte[] hash, ulong prefixTarget)
            => MeetsTarget(hash, PrefixTargetToTarget32(prefixTarget));

        public static bool MeetsDifficulty(byte[] hash, uint prefixTarget)
            => MeetsTarget(hash, (ulong)prefixTarget);

        private static byte[] PrefixTargetToTarget32(ulong prefixTarget)
        {
            var t = new byte[32];
            t[0] = (byte)(prefixTarget >> 56);
            t[1] = (byte)(prefixTarget >> 48);
            t[2] = (byte)(prefixTarget >> 40);
            t[3] = (byte)(prefixTarget >> 32);
            t[4] = (byte)(prefixTarget >> 24);
            t[5] = (byte)(prefixTarget >> 16);
            t[6] = (byte)(prefixTarget >> 8);
            t[7] = (byte)(prefixTarget);
            for (int i = 8; i < 32; i++) t[i] = 0xFF;
            return t;
        }
    }
}

