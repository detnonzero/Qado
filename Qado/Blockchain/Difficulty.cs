using System;
using System.Numerics;

namespace Qado.Blockchain
{
    public static class Difficulty
    {
        public const int HashSize = 32;

        private static readonly byte[] _powLimit =
            Convert.FromHexString("0000" + new string('F', 60)); // 32-byte big-endian target

        private static readonly byte[] _minTarget =
            Convert.FromHexString("0000000000000000000000000000000000000000000000000000000000000001");

        public static ReadOnlySpan<byte> PowLimit => _powLimit;
        public static ReadOnlySpan<byte> MinTarget => _minTarget;

        private static readonly BigInteger PowLimitInt = new(_powLimit, isUnsigned: true, isBigEndian: true);
        private static readonly BigInteger MinTargetInt = BigInteger.One;

        static Difficulty()
        {
            if (_powLimit.Length != HashSize) throw new InvalidOperationException("PowLimit must be 32 bytes.");
            if (_minTarget.Length != HashSize) throw new InvalidOperationException("MinTarget must be 32 bytes.");
            if (CompareBE(_minTarget, _powLimit) > 0) throw new InvalidOperationException("MinTarget must be <= PowLimit.");
        }

        public static bool Meets(byte[] hash, byte[] target)
        {
            if (hash is not { Length: HashSize }) return false;
            if (target is not { Length: HashSize }) return false;

            if (IsZero32(target))
                return false;

            ReadOnlySpan<byte> effective =
                CompareBE(target, _powLimit) > 0 ? _powLimit : target;

            return CompareBE(hash, effective) <= 0;
        }

        public static bool IsValidTarget(byte[] target)
        {
            if (target is not { Length: HashSize }) return false;
            if (IsZero32(target)) return false;
            if (CompareBE(target, _minTarget) < 0) return false;
            if (CompareBE(target, _powLimit) > 0) return false;
            return true;
        }

        public static byte[] ClampTarget(byte[] target)
        {
            if (target is not { Length: HashSize })
                throw new ArgumentException("Target must be 32 bytes.", nameof(target));

            if (IsZero32(target))
                return (byte[])_minTarget.Clone();

            if (CompareBE(target, _minTarget) < 0)
                return (byte[])_minTarget.Clone();

            if (CompareBE(target, _powLimit) > 0)
                return (byte[])_powLimit.Clone();

            return (byte[])target.Clone();
        }

        public static void ClampTargetTo(ReadOnlySpan<byte> target, Span<byte> dst)
        {
            if (target.Length != HashSize) throw new ArgumentException("Target must be 32 bytes.", nameof(target));
            if (dst.Length != HashSize) throw new ArgumentException("dst must be 32 bytes.", nameof(dst));

            if (IsZero32(target))
            {
                _minTarget.AsSpan().CopyTo(dst);
                return;
            }

            if (CompareBE(target, _minTarget) < 0)
            {
                _minTarget.AsSpan().CopyTo(dst);
                return;
            }

            if (CompareBE(target, _powLimit) > 0)
            {
                _powLimit.AsSpan().CopyTo(dst);
                return;
            }

            target.CopyTo(dst);
        }

        public static BigInteger TargetToDifficulty(byte[] target)
        {
            var tClamped = ClampTarget(target);
            var t = new BigInteger(tClamped, isUnsigned: true, isBigEndian: true);

            if (t <= 0) return BigInteger.One;

            var d = PowLimitInt / t;
            return d <= 0 ? BigInteger.One : d;
        }

        public static byte[] DifficultyToTarget(BigInteger difficulty)
        {
            if (difficulty <= 1)
                return (byte[])_powLimit.Clone();

            BigInteger t = PowLimitInt / difficulty;
            if (t <= 0) t = MinTargetInt;

            var bytes = BigIntegerToTarget32(t);
            return ClampTarget(bytes);
        }

        public static BigInteger TargetToBigInteger(byte[] target)
            => new BigInteger(target, isUnsigned: true, isBigEndian: true);

        public static byte[] BigIntegerToTarget32(BigInteger v)
        {
            if (v <= 0) return (byte[])_minTarget.Clone();

            var raw = v.ToByteArray(isUnsigned: true, isBigEndian: true);

            if (raw.Length > HashSize)
                raw = raw.AsSpan(raw.Length - HashSize, HashSize).ToArray();

            var out32 = new byte[HashSize];
            Buffer.BlockCopy(raw, 0, out32, HashSize - raw.Length, raw.Length);
            return out32;
        }

        private static int CompareBE(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
        {
            int n = a.Length < b.Length ? a.Length : b.Length;
            for (int i = 0; i < n; i++)
            {
                int d = a[i].CompareTo(b[i]);
                if (d != 0) return d;
            }
            return a.Length.CompareTo(b.Length);
        }

        private static bool IsZero32(ReadOnlySpan<byte> a)
        {
            if (a.Length != HashSize) return false;
            for (int i = 0; i < HashSize; i++)
                if (a[i] != 0) return false;
            return true;
        }
    }
}

