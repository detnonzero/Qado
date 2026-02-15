using System;
using System.Buffers.Binary;
using System.Numerics;

namespace Qado.Utils
{
    public static class ChainworkUtil
    {
        public static UInt128 IncrementFromTarget(byte[] target)
        {
            if (target is not { Length: 32 })
                return 1;

            BigInteger diff = Qado.Blockchain.Difficulty.TargetToDifficulty(target);
            if (diff <= 0) return 1;

            BigInteger max = (BigInteger.One << 128) - 1;
            if (diff >= max) return UInt128.MaxValue;

            byte[] raw = diff.ToByteArray(isUnsigned: true, isBigEndian: true);

            Span<byte> buf = stackalloc byte[16];
            buf.Clear();

            raw.AsSpan().CopyTo(buf.Slice(16 - raw.Length));

            ulong hi = BinaryPrimitives.ReadUInt64BigEndian(buf.Slice(0, 8));
            ulong lo = BinaryPrimitives.ReadUInt64BigEndian(buf.Slice(8, 8));

            UInt128 v = ((UInt128)hi << 64) | lo;
            return v == 0 ? 1 : v;
        }

        public static UInt128 Add(UInt128 parentWork, UInt128 delta) => parentWork + delta;
    }
}

