using System;
using System.Buffers.Binary;

namespace Qado.Utils
{
    public static class U128
    {
        public static void WriteBE(Span<byte> dst, UInt128 value)
        {
            if (dst.Length < 16) throw new ArgumentException("dst too small");
            ulong hi = (ulong)(value >> 64);
            ulong lo = (ulong)(value & ulong.MaxValue);
            BinaryPrimitives.WriteUInt64BigEndian(dst[..8], hi);
            BinaryPrimitives.WriteUInt64BigEndian(dst.Slice(8, 8), lo);
        }

        public static UInt128 ReadBE(ReadOnlySpan<byte> src)
        {
            if (src.Length < 16) throw new ArgumentException("src too small");
            ulong hi = BinaryPrimitives.ReadUInt64BigEndian(src[..8]);
            ulong lo = BinaryPrimitives.ReadUInt64BigEndian(src.Slice(8, 8));
            return ((UInt128)hi << 64) | lo;
        }

        [Obsolete("Use WriteBE")]
        public static void WriteLE(Span<byte> dst, UInt128 value)
        {
            if (dst.Length < 16) throw new ArgumentException("dst too small");
            ulong lo = (ulong)(value & ulong.MaxValue);
            ulong hi = (ulong)(value >> 64);
            BinaryPrimitives.WriteUInt64LittleEndian(dst[..8], lo);
            BinaryPrimitives.WriteUInt64LittleEndian(dst.Slice(8, 8), hi);
        }

        [Obsolete("Use ReadBE")]
        public static UInt128 ReadLE(ReadOnlySpan<byte> src)
        {
            if (src.Length < 16) throw new ArgumentException("src too small");
            ulong lo = BinaryPrimitives.ReadUInt64LittleEndian(src[..8]);
            ulong hi = BinaryPrimitives.ReadUInt64LittleEndian(src.Slice(8, 8));
            return ((UInt128)hi << 64) | lo;
        }
    }
}

