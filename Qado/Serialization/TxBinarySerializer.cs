using System;
using System.Buffers.Binary;
using Qado.Blockchain;

namespace Qado.Serialization
{
    public static class TxBinarySerializer
    {
        private const byte FormatVersion = 2;

        public const int MaxSignatureBytes = 1024;          // upper bound against oversized signatures
        public const int MaxTxSize = ConsensusRules.MaxTransactionSizeBytes;

        public static int GetSize(Transaction tx)
        {
            if (tx == null) throw new ArgumentNullException(nameof(tx));

            int sigLen = tx.Signature?.Length ?? 0;
            if (sigLen < 0 || sigLen > ushort.MaxValue) throw new ArgumentException("Signature too long.", nameof(tx));
            if (sigLen > MaxSignatureBytes) throw new ArgumentException($"Signature too long ({sigLen} > {MaxSignatureBytes}).", nameof(tx));

            int size = 1 + 4 + 32 + 32 + 16 + 16 + 8 + 2 + sigLen;
            if (size > MaxTxSize) throw new ArgumentException($"Tx too large: {size} bytes (max {MaxTxSize}).", nameof(tx));
            return size;
        }

        public static int Write(Span<byte> dst, Transaction tx)
        {
            if (tx == null) throw new ArgumentNullException(nameof(tx));
            if (tx.Sender is not { Length: 32 }) throw new ArgumentException("Sender must be 32 bytes.", nameof(tx));
            if (tx.Recipient is not { Length: 32 }) throw new ArgumentException("Recipient must be 32 bytes.", nameof(tx));

            var sig = tx.Signature ?? Array.Empty<byte>();
            if (sig.Length > ushort.MaxValue) throw new ArgumentException("Signature too long.", nameof(tx));
            if (sig.Length > MaxSignatureBytes) throw new ArgumentException($"Signature too long ({sig.Length} > {MaxSignatureBytes}).", nameof(tx));

            int need = 1 + 4 + 32 + 32 + 16 + 16 + 8 + 2 + sig.Length;
            if (need > MaxTxSize) throw new ArgumentException($"Tx too large: {need} bytes (max {MaxTxSize}).", nameof(tx));
            if (dst.Length < need) throw new ArgumentException("Destination buffer too small.", nameof(dst));

            int o = 0;

            dst[o++] = FormatVersion;

            BinaryPrimitives.WriteUInt32BigEndian(dst.Slice(o, 4), tx.ChainId);
            o += 4;

            tx.Sender.AsSpan().CopyTo(dst.Slice(o, 32)); o += 32;
            tx.Recipient.AsSpan().CopyTo(dst.Slice(o, 32)); o += 32;

            WriteU64AsU128BE(dst.Slice(o, 16), tx.Amount); o += 16;
            WriteU64AsU128BE(dst.Slice(o, 16), tx.Fee); o += 16;

            BinaryPrimitives.WriteUInt64BigEndian(dst.Slice(o, 8), tx.TxNonce);
            o += 8;

            BinaryPrimitives.WriteUInt16BigEndian(dst.Slice(o, 2), (ushort)sig.Length);
            o += 2;

            if (sig.Length > 0)
            {
                sig.AsSpan().CopyTo(dst.Slice(o, sig.Length));
                o += sig.Length;
            }

            if (o != need)
                throw new InvalidOperationException($"TxBinarySerializer.Write wrote {o} bytes, expected {need}.");

            return o;
        }

        public static Transaction Read(ReadOnlySpan<byte> src)
        {
            if (src.Length < 1) throw new ArgumentException("Buffer too small.", nameof(src));
            if (src.Length > MaxTxSize) throw new ArgumentException($"Tx buffer too large: {src.Length} bytes (max {MaxTxSize}).", nameof(src));

            int o = 0;

            byte ver = src[o++];
            if (ver != FormatVersion) throw new InvalidOperationException($"Unsupported tx version {ver}");

            const int fixedAfterVer = 4 + 32 + 32 + 16 + 16 + 8 + 2;
            if (src.Length < o + fixedAfterVer) throw new ArgumentException("Buffer too small.", nameof(src));

            uint chainId = BinaryPrimitives.ReadUInt32BigEndian(src.Slice(o, 4)); o += 4;

            byte[] sender = src.Slice(o, 32).ToArray(); o += 32;
            byte[] recipient = src.Slice(o, 32).ToArray(); o += 32;

            ulong amount = ReadU64FromU128BE(src.Slice(o, 16)); o += 16;
            ulong fee = ReadU64FromU128BE(src.Slice(o, 16)); o += 16;

            ulong nonce = BinaryPrimitives.ReadUInt64BigEndian(src.Slice(o, 8)); o += 8;

            ushort sigLen = BinaryPrimitives.ReadUInt16BigEndian(src.Slice(o, 2)); o += 2;

            if (sigLen > MaxSignatureBytes)
                throw new ArgumentException($"Signature too long ({sigLen} > {MaxSignatureBytes}).", nameof(src));

            if (src.Length < o + sigLen) throw new ArgumentException("Buffer too small for signature.", nameof(src));

            byte[] sig = sigLen == 0 ? Array.Empty<byte>() : src.Slice(o, sigLen).ToArray();
            o += sigLen;

            if (o != src.Length)
                throw new ArgumentException($"Trailing bytes after tx payload: {src.Length - o}.", nameof(src));

            return new Transaction
            {
                ChainId = chainId,
                Sender = sender,
                Recipient = recipient,
                Amount = amount,
                Fee = fee,
                TxNonce = nonce,
                Signature = sig
            };
        }

        public static Transaction Read(byte[] data)
            => Read((ReadOnlySpan<byte>)data);


        private static void WriteU64AsU128BE(Span<byte> dst16, ulong value)
        {
            if (dst16.Length != 16) throw new ArgumentException("dst16 must be 16 bytes.", nameof(dst16));
            BinaryPrimitives.WriteUInt64BigEndian(dst16.Slice(0, 8), 0UL);
            BinaryPrimitives.WriteUInt64BigEndian(dst16.Slice(8, 8), value);
        }

        private static ulong ReadU64FromU128BE(ReadOnlySpan<byte> src16)
        {
            if (src16.Length != 16) throw new ArgumentException("src16 must be 16 bytes.", nameof(src16));

            ulong hi = BinaryPrimitives.ReadUInt64BigEndian(src16.Slice(0, 8));
            ulong lo = BinaryPrimitives.ReadUInt64BigEndian(src16.Slice(8, 8));

            if (hi != 0) throw new InvalidOperationException("amount/fee does not fit into ulong (hi64 != 0).");

            return lo;
        }
    }
}

