using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using Qado.Networking;
using Qado.Serialization;

namespace Qado.Blockchain
{
    public sealed class Transaction
    {
        public const int PubKeySize = 32;
        public const int SignatureSize = 64;

        public uint ChainId { get; set; } = NetworkParams.ChainId;

        public byte[] Sender { get; set; } = Array.Empty<byte>();

        public byte[] Recipient { get; set; } = Array.Empty<byte>();

        public ulong Amount { get; set; }
        public ulong Fee { get; set; }
        public ulong TxNonce { get; set; }

        public byte[] Signature { get; set; } = Array.Empty<byte>();

        public bool IsCoinbase
            => Sender is { Length: PubKeySize }
               && IsAllZero(Sender)
               && (Signature is null || Signature.Length == 0);

        public byte[] ToHashBytes()
        {
            Validate32(Sender, nameof(Sender));
            Validate32(Recipient, nameof(Recipient));

            var buf = new byte[4 + PubKeySize + PubKeySize + 8 + 8 + 8];
            int o = 0;

            BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(o, 4), ChainId); o += 4;
            Sender.AsSpan(0, PubKeySize).CopyTo(buf.AsSpan(o, PubKeySize)); o += PubKeySize;
            Recipient.AsSpan(0, PubKeySize).CopyTo(buf.AsSpan(o, PubKeySize)); o += PubKeySize;

            BinaryPrimitives.WriteUInt64BigEndian(buf.AsSpan(o, 8), Amount); o += 8;
            BinaryPrimitives.WriteUInt64BigEndian(buf.AsSpan(o, 8), Fee); o += 8;
            BinaryPrimitives.WriteUInt64BigEndian(buf.AsSpan(o, 8), TxNonce); o += 8;

            return buf;
        }

        public byte[] ComputeTransactionHash()
            => SHA256.HashData(ToHashBytes());

        public void ValidateBasicOrThrow()
        {
            Validate32(Sender, nameof(Sender));
            Validate32(Recipient, nameof(Recipient));

            if (ChainId == 0)
                throw new InvalidOperationException("ChainId must be non-zero.");

            if (IsAllZero(Recipient))
                throw new InvalidOperationException("Recipient must not be the zero address.");

            if (IsAllZero(Sender))
            {
                if (Fee != 0) throw new InvalidOperationException("Coinbase must have Fee=0.");
                if (TxNonce != 0) throw new InvalidOperationException("Coinbase must have TxNonce=0.");
                if (Signature is not null && Signature.Length != 0) throw new InvalidOperationException("Coinbase must have empty Signature.");
                return;
            }

            if (Amount == 0)
                throw new InvalidOperationException("Amount must be > 0.");

            if (Signature is not { Length: SignatureSize })
                throw new InvalidOperationException("Non-coinbase Signature must be 64 bytes.");
        }

        public byte[] ToBytes()
        {
            var buf = new byte[TxBinarySerializer.GetSize(this)];
            _ = TxBinarySerializer.Write(buf, this);
            return buf;
        }

        public static Transaction FromBytes(byte[] data) => TxBinarySerializer.Read(data);

        private static void Validate32(byte[]? v, string name)
        {
            if (v is not { Length: PubKeySize })
                throw new InvalidOperationException($"{name} must be {PubKeySize} bytes.");
        }

        private static bool IsAllZero(ReadOnlySpan<byte> a)
        {
            for (int i = 0; i < a.Length; i++)
                if (a[i] != 0) return false;
            return true;
        }
    }
}

