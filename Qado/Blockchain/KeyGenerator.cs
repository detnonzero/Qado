using System;
using NSec.Cryptography;
using Qado.Storage;

namespace Qado.Blockchain
{
    public static class KeyGenerator
    {
        public const int PrivateKeySize = 32; // Ed25519 seed
        public const int PublicKeySize = 32;

        private static readonly SignatureAlgorithm Algo = SignatureAlgorithm.Ed25519;

        public static string GeneratePrivateKeyHex()
        {
            using var key = Key.Create(Algo, new KeyCreationParameters
            {
                ExportPolicy = KeyExportPolicies.AllowPlaintextExport
            });

            var privateKeyBytes = key.Export(KeyBlobFormat.RawPrivateKey); // 32-byte seed
            if (privateKeyBytes.Length != PrivateKeySize)
                throw new InvalidOperationException($"Ed25519 RawPrivateKey must be {PrivateKeySize} bytes.");

            return ToLowerHex(privateKeyBytes);
        }

        public static string GetPublicKeyFromPrivateKeyHex(string privateKeyHex)
        {
            var priv = ParseHex(privateKeyHex, expectedLen: PrivateKeySize, nameof(privateKeyHex));

            using var key = Key.Import(Algo, priv, KeyBlobFormat.RawPrivateKey);
            var publicKeyBytes = key.Export(KeyBlobFormat.RawPublicKey); // 32-byte public key

            if (publicKeyBytes.Length != PublicKeySize)
                throw new InvalidOperationException($"Ed25519 RawPublicKey must be {PublicKeySize} bytes.");

            return ToLowerHex(publicKeyBytes);
        }

        public static (string privHex, string pubHex) GenerateKeypairHex()
        {
            var (priv, pub) = GenerateKeypairBytes();
            return (ToLowerHex(priv), ToLowerHex(pub));
        }

        public static (byte[] priv, byte[] pub) GenerateKeypairBytes()
        {
            using var key = Key.Create(Algo, new KeyCreationParameters
            {
                ExportPolicy = KeyExportPolicies.AllowPlaintextExport
            });

            var priv = key.Export(KeyBlobFormat.RawPrivateKey);
            var pub = key.Export(KeyBlobFormat.RawPublicKey);

            if (priv.Length != PrivateKeySize)
                throw new InvalidOperationException($"Ed25519 RawPrivateKey must be {PrivateKeySize} bytes.");
            if (pub.Length != PublicKeySize)
                throw new InvalidOperationException($"Ed25519 RawPublicKey must be {PublicKeySize} bytes.");

            return (priv, pub);
        }

        public static (string privHex, string pubHex) GenerateAndStore()
        {
            var (privHex, pubHex) = GenerateKeypairHex();

            KeyStore.AddKey(privHex, pubHex);

            return (privHex, pubHex);
        }


        private static string ToLowerHex(byte[] bytes)
            => Convert.ToHexString(bytes).ToLowerInvariant();

        private static byte[] ParseHex(string hex, int? expectedLen, string paramName)
        {
            if (hex is null) throw new ArgumentNullException(paramName);

            hex = hex.Trim();
            if (hex.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
                hex = hex[2..];

            if (hex.Length == 0)
                throw new FormatException($"{paramName} must not be empty.");

            if ((hex.Length & 1) != 0)
                throw new FormatException($"{paramName} must have even length.");

            for (int i = 0; i < hex.Length; i++)
            {
                char c = hex[i];
                bool ok =
                    (c >= '0' && c <= '9') ||
                    (c >= 'a' && c <= 'f') ||
                    (c >= 'A' && c <= 'F');
                if (!ok)
                    throw new FormatException($"{paramName} contains non-hex characters.");
            }

            byte[] bytes;
            try
            {
                bytes = Convert.FromHexString(hex);
            }
            catch (Exception ex)
            {
                throw new FormatException($"{paramName} is not valid hex.", ex);
            }

            if (expectedLen.HasValue && bytes.Length != expectedLen.Value)
                throw new FormatException($"{paramName} must be {expectedLen.Value} bytes, got {bytes.Length}.");

            return bytes;
        }
    }
}

