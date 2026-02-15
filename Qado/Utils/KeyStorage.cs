using System;
using System.Collections.Generic;
using Qado.Blockchain;
using Qado.Storage;

namespace Qado.Utils
{
    public static class KeyStorage
    {
        public static string? TryLoadFirstPrivateKey()
        {
            var keys = KeyStore.GetAllKeys(); 
            return keys.Count > 0 ? keys[0].PrivHex : null;
        }

        public static void SavePrivateKeys(List<string> privateKeys)
        {
            if (privateKeys == null) throw new ArgumentNullException(nameof(privateKeys));

            var normalized = new List<(string PrivHex, string PubHex)>(privateKeys.Count);
            foreach (var pk in privateKeys)
            {
                var p = (pk ?? string.Empty).Trim().ToLowerInvariant();
                if (!IsHex(p) || p.Length != 64)
                    throw new FormatException("Each private key must be 64 hex characters (32 bytes).");
                var pub = KeyGenerator.GetPublicKeyFromPrivateKeyHex(p).ToLowerInvariant();
                normalized.Add((p, pub));
            }

            KeyStore.ReplaceAll(normalized);
        }


        public static List<string> LoadAllPrivateKeys()
        {
            var list = new List<string>();
            var keys = KeyStore.GetAllKeys();
            for (int i = 0; i < keys.Count; i++)
                list.Add(keys[i].PrivHex);
            return list;
        }

        public static bool IsHex(string? input)
        {
            if (string.IsNullOrWhiteSpace(input)) return false;
            var s = input.Trim();
            if ((s.Length & 1) != 0) return false;

            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                bool ok = (c >= '0' && c <= '9') ||
                          (c >= 'a' && c <= 'f') ||
                          (c >= 'A' && c <= 'F');
                if (!ok) return false;
            }
            return true;
        }
    }
}

