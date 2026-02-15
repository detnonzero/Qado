using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;

namespace Qado.Storage
{
    public static class KeyStore
    {
        private static readonly byte[] PrivBlobMagic = { (byte)'Q', (byte)'K', 1 };
        private static readonly byte[] PrivBlobEntropy =
            SHA256.HashData(Encoding.UTF8.GetBytes("Qado.KeyStore.PrivateKey.V1"));

        public static void AddKey(string privHex, string pubHex, SqliteTransaction? tx = null)
        {
            if (string.IsNullOrWhiteSpace(privHex)) throw new ArgumentNullException(nameof(privHex));
            if (string.IsNullOrWhiteSpace(pubHex)) throw new ArgumentNullException(nameof(pubHex));

            byte[] priv = Convert.FromHexString(privHex);
            byte[] pub = Convert.FromHexString(pubHex);
            if (priv.Length != 32) throw new ArgumentException("priv must be 32 bytes", nameof(privHex));
            if (pub.Length != 32) throw new ArgumentException("pub must be 32 bytes", nameof(pubHex));
            byte[] privBlob = ProtectPrivateKey(priv);

            if (tx != null)
            {
                using var cmd = tx.Connection!.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "INSERT OR REPLACE INTO keys(pub,priv) VALUES($p,$k);";
                cmd.Parameters.AddWithValue("$p", pub);
                cmd.Parameters.AddWithValue("$k", privBlob);
                cmd.ExecuteNonQuery();
                return;
            }

            lock (Db.Sync)
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = "INSERT OR REPLACE INTO keys(pub,priv) VALUES($p,$k);";
                cmd.Parameters.AddWithValue("$p", pub);
                cmd.Parameters.AddWithValue("$k", privBlob);
                cmd.ExecuteNonQuery();
            }
        }

        public static void ReplaceAll(IReadOnlyList<(string PrivHex, string PubHex)> keys)
        {
            if (keys == null) throw new ArgumentNullException(nameof(keys));

            lock (Db.Sync)
            {
                using var tx = Db.Connection.BeginTransaction();

                using (var del = tx.Connection!.CreateCommand())
                {
                    del.Transaction = tx;
                    del.CommandText = "DELETE FROM keys;";
                    del.ExecuteNonQuery();
                }

                for (int i = 0; i < keys.Count; i++)
                    AddKey(keys[i].PrivHex, keys[i].PubHex, tx);

                tx.Commit();
            }
        }

        public static List<(string PrivHex, string PubHex)> GetAllKeys()
        {
            lock (Db.Sync)
            {
                var list = new List<(string PrivHex, string PubHex)>();

                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = "SELECT priv,pub FROM keys;";

                using var r = cmd.ExecuteReader();
                while (r.Read())
                {
                    var privBytes = UnprotectPrivateKeyOrLegacy((byte[])r[0]);
                    var pubBytes = (byte[])r[1];

                    var priv = Convert.ToHexString(privBytes).ToLowerInvariant();
                    var pub = Convert.ToHexString(pubBytes).ToLowerInvariant();

                    list.Add((priv, pub));
                }

                return list;
            }
        }

        private static byte[] ProtectPrivateKey(byte[] rawPriv)
        {
            byte[] cipher = ProtectedData.Protect(rawPriv, PrivBlobEntropy, DataProtectionScope.CurrentUser);
            var blob = new byte[PrivBlobMagic.Length + cipher.Length];
            Buffer.BlockCopy(PrivBlobMagic, 0, blob, 0, PrivBlobMagic.Length);
            Buffer.BlockCopy(cipher, 0, blob, PrivBlobMagic.Length, cipher.Length);
            return blob;
        }

        private static byte[] UnprotectPrivateKeyOrLegacy(byte[] stored)
        {
            if (stored is not { Length: > 0 })
                throw new FormatException("Stored private key blob is empty.");

            if (HasMagicPrefix(stored))
            {
                var enc = new byte[stored.Length - PrivBlobMagic.Length];
                Buffer.BlockCopy(stored, PrivBlobMagic.Length, enc, 0, enc.Length);
                byte[] plain = ProtectedData.Unprotect(enc, PrivBlobEntropy, DataProtectionScope.CurrentUser);
                if (plain.Length != 32)
                    throw new FormatException($"Unprotected private key has invalid length {plain.Length}.");
                return plain;
            }

            if (stored.Length == 32)
                return (byte[])stored.Clone();

            throw new FormatException("Stored private key format is invalid.");
        }

        private static bool HasMagicPrefix(byte[] blob)
        {
            if (blob.Length <= PrivBlobMagic.Length) return false;
            for (int i = 0; i < PrivBlobMagic.Length; i++)
                if (blob[i] != PrivBlobMagic[i]) return false;
            return true;
        }
    }
}

