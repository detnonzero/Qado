using System;
using NSec.Cryptography;
using Qado.Networking;
using Qado.Storage;

namespace Qado.Blockchain
{
    public static class TransactionValidator
    {
        private const uint ChainId = NetworkParams.ChainId;
        private const int PubKeySize = 32;
        private const int SigSize = 64;

        public static bool ValidateBasic(Transaction tx, out string reason)
        {
            reason = "OK";

            if (tx is null) { reason = "TX is null"; return false; }

            if (tx.Sender is null || tx.Sender.Length != PubKeySize) { reason = "Sender must be 32 bytes"; return false; }
            if (tx.Recipient is null || tx.Recipient.Length != PubKeySize) { reason = "Recipient must be 32 bytes"; return false; }

            if (tx.ChainId != ChainId) { reason = "Invalid ChainId"; return false; }

            if (IsZero32(tx.Recipient))
            {
                reason = "Recipient must not be zero address";
                return false;
            }

            bool isCoinbase = IsCoinbase(tx);

            if (!isCoinbase)
            {
                if (tx.Amount == 0) { reason = "Amount must be greater than 0"; return false; }
                if (tx.Signature is null || tx.Signature.Length != SigSize) { reason = "Invalid or missing signature"; return false; }

                if (!VerifySignature(tx))
                {
                    reason = "Invalid signature";
                    return false;
                }

                return true;
            }

            if (tx.Signature is not null && tx.Signature.Length != 0)
            {
                reason = "Coinbase must not be signed";
                return false;
            }

            if (tx.Fee != 0) { reason = "Coinbase fee must be 0"; return false; }
            if (tx.TxNonce != 0) { reason = "Coinbase nonce must be 0"; return false; }

            if (tx.Amount == 0)
            {
                return true;
            }

            return true;
        }

        public static bool ValidateForMempool(Transaction tx, out string reason)
        {
            reason = "OK";

            if (!ValidateBasic(tx, out reason))
                return false;

            if (IsCoinbase(tx))
            {
                reason = "Coinbase transaction is not allowed in mempool";
                return false;
            }

            string senderHex = Convert.ToHexString(tx.Sender).ToLowerInvariant();

            ulong confirmedNonce = StateStore.GetNonceU64(senderHex);

            int pendingTxCount = Qado.Mempool.MempoolManager.PendingCount(senderHex);
            if (pendingTxCount < 0)
            {
                reason = "PendingCount invalid";
                return false;
            }

            if (!TryAddU64(confirmedNonce, (ulong)pendingTxCount, out var tmp) ||
                !TryAddU64(tmp, 1UL, out var expectedNonce))
            {
                reason = "Nonce overflow";
                return false;
            }

            if (tx.TxNonce != expectedNonce)
            {
                reason = $"Invalid nonce: expected {expectedNonce}, got {tx.TxNonce}";
                return false;
            }

            ulong balance = StateStore.GetBalanceU64(senderHex);

            if (!TryAddU64(tx.Amount, tx.Fee, out var totalCost))
            {
                reason = "Amount+Fee overflow";
                return false;
            }

            if (balance < totalCost)
            {
                reason = "Insufficient balance";
                return false;
            }

            return true;
        }

        public static bool Validate(Transaction tx, out string reason)
            => ValidateForMempool(tx, out reason);

        public static bool IsCoinbase(Transaction tx)
        {
            if (tx?.Sender is null || tx.Sender.Length != PubKeySize) return false;
            if (!IsZero32(tx.Sender)) return false;

            return tx.Signature is null || tx.Signature.Length == 0;
        }

        private static bool VerifySignature(Transaction tx)
        {
            try
            {
                var algorithm = SignatureAlgorithm.Ed25519;
                var publicKey = PublicKey.Import(algorithm, tx.Sender, KeyBlobFormat.RawPublicKey);
                return algorithm.Verify(publicKey, tx.ToHashBytes(), tx.Signature);
            }
            catch
            {
                return false;
            }
        }

        private static bool TryAddU64(ulong a, ulong b, out ulong sum)
        {
            if (ulong.MaxValue - a < b)
            {
                sum = 0;
                return false;
            }
            sum = a + b;
            return true;
        }

        private static bool IsZero32(byte[] x)
        {
            if (x is null || x.Length != PubKeySize) return false;
            for (int i = 0; i < PubKeySize; i++)
                if (x[i] != 0) return false;
            return true;
        }
    }
}

