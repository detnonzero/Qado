using System;
using System.Windows.Controls;
using Qado.Logging;
using Qado.Storage;

namespace Qado.CodeBehindHelper
{
    public static class BalanceHelper
    {
        public static void UpdateBalanceUI(TextBlock balanceTarget, string privateKeyHex, ILogSink? log = null)
        {
            if (balanceTarget == null) return;

            string pk = NormalizeHex(privateKeyHex);

            if (!IsValidHex(pk, expectedBytes: 32))
            {
                SetText(balanceTarget, "—");
                log?.Warn("Balance", "Invalid private key format.");
                return;
            }

            string publicKeyHex;
            try
            {
                publicKeyHex = Qado.Blockchain.KeyGenerator.GetPublicKeyFromPrivateKeyHex(pk);
            }
            catch (Exception ex)
            {
                SetText(balanceTarget, "—");
                log?.Warn("Balance", $"Failed to derive public key: {ex.Message}");
                return;
            }

            publicKeyHex = publicKeyHex.ToLowerInvariant();

            ulong atomsBalance;
            try
            {
                atomsBalance = StateStore.GetBalanceU64(publicKeyHex);
            }
            catch (Exception ex)
            {
                SetText(balanceTarget, "—");
                log?.Error("Balance", $"State read failed: {ex.Message}");
                return;
            }

            string qado = QadoAmountParser.FormatNanoToQado(atomsBalance);
            SetText(balanceTarget, $"{qado} QADO");
        }

        public static ulong GetConfirmedNonce(string publicKeyHex)
        {
            string pk = NormalizeHex(publicKeyHex);
            if (!IsValidHex(pk, expectedBytes: 32)) return 0UL;

            try
            {
                return StateStore.GetNonceU64(pk.ToLowerInvariant());
            }
            catch
            {
                return 0UL;
            }
        }


        private static void SetText(TextBlock tb, string text)
        {
            if (tb.Dispatcher.CheckAccess())
            {
                tb.Text = text;
                return;
            }

            tb.Dispatcher.BeginInvoke(new Action(() => tb.Text = text));
        }


        private static string NormalizeHex(string hex)
        {
            if (hex == null) return string.Empty;
            hex = hex.Trim();
            if (hex.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
                hex = hex[2..];
            return hex;
        }

        private static bool IsValidHex(string hex, int expectedBytes)
        {
            if (string.IsNullOrWhiteSpace(hex)) return false;
            if (hex.Length != expectedBytes * 2) return false;

            for (int i = 0; i < hex.Length; i++)
            {
                char c = hex[i];
                bool ok =
                    (c >= '0' && c <= '9') ||
                    (c >= 'a' && c <= 'f') ||
                    (c >= 'A' && c <= 'F');
                if (!ok) return false;
            }
            return true;
        }
    }
}

