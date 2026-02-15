using System;
using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;
using Qado.Blockchain;
using Qado.Storage;

namespace Qado.CodeBehindHelper
{
    public static class KeyHelper
    {
        public static void UpdateKeyUI(
            Button acceptButton,
            Button generateButton,
            TextBox privateKeyBox,
            TextBox publicKeyBox,
            ComboBox keyComboBox)
        {
            if (acceptButton == null || generateButton == null || privateKeyBox == null || publicKeyBox == null || keyComboBox == null)
                return;

            string selected = keyComboBox.SelectedItem as string ?? string.Empty;
            bool isNewEntry = string.Equals(selected, "New", StringComparison.Ordinal);

            acceptButton.Content = "Accept Private Key";

            if (isNewEntry)
            {
                acceptButton.IsEnabled = true;

                bool canGenerate = KeyStore.GetAllKeys().Count == 0;
                generateButton.IsEnabled = canGenerate;
                generateButton.Opacity = canGenerate ? 1.0 : 0.5;

                privateKeyBox.IsReadOnly = false;
                privateKeyBox.Text = string.Empty;
                publicKeyBox.Text = string.Empty;
                return;
            }

            acceptButton.IsEnabled = false;
            generateButton.IsEnabled = false;
            generateButton.Opacity = 0.5;

            privateKeyBox.IsReadOnly = true;

            string privHex = NormalizeHex(selected);
            privateKeyBox.Text = privHex;

            if (!IsValidHex(privHex, 32))
            {
                publicKeyBox.Text = string.Empty;
                return;
            }

            try
            {
                publicKeyBox.Text = KeyGenerator.GetPublicKeyFromPrivateKeyHex(privHex);
            }
            catch
            {
                publicKeyBox.Text = string.Empty;
            }
        }

        public static void HandleGenerateClick(
            Button acceptButton,
            Button generateButton,
            TextBox privateKeyBox,
            TextBox publicKeyBox,
            ComboBox keyComboBox,
            TextBlock balanceTextBlock)
        {
            var keys = KeyStore.GetAllKeys();
            if (keys.Count > 0)
            {
                MessageBox.Show("A key already exists. Generation is disabled by design.", "QADO",
                    MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var (privHex, pubHex) = KeyGenerator.GenerateAndStore();

            privateKeyBox.Text = privHex;
            publicKeyBox.Text = pubHex;

            EnsureComboBoxContains(keyComboBox, privHex);
            keyComboBox.SelectedItem = privHex;

            BalanceHelper.UpdateBalanceUI(balanceTextBlock, privHex);

            UpdateKeyUI(acceptButton, generateButton, privateKeyBox, publicKeyBox, keyComboBox);
        }

        public static void HandleAcceptClick(
            Button acceptButton,
            Button generateButton,
            TextBox privateKeyBox,
            TextBox publicKeyBox,
            ComboBox keyComboBox,
            TextBlock balanceTextBlock)
        {
            string input = NormalizeHex(privateKeyBox?.Text ?? string.Empty);

            if (!IsValidHex(input, 32))
            {
                MessageBox.Show("Invalid private key format. Must be 64 hex characters (32 bytes).", "QADO",
                    MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            try
            {
                string pubHex = KeyGenerator.GetPublicKeyFromPrivateKeyHex(input);

                KeyStore.AddKey(input.ToLowerInvariant(), pubHex.ToLowerInvariant());

                publicKeyBox.Text = pubHex;

                EnsureComboBoxContains(keyComboBox, input);
                keyComboBox.SelectedItem = input;

                BalanceHelper.UpdateBalanceUI(balanceTextBlock, input);

                UpdateKeyUI(acceptButton, generateButton, privateKeyBox, publicKeyBox, keyComboBox);
            }
            catch
            {
                MessageBox.Show("Invalid private key content.", "QADO",
                    MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }


        private static void EnsureComboBoxContains(ComboBox cb, string privHex)
        {
            if (cb == null) return;

            bool hasNew = false;
            for (int i = 0; i < cb.Items.Count; i++)
            {
                if (cb.Items[i] is string s && string.Equals(s, "New", StringComparison.Ordinal))
                {
                    hasNew = true;
                    break;
                }
            }

            for (int i = 0; i < cb.Items.Count; i++)
            {
                if (cb.Items[i] is string s && string.Equals(NormalizeHex(s), privHex, StringComparison.OrdinalIgnoreCase))
                    return;
            }

            if (!hasNew)
                cb.Items.Insert(0, "New");

            cb.Items.Add(privHex);
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

