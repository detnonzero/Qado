using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using NSec.Cryptography;
using Qado.Blockchain;
using Qado.Serialization;
using Qado.Logging;
using Qado.Mempool;
using Qado.Networking;
using Qado.Storage;

namespace Qado.CodeBehindHelper
{
    public static class TransactionHelper
    {
        private const int GossipPort = GenesisConfig.P2PPort; // fallback port for peers without endpoint metadata
        private const int GossipMaxPeers = 64;
        private const int GossipConcurrency = 12;
        private const int TcpTimeoutMs = 7000;

        public static async Task<Transaction?> HandleSend(
            TextBox recipientBox,
            TextBox amountBox,
            TextBox feeBox,
            string privateKeyHex,
            MempoolManager mempool,
            ILogSink? logSink,
            Action<Transaction>? onAccepted)
        {
            try
            {
                if (recipientBox == null || amountBox == null || feeBox == null)
                    throw new Exception("UI inputs missing.");

                string recipHex = NormalizeHex(recipientBox.Text).ToLowerInvariant();
                if (!IsValidHex(recipHex, 32))
                    throw new Exception("Invalid recipient public key. Must be 64 hex characters (32 bytes).");

                var recipBytes = Convert.FromHexString(recipHex);
                if (IsZero32(recipBytes))
                    throw new Exception("Recipient must not be zero address.");

                if (!QadoAmountParser.TryParseQadoToNanoU64((amountBox.Text ?? "").Trim(), out ulong amount) || amount == 0)
                    throw new Exception("Invalid amount.");

                if (!QadoAmountParser.TryParseQadoToNanoU64((feeBox.Text ?? "").Trim(), out ulong fee))
                    throw new Exception("Invalid fee.");

                privateKeyHex = NormalizeHex(privateKeyHex);
                if (!IsValidHex(privateKeyHex, 32))
                    throw new Exception("Invalid private key. Must be 64 hex characters (32 bytes).");

                if (!TryAddU64(amount, fee, out _))
                    throw new Exception("Amount + fee overflow.");

                var privBytes = Convert.FromHexString(privateKeyHex);
                using var key = Key.Import(SignatureAlgorithm.Ed25519, privBytes, KeyBlobFormat.RawPrivateKey);

                var pubBytes = key.Export(KeyBlobFormat.RawPublicKey);
                if (pubBytes.Length != 32) throw new Exception("Derived public key invalid.");

                string senderHex = Convert.ToHexString(pubBytes).ToLowerInvariant();

                if (!TryComputeNextNonceGapFree(senderHex, out ulong nonce))
                    throw new Exception("Nonce overflow.");

                var tx = new Transaction
                {
                    ChainId = NetworkParams.ChainId,
                    Sender = pubBytes,
                    Recipient = recipBytes,
                    Amount = amount,
                    Fee = fee,
                    TxNonce = nonce,
                    Signature = Array.Empty<byte>()
                };

                var msg = tx.ToHashBytes();
                tx.Signature = SignatureAlgorithm.Ed25519.Sign(key, msg);

                if (!TransactionValidator.ValidateForMempool(tx, out var reason))
                    throw new Exception($"Transaction invalid: {reason}");

                if (!mempool.TryAdd(tx))
                    throw new Exception("Transaction rejected by mempool.");

                onAccepted?.Invoke(tx);

                MessageBox.Show("Transaction sent!", "QADO", MessageBoxButton.OK, MessageBoxImage.Information);

                logSink?.Info("TX",
                    $"New TX → from {Short(senderHex)} to {Short(recipHex)} | Fee={tx.Fee} | Amount={tx.Amount} | Nonce={tx.TxNonce}");

                _ = BroadcastTxAsync(tx, logSink);

                return tx;
            }
            catch (Exception ex)
            {
                logSink?.Error("TX", ex.Message);
                MessageBox.Show($"Error sending transaction:\n{ex.Message}", "QADO", MessageBoxButton.OK, MessageBoxImage.Error);
                return null;
            }
            finally
            {
                await Task.Yield(); // preserve async contract
            }
        }

        private static bool TryComputeNextNonceGapFree(string senderHex, out ulong nextNonce)
        {
            nextNonce = 0;

            ulong confirmed = StateStore.GetNonceU64(senderHex);

            int pending = MempoolManager.PendingCount(senderHex);
            if (pending < 0) return false;

            if (!TryAddU64(confirmed, (ulong)pending, out var tmp)) return false;
            if (!TryAddU64(tmp, 1UL, out nextNonce)) return false;

            return true;
        }

        private static bool TryAddU64(ulong a, ulong b, out ulong sum)
        {
            if (ulong.MaxValue - a < b) { sum = 0; return false; }
            sum = a + b;
            return true;
        }

        private static string NormalizeHex(string? s)
        {
            s ??= "";
            s = s.Trim();
            if (s.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
                s = s[2..];
            return s;
        }

        private static bool IsValidHex(string hex, int expectedBytes)
        {
            if (string.IsNullOrWhiteSpace(hex) || hex.Length != expectedBytes * 2) return false;
            for (int i = 0; i < hex.Length; i++)
            {
                char c = hex[i];
                bool ok = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
                if (!ok) return false;
            }
            return true;
        }

        private static bool IsZero32(byte[] x)
        {
            if (x is not { Length: 32 }) return true;
            for (int i = 0; i < 32; i++) if (x[i] != 0) return false;
            return true;
        }

        private static string Short(string hex, int head = 6)
            => string.IsNullOrEmpty(hex) || hex.Length <= head ? hex : $"{hex[..head]}…";


        private static async Task BroadcastTxAsync(Transaction tx, ILogSink? log)
        {
            try
            {
                int size = TxBinarySerializer.GetSize(tx);
                var buf = new byte[size];
                _ = TxBinarySerializer.Write(buf, tx);

                if (P2PNode.Instance != null)
                {
                    await P2PNode.Instance.BroadcastTxAsync(tx).ConfigureAwait(false);
                    log?.Info("Gossip", $"Broadcasted TX via active P2P sessions (nonce={tx.TxNonce}).");
                    return;
                }

                var peers = LoadPeersSafe();
                if (peers.Count == 0)
                {
                    log?.Warn("Gossip", "No peers to broadcast tx.");
                    return;
                }

                using var sem = new SemaphoreSlim(GossipConcurrency);
                var tasks = new List<Task>(peers.Count);

                foreach (var (ip, port) in peers)
                    tasks.Add(SendOneAsync(ip, port, MsgType.Tx, buf, sem, log));

                await Task.WhenAll(tasks).ConfigureAwait(false);
                log?.Info("Gossip", $"Broadcasted TX (handshake + tx, nonce={tx.TxNonce}) to {peers.Count} peer(s).");
            }
            catch (Exception ex)
            {
                log?.Error("Gossip", $"Broadcast tx error: {ex}");
            }
        }

        private static async Task SendOneAsync(string ip, int port, MsgType type, byte[] payload, SemaphoreSlim sem, ILogSink? log)
        {
            await sem.WaitAsync().ConfigureAwait(false);
            try
            {
                using var client = new TcpClient
                {
                    ReceiveTimeout = TcpTimeoutMs,
                    SendTimeout = TcpTimeoutMs
                };

                await client.ConnectAsync(ip, port).ConfigureAwait(false);

                using var ns = client.GetStream();
                var handshake = BuildHandshakePayload((ushort)GossipPort);
                await P2PNode.WriteFrame(ns, MsgType.Handshake, handshake, CancellationToken.None).ConfigureAwait(false);
                await P2PNode.WriteFrame(ns, type, payload, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                log?.Warn("Gossip", $"{type} broadcast to {ip}:{port} failed: {ex.Message}");
            }
            finally
            {
                sem.Release();
            }
        }

        private static List<(string ip, int port)> LoadPeersSafe()
        {
            var list = new List<(string, int)>();
            if (Db.Connection == null) return list;

            lock (Db.Sync)
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = "SELECT ip, port FROM peers ORDER BY last_seen DESC LIMIT $n;";
                cmd.Parameters.AddWithValue("$n", GossipMaxPeers);

                using var r = cmd.ExecuteReader();
                while (r.Read())
                {
                    string ip = r.GetString(0);
                    int port = r.GetInt32(1);

                    if (string.IsNullOrWhiteSpace(ip)) continue;
                    if (port <= 0) port = GossipPort;
                    if (SelfPeerGuard.IsSelf(ip, port)) continue;

                    list.Add((ip, port));
                }
            }

            return list;
        }

        private static byte[] BuildHandshakePayload(ushort listenPort)
        {
            var buf = new byte[1 + 1 + 32 + 2];
            buf[0] = 1; // protocol version
            buf[1] = GenesisConfig.NetworkId;

            byte[] nodeId = GetOrCreateNodeId();
            Buffer.BlockCopy(nodeId, 0, buf, 2, 32);

            buf[34] = (byte)(listenPort & 0xFF);
            buf[35] = (byte)((listenPort >> 8) & 0xFF);
            return buf;
        }

        private static byte[] GetOrCreateNodeId()
        {
            var hex = MetaStore.Get("NodeId");
            if (!string.IsNullOrWhiteSpace(hex))
            {
                try
                {
                    var parsed = Convert.FromHexString(hex);
                    if (parsed.Length == 32)
                        return parsed;
                }
                catch { }
            }

            var id = new byte[32];
            RandomNumberGenerator.Fill(id);
            MetaStore.Set("NodeId", Convert.ToHexString(id).ToLowerInvariant());
            return id;
        }
    }
}

