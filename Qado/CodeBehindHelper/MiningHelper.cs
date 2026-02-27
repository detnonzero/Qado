using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Microsoft.Data.Sqlite;
using Qado.Blockchain;
using Qado.Serialization;
using Qado.Logging;
using Qado.Mempool;
using Qado.Networking;
using Qado.Storage;

namespace Qado.CodeBehindHelper
{
    public static class MiningHelper
    {
        private static readonly object _gate = new();

        private static CancellationTokenSource? _cts;

        private static long _blockStartUtcTicks;
        private static long _hashCount;

        private static Miner? _currentMiner;

        private const int GossipMaxPeers = 64;
        private const int GossipConcurrency = 12;
        private const int TcpTimeoutMs = 7000;
        private const int DefaultPeerPort = P2PNode.DefaultPort;

        public static bool IsMiningActive
        {
            get { lock (_gate) return _cts != null; }
        }

        public static void StartMining(
            string privateKeyHex,
            MempoolManager mempool,
            ILogSink logSink,
            Button startMiningButton,
            Panel miningStatsPanel,
            TextBlock blockUptimeText,
            TextBlock currentHashrateText,
            TextBlock nonceText,
            TextBlock balanceTextBlock,
            Action<Action> startUiTimer)
        {
            privateKeyHex = NormalizeHex(privateKeyHex);

            if (!IsValidHex(privateKeyHex, 32) || IsAllZeroHex(privateKeyHex))
            {
                logSink.Error("Mining", "Invalid private key.");
                return;
            }

            if (mempool == null) throw new ArgumentNullException(nameof(mempool));
            if (logSink == null) throw new ArgumentNullException(nameof(logSink));

            StopMiningInternal(
                startMiningButton,
                miningStatsPanel,
                balanceTextBlock,
                privateKeyHex,
                stopUiTimer: null,
                logSink: logSink);

            var newCts = new CancellationTokenSource();

            lock (_gate)
            {
                _cts = newCts;
                Interlocked.Exchange(ref _blockStartUtcTicks, DateTime.UtcNow.Ticks);
                Interlocked.Exchange(ref _hashCount, 0);
                _currentMiner = null;
            }

            UiInvoke(() =>
            {
                startMiningButton.Content = "Stop Mining";
                miningStatsPanel.Visibility = Visibility.Visible;
                try { BalanceHelper.UpdateBalanceUI(balanceTextBlock, privateKeyHex); } catch { }
            });

            startUiTimer(() =>
            {
                long startTicks = Interlocked.Read(ref _blockStartUtcTicks);
                var uptime = startTicks == 0 ? 0 : (int)(DateTime.UtcNow - new DateTime(startTicks, DateTimeKind.Utc)).TotalSeconds;

                var hashes = Interlocked.Read(ref _hashCount);
                var hashrate = uptime > 0 ? (hashes / Math.Max(1, uptime)) : hashes;

                blockUptimeText.Text = $"Block Uptime: {uptime}s";
                currentHashrateText.Text = $"Current Hashrate: {hashrate} H/s";
                nonceText.Text = $"Nonce: {GetCurrentNonceUnsafe()}";
            });

            _ = Task.Run(() => MiningLoopAsync(privateKeyHex, mempool, logSink, balanceTextBlock, newCts.Token));
        }

        public static void StopMining(
            Button startMiningButton,
            Panel miningStatsPanel,
            TextBlock balanceTextBlock,
            string privateKeyHex,
            Action stopUiTimer)
        {
            StopMiningInternal(
                startMiningButton,
                miningStatsPanel,
                balanceTextBlock,
                NormalizeHex(privateKeyHex),
                stopUiTimer,
                logSink: null);
        }

        public static void StopMiningForShutdown()
        {
            CancellationTokenSource? old;
            lock (_gate)
            {
                old = _cts;
                _cts = null;
                _currentMiner = null;
            }

            try { old?.Cancel(); } catch { }
            try { old?.Dispose(); } catch { }
        }

        private static void StopMiningInternal(
            Button startMiningButton,
            Panel miningStatsPanel,
            TextBlock balanceTextBlock,
            string privateKeyHex,
            Action? stopUiTimer,
            ILogSink? logSink)
        {
            CancellationTokenSource? old;
            lock (_gate)
            {
                old = _cts;
                _cts = null;
                _currentMiner = null;
            }

            try { old?.Cancel(); } catch { }
            try { old?.Dispose(); } catch { }

            UiInvoke(() =>
            {
                startMiningButton.Content = "Start Mining";
                miningStatsPanel.Visibility = Visibility.Collapsed;

                try { stopUiTimer?.Invoke(); } catch { }

                try
                {
                    if (IsValidHex(privateKeyHex, 32))
                        BalanceHelper.UpdateBalanceUI(balanceTextBlock, privateKeyHex);
                }
                catch { }
            });

            logSink?.Info("Mining", "Mining stopped.");
        }

        private static async Task MiningLoopAsync(
            string privateKeyHex,
            MempoolManager mempool,
            ILogSink logSink,
            TextBlock balanceTextBlock,
            CancellationToken token)
        {
            logSink.Info("Mining", "Mining loop started");

            try
            {
                while (true)
                {
                    token.ThrowIfCancellationRequested();

                    Interlocked.Exchange(ref _blockStartUtcTicks, DateTime.UtcNow.Ticks);
                    Interlocked.Exchange(ref _hashCount, 0);

                    var buffer = mempool.GetBuffer();

                    var miner = new Miner(
                        privateKeyHex,
                        () => buffer.GetAllReadyTransactionsSortedByFee(),
                        null, // compatibility parameter, currently unused
                        async (block) => await OnBlockMinedAsync(block, privateKeyHex, mempool, logSink, balanceTextBlock).ConfigureAwait(false),
                        _ => { },
                        () => { Interlocked.Increment(ref _hashCount); }, // hash-rate callback
                        logSink
                    );

                    lock (_gate) _currentMiner = miner;

                    await miner.StartMiningAsync(token).ConfigureAwait(false);

                    token.ThrowIfCancellationRequested();

                    await Task.Delay(150, token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                logSink.Error("Mining", $"Mining loop crashed: {ex}");
            }
            finally
            {
                lock (_gate) _currentMiner = null;
                logSink.Warn("Mining", "Mining loop stopped");
            }
        }

        private static async Task OnBlockMinedAsync(
            Block block,
            string privateKeyHex,
            MempoolManager mempool,
            ILogSink logSink,
            TextBlock balanceTextBlock)
        {
            try
            {
                if (!BlockValidator.ValidateSelfMinedBlock(block, out var reason))
                {
                    logSink.Warn("Mining", $"Rejecting mined block: {reason}");
                    return;
                }

                if (Db.Connection == null)
                {
                    logSink.Error("Mining", "Db.Connection is null (cannot persist block).");
                    return;
                }

                bool extendedCanon = false;
                ulong newCanonHeight = 0;

                lock (Db.Sync)
                {
                    using var tx = Db.Connection.BeginTransaction();

                    if (TryComputeCanonExtensionHeight(block, tx, out newCanonHeight))
                    {
                        block.BlockHeight = newCanonHeight;

                        if (block.BlockHash is not { Length: 32 } || IsZero32(block.BlockHash))
                            block.BlockHash = block.ComputeBlockHash();

                        BlockStore.SaveBlock(block, tx, BlockIndexStore.StatusCanonicalStateValidated);

                        StateApplier.ApplyBlockWithUndo(block, tx);

                        BlockStore.SetCanonicalHashAtHeight(newCanonHeight, block.BlockHash!, tx);
                        MetaStore.Set("LatestBlockHash", Convert.ToHexString(block.BlockHash!).ToLowerInvariant(), tx);
                        MetaStore.Set("LatestHeight", newCanonHeight.ToString(), tx);

                        extendedCanon = true;
                    }
                    else
                    {
                        if (block.BlockHash is not { Length: 32 } || IsZero32(block.BlockHash))
                            block.BlockHash = block.ComputeBlockHash();

                        BlockStore.SaveBlock(block, tx, BlockIndexStore.StatusSideStatelessAccepted);
                    }

                    tx.Commit();
                }

                logSink.Info("Mining", extendedCanon
                    ? $"✅ Mined + stored: h={newCanonHeight}"
                    : $"✅ Mined + stored (side/reorg candidate): h={block.BlockHeight}");

                if (!extendedCanon)
                {
                    try { ChainSelector.MaybeAdoptNewTip(block.BlockHash!, logSink, mempool); }
                    catch { }
                }

                try { mempool.RemoveIncluded(block); } catch { }

                UiBeginInvoke(() =>
                {
                    try
                    {
                        if (Application.Current?.MainWindow is MainWindow mw)
                            mw.RefreshUiAfterNewBlock();

                        BalanceHelper.UpdateBalanceUI(balanceTextBlock, privateKeyHex);
                    }
                    catch { }
                });

                try
                {
                    if (P2PNode.Instance != null)
                        _ = Task.Run(() => P2PNode.Instance.BroadcastBlockAsync(block));
                    else
                        _ = Task.Run(() => BroadcastBlockFallbackAsync(block, logSink));
                }
                catch { }

                Interlocked.Exchange(ref _blockStartUtcTicks, DateTime.UtcNow.Ticks);
                Interlocked.Exchange(ref _hashCount, 0);
            }
            catch (Exception ex)
            {
                logSink.Error("Mining", $"Persist error: {ex}");
            }

            await Task.CompletedTask;
        }

        private static bool TryComputeCanonExtensionHeight(Block blk, SqliteTransaction tx, out ulong newHeight)
        {
            newHeight = 0;

            var prev = blk.Header?.PreviousBlockHash;
            if (prev is not { Length: 32 }) return false;

            ulong tipH = BlockStore.GetLatestHeight(tx);
            var tipHash = BlockStore.GetCanonicalHashAtHeight(tipH, tx);
            if (tipHash is not { Length: 32 }) return false;

            if (!BytesEqual32(prev, tipHash))
                return false;

            newHeight = tipH + 1UL;
            return true;
        }


        private static async Task BroadcastBlockFallbackAsync(Block block, ILogSink log)
        {
            try
            {
                int size = BlockBinarySerializer.GetSize(block);
                var buf = new byte[size];
                _ = BlockBinarySerializer.Write(buf, block);

                var peers = LoadPeers();
                if (peers.Count == 0)
                {
                    log.Warn("Gossip", "No peers to broadcast block.");
                    return;
                }

                using var sem = new SemaphoreSlim(GossipConcurrency);
                var tasks = new List<Task>(peers.Count);

                foreach (var (ip, port) in peers)
                    tasks.Add(SendOneAsync(ip, port, MsgType.Block, buf, sem, log));

                await Task.WhenAll(tasks).ConfigureAwait(false);
                log.Info("Gossip", $"Broadcasted block {block.BlockHeight} to {peers.Count} peer(s).");
            }
            catch (Exception ex)
            {
                log.Error("Gossip", $"Broadcast block error: {ex}");
            }
        }

        private static async Task SendOneAsync(string ip, int port, MsgType type, byte[] payload, SemaphoreSlim sem, ILogSink log)
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
                var handshake = BuildHandshakePayload((ushort)DefaultPeerPort);
                await P2PNode.WriteFrame(ns, MsgType.Handshake, handshake, CancellationToken.None).ConfigureAwait(false);
                await P2PNode.WriteFrame(ns, type, payload, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                log.Warn("Gossip", $"{type} broadcast to {ip}:{port} failed: {ex.Message}");
            }
            finally
            {
                sem.Release();
            }
        }

        private static List<(string ip, int port)> LoadPeers()
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
                    if (port <= 0) port = DefaultPeerPort;
                    if (SelfPeerGuard.IsSelf(ip, port)) continue;

                    list.Add((ip, port));
                }
            }

            return list;
        }


        private static uint GetCurrentNonceUnsafe()
        {
            lock (_gate)
                return _currentMiner?.CurrentNonce ?? 0u;
        }

        private static void UiInvoke(Action a)
        {
            try
            {
                var app = Application.Current;
                if (app?.Dispatcher == null) { a(); return; }

                if (app.Dispatcher.CheckAccess()) a();
                else app.Dispatcher.Invoke(a);
            }
            catch
            {
                try { a(); } catch { }
            }
        }

        private static void UiBeginInvoke(Action a)
        {
            try
            {
                var app = Application.Current;
                if (app?.Dispatcher == null) return;
                if (app.Dispatcher.HasShutdownStarted || app.Dispatcher.HasShutdownFinished) return;
                app.Dispatcher.BeginInvoke(a);
            }
            catch { }
        }

        private static bool BytesEqual32(byte[] a, byte[] b)
        {
            if (a.Length != 32 || b.Length != 32) return false;
            int diff = 0;
            for (int i = 0; i < 32; i++) diff |= a[i] ^ b[i];
            return diff == 0;
        }

        private static bool IsZero32(byte[]? h)
        {
            if (h is not { Length: 32 }) return true;
            for (int i = 0; i < 32; i++) if (h[i] != 0) return false;
            return true;
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
            if (string.IsNullOrWhiteSpace(hex) || hex.Length != expectedBytes * 2) return false;
            for (int i = 0; i < hex.Length; i++)
            {
                char c = hex[i];
                bool ok = (c >= '0' && c <= '9') ||
                          (c >= 'a' && c <= 'f') ||
                          (c >= 'A' && c <= 'F');
                if (!ok) return false;
            }
            return true;
        }

        private static bool IsAllZeroHex(string hex)
        {
            if (string.IsNullOrEmpty(hex)) return true;
            for (int i = 0; i < hex.Length; i++)
                if (hex[i] != '0') return false;
            return true;
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

