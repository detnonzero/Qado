using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using Microsoft.Data.Sqlite;
using Qado.Blockchain;
using Qado.CodeBehindHelper;
using Qado.Logging;
using Qado.Mempool;
using Qado.Networking;
using Qado.Api;
using Qado.Storage;
using Qado.Utils;

namespace Qado
{
    public partial class MainWindow : Window, ILogSink
    {
        public static MainWindow Instance { get; private set; } = null!;

        public ObservableCollection<LogEntry> Logs { get; } = new();

        private readonly ObservableCollection<MempoolRow> _mempoolPreview = new();
        public ObservableCollection<MempoolRow> MempoolPreview => _mempoolPreview;

        private readonly ObservableCollection<PeerRow> _peers = new();

        private readonly ObservableCollection<AccountStateViewModel> _accounts = new();

        private readonly ObservableCollection<TxRow> _txRows = new();
        private readonly ObservableCollection<BlockRow> _blockRows = new();

        private MempoolManager _mempool = null!;
        public MempoolManager Mempool => _mempool;

        private Timer? _uiTimer;
        private Timer? _mempoolCleanupTimer;
        private Timer? _blockExplorerTimer;
        private Timer? _peerReloadDebounceTimer;
        private Timer? _nodeStatusDebounceTimer;

        private CancellationTokenSource? _p2pCts;
        private P2PNode? _p2pNode;
        private ExchangeApiHost? _exchangeApiHost;

        private const int InitialCount = 100;
        private const int PageSize = 50;
        private const int PeerReloadDebounceMs = 300;
        private const int NodeStatusDebounceMs = 400;
        private const int BlockExplorerRefreshDebounceMs = 5000;
        private static readonly TimeSpan MempoolCleanupInterval = TimeSpan.FromSeconds(15);
        private const int MempoolCleanupMaxAgeSeconds = 3600;
        private static readonly TimeSpan PeerSeenRecentlyWindow = TimeSpan.FromMinutes(5);

        private ulong _nextHeightToLoad = 0;
        private bool _isLoading = false;
        private bool _initialized = false;
        private bool _isClosing = false;
        private int _tipAdoptRequested;
        private int _tipAdoptWorkerRunning;
        private int _accountsReloadRequested;
        private int _accountsReloadWorkerRunning;
        private int _peersReloadRequested;
        private int _peersReloadWorkerRunning;
        private int _nodeStatusRefreshRequested;
        private int _nodeStatusRefreshWorkerRunning;
        private int _blockExplorerRefreshRequested;

        private readonly HashSet<ulong> _displayedHeights = new();

        private static readonly object _appLogFileSync = new();
        private static readonly string _appLogPath = Path.Combine(AppContext.BaseDirectory, "data", "app.log");

        public MainWindow()
        {
            Instance = this;

            InitializeComponent();
            DataContext = this;
            PeerHostTextBox.Text = GenesisConfig.GenesisHost;
            PeerPortTextBox.Text = GenesisConfig.P2PPort.ToString();

            if (GuiUtils.IsDesignMode)
                return;

            _peerReloadDebounceTimer = new Timer(_ =>
            {
                if (_isClosing) return;
                try { Dispatcher.BeginInvoke(new Action(ReloadPeers)); } catch { }
            }, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

            _nodeStatusDebounceTimer = new Timer(_ =>
            {
                if (_isClosing) return;
                try { Dispatcher.BeginInvoke(new Action(RefreshNodeStatusHeader)); } catch { }
            }, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

            _blockExplorerTimer = new Timer(_ =>
            {
                if (_isClosing) return;
                if (Interlocked.Exchange(ref _blockExplorerRefreshRequested, 0) == 0) return;
                try { Dispatcher.BeginInvoke(new Action(PrependNewBlocksOrReloadIfReorg)); } catch { }
            }, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

            PeerStore.PeerListChanged += OnPeerListChanged;

            Loaded += async (_, __) =>
            {
                if (_isClosing) return;

                LogListView.ItemsSource = Logs;
                MempoolListView.ItemsSource = _mempoolPreview;
                PeersListView.ItemsSource = _peers;
                StateListView.ItemsSource = _accounts;
                TxListView.ItemsSource = _txRows;
                BlocksListView.ItemsSource = _blockRows;
                BlocksListView.AddHandler(ScrollViewer.ScrollChangedEvent, new ScrollChangedEventHandler(BlockScroll_ScrollChanged));

                ClearLogButton.Click -= ClearLogButton_Click;
                ClearLogButton.Click += ClearLogButton_Click;

                ReloadBlocksButton.Click -= ReloadBlocksButton_Click;
                ReloadBlocksButton.Click += ReloadBlocksButton_Click;

                ConnectPeerButton.Click -= ConnectPeerButton_Click;
                ConnectPeerButton.Click += ConnectPeerButton_Click;

                CopyKeyButton.Click -= CopyKeyButton_Click;
                CopyKeyButton.Click += CopyKeyButton_Click;

                _mempool = new MempoolManager(
                    senderHex => StateStore.GetBalanceU64(senderHex),
                    senderHex => StateStore.GetNonceU64(senderHex),
                    this
                );

                MempoolPreviewHelper.StartMempoolUiUpdater(
                    _mempool,
                    _mempoolPreview,
                    onUiTick: ScheduleNodeStatusRefresh);

                _mempoolCleanupTimer = new Timer(_ =>
                {
                    if (_isClosing) return;

                    try
                    {
                        int evicted = _mempool.EvictStaleAndLowFee(MempoolCleanupMaxAgeSeconds);
                        int purged = _mempool.PurgeInvalid();
                        if (evicted > 0 || purged > 0)
                            ScheduleNodeStatusRefresh();
                    }
                    catch (Exception ex)
                    {
                        Warn("Mempool", $"Cleanup failed: {ex.Message}");
                    }
                }, null, MempoolCleanupInterval, MempoolCleanupInterval);

                PopulateKeyDropdown();

                if (_isClosing) return;

                _p2pCts = new CancellationTokenSource();
                try
                {
                    var node = await GenesisBootstrapper.BootstrapAsync(_mempool, this, _p2pCts.Token);
                    if (_isClosing)
                    {
                        try { node.Stop(); } catch { }
                        return;
                    }

                    _p2pNode = node;
                }
                catch (Exception ex)
                {
                    Warn("Bootstrap", $"Bootstrap failed: {ex.Message}");
                }

                if (_isClosing) return;
                await StartExchangeApiAsync();
                if (_isClosing) return;

                RequestTipAdoption();
                InitializeBlockExplorerLazy();

                LoadAccounts();
                ReloadPeers();
                ScheduleNodeStatusRefresh();

                Info("Startup", "Qado UI initialized.");
            };
        }

        protected override void OnClosed(EventArgs e)
        {
            _isClosing = true;

            _mempoolCleanupTimer?.Dispose();
            _uiTimer?.Dispose();
            _blockExplorerTimer?.Dispose();
            _peerReloadDebounceTimer?.Dispose();
            _peerReloadDebounceTimer = null;
            _nodeStatusDebounceTimer?.Dispose();
            _nodeStatusDebounceTimer = null;
            PeerStore.PeerListChanged -= OnPeerListChanged;
            MiningHelper.StopMiningForShutdown();
            MempoolPreviewHelper.StopMempoolUiUpdater();

            try { _p2pCts?.Cancel(); } catch { }
            try { _p2pNode?.Stop(); } catch { }
            try { _p2pCts?.Dispose(); } catch { }
            _p2pCts = null;
            _p2pNode = null;

            var apiHost = _exchangeApiHost;
            _exchangeApiHost = null;
            if (apiHost != null)
            {
                _ = Task.Run(async () =>
                {
                    try
                    {
                        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                        await apiHost.StopAsync(cts.Token).ConfigureAwait(false);
                    }
                    catch { }
                    try { apiHost.Dispose(); } catch { }
                });
            }

            base.OnClosed(e);

            // Hard fallback: if any component still hangs after window close,
            // force-process-exit so no background process remains.
            _ = Task.Run(async () =>
            {
                try { await Task.Delay(TimeSpan.FromSeconds(8)).ConfigureAwait(false); } catch { }
                try { Environment.Exit(0); } catch { }
            });
        }

        public void Info(string category, string message) => WriteLog("INFO", category, message);

        public void Warn(string category, string message) => WriteLog("WARN", category, message);

        public void Error(string category, string message) => WriteLog("ERROR", category, message);

        private void WriteLog(string level, string category, string message)
        {
            category ??= "";
            message ??= "";

            string uiTs = DateTime.UtcNow.ToString("HH:mm:ss");
            string text = $"[{DateTime.UtcNow:O}] [{level}] [{category}] {message}";

            try
            {
                var dir = Path.GetDirectoryName(_appLogPath);
                if (!string.IsNullOrWhiteSpace(dir))
                    Directory.CreateDirectory(dir);

                lock (_appLogFileSync)
                    File.AppendAllText(_appLogPath, text + Environment.NewLine);
            }
            catch
            {
            }

            Dispatcher.BeginInvoke(new Action(() =>
                Logs.Insert(0, new LogEntry
                {
                    Timestamp = uiTs,
                    Level = level,
                    Category = category,
                    Message = message
                })));
        }


        public class LogEntry
        {
            public string Timestamp { get; set; } = "";
            public string Level { get; set; } = "";
            public string Category { get; set; } = "";
            public string Message { get; set; } = "";
        }

        public void StartUiTimer(Action updateAction)
        {
            _uiTimer = new Timer(_ => Dispatcher.Invoke(updateAction), null, 1000, 1000);
        }

        public void StopUiTimer()
        {
            _uiTimer?.Dispose();
            _uiTimer = null;
        }

        public void AddTxToPreview(Transaction tx)
        {
            if (TransactionValidator.IsCoinbase(tx)) return;
            var row = BuildMempoolRow(tx);

            Dispatcher.BeginInvoke(new Action(() =>
            {
                _mempoolPreview.Insert(0, row);
                if (_mempoolPreview.Count > 1000)
                    _mempoolPreview.RemoveAt(_mempoolPreview.Count - 1);
                ScheduleNodeStatusRefresh();
            }));
        }

        public void RefreshUiAfterNewBlock()
        {
            try
            {
                ScheduleBlockExplorerRefresh();

                Dispatcher.BeginInvoke(new Action(() =>
                {
                    var keyText = PrivateKeyComboBox?.Text?.Trim() ?? "";
                    if (!string.IsNullOrWhiteSpace(keyText))
                        BalanceHelper.UpdateBalanceUI(BalanceTextBlock, keyText);
                }));

                Dispatcher.BeginInvoke(new Action(LoadAccounts));
                Dispatcher.BeginInvoke(new Action(ReloadPeers));
                ScheduleNodeStatusRefresh();

                RequestTipAdoption();
            }
            catch
            {
            }
        }

        private void ScheduleBlockExplorerRefresh()
        {
            if (_isClosing || !_initialized)
                return;

            Interlocked.Exchange(ref _blockExplorerRefreshRequested, 1);
            _blockExplorerTimer?.Change(
                TimeSpan.FromMilliseconds(BlockExplorerRefreshDebounceMs),
                Timeout.InfiniteTimeSpan);
        }

        private void RequestTipAdoption()
        {
            if (_isClosing) return;

            Interlocked.Exchange(ref _tipAdoptRequested, 1);
            if (Interlocked.CompareExchange(ref _tipAdoptWorkerRunning, 1, 0) != 0)
                return;

            _ = Task.Run(() =>
            {
                try
                {
                    while (!_isClosing)
                    {
                        if (Interlocked.Exchange(ref _tipAdoptRequested, 0) == 0)
                            break;

                        TryAdoptCurrentTipCore();
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref _tipAdoptWorkerRunning, 0);

                    if (!_isClosing && Volatile.Read(ref _tipAdoptRequested) != 0)
                        RequestTipAdoption();
                }
            });
        }

        private void TryAdoptCurrentTipCore()
        {
            try
            {
                ulong latest = BlockStore.GetLatestHeight();
                var tip = BlockStore.GetBlockByHeight(latest);
                var cand = tip?.BlockHash;
                if (cand is { Length: 32 })
                    Blockchain.ChainSelector.MaybeAdoptNewTip(cand, this, _mempool);
            }
            catch (Exception ex)
            {
                Warn("ChainSel", $"Tip adoption attempt failed: {ex.Message}");
            }
        }

        private void PrivateKeyComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (GuiUtils.IsDesignMode) return;

            if (PrivateKeyComboBox.SelectedItem is string selected)
            {
                if (selected == "New")
                {
                    PrivateKeyComboBox.IsReadOnly = false;
                    PrivateKeyComboBox.Text = "";
                    AcceptPrivateKeyButton.IsEnabled = true;
                    AcceptPrivateKeyButton.Opacity = 1.0;
                    GeneratePrivateKeyButton.IsEnabled = true;
                    GeneratePrivateKeyButton.Opacity = 1.0;
                    SpendPublicKeyTextBox.Text = "";
                }
                else
                {
                    PrivateKeyComboBox.IsReadOnly = true;
                    PrivateKeyComboBox.Text = selected;
                    SpendPublicKeyTextBox.Text = KeyGenerator.GetPublicKeyFromPrivateKeyHex(selected);
                    AcceptPrivateKeyButton.IsEnabled = false;
                    AcceptPrivateKeyButton.Opacity = 0.5;
                    GeneratePrivateKeyButton.IsEnabled = false;
                    GeneratePrivateKeyButton.Opacity = 0.5;

                    BalanceHelper.UpdateBalanceUI(BalanceTextBlock, selected);
                }
            }
        }

        private void AcceptPrivateKeyButton_Click(object sender, RoutedEventArgs e)
        {
            if (GuiUtils.IsDesignMode) return;

            string privateKey = PrivateKeyComboBox.Text.Trim();

            if (string.IsNullOrWhiteSpace(privateKey) || privateKey.Length != 64 || !KeyStorage.IsHex(privateKey))
            {
                MessageBox.Show("Invalid private key format. Must be 64-character hex.", "QADO", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            try
            {
                var publicKey = KeyGenerator.GetPublicKeyFromPrivateKeyHex(privateKey);
                var keys = KeyStorage.LoadAllPrivateKeys();

                if (!keys.Contains(privateKey))
                {
                    keys.Insert(0, privateKey);
                    KeyStorage.SavePrivateKeys(keys);
                    Info("Keys", "Private key saved.");
                }
                else
                {
                    Info("Keys", "Private key already exists.");
                }

                PopulateKeyDropdown();
                PrivateKeyComboBox.SelectedItem = privateKey;
                PrivateKeyComboBox.IsReadOnly = true;
                AcceptPrivateKeyButton.IsEnabled = false;
                AcceptPrivateKeyButton.Opacity = 0.5;
                GeneratePrivateKeyButton.IsEnabled = false;
                GeneratePrivateKeyButton.Opacity = 0.5;
                SpendPublicKeyTextBox.Text = publicKey;

                BalanceHelper.UpdateBalanceUI(BalanceTextBlock, privateKey);
            }
            catch
            {
                MessageBox.Show("The private key content is invalid or not usable.", "QADO", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void GeneratePrivateKeyButton_Click(object sender, RoutedEventArgs e)
        {
            if (GuiUtils.IsDesignMode) return;

            var newPrivateKey = KeyGenerator.GeneratePrivateKeyHex();
            var keys = KeyStorage.LoadAllPrivateKeys();

            if (!keys.Contains(newPrivateKey))
            {
                keys.Insert(0, newPrivateKey);
                KeyStorage.SavePrivateKeys(keys);
            }

            PopulateKeyDropdown();
            PrivateKeyComboBox.SelectedItem = newPrivateKey;
            PrivateKeyComboBox.Text = newPrivateKey;
            PrivateKeyComboBox.IsReadOnly = true;
            SpendPublicKeyTextBox.Text = KeyGenerator.GetPublicKeyFromPrivateKeyHex(newPrivateKey);
            AcceptPrivateKeyButton.IsEnabled = false;
            AcceptPrivateKeyButton.Opacity = 0.5;
            GeneratePrivateKeyButton.IsEnabled = false;
            GeneratePrivateKeyButton.Opacity = 0.5;

            BalanceHelper.UpdateBalanceUI(BalanceTextBlock, newPrivateKey);
            Info("Key", "New private key generated.");
        }

        private void CopyKeyButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var txt = SpendPublicKeyTextBox.Text?.Trim() ?? "";
                if (!string.IsNullOrWhiteSpace(txt))
                {
                    Clipboard.SetText(txt);
                    Info("UI", "Wallet address copied to clipboard.");
                }
            }
            catch (Exception ex)
            {
                Warn("UI", $"Copy failed: {ex.Message}");
            }
        }

        private void PopulateKeyDropdown()
        {
            if (GuiUtils.IsDesignMode) return;

            var keys = KeyStorage.LoadAllPrivateKeys();
            PrivateKeyComboBox.Items.Clear();
            PrivateKeyComboBox.Items.Add("New");

            foreach (var key in keys)
                PrivateKeyComboBox.Items.Add(key);

            PrivateKeyComboBox.SelectedIndex = 0;
            PrivateKeyComboBox.Text = "";
        }

        private void StartMiningButton_Click(object sender, RoutedEventArgs e)
        {
            if (GuiUtils.IsDesignMode) return;

            string privateKey = PrivateKeyComboBox.Text.Trim();

            if (MiningHelper.IsMiningActive)
            {
                MiningHelper.StopMining(
                    StartMiningButton,
                    MiningStatsPanel,
                    BalanceTextBlock,
                    privateKey,
                    StopUiTimer
                );
            }
            else
            {
                MiningHelper.StartMining(
                    privateKey,
                    Mempool,
                    this,
                    StartMiningButton,
                    MiningStatsPanel,
                    BlockUptimeText,
                    CurrentHashrateText,
                    NonceText,
                    BalanceTextBlock,
                    StartUiTimer
                );
            }
        }

        private void AmountTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
        }

        private async void SendButton_Click(object sender, RoutedEventArgs e)
        {
            if (GuiUtils.IsDesignMode) return;

            try
            {
                string privateKey = PrivateKeyComboBox.Text.Trim();

                var tx = await TransactionHelper.HandleSend(
                    RecipientAddressTextBox,
                    AmountTextBox,
                    FeeTextBox,
                    privateKey,
                    Mempool,
                    this,
                    t => AddTxToPreview(t)
                );

                if (tx != null)
                {
                    RecipientAddressTextBox.Text = "";
                    AmountTextBox.Text = "";
                    FeeTextBox.Text = "";

                    LoadAccounts();
                }
            }
            catch (Exception ex)
            {
                Error("Send", ex.Message);
            }
        }

        private async void ConnectPeerButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                string host = PeerHostTextBox.Text.Trim();
                string portText = PeerPortTextBox.Text.Trim();

                if (string.IsNullOrWhiteSpace(host))
                {
                    Warn("P2P", "Host is empty.");
                    return;
                }

                if (!int.TryParse(portText, out int port) || port <= 0 || port > 65535)
                {
                    Warn("P2P", "Invalid port.");
                    return;
                }

                if (_p2pNode == null)
                {
                    Warn("P2P", "P2P node is not running.");
                    return;
                }

                Info("P2P", $"Dialing peer: {host}:{port}");
                await _p2pNode.ConnectAsync(host, port, _p2pCts?.Token ?? CancellationToken.None);
                SchedulePeerReload();
                ScheduleNodeStatusRefresh();
            }
            catch (Exception ex)
            {
                Warn("P2P", $"Connect failed: {ex.Message}");
            }
        }

        private void OnPeerListChanged()
            => SchedulePeerReload();

        private void SchedulePeerReload()
        {
            if (_isClosing) return;
            try { _peerReloadDebounceTimer?.Change(PeerReloadDebounceMs, Timeout.Infinite); } catch { }
        }

        private void ScheduleNodeStatusRefresh()
        {
            if (_isClosing) return;
            try { _nodeStatusDebounceTimer?.Change(NodeStatusDebounceMs, Timeout.Infinite); } catch { }
        }

        private void ClearLogButton_Click(object sender, RoutedEventArgs e)
        {
            Logs.Clear();
        }

        private void ReloadPeers()
        {
            if (_isClosing) return;

            Interlocked.Exchange(ref _peersReloadRequested, 1);
            if (Interlocked.CompareExchange(ref _peersReloadWorkerRunning, 1, 0) != 0)
                return;

            _ = Task.Run(() =>
            {
                try
                {
                    while (!_isClosing)
                    {
                        if (Interlocked.Exchange(ref _peersReloadRequested, 0) == 0)
                            break;

                        var rows = BuildPeerRowsSnapshot();
                        if (_isClosing) break;

                        try
                        {
                            Dispatcher.BeginInvoke(new Action(() =>
                            {
                                if (_isClosing) return;
                                _peers.Clear();
                                for (int i = 0; i < rows.Count; i++)
                                    _peers.Add(rows[i]);
                                ScheduleNodeStatusRefresh();
                            }));
                        }
                        catch
                        {
                        }
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref _peersReloadWorkerRunning, 0);
                    if (!_isClosing && Volatile.Read(ref _peersReloadRequested) != 0)
                        ReloadPeers();
                }
            });
        }

        private List<PeerRow> BuildPeerRowsSnapshot()
        {
            var rows = new List<PeerRow>();

            if (Db.Connection == null)
            {
                rows.Add(new PeerRow { Status = "Database not initialized yet." });
                return rows;
            }

            try
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = @"
SELECT p.ip, p.port, p.last_seen, IFNULL(a.announced_at, 0)
FROM peers p
LEFT JOIN peer_announced a ON a.id = p.id
ORDER BY
  CASE
    WHEN p.last_seen > 0 THEN p.last_seen
    ELSE IFNULL(a.announced_at, 0)
  END DESC,
  p.ip ASC,
  p.port ASC
LIMIT 200;";
                using var r = cmd.ExecuteReader();
                while (r.Read())
                {
                    string ip = r.GetString(0);
                    int port = r.GetInt32(1);
                    if (SelfPeerGuard.IsSelf(ip, port)) continue;

                    long lastSeenTs = r.GetInt64(2);
                    long announcedTs = r.GetInt64(3);

                    bool hasSeen = lastSeenTs > 0;
                    long displayTs = hasSeen ? lastSeenTs : announcedTs;
                    bool hasAnyTimestamp = displayTs > 0;
                    var seenAt = hasAnyTimestamp ? DateTimeOffset.FromUnixTimeSeconds(displayTs) : DateTimeOffset.UnixEpoch;

                    string lastSeenText;
                    if (!hasAnyTimestamp)
                    {
                        lastSeenText = "-";
                    }
                    else if (hasSeen)
                    {
                        lastSeenText = $"{seenAt.UtcDateTime:yyyy-MM-dd HH:mm:ss} UTC";
                    }
                    else
                    {
                        lastSeenText = $"{seenAt.UtcDateTime:yyyy-MM-dd HH:mm:ss} UTC (announced)";
                    }

                    rows.Add(new PeerRow
                    {
                        Endpoint = $"{ip}:{port}",
                        LastSeenUtc = lastSeenText,
                        Status = hasSeen
                            ? GetPeerStatus(ip, port, seenAt)
                            : GetAnnouncedPeerStatus(ip, port, seenAt, hasAnyTimestamp)
                    });
                }

                if (rows.Count == 0)
                    rows.Add(new PeerRow { Status = "No peers known." });
            }
            catch (SqliteException ex)
            {
                rows.Clear();
                rows.Add(new PeerRow { Status = $"SQLite error: {ex.Message}" });
            }
            catch (Exception ex)
            {
                rows.Clear();
                rows.Add(new PeerRow { Status = $"Error loading peers: {ex.Message}" });
            }

            return rows;
        }

        private string GetPeerStatus(string ip, int port, DateTimeOffset seenAt)
        {
            if (_p2pNode?.IsPeerConnected(ip, port) == true)
                return "Connected";

            bool coolingDown = PeerFailTracker.ShouldBan(ip);

            var age = DateTimeOffset.UtcNow - seenAt;
            bool stale = age > PeerSeenRecentlyWindow;

            if (coolingDown && stale)
                return "Stale (cooling down)";

            if (coolingDown)
                return "Cooling down";

            if (!stale)
                return "Seen recently";

            return "Stale";
        }

        private string GetAnnouncedPeerStatus(string ip, int port, DateTimeOffset announcedAt, bool hasTimestamp)
        {
            if (_p2pNode?.IsPeerConnected(ip, port) == true)
                return "Connected";

            bool coolingDown = PeerFailTracker.ShouldBan(ip);
            if (!hasTimestamp)
                return coolingDown ? "Unverified (cooling down)" : "Unverified";

            bool stale = (DateTimeOffset.UtcNow - announcedAt) > PeerSeenRecentlyWindow;

            if (coolingDown && stale)
                return "Unverified (cooling down)";

            if (coolingDown)
                return "Announced (cooling down)";

            return stale ? "Unverified" : "Announced";
        }

        private void RefreshNodeStatusHeader()
        {
            if (_isClosing) return;

            Interlocked.Exchange(ref _nodeStatusRefreshRequested, 1);
            if (Interlocked.CompareExchange(ref _nodeStatusRefreshWorkerRunning, 1, 0) != 0)
                return;

            _ = Task.Run(() =>
            {
                try
                {
                    while (!_isClosing)
                    {
                        if (Interlocked.Exchange(ref _nodeStatusRefreshRequested, 0) == 0)
                            break;

                        var snap = ReadNodeStatusSnapshot();
                        if (_isClosing) break;

                        try
                        {
                            Dispatcher.BeginInvoke(new Action(() => ApplyNodeStatusSnapshot(snap)));
                        }
                        catch
                        {
                        }
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref _nodeStatusRefreshWorkerRunning, 0);
                    if (!_isClosing && Volatile.Read(ref _nodeStatusRefreshRequested) != 0)
                        RefreshNodeStatusHeader();
                }
            });
        }

        private (ulong tipHeight, string tipHashText, int mempoolCount) ReadNodeStatusSnapshot()
        {
            ulong tipHeight = 0;
            string tipHashText = "-";
            int mempoolCount = 0;

            try
            {
                tipHeight = BlockStore.GetLatestHeight();
                var tipHash = BlockStore.GetCanonicalHashAtHeight(tipHeight);
                if (tipHash is { Length: 32 })
                    tipHashText = Convert.ToHexString(tipHash).ToLowerInvariant();
            }
            catch
            {
            }

            try { mempoolCount = _mempool?.Count ?? 0; } catch { }

            return (tipHeight, tipHashText, mempoolCount);
        }

        private void ApplyNodeStatusSnapshot((ulong tipHeight, string tipHashText, int mempoolCount) snap)
        {
            if (_isClosing) return;

            int peerCount = 0;
            try
            {
                for (int i = 0; i < _peers.Count; i++)
                {
                    if (!string.IsNullOrWhiteSpace(_peers[i].Endpoint))
                        peerCount++;
                }
            }
            catch
            {
            }

            TipHeightText.Text = snap.tipHeight.ToString();
            TipHashText.Text = snap.tipHashText;
            MempoolCountText.Text = snap.mempoolCount.ToString();
            PeerCountText.Text = peerCount.ToString();
        }

        private void LoadAccounts()
        {
            if (_isClosing) return;

            Interlocked.Exchange(ref _accountsReloadRequested, 1);
            if (Interlocked.CompareExchange(ref _accountsReloadWorkerRunning, 1, 0) != 0)
                return;

            _ = Task.Run(() =>
            {
                try
                {
                    while (!_isClosing)
                    {
                        if (Interlocked.Exchange(ref _accountsReloadRequested, 0) == 0)
                            break;

                        var rows = BuildAccountRowsSnapshot(out var errorMessage);
                        if (_isClosing) break;

                        try
                        {
                            Dispatcher.BeginInvoke(new Action(() =>
                            {
                                if (_isClosing) return;

                                _accounts.Clear();
                                for (int i = 0; i < rows.Count; i++)
                                    _accounts.Add(rows[i]);

                                if (!string.IsNullOrWhiteSpace(errorMessage))
                                    Warn("State", errorMessage);
                            }));
                        }
                        catch
                        {
                        }
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref _accountsReloadWorkerRunning, 0);
                    if (!_isClosing && Volatile.Read(ref _accountsReloadRequested) != 0)
                        LoadAccounts();
                }
            });
        }

        private List<AccountStateViewModel> BuildAccountRowsSnapshot(out string? errorMessage)
        {
            errorMessage = null;
            var rows = new List<AccountStateViewModel>(256);

            if (Db.Connection == null)
            {
                rows.Add(new AccountStateViewModel
                {
                    PublicKey = "Database not initialized yet.",
                    Balance = 0UL,
                    Nonce = 0UL
                });
                return rows;
            }

            try
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = "SELECT addr, balance, nonce FROM accounts WHERE 1 LIMIT 1000;";
                using var r = cmd.ExecuteReader();
                while (r.Read())
                {
                    var addr = Convert.ToHexString((byte[])r[0]).ToLowerInvariant();

                    ulong balance = ReadU64Blob(r.GetValue(1));
                    ulong nonce = ReadU64IntegerOrBlob(r.GetValue(2));

                    rows.Add(new AccountStateViewModel
                    {
                        PublicKey = addr,
                        Balance = balance,
                        Nonce = nonce
                    });
                }

                rows.Sort(static (a, b) =>
                {
                    int c = b.Balance.CompareTo(a.Balance); // hard-coded: highest balance first
                    if (c != 0) return c;

                    c = b.Nonce.CompareTo(a.Nonce);
                    if (c != 0) return c;

                    return string.CompareOrdinal(a.PublicKey, b.PublicKey);
                });
            }
            catch (Exception ex)
            {
                rows.Clear();
                errorMessage = $"LoadAccounts failed: {ex.Message}";
            }

            return rows;
        }

        public class AccountStateViewModel
        {
            public string PublicKey { get; set; } = "";
            public ulong Balance { get; set; }
            public ulong Nonce { get; set; }
            public string BalanceFormatted => QadoAmountParser.FormatNanoToQado(Balance);
        }

        public sealed class PeerRow
        {
            public string Endpoint { get; set; } = "";
            public string LastSeenUtc { get; set; } = "";
            public string Status { get; set; } = "";
        }

        public sealed class MempoolRow
        {
            public string From { get; set; } = "";
            public string To { get; set; } = "";
            public string AmountQado { get; set; } = "";
            public string FeeQado { get; set; } = "";
            public ulong Nonce { get; set; }
        }

        public sealed class BlockRow
        {
            public ulong Height { get; set; }
            public string Hash { get; set; } = "";
            public string PreviousHash { get; set; } = "";
            public string TimestampLocal { get; set; } = "";
            public string Miner { get; set; } = "";
            public string Nonce { get; set; } = "";
            public string Difficulty { get; set; } = "";
            public string Version { get; set; } = "";
            public string MerkleRoot { get; set; } = "";
            public int TxCount { get; set; }
            public byte[] HashBytes { get; set; } = Array.Empty<byte>();
            public Block Block { get; set; } = null!;
        }

        public sealed class TxRow
        {
            public int Index { get; set; }
            public string From { get; set; } = "";
            public string To { get; set; } = "";
            public string AmountQado { get; set; } = "";
            public string FeeQado { get; set; } = "";
            public long Nonce { get; set; }
        }

        public void ShowBlockTransactions(Block block)
        {
            if (block == null) return;

            Dispatcher.BeginInvoke(new Action(() =>
            {
                try
                {
                    string hash = Convert.ToHexString(block.BlockHash).ToLowerInvariant();
                    TxHeaderText.Text = $"Block {block.BlockHeight} - {hash}";

                    _txRows.Clear();

                    for (int i = 0; i < block.Transactions.Count; i++)
                    {
                        var tx = block.Transactions[i];

                        _txRows.Add(new TxRow
                        {
                            Index = i,
                            From = Convert.ToHexString(tx.Sender).ToLowerInvariant(),
                            To = Convert.ToHexString(tx.Recipient).ToLowerInvariant(),
                            AmountQado = QadoAmountParser.FormatNanoToQado(tx.Amount) + " QADO",
                            FeeQado = QadoAmountParser.FormatNanoToQado(tx.Fee) + " QADO",
                            Nonce = tx.TxNonce <= long.MaxValue ? (long)tx.TxNonce : long.MaxValue
                        });
                    }

                    SelectTabByHeader("Transactions");
                }
                catch
                {
                }
            }));
        }

        private void SelectTabByHeader(string header)
        {
            if (MainTabs == null) return;

            for (int i = 0; i < MainTabs.Items.Count; i++)
            {
                if (MainTabs.Items[i] is TabItem ti &&
                    ti.Header is string h &&
                    string.Equals(h, header, StringComparison.OrdinalIgnoreCase))
                {
                    MainTabs.SelectedIndex = i;
                    return;
                }
            }
        }

        private void BlocksListView_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (BlocksListView.SelectedItem is BlockRow row && row.Block != null)
                ShowBlockTransactions(row.Block);
        }

        private void InitializeBlockExplorerLazy()
        {
            if (_initialized) return;
            _initialized = true;

            ReloadAllBlocks();
        }

        private void ReloadBlocksButton_Click(object sender, RoutedEventArgs e)
        {
            ReloadAllBlocks();
        }

        public void ReloadAllBlocks()
        {
            if (_isLoading) return;

            _nextHeightToLoad = BlockStore.GetLatestHeight();

            Dispatcher.InvokeAsync(() =>
            {
                _blockRows.Clear();
                _displayedHeights.Clear();
            });

            _ = AppendMore(InitialCount);
        }

        private async Task AppendMore(int count)
        {
            if (_isLoading) return;

            _isLoading = true;

            try
            {
                var startHeight = _nextHeightToLoad;
                var blocks = await Task.Run(() => BlockExplorerHelper.ReadBlocksDescendingFrom(startHeight, count));

                await Dispatcher.InvokeAsync(() =>
                {
                    for (int i = 0; i < blocks.Count; i++)
                    {
                        var b = blocks[i];

                        if (!_displayedHeights.Add(b.BlockHeight))
                            continue;

                        _blockRows.Add(BuildBlockRow(b));
                    }
                });

                if (blocks.Count > 0)
                {
                    ulong min = blocks[0].BlockHeight;
                    for (int i = 1; i < blocks.Count; i++)
                        if (blocks[i].BlockHeight < min) min = blocks[i].BlockHeight;

                    _nextHeightToLoad = min > 0 ? min - 1 : 0;
                }
            }
            finally
            {
                _isLoading = false;
            }
        }

        private async void PrependNewBlocksOrReloadIfReorg()
        {
            if (_isLoading) return;

            var latestHeight = BlockStore.GetLatestHeight();
            var latestBlock = BlockStore.GetBlockByHeight(latestHeight);
            var latestHash = latestBlock?.BlockHash;

            if (_blockRows.Count == 0)
            {
                _nextHeightToLoad = latestHeight;
                await AppendMore(InitialCount);
                return;
            }

            var topRow = _blockRows[0];
            var topH = topRow.Height;
            var topHash = topRow.HashBytes;

            if (latestHeight == topH)
            {
                if (latestHash is not { Length: 32 } || topHash is not { Length: 32 } || !BytesEqual32(latestHash, topHash))
                {
                    ReloadAllBlocks();
                    return;
                }
            }

            if (latestHeight > topH)
            {
                int toFetch = (int)Math.Min((long)(latestHeight - topH), PageSize);
                var newBlocks = await Task.Run(() => BlockExplorerHelper.ReadBlocksDescendingFrom(latestHeight, toFetch));

                await Dispatcher.InvokeAsync(() =>
                {
                    double oldOffset = GetBlocksVerticalOffset();

                    for (int i = newBlocks.Count - 1; i >= 0; i--)
                    {
                        var b = newBlocks[i];
                        if (!_displayedHeights.Add(b.BlockHeight))
                            continue;

                        _blockRows.Insert(0, BuildBlockRow(b));
                    }

                    SetBlocksVerticalOffset(oldOffset);
                });
            }
        }

        private static bool BytesEqual32(byte[] a, byte[] b)
        {
            if (a.Length != 32 || b.Length != 32) return false;
            for (int i = 0; i < 32; i++) if (a[i] != b[i]) return false;
            return true;
        }

        private async void BlockScroll_ScrollChanged(object sender, ScrollChangedEventArgs e)
        {
            double remaining = e.ExtentHeight - (e.VerticalOffset + e.ViewportHeight);

            if (!_isLoading && remaining < 40.0)
            {
                if (_nextHeightToLoad > 0 || HasGenesisMissingInLoadedBlocks())
                    await AppendMore(PageSize);
            }
        }

        private static MempoolRow BuildMempoolRow(Transaction tx)
        {
            return new MempoolRow
            {
                From = ToHex(tx.Sender),
                To = ToHex(tx.Recipient),
                AmountQado = QadoAmountParser.FormatNanoToQado(tx.Amount) + " QADO",
                FeeQado = QadoAmountParser.FormatNanoToQado(tx.Fee) + " QADO",
                Nonce = tx.TxNonce
            };
        }

        private static BlockRow BuildBlockRow(Block block)
        {
            var header = block.Header;
            if (header == null)
            {
                return new BlockRow
                {
                    Height = block.BlockHeight,
                    Hash = ToHex(block.BlockHash),
                    TxCount = block.Transactions?.Count ?? 0,
                    HashBytes = block.BlockHash is null ? Array.Empty<byte>() : (byte[])block.BlockHash.Clone(),
                    Block = block
                };
            }

            string difficultyText;
            try
            {
                BigInteger diff = Difficulty.TargetToDifficulty(header.Target);
                difficultyText = diff.ToString();
            }
            catch
            {
                difficultyText = "1";
            }

            return new BlockRow
            {
                Height = block.BlockHeight,
                Hash = ToHex(block.BlockHash),
                PreviousHash = ToHex(header.PreviousBlockHash),
                TimestampLocal = SafeUnixToLocal(header.Timestamp),
                Miner = ToHex(header.Miner),
                Nonce = header.Nonce.ToString(),
                Difficulty = difficultyText,
                Version = header.Version.ToString(),
                MerkleRoot = ToHex(header.MerkleRoot),
                TxCount = block.Transactions?.Count ?? 0,
                HashBytes = block.BlockHash is null ? Array.Empty<byte>() : (byte[])block.BlockHash.Clone(),
                Block = block
            };
        }

        private static string ToHex(byte[]? value)
            => value == null || value.Length == 0 ? "" : Convert.ToHexString(value).ToLowerInvariant();

        private bool HasGenesisMissingInLoadedBlocks()
        {
            if (_blockRows.Count == 0) return true;

            for (int i = 0; i < _blockRows.Count; i++)
            {
                if (_blockRows[i].Height == 0UL)
                    return false;
            }

            return true;
        }

        private double GetBlocksVerticalOffset()
        {
            var scroll = FindDescendant<ScrollViewer>(BlocksListView);
            return scroll?.VerticalOffset ?? 0.0;
        }

        private void SetBlocksVerticalOffset(double offset)
        {
            var scroll = FindDescendant<ScrollViewer>(BlocksListView);
            scroll?.ScrollToVerticalOffset(offset);
        }

        private static T? FindDescendant<T>(DependencyObject root) where T : DependencyObject
        {
            if (root == null) return null;

            int count = VisualTreeHelper.GetChildrenCount(root);
            for (int i = 0; i < count; i++)
            {
                var child = VisualTreeHelper.GetChild(root, i);
                if (child is T found)
                    return found;

                var nested = FindDescendant<T>(child);
                if (nested != null)
                    return nested;
            }

            return null;
        }

        private static string SafeUnixToLocal(ulong unixSeconds)
        {
            try
            {
                return DateTimeOffset.FromUnixTimeSeconds((long)unixSeconds)
                    .ToLocalTime()
                    .ToString("yyyy-MM-dd HH:mm:ss");
            }
            catch
            {
                return unixSeconds.ToString();
            }
        }

        public void RefreshTip() => PrependNewBlocksOrReloadIfReorg();


        private static ulong ReadU64Blob(object v)
        {
            if (v is DBNull || v is null) return 0UL;

            if (v is byte[] b)
            {
                if (b.Length != 8) return 0UL;
                return System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(b);
            }

            if (v is long l)
                return l < 0 ? 0UL : (ulong)l;

            return 0UL;
        }

        private static ulong ReadU64IntegerOrBlob(object v)
        {
            if (v is DBNull || v is null) return 0UL;

            if (v is long l)
                return l < 0 ? 0UL : (ulong)l;

            if (v is byte[] b)
            {
                if (b.Length != 8) return 0UL;
                return System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(b);
            }

            return 0UL;
        }

        private async Task StartExchangeApiAsync()
        {
            if (_isClosing || _exchangeApiHost is not null) return;

            var port = ExchangeApiHost.ParsePortOrDefault(Environment.GetEnvironmentVariable("QADO_API_PORT"), NetworkParams.ApiPort);

            try
            {
                var host = new ExchangeApiHost(_mempool, () => _p2pNode, this);
                _exchangeApiHost = host;
                await host.StartAsync(port).ConfigureAwait(false);

                if (_isClosing)
                {
                    try
                    {
                        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                        await host.StopAsync(cts.Token).ConfigureAwait(false);
                    }
                    catch { }
                    try { host.Dispose(); } catch { }

                    if (ReferenceEquals(_exchangeApiHost, host))
                        _exchangeApiHost = null;
                    return;
                }

                Info("API", $"Exchange API active on port {port} (without TLS; optional via reverse proxy).");
            }
            catch (Exception ex)
            {
                try { _exchangeApiHost?.Dispose(); } catch { }
                _exchangeApiHost = null;
                Warn("API", $"Exchange API could not start: {ex.Message}");
            }
        }
    }
}


