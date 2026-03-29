using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Input;
using System.Windows.Media;
using Microsoft.Data.Sqlite;
using Qado.Blockchain;
using Qado.CodeBehindHelper;
using Qado.Logging;
using Qado.Mempool;
using Qado.Mining;
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
        private readonly ObservableCollection<PersonalTxRow> _personalTxRows = new();
        private readonly ObservableCollection<AddressBookRow> _addressBookRows = new();
        private readonly ObservableCollection<BlockRow> _blockRows = new();

        private MempoolManager _mempool = null!;
        public MempoolManager Mempool => _mempool;

        private Timer? _uiTimer;
        private Timer? _mempoolCleanupTimer;
        private Timer? _blockExplorerTimer;
        private Timer? _peerReloadDebounceTimer;
        private Timer? _nodeStatusDebounceTimer;
        private Timer? _nodeStatusPollTimer;

        private CancellationTokenSource? _p2pCts;
        private P2PNode? _p2pNode;
        private ExchangeApiHost? _exchangeApiHost;

        private const int InitialCount = 100;
        private const int PageSize = 50;
        private const int PeerReloadDebounceMs = 300;
        private const int NodeStatusDebounceMs = 400;
        private const int BlockExplorerRefreshDebounceMs = 5000;
        private static readonly TimeSpan TrySyncCooldown = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan MempoolCleanupInterval = TimeSpan.FromSeconds(15);
        private static readonly TimeSpan TipAdoptionWarnCooldown = TimeSpan.FromSeconds(10);
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
        private int _nodeStatusTimerArmed;
        private int _nodeStatusRefreshRequested;
        private int _nodeStatusRefreshWorkerRunning;
        private int _blockExplorerRefreshRequested;
        private int _personalTxReloadRequested;
        private int _personalTxReloadWorkerRunning;
        private string _personalTxTargetPubKey = "";
        private RightClickCellContext? _lastRightClickCell;
        private DateTime _lastTrySyncUtc = DateTime.MinValue;
        private DateTime _nextTipAdoptionWarnUtc = DateTime.MinValue;

        private readonly HashSet<ulong> _displayedHeights = new();

        private static readonly object _appLogFileSync = new();
        private static readonly string _appLogPath = Path.Combine(AppContext.BaseDirectory, "data", "app.log");
        private static readonly object _addressBookFileSync = new();
        private static readonly string _addressBookPath = Path.Combine(AppContext.BaseDirectory, "data", "addressbook.txt");
        private const string MiningBackendMetaKey = "MiningBackend";
        private const string MiningOpenClDeviceMetaKey = "MiningOpenClDevice";
        private const string SelectedKeyMetaKey = "SelectedKey";
        private const string SelectedKeyNewValue = "new";

        private sealed class RightClickCellContext
        {
            public RightClickCellContext(ListView listView, object item, int columnIndex)
            {
                ListView = listView;
                Item = item;
                ColumnIndex = columnIndex;
            }

            public ListView ListView { get; }
            public object Item { get; }
            public int ColumnIndex { get; }
        }

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
                Interlocked.Exchange(ref _nodeStatusTimerArmed, 0);
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
                PersonalTxListView.ItemsSource = _personalTxRows;
                AddressBookListView.ItemsSource = _addressBookRows;
                BlocksListView.ItemsSource = _blockRows;
                BlocksListView.AddHandler(ScrollViewer.ScrollChangedEvent, new ScrollChangedEventHandler(BlockScroll_ScrollChanged));

                ClearLogButton.Click -= ClearLogButton_Click;
                ClearLogButton.Click += ClearLogButton_Click;

                ReloadBlocksButton.Click -= ReloadBlocksButton_Click;
                ReloadBlocksButton.Click += ReloadBlocksButton_Click;
                TrySyncButton.Click -= TrySyncButton_Click;
                TrySyncButton.Click += TrySyncButton_Click;

                ConnectPeerButton.Click -= ConnectPeerButton_Click;
                ConnectPeerButton.Click += ConnectPeerButton_Click;

                CopyKeyButton.Click -= CopyKeyButton_Click;
                CopyKeyButton.Click += CopyKeyButton_Click;
                AddressBookAddButton.Click -= AddressBookAddButton_Click;
                AddressBookAddButton.Click += AddressBookAddButton_Click;
                InitializeCopySupport();

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
                InitializeMiningControls();
                if (KeyStore.IsPortablePlaintextModeEnabled)
                {
                    Warn("Security", "Portable plaintext keystore mode is enabled (QADO_KEYSTORE_MODE=portable_plaintext). Private keys are stored unencrypted.");
                }

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
                ReloadPersonalTransactions();
                LoadAddressBook();
                ReloadPeers();
                ScheduleNodeStatusRefresh();
                _nodeStatusPollTimer?.Dispose();
                _nodeStatusPollTimer = new Timer(_ =>
                {
                    if (_isClosing) return;
                    ScheduleNodeStatusRefresh();
                }, null, TimeSpan.Zero, TimeSpan.FromSeconds(1));

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
            _nodeStatusPollTimer?.Dispose();
            _nodeStatusPollTimer = null;
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
            _uiTimer?.Dispose();
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
                Dispatcher.BeginInvoke(new Action(ReloadPersonalTransactions));
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
                if (_p2pNode?.IsInitialBlockSyncActive == true)
                    return;

                ulong latest = BlockStore.GetLatestHeight();
                var tip = BlockStore.GetBlockByHeight(latest);
                var cand = tip?.BlockHash;
                if (cand is { Length: 32 })
                    Blockchain.ChainSelector.MaybeAdoptNewTip(cand, this, _mempool);
            }
            catch (Exception ex)
            {
                var now = DateTime.UtcNow;
                if (now >= _nextTipAdoptionWarnUtc)
                {
                    _nextTipAdoptionWarnUtc = now + TipAdoptionWarnCooldown;
                    Warn("ChainSel", $"Tip adoption attempt failed: {ex.Message}");
                }
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
                    ReloadPersonalTransactions();
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
                    ReloadPersonalTransactions();
                }
            }

            PersistSelectedPrivateKeyNoThrow();
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
                ReloadPersonalTransactions();
                PersistSelectedPrivateKeyNoThrow();
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
            ReloadPersonalTransactions();
            PersistSelectedPrivateKeyNoThrow();
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

            List<string> keys;
            try
            {
                keys = KeyStorage.LoadAllPrivateKeys();
            }
            catch (Exception ex)
            {
                Warn("Keys", $"Could not load private keys from key store: {ex.Message}");
                keys = new List<string>();
            }

            PrivateKeyComboBox.Items.Clear();
            PrivateKeyComboBox.Items.Add("New");

            foreach (var key in keys)
                PrivateKeyComboBox.Items.Add(key);

            string? selectedKey = TryResolveSavedPrivateKey(keys);
            if (!string.IsNullOrWhiteSpace(selectedKey))
            {
                PrivateKeyComboBox.SelectedItem = selectedKey;
            }
            else if (keys.Count > 0)
            {
                PrivateKeyComboBox.SelectedItem = keys[0];
            }
            else
            {
                PrivateKeyComboBox.SelectedItem = "New";
            }
        }

        private string? TryResolveSavedPrivateKey(List<string> keys)
        {
            try
            {
                string? saved = MetaStore.Get(SelectedKeyMetaKey);
                if (string.IsNullOrWhiteSpace(saved))
                    return null;

                if (string.Equals(saved, SelectedKeyNewValue, StringComparison.Ordinal))
                    return "New";

                if (!saved.StartsWith("pub:", StringComparison.Ordinal))
                    return null;

                string savedPubKey = saved[4..];
                for (int i = 0; i < keys.Count; i++)
                {
                    try
                    {
                        string pubKey = KeyGenerator.GetPublicKeyFromPrivateKeyHex(keys[i]);
                        if (string.Equals(pubKey, savedPubKey, StringComparison.OrdinalIgnoreCase))
                            return keys[i];
                    }
                    catch
                    {
                    }
                }
            }
            catch
            {
            }

            return null;
        }

        private void PersistSelectedPrivateKeyNoThrow()
        {
            try
            {
                string? selected = PrivateKeyComboBox?.SelectedItem as string;
                if (string.IsNullOrWhiteSpace(selected) || string.Equals(selected, "New", StringComparison.Ordinal))
                {
                    MetaStore.Set(SelectedKeyMetaKey, SelectedKeyNewValue);
                    return;
                }

                string pubKey = KeyGenerator.GetPublicKeyFromPrivateKeyHex(selected).ToLowerInvariant();
                MetaStore.Set(SelectedKeyMetaKey, "pub:" + pubKey);
            }
            catch
            {
            }
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
                var backendKind = GetSelectedMiningBackendKind();
                var openClDevice = backendKind == MiningBackendKind.OpenCl
                    ? (MiningDeviceComboBox.SelectedItem as OpenClMiningDevice)
                    : null;

                MiningHelper.StartMining(
                    privateKey,
                    backendKind,
                    openClDevice,
                    Mempool,
                    this,
                    StartMiningButton,
                    MiningStatsPanel,
                    NetworkHashrateText,
                    BlockUptimeText,
                    CurrentHashrateText,
                    NonceText,
                    NextBlockProbabilityText,
                    BalanceTextBlock,
                    StartUiTimer
                );
            }
        }

        private void InitializeMiningControls()
        {
            try
            {
                MiningBackendComboBox.Items.Clear();
                MiningBackendComboBox.Items.Add("CPU");
                MiningBackendComboBox.Items.Add("OpenCL");

                string? savedBackend = null;
                string? savedDeviceId = null;
                try
                {
                    savedBackend = MetaStore.Get(MiningBackendMetaKey);
                    savedDeviceId = MetaStore.Get(MiningOpenClDeviceMetaKey);
                }
                catch
                {
                }

                MiningBackendComboBox.SelectedItem = string.Equals(savedBackend, "opencl", StringComparison.OrdinalIgnoreCase)
                    ? "OpenCL"
                    : "CPU";

                RefreshMiningDevices(savedDeviceId);
                ApplyMiningBackendSelectionUi();
            }
            catch (Exception ex)
            {
                Warn("Mining", $"Failed to initialize mining controls: {ex.Message}");
            }
        }

        private void RefreshMiningDevices(string? preferredDeviceId = null)
        {
            try
            {
                MiningDeviceComboBox.Items.Clear();

                string? preferred = preferredDeviceId;
                if (string.IsNullOrWhiteSpace(preferred))
                    preferred = GetSavedMiningDeviceIdNoThrow();

                switch (GetSelectedMiningBackendKind())
                {
                    case MiningBackendKind.OpenCl:
                    {
                        var devices = OpenClDiscovery.DiscoverDevices(this);
                        for (int i = 0; i < devices.Count; i++)
                            MiningDeviceComboBox.Items.Add(devices[i]);
                        SelectPreferredMiningDevice(preferred, item => (item as OpenClMiningDevice)?.Id);
                        return;
                    }
                    default:
                        MiningDeviceComboBox.SelectedItem = null;
                        return;
                }
            }
            catch (Exception ex)
            {
                Warn("Mining", $"Mining device refresh failed: {ex.Message}");
            }
        }

        private void MiningBackendComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (GuiUtils.IsDesignMode) return;

            if (GetSelectedMiningBackendKind() != MiningBackendKind.Cpu)
                RefreshMiningDevices();

            ApplyMiningBackendSelectionUi();
            PersistMiningPreferencesNoThrow();
        }

        private void MiningDeviceComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (GuiUtils.IsDesignMode) return;
            PersistMiningPreferencesNoThrow();
        }

        private void ApplyMiningBackendSelectionUi()
        {
            if (MiningDevicePanel == null || MiningDeviceLabelText == null)
                return;

            var backendKind = GetSelectedMiningBackendKind();
            MiningDevicePanel.Visibility = backendKind == MiningBackendKind.Cpu
                ? Visibility.Collapsed
                : Visibility.Visible;
            MiningDeviceLabelText.Text = "OpenCL Device";
        }

        private MiningBackendKind GetSelectedMiningBackendKind()
        {
            string selected = MiningBackendComboBox?.SelectedItem as string ?? "CPU";
            return string.Equals(selected, "OpenCL", StringComparison.OrdinalIgnoreCase)
                ? MiningBackendKind.OpenCl
                : MiningBackendKind.Cpu;
        }

        private void PersistMiningPreferencesNoThrow()
        {
            try
            {
                var backendKind = GetSelectedMiningBackendKind();
                MetaStore.Set(
                    MiningBackendMetaKey,
                    backendKind == MiningBackendKind.OpenCl
                        ? "opencl"
                        : "cpu");

                if (backendKind == MiningBackendKind.OpenCl)
                {
                    var selectedOpenClDevice = MiningDeviceComboBox?.SelectedItem as OpenClMiningDevice;
                    MetaStore.Set(MiningOpenClDeviceMetaKey, selectedOpenClDevice?.Id ?? string.Empty);
                }
            }
            catch
            {
            }
        }

        private string? GetSavedMiningDeviceIdNoThrow()
        {
            try
            {
                return MetaStore.Get(MiningOpenClDeviceMetaKey);
            }
            catch
            {
                return null;
            }
        }

        private void SelectPreferredMiningDevice(string? preferredDeviceId, Func<object?, string?> getDeviceId)
        {
            if (MiningDeviceComboBox.Items.Count == 0)
            {
                MiningDeviceComboBox.SelectedItem = null;
                return;
            }

            object? selected = null;
            if (!string.IsNullOrWhiteSpace(preferredDeviceId))
            {
                for (int i = 0; i < MiningDeviceComboBox.Items.Count; i++)
                {
                    var candidate = MiningDeviceComboBox.Items[i];
                    if (string.Equals(getDeviceId(candidate), preferredDeviceId, StringComparison.Ordinal))
                    {
                        selected = candidate;
                        break;
                    }
                }
            }

            MiningDeviceComboBox.SelectedItem = selected ?? MiningDeviceComboBox.Items[0];
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
            Interlocked.Exchange(ref _nodeStatusRefreshRequested, 1);
            if (Interlocked.CompareExchange(ref _nodeStatusTimerArmed, 1, 0) != 0)
                return;

            try
            {
                _nodeStatusDebounceTimer?.Change(NodeStatusDebounceMs, Timeout.Infinite);
            }
            catch
            {
                Interlocked.Exchange(ref _nodeStatusTimerArmed, 0);
            }
        }

        private void ClearLogButton_Click(object sender, RoutedEventArgs e)
        {
            Logs.Clear();
        }

        private void AddressBookAddButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                string normalized = NormalizeHex64(AddressBookInputTextBox?.Text);
                if (string.IsNullOrWhiteSpace(normalized) || IsZeroHex64(normalized))
                {
                    if (AddressBookStatusText != null)
                        AddressBookStatusText.Text = "Invalid pubkey: must be 64-char hex (32 bytes).";
                    Warn("AddressBook", "Rejected invalid pubkey (expected 64-char hex32).");
                    return;
                }

                if (!TryAddAddressBookEntry(normalized, out var message))
                {
                    if (AddressBookStatusText != null)
                        AddressBookStatusText.Text = message;
                    return;
                }

                if (AddressBookInputTextBox != null)
                    AddressBookInputTextBox.Text = "";
                if (AddressBookStatusText != null)
                    AddressBookStatusText.Text = message;
            }
            catch (Exception ex)
            {
                if (AddressBookStatusText != null)
                    AddressBookStatusText.Text = $"Add failed: {ex.Message}";
                Warn("AddressBook", $"Add failed: {ex.Message}");
            }
        }

        private void AddressBookDeleteButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                string normalized = "";
                if (sender is FrameworkElement fe)
                {
                    if (fe.Tag is string tag)
                        normalized = NormalizeHex64(tag);

                    if (normalized.Length == 0 && fe.DataContext is AddressBookRow row)
                        normalized = NormalizeHex64(row.PublicKey);
                }

                if (normalized.Length != 64)
                {
                    if (AddressBookStatusText != null)
                        AddressBookStatusText.Text = "Delete failed: invalid row key.";
                    return;
                }

                if (!TryRemoveAddressBookEntry(normalized, out var message))
                {
                    if (AddressBookStatusText != null)
                        AddressBookStatusText.Text = message;
                    return;
                }

                if (AddressBookStatusText != null)
                    AddressBookStatusText.Text = message;
            }
            catch (Exception ex)
            {
                if (AddressBookStatusText != null)
                    AddressBookStatusText.Text = $"Delete failed: {ex.Message}";
                Warn("AddressBook", $"Delete failed: {ex.Message}");
            }
        }

        private void LoadAddressBook()
        {
            _addressBookRows.Clear();
            var entries = ReadAddressBookEntries();
            for (int i = 0; i < entries.Count; i++)
                _addressBookRows.Add(new AddressBookRow { PublicKey = entries[i] });

            AddressBookStatusText.Text = _addressBookRows.Count == 0
                ? "No addresses yet."
                : $"{_addressBookRows.Count} address(es).";
        }

        private bool TryAddAddressBookEntry(string pubKeyHex, out string message)
        {
            for (int i = 0; i < _addressBookRows.Count; i++)
            {
                if (string.Equals(_addressBookRows[i].PublicKey, pubKeyHex, StringComparison.Ordinal))
                {
                    message = "Address already exists.";
                    return false;
                }
            }

            int insertAt = 0;
            while (insertAt < _addressBookRows.Count &&
                   string.CompareOrdinal(_addressBookRows[insertAt].PublicKey, pubKeyHex) < 0)
            {
                insertAt++;
            }

            _addressBookRows.Insert(insertAt, new AddressBookRow { PublicKey = pubKeyHex });
            SaveAddressBook_NoThrow();
            message = $"Address added ({_addressBookRows.Count} total).";
            Info("AddressBook", $"Address added: {pubKeyHex}");
            return true;
        }

        private bool TryRemoveAddressBookEntry(string pubKeyHex, out string message)
        {
            int index = -1;
            for (int i = 0; i < _addressBookRows.Count; i++)
            {
                if (string.Equals(_addressBookRows[i].PublicKey, pubKeyHex, StringComparison.Ordinal))
                {
                    index = i;
                    break;
                }
            }

            if (index < 0)
            {
                message = "Address not found.";
                return false;
            }

            _addressBookRows.RemoveAt(index);
            SaveAddressBook_NoThrow();

            message = _addressBookRows.Count == 0
                ? "No addresses yet."
                : $"Address removed ({_addressBookRows.Count} total).";
            Info("AddressBook", $"Address removed: {pubKeyHex}");
            return true;
        }

        private static List<string> ReadAddressBookEntries()
        {
            var rows = new List<string>(128);

            try
            {
                lock (_addressBookFileSync)
                {
                    if (!File.Exists(_addressBookPath))
                        return rows;

                    var seen = new HashSet<string>(StringComparer.Ordinal);
                    var lines = File.ReadAllLines(_addressBookPath);
                    for (int i = 0; i < lines.Length; i++)
                    {
                        string normalized = NormalizeHex64(lines[i]);
                        if (normalized.Length != 64 || IsZeroHex64(normalized))
                            continue;

                        if (seen.Add(normalized))
                            rows.Add(normalized);
                    }
                }

                rows.Sort(StringComparer.Ordinal);
            }
            catch
            {
            }

            return rows;
        }

        private void SaveAddressBook_NoThrow()
        {
            try
            {
                string? dir = Path.GetDirectoryName(_addressBookPath);
                if (!string.IsNullOrWhiteSpace(dir))
                    Directory.CreateDirectory(dir);

                var lines = new string[_addressBookRows.Count];
                for (int i = 0; i < _addressBookRows.Count; i++)
                    lines[i] = _addressBookRows[i].PublicKey;

                lock (_addressBookFileSync)
                    File.WriteAllLines(_addressBookPath, lines);
            }
            catch (Exception ex)
            {
                Warn("AddressBook", $"Save failed: {ex.Message}");
            }
        }

        private void InitializeCopySupport()
        {
            AttachListViewCopySupport(BlocksListView);
            AttachListViewCopySupport(MempoolListView);
            AttachListViewCopySupport(PeersListView);
            AttachListViewCopySupport(StateListView);
            AttachListViewCopySupport(TxListView);
            AttachListViewCopySupport(PersonalTxListView);
            AttachListViewCopySupport(AddressBookListView);
            AttachListViewCopySupport(LogListView);

            AttachTextBlockCopySupport(TipHeightText);
            AttachTextBlockCopySupport(TipHashText);
            AttachTextBlockCopySupport(BalanceTextBlock);
            AttachTextBlockCopySupport(MempoolCountText);
            AttachTextBlockCopySupport(PeerCountText);
            AttachTextBlockCopySupport(NetworkHashrateText);
            AttachTextBlockCopySupport(BlockUptimeText);
            AttachTextBlockCopySupport(CurrentHashrateText);
            AttachTextBlockCopySupport(NonceText);
            AttachTextBlockCopySupport(NextBlockProbabilityText);
            AttachTextBlockCopySupport(SendStatusText);
            AttachTextBlockCopySupport(TxHeaderText);
            AttachTextBlockCopySupport(PersonalTxHeaderText);
            AttachTextBlockCopySupport(AddressBookStatusText);
        }

        private void AttachListViewCopySupport(ListView listView)
        {
            if (listView == null)
                return;

            listView.PreviewMouseRightButtonDown -= ListView_PreviewMouseRightButtonDown;
            listView.PreviewMouseRightButtonDown += ListView_PreviewMouseRightButtonDown;

            var menu = new ContextMenu();
            var copyCellItem = new MenuItem { Header = "Copy cell" };
            copyCellItem.Click += (_, __) => CopySingleCellFromListView(listView);
            menu.Items.Add(copyCellItem);
            menu.Opened += (_, __) => copyCellItem.IsEnabled = HasCopyableCellContext(listView);
            listView.ContextMenu = menu;
        }

        private void ListView_PreviewMouseRightButtonDown(object sender, MouseButtonEventArgs e)
        {
            if (sender is not ListView listView)
                return;

            DependencyObject? current = e.OriginalSource as DependencyObject;
            var item = FindAncestor<ListViewItem>(current);
            if (item is null)
            {
                _lastRightClickCell = null;
                return;
            }

            object? rowItem = listView.ItemContainerGenerator.ItemFromContainer(item);
            if (ReferenceEquals(rowItem, DependencyProperty.UnsetValue) || rowItem is null)
                rowItem = item.DataContext;
            if (rowItem is null)
            {
                _lastRightClickCell = null;
                return;
            }

            int columnIndex = ResolveClickedGridColumnIndex(listView, item, e);
            _lastRightClickCell = new RightClickCellContext(listView, rowItem, columnIndex);

            if (item.IsSelected)
                return;

            if ((Keyboard.Modifiers & ModifierKeys.Control) == 0)
                listView.SelectedItems.Clear();

            item.IsSelected = true;
        }

        private bool HasCopyableCellContext(ListView listView)
        {
            if (_lastRightClickCell is null)
                return false;
            if (!ReferenceEquals(_lastRightClickCell.ListView, listView))
                return false;
            if (_lastRightClickCell.Item is null)
                return false;

            if (_lastRightClickCell.ColumnIndex < 0)
                return true;

            return listView.View is GridView gv &&
                   _lastRightClickCell.ColumnIndex < gv.Columns.Count;
        }

        private void CopySingleCellFromListView(ListView listView)
        {
            if (listView == null || _lastRightClickCell is null)
                return;

            if (!ReferenceEquals(_lastRightClickCell.ListView, listView))
                return;

            string text;
            if (listView.View is GridView gv &&
                gv.Columns.Count > 0 &&
                _lastRightClickCell.ColumnIndex >= 0 &&
                _lastRightClickCell.ColumnIndex < gv.Columns.Count)
            {
                text = GetGridCellText(_lastRightClickCell.Item, gv.Columns[_lastRightClickCell.ColumnIndex]);
            }
            else
            {
                text = SanitizeClipboardCell(_lastRightClickCell.Item?.ToString() ?? "");
            }

            try
            {
                Clipboard.SetText(text);
            }
            catch (Exception ex)
            {
                Warn("UI", $"Copy failed: {ex.Message}");
            }
        }

        private static int ResolveClickedGridColumnIndex(ListView listView, ListViewItem item, MouseButtonEventArgs e)
        {
            if (listView.View is not GridView gv || gv.Columns.Count == 0)
                return -1;

            var src = e.OriginalSource as DependencyObject;
            var presenter = FindAncestor<GridViewRowPresenter>(src) ?? FindDescendant<GridViewRowPresenter>(item);
            if (presenter is null)
                return -1;

            double x = e.GetPosition(presenter).X;
            if (x < 0)
                return -1;

            double cursor = 0;
            for (int i = 0; i < gv.Columns.Count; i++)
            {
                var col = gv.Columns[i];
                double width = col.ActualWidth;
                if (width <= 0 || double.IsNaN(width))
                {
                    width = col.Width;
                    if (width <= 0 || double.IsNaN(width))
                        width = 0;
                }

                if (x >= cursor && x < cursor + width)
                    return i;

                cursor += width;
            }

            if (gv.Columns.Count > 0 && x >= cursor - 1)
                return gv.Columns.Count - 1;

            return -1;
        }

        private static T? FindAncestor<T>(DependencyObject? start) where T : DependencyObject
        {
            var current = start;
            while (current != null)
            {
                if (current is T matched)
                    return matched;
                current = VisualTreeHelper.GetParent(current);
            }

            return null;
        }

        private void AttachTextBlockCopySupport(TextBlock textBlock)
        {
            if (textBlock == null)
                return;

            var menu = new ContextMenu();
            var copyItem = new MenuItem { Header = "Copy text" };
            copyItem.Click += (_, __) =>
            {
                var text = textBlock.Text ?? "";
                if (string.IsNullOrWhiteSpace(text))
                    return;

                try
                {
                    Clipboard.SetText(text);
                }
                catch (Exception ex)
                {
                    Warn("UI", $"Copy failed: {ex.Message}");
                }
            };

            menu.Items.Add(copyItem);
            textBlock.ContextMenu = menu;
        }

        private static string GetGridCellText(object? item, GridViewColumn column)
        {
            if (item == null)
                return "";

            if (column.DisplayMemberBinding is Binding binding)
            {
                string path = binding.Path?.Path ?? "";
                if (!string.IsNullOrWhiteSpace(path))
                    return SanitizeClipboardCell(ReadPropertyPath(item, path));
            }

            return SanitizeClipboardCell(item.ToString() ?? "");
        }

        private static string ReadPropertyPath(object root, string path)
        {
            object? current = root;
            var segments = path.Split('.', StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < segments.Length; i++)
            {
                if (current == null)
                    return "";

                var prop = current.GetType().GetProperty(segments[i]);
                if (prop == null)
                    return "";

                current = prop.GetValue(current);
            }

            return current?.ToString() ?? "";
        }

        private static string SanitizeClipboardCell(string value)
            => (value ?? "").Replace('\t', ' ').Replace('\r', ' ').Replace('\n', ' ');

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
                lock (Db.Sync)
                {
                    using var cmd = Db.Connection.CreateCommand();
                    cmd.CommandText = @"
SELECT p.ip,
       p.port,
       MAX(p.last_seen) AS last_seen,
       MAX(IFNULL(a.announced_at, 0)) AS announced_at
FROM peers p
LEFT JOIN peer_announced a ON a.id = p.id
GROUP BY p.ip, p.port
ORDER BY
  CASE
    WHEN MAX(p.last_seen) > 0 THEN MAX(p.last_seen)
    ELSE MAX(IFNULL(a.announced_at, 0))
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

                        bool connected = _p2pNode?.IsPeerConnected(ip, port) == true;
                        rows.Add(new PeerRow
                        {
                            Endpoint = $"{ip}:{port}",
                            LastSeenUtc = lastSeenText,
                            LatencyMs = GetPeerLatencyText(ip, port, connected),
                            Status = hasSeen
                                ? GetPeerStatus(ip, port, seenAt, connected)
                                : GetAnnouncedPeerStatus(ip, port, seenAt, hasAnyTimestamp, connected)
                        });
                    }
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

        private string GetPeerStatus(string ip, int port, DateTimeOffset seenAt, bool connected)
        {
            if (connected)
                return GetConnectedPeerStatus(ip, port);

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

        private string GetAnnouncedPeerStatus(string ip, int port, DateTimeOffset announcedAt, bool hasTimestamp, bool connected)
        {
            if (connected)
                return GetConnectedPeerStatus(ip, port);

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

        private string GetPeerLatencyText(string ip, int port, bool connected)
        {
            if (!connected)
                return "-";

            int? latencyMs = _p2pNode?.GetPeerLatencyMs(ip, port);
            if (latencyMs is int ms && ms >= 0)
                return ms.ToString();

            return "...";
        }

        private string GetConnectedPeerStatus(string ip, int port)
        {
            string? direction = _p2pNode?.GetPeerDirectionText(ip, port);
            bool? isPublic = _p2pNode?.GetPeerPublicStatus(ip, port);

            if (!string.IsNullOrWhiteSpace(direction))
            {
                if (isPublic == true)
                    return $"Connected ({direction}, public)";
                if (isPublic == false)
                    return $"Connected ({direction}, non-public)";
                return $"Connected ({direction})";
            }

            if (isPublic == true)
                return "Connected (public)";
            if (isPublic == false)
                return "Connected (non-public)";
            return "Connected";
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

        private (ulong height, string tipHashText, int mempoolCount) ReadNodeStatusSnapshot()
        {
            ulong height = 0;
            string tipHashText = "-";
            int mempoolCount = 0;

            try
            {
                height = BlockStore.GetLatestHeight();
                var tipHash = BlockStore.GetCanonicalHashAtHeight(height);
                if (tipHash is { Length: 32 })
                    tipHashText = Convert.ToHexString(tipHash).ToLowerInvariant();
            }
            catch
            {
            }

            try { mempoolCount = _mempool?.Count ?? 0; } catch { }

            return (height, tipHashText, mempoolCount);
        }

        private void ApplyNodeStatusSnapshot((ulong height, string tipHashText, int mempoolCount) snap)
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

            TipHeightText.Text = snap.height.ToString();
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

        private void ReloadPersonalTransactions()
        {
            if (_isClosing) return;

            _personalTxTargetPubKey = NormalizeHex64(SpendPublicKeyTextBox?.Text);
            Interlocked.Exchange(ref _personalTxReloadRequested, 1);
            if (Interlocked.CompareExchange(ref _personalTxReloadWorkerRunning, 1, 0) != 0)
                return;

            _ = Task.Run(() =>
            {
                try
                {
                    while (!_isClosing)
                    {
                        if (Interlocked.Exchange(ref _personalTxReloadRequested, 0) == 0)
                            break;

                        string selectedPubKey = _personalTxTargetPubKey;
                        var rows = BuildPersonalTxRowsSnapshot(selectedPubKey, out var headerText, out var errorMessage);
                        if (_isClosing) break;

                        try
                        {
                            Dispatcher.BeginInvoke(new Action(() =>
                            {
                                if (_isClosing) return;

                                PersonalTxHeaderText.Text = headerText;
                                _personalTxRows.Clear();
                                for (int i = 0; i < rows.Count; i++)
                                    _personalTxRows.Add(rows[i]);

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
                    Interlocked.Exchange(ref _personalTxReloadWorkerRunning, 0);
                    if (!_isClosing && Volatile.Read(ref _personalTxReloadRequested) != 0)
                        ReloadPersonalTransactions();
                }
            });
        }

        private List<PersonalTxRow> BuildPersonalTxRowsSnapshot(string selectedPubKey, out string headerText, out string? errorMessage)
        {
            errorMessage = null;
            var rows = new List<PersonalTxRow>(256);

            if (string.IsNullOrWhiteSpace(selectedPubKey))
            {
                headerText = "Select a key to view personal transactions.";
                return rows;
            }

            if (Db.Connection == null)
            {
                headerText = $"Selected account: {selectedPubKey} (database not initialized yet).";
                return rows;
            }

            ulong runningBalance = 0UL;

            try
            {
                lock (Db.Sync)
                {
                    ulong tipHeight = BlockStore.GetLatestHeight();

                    for (ulong h = 0; ; h++)
                    {
                        var hash = BlockStore.GetCanonicalHashAtHeight(h);
                        if (hash is { Length: 32 })
                        {
                            var block = BlockStore.GetBlockByHash(hash);
                            if (block?.Transactions is { Count: > 0 })
                            {
                                ulong blockTs = block.Header?.Timestamp ?? 0UL;
                                for (int i = 0; i < block.Transactions.Count; i++)
                                {
                                    var tx = block.Transactions[i];
                                    if (tx == null) continue;

                                    string from = ToHex(tx.Sender);
                                    string to = ToHex(tx.Recipient);
                                    bool senderMatch = string.Equals(from, selectedPubKey, StringComparison.Ordinal);
                                    bool recipientMatch = string.Equals(to, selectedPubKey, StringComparison.Ordinal);

                                    if (!senderMatch && !recipientMatch)
                                        continue;

                                    if (TransactionValidator.IsCoinbase(tx))
                                    {
                                        if (recipientMatch)
                                            runningBalance = AddU64Saturating(runningBalance, tx.Amount);
                                    }
                                    else
                                    {
                                        if (senderMatch)
                                        {
                                            if (TryAddU64(tx.Amount, tx.Fee, out var totalCost))
                                            {
                                                runningBalance = runningBalance >= totalCost
                                                    ? runningBalance - totalCost
                                                    : 0UL;
                                            }
                                            else
                                            {
                                                runningBalance = 0UL;
                                            }
                                        }

                                        if (recipientMatch)
                                            runningBalance = AddU64Saturating(runningBalance, tx.Amount);
                                    }

                                    rows.Add(new PersonalTxRow
                                    {
                                        TimestampUtc = SafeUnixToUtc(blockTs),
                                        Type = senderMatch ? "sent" : "received",
                                        From = from,
                                        To = to,
                                        AmountQado = QadoAmountParser.FormatNanoToQado(tx.Amount),
                                        AmountForeground = senderMatch ? Brushes.Red : Brushes.Green,
                                        FeeQado = QadoAmountParser.FormatNanoToQado(tx.Fee),
                                        NewBalanceQado = QadoAmountParser.FormatNanoToQado(runningBalance),
                                        Nonce = tx.TxNonce <= long.MaxValue ? (long)tx.TxNonce : long.MaxValue,
                                        BlockTimestampUnix = blockTs,
                                        BlockHeight = h,
                                        TxIndex = i
                                    });
                                }
                            }
                        }

                        if (h == tipHeight)
                            break;
                    }
                }

                rows.Sort(static (a, b) =>
                {
                    int c = b.BlockTimestampUnix.CompareTo(a.BlockTimestampUnix);
                    if (c != 0) return c;

                    c = b.Nonce.CompareTo(a.Nonce);
                    if (c != 0) return c;

                    c = b.BlockHeight.CompareTo(a.BlockHeight);
                    if (c != 0) return c;

                    return b.TxIndex.CompareTo(a.TxIndex);
                });

                headerText = $"Selected account: {selectedPubKey} ({rows.Count} confirmed tx).";
            }
            catch (Exception ex)
            {
                rows.Clear();
                headerText = $"Selected account: {selectedPubKey}";
                errorMessage = $"Load personal transactions failed: {ex.Message}";
            }

            return rows;
        }

        private static string NormalizeHex64(string? value)
        {
            var hex = (value ?? "").Trim().ToLowerInvariant();
            if (hex.Length != 64) return "";

            for (int i = 0; i < hex.Length; i++)
            {
                char c = hex[i];
                bool isHex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
                if (!isHex) return "";
            }

            return hex;
        }

        private static bool IsZeroHex64(string hex)
        {
            if (hex is null || hex.Length != 64)
                return false;

            for (int i = 0; i < hex.Length; i++)
            {
                if (hex[i] != '0')
                    return false;
            }

            return true;
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
                lock (Db.Sync)
                {
                    using var cmd = Db.Connection.CreateCommand();
                    cmd.CommandText = "SELECT addr, balance, nonce FROM accounts WHERE 1 LIMIT 1000;";
                    using var r = cmd.ExecuteReader();
                    if (r.FieldCount < 3)
                    {
                        errorMessage = $"LoadAccounts failed: accounts schema mismatch (expected 3 columns, got {r.FieldCount}).";
                        return rows;
                    }

                    while (r.Read())
                    {
                        byte[]? addrBytes = null;
                        try { addrBytes = r.GetValue(0) as byte[]; } catch { }
                        if (addrBytes is not { Length: > 0 })
                            continue;

                        object balanceRaw;
                        object nonceRaw;
                        try { balanceRaw = r.IsDBNull(1) ? DBNull.Value : r.GetValue(1); } catch { balanceRaw = DBNull.Value; }
                        try { nonceRaw = r.IsDBNull(2) ? DBNull.Value : r.GetValue(2); } catch { nonceRaw = DBNull.Value; }

                        var addr = Convert.ToHexString(addrBytes).ToLowerInvariant();
                        ulong balance = ReadU64Blob(balanceRaw);
                        ulong nonce = ReadU64IntegerOrBlob(nonceRaw);

                        rows.Add(new AccountStateViewModel
                        {
                            PublicKey = addr,
                            Balance = balance,
                            Nonce = nonce
                        });
                    }
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
            public string LatencyMs { get; set; } = "-";
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

        public sealed class PersonalTxRow
        {
            public string TimestampUtc { get; set; } = "";
            public string Type { get; set; } = "";
            public string From { get; set; } = "";
            public string To { get; set; } = "";
            public string AmountQado { get; set; } = "";
            public Brush AmountForeground { get; set; } = Brushes.Black;
            public string FeeQado { get; set; } = "";
            public string NewBalanceQado { get; set; } = "";
            public long Nonce { get; set; }

            public ulong BlockTimestampUnix { get; set; }
            public ulong BlockHeight { get; set; }
            public int TxIndex { get; set; }
        }

        public sealed class AddressBookRow
        {
            public string PublicKey { get; set; } = "";
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
                            AmountQado = QadoAmountParser.FormatNanoToQado(tx.Amount),
                            FeeQado = QadoAmountParser.FormatNanoToQado(tx.Fee),
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

        private void TrySyncButton_Click(object sender, RoutedEventArgs e)
        {
            if (_isClosing) return;

            var now = DateTime.UtcNow;
            var elapsed = now - _lastTrySyncUtc;
            if (elapsed < TrySyncCooldown)
            {
                int waitSec = (int)Math.Ceiling((TrySyncCooldown - elapsed).TotalSeconds);
                Info("Sync", $"Try Sync ignored (cooldown {waitSec}s).");
                return;
            }

            _lastTrySyncUtc = now;

            try
            {
                if (_p2pNode == null)
                {
                    Warn("Sync", "Try Sync failed: P2P node is not initialized.");
                    return;
                }

                _p2pNode.RequestSyncNow("ui-try-sync");
                Info("Sync", "Try Sync requested.");
            }
            catch (Exception ex)
            {
                Warn("Sync", $"Try Sync failed: {ex.Message}");
            }
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
                AmountQado = QadoAmountParser.FormatNanoToQado(tx.Amount),
                FeeQado = QadoAmountParser.FormatNanoToQado(tx.Fee),
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

        private static string SafeUnixToUtc(ulong unixSeconds)
        {
            try
            {
                return DateTimeOffset.FromUnixTimeSeconds((long)unixSeconds)
                    .UtcDateTime
                    .ToString("yyyy-MM-dd HH:mm:ss 'UTC'");
            }
            catch
            {
                return unixSeconds.ToString();
            }
        }

        private static bool TryAddU64(ulong a, ulong b, out ulong sum)
        {
            if (ulong.MaxValue - a < b)
            {
                sum = 0UL;
                return false;
            }

            sum = a + b;
            return true;
        }

        private static ulong AddU64Saturating(ulong a, ulong b)
        {
            return ulong.MaxValue - a < b ? ulong.MaxValue : a + b;
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


