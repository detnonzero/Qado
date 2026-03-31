using System.Globalization;
using Microsoft.Data.Sqlite;
using Qado.Api;
using Qado.Blockchain;
using Qado.Logging;
using Qado.Mempool;
using Qado.Mining;
using Qado.Networking;
using Qado.Storage;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        NodeHostOptions options;
        try
        {
            options = NodeHostOptions.Parse(args ?? Array.Empty<string>());
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Argument error: {ex.Message}");
            PrintUsage();
            return 2;
        }

        if (options.ShowHelp)
        {
            PrintUsage();
            return 0;
        }

        string dataDir = options.ResolveDataDirectory();
        Directory.CreateDirectory(dataDir);
        string dbPath = Path.Combine(dataDir, NetworkParams.DbFileName);

        using var shutdownCts = new CancellationTokenSource();
        var log = new ConsoleLogSink(options.Label);

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            if (!shutdownCts.IsCancellationRequested)
                shutdownCts.Cancel();
        };

        SelfPeerGuard.InitializeAtStartup(options.P2pPort);

        P2PNode? node = null;
        ExchangeApiHost? apiHost = null;
        OpenClMiner? openClMiner = null;
        Task? miningTask = null;

        try
        {
            Db.Initialize(dbPath);
            GenesisBlockProvider.EnsureGenesisBlockStored();

            var startupAudit = StartupIntegrityAudit.Run();
            startupAudit = StartupIntegrityAudit.RepairDerivedStateIfNeeded(startupAudit);
            StartupIntegrityAudit.ThrowIfBlocking(startupAudit);

            var mempool = new MempoolManager(
                senderHex => StateStore.GetBalanceU64(senderHex),
                senderHex => StateStore.GetNonceU64(senderHex),
                log);

            node = new P2PNode(mempool, log);
            node.Start(options.P2pPort, shutdownCts.Token);
            node.StartPeerExchangeLoop(shutdownCts.Token);
            node.StartInventoryRefreshLoop(shutdownCts.Token);
            node.StartLatencyProbeLoop(shutdownCts.Token);

            if (options.NoDefaultSeed)
            {
                if (options.BootstrapPeers.Count > 0)
                    _ = RunBootstrapReconnectLoopAsync(node, options.BootstrapPeers, shutdownCts.Token, log);
            }
            else
            {
                node.StartReconnectLoop(shutdownCts.Token);
                _ = node.ConnectSeedAndKnownPeersAsync(shutdownCts.Token);
            }

            for (int i = 0; i < options.BootstrapPeers.Count; i++)
            {
                var peer = options.BootstrapPeers[i];
                await node.ConnectAsync(peer.Host, peer.Port, shutdownCts.Token).ConfigureAwait(false);
            }

            if (options.ApiPort is int apiPort)
            {
                apiHost = new ExchangeApiHost(mempool, () => node, log);
                await apiHost.StartAsync(apiPort, shutdownCts.Token).ConfigureAwait(false);
            }

            if (options.MineOpenCl)
            {
                if (string.IsNullOrWhiteSpace(options.MinerPublicKey))
                    throw new InvalidOperationException("OpenCL mining requires --miner-public-key <hex32>.");

                string minerPublicKey = NormalizeHex32(options.MinerPublicKey, "--miner-public-key");
                var device = ResolveOpenClDevice(options.OpenClDeviceSelector, log);
                if (device == null)
                    throw new InvalidOperationException("No suitable OpenCL GPU device was found.");

                var buffer = mempool.GetBuffer();
                openClMiner = new OpenClMiner(
                    device,
                    minerPublicKey,
                    () => buffer.GetAllReadyTransactionsSortedByFee(),
                    block => OnBlockMinedAsync(block, node, mempool, log),
                    _ => { },
                    () => { },
                    _ => { },
                    log,
                    true);

                miningTask = openClMiner.StartMiningAsync(shutdownCts.Token);
                log.Info(
                    "NodeHost",
                    $"Headless OpenCL mining enabled. device={device.DisplayName} miner={ShortHex(minerPublicKey)}");
            }

            log.Info(
                "NodeHost",
                $"Running label={options.Label} dataDir={dataDir} p2p={options.P2pPort}" +
                $"{(options.ApiPort is int activeApiPort ? $" api={activeApiPort}" : " api=disabled")}" +
                $"{(options.NoDefaultSeed ? " defaultSeed=off" : " defaultSeed=on")}" +
                $"{(options.MineOpenCl ? " mining=opencl" : " mining=off")}" +
                $" bootstrapPeers={options.BootstrapPeers.Count}");

            try
            {
                var lifetimeTask = Task.Delay(Timeout.InfiniteTimeSpan, shutdownCts.Token);
                if (miningTask == null)
                {
                    await lifetimeTask.ConfigureAwait(false);
                }
                else
                {
                    var completed = await Task.WhenAny(lifetimeTask, miningTask).ConfigureAwait(false);
                    if (completed == miningTask)
                    {
                        await miningTask.ConfigureAwait(false);
                        if (!shutdownCts.IsCancellationRequested)
                        {
                            log.Warn("NodeHost", "OpenCL mining task exited unexpectedly.");
                            return 1;
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (shutdownCts.IsCancellationRequested)
            {
            }

            log.Info("NodeHost", "Shutdown requested.");
            return 0;
        }
        catch (OperationCanceledException) when (shutdownCts.IsCancellationRequested)
        {
            log.Info("NodeHost", "Shutdown requested during startup.");
            return 0;
        }
        catch (Exception ex)
        {
            log.Error("NodeHost", ex.ToString());
            return 1;
        }
        finally
        {
            if (apiHost != null)
            {
                try
                {
                    using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                    await apiHost.StopAsync(stopCts.Token).ConfigureAwait(false);
                }
                catch
                {
                }

                try { apiHost.Dispose(); } catch { }
            }

            if (openClMiner != null)
            {
                try { openClMiner.Dispose(); } catch { }
            }

            if (node != null)
            {
                try { node.Stop(); } catch { }
            }

            try { Db.Shutdown(); } catch { }
        }
    }

    private static void PrintUsage()
    {
        Console.WriteLine("Qado.NodeHost");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run --project Qado.NodeHost -- [options]");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --label <name>              Optional node label for logs.");
        Console.WriteLine("  --data-dir <path>           Custom data directory.");
        Console.WriteLine("  --p2p-port <port>           P2P listen port. Default: mainnet source default.");
        Console.WriteLine("  --api-port <port>           Enable embedded API on this port.");
        Console.WriteLine("  --peer <host:port>          Bootstrap peer; repeatable.");
        Console.WriteLine("  --no-default-seed           Do not auto-dial the built-in genesis seed/known peers.");
        Console.WriteLine("  --mine-opencl               Enable headless OpenCL mining.");
        Console.WriteLine("  --miner-public-key <hex32>  32-byte miner public key as 64-char hex.");
        Console.WriteLine("  --opencl-device <value>     OpenCL device id or case-insensitive substring match.");
        Console.WriteLine("  --help                      Show this help.");
        Console.WriteLine();
        Console.WriteLine("Environment overrides:");
        Console.WriteLine("  QADO_NODE_LABEL");
        Console.WriteLine("  QADO_NODE_DATA_DIR");
        Console.WriteLine("  QADO_NODE_P2P_PORT");
        Console.WriteLine("  QADO_NODE_API_PORT");
        Console.WriteLine("  QADO_NODE_BOOTSTRAP_PEERS   Comma/semicolon-separated host:port list.");
        Console.WriteLine("  QADO_NODE_NO_DEFAULT_SEED   1/true to disable built-in seed dialing.");
        Console.WriteLine("  QADO_NODE_MINE_OPENCL       1/true to enable headless OpenCL mining.");
        Console.WriteLine("  QADO_NODE_MINER_PUBLIC_KEY  64-char hex public key for mining rewards.");
        Console.WriteLine("  QADO_NODE_OPENCL_DEVICE     Device id or name substring selector.");
    }

    private static async Task RunBootstrapReconnectLoopAsync(
        P2PNode node,
        IReadOnlyList<BootstrapPeer> peers,
        CancellationToken ct,
        ILogSink log)
    {
        while (!ct.IsCancellationRequested)
        {
            for (int i = 0; i < peers.Count; i++)
            {
                if (ct.IsCancellationRequested)
                    break;

                var peer = peers[i];
                try
                {
                    await node.ConnectAsync(peer.Host, peer.Port, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    log.Warn("NodeHost", $"Custom bootstrap reconnect to {peer.Host}:{peer.Port} failed: {ex.Message}");
                }
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(15), ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
        }
    }

    private sealed class ConsoleLogSink : ILogSink
    {
        private readonly object _gate = new();
        private readonly string _label;

        public ConsoleLogSink(string label)
        {
            _label = string.IsNullOrWhiteSpace(label) ? "node" : label.Trim();
        }

        public void Info(string category, string message) => Write("INFO", category, message);
        public void Warn(string category, string message) => Write("WARN", category, message);
        public void Error(string category, string message) => Write("ERROR", category, message);

        private void Write(string level, string category, string message)
        {
            lock (_gate)
            {
                Console.WriteLine($"[{DateTime.UtcNow:O}] [{_label}] [{level}] [{category}] {message}");
            }
        }
    }

    private sealed record BootstrapPeer(string Host, int Port);

    private sealed record NodeHostOptions
    {
        public string Label { get; private init; } = Environment.GetEnvironmentVariable("QADO_NODE_LABEL")?.Trim() ?? Environment.MachineName;
        public string? DataDir { get; private init; } = Environment.GetEnvironmentVariable("QADO_NODE_DATA_DIR");
        public int P2pPort { get; private init; } = ParsePortOrDefault(Environment.GetEnvironmentVariable("QADO_NODE_P2P_PORT"), GenesisConfig.P2PPort);
        public int? ApiPort { get; private init; } = ParseOptionalPort(Environment.GetEnvironmentVariable("QADO_NODE_API_PORT"));
        public bool NoDefaultSeed { get; private init; } = ParseBool(Environment.GetEnvironmentVariable("QADO_NODE_NO_DEFAULT_SEED"));
        public bool MineOpenCl { get; private init; } = ParseBool(Environment.GetEnvironmentVariable("QADO_NODE_MINE_OPENCL"));
        public string? MinerPublicKey { get; private init; } = Environment.GetEnvironmentVariable("QADO_NODE_MINER_PUBLIC_KEY");
        public string? OpenClDeviceSelector { get; private init; } = Environment.GetEnvironmentVariable("QADO_NODE_OPENCL_DEVICE");
        public bool ShowHelp { get; private init; }
        public List<BootstrapPeer> BootstrapPeers { get; } = ParseBootstrapPeers(Environment.GetEnvironmentVariable("QADO_NODE_BOOTSTRAP_PEERS"));

        public static NodeHostOptions Parse(string[] args)
        {
            var options = new NodeHostOptions();

            for (int i = 0; i < args.Length; i++)
            {
                string arg = args[i] ?? string.Empty;
                switch (arg)
                {
                    case "--help":
                    case "-h":
                    case "/?":
                        options = options with { ShowHelp = true };
                        break;

                    case "--label":
                        options = options with { Label = RequireValue(args, ref i, arg) };
                        break;

                    case "--data-dir":
                        options = options with { DataDir = RequireValue(args, ref i, arg) };
                        break;

                    case "--p2p-port":
                        options = options with { P2pPort = ParseRequiredPort(RequireValue(args, ref i, arg), arg) };
                        break;

                    case "--api-port":
                        options = options with { ApiPort = ParseRequiredPort(RequireValue(args, ref i, arg), arg) };
                        break;

                    case "--peer":
                        options.BootstrapPeers.Add(ParseEndpoint(RequireValue(args, ref i, arg), arg));
                        break;

                    case "--no-default-seed":
                        options = options with { NoDefaultSeed = true };
                        break;

                    case "--mine-opencl":
                        options = options with { MineOpenCl = true };
                        break;

                    case "--miner-public-key":
                        options = options with { MinerPublicKey = RequireValue(args, ref i, arg) };
                        break;

                    case "--opencl-device":
                        options = options with { OpenClDeviceSelector = RequireValue(args, ref i, arg) };
                        break;

                    default:
                        throw new InvalidOperationException($"Unknown argument '{arg}'.");
                }
            }

            return options;
        }

        public string ResolveDataDirectory()
        {
            if (!string.IsNullOrWhiteSpace(DataDir))
                return Path.GetFullPath(DataDir);

            string safeLabel = SanitizeSegment(Label);
            return Path.Combine(AppContext.BaseDirectory, "data-nodehost", safeLabel);
        }

        private static string RequireValue(string[] args, ref int index, string option)
        {
            if (index + 1 >= args.Length || string.IsNullOrWhiteSpace(args[index + 1]))
                throw new InvalidOperationException($"Missing value for {option}.");

            index++;
            return args[index];
        }

        private static int ParsePortOrDefault(string? raw, int fallback)
        {
            if (string.IsNullOrWhiteSpace(raw))
                return fallback;
            return ParseRequiredPort(raw, "port");
        }

        private static int? ParseOptionalPort(string? raw)
        {
            if (string.IsNullOrWhiteSpace(raw))
                return null;
            return ParseRequiredPort(raw, "port");
        }

        private static int ParseRequiredPort(string raw, string option)
        {
            if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out int port) ||
                port <= 0 ||
                port > 65535)
            {
                throw new InvalidOperationException($"Invalid value for {option}: '{raw}'.");
            }

            return port;
        }

        private static bool ParseBool(string? raw)
        {
            if (string.IsNullOrWhiteSpace(raw))
                return false;

            return raw.Equals("1", StringComparison.OrdinalIgnoreCase) ||
                   raw.Equals("true", StringComparison.OrdinalIgnoreCase) ||
                   raw.Equals("yes", StringComparison.OrdinalIgnoreCase) ||
                   raw.Equals("on", StringComparison.OrdinalIgnoreCase);
        }

        private static List<BootstrapPeer> ParseBootstrapPeers(string? raw)
        {
            var peers = new List<BootstrapPeer>();
            if (string.IsNullOrWhiteSpace(raw))
                return peers;

            string[] parts = raw.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            for (int i = 0; i < parts.Length; i++)
                peers.Add(ParseEndpoint(parts[i], "QADO_NODE_BOOTSTRAP_PEERS"));

            return peers;
        }

        private static BootstrapPeer ParseEndpoint(string raw, string source)
        {
            if (string.IsNullOrWhiteSpace(raw))
                throw new InvalidOperationException($"Empty endpoint in {source}.");

            string value = raw.Trim();
            if (value.StartsWith("[", StringComparison.Ordinal))
            {
                int close = value.IndexOf(']');
                if (close <= 1 || close + 2 >= value.Length || value[close + 1] != ':')
                    throw new InvalidOperationException($"Invalid endpoint '{raw}'.");

                string host = value[1..close];
                int port = ParseRequiredPort(value[(close + 2)..], source);
                return new BootstrapPeer(host, port);
            }

            int lastColon = value.LastIndexOf(':');
            if (lastColon <= 0 || lastColon == value.Length - 1)
                throw new InvalidOperationException($"Invalid endpoint '{raw}'. Use host:port or [ipv6]:port.");

            string hostPart = value[..lastColon];
            string portPart = value[(lastColon + 1)..];

            if (hostPart.Contains(':', StringComparison.Ordinal))
                throw new InvalidOperationException($"IPv6 endpoints must use brackets in {source}: '{raw}'.");

            int parsedPort = ParseRequiredPort(portPart, source);
            return new BootstrapPeer(hostPart, parsedPort);
        }

        private static string SanitizeSegment(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return "node";

            var chars = value.Trim().ToCharArray();
            for (int i = 0; i < chars.Length; i++)
            {
                if (Path.GetInvalidFileNameChars().Contains(chars[i]))
                    chars[i] = '_';
            }

            return new string(chars);
        }
    }

    private static OpenClMiningDevice? ResolveOpenClDevice(string? selector, ILogSink log)
    {
        var devices = OpenClDiscovery.DiscoverDevices(log);
        if (devices.Count == 0)
            return null;

        if (!string.IsNullOrWhiteSpace(selector))
        {
            string needle = selector.Trim();
            for (int i = 0; i < devices.Count; i++)
            {
                var device = devices[i];
                if (string.Equals(device.Id, needle, StringComparison.Ordinal) ||
                    device.DisplayName.Contains(needle, StringComparison.OrdinalIgnoreCase) ||
                    device.DeviceName.Contains(needle, StringComparison.OrdinalIgnoreCase) ||
                    device.Vendor.Contains(needle, StringComparison.OrdinalIgnoreCase) ||
                    device.PlatformName.Contains(needle, StringComparison.OrdinalIgnoreCase))
                    return device;
            }

            log.Warn("NodeHost", $"OpenCL device selector '{selector}' did not match any discovered GPU. Falling back to auto-select.");
        }

        for (int i = 0; i < devices.Count; i++)
        {
            if (devices[i].DeviceName.Contains("1070", StringComparison.OrdinalIgnoreCase))
                return devices[i];
        }

        return devices[0];
    }

    private static async Task OnBlockMinedAsync(
        Block block,
        P2PNode node,
        MempoolManager mempool,
        ILogSink log)
    {
        try
        {
            if (Db.Connection == null)
            {
                log.Error("Mining", "Db.Connection is null (cannot persist block).");
                return;
            }

            bool extendedCanon = false;
            bool canonicalized = false;
            ulong newCanonHeight = 0;
            string? rejectReason = null;
            bool syncActive = node.IsInitialBlockSyncActive;

            lock (Db.Sync)
            {
                using var tx = Db.Connection.BeginTransaction();

                if (!syncActive && TryComputeCanonExtensionHeight(block, tx, out newCanonHeight))
                {
                    block.BlockHeight = newCanonHeight;

                    if (block.BlockHash is not { Length: 32 } || IsZero32(block.BlockHash))
                        block.BlockHash = block.ComputeBlockHash();

                    if (!BlockValidator.ValidateNetworkTipBlock(block, out var tipReason, tx))
                    {
                        rejectReason = tipReason;
                        tx.Rollback();
                        goto PersistDone;
                    }

                    BlockStore.SaveBlock(block, tx, BlockIndexStore.StatusCanonicalStateValidated);
                    StateApplier.ApplyBlockWithUndo(block, tx);
                    BlockStore.SetCanonicalHashAtHeight(newCanonHeight, block.BlockHash!, tx);
                    extendedCanon = true;
                }
                else
                {
                    if (block.BlockHash is not { Length: 32 } || IsZero32(block.BlockHash))
                        block.BlockHash = block.ComputeBlockHash();

                    if (!BlockValidator.ValidateNetworkSideBlockStateless(block, out var sideReason, tx))
                    {
                        rejectReason = sideReason;
                        tx.Rollback();
                        goto PersistDone;
                    }

                    BlockStore.SaveBlock(block, tx, BlockIndexStore.StatusSideStatelessAccepted);
                }

                tx.Commit();
            }

        PersistDone:
            if (!string.IsNullOrWhiteSpace(rejectReason))
            {
                log.Warn("Mining", $"Rejecting mined block (current state): {rejectReason}");
                return;
            }

            log.Info(
                "Mining",
                extendedCanon
                    ? $"Mined + stored: h={newCanonHeight}"
                    : $"Mined + stored (side/reorg candidate): h={block.BlockHeight}");

            if (syncActive && !extendedCanon)
                log.Info("Mining", "Initial block sync active; mined side candidate was not adopted into canonical chain.");

            canonicalized = extendedCanon;

            if (!extendedCanon && !syncActive)
            {
                try
                {
                    ChainSelector.MaybeAdoptNewTip(block.BlockHash!, log, mempool);
                    if (BlockIndexStore.TryGetStatus(block.BlockHash!, out var status) &&
                        BlockIndexStore.IsValidatedStatus(status))
                    {
                        canonicalized = true;
                    }
                }
                catch
                {
                }
            }

            if (canonicalized)
            {
                try { mempool.RemoveIncluded(block); } catch { }
            }

            try
            {
                await node.BroadcastBlockAsync(block).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                log.Warn("Gossip", $"Block broadcast task failed: {ex.Message}");
            }
        }
        catch (Exception ex)
        {
            log.Error("Mining", $"Persist error: {ex}");
        }
    }

    private static bool TryComputeCanonExtensionHeight(Block blk, SqliteTransaction tx, out ulong newHeight)
    {
        newHeight = 0;

        var prev = blk.Header?.PreviousBlockHash;
        if (prev is not { Length: 32 })
            return false;

        ulong tipH = BlockStore.GetLatestHeight(tx);
        var tipHash = BlockStore.GetCanonicalHashAtHeight(tipH, tx);
        if (tipHash is not { Length: 32 })
            return false;

        if (!BytesEqual32(prev, tipHash))
            return false;

        newHeight = tipH + 1UL;
        return true;
    }

    private static bool BytesEqual32(byte[] a, byte[] b)
    {
        if (a == null || b == null || a.Length != 32 || b.Length != 32)
            return false;

        return a.AsSpan().SequenceEqual(b);
    }

    private static bool IsZero32(byte[]? value)
    {
        if (value is not { Length: 32 })
            return true;

        for (int i = 0; i < value.Length; i++)
        {
            if (value[i] != 0)
                return false;
        }

        return true;
    }

    private static string NormalizeHex32(string value, string paramName)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{paramName} is required.");

        string normalized = value.Trim();
        if (normalized.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
            normalized = normalized[2..];

        byte[] bytes = Convert.FromHexString(normalized);
        if (bytes.Length != 32)
            throw new InvalidOperationException($"{paramName} must be a 32-byte hex value.");

        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    private static string ShortHex(string hex)
    {
        if (string.IsNullOrWhiteSpace(hex))
            return string.Empty;

        string normalized = hex.Trim();
        return normalized.Length <= 16
            ? normalized
            : $"{normalized[..8]}...{normalized[^8..]}";
    }
}
