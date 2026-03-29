using System.Globalization;
using Qado.Api;
using Qado.Blockchain;
using Qado.Logging;
using Qado.Mempool;
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

            log.Info(
                "NodeHost",
                $"Running label={options.Label} dataDir={dataDir} p2p={options.P2pPort}" +
                $"{(options.ApiPort is int activeApiPort ? $" api={activeApiPort}" : " api=disabled")}" +
                $"{(options.NoDefaultSeed ? " defaultSeed=off" : " defaultSeed=on")}" +
                $" bootstrapPeers={options.BootstrapPeers.Count}");

            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, shutdownCts.Token).ConfigureAwait(false);
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
        Console.WriteLine("  --help                      Show this help.");
        Console.WriteLine();
        Console.WriteLine("Environment overrides:");
        Console.WriteLine("  QADO_NODE_LABEL");
        Console.WriteLine("  QADO_NODE_DATA_DIR");
        Console.WriteLine("  QADO_NODE_P2P_PORT");
        Console.WriteLine("  QADO_NODE_API_PORT");
        Console.WriteLine("  QADO_NODE_BOOTSTRAP_PEERS   Comma/semicolon-separated host:port list.");
        Console.WriteLine("  QADO_NODE_NO_DEFAULT_SEED   1/true to disable built-in seed dialing.");
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
}
