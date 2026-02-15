using System.Collections.Concurrent;
using System.Globalization;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.DependencyInjection;
using Qado.Blockchain;
using Qado.Mempool;
using Qado.Networking;
using Qado.Serialization;
using Qado.Storage;

namespace Qado.Api
{
    public sealed class ExchangeApiHost : IDisposable
    {
        private readonly object _sync = new();
        private readonly MempoolManager _mempool;
        private readonly Func<P2PNode?> _getP2PNode;
        private readonly Qado.Logging.ILogSink? _log;
        private readonly ConcurrentDictionary<string, BroadcastResponsePayload> _broadcastCache = new(StringComparer.Ordinal);

        private WebApplication? _app;
        private bool _disposed;

        public int Port { get; private set; }
        public bool IsRunning => _app is not null;

        public ExchangeApiHost(MempoolManager mempool, Func<P2PNode?> getP2PNode, Qado.Logging.ILogSink? log = null)
        {
            _mempool = mempool ?? throw new ArgumentNullException(nameof(mempool));
            _getP2PNode = getP2PNode ?? throw new ArgumentNullException(nameof(getP2PNode));
            _log = log;
        }

        public async Task StartAsync(int port, CancellationToken ct = default)
        {
            ThrowIfDisposed();

            WebApplication? appToStart;
            lock (_sync)
            {
                if (_app != null) return;

                Port = port;
                _broadcastCache.Clear();

                var builder = WebApplication.CreateBuilder(new WebApplicationOptions
                {
                    Args = Array.Empty<string>()
                });

                builder.WebHost.ConfigureKestrel(options => options.ListenAnyIP(port));

                builder.Services.ConfigureHttpJsonOptions(options =>
                {
                    options.SerializerOptions.PropertyNamingPolicy = null;
                });

                builder.Services.AddRateLimiter(options =>
                {
                    options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
                    options.GlobalLimiter = PartitionedRateLimiter.Create<HttpContext, string>(context =>
                    {
                        var key = context.Connection.RemoteIpAddress?.ToString() ?? "unknown";
                        return RateLimitPartition.GetTokenBucketLimiter(
                            key,
                            _ => new TokenBucketRateLimiterOptions
                            {
                                TokenLimit = 120,
                                TokensPerPeriod = 60,
                                ReplenishmentPeriod = TimeSpan.FromSeconds(1),
                                AutoReplenishment = true,
                                QueueLimit = 0,
                                QueueProcessingOrder = QueueProcessingOrder.OldestFirst
                            });
                    });
                });

                var app = builder.Build();
                ConfigureMiddleware(app);
                ConfigureRoutes(app);

                _app = app;
                appToStart = app;
            }

            await appToStart.StartAsync(ct).ConfigureAwait(false);
            _log?.Info("API", $"Exchange API listening on http://0.0.0.0:{port}");
        }

        public async Task StopAsync(CancellationToken ct = default)
        {
            WebApplication? appToStop;
            lock (_sync)
            {
                appToStop = _app;
                _app = null;
            }

            if (appToStop == null) return;

            try { await appToStop.StopAsync(ct).ConfigureAwait(false); } catch { }
            try { await appToStop.DisposeAsync().ConfigureAwait(false); } catch { }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                StopAsync(cts.Token).GetAwaiter().GetResult();
            }
            catch { }
        }

        public static int ParsePortOrDefault(string? raw, int fallback = NetworkParams.ApiPort)
        {
            if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var p) &&
                p >= 1 && p <= 65535)
                return p;
            return fallback;
        }

        private void ConfigureMiddleware(WebApplication app)
        {
            app.UseRateLimiter();
        }

        private void ConfigureRoutes(WebApplication app)
        {
            app.MapGet("/v1/health", (HttpContext http) =>
            {
                return Results.Json(new
                {
                    status = "ok",
                    network = NetworkParams.Name,
                    node_version = typeof(ExchangeApiHost).Assembly.GetName().Version?.ToString() ?? "0.0.0",
                    timestamp_utc = DateTimeOffset.UtcNow.ToString("O")
                });
            });

            app.MapGet("/v1/network", () =>
            {
                var genesis = BlockStore.GetCanonicalHashAtHeight(0);
                return Results.Json(new
                {
                    chain_name = "qado",
                    symbol = "QADO",
                    decimals = 9,
                    chain_id = NetworkParams.ChainId.ToString(CultureInfo.InvariantCulture),
                    network_id = GenesisConfig.NetworkId.ToString(CultureInfo.InvariantCulture),
                    p2p_port = GenesisConfig.P2PPort,
                    genesis_hash = genesis is { Length: 32 } ? Hex(genesis) : ""
                });
            });

            app.MapGet("/v1/tip", () =>
            {
                var tipHeight = BlockStore.GetLatestHeight();
                var tipHash = BlockStore.GetCanonicalHashAtHeight(tipHeight);
                if (tipHash is not { Length: 32 })
                    return NotFound(Error("tip_not_found", "Canonical tip is unavailable.", null));

                var tipBlock = BlockStore.GetBlockByHash(tipHash);
                var ts = tipBlock?.Header?.Timestamp ?? 0UL;
                var chainwork = BlockIndexStore.GetChainwork(tipHash);

                return Results.Json(new
                {
                    height = U64(tipHeight),
                    hash = Hex(tipHash),
                    timestamp_utc = UnixToIso(ts),
                    chainwork = chainwork.ToString(CultureInfo.InvariantCulture)
                });
            });

            app.MapGet("/v1/block/{block_ref}", (string block_ref, HttpContext http) =>
            {
                Block? block = null;
                byte[]? blockHash = null;
                ulong blockHeight = 0;

                if (ulong.TryParse(block_ref, NumberStyles.None, CultureInfo.InvariantCulture, out var parsedHeight))
                {
                    blockHeight = parsedHeight;
                    blockHash = BlockStore.GetCanonicalHashAtHeight(parsedHeight);
                    block = BlockStore.GetBlockByHeight(parsedHeight);
                }
                else if (TryParseHex32(block_ref, out var parsedHash))
                {
                    blockHash = parsedHash;
                    block = BlockStore.GetBlockByHash(parsedHash);
                    blockHeight = block?.BlockHeight ?? 0;
                }
                else
                {
                    return BadRequest(Error("invalid_block_ref", "block_ref must be decimal height or 64-char lowercase hash.", http));
                }

                if (block is null || blockHash is not { Length: 32 })
                    return NotFound(Error("block_not_found", "Block was not found.", http));

                var txids = new List<string>(block.Transactions.Count);
                for (int i = 0; i < block.Transactions.Count; i++)
                    txids.Add(Hex(block.Transactions[i].ComputeTransactionHash()));

                return Results.Json(new
                {
                    hash = Hex(blockHash),
                    height = U64(blockHeight),
                    prev_hash = Hex(block.Header.PreviousBlockHash),
                    timestamp_utc = UnixToIso(block.Header.Timestamp),
                    miner = Hex(block.Header.Miner),
                    tx_count = block.Transactions.Count,
                    txids
                });
            });

            app.MapGet("/v1/address/{address}", (string address, HttpContext http) =>
            {
                if (!TryNormalizeHex32(address, out var addrHex))
                    return BadRequest(Error("invalid_address", "address must be 64-char lowercase hex.", http));

                var balance = StateStore.GetBalanceU64(addrHex);
                var nonce = StateStore.GetNonceU64(addrHex);
                var pendingOutgoing = MempoolManager.PendingCount(addrHex);
                var pendingIncoming = CountPendingIncoming(addrHex);
                var latestHeight = BlockStore.GetLatestHeight();

                return Results.Json(new
                {
                    address = addrHex,
                    balance_atomic = U64(balance),
                    nonce = U64(nonce),
                    pending_outgoing_count = pendingOutgoing < 0 ? 0 : pendingOutgoing,
                    pending_incoming_count = pendingIncoming,
                    latest_observed_height = U64(latestHeight)
                });
            });

            app.MapGet("/v1/tx/{txid}", (string txid, HttpContext http) =>
            {
                if (!TryParseHex32(txid, out var txidBytes))
                    return BadRequest(Error("invalid_txid", "txid must be 64-char lowercase hex.", http));

                var tipHeight = BlockStore.GetLatestHeight();
                if (TryGetIndexedTransaction(txidBytes, out var hit))
                {
                    var confirmations = hit.IsCanonical && tipHeight >= hit.BlockHeight
                        ? U64((tipHeight - hit.BlockHeight) + 1UL)
                        : "0";

                    return Results.Json(new
                    {
                        txid = Hex(txidBytes),
                        status = hit.IsCanonical ? "confirmed" : "orphaned",
                        confirmations,
                        block_hash = Hex(hit.BlockHash),
                        block_height = U64(hit.BlockHeight),
                        timestamp_utc = UnixToIso(hit.BlockTimestamp),
                        from = Hex(hit.Tx.Sender),
                        to = Hex(hit.Tx.Recipient),
                        amount_atomic = U64(hit.Tx.Amount),
                        fee_atomic = U64(hit.Tx.Fee),
                        nonce = U64(hit.Tx.TxNonce),
                        raw_tx_hex = Hex(hit.Tx.ToBytes())
                    });
                }

                if (TryGetMempoolTransaction(txidBytes, out var memTx))
                {
                    return Results.Json(new
                    {
                        txid = Hex(txidBytes),
                        status = "mempool",
                        confirmations = "0",
                        from = Hex(memTx.Sender),
                        to = Hex(memTx.Recipient),
                        amount_atomic = U64(memTx.Amount),
                        fee_atomic = U64(memTx.Fee),
                        nonce = U64(memTx.TxNonce),
                        raw_tx_hex = Hex(memTx.ToBytes())
                    });
                }

                return NotFound(Error("tx_not_found", "Transaction was not found.", http));
            });

            app.MapGet("/v1/tx/{txid}/confirmations", (string txid, HttpContext http) =>
            {
                if (!TryParseHex32(txid, out var txidBytes))
                    return BadRequest(Error("invalid_txid", "txid must be 64-char lowercase hex.", http));

                var tipHeight = BlockStore.GetLatestHeight();
                if (TryGetIndexedTransaction(txidBytes, out var hit))
                {
                    var confirmations = hit.IsCanonical && tipHeight >= hit.BlockHeight
                        ? U64((tipHeight - hit.BlockHeight) + 1UL)
                        : "0";

                    return Results.Json(new
                    {
                        txid = Hex(txidBytes),
                        status = hit.IsCanonical ? "confirmed" : "orphaned",
                        confirmations,
                        block_hash = Hex(hit.BlockHash),
                        block_height = U64(hit.BlockHeight),
                        tip_height = U64(tipHeight)
                    });
                }

                if (TryGetMempoolTransaction(txidBytes, out _))
                {
                    return Results.Json(new
                    {
                        txid = Hex(txidBytes),
                        status = "mempool",
                        confirmations = "0",
                        tip_height = U64(tipHeight)
                    });
                }

                return NotFound(Error("tx_not_found", "Transaction was not found.", http));
            });

            app.MapPost("/v1/tx/broadcast", (BroadcastRequest body, HttpContext http) =>
            {
                if (body is null || string.IsNullOrWhiteSpace(body.raw_tx_hex))
                    return BadRequest(Error("invalid_request", "raw_tx_hex is required.", http));

                if (!TryParseHex(body.raw_tx_hex, out var rawBytes))
                    return BadRequest(Error("invalid_raw_tx", "raw_tx_hex must be valid lowercase hex.", http));

                Transaction tx;
                try
                {
                    tx = TxBinarySerializer.Read(rawBytes);
                }
                catch (Exception ex)
                {
                    return BadRequest(Error("invalid_raw_tx", $"raw_tx decode failed: {ex.Message}", http));
                }

                var txidBytes = tx.ComputeTransactionHash();
                var txid = Hex(txidBytes);

                if (!string.IsNullOrWhiteSpace(body.idempotency_key) &&
                    TryGetCachedBroadcast(body.idempotency_key!, out var cached))
                {
                    return Results.Json(cached);
                }

                if (TxIndexStore.Get(txidBytes) is not null)
                {
                    var confirmed = new BroadcastResponsePayload(true, txid, "confirmed", null);
                    CacheBroadcast(body.idempotency_key, confirmed);
                    return Results.Json(confirmed);
                }

                if (TryGetMempoolTransaction(txidBytes, out _))
                {
                    var mempool = new BroadcastResponsePayload(true, txid, "mempool", null);
                    CacheBroadcast(body.idempotency_key, mempool);
                    return Results.Json(mempool);
                }

                if (!_mempool.TryAdd(tx))
                    return BadRequest(Error("tx_rejected", "Transaction rejected by mempool policy.", http));

                var node = _getP2PNode();
                if (node is not null)
                    _ = Task.Run(() => node.BroadcastTxAsync(tx));

                var accepted = new BroadcastResponsePayload(true, txid, "mempool", null);
                CacheBroadcast(body.idempotency_key, accepted);
                return Results.Json(accepted);
            });
        }

        private bool TryGetIndexedTransaction(byte[] txid, out IndexedTxHit hit)
        {
            hit = default;

            var idx = TxIndexStore.Get(txid);
            if (idx is null) return false;

            var (blockHash, blockHeight, _, _) = idx.Value;
            var block = BlockStore.GetBlockByHash(blockHash);
            if (block is null) return false;

            Transaction? tx = null;
            for (int i = 0; i < block.Transactions.Count; i++)
            {
                var t = block.Transactions[i];
                if (BytesEqual(txid, t.ComputeTransactionHash()))
                {
                    tx = t;
                    break;
                }
            }

            if (tx is null) return false;

            var canonAtHeight = BlockStore.GetCanonicalHashAtHeight(blockHeight);
            var isCanonical = canonAtHeight is { Length: 32 } && BytesEqual(canonAtHeight, blockHash);
            var ts = block.Header?.Timestamp ?? 0UL;

            hit = new IndexedTxHit(tx, blockHash, blockHeight, ts, isCanonical);
            return true;
        }

        private bool TryGetMempoolTransaction(byte[] txid, out Transaction tx)
        {
            var all = _mempool.GetAll();
            for (int i = 0; i < all.Count; i++)
            {
                var t = all[i];
                if (BytesEqual(txid, t.ComputeTransactionHash()))
                {
                    tx = t;
                    return true;
                }
            }

            tx = null!;
            return false;
        }

        private int CountPendingIncoming(string recipientHex)
        {
            var all = _mempool.GetAll();
            int count = 0;
            for (int i = 0; i < all.Count; i++)
            {
                var to = Hex(all[i].Recipient);
                if (string.Equals(to, recipientHex, StringComparison.Ordinal))
                    count++;
            }
            return count;
        }

        private bool TryGetCachedBroadcast(string key, out BroadcastResponsePayload payload)
        {
            payload = default!;
            if (string.IsNullOrWhiteSpace(key)) return false;
            return _broadcastCache.TryGetValue(key.Trim(), out payload);
        }

        private void CacheBroadcast(string? key, BroadcastResponsePayload payload)
        {
            if (string.IsNullOrWhiteSpace(key)) return;
            _broadcastCache[key.Trim()] = payload;
        }

        private static IResult BadRequest(object payload)
            => TypedResults.Json(payload, statusCode: StatusCodes.Status400BadRequest);

        private static IResult NotFound(object payload)
            => TypedResults.Json(payload, statusCode: StatusCodes.Status404NotFound);

        private static object Error(string code, string message, HttpContext? context)
        {
            return new
            {
                code,
                message,
                request_id = context?.TraceIdentifier
            };
        }

        private static bool TryNormalizeHex32(string input, out string hexLower)
        {
            hexLower = "";
            if (!TryParseHex32(input, out var bytes)) return false;
            hexLower = Hex(bytes);
            return true;
        }

        private static bool TryParseHex32(string input, out byte[] bytes)
        {
            bytes = Array.Empty<byte>();
            if (string.IsNullOrWhiteSpace(input)) return false;
            var s = input.Trim().ToLowerInvariant();
            if (s.StartsWith("0x", StringComparison.Ordinal)) s = s[2..];
            if (s.Length != 64) return false;
            if (!TryParseHex(s, out var parsed)) return false;
            if (parsed.Length != 32) return false;
            bytes = parsed;
            return true;
        }

        private static bool TryParseHex(string input, out byte[] bytes)
        {
            bytes = Array.Empty<byte>();
            if (string.IsNullOrWhiteSpace(input)) return false;
            var s = input.Trim().ToLowerInvariant();
            if (s.StartsWith("0x", StringComparison.Ordinal)) s = s[2..];
            if ((s.Length & 1) != 0) return false;

            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
                    return false;
            }

            try
            {
                bytes = Convert.FromHexString(s);
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static string Hex(byte[] bytes) => Convert.ToHexString(bytes).ToLowerInvariant();
        private static string U64(ulong v) => v.ToString(CultureInfo.InvariantCulture);

        private static string UnixToIso(ulong unixSeconds)
        {
            if (unixSeconds > (ulong)long.MaxValue) unixSeconds = (ulong)long.MaxValue;
            return DateTimeOffset.FromUnixTimeSeconds((long)unixSeconds).UtcDateTime.ToString("O");
        }

        private static bool BytesEqual(byte[] a, byte[] b)
        {
            if (a.Length != b.Length) return false;
            int diff = 0;
            for (int i = 0; i < a.Length; i++) diff |= a[i] ^ b[i];
            return diff == 0;
        }

        private void ThrowIfDisposed()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(ExchangeApiHost));
        }

        private readonly record struct IndexedTxHit(
            Transaction Tx,
            byte[] BlockHash,
            ulong BlockHeight,
            ulong BlockTimestamp,
            bool IsCanonical);

        private sealed class BroadcastRequest
        {
            public string raw_tx_hex { get; set; } = "";
            public string? idempotency_key { get; set; }
        }

        private sealed record BroadcastResponsePayload(
            bool accepted,
            string txid,
            string status,
            string? error);
    }
}

