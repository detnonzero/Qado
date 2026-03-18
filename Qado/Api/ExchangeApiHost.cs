using System.Collections.Concurrent;
using System.Buffers.Binary;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Security.Cryptography;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Data.Sqlite;
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
        private readonly object _miningJobSync = new();
        private readonly Dictionary<string, MiningJobSnapshot> _miningJobsById = new(StringComparer.Ordinal);
        private readonly Dictionary<string, string> _latestMiningJobByMiner = new(StringComparer.Ordinal);

        private const int MaxMiningJobs = 64;
        private static readonly TimeSpan MiningJobTtl = TimeSpan.FromMinutes(2);

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

            app.MapPost("/v1/mining/job", (MiningJobRequest body, HttpContext http) =>
            {
                if (body is null || !TryNormalizeHex32(body.miner, out var minerHex))
                    return BadRequest(Error("invalid_miner", "miner must be 64-char lowercase hex.", http));

                if (!TryBuildMiningJob(minerHex, out var job, out var reason))
                    return BadRequest(Error(reason, "Unable to build mining job.", http));

                CacheMiningJob(job);

                return Results.Json(new
                {
                    job_id = job.JobId,
                    height = U64(job.Height),
                    prev_hash = Hex(job.PrevHash),
                    target = Hex(job.Target),
                    timestamp = U64(job.Timestamp),
                    merkle_root = Hex(job.MerkleRoot),
                    coinbase_amount = U64(job.CoinbaseAmount),
                    tx_count = job.TxCount,
                    header_hex_zero_nonce = Hex(job.HeaderZeroNonce),
                    precomputed_cv = Hex(UIntWordsToLittleEndianBytes(job.PrecomputedCvWords)),
                    block1_base = Hex(UIntWordsToLittleEndianBytes(job.Block1Words)),
                    block2 = Hex(UIntWordsToLittleEndianBytes(job.Block2Words)),
                    target_words = job.TargetWords.Select(w => w.ToString("x8", CultureInfo.InvariantCulture)).ToArray()
                });
            }).DisableRateLimiting();

            app.MapPost("/v1/mining/submit", async (MiningSubmitRequest body, HttpContext http) =>
            {
                if (body is null || string.IsNullOrWhiteSpace(body.job_id))
                    return BadRequest(Error("invalid_request", "job_id is required.", http));
                if (!ulong.TryParse(body.nonce, NumberStyles.None, CultureInfo.InvariantCulture, out var nonce))
                    return BadRequest(Error("invalid_nonce", "nonce must be an unsigned integer string.", http));

                ulong? timestampOverride = null;
                if (!string.IsNullOrWhiteSpace(body.timestamp))
                {
                    if (!ulong.TryParse(body.timestamp, NumberStyles.None, CultureInfo.InvariantCulture, out var parsedTimestamp))
                        return BadRequest(Error("invalid_timestamp", "timestamp must be an unsigned integer string.", http));
                    timestampOverride = parsedTimestamp;
                }

                var job = GetMiningJob(body.job_id.Trim(), out var getReason);
                if (job is null)
                    return Results.Json(new { accepted = false, reason = getReason });

                ulong effectiveTimestamp = timestampOverride ?? job.Timestamp;
                if (effectiveTimestamp < job.Timestamp)
                    return Results.Json(new { accepted = false, reason = "invalid_timestamp" });

                if (IsMiningJobStale(job))
                {
                    RemoveMiningJob(job.JobId);
                    return Results.Json(new { accepted = false, reason = "stale_job" });
                }

                var block = RebuildBlockFromJob(job, nonce, effectiveTimestamp);
                block.BlockHash = block.ComputeBlockHash();

                if (!Difficulty.Meets(block.BlockHash, block.Header.Target))
                    return Results.Json(new { accepted = false, reason = "invalid_pow" });

                var submit = await TrySubmitMiningJobAsync(job, block).ConfigureAwait(false);
                if (!submit.accepted)
                    return Results.Json(new { accepted = false, reason = submit.reason });

                RemoveMiningJob(job.JobId);
                return Results.Json(new
                {
                    accepted = true,
                    hash = Hex(block.BlockHash),
                    height = U64(block.BlockHeight)
                });
            }).DisableRateLimiting();
        }

        private bool TryBuildMiningJob(string minerHex, out MiningJobSnapshot job, out string reason)
        {
            job = default!;
            reason = "job_build_failed";

            if (!TryParseHex32(minerHex, out var minerBytes))
            {
                reason = "invalid_miner";
                return false;
            }

            var txs = _mempool.GetBuffer().GetAllReadyTransactionsSortedByFee() ?? new List<Transaction>(0);

            ulong tipHeight = BlockStore.GetLatestHeight();
            byte[]? prevHash = BlockStore.GetCanonicalHashAtHeight(tipHeight);
            if (prevHash is not { Length: 32 })
                prevHash = new byte[32];

            ulong newHeight = tipHeight + 1UL;
            byte[] target = Difficulty.ClampTarget(DifficultyCalculator.GetTargetForHeight(newHeight));
            ulong subsidy = RewardCalculator.GetBlockSubsidy(newHeight);

            int maxNonCoinbaseTx = Math.Max(0, ConsensusRules.MaxTransactionsPerBlock - 1);
            var selectedTxs = new List<Transaction>(capacity: Math.Min(txs.Count, maxNonCoinbaseTx));
            for (int i = 0; i < txs.Count && selectedTxs.Count < maxNonCoinbaseTx; i++)
            {
                if (txs[i] != null)
                    selectedTxs.Add(CloneTransaction(txs[i]));
            }

            if (!TrySumFees(selectedTxs, out ulong totalFees))
            {
                reason = "fee_overflow";
                return false;
            }

            if (!TryAddU64(subsidy, totalFees, out ulong coinbaseAmount))
            {
                reason = "coinbase_overflow";
                return false;
            }

            var header = new BlockHeader
            {
                Version = 1,
                PreviousBlockHash = (byte[])prevHash.Clone(),
                MerkleRoot = new byte[32],
                Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                Target = (byte[])target.Clone(),
                Nonce = 0,
                Miner = (byte[])minerBytes.Clone()
            };

            var block = new Block
            {
                BlockHeight = newHeight,
                Header = header,
                Transactions = selectedTxs,
                BlockHash = new byte[32]
            };

            block.InsertCoinbaseTransaction(coinbaseAmount);
            block.RecomputeAndSetMerkleRoot();

            byte[] headerZeroNonce = block.Header.ToHashBytesWithNonce(0);
            var block0Words = new uint[16];
            var block1Words = new uint[16];
            var block2Words = new uint[16];
            var precomputedCvWords = new uint[8];
            var targetWords = new uint[8];

            WriteWordBlock(headerZeroNonce, 0, block0Words);
            WriteWordBlock(headerZeroNonce, 64, block1Words);
            WriteWordBlock(headerZeroNonce, 128, block2Words);
            PrecomputeChunk0Cv(block0Words, precomputedCvWords);
            WriteTargetWords(target, targetWords);

            var txPayloads = new List<byte[]>(block.Transactions.Count);
            for (int i = 0; i < block.Transactions.Count; i++)
                txPayloads.Add(block.Transactions[i].ToBytes());

            job = new MiningJobSnapshot(
                JobId: CreateJobId(),
                MinerHex: minerHex,
                Height: newHeight,
                PrevHash: (byte[])prevHash.Clone(),
                Target: (byte[])target.Clone(),
                Timestamp: block.Header.Timestamp,
                MerkleRoot: (byte[])block.Header.MerkleRoot.Clone(),
                MinerBytes: (byte[])minerBytes.Clone(),
                CoinbaseAmount: coinbaseAmount,
                TxCount: block.Transactions.Count,
                HeaderZeroNonce: headerZeroNonce,
                PrecomputedCvWords: precomputedCvWords,
                Block1Words: block1Words,
                Block2Words: block2Words,
                TargetWords: targetWords,
                TransactionPayloads: txPayloads,
                CreatedUtc: DateTime.UtcNow,
                ExpiresUtc: DateTime.UtcNow + MiningJobTtl);

            return true;
        }

        private void CacheMiningJob(MiningJobSnapshot job)
        {
            lock (_miningJobSync)
            {
                CleanupMiningJobsNoLock();

                if (_latestMiningJobByMiner.TryGetValue(job.MinerHex, out var oldJobId))
                    _miningJobsById.Remove(oldJobId);

                _miningJobsById[job.JobId] = job;
                _latestMiningJobByMiner[job.MinerHex] = job.JobId;

                TrimMiningJobsNoLock();
            }
        }

        private MiningJobSnapshot? GetMiningJob(string jobId, out string reason)
        {
            lock (_miningJobSync)
            {
                CleanupMiningJobsNoLock();

                if (!_miningJobsById.TryGetValue(jobId, out var job))
                {
                    reason = "job_not_found";
                    return null;
                }

                reason = "ok";
                return job;
            }
        }

        private void RemoveMiningJob(string jobId)
        {
            lock (_miningJobSync)
            {
                if (!_miningJobsById.TryGetValue(jobId, out var job))
                    return;

                _miningJobsById.Remove(jobId);
                if (_latestMiningJobByMiner.TryGetValue(job.MinerHex, out var latestId) &&
                    string.Equals(latestId, jobId, StringComparison.Ordinal))
                {
                    _latestMiningJobByMiner.Remove(job.MinerHex);
                }
            }
        }

        private bool IsMiningJobStale(MiningJobSnapshot job)
        {
            if (DateTime.UtcNow > job.ExpiresUtc)
                return true;

            ulong tipHeight = BlockStore.GetLatestHeight();
            if (tipHeight + 1UL != job.Height)
                return true;

            var tipHash = BlockStore.GetCanonicalHashAtHeight(tipHeight);
            return tipHash is not { Length: 32 } || !BytesEqual(tipHash, job.PrevHash);
        }

        private async Task<(bool accepted, string reason)> TrySubmitMiningJobAsync(MiningJobSnapshot job, Block block)
        {
            try
            {
                if (Db.Connection == null)
                    return (false, "db_unavailable");

                bool canonicalized = false;
                ulong newCanonHeight = 0;

                lock (Db.Sync)
                {
                    using var tx = Db.Connection.BeginTransaction();

                    if (!TryComputeCanonExtensionHeight(block, tx, out newCanonHeight))
                    {
                        tx.Rollback();
                        return (false, "stale_job");
                    }

                    block.BlockHeight = newCanonHeight;

                    if (!BlockValidator.ValidateNetworkTipBlock(block, out var reason, tx))
                    {
                        tx.Rollback();
                        return (false, reason switch
                        {
                            "Timestamp too far in the future" => "invalid_timestamp",
                            _ => "submit_rejected"
                        });
                    }

                    BlockStore.SaveBlock(block, tx, BlockIndexStore.StatusCanonicalStateValidated);
                    StateApplier.ApplyBlockWithUndo(block, tx);
                    BlockStore.SetCanonicalHashAtHeight(newCanonHeight, block.BlockHash!, tx);

                    tx.Commit();
                    canonicalized = true;
                }

                if (canonicalized)
                {
                    try { _mempool.RemoveIncluded(block); } catch { }
                }

                try
                {
                    var node = _getP2PNode();
                    if (node is not null)
                        _ = Task.Run(() => node.BroadcastBlockAsync(block));
                }
                catch { }

                _log?.Info("MiningApi", $"Accepted external mining submit: h={block.BlockHeight} hash={Hex(block.BlockHash)}");
                return (true, "ok");
            }
            catch (Exception ex)
            {
                _log?.Warn("MiningApi", $"Submit failed: {ex.Message}");
                return (false, "submit_rejected");
            }
        }

        private static Block RebuildBlockFromJob(MiningJobSnapshot job, ulong nonce, ulong timestamp)
        {
            var txs = new List<Transaction>(job.TransactionPayloads.Count);
            for (int i = 0; i < job.TransactionPayloads.Count; i++)
                txs.Add(TxBinarySerializer.Read(job.TransactionPayloads[i]));

            var header = new BlockHeader
            {
                Version = 1,
                PreviousBlockHash = (byte[])job.PrevHash.Clone(),
                MerkleRoot = (byte[])job.MerkleRoot.Clone(),
                Timestamp = timestamp,
                Target = (byte[])job.Target.Clone(),
                Nonce = nonce,
                Miner = (byte[])job.MinerBytes.Clone()
            };

            return new Block
            {
                BlockHeight = job.Height,
                Header = header,
                Transactions = txs,
                BlockHash = new byte[32]
            };
        }

        private static string CreateJobId()
            => Convert.ToHexString(RandomNumberGenerator.GetBytes(16)).ToLowerInvariant();

        private void CleanupMiningJobsNoLock()
        {
            if (_miningJobsById.Count == 0)
                return;

            var now = DateTime.UtcNow;
            var expiredIds = new List<string>();

            foreach (var kvp in _miningJobsById)
            {
                if (now > kvp.Value.ExpiresUtc)
                    expiredIds.Add(kvp.Key);
            }

            for (int i = 0; i < expiredIds.Count; i++)
                RemoveMiningJobNoLock(expiredIds[i]);
        }

        private void TrimMiningJobsNoLock()
        {
            if (_miningJobsById.Count <= MaxMiningJobs)
                return;

            var overflow = _miningJobsById.Count - MaxMiningJobs;
            var oldest = _miningJobsById
                .OrderBy(kvp => kvp.Value.CreatedUtc)
                .Take(overflow)
                .Select(kvp => kvp.Key)
                .ToArray();

            for (int i = 0; i < oldest.Length; i++)
                RemoveMiningJobNoLock(oldest[i]);
        }

        private void RemoveMiningJobNoLock(string jobId)
        {
            if (!_miningJobsById.TryGetValue(jobId, out var job))
                return;

            _miningJobsById.Remove(jobId);
            if (_latestMiningJobByMiner.TryGetValue(job.MinerHex, out var latestId) &&
                string.Equals(latestId, jobId, StringComparison.Ordinal))
            {
                _latestMiningJobByMiner.Remove(job.MinerHex);
            }
        }

        private static bool TryComputeCanonExtensionHeight(Block block, SqliteTransaction tx, out ulong newHeight)
        {
            newHeight = 0;

            var prev = block.Header?.PreviousBlockHash;
            if (prev is not { Length: 32 })
                return false;

            ulong tipHeight = BlockStore.GetLatestHeight(tx);
            var tipHash = BlockStore.GetCanonicalHashAtHeight(tipHeight, tx);
            if (tipHash is not { Length: 32 })
                return false;

            if (!BytesEqual(prev, tipHash))
                return false;

            newHeight = tipHeight + 1UL;
            return true;
        }

        private static Transaction CloneTransaction(Transaction tx)
            => TxBinarySerializer.Read(tx.ToBytes());

        private static bool TrySumFees(List<Transaction> txs, out ulong totalFees)
        {
            totalFees = 0;
            for (int i = 0; i < txs.Count; i++)
            {
                if (!TryAddU64(totalFees, txs[i].Fee, out totalFees))
                    return false;
            }
            return true;
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

        private static byte[] UIntWordsToLittleEndianBytes(uint[] words)
        {
            var dst = new byte[words.Length * 4];
            for (int i = 0; i < words.Length; i++)
                BinaryPrimitives.WriteUInt32LittleEndian(dst.AsSpan(i * 4, 4), words[i]);
            return dst;
        }

        private static void WriteWordBlock(byte[] src, int offset, uint[] dst)
        {
            var block = new byte[64];
            int available = Math.Max(0, Math.Min(64, src.Length - offset));
            if (available > 0)
                Buffer.BlockCopy(src, offset, block, 0, available);

            for (int i = 0; i < 16; i++)
                dst[i] = BinaryPrimitives.ReadUInt32LittleEndian(block.AsSpan(i * 4, 4));
        }

        private static void WriteTargetWords(byte[] target, uint[] dst)
        {
            for (int i = 0; i < 8; i++)
                dst[i] = BinaryPrimitives.ReadUInt32BigEndian(target.AsSpan(i * 4, 4));
        }

        private static void PrecomputeChunk0Cv(uint[] block0Words, uint[] dstCv)
        {
            Span<uint> cv = stackalloc uint[8]
            {
                0x6A09E667u, 0xBB67AE85u, 0x3C6EF372u, 0xA54FF53Au,
                0x510E527Fu, 0x9B05688Cu, 0x1F83D9ABu, 0x5BE0CD19u
            };

            var blockWords = new uint[16];
            Array.Copy(block0Words, blockWords, 16);

            Span<uint> outWords = stackalloc uint[16];
            CompressWordsCpu(cv, blockWords, 0u, 0u, 64u, 1u, outWords);

            for (int i = 0; i < 8; i++)
                dstCv[i] = outWords[i];
        }

        private static void CompressWordsCpu(
            ReadOnlySpan<uint> cv,
            uint[] blockWords,
            uint counterLow,
            uint counterHigh,
            uint blockLen,
            uint flags,
            Span<uint> outWords)
        {
            uint[] v = new uint[16];
            for (int i = 0; i < 8; i++) v[i] = cv[i];
            v[8] = 0x6A09E667u;
            v[9] = 0xBB67AE85u;
            v[10] = 0x3C6EF372u;
            v[11] = 0xA54FF53Au;
            v[12] = counterLow;
            v[13] = counterHigh;
            v[14] = blockLen;
            v[15] = flags;

            RoundFnCpu(v, blockWords);
            PermuteCpu(blockWords);
            RoundFnCpu(v, blockWords);
            PermuteCpu(blockWords);
            RoundFnCpu(v, blockWords);
            PermuteCpu(blockWords);
            RoundFnCpu(v, blockWords);
            PermuteCpu(blockWords);
            RoundFnCpu(v, blockWords);
            PermuteCpu(blockWords);
            RoundFnCpu(v, blockWords);
            PermuteCpu(blockWords);
            RoundFnCpu(v, blockWords);

            for (int i = 0; i < 8; i++)
            {
                outWords[i] = v[i] ^ v[i + 8];
                outWords[i + 8] = v[i + 8] ^ cv[i];
            }
        }

        private static void RoundFnCpu(uint[] v, uint[] m)
        {
            GCpu(ref v[0], ref v[4], ref v[8], ref v[12], m[0], m[1]);
            GCpu(ref v[1], ref v[5], ref v[9], ref v[13], m[2], m[3]);
            GCpu(ref v[2], ref v[6], ref v[10], ref v[14], m[4], m[5]);
            GCpu(ref v[3], ref v[7], ref v[11], ref v[15], m[6], m[7]);

            GCpu(ref v[0], ref v[5], ref v[10], ref v[15], m[8], m[9]);
            GCpu(ref v[1], ref v[6], ref v[11], ref v[12], m[10], m[11]);
            GCpu(ref v[2], ref v[7], ref v[8], ref v[13], m[12], m[13]);
            GCpu(ref v[3], ref v[4], ref v[9], ref v[14], m[14], m[15]);
        }

        private static void PermuteCpu(uint[] m)
        {
            uint[] t = new uint[16];
            t[0] = m[2]; t[1] = m[6]; t[2] = m[3]; t[3] = m[10];
            t[4] = m[7]; t[5] = m[0]; t[6] = m[4]; t[7] = m[13];
            t[8] = m[1]; t[9] = m[11]; t[10] = m[12]; t[11] = m[5];
            t[12] = m[9]; t[13] = m[14]; t[14] = m[15]; t[15] = m[8];
            Array.Copy(t, m, 16);
        }

        private static void GCpu(ref uint a, ref uint b, ref uint c, ref uint d, uint mx, uint my)
        {
            a = a + b + mx;
            d = RotateRight(d ^ a, 16);
            c = c + d;
            b = RotateRight(b ^ c, 12);
            a = a + b + my;
            d = RotateRight(d ^ a, 8);
            c = c + d;
            b = RotateRight(b ^ c, 7);
        }

        private static uint RotateRight(uint value, int shift)
            => (value >> shift) | (value << (32 - shift));

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

        private sealed class MiningJobRequest
        {
            public string miner { get; set; } = "";
        }

        private sealed class MiningSubmitRequest
        {
            public string job_id { get; set; } = "";
            public string nonce { get; set; } = "";
            public string? timestamp { get; set; }
        }

        private sealed record MiningJobSnapshot(
            string JobId,
            string MinerHex,
            ulong Height,
            byte[] PrevHash,
            byte[] Target,
            ulong Timestamp,
            byte[] MerkleRoot,
            byte[] MinerBytes,
            ulong CoinbaseAmount,
            int TxCount,
            byte[] HeaderZeroNonce,
            uint[] PrecomputedCvWords,
            uint[] Block1Words,
            uint[] Block2Words,
            uint[] TargetWords,
            List<byte[]> TransactionPayloads,
            DateTime CreatedUtc,
            DateTime ExpiresUtc);

        private sealed record BroadcastResponsePayload(
            bool accepted,
            string txid,
            string status,
            string? error);
    }
}

