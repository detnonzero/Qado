using System;
using System.Collections.Generic;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using NSec.Cryptography;
using Qado.CodeBehindHelper;
using Qado.Logging;
using Qado.Storage;
using Qado.Utils;

namespace Qado.Blockchain
{
    public sealed class Miner
    {
        private readonly byte[] _minerPublicKey; // 32-byte miner public key
        private readonly Func<List<Transaction>> _getReadyTransactions;
        private readonly Func<Block, Task> _onBlockMinedAsync;
        private readonly Action<Block> _onBlockAccepted;
        private readonly Action? _onHashIteration;
        private readonly ILogSink? _log;

        public uint CurrentNonce { get; private set; }

        private const uint TimestampRefreshMask = 0xFFF;

        private const uint TipRefreshMask = 0x3FF;

        public Miner(
            string privateKeyHex,
            Func<List<Transaction>> getReadyTransactions,
            Func<byte>? getDifficulty, // compatibility callback, currently unused
            Func<Block, Task> onBlockMinedAsync,
            Action<Block> onBlockAccepted,
            Action? onHashIteration = null,
            ILogSink? log = null)
        {
            _ = getDifficulty; // keep compatibility parameter without warnings

            if (string.IsNullOrWhiteSpace(privateKeyHex))
                throw new ArgumentException("privateKeyHex required", nameof(privateKeyHex));

            var privBytes = Convert.FromHexString(privateKeyHex.Trim().StartsWith("0x", StringComparison.OrdinalIgnoreCase)
                ? privateKeyHex.Trim()[2..]
                : privateKeyHex.Trim());

            if (privBytes.Length != 32)
                throw new ArgumentException("privateKeyHex must be 32 bytes", nameof(privateKeyHex));

            using var key = Key.Import(SignatureAlgorithm.Ed25519, privBytes, KeyBlobFormat.RawPrivateKey);
            _minerPublicKey = key.Export(KeyBlobFormat.RawPublicKey);
            if (_minerPublicKey.Length != 32)
                throw new InvalidOperationException("Miner pubkey must be 32 bytes");

            _getReadyTransactions = getReadyTransactions ?? throw new ArgumentNullException(nameof(getReadyTransactions));
            _onBlockMinedAsync = onBlockMinedAsync ?? throw new ArgumentNullException(nameof(onBlockMinedAsync));
            _onBlockAccepted = onBlockAccepted ?? throw new ArgumentNullException(nameof(onBlockAccepted));
            _onHashIteration = onHashIteration;
            _log = log;
        }

        public Task StartMiningAsync(CancellationToken externalToken)
            => Task.Run(() => MineLoop(externalToken), externalToken);

        private void MineLoop(CancellationToken token)
        {
            try
            {

                var txs = _getReadyTransactions() ?? new List<Transaction>(0);

                ulong tipHeight = BlockStore.GetLatestHeight();
                byte[]? prevHash = BlockStore.GetCanonicalHashAtHeight(tipHeight);
                if (prevHash is not { Length: 32 })
                    prevHash = new byte[32];

                ulong newHeight = tipHeight + 1UL;

                byte[] target = DifficultyCalculator.GetTargetForHeight(newHeight);
                target = Difficulty.ClampTarget(target);

                BigInteger diff = Difficulty.TargetToDifficulty(target);

                ulong subsidy = RewardCalculator.GetBlockSubsidy(newHeight);

                int maxNonCoinbaseTx = Math.Max(0, ConsensusRules.MaxTransactionsPerBlock - 1);
                var selectedTxs = new List<Transaction>(capacity: Math.Min(txs.Count, maxNonCoinbaseTx));
                for (int i = 0; i < txs.Count && selectedTxs.Count < maxNonCoinbaseTx; i++)
                {
                    if (txs[i] != null) selectedTxs.Add(txs[i]);
                }

                if (!TrySumFees(selectedTxs, out ulong totalFees))
                {
                    _log?.Error("Mining", "totalFees overflow (rejecting block template)");
                    return;
                }

                if (!TryAddU64(subsidy, totalFees, out ulong coinbaseAmount))
                {
                    _log?.Error("Mining", "coinbase amount overflow (rejecting block template)");
                    return;
                }

                var header = new BlockHeader
                {
                    Version = 1,
                    PreviousBlockHash = prevHash,
                    MerkleRoot = new byte[32],
                    Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    Target = target,
                    Nonce = 0,
                    Miner = (byte[])_minerPublicKey.Clone()
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

                CurrentNonce = 0;

                _log?.Info("Mining",
                    $"⛏️ Mining h={newHeight} | diff={diff} | target={Hex(target, 16)}… | " +
                    $"txs={block.Transactions.Count} | fees={QadoAmountParser.FormatNanoToQado(totalFees)} | " +
                    $"subsidy={QadoAmountParser.FormatNanoToQado(subsidy)}");


                while (!token.IsCancellationRequested)
                {
                    if ((CurrentNonce & TipRefreshMask) == 0)
                    {
                        ulong curTipH = BlockStore.GetLatestHeight();
                        if (curTipH != tipHeight)
                        {
                            _log?.Info("Mining", $"❌ Aborted: tip height changed {tipHeight} → {curTipH}.");
                            return;
                        }

                        byte[]? curTipHashAtH = BlockStore.GetCanonicalHashAtHeight(tipHeight);
                        if (curTipHashAtH is { Length: 32 } && !BytesEqual32(curTipHashAtH, prevHash))
                        {
                            _log?.Info("Mining", $"❌ Aborted: tip hash changed at height {tipHeight}.");
                            return;
                        }
                    }

                    _onHashIteration?.Invoke();

                    if ((CurrentNonce & TimestampRefreshMask) == 0)
                    {
                        ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                        if (now > header.Timestamp)
                            header.Timestamp = now;
                    }

                    var headerBytes = header.ToHashBytesWithNonce(CurrentNonce);

                    var hash = Argon2Util.ComputeHash(
                        headerBytes,
                        memoryKb: ConsensusRules.PowMemoryKb,
                        iterations: ConsensusRules.PowIterations,
                        parallelism: ConsensusRules.PowParallelism);

                    if (Difficulty.Meets(hash, target))
                    {
                        header.Nonce = CurrentNonce;
                        block.BlockHash = hash;

                        _log?.Info("Mining", $"✅ Block FOUND h={newHeight} nonce={CurrentNonce} hash={Hex(hash, 8)}…");

                        _onBlockAccepted(block);

                        _ = Task.Run(() => _onBlockMinedAsync(block));

                        return;
                    }

                    if (CurrentNonce % 250_000 == 0)
                        _log?.Info("Mining", $"⏳ h={newHeight} nonce={CurrentNonce} hash={Hex(hash, 8)}…");

                    unchecked { CurrentNonce++; }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _log?.Error("Miner", $"Exception: {ex.Message}");
            }
        }

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

        private static bool BytesEqual32(byte[] a, byte[] b)
        {
            if (a.Length != 32 || b.Length != 32) return false;
            int diff = 0;
            for (int i = 0; i < 32; i++) diff |= a[i] ^ b[i];
            return diff == 0;
        }

        private static string Hex(byte[] data, int take = -1)
        {
            var hex = Convert.ToHexString(data).ToLowerInvariant();
            return (take > 0 && hex.Length > take) ? hex[..take] : hex;
        }
    }
}

