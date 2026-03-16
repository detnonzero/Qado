using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using NSec.Cryptography;
using OpenCL.Net;
using Qado.Blockchain;
using Qado.CodeBehindHelper;
using Qado.Logging;
using Qado.Storage;

namespace Qado.Mining
{
    public sealed class OpenClMiner : IDisposable
    {
        private const int HashBatchSize = 262_144;
        private static readonly long MaintenanceIntervalTicks = Stopwatch.Frequency;

        private readonly OpenClMiningDevice _device;
        private readonly byte[] _minerPublicKey;
        private readonly Func<List<Transaction>> _getReadyTransactions;
        private readonly Func<Block, Task> _onBlockMinedAsync;
        private readonly Action<Block> _onBlockAccepted;
        private readonly Action? _onTemplateStarted;
        private readonly Action<long> _onHashesCompleted;
        private readonly ILogSink? _log;

        private readonly int[] _foundFlagHost = new int[1];
        private readonly ulong[] _foundNonceHost = new ulong[1];
        private readonly byte[] _foundHashHost = new byte[32];

        private readonly uint[] _block0Words = new uint[16];
        private readonly uint[] _block1Words = new uint[16];
        private readonly uint[] _block2Words = new uint[16];
        private readonly byte[] _targetBytes = new byte[32];

        private Context _context;
        private CommandQueue _queue;
        private Program _program;
        private Kernel _kernel;
        private IMem? _block0Buffer;
        private IMem? _block1Buffer;
        private IMem? _block2Buffer;
        private IMem? _targetBuffer;
        private IMem? _foundFlagBuffer;
        private IMem? _foundNonceBuffer;
        private IMem? _foundHashBuffer;
        private bool _initialized;
        private bool _disposed;

        public OpenClMiner(
            OpenClMiningDevice device,
            string privateKeyHex,
            Func<List<Transaction>> getReadyTransactions,
            Func<Block, Task> onBlockMinedAsync,
            Action<Block> onBlockAccepted,
            Action? onTemplateStarted,
            Action<long> onHashesCompleted,
            ILogSink? log = null)
        {
            _device = device ?? throw new ArgumentNullException(nameof(device));
            _getReadyTransactions = getReadyTransactions ?? throw new ArgumentNullException(nameof(getReadyTransactions));
            _onBlockMinedAsync = onBlockMinedAsync ?? throw new ArgumentNullException(nameof(onBlockMinedAsync));
            _onBlockAccepted = onBlockAccepted ?? throw new ArgumentNullException(nameof(onBlockAccepted));
            _onTemplateStarted = onTemplateStarted;
            _onHashesCompleted = onHashesCompleted ?? throw new ArgumentNullException(nameof(onHashesCompleted));
            _log = log;

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
        }

        public ulong CurrentNonce { get; private set; }

        public Task StartMiningAsync(CancellationToken token)
            => Task.Run(() => MineLoop(token), token);

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            ReleaseAllNoThrow();
        }

        private void MineLoop(CancellationToken token)
        {
            try
            {
                EnsureOpenClReady();
                _log?.Info("Mining", $"OpenCL mining enabled on {_device.DisplayName}.");

                while (true)
                {
                    token.ThrowIfCancellationRequested();

                    var txs = _getReadyTransactions() ?? new List<Transaction>(0);

                    ulong tipHeight = BlockStore.GetLatestHeight();
                    byte[]? prevHash = BlockStore.GetCanonicalHashAtHeight(tipHeight);
                    if (prevHash is not { Length: 32 })
                        prevHash = new byte[32];

                    ulong newHeight = tipHeight + 1UL;
                    byte[] target = Difficulty.ClampTarget(DifficultyCalculator.GetTargetForHeight(newHeight));
                    BigInteger diff = Difficulty.TargetToDifficulty(target);
                    ulong subsidy = RewardCalculator.GetBlockSubsidy(newHeight);

                    int maxNonCoinbaseTx = Math.Max(0, ConsensusRules.MaxTransactionsPerBlock - 1);
                    var selectedTxs = new List<Transaction>(capacity: Math.Min(txs.Count, maxNonCoinbaseTx));
                    for (int i = 0; i < txs.Count && selectedTxs.Count < maxNonCoinbaseTx; i++)
                    {
                        if (txs[i] != null)
                            selectedTxs.Add(txs[i]);
                    }

                    if (!TrySumFees(selectedTxs, out ulong totalFees))
                    {
                        _log?.Error("Mining", "totalFees overflow (rejecting OpenCL block template)");
                        return;
                    }

                    if (!TryAddU64(subsidy, totalFees, out ulong coinbaseAmount))
                    {
                        _log?.Error("Mining", "coinbase amount overflow (rejecting OpenCL block template)");
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

                    _onTemplateStarted?.Invoke();
                    CurrentNonce = 0;
                    _log?.Info(
                        "Mining",
                        $"Mining (OpenCL) h={newHeight} | diff={diff} | target={Hex(target, 16)}... | " +
                        $"txs={block.Transactions.Count} | fees={QadoAmountParser.FormatNanoToQado(totalFees)} | " +
                        $"subsidy={QadoAmountParser.FormatNanoToQado(subsidy)}");

                    if (MineCurrentTemplate(block, tipHeight, prevHash, token))
                    {
                        token.ThrowIfCancellationRequested();
                        try { Task.Delay(150, token).Wait(token); } catch (OperationCanceledException) { throw; }
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                _log?.Error("Mining", $"OpenCL miner crashed: {ex}");
            }
            finally
            {
                CurrentNonce = 0;
                _log?.Warn("Mining", "OpenCL mining loop stopped");
            }
        }

        private bool MineCurrentTemplate(Block block, ulong tipHeight, byte[] prevHash, CancellationToken token)
        {
            if (block.Header == null)
                return false;

            UploadTemplate(block.Header.ToHashBytesWithNonce(0), block.Header.Target);

            long nextMaintenanceTick = Stopwatch.GetTimestamp();
            ulong nonceBase = 0;

            while (!token.IsCancellationRequested)
            {
                long nowTick = Stopwatch.GetTimestamp();
                if (nowTick >= nextMaintenanceTick)
                {
                    ulong curTipHeight = BlockStore.GetLatestHeight();
                    if (curTipHeight != tipHeight)
                    {
                        _log?.Info("Mining", $"Aborted: tip height changed {tipHeight} -> {curTipHeight}.");
                        return false;
                    }

                    byte[]? curTipHashAtHeight = BlockStore.GetCanonicalHashAtHeight(tipHeight);
                    if (curTipHashAtHeight is { Length: 32 } && !BytesEqual32(curTipHashAtHeight, prevHash))
                    {
                        _log?.Info("Mining", $"Aborted: tip hash changed at height {tipHeight}.");
                        return false;
                    }

                    ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                    if (now > block.Header.Timestamp)
                    {
                        block.Header.Timestamp = now;
                        UploadTemplate(block.Header.ToHashBytesWithNonce(0), block.Header.Target);
                    }

                    do
                    {
                        nextMaintenanceTick += MaintenanceIntervalTicks;
                    }
                    while (nowTick >= nextMaintenanceTick);
                }

                if (TryMineBatch(nonceBase, out var foundNonce, out var foundHash))
                {
                    CurrentNonce = foundNonce;
                    block.Header.Nonce = foundNonce;
                    block.BlockHash = foundHash;

                    _log?.Info("Mining", $"Block FOUND h={block.BlockHeight} nonce={foundNonce} hash={Hex(foundHash, 8)}...");

                    _onBlockAccepted(block);
                    _ = Task.Run(() => _onBlockMinedAsync(block));
                    return true;
                }

                nonceBase = unchecked(nonceBase + (ulong)HashBatchSize);
                CurrentNonce = nonceBase;
                _onHashesCompleted(HashBatchSize);
            }

            return false;
        }

        private bool TryMineBatch(ulong nonceBase, out ulong foundNonce, out byte[] foundHash)
        {
            foundNonce = 0;
            foundHash = Array.Empty<byte>();

            if (!_initialized)
                throw new InvalidOperationException("OpenCL miner is not initialized.");

            Array.Clear(_foundFlagHost, 0, _foundFlagHost.Length);
            Array.Clear(_foundNonceHost, 0, _foundNonceHost.Length);
            Array.Clear(_foundHashHost, 0, _foundHashHost.Length);

            ErrorCode error;
            Event ev;

            error = Cl.EnqueueWriteBuffer(_queue, _foundFlagBuffer, Bool.True, IntPtr.Zero, (IntPtr)sizeof(int), _foundFlagHost, 0, null, out ev);
            ThrowIfError(error, "reset found flag");
            ReleaseEventNoThrow(ev);

            error = Cl.SetKernelArg(_kernel, 4, nonceBase);
            ThrowIfError(error, "set nonce base");

            error = Cl.EnqueueNDRangeKernel(_queue, _kernel, 1, null, new[] { (IntPtr)HashBatchSize }, null, 0, null, out ev);
            ThrowIfError(error, "launch search kernel");
            ReleaseEventNoThrow(ev);

            error = Cl.Finish(_queue);
            ThrowIfError(error, "finish search kernel");

            error = Cl.EnqueueReadBuffer(_queue, _foundFlagBuffer, Bool.True, IntPtr.Zero, (IntPtr)sizeof(int), _foundFlagHost, 0, null, out ev);
            ThrowIfError(error, "read found flag");
            ReleaseEventNoThrow(ev);

            if (_foundFlagHost[0] == 0)
                return false;

            error = Cl.EnqueueReadBuffer(_queue, _foundNonceBuffer, Bool.True, IntPtr.Zero, (IntPtr)sizeof(ulong), _foundNonceHost, 0, null, out ev);
            ThrowIfError(error, "read found nonce");
            ReleaseEventNoThrow(ev);

            error = Cl.EnqueueReadBuffer(_queue, _foundHashBuffer, Bool.True, IntPtr.Zero, (IntPtr)_foundHashHost.Length, _foundHashHost, 0, null, out ev);
            ThrowIfError(error, "read found hash");
            ReleaseEventNoThrow(ev);

            foundNonce = _foundNonceHost[0];
            foundHash = (byte[])_foundHashHost.Clone();
            return true;
        }

        private void UploadTemplate(byte[] headerBytesZeroNonce, byte[] target)
        {
            if (headerBytesZeroNonce == null || headerBytesZeroNonce.Length != BlockHeader.HashInputSizeBytes)
                throw new ArgumentException($"headerBytesZeroNonce must be {BlockHeader.HashInputSizeBytes} bytes.", nameof(headerBytesZeroNonce));
            if (target is not { Length: 32 })
                throw new ArgumentException("target must be 32 bytes.", nameof(target));

            WriteWordBlock(headerBytesZeroNonce, 0, _block0Words);
            WriteWordBlock(headerBytesZeroNonce, 64, _block1Words);
            WriteWordBlock(headerBytesZeroNonce, 128, _block2Words);
            Buffer.BlockCopy(target, 0, _targetBytes, 0, 32);

            ErrorCode error;
            Event ev;

            error = Cl.EnqueueWriteBuffer(_queue, _block0Buffer, Bool.True, IntPtr.Zero, (IntPtr)(16 * sizeof(uint)), _block0Words, 0, null, out ev);
            ThrowIfError(error, "upload block0 template");
            ReleaseEventNoThrow(ev);

            error = Cl.EnqueueWriteBuffer(_queue, _block1Buffer, Bool.True, IntPtr.Zero, (IntPtr)(16 * sizeof(uint)), _block1Words, 0, null, out ev);
            ThrowIfError(error, "upload block1 template");
            ReleaseEventNoThrow(ev);

            error = Cl.EnqueueWriteBuffer(_queue, _block2Buffer, Bool.True, IntPtr.Zero, (IntPtr)(16 * sizeof(uint)), _block2Words, 0, null, out ev);
            ThrowIfError(error, "upload block2 template");
            ReleaseEventNoThrow(ev);

            error = Cl.EnqueueWriteBuffer(_queue, _targetBuffer, Bool.True, IntPtr.Zero, (IntPtr)32, _targetBytes, 0, null, out ev);
            ThrowIfError(error, "upload target");
            ReleaseEventNoThrow(ev);

            error = Cl.Finish(_queue);
            ThrowIfError(error, "finish template upload");
        }

        private void EnsureOpenClReady()
        {
            if (_initialized)
                return;
            if (_disposed)
                throw new ObjectDisposedException(nameof(OpenClMiner));

            ErrorCode error;

            _context = Cl.CreateContext(null, 1, new[] { _device.DeviceHandle }, null, IntPtr.Zero, out error);
            ThrowIfError(error, "create OpenCL context");

            _queue = Cl.CreateCommandQueue(_context, _device.DeviceHandle, CommandQueueProperties.None, out error);
            ThrowIfError(error, "create OpenCL queue");

            _program = Cl.CreateProgramWithSource(_context, 1, new[] { OpenClKernelSource.Source }, null, out error);
            ThrowIfError(error, "create OpenCL program");

            error = Cl.BuildProgram(_program, 1, new[] { _device.DeviceHandle }, string.Empty, null, IntPtr.Zero);
            if (error != ErrorCode.Success)
            {
                string buildLog = SafeBuildLog();
                throw new InvalidOperationException($"OpenCL build failed on {_device.DisplayName}: {error}. {buildLog}".Trim());
            }

            _kernel = Cl.CreateKernel(_program, "search_nonce", out error);
            ThrowIfError(error, "create OpenCL kernel");

            _block0Buffer = Cl.CreateBuffer(_context, MemFlags.ReadOnly, (IntPtr)(16 * sizeof(uint)), out error);
            ThrowIfError(error, "create block0 buffer");
            _block1Buffer = Cl.CreateBuffer(_context, MemFlags.ReadOnly, (IntPtr)(16 * sizeof(uint)), out error);
            ThrowIfError(error, "create block1 buffer");
            _block2Buffer = Cl.CreateBuffer(_context, MemFlags.ReadOnly, (IntPtr)(16 * sizeof(uint)), out error);
            ThrowIfError(error, "create block2 buffer");
            _targetBuffer = Cl.CreateBuffer(_context, MemFlags.ReadOnly, (IntPtr)32, out error);
            ThrowIfError(error, "create target buffer");
            _foundFlagBuffer = Cl.CreateBuffer(_context, MemFlags.ReadWrite | MemFlags.CopyHostPtr, (IntPtr)sizeof(int), _foundFlagHost, out error);
            ThrowIfError(error, "create found-flag buffer");
            _foundNonceBuffer = Cl.CreateBuffer(_context, MemFlags.ReadWrite | MemFlags.CopyHostPtr, (IntPtr)sizeof(ulong), _foundNonceHost, out error);
            ThrowIfError(error, "create found-nonce buffer");
            _foundHashBuffer = Cl.CreateBuffer(_context, MemFlags.ReadWrite | MemFlags.CopyHostPtr, (IntPtr)_foundHashHost.Length, _foundHashHost, out error);
            ThrowIfError(error, "create found-hash buffer");

            error = Cl.SetKernelArg(_kernel, 0, _block0Buffer);
            ThrowIfError(error, "bind block0 buffer");
            error = Cl.SetKernelArg(_kernel, 1, _block1Buffer);
            ThrowIfError(error, "bind block1 buffer");
            error = Cl.SetKernelArg(_kernel, 2, _block2Buffer);
            ThrowIfError(error, "bind block2 buffer");
            error = Cl.SetKernelArg(_kernel, 3, _targetBuffer);
            ThrowIfError(error, "bind target buffer");
            error = Cl.SetKernelArg(_kernel, 5, _foundFlagBuffer);
            ThrowIfError(error, "bind found-flag buffer");
            error = Cl.SetKernelArg(_kernel, 6, _foundNonceBuffer);
            ThrowIfError(error, "bind found-nonce buffer");
            error = Cl.SetKernelArg(_kernel, 7, _foundHashBuffer);
            ThrowIfError(error, "bind found-hash buffer");

            _initialized = true;
        }

        private string SafeBuildLog()
        {
            try
            {
                ErrorCode error;
                return Cl.GetProgramBuildInfo(_program, _device.DeviceHandle, ProgramBuildInfo.Log, out error).ToString().Trim();
            }
            catch
            {
                return string.Empty;
            }
        }

        private void ReleaseAllNoThrow()
        {
            try
            {
                if (_foundHashBuffer != null) Cl.ReleaseMemObject(_foundHashBuffer);
            }
            catch { }
            _foundHashBuffer = null;

            try
            {
                if (_foundNonceBuffer != null) Cl.ReleaseMemObject(_foundNonceBuffer);
            }
            catch { }
            _foundNonceBuffer = null;

            try
            {
                if (_foundFlagBuffer != null) Cl.ReleaseMemObject(_foundFlagBuffer);
            }
            catch { }
            _foundFlagBuffer = null;

            try
            {
                if (_targetBuffer != null) Cl.ReleaseMemObject(_targetBuffer);
            }
            catch { }
            _targetBuffer = null;

            try
            {
                if (_block2Buffer != null) Cl.ReleaseMemObject(_block2Buffer);
            }
            catch { }
            _block2Buffer = null;

            try
            {
                if (_block1Buffer != null) Cl.ReleaseMemObject(_block1Buffer);
            }
            catch { }
            _block1Buffer = null;

            try
            {
                if (_block0Buffer != null) Cl.ReleaseMemObject(_block0Buffer);
            }
            catch { }
            _block0Buffer = null;

            try
            {
                Cl.ReleaseKernel(_kernel);
            }
            catch { }
            _kernel = default;

            try
            {
                Cl.ReleaseProgram(_program);
            }
            catch { }
            _program = default;

            try
            {
                Cl.ReleaseCommandQueue(_queue);
            }
            catch { }
            _queue = default;

            try
            {
                Cl.ReleaseContext(_context);
            }
            catch { }
            _context = default;

            _initialized = false;
        }

        private static void ReleaseEventNoThrow(Event ev)
        {
            try
            {
                Cl.ReleaseEvent(ev);
            }
            catch
            {
            }
        }

        private static void ThrowIfError(ErrorCode error, string operation)
        {
            if (error != ErrorCode.Success)
                throw new InvalidOperationException($"OpenCL {operation} failed: {error}");
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
            if (a.Length != 32 || b.Length != 32)
                return false;

            int diff = 0;
            for (int i = 0; i < 32; i++)
                diff |= a[i] ^ b[i];

            return diff == 0;
        }

        private static string Hex(byte[] data, int take = -1)
        {
            var hex = Convert.ToHexString(data).ToLowerInvariant();
            return (take > 0 && hex.Length > take) ? hex[..take] : hex;
        }
    }
}
