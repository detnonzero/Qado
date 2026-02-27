using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Numerics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Qado.Blockchain;
using Qado.Logging;
using Qado.Storage;
using Qado.Utils;

namespace Qado.Networking
{
    public readonly record struct HeaderSyncPlan(
        byte[] BestHeaderHash,
        byte[] ForkPointHash,
        IReadOnlyList<byte[]> MissingBlockHashes,
        IReadOnlyList<byte[]> RevalidateBlockHashes);

    public sealed class HeaderSyncManager : IDisposable
    {
        public const int MaxHeaderSyncPeers = 2;
        public const int MaxHeadersPerMessage = 2000;
        public const int MaxLocatorHashes = 64;
        public static readonly TimeSpan HeaderResponseTimeout = TimeSpan.FromSeconds(20);
        public static readonly int MaxHeadersPayloadBytes = 4 + (MaxHeadersPerMessage * BlockHeader.PowHeaderSize);

        private const int MedianTimePastWindow = 11;
        private const int MaxFutureTimeDriftSeconds = 2 * 60 * 60;
        private const int RetargetWindowSolveTimes = 60;
        private const int RetargetMaxAdjustFactor = 2;

        private readonly object _gate = new();
        private readonly Func<IReadOnlyCollection<PeerSession>> _sessionSnapshot;
        private readonly Func<PeerSession, MsgType, byte[], CancellationToken, Task> _sendFrameAsync;
        private readonly Action<HeaderSyncPlan> _onSyncCompleted;
        private readonly ILogSink? _log;

        private readonly SemaphoreSlim _tick = new(0, 1);
        private readonly Channel<InboundHeadersFrame> _headersQueue = Channel.CreateUnbounded<InboundHeadersFrame>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
        private readonly CancellationTokenSource _disposeCts = new();
        private int _started;

        private readonly Dictionary<string, DateTime> _awaitingByPeerKey = new(StringComparer.Ordinal);
        private bool _completed;
        private readonly HashSet<string> _noProgressPeerKeys = new(StringComparer.Ordinal);

        private readonly record struct InboundHeadersFrame(string PeerKey, byte[] Payload);

        private readonly record struct HeaderContext(
            byte[] Hash,
            byte[] PrevHash,
            ulong Height,
            ulong Timestamp,
            byte[] Target,
            UInt128 Chainwork,
            int Status,
            bool IsBad,
            bool BadAncestor);

        public HeaderSyncManager(
            Func<IReadOnlyCollection<PeerSession>> sessionSnapshot,
            Func<PeerSession, MsgType, byte[], CancellationToken, Task> sendFrameAsync,
            Action<HeaderSyncPlan> onSyncCompleted,
            ILogSink? log = null)
        {
            _sessionSnapshot = sessionSnapshot ?? throw new ArgumentNullException(nameof(sessionSnapshot));
            _sendFrameAsync = sendFrameAsync ?? throw new ArgumentNullException(nameof(sendFrameAsync));
            _onSyncCompleted = onSyncCompleted ?? throw new ArgumentNullException(nameof(onSyncCompleted));
            _log = log;
        }

        public bool IsCompleted
        {
            get
            {
                lock (_gate)
                    return _completed;
            }
        }

        public bool TryBuildCurrentPlan(out HeaderSyncPlan plan)
        {
            if (!TryBuildBestChain(out var chain) || chain.Count == 0)
            {
                plan = default;
                return false;
            }

            plan = BuildDownloadPlan(chain);
            return true;
        }

        public void Start(CancellationToken ct)
        {
            if (Interlocked.Exchange(ref _started, 1) != 0)
                return;

            _ = Task.Run(() => ControlLoopAsync(ct), CancellationToken.None);
            _ = Task.Run(() => ProcessHeadersLoopAsync(ct), CancellationToken.None);

            RequestTick();
        }

        public void OnPeerReady(PeerSession peer)
        {
            if (peer == null || !peer.RemoteIsPublic)
                return;

            lock (_gate)
            {
                string key = NormalizePeerKey(peer.SessionKey);
                _noProgressPeerKeys.Remove(key);

                if (_completed)
                {
                    _completed = false;
                    _awaitingByPeerKey.Clear();
                }
            }

            RequestTick();
        }

        public void RequestResync(string reason = "external")
        {
            bool changed = false;
            lock (_gate)
            {
                if (_completed || _awaitingByPeerKey.Count > 0)
                {
                    changed = true;
                }

                _completed = false;
                _awaitingByPeerKey.Clear();
                _noProgressPeerKeys.Clear();
            }

            if (changed)
                _log?.Info("HeaderSync", $"resync requested ({reason}).");

            RequestTick();
        }

        public void OnPeerDisconnected(PeerSession peer)
        {
            if (peer == null)
                return;

            string key = NormalizePeerKey(peer.SessionKey);
            bool changed = false;

            lock (_gate)
            {
                changed = _awaitingByPeerKey.Remove(key);
                if (changed)
                    _noProgressPeerKeys.Add(key);
            }

            if (changed)
                RequestTick();
        }

        public void OnHeaders(PeerSession peer, byte[] payload)
        {
            if (peer == null || payload == null || payload.Length == 0)
                return;

            if (!peer.RemoteIsPublic)
                return;

            string key = NormalizePeerKey(peer.SessionKey);
            bool shouldQueue;

            lock (_gate)
            {
                shouldQueue = !_completed &&
                              _awaitingByPeerKey.ContainsKey(key);
            }

            if (!shouldQueue)
                return;

            _headersQueue.Writer.TryWrite(new InboundHeadersFrame(key, (byte[])payload.Clone()));
            RequestTick();
        }

        public byte[] BuildHeadersResponseForLocator(byte[] locatorPayload)
        {
            if (!BlockDownloadManager.TryParseHashListPayload(locatorPayload, MaxLocatorHashes, out var locator))
                return BuildHeadersPayload(Array.Empty<byte[]>());

            if (!TryBuildBestChain(out var bestChain) || bestChain.Count == 0)
                return BuildHeadersPayload(Array.Empty<byte[]>());

            var indexByHash = new Dictionary<string, int>(bestChain.Count, StringComparer.Ordinal);
            for (int i = 0; i < bestChain.Count; i++)
                indexByHash[ToHex(bestChain[i].Hash)] = i;

            int anchor = -1;
            for (int i = 0; i < locator.Count; i++)
            {
                var h = locator[i];
                if (h is not { Length: 32 })
                    continue;

                if (indexByHash.TryGetValue(ToHex(h), out anchor))
                    break;
            }

            int start = anchor + 1;
            if (start < 0)
                start = 0;

            var headers = new List<byte[]>(Math.Min(MaxHeadersPerMessage, bestChain.Count));
            for (int i = start; i < bestChain.Count && headers.Count < MaxHeadersPerMessage; i++)
            {
                var hash = bestChain[i].Hash;
                if (!TryLoadHeaderPowBytes(hash, out var bytes))
                    break;

                headers.Add(bytes);
            }

            return BuildHeadersPayload(headers);
        }

        public static byte[] BuildHeadersPayload(IReadOnlyList<byte[]> headerPowBytes)
        {
            if (headerPowBytes == null || headerPowBytes.Count == 0)
            {
                var empty = new byte[4];
                BinaryPrimitives.WriteUInt32LittleEndian(empty, 0);
                return empty;
            }

            var valid = new List<byte[]>(Math.Min(MaxHeadersPerMessage, headerPowBytes.Count));
            for (int i = 0; i < headerPowBytes.Count && valid.Count < MaxHeadersPerMessage; i++)
            {
                var h = headerPowBytes[i];
                if (h is { Length: BlockHeader.PowHeaderSize })
                    valid.Add(h);
            }

            int payloadLen = 4 + (valid.Count * BlockHeader.PowHeaderSize);
            var payload = new byte[payloadLen];
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), (uint)valid.Count);

            int o = 4;
            for (int i = 0; i < valid.Count; i++)
            {
                valid[i].AsSpan(0, BlockHeader.PowHeaderSize).CopyTo(payload.AsSpan(o, BlockHeader.PowHeaderSize));
                o += BlockHeader.PowHeaderSize;
            }

            return payload;
        }

        public static bool TryParseHeadersPayload(byte[] payload, out List<BlockHeader> headers)
        {
            headers = new List<BlockHeader>();
            if (payload == null || payload.Length < 4)
                return false;

            uint declared = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(0, 4));
            if (declared > int.MaxValue)
                return false;

            int count = (int)declared;
            if (count < 0 || count > MaxHeadersPerMessage)
                return false;

            int expected = 4 + (count * BlockHeader.PowHeaderSize);
            if (payload.Length != expected)
                return false;

            headers = new List<BlockHeader>(count);
            int o = 4;
            for (int i = 0; i < count; i++)
            {
                var slice = payload.AsSpan(o, BlockHeader.PowHeaderSize);
                if (!TryParseHeaderPowBytes(slice, out var header))
                    return false;

                headers.Add(header);
                o += BlockHeader.PowHeaderSize;
            }

            return true;
        }

        private async Task ControlLoopAsync(CancellationToken externalCt)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(externalCt, _disposeCts.Token);
            var ct = linked.Token;

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await ControlStepAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _log?.Warn("HeaderSync", $"control error: {ex.Message}");
                }

                try
                {
                    await _tick.WaitAsync(TimeSpan.FromSeconds(1), ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch
                {
                }
            }
        }

        private async Task ControlStepAsync(CancellationToken ct)
        {
            var peers = _sessionSnapshot();
            if (peers == null || peers.Count == 0)
                return;

            var peersToQuery = new List<(PeerSession Peer, string Key)>(MaxHeaderSyncPeers);
            bool shouldEmitCompletion = false;

            lock (_gate)
            {
                if (_completed)
                    return;

                var staleKeys = new List<string>();
                foreach (var kv in _awaitingByPeerKey)
                {
                    var current = FindPeerByKey(peers, kv.Key);
                    if (current == null ||
                        !current.HandshakeOk ||
                        !current.Client.Connected ||
                        !current.RemoteIsPublic)
                    {
                        staleKeys.Add(kv.Key);
                    }
                }

                for (int i = 0; i < staleKeys.Count; i++)
                {
                    string stale = staleKeys[i];
                    _awaitingByPeerKey.Remove(stale);
                    _noProgressPeerKeys.Add(stale);
                }

                DateTime now = DateTime.UtcNow;
                var timedOutKeys = new List<string>();
                foreach (var kv in _awaitingByPeerKey)
                {
                    if ((now - kv.Value) > HeaderResponseTimeout)
                        timedOutKeys.Add(kv.Key);
                }

                for (int i = 0; i < timedOutKeys.Count; i++)
                {
                    string key = timedOutKeys[i];
                    _awaitingByPeerKey.Remove(key);
                    _noProgressPeerKeys.Add(key);
                    _log?.Warn("HeaderSync", $"headers timeout; rotating peer {key}.");
                }

                int slots = MaxHeaderSyncPeers - _awaitingByPeerKey.Count;
                if (slots > 0)
                {
                    var selected = SelectPublicPeers(peers, slots, _noProgressPeerKeys, _awaitingByPeerKey.Keys);
                    for (int i = 0; i < selected.Count; i++)
                    {
                        var peer = selected[i];
                        string key = NormalizePeerKey(peer.SessionKey);
                        _awaitingByPeerKey[key] = now;
                        peersToQuery.Add((peer, key));
                        _log?.Info("HeaderSync", $"selected public peer {peer.RemoteEndpoint}");
                    }
                }

                if (_awaitingByPeerKey.Count == 0 && peersToQuery.Count == 0)
                {
                    if (_noProgressPeerKeys.Count > 0)
                    {
                        _completed = true;
                        _noProgressPeerKeys.Clear();
                        shouldEmitCompletion = true;
                    }
                }
            }

            if (shouldEmitCompletion)
            {
                var plan = BuildDownloadPlan();
                _log?.Info(
                    "HeaderSync",
                    $"complete: best={ShortHex(plan.BestHeaderHash)}, fork={ShortHex(plan.ForkPointHash)}, missing={plan.MissingBlockHashes.Count}, revalidate={plan.RevalidateBlockHashes.Count}");
                try { _onSyncCompleted(plan); } catch { }
                return;
            }

            var locator = BuildLocatorPayload();
            for (int i = 0; i < peersToQuery.Count; i++)
            {
                var (peerToQuery, peerKey) = peersToQuery[i];
                try
                {
                    await _sendFrameAsync(peerToQuery, MsgType.GetHeaders, locator, ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _log?.Warn("HeaderSync", $"getheaders send failed for {peerToQuery.RemoteEndpoint}: {ex.Message}");
                    FailPeer(peerKey);
                }
            }
        }

        private async Task ProcessHeadersLoopAsync(CancellationToken externalCt)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(externalCt, _disposeCts.Token);
            var ct = linked.Token;

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    if (!await _headersQueue.Reader.WaitToReadAsync(ct).ConfigureAwait(false))
                        break;

                    while (_headersQueue.Reader.TryRead(out var frame))
                    {
                        bool process;
                        lock (_gate)
                        {
                            process = !_completed &&
                                      _awaitingByPeerKey.Remove(frame.PeerKey);
                        }

                        if (!process)
                            continue;

                        if (!TryParseHeadersPayload(frame.Payload, out var headers))
                        {
                            _log?.Warn("HeaderSync", "invalid headers payload");
                            FailPeer(frame.PeerKey);
                            continue;
                        }

                        if (!TryValidateAndCommitBatch(headers, out var committed, out var reason))
                        {
                            _log?.Warn("HeaderSync", $"header batch rejected: {reason}");
                            FailPeer(frame.PeerKey);
                            continue;
                        }

                        if (committed > 0)
                            _log?.Info("HeaderSync", $"headers committed: {committed}");

                        if (committed == 0)
                        {
                            lock (_gate)
                            {
                                _noProgressPeerKeys.Add(frame.PeerKey);
                            }
                            _log?.Info("HeaderSync", $"no new headers from peer {frame.PeerKey}; rotating.");
                            RequestTick();
                            continue;
                        }

                        lock (_gate)
                        {
                            _noProgressPeerKeys.Remove(frame.PeerKey);
                        }

                        RequestTick();
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _log?.Warn("HeaderSync", $"processing error: {ex.Message}");
                }
            }
        }

        private void FailPeer(string peerKey)
        {
            if (string.IsNullOrWhiteSpace(peerKey))
                return;

            lock (_gate)
            {
                _awaitingByPeerKey.Remove(peerKey);
                _noProgressPeerKeys.Add(peerKey);
            }

            RequestTick();
        }

        private byte[] BuildLocatorPayload()
        {
            var locator = BuildLocatorHashes();
            return BlockDownloadManager.BuildHashListPayload(locator, MaxLocatorHashes);
        }

        private List<byte[]> BuildLocatorHashes()
        {
            if (!TryBuildBestChain(out var bestChain) || bestChain.Count == 0)
                return new List<byte[]>();

            var locator = new List<byte[]>(Math.Min(MaxLocatorHashes, bestChain.Count));
            int idx = bestChain.Count - 1;
            int step = 1;
            int picks = 0;

            while (idx >= 0 && locator.Count < MaxLocatorHashes)
            {
                locator.Add((byte[])bestChain[idx].Hash.Clone());

                if (idx == 0)
                    break;

                if (picks < 10)
                {
                    idx -= 1;
                }
                else
                {
                    idx -= step;
                    step = Math.Min(step * 2, 1 << 20);
                }

                picks++;
            }

            var genesisHash = bestChain[0].Hash;
            if (genesisHash is { Length: 32 })
            {
                string g = ToHex(genesisHash);
                bool hasGenesis = false;
                for (int i = 0; i < locator.Count; i++)
                {
                    if (string.Equals(ToHex(locator[i]), g, StringComparison.Ordinal))
                    {
                        hasGenesis = true;
                        break;
                    }
                }

                if (!hasGenesis)
                {
                    if (locator.Count < MaxLocatorHashes)
                        locator.Add((byte[])genesisHash.Clone());
                    else
                        locator[locator.Count - 1] = (byte[])genesisHash.Clone();
                }
            }

            return locator;
        }

        private HeaderSyncPlan BuildDownloadPlan()
        {
            if (!TryBuildBestChain(out var chain) || chain.Count == 0)
            {
                return new HeaderSyncPlan(
                    BestHeaderHash: Array.Empty<byte>(),
                    ForkPointHash: Array.Empty<byte>(),
                    MissingBlockHashes: Array.Empty<byte[]>(),
                    RevalidateBlockHashes: Array.Empty<byte[]>());
            }

            return BuildDownloadPlan(chain);
        }

        private static HeaderSyncPlan BuildDownloadPlan(List<BlockIndexStore.HeaderRecord> chain)
        {
            if (chain == null || chain.Count == 0)
            {
                return new HeaderSyncPlan(
                    BestHeaderHash: Array.Empty<byte>(),
                    ForkPointHash: Array.Empty<byte>(),
                    MissingBlockHashes: Array.Empty<byte[]>(),
                    RevalidateBlockHashes: Array.Empty<byte[]>());
            }

            int forkIdx = -1;
            for (int i = chain.Count - 1; i >= 0; i--)
            {
                if (BlockIndexStore.IsValidatedStatus(chain[i].Status))
                {
                    forkIdx = i;
                    break;
                }
            }

            if (forkIdx < 0)
                forkIdx = 0;

            var missing = new List<byte[]>();
            var revalidate = new List<byte[]>();

            for (int i = forkIdx + 1; i < chain.Count; i++)
            {
                var row = chain[i];
                if (row.IsBad || row.BadAncestor)
                    continue;

                if (row.Status == BlockIndexStore.StatusHaveBlockPayload)
                {
                    revalidate.Add((byte[])row.Hash.Clone());
                    continue;
                }

                bool hasPayload = row.FileId >= 0 && row.RecordOffset >= 0 && row.RecordSize > 0;
                if (row.Status == BlockIndexStore.StatusHeaderOnly || !hasPayload)
                {
                    missing.Add((byte[])row.Hash.Clone());
                }
            }

            return new HeaderSyncPlan(
                BestHeaderHash: (byte[])chain[^1].Hash.Clone(),
                ForkPointHash: (byte[])chain[forkIdx].Hash.Clone(),
                MissingBlockHashes: missing,
                RevalidateBlockHashes: revalidate);
        }

        private bool TryValidateAndCommitBatch(
            List<BlockHeader> headers,
            out int committed,
            out string reason)
        {
            committed = 0;
            reason = "ok";

            var pendingByHash = new Dictionary<string, HeaderContext>(StringComparer.Ordinal);
            var accepted = new List<(HeaderContext context, byte[] miner, byte[] headerPowBytes)>(headers.Count);
            ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            for (int i = 0; i < headers.Count; i++)
            {
                var header = headers[i];
                byte[] headerPow;

                try
                {
                    headerPow = header.ToHashBytes();
                }
                catch
                {
                    reason = "header serialization failed";
                    return false;
                }

                byte[] hash;
                try
                {
                    hash = Argon2Util.ComputeHash(
                        headerPow,
                        memoryKb: ConsensusRules.PowMemoryKb,
                        iterations: ConsensusRules.PowIterations,
                        parallelism: ConsensusRules.PowParallelism);
                }
                catch
                {
                    reason = "hash compute failed";
                    return false;
                }

                if (hash is not { Length: 32 })
                {
                    reason = "header hash length invalid";
                    return false;
                }

                string hashHex = ToHex(hash);
                if (pendingByHash.ContainsKey(hashHex) || BlockIndexStore.ContainsHash(hash))
                    continue;

                if (!Difficulty.IsValidTarget(header.Target))
                {
                    reason = "target out of range";
                    return false;
                }

                byte[] haveTarget = (byte[])header.Target.Clone();

                if (!Difficulty.Meets(hash, haveTarget))
                {
                    reason = "pow not meeting target";
                    return false;
                }

                if (header.Timestamp > now + (ulong)MaxFutureTimeDriftSeconds)
                {
                    reason = "timestamp too far in future";
                    return false;
                }

                var prev = header.PreviousBlockHash;
                if (prev is not { Length: 32 })
                {
                    reason = "invalid prev hash";
                    return false;
                }

                if (IsZero32(prev))
                {
                    if (!BlockIndexStore.ContainsHash(hash))
                    {
                        reason = "unexpected genesis-like header";
                        return false;
                    }

                    continue;
                }

                if (!TryResolveHeaderContext(prev, pendingByHash, out var parent))
                {
                    reason = "parent header unknown";
                    return false;
                }

                if (parent.IsBad || parent.BadAncestor)
                {
                    reason = "parent marked invalid";
                    return false;
                }

                ulong height;
                try
                {
                    height = checked(parent.Height + 1UL);
                }
                catch
                {
                    reason = "height overflow";
                    return false;
                }

                if (!TryComputeMedianTimePast(prev, pendingByHash, out var mtp))
                {
                    reason = "cannot compute median time past";
                    return false;
                }

                if (header.Timestamp <= mtp)
                {
                    reason = "timestamp not greater than median time past";
                    return false;
                }

                if (!TryComputeExpectedTarget(height, prev, pendingByHash, out var expectedTarget))
                {
                    reason = "difficulty computation failed";
                    return false;
                }

                if (!BytesEqual32(haveTarget, expectedTarget))
                {
                    reason = "target mismatch";
                    return false;
                }

                UInt128 chainwork = parent.Chainwork + ChainworkUtil.IncrementFromTarget(haveTarget);

                var ctx = new HeaderContext(
                    Hash: (byte[])hash.Clone(),
                    PrevHash: (byte[])prev.Clone(),
                    Height: height,
                    Timestamp: header.Timestamp,
                    Target: (byte[])haveTarget.Clone(),
                    Chainwork: chainwork,
                    Status: BlockIndexStore.StatusHeaderOnly,
                    IsBad: false,
                    BadAncestor: false);

                pendingByHash[hashHex] = ctx;
                accepted.Add((ctx, (byte[])header.Miner.Clone(), headerPow));
            }

            if (accepted.Count == 0)
                return true;

            lock (Db.Sync)
            {
                using var tx = Db.Connection!.BeginTransaction();

                for (int i = 0; i < accepted.Count; i++)
                {
                    var entry = accepted[i];
                    var row = entry.context;
                    if (BlockIndexStore.ContainsHash(row.Hash, tx))
                        continue;

                    BlockIndexStore.Upsert(
                        hash: row.Hash,
                        prevHash: row.PrevHash,
                        height: row.Height,
                        ts: row.Timestamp,
                        target32: row.Target,
                        miner32: entry.miner,
                        chainwork: row.Chainwork,
                        fileId: -1,
                        recordOffset: -1,
                        recordSize: 0,
                        statusFlags: BlockIndexStore.StatusHeaderOnly,
                        tx: tx);

                    BlockIndexStore.UpsertHeaderBytes(row.Hash, entry.headerPowBytes, tx);
                    committed++;
                }

                tx.Commit();
            }

            return true;
        }

        private static bool TryParseHeaderPowBytes(ReadOnlySpan<byte> bytes, out BlockHeader header)
        {
            header = new BlockHeader();
            if (bytes.Length != BlockHeader.PowHeaderSize)
                return false;

            int o = 0;
            byte version = bytes[o++];
            var prev = bytes.Slice(o, 32).ToArray(); o += 32;
            var merkle = bytes.Slice(o, 32).ToArray(); o += 32;
            ulong ts = BinaryPrimitives.ReadUInt64BigEndian(bytes.Slice(o, 8)); o += 8;
            var target = bytes.Slice(o, 32).ToArray(); o += 32;
            uint nonce = BinaryPrimitives.ReadUInt32BigEndian(bytes.Slice(o, 4)); o += 4;
            var miner = bytes.Slice(o, 32).ToArray(); o += 32;

            if (o != BlockHeader.PowHeaderSize)
                return false;

            try
            {
                header = new BlockHeader
                {
                    Version = version,
                    PreviousBlockHash = prev,
                    MerkleRoot = merkle,
                    Timestamp = ts,
                    Target = target,
                    Nonce = nonce,
                    Miner = miner
                };
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static bool TryResolveHeaderContext(
            byte[] hash,
            IReadOnlyDictionary<string, HeaderContext> pendingByHash,
            out HeaderContext context)
        {
            context = default;
            if (hash is not { Length: 32 })
                return false;

            string key = ToHex(hash);
            if (pendingByHash.TryGetValue(key, out context))
                return true;

            if (!BlockIndexStore.TryGetHeaderRecord(hash, out var row))
                return false;

            context = new HeaderContext(
                Hash: (byte[])row.Hash.Clone(),
                PrevHash: (byte[])row.PrevHash.Clone(),
                Height: row.Height,
                Timestamp: row.Timestamp,
                Target: (byte[])row.Target.Clone(),
                Chainwork: row.Chainwork,
                Status: row.Status,
                IsBad: row.IsBad,
                BadAncestor: row.BadAncestor);
            return true;
        }

        private static bool TryComputeMedianTimePast(
            byte[] parentHash,
            IReadOnlyDictionary<string, HeaderContext> pendingByHash,
            out ulong mtp)
        {
            mtp = 0;
            var values = new List<ulong>(MedianTimePastWindow);

            var cur = (byte[])parentHash.Clone();
            for (int i = 0; i < MedianTimePastWindow; i++)
            {
                if (!TryResolveHeaderContext(cur, pendingByHash, out var ctx))
                    break;

                if (ctx.IsBad || ctx.BadAncestor)
                    return false;

                values.Add(ctx.Timestamp);
                if (ctx.Height == 0 || IsZero32(ctx.PrevHash))
                    break;

                cur = ctx.PrevHash;
            }

            if (values.Count == 0)
                return false;

            values.Sort();
            mtp = values[values.Count / 2];
            return true;
        }

        private static bool TryComputeExpectedTarget(
            ulong nextHeight,
            byte[] parentHash,
            IReadOnlyDictionary<string, HeaderContext> pendingByHash,
            out byte[] expectedTarget)
        {
            expectedTarget = Difficulty.PowLimit.ToArray();
            if (nextHeight == 0)
                return true;

            if (!TryResolveHeaderContext(parentHash, pendingByHash, out var parent))
                return false;

            if (parent.IsBad || parent.BadAncestor)
                return false;

            byte[] lastTarget;
            try
            {
                lastTarget = Difficulty.ClampTarget(parent.Target);
            }
            catch
            {
                return false;
            }

            ulong end = nextHeight - 1;
            int samples = (int)Math.Min((ulong)RetargetWindowSolveTimes, end);
            if (samples < 3)
            {
                expectedTarget = lastTarget;
                return true;
            }

            var newestToOldest = new List<HeaderContext>(samples + 1);
            var cur = (byte[])parentHash.Clone();
            while (newestToOldest.Count < samples + 1)
            {
                if (!TryResolveHeaderContext(cur, pendingByHash, out var ctx))
                    break;

                if (ctx.IsBad || ctx.BadAncestor)
                    return false;

                newestToOldest.Add(ctx);
                if (ctx.Height == 0 || IsZero32(ctx.PrevHash))
                    break;

                cur = ctx.PrevHash;
            }

            if (newestToOldest.Count < 4)
            {
                expectedTarget = lastTarget;
                return true;
            }

            int usableSamples = Math.Min(samples, newestToOldest.Count - 1);
            if (usableSamples < 3)
            {
                expectedTarget = lastTarget;
                return true;
            }

            newestToOldest.RemoveRange(usableSamples + 1, newestToOldest.Count - (usableSamples + 1));
            newestToOldest.Reverse(); // oldest -> newest

            BigInteger sumWst = BigInteger.Zero;
            BigInteger sumDiff = BigInteger.Zero;

            ulong prevTs = newestToOldest[0].Timestamp;
            for (int i = 1; i <= usableSamples; i++)
            {
                long solveTime = unchecked((long)newestToOldest[i].Timestamp - (long)prevTs);
                prevTs = newestToOldest[i].Timestamp;
                if (solveTime <= 0)
                    solveTime = 1;

                sumWst += (BigInteger)i * solveTime;

                byte[] t;
                try
                {
                    t = Difficulty.ClampTarget(newestToOldest[i].Target);
                }
                catch
                {
                    return false;
                }

                sumDiff += Difficulty.TargetToDifficulty(t);
            }

            if (sumWst <= 0 || sumDiff <= 0)
            {
                expectedTarget = lastTarget;
                return true;
            }

            BigInteger k = (BigInteger)usableSamples * (usableSamples + 1) / 2;
            BigInteger num = sumDiff * DifficultyCalculator.TargetBlockTimeSeconds * k;
            BigInteger den = sumWst * usableSamples;
            if (den <= 0)
            {
                expectedTarget = lastTarget;
                return true;
            }

            BigInteger nextDiff = num / den;
            if (nextDiff <= 0)
                nextDiff = BigInteger.One;

            BigInteger lastDiff = Difficulty.TargetToDifficulty(lastTarget);
            BigInteger maxUp = lastDiff * RetargetMaxAdjustFactor;
            BigInteger maxDown = lastDiff / RetargetMaxAdjustFactor;
            if (maxDown <= 0)
                maxDown = BigInteger.One;

            if (nextDiff > maxUp) nextDiff = maxUp;
            if (nextDiff < maxDown) nextDiff = maxDown;

            expectedTarget = Difficulty.DifficultyToTarget(nextDiff);
            return true;
        }

        private static List<PeerSession> SelectPublicPeers(
            IReadOnlyCollection<PeerSession> peers,
            int maxCount,
            ISet<string>? excludedPeerKeys = null,
            IEnumerable<string>? alreadySelectedPeerKeys = null)
        {
            var selected = new List<PeerSession>(Math.Max(0, maxCount));
            if (maxCount <= 0)
                return selected;

            HashSet<string>? skip = null;
            if (alreadySelectedPeerKeys != null)
                skip = new HashSet<string>(alreadySelectedPeerKeys, StringComparer.Ordinal);

            foreach (var peer in peers)
            {
                if (peer == null || !peer.HandshakeOk || !peer.Client.Connected || !peer.RemoteIsPublic)
                    continue;

                string key = NormalizePeerKey(peer.SessionKey);
                if (excludedPeerKeys != null && excludedPeerKeys.Contains(key))
                    continue;
                if (skip != null && skip.Contains(key))
                    continue;

                selected.Add(peer);
                if (selected.Count >= maxCount)
                    break;
            }

            return selected;
        }

        private static PeerSession? FindPeerByKey(IReadOnlyCollection<PeerSession> peers, string peerKey)
        {
            foreach (var peer in peers)
            {
                if (peer == null)
                    continue;

                if (string.Equals(NormalizePeerKey(peer.SessionKey), peerKey, StringComparison.Ordinal))
                    return peer;
            }

            return null;
        }

        private static bool TryBuildBestChain(out List<BlockIndexStore.HeaderRecord> chain)
        {
            chain = new List<BlockIndexStore.HeaderRecord>();

            if (!BlockIndexStore.TryGetBestHeaderTip(out var tip, out _, out _))
                return false;

            var reverse = new List<BlockIndexStore.HeaderRecord>();
            var cur = tip;
            int guard = 0;

            while (cur is { Length: 32 })
            {
                if (!BlockIndexStore.TryGetHeaderRecord(cur, out var row))
                    return false;

                if (row.IsBad || row.BadAncestor)
                    return false;

                reverse.Add(row);
                if (row.Height == 0 || IsZero32(row.PrevHash))
                    break;

                cur = row.PrevHash;
                guard++;
                if (guard > 5_000_000)
                    return false;
            }

            reverse.Reverse();
            chain = reverse;
            return chain.Count > 0;
        }

        private static bool TryLoadHeaderPowBytes(byte[] hash, out byte[] headerPowBytes)
        {
            headerPowBytes = Array.Empty<byte>();
            if (hash is not { Length: 32 })
                return false;

            if (BlockIndexStore.TryGetHeaderBytes(hash, out headerPowBytes))
                return true;

            var block = BlockStore.GetBlockByHash(hash);
            if (block?.Header == null)
                return false;

            headerPowBytes = block.Header.ToHashBytes();
            return headerPowBytes.Length == BlockHeader.PowHeaderSize;
        }

        private void RequestTick()
        {
            try { _tick.Release(); } catch (SemaphoreFullException) { }
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

        private static string NormalizePeerKey(string endpoint)
            => (endpoint ?? string.Empty).Trim().ToLowerInvariant();

        private static string ToHex(byte[] hash)
            => Convert.ToHexString(hash).ToLowerInvariant();

        private static string ShortHex(byte[] hash)
        {
            if (hash is not { Length: 32 })
                return "n/a";

            string hex = ToHex(hash);
            return hex.Length > 16 ? hex[..16] + "..." : hex;
        }

        public void Dispose()
        {
            _disposeCts.Cancel();
            _tick.Dispose();
            _disposeCts.Dispose();
        }
    }
}
