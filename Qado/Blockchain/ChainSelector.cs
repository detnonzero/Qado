using System;
using System.Collections.Generic;
using System.Windows;
using Microsoft.Data.Sqlite;
using Qado.Logging;
using Qado.Storage;

namespace Qado.Blockchain
{
    public static class ChainSelector
    {
        public static void MaybeAdoptNewTip(byte[] candidateTipHash, ILogSink? log = null)
        {
            if (candidateTipHash is not { Length: 32 })
                return;

            var canonTipHeight = BlockStore.GetLatestHeight();
            var canonTipHash = BlockStore.GetCanonicalHashAtHeight(canonTipHeight);

            if (canonTipHash is not { Length: 32 })
                return;

            if (BytesEqual32(canonTipHash, candidateTipHash))
                return;

            UInt128 currentWork = BlockIndexStore.GetChainwork(canonTipHash);
            UInt128 candidateWork = BlockIndexStore.GetChainwork(candidateTipHash);

            if (candidateWork == 0)
            {
                log?.Warn("ChainSel", "Candidate has no chainwork (0). Skipping adoption until chainwork is available.");
                return;
            }

            if (currentWork != 0 && candidateWork <= currentWork)
                return;

            log?.Warn("ChainSel", $"Adopting stronger chain (oldTip={ToHex(canonTipHash, 16)} → newTip={ToHex(candidateTipHash, 16)}).");

            if (!TryBuildCandidateHeightMap(candidateTipHash, out var candTipHeight, out var candByHeight, log))
            {
                log?.Warn("ChainSel", "Failed to build candidate chain map; skipping adoption.");
                return;
            }

            ulong fromHeight = FindFirstDivergingHeight(candTipHeight, candByHeight);

            var newCanonAsc = new List<(ulong Height, byte[] Hash)>(checked((int)Math.Min((candTipHeight - fromHeight) + 1, 5_000_000)));
            for (ulong h = fromHeight; h <= candTipHeight; h++)
            {
                if (!candByHeight.TryGetValue(h, out var hashAtH) || hashAtH is not { Length: 32 })
                {
                    log?.Warn("ChainSel", $"Candidate chain map missing height {h}; skipping adoption.");
                    return;
                }
                newCanonAsc.Add((h, hashAtH));
            }

            ulong rollbackTop = canonTipHeight;

            List<Block> newBlocksAsc;

            lock (Db.Sync)
            {
                if (!TryPrefetchBlocksAscending(newCanonAsc, out newBlocksAsc, log))
                {
                    log?.Warn("ChainSel", "Failed to prefetch candidate blocks; skipping adoption.");
                    return;
                }

                try
                {
                    using var tx = Db.Connection.BeginTransaction();

                    if (!TryPrefetchOldCanonDescending(fromHeight, rollbackTop, tx, out var oldCanonDesc, log))
                    {
                        log?.Warn("ChainSel", "Failed to prefetch old canonical hashes; skipping adoption.");
                        tx.Rollback();
                        return;
                    }

                    for (int i = 0; i < oldCanonDesc.Count; i++)
                        StateUndoStore.RollbackBlock(oldCanonDesc[i], tx);

                    BlockStore.DeleteCanonicalFromHeight(fromHeight, tx);

                    for (int i = 0; i < newCanonAsc.Count; i++)
                        BlockStore.SetCanonicalHashAtHeight(newCanonAsc[i].Height, newCanonAsc[i].Hash, tx);

                    for (int i = 0; i < newBlocksAsc.Count; i++)
                        StateApplier.ApplyBlockWithUndo(newBlocksAsc[i], tx);

                    MetaStore.Set("LatestBlockHash", Hex(candidateTipHash), tx);
                    MetaStore.Set("LatestHeight", candTipHeight.ToString(), tx);

                    tx.Commit();
                    log?.Info("ChainSel", $"Adopted tip {ToHex(candidateTipHash, 16)} @ height {candTipHeight} (fromHeight={fromHeight}).");
                }
                catch (Exception ex)
                {
                    log?.Error("ChainSel", $"Atomic adoption failed: {ex.Message}");
                    TryNotifyUiReload();
                    return;
                }
            }

            TryNotifyUiReload();
        }


        private static bool TryPrefetchOldCanonDescending(
            ulong fromHeight,
            ulong rollbackTop,
            SqliteTransaction tx,
            out List<byte[]> hashesDesc,
            ILogSink? log)
        {
            hashesDesc = new List<byte[]>();

            if (rollbackTop < fromHeight)
                return true;

            ulong h = rollbackTop;
            while (true)
            {
                var old = BlockStore.GetCanonicalHashAtHeight(h, tx);
                if (old is not { Length: 32 })
                {
                    log?.Warn("ChainSel", $"Canonical hash missing at height {h} during rollback prefetch. Aborting adoption.");
                    return false;
                }

                hashesDesc.Add(old);

                if (h == fromHeight) break;
                h--;
            }

            return true;
        }

        private static bool TryPrefetchBlocksAscending(
            List<(ulong Height, byte[] Hash)> mappingAsc,
            out List<Block> blocksAsc,
            ILogSink? log)
        {
            blocksAsc = new List<Block>(mappingAsc.Count);

            for (int i = 0; i < mappingAsc.Count; i++)
            {
                var (h, hash) = mappingAsc[i];

                var b = BlockStore.GetBlockByHash(hash);
                if (b == null)
                {
                    log?.Warn("ChainSel", $"Missing candidate block payload for height {h} ({ToHex(hash, 16)}).");
                    return false;
                }

                b.BlockHeight = h;
                b.BlockHash = (byte[])hash.Clone();
                blocksAsc.Add(b);
            }

            if (!ValidateCandidateSequenceStateless(mappingAsc, blocksAsc, log))
                return false;

            return true;
        }

        private static bool ValidateCandidateSequenceStateless(
            List<(ulong Height, byte[] Hash)> mappingAsc,
            List<Block> blocksAsc,
            ILogSink? log)
        {
            if (mappingAsc.Count != blocksAsc.Count)
                return false;

            if (blocksAsc.Count == 0)
                return true;

            var firstHeight = mappingAsc[0].Height;
            var first = blocksAsc[0];

            if (firstHeight == 0)
            {
                if (first.Header?.PreviousBlockHash is not { Length: 32 } || !IsZero32(first.Header.PreviousBlockHash))
                {
                    log?.Warn("ChainSel", "Candidate genesis does not have zero prev hash.");
                    return false;
                }

                if (first.BlockHash is not { Length: 32 } || IsZero32(first.BlockHash))
                    first.BlockHash = first.ComputeBlockHash();

                var expectedGenesis = GenesisBlockProvider.GetGenesisBlock();
                if (!BytesEqual32(first.BlockHash, expectedGenesis.BlockHash!))
                {
                    log?.Warn("ChainSel", "Candidate genesis hash mismatch.");
                    return false;
                }

                if (!GenesisBlockProvider.ValidateGenesisBlock(first, out var gReason))
                {
                    log?.Warn("ChainSel", $"Candidate genesis invalid: {gReason}");
                    return false;
                }
            }
            else
            {
                var expectedParent = BlockStore.GetCanonicalHashAtHeight(firstHeight - 1);
                if (expectedParent is not { Length: 32 })
                {
                    log?.Warn("ChainSel", $"Cannot validate candidate parent at height {firstHeight - 1}.");
                    return false;
                }

                if (first.Header?.PreviousBlockHash is not { Length: 32 } || !BytesEqual32(first.Header.PreviousBlockHash, expectedParent))
                {
                    log?.Warn("ChainSel", $"Candidate chain parent mismatch at height {firstHeight}.");
                    return false;
                }
            }

            for (int i = 0; i < blocksAsc.Count; i++)
            {
                var b = blocksAsc[i];

                if (!BlockValidator.ValidateNetworkSideBlockStateless(b, out var reason))
                {
                    log?.Warn("ChainSel", $"Candidate block invalid at height {b.BlockHeight}: {reason}");
                    return false;
                }

                if (i == 0) continue;

                var prev = blocksAsc[i - 1];
                if (b.Header?.PreviousBlockHash is not { Length: 32 } || !BytesEqual32(b.Header.PreviousBlockHash, prev.BlockHash))
                {
                    log?.Warn("ChainSel", $"Candidate block linkage mismatch at height {b.BlockHeight}.");
                    return false;
                }
            }

            return true;
        }


        private static bool TryBuildCandidateHeightMap(
            byte[] candidateTipHash,
            out ulong tipHeight,
            out Dictionary<ulong, byte[]> byHeight,
            ILogSink? log)
        {
            tipHeight = 0;
            byHeight = new Dictionary<ulong, byte[]>();

            var curHash = candidateTipHash;
            int guard = 0;

            while (curHash is { Length: 32 })
            {
                if (!BlockIndexStore.TryGetMeta(curHash, out var h, out var prevHash, out _))
                {
                    log?.Warn("ChainSel", $"Missing block_index row for hash {ToHex(curHash, 16)} while walking candidate chain.");
                    return false;
                }

                if (guard == 0)
                    tipHeight = h;

                if (byHeight.TryGetValue(h, out var existing))
                {
                    if (!BytesEqual32(existing, curHash))
                    {
                        log?.Warn("ChainSel", $"Height collision in candidate chain walk at height {h}; aborting.");
                        return false;
                    }
                }
                else
                {
                    byHeight[h] = (byte[])curHash.Clone();
                }

                if (h == 0)
                    break;

                if (prevHash is not { Length: 32 })
                {
                    log?.Warn("ChainSel", $"Invalid prevHash length while walking candidate chain at height {h}.");
                    return false;
                }

                if (IsZero32(prevHash))
                {
                    log?.Warn("ChainSel", $"Non-genesis block at height {h} has zero prevHash; aborting.");
                    return false;
                }

                curHash = prevHash;

                guard++;
                if (guard > 5_000_000)
                {
                    log?.Error("ChainSel", "Candidate chain walk exceeded guard limit (possible loop/corruption).");
                    return false;
                }
            }

            return byHeight.Count > 0;
        }

        private static ulong FindFirstDivergingHeight(ulong candTipHeight, Dictionary<ulong, byte[]> candByHeight)
        {
            ulong h = candTipHeight;
            while (true)
            {
                if (candByHeight.TryGetValue(h, out var candHash) && candHash is { Length: 32 })
                {
                    var canonHash = BlockStore.GetCanonicalHashAtHeight(h);
                    if (canonHash is { Length: 32 } && BytesEqual32(canonHash, candHash))
                        return h + 1;
                }

                if (h == 0) break;
                h--;
            }

            return 0;
        }


        private static void TryNotifyUiReload()
        {
            try
            {
                Application.Current?.Dispatcher.InvokeAsync(() =>
                {
                    try
                    {
                        if (Application.Current?.MainWindow is Qado.MainWindow mw)
                        {
                            mw.RefreshUiAfterNewBlock();
                            return;
                        }

                        if (Qado.MainWindow.Instance != null)
                            Qado.MainWindow.Instance.RefreshUiAfterNewBlock();
                    }
                    catch { }
                });
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

        private static string ToHex(byte[] data, int takeChars = -1)
        {
            if (data is null || data.Length == 0) return "";
            var hex = Convert.ToHexString(data).ToLowerInvariant();
            if (takeChars > 0 && hex.Length > takeChars) return hex[..takeChars] + "…";
            return hex;
        }

        private static string Hex(byte[] b) => Convert.ToHexString(b).ToLowerInvariant();
    }
}

