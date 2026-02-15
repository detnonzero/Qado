using System;
using Qado.Blockchain;
using Qado.Mempool;
using Qado.Storage;
using Qado.Logging;

namespace Qado.Validation
{
    public static class ReorgHelper
    {
        public static bool TryReorgOneBlock(Block incoming, MempoolManager mempool, out string error, ILogSink? log = null)
        {
            error = string.Empty;

            if (incoming is null) { error = "Incoming block is null"; return false; }
            if (incoming.Header is null) { error = "Incoming header is null"; return false; }

            if (incoming.BlockHash is not { Length: 32 })
                incoming.BlockHash = incoming.ComputeBlockHash();

            ulong tipHeight = BlockStore.GetLatestHeight();
            if (tipHeight == 0)
            {
                error = "No blocks found (chain is empty)";
                log?.Warn("Reorg", error);
                return false;
            }

            var tipHash = BlockStore.GetCanonicalHashAtHeight(tipHeight);
            if (tipHash is not { Length: 32 })
            {
                error = $"Failed to load canonical tip hash at height {tipHeight}";
                log?.Error("Reorg", error);
                return false;
            }

            var current = BlockStore.GetBlockByHash(tipHash);
            if (current?.Header is null || current.BlockHash is not { Length: 32 })
            {
                error = $"Failed to load canonical tip payload at height {tipHeight}";
                log?.Error("Reorg", error);
                return false;
            }

            if (incoming.BlockHeight != tipHeight)
            {
                error = $"Reorg(1) only allowed at tip height {tipHeight}, got {incoming.BlockHeight}";
                log?.Warn("Reorg", error);
                return false;
            }

            var incPrev = incoming.Header.PreviousBlockHash;
            var curPrev = current.Header.PreviousBlockHash;

            if (incPrev is not { Length: 32 } || curPrev is not { Length: 32 })
            {
                error = "Missing PreviousBlockHash";
                log?.Warn("Reorg", error);
                return false;
            }

            if (!BytesEqual32(incPrev, curPrev))
            {
                error = "Not a sibling fork: parent hash differs";
                log?.Warn("Reorg", $"{error}. expectedParent={Hex(curPrev, 8)}… gotParent={Hex(incPrev, 8)}…");
                return false;
            }

            if (BytesEqual32(incoming.BlockHash, current.BlockHash))
            {
                log?.Info("Reorg", "Incoming block equals current tip. No action.");
                return false;
            }

            if (!BlockValidator.ValidateNetworkSideBlockStateless(incoming, out var reason))
            {
                error = $"Incoming block invalid: {reason}";
                log?.Warn("Reorg", error);
                return false;
            }

            try
            {
                var currWork = BlockIndexStore.GetChainwork(current.BlockHash);
                var incWork = BlockIndexStore.GetChainwork(incoming.BlockHash);

                if (incWork != 0 && currWork != 0 && incWork <= currWork)
                {
                    error = "Incoming block has worse or equal chainwork";
                    log?.Warn("Reorg", error);
                    return false;
                }
            }
            catch
            {
            }

            var oldTipHash = (byte[])current.BlockHash.Clone();

            try
            {
                BlockPersistHelper.Persist(incoming, log);
            }
            catch (Exception ex)
            {
                error = $"Persist failed: {ex.Message}";
                log?.Error("Reorg", error);
                return false;
            }

            try
            {
                ChainSelector.MaybeAdoptNewTip(incoming.BlockHash, log);
            }
            catch (Exception ex)
            {
                error = $"Adoption attempt failed: {ex.Message}";
                log?.Error("Reorg", error);
                return false;
            }

            var newTipHash = BlockStore.GetCanonicalHashAtHeight(tipHeight);
            bool adopted = newTipHash is { Length: 32 } && BytesEqual32(newTipHash, incoming.BlockHash);

            if (!adopted)
            {
                error = "Reorg not adopted (candidate stored but did not become canonical)";
                log?.Info("Reorg", error);
                return false;
            }

            try
            {
                BlockInsertSafeGuard.RequeueOldBlockTxs(current, incoming, mempool, log);
            }
            catch
            {
            }

            log?.Info("Reorg",
                $"✅ Reorg(1) adopted at height {tipHeight}: oldTip={Hex(oldTipHash, 8)}… → newTip={Hex(incoming.BlockHash, 8)}…");

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

