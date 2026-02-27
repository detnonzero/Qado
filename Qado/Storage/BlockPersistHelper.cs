using System;
using Microsoft.Data.Sqlite;
using Qado.Blockchain;
using Qado.Logging;
using Qado.Mempool;

namespace Qado.Storage
{
    public static class BlockPersistHelper
    {
        public static void Persist(Block block, ILogSink? log = null, MempoolManager? mempool = null)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            EnsureBlockHash(block);

            bool canonExtended = false;

            lock (Db.Sync)
            {
                using var tx = Db.Connection.BeginTransaction();

                BlockStore.SaveBlock(block, tx, BlockIndexStore.StatusSideStatelessAccepted);

                if (TryExtendCanonTipWithState(block, tx, out _))
                {
                    canonExtended = true;
                }
                else
                {
                    log?.Info("Persist", $"Stored (side/reorg candidate): h={block.BlockHeight}");
                }

                tx.Commit();
            }

            if (!canonExtended)
            {
                try { ChainSelector.MaybeAdoptNewTip(block.BlockHash!, log, mempool); }
                catch (Exception ex) { log?.Warn("Persist", $"ChainSelector adoption failed: {ex.Message}"); }
            }
        }

        private static bool TryExtendCanonTipWithState(Block block, SqliteTransaction tx, out ulong newHeight)
        {
            newHeight = 0;

            var prev = block.Header?.PreviousBlockHash;
            if (prev is not { Length: 32 }) return false;

            ulong tipH = BlockStore.GetLatestHeight(tx);
            var tipHash = BlockStore.GetCanonicalHashAtHeight(tipH, tx);
            if (tipHash is not { Length: 32 }) return false;

            if (!BytesEqual32(prev, tipHash))
                return false;

            newHeight = tipH + 1UL;

            EnsureBlockHash(block);
            block.BlockHeight = newHeight;

                StateApplier.ApplyBlockWithUndo(block, tx);

                BlockStore.SetCanonicalHashAtHeight(newHeight, block.BlockHash!, tx);
                BlockIndexStore.SetStatus(block.BlockHash!, BlockIndexStore.StatusCanonicalStateValidated, tx);
                MetaStore.Set("LatestBlockHash", Convert.ToHexString(block.BlockHash!).ToLowerInvariant(), tx);
                MetaStore.Set("LatestHeight", newHeight.ToString(), tx);

            return true;
        }

        private static void EnsureBlockHash(Block block)
        {
            if (block.BlockHash is { Length: 32 } && !IsZero32(block.BlockHash))
                return;

            block.BlockHash = block.ComputeBlockHash();
        }

        private static bool IsZero32(byte[] h)
        {
            for (int i = 0; i < 32; i++) if (h[i] != 0) return false;
            return true;
        }

        private static bool BytesEqual32(byte[] a, byte[] b)
        {
            if (a.Length != 32 || b.Length != 32) return false;
            for (int i = 0; i < 32; i++)
                if (a[i] != b[i]) return false;
            return true;
        }
    }
}

