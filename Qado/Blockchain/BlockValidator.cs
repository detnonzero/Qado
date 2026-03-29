using System;
using System.Collections.Generic;
using Qado.Networking;
using Qado.Storage;
using Microsoft.Data.Sqlite;

namespace Qado.Blockchain
{
    public static class BlockValidator
    {
        private const int MaxTransactionsPerBlock = ConsensusRules.MaxTransactionsPerBlock;
        private const int MaxSignatureChecksPerBlock = ConsensusRules.MaxTransactionsPerBlock;
        private const int MaxFutureTimeDriftSeconds = 2 * 60 * 60; // 2 hours
        private const int MedianTimePastWindow = 11;
        private const uint ChainId = NetworkParams.ChainId;


        public static bool ValidateNetworkBlock(Block block, out string reason, SqliteTransaction? tx = null)
            => ValidateNetworkTipBlock(block, out reason, tx);

        public static bool ValidateNetworkBlockStateless(Block block, out string reason, SqliteTransaction? tx = null)
            => ValidateNetworkSideBlockStateless(block, out reason, tx);

        public static bool ValidateNetworkBlockStateless(Block block, bool requirePrevKnown, out string reason, SqliteTransaction? tx = null)
            => ValidateCommon(
                block,
                strictTxChecks: true,
                requirePrevKnown: requirePrevKnown,
                enforceTipExtending: false,
                validateStateAgainstCanonical: false,
                skipHeightSensitiveChecksWithoutParent: false,
                tx: tx,
                out reason);

        public static bool ValidateNetworkBlockStatelessCandidate(Block block, out string reason, SqliteTransaction? tx = null)
            => ValidateNetworkSideBlockStateless(block, out reason, tx);

        public static bool ValidateNetworkOrphanBlockStateless(Block block, out string reason, SqliteTransaction? tx = null)
            => ValidateCommon(
                block,
                strictTxChecks: true,
                requirePrevKnown: false,
                enforceTipExtending: false,
                validateStateAgainstCanonical: false,
                skipHeightSensitiveChecksWithoutParent: true,
                tx: tx,
                out reason);


        public static bool ValidateSelfMinedBlock(Block block, out string reason, SqliteTransaction? tx = null)
            => ValidateCommon(
                block,
                strictTxChecks: false, // locally mined block: skip strict tx checks
                requirePrevKnown: true,
                enforceTipExtending: true,
                validateStateAgainstCanonical: true,
                skipHeightSensitiveChecksWithoutParent: false,
                tx: tx,
                out reason);

        public static bool ValidateNetworkTipBlock(Block block, out string reason, SqliteTransaction? tx = null)
            => ValidateCommon(
                block,
                strictTxChecks: true,
                requirePrevKnown: true,
                enforceTipExtending: true,
                validateStateAgainstCanonical: true,
                skipHeightSensitiveChecksWithoutParent: false,
                tx: tx,
                out reason);

        public static bool ValidateNetworkTipBlock(
            Block block,
            Block canonicalTip,
            Func<ulong, Block?> getCanonicalBlockByHeight,
            out string reason,
            SqliteTransaction? tx = null)
            => ValidateCommon(
                block,
                strictTxChecks: true,
                requirePrevKnown: true,
                enforceTipExtending: true,
                validateStateAgainstCanonical: true,
                skipHeightSensitiveChecksWithoutParent: false,
                tx: tx,
                out reason,
                canonicalTipOverride: canonicalTip,
                getCanonicalBlockByHeightOverride: getCanonicalBlockByHeight);

        public static bool ValidateNetworkSideBlockStateless(Block block, out string reason, SqliteTransaction? tx = null)
            => ValidateCommon(
                block,
                strictTxChecks: true,
                requirePrevKnown: true,
                enforceTipExtending: false,
                validateStateAgainstCanonical: false,
                skipHeightSensitiveChecksWithoutParent: false,
                tx: tx,
                out reason);

        public static bool ValidateNetworkSideBlockStateless(
            Block block,
            Func<ulong, Block?> getChainBlockByHeight,
            out string reason,
            SqliteTransaction? tx = null)
            => ValidateCommon(
                block,
                strictTxChecks: true,
                requirePrevKnown: true,
                enforceTipExtending: false,
                validateStateAgainstCanonical: false,
                skipHeightSensitiveChecksWithoutParent: false,
                tx: tx,
                out reason,
                canonicalTipOverride: null,
                getCanonicalBlockByHeightOverride: getChainBlockByHeight);


        private static bool ValidateCommon(
            Block block,
            bool strictTxChecks,
            bool requirePrevKnown,
            bool enforceTipExtending,
            bool validateStateAgainstCanonical,
            bool skipHeightSensitiveChecksWithoutParent,
            SqliteTransaction? tx,
            out string reason,
            Block? canonicalTipOverride = null,
            Func<ulong, Block?>? getCanonicalBlockByHeightOverride = null)
        {
            reason = "OK";

            if (block is null) { reason = "Block is null"; return false; }
            if (block.Header is null) { reason = "Missing header"; return false; }

            var header = block.Header;

            if (header.Miner is null || header.Miner.Length != 32) { reason = "Invalid header miner"; return false; }
            if (header.MerkleRoot is null || header.MerkleRoot.Length != 32) { reason = "Invalid header merkle root"; return false; }
            if (header.Target is null || header.Target.Length != 32) { reason = "Invalid header target length"; return false; }

            ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            if (header.Timestamp > now + (ulong)MaxFutureTimeDriftSeconds)
            {
                reason = "Timestamp too far in the future";
                return false;
            }

            var txs = block.Transactions;
            if (txs is null || txs.Count == 0) { reason = "Empty transactions"; return false; }
            if (txs.Count > MaxTransactionsPerBlock) { reason = "Too many transactions"; return false; }

            var merkle = block.ComputeMerkleRoot();
            if (merkle is null || merkle.Length != 32) { reason = "Merkle computation failed"; return false; }
            if (!BytesEqual32(merkle, header.MerkleRoot)) { reason = "Invalid Merkle root"; return false; }

            var coinbase = txs[0];
            if (coinbase is null) { reason = "Missing coinbase"; return false; }

            if (!TransactionValidator.IsCoinbase(coinbase))
            {
                reason = "TX[0] must be coinbase";
                return false;
            }

            if (coinbase.Recipient is null || coinbase.Recipient.Length != 32) { reason = "Coinbase recipient invalid"; return false; }
            if (!BytesEqual32(coinbase.Recipient, header.Miner)) { reason = "Coinbase recipient must equal header miner"; return false; }
            if (coinbase.Fee != 0) { reason = "Coinbase fee must be zero"; return false; }
            if (coinbase.TxNonce != 0) { reason = "Coinbase nonce must be zero"; return false; }
            if (coinbase.ChainId != ChainId) { reason = "Coinbase invalid ChainId"; return false; }
            if (coinbase.Signature is not null && coinbase.Signature.Length != 0) { reason = "Coinbase must not be signed"; return false; }

            Block? prevBlock = null;

            bool isGenesisCandidate =
                block.BlockHeight == 0 &&
                header.PreviousBlockHash is { Length: 32 } &&
                IsAllZero32(header.PreviousBlockHash);

            if (!isGenesisCandidate)
            {
                if (header.PreviousBlockHash is not { Length: 32 })
                {
                    reason = "Invalid previous hash";
                    return false;
                }

                if (enforceTipExtending)
                {
                    var tip = canonicalTipOverride;
                    if (tip?.BlockHash is not { Length: 32 })
                        tip = BlockStore.GetLatestBlock(tx);
                    if (tip?.BlockHash is not { Length: 32 })
                    {
                        reason = "Local tip unknown";
                        return false;
                    }

                    if (!BytesEqual32(header.PreviousBlockHash, tip.BlockHash))
                    {
                        reason = "Block must extend current canonical tip";
                        return false;
                    }
                }

                if (requirePrevKnown)
                {
                    if (enforceTipExtending &&
                        canonicalTipOverride?.BlockHash is { Length: 32 } &&
                        canonicalTipOverride.BlockHeight + 1UL == block.BlockHeight)
                    {
                        prevBlock = canonicalTipOverride;
                    }
                    else if (getCanonicalBlockByHeightOverride != null && block.BlockHeight > 0)
                    {
                        prevBlock = getCanonicalBlockByHeightOverride(block.BlockHeight - 1UL);
                    }
                    else
                    {
                        prevBlock = BlockStore.GetBlockByHash(header.PreviousBlockHash, tx);
                    }

                    if (prevBlock == null)
                    {
                        reason = "Previous block missing";
                        return false;
                    }

                    if (block.BlockHeight != prevBlock.BlockHeight + 1)
                    {
                        reason = "Height mismatch";
                        return false;
                    }

                    ulong mtp = ComputeMedianTimePast(prevBlock, MedianTimePastWindow, tx, getCanonicalBlockByHeightOverride);
                    if (header.Timestamp <= mtp)
                    {
                        reason = "Timestamp must be greater than median time past";
                        return false;
                    }
                }
            }
            else
            {
                if (!GenesisBlockProvider.ValidateGenesisBlock(block, out var gReason))
                {
                    reason = $"Invalid genesis: {gReason}";
                    return false;
                }
            }

            if (!Difficulty.IsValidTarget(header.Target))
            {
                reason = "Invalid target";
                return false;
            }

            var haveTarget = header.Target;

            if (block.BlockHeight > 0 && prevBlock != null)
            {
                var expectedTarget = ComputeExpectedTargetForThisBlock(block, prevBlock, tx, getCanonicalBlockByHeightOverride);
                if (expectedTarget is null || expectedTarget.Length != 32) { reason = "Expected target computation failed"; return false; }
                if (!BytesEqual32(haveTarget, expectedTarget))
                {
                    reason = "Target mismatch";
                    return false;
                }
            }
            else if (block.BlockHeight > 0 && requirePrevKnown && prevBlock == null)
            {
                reason = "Prev block required to validate target";
                return false;
            }

            var headerHash = block.ComputeBlockHash();
            if (headerHash is null || headerHash.Length != 32) { reason = "PoW hash invalid"; return false; }
            if (!Difficulty.Meets(headerHash, haveTarget))
            {
                reason = "PoW not meeting target";
                return false;
            }

            if (block.BlockHash is { Length: 32 } && !BytesEqual32(block.BlockHash, headerHash))
            {
                reason = "Stored BlockHash mismatch";
                return false;
            }

            var seen = new HashSet<byte[]>(ByteArray32Comparer.Instance);
            int sigChecks = 0;

            var cbid = coinbase.ComputeTransactionHash();
            if (cbid is null || cbid.Length != 32) { reason = "Coinbase txid invalid"; return false; }
            seen.Add(cbid);

            for (int i = 1; i < txs.Count; i++)
            {
                var transaction = txs[i];
                if (transaction is null) { reason = $"TX[{i}] is null"; return false; }

                if (TransactionValidator.IsCoinbase(transaction))
                {
                    reason = $"TX[{i}] must not be coinbase";
                    return false;
                }

                if (transaction.Sender is null || transaction.Sender.Length != 32 || transaction.Recipient is null || transaction.Recipient.Length != 32)
                {
                    reason = $"TX[{i}] invalid endpoints";
                    return false;
                }

                if (transaction.Amount == 0) { reason = $"TX[{i}] amount must be > 0"; return false; }
                if (transaction.ChainId != ChainId) { reason = $"TX[{i}] invalid ChainId"; return false; }

                var txid = transaction.ComputeTransactionHash();
                if (txid is null || txid.Length != 32) { reason = $"TX[{i}] txid invalid"; return false; }
                if (!seen.Add(txid)) { reason = $"TX[{i}] duplicate txid"; return false; }

                if (strictTxChecks)
                {
                    if (!TransactionValidator.ValidateBasic(transaction, out var txReason))
                    {
                        reason = $"TX[{i}] invalid: {txReason}";
                        return false;
                    }

                    sigChecks++;
                    if (sigChecks > MaxSignatureChecksPerBlock)
                    {
                        reason = "Too many signature checks";
                        return false;
                    }
                }
            }

            ulong totalFees = 0;
            for (int i = 1; i < txs.Count; i++)
            {
                ulong f = txs[i].Fee;
                if (ulong.MaxValue - totalFees < f) { reason = "Fee sum overflow"; return false; }
                totalFees += f;
            }

            bool skipCoinbaseAmountCheck =
                skipHeightSensitiveChecksWithoutParent &&
                !isGenesisCandidate &&
                prevBlock == null;
            if (!skipCoinbaseAmountCheck)
            {
                ulong expectedSubsidy = RewardCalculator.GetBlockSubsidy(block.BlockHeight);
                if (ulong.MaxValue - expectedSubsidy < totalFees) { reason = "Coinbase sum overflow"; return false; }

                ulong expectedCoinbase = expectedSubsidy + totalFees;
                if (coinbase.Amount != expectedCoinbase)
                {
                    reason = "Coinbase amount mismatch";
                    return false;
                }
            }

            if (validateStateAgainstCanonical)
            {
                if (!enforceTipExtending)
                {
                    reason = "State check only valid for tip-extending blocks";
                    return false;
                }

                if (!ValidateAgainstCanonicalState(block, tx, out reason))
                    return false;
            }

            return true;
        }

        private static bool ValidateAgainstCanonicalState(Block block, SqliteTransaction? tx, out string reason)
        {
            reason = "OK";

            var bal = new Dictionary<string, ulong>(StringComparer.Ordinal);
            var nonce = new Dictionary<string, ulong>(StringComparer.Ordinal);

            ulong GetBal(string a)
            {
                if (bal.TryGetValue(a, out var v)) return v;
                var v0 = StateStore.GetBalance(a, tx);
                bal[a] = v0;
                return v0;
            }

            ulong GetNonce(string a)
            {
                if (nonce.TryGetValue(a, out var v)) return v;
                var v0 = StateStore.GetNonce(a, tx);
                nonce[a] = v0;
                return v0;
            }

            void SetBal(string a, ulong v) => bal[a] = v;
            void SetNonce(string a, ulong v) => nonce[a] = v;

            var txs = block.Transactions;

            {
                var cb = txs[0];
                string r = Hex32Lower(cb.Recipient);
                var rb = GetBal(r);
                if (ulong.MaxValue - rb < cb.Amount) { reason = "State overflow (coinbase)"; return false; }
                SetBal(r, rb + cb.Amount);
            }

            for (int i = 1; i < txs.Count; i++)
            {
                var transaction = txs[i];

                string s = Hex32Lower(transaction.Sender);
                string r = Hex32Lower(transaction.Recipient);

                var sb = GetBal(s);
                var sn = GetNonce(s);

                if (!NonceRules.TryGetExpectedNextNonce(sn, out var expectedNonce))
                {
                    reason = $"TX[{i}] sender nonce exhausted";
                    return false;
                }

                if (transaction.TxNonce != expectedNonce)
                {
                    reason = $"TX[{i}] nonce mismatch (have {transaction.TxNonce}, expected {expectedNonce})";
                    return false;
                }

                if (ulong.MaxValue - transaction.Amount < transaction.Fee)
                {
                    reason = $"TX[{i}] cost overflow";
                    return false;
                }

                ulong cost = transaction.Amount + transaction.Fee;

                if (sb < cost)
                {
                    reason = $"TX[{i}] insufficient funds";
                    return false;
                }

                SetBal(s, sb - cost);
                SetNonce(s, transaction.TxNonce);

                var rb = GetBal(r);
                if (ulong.MaxValue - rb < transaction.Amount)
                {
                    reason = $"TX[{i}] recipient balance overflow";
                    return false;
                }

                SetBal(r, rb + transaction.Amount);
            }

            return true;
        }

        private static byte[] ComputeExpectedTargetForThisBlock(
            Block block,
            Block prevBlock,
            SqliteTransaction? tx,
            Func<ulong, Block?>? getCanonicalBlockByHeightOverride = null)
        {
            ulong nextHeight = block.BlockHeight;

            if (getCanonicalBlockByHeightOverride != null)
            {
                Block? CachedGetter(ulong h)
                {
                    if (h == prevBlock.BlockHeight)
                        return prevBlock;
                    return getCanonicalBlockByHeightOverride(h);
                }

                return DifficultyCalculator.GetNextTarget(nextHeight, CachedGetter);
            }

            var map = BuildAncestorMap(prevBlock, maxBlocks: 256, tx);
            Block? Getter(ulong h) => map.TryGetValue(h, out var b) ? b : null;

            return DifficultyCalculator.GetNextTarget(nextHeight, Getter);
        }

        private static Dictionary<ulong, Block> BuildAncestorMap(Block tipPrev, int maxBlocks, SqliteTransaction? tx)
        {
            var map = new Dictionary<ulong, Block>(capacity: Math.Max(16, maxBlocks));
            Block? cur = tipPrev;

            for (int i = 0; i < maxBlocks && cur != null; i++)
            {
                map[cur.BlockHeight] = cur;

                if (cur.BlockHeight == 0) break;

                var ph = cur.Header?.PreviousBlockHash;
                if (ph is not { Length: 32 }) break;

                cur = BlockStore.GetBlockByHash(ph, tx);
            }

            return map;
        }

        private static bool IsAllZero32(byte[] a)
        {
            if (a is not { Length: 32 }) return false;
            for (int i = 0; i < 32; i++)
                if (a[i] != 0) return false;
            return true;
        }

        private static bool BytesEqual32(byte[] a, byte[] b)
        {
            if (a is null || b is null) return false;
            if (a.Length != 32 || b.Length != 32) return false;

            int diff = 0;
            for (int i = 0; i < 32; i++) diff |= a[i] ^ b[i];
            return diff == 0;
        }

        private static string Hex32Lower(byte[] b32)
            => Convert.ToHexString(b32).ToLowerInvariant();

        private static ulong ComputeMedianTimePast(
            Block prevBlock,
            int window,
            SqliteTransaction? tx,
            Func<ulong, Block?>? getCanonicalBlockByHeightOverride = null)
        {
            var values = new List<ulong>(Math.Max(1, window));

            if (getCanonicalBlockByHeightOverride != null)
            {
                ulong h = prevBlock.BlockHeight;
                for (int i = 0; i < window; i++)
                {
                    Block? cur = h == prevBlock.BlockHeight
                        ? prevBlock
                        : getCanonicalBlockByHeightOverride(h);
                    if (cur?.Header is null)
                        break;

                    values.Add(cur.Header.Timestamp);
                    if (h == 0)
                        break;

                    h--;
                }
            }
            else
            {
                Block? cur = prevBlock;
                for (int i = 0; i < window && cur?.Header is not null; i++)
                {
                    values.Add(cur.Header.Timestamp);

                    if (cur.BlockHeight == 0)
                        break;

                    var ph = cur.Header.PreviousBlockHash;
                    if (ph is not { Length: 32 })
                        break;

                    cur = BlockStore.GetBlockByHash(ph, tx);
                }
            }

            if (values.Count == 0)
                return prevBlock.Header.Timestamp;

            values.Sort();
            return values[values.Count / 2];
        }

        private sealed class ByteArray32Comparer : IEqualityComparer<byte[]>
        {
            public static readonly ByteArray32Comparer Instance = new();

            public bool Equals(byte[]? x, byte[]? y)
            {
                if (ReferenceEquals(x, y)) return true;
                if (x is null || y is null) return false;
                return BytesEqual32(x, y);
            }

            public int GetHashCode(byte[] obj)
            {
                if (obj is null || obj.Length != 32) return 0;

                unchecked
                {
                    int h = 17;
                    for (int i = 0; i < 32; i += 4)
                        h = (h * 31) + (obj[i] | (obj[i + 1] << 8) | (obj[i + 2] << 16) | (obj[i + 3] << 24));
                    return h;
                }
            }
        }
    }
}

