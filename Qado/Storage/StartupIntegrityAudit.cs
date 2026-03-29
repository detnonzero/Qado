using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Data.Sqlite;
using Qado.Blockchain;
using Qado.Serialization;

namespace Qado.Storage
{
    public enum StartupAuditDisposition
    {
        Continue,
        HardStopCanonCorruption,
        RepairableDerivedState
    }

    public enum StartupAuditIssueCode
    {
        CryptoRuntimeSelfTestFailed,
        MissingGenesisCanon,
        GenesisMismatch,
        CanonGap,
        MissingCanonicalIndex,
        CanonHeightMismatch,
        MissingCanonicalPayload,
        CorruptCanonicalPayload,
        CanonHashMismatch,
        CanonPrevMismatch,
        MissingCanonicalUndo
    }

    public sealed record StartupAuditIssue(
        StartupAuditIssueCode Code,
        string Message,
        ulong? Height = null,
        string? HashHex = null);

    public sealed record StartupAuditResult(
        StartupAuditDisposition Disposition,
        ulong TipHeight,
        string? TipHashHex,
        IReadOnlyList<StartupAuditIssue> Issues);

    public static class StartupIntegrityAudit
    {
        private const string KnownMainnetSignatureProbeTxHex =
            "0200000001f721b74f3946370d78baabd330b5619638464af1d3ee97fd7d3fea216e37ff2e" +
            "12a1be846a42cd13337b7564d2824fdf245a5f1c8f171a1269b98c3131e94b790000000000" +
            "000000000000003b9aca0000000000000000000000000005f5e10000000000000000010040" +
            "331be203329581e2fc7cc4b67a26c7be4153cd2a18eebfa7dd4c443930cd7f6977558864370" +
            "b362253700422982c06040753cbea042f261d38401ccc9914a808";

        public static StartupAuditResult Run()
        {
            if (Db.Connection == null)
                throw new InvalidOperationException("Db.Initialize must run before startup integrity audit.");

            var issues = new List<StartupAuditIssue>();
            ulong tipHeight = 0;
            string? tipHashHex = null;
            bool repairableDerivedState = false;

            if (!TryValidateKnownMainnetSignatureProbe(out var cryptoReason))
            {
                return CreateBlockingResult(
                    issues,
                    tipHeight,
                    tipHashHex,
                    StartupAuditIssueCode.CryptoRuntimeSelfTestFailed,
                    cryptoReason);
            }

            lock (Db.Sync)
            {
                using var tx = Db.Connection.BeginTransaction();

                if (!BlockStore.TryGetLatestHeight(out tipHeight, tx))
                    return CreateBlockingResult(
                        issues,
                        0UL,
                        null,
                        StartupAuditIssueCode.MissingGenesisCanon,
                        "canonical genesis entry is missing");

                byte[] expectedGenesisHash = GenesisBlockProvider.GetGenesisBlock().BlockHash
                    ?? throw new InvalidOperationException("expected genesis hash is unavailable");

                byte[]? previousCanonicalHash = null;

                for (ulong height = 0; height <= tipHeight; height++)
                {
                    byte[]? canonicalHash = GetCanonicalHashAtHeight(height, tx);
                    if (canonicalHash is not { Length: 32 })
                    {
                        return CreateBlockingResult(
                            issues,
                            tipHeight,
                            tipHashHex,
                            height == 0 ? StartupAuditIssueCode.MissingGenesisCanon : StartupAuditIssueCode.CanonGap,
                            height == 0 ? "canonical genesis entry is missing" : $"canonical chain has a gap at height {height}",
                            height);
                    }

                    if (height == tipHeight)
                        tipHashHex = Hex(canonicalHash);

                    if (height == 0 && !BytesEqual32(canonicalHash, expectedGenesisHash))
                    {
                        return CreateBlockingResult(
                            issues,
                            tipHeight,
                            tipHashHex,
                            StartupAuditIssueCode.GenesisMismatch,
                            $"canonical genesis hash mismatch: found {Hex(canonicalHash)}, expected {Hex(expectedGenesisHash)}",
                            height,
                            Hex(canonicalHash));
                    }

                    if (!TryGetCanonicalIndexMeta(canonicalHash, tx, out var indexedHeight, out var indexedPrevHash))
                    {
                        return CreateBlockingResult(
                            issues,
                            tipHeight,
                            tipHashHex,
                            StartupAuditIssueCode.MissingCanonicalIndex,
                            $"block_index entry missing for canonical block at height {height}",
                            height,
                            Hex(canonicalHash));
                    }

                    if (indexedHeight != height)
                    {
                        return CreateBlockingResult(
                            issues,
                            tipHeight,
                            tipHashHex,
                            StartupAuditIssueCode.CanonHeightMismatch,
                            $"block_index height mismatch for canonical block at height {height}: indexed={indexedHeight}",
                            height,
                            Hex(canonicalHash));
                    }

                    if (!TryGetPayload(canonicalHash, tx, out var payload))
                    {
                        return CreateBlockingResult(
                            issues,
                            tipHeight,
                            tipHashHex,
                            StartupAuditIssueCode.MissingCanonicalPayload,
                            $"payload missing for canonical block at height {height}",
                            height,
                            Hex(canonicalHash));
                    }

                    Block block;
                    try
                    {
                        block = BlockBinarySerializer.Read(payload);
                    }
                    catch (Exception ex)
                    {
                        return CreateBlockingResult(
                            issues,
                            tipHeight,
                            tipHashHex,
                            StartupAuditIssueCode.CorruptCanonicalPayload,
                            $"payload decode failed for canonical block at height {height}: {ex.Message}",
                            height,
                            Hex(canonicalHash));
                    }

                    if (block.Header is null)
                    {
                        return CreateBlockingResult(
                            issues,
                            tipHeight,
                            tipHashHex,
                            StartupAuditIssueCode.CorruptCanonicalPayload,
                            $"canonical block payload at height {height} is missing a header",
                            height,
                            Hex(canonicalHash));
                    }

                    byte[] computedHash;
                    try
                    {
                        computedHash = block.ComputeBlockHash();
                    }
                    catch (Exception ex)
                    {
                        return CreateBlockingResult(
                            issues,
                            tipHeight,
                            tipHashHex,
                            StartupAuditIssueCode.CorruptCanonicalPayload,
                            $"block hash recomputation failed for canonical block at height {height}: {ex.Message}",
                            height,
                            Hex(canonicalHash));
                    }

                    if (!BytesEqual32(computedHash, canonicalHash))
                    {
                        return CreateBlockingResult(
                            issues,
                            tipHeight,
                            tipHashHex,
                            StartupAuditIssueCode.CanonHashMismatch,
                            $"canonical payload hash mismatch at height {height}: computed={Hex(computedHash)}, canon={Hex(canonicalHash)}",
                            height,
                            Hex(canonicalHash));
                    }

                    if (height > 0)
                    {
                        if (block.Header.PreviousBlockHash is not { Length: 32 } || previousCanonicalHash is not { Length: 32 })
                        {
                            return CreateBlockingResult(
                                issues,
                                tipHeight,
                                tipHashHex,
                                StartupAuditIssueCode.CanonPrevMismatch,
                                $"canonical block at height {height} is missing a valid previous hash",
                                height,
                                Hex(canonicalHash));
                        }

                        if (!BytesEqual32(block.Header.PreviousBlockHash, previousCanonicalHash))
                        {
                            return CreateBlockingResult(
                                issues,
                                tipHeight,
                                tipHashHex,
                                StartupAuditIssueCode.CanonPrevMismatch,
                                $"canonical previous-hash mismatch at height {height}: expected {Hex(previousCanonicalHash)}, got {Hex(block.Header.PreviousBlockHash)}",
                                height,
                                Hex(canonicalHash));
                        }

                        if (!BytesEqual32(indexedPrevHash, previousCanonicalHash))
                        {
                            return CreateBlockingResult(
                                issues,
                                tipHeight,
                                tipHashHex,
                                StartupAuditIssueCode.CanonPrevMismatch,
                                $"block_index previous-hash mismatch at height {height}: expected {Hex(previousCanonicalHash)}, indexed {Hex(indexedPrevHash)}",
                                height,
                                Hex(canonicalHash));
                        }

                        if (!HasStateUndo(canonicalHash, tx) && !repairableDerivedState)
                        {
                            issues.Add(new StartupAuditIssue(
                                StartupAuditIssueCode.MissingCanonicalUndo,
                                $"state_undo missing for canonical block at height {height}",
                                height,
                                Hex(canonicalHash)));
                            repairableDerivedState = true;
                        }
                    }

                    previousCanonicalHash = (byte[])canonicalHash.Clone();
                }
            }

            return new StartupAuditResult(
                repairableDerivedState ? StartupAuditDisposition.RepairableDerivedState : StartupAuditDisposition.Continue,
                tipHeight,
                tipHashHex,
                issues);
        }

        public static StartupAuditResult RepairDerivedStateIfNeeded(StartupAuditResult result)
        {
            if (result == null)
                throw new ArgumentNullException(nameof(result));
            if (result.Disposition != StartupAuditDisposition.RepairableDerivedState)
                return result;
            if (string.IsNullOrWhiteSpace(result.TipHashHex))
                throw new InvalidOperationException("repairable startup audit result is missing a canonical tip hash");

            StateRebuilder.RebuildToTip(Convert.FromHexString(result.TipHashHex));
            var repaired = Run();
            if (repaired.Disposition == StartupAuditDisposition.RepairableDerivedState)
            {
                throw new InvalidOperationException(
                    "Derived-state rebuild completed but the startup integrity audit still reports repairable drift.");
            }

            return repaired;
        }

        public static void ThrowIfBlocking(StartupAuditResult result)
        {
            if (result == null)
                throw new ArgumentNullException(nameof(result));
            if (result.Disposition == StartupAuditDisposition.Continue)
                return;

            var message = new StringBuilder();
            message.Append("Startup integrity audit failed.");

            if (result.Issues.Count > 0)
            {
                message.Append(' ');
                message.Append(result.Issues[0].Message);
            }

            switch (result.Disposition)
            {
                case StartupAuditDisposition.HardStopCanonCorruption:
                    message.AppendLine();
                    if (result.Issues.Count > 0 && result.Issues[0].Code == StartupAuditIssueCode.CryptoRuntimeSelfTestFailed)
                    {
                        message.Append(
                            "Redeploy the full publish folder, including the runtimes directory, and avoid mixing files from older builds. " +
                            "Verify that runtimes\\win-x64\\native\\libsodium.dll is present next to the deployed app.");
                    }
                    else
                    {
                        message.Append("QADO did not modify your database. Back up the data directory and perform a chain resync before restarting.");
                    }
                    break;
                case StartupAuditDisposition.RepairableDerivedState:
                    message.AppendLine();
                    message.Append("QADO detected repairable derived-state drift. Run the repair flow before restarting.");
                    break;
            }

            throw new InvalidOperationException(message.ToString());
        }

        private static StartupAuditResult CreateBlockingResult(
            List<StartupAuditIssue> issues,
            ulong tipHeight,
            string? tipHashHex,
            StartupAuditIssueCode code,
            string message,
            ulong? height = null,
            string? hashHex = null)
        {
            issues.Add(new StartupAuditIssue(code, message, height, hashHex));
            return new StartupAuditResult(StartupAuditDisposition.HardStopCanonCorruption, tipHeight, tipHashHex, issues);
        }

        private static byte[]? GetCanonicalHashAtHeight(ulong height, SqliteTransaction tx)
        {
            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "SELECT hash FROM canon WHERE height=$h LIMIT 1;";
            cmd.Parameters.AddWithValue("$h", (long)height);
            var value = cmd.ExecuteScalar() as byte[];
            return value is { Length: 32 } ? (byte[])value.Clone() : null;
        }

        private static bool TryGetCanonicalIndexMeta(byte[] hash, SqliteTransaction tx, out ulong height, out byte[] prevHash)
        {
            height = 0;
            prevHash = Array.Empty<byte>();

            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "SELECT height, prev_hash FROM block_index WHERE hash=$h LIMIT 1;";
            cmd.Parameters.AddWithValue("$h", hash);

            using var reader = cmd.ExecuteReader();
            if (!reader.Read())
                return false;

            if (reader.IsDBNull(0))
                return false;

            long rawHeight = reader.GetInt64(0);
            if (rawHeight < 0)
                return false;

            if (reader[1] is not byte[] { Length: 32 } prev)
                return false;

            height = (ulong)rawHeight;
            prevHash = (byte[])prev.Clone();
            return true;
        }

        private static bool TryGetPayload(byte[] hash, SqliteTransaction tx, out byte[] payload)
        {
            payload = Array.Empty<byte>();

            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "SELECT payload FROM block_payloads WHERE hash=$h LIMIT 1;";
            cmd.Parameters.AddWithValue("$h", hash);

            if (cmd.ExecuteScalar() is not byte[] { Length: > 0 } value)
                return false;

            payload = (byte[])value.Clone();
            return true;
        }

        private static bool HasStateUndo(byte[] hash, SqliteTransaction tx)
        {
            using var cmd = tx.Connection!.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "SELECT 1 FROM state_undo WHERE block_hash=$h LIMIT 1;";
            cmd.Parameters.AddWithValue("$h", hash);
            var value = cmd.ExecuteScalar();
            return value != null && value is not DBNull;
        }

        private static bool BytesEqual32(byte[] a, byte[] b)
        {
            if (a is not { Length: 32 } || b is not { Length: 32 })
                return false;

            int diff = 0;
            for (int i = 0; i < 32; i++)
                diff |= a[i] ^ b[i];
            return diff == 0;
        }

        private static string Hex(byte[] bytes)
            => Convert.ToHexString(bytes).ToLowerInvariant();

        private static bool TryValidateKnownMainnetSignatureProbe(out string reason)
        {
            try
            {
                var tx = TxBinarySerializer.Read(Convert.FromHexString(KnownMainnetSignatureProbeTxHex));
                if (TransactionValidator.ValidateBasic(tx, out var validationReason))
                {
                    reason = "OK";
                    return true;
                }

                reason =
                    "cryptographic runtime self-test failed: a known mainnet transaction signature did not validate " +
                    $"({validationReason}). This usually means the deployment is incomplete or mixes files from different builds.";
                return false;
            }
            catch (Exception ex)
            {
                reason =
                    "cryptographic runtime self-test failed before sync startup: " +
                    $"{ex.GetType().Name}: {ex.Message}. Redeploy the full publish folder, including runtimes.";
                return false;
            }
        }
    }
}
