using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using NSec.Cryptography;
using Qado.Api;
using Qado.Blockchain;
using Qado.Logging;
using Qado.Mempool;
using Qado.Networking;
using Qado.Serialization;
using Qado.Storage;
using Qado.Utils;

internal static class Program
{
    private const byte MerkleLeafTag = 0x00;
    private const byte MerkleNodeTag = 0x01;
    private static readonly byte[] SelfTestEasyTarget = Convert.FromHexString("7" + new string('F', 63));

    private sealed class ListLogSink : ILogSink
    {
        public List<string> Lines { get; } = new();

        public void Info(string category, string message) => Lines.Add($"INFO:{category}:{message}");
        public void Warn(string category, string message) => Lines.Add($"WARN:{category}:{message}");
        public void Error(string category, string message) => Lines.Add($"ERR:{category}:{message}");
    }

    private sealed record TestCase(string Name, Action Body);

    private static string _tempRoot = string.Empty;
    private static int _passed;
    private static int _failed;
    private static readonly IReadOnlyList<TestCase> Tests = new[]
    {
        new TestCase("Genesis_Stored", TestGenesisStored),
        new TestCase("StartupIntegrityAudit_ValidCanonicalChain_Passes", TestStartupIntegrityAuditValidCanonicalChainPasses),
        new TestCase("StartupIntegrityAudit_CanonGap_BlocksStartup", TestStartupIntegrityAuditCanonGapBlocksStartup),
        new TestCase("StartupIntegrityAudit_MissingCanonicalPayload_BlocksStartup", TestStartupIntegrityAuditMissingCanonicalPayloadBlocksStartup),
        new TestCase("StartupIntegrityAudit_CanonPrevMismatch_BlocksStartup", TestStartupIntegrityAuditCanonPrevMismatchBlocksStartup),
        new TestCase("StartupIntegrityAudit_MissingCanonicalUndo_RebuildsDerivedState", TestStartupIntegrityAuditMissingCanonicalUndoRebuildsDerivedState),
        new TestCase("StartupRecovery_BackupAndChainResync_PreservesKeysAndResetsChain", TestStartupRecoveryBackupAndChainResyncPreservesKeysAndResetsChain),
        new TestCase("BlockPayloads_StoredInSqlite", TestBlockPayloadsStoredInSqlite),
        new TestCase("StateApplier_RejectsNonceGap", TestStateApplierRejectsNonceGap),
        new TestCase("StateApplier_AcceptsSequentialNonce", TestStateApplierAcceptsSequentialNonce),
        new TestCase("StateApplier_SelfTransfer_MinedBySender_IsNotInflationary", TestStateApplierSelfTransferMinedBySenderIsNotInflationary),
        new TestCase("StateUndoStore_RollbackRestoresNonce", TestStateUndoStoreRollbackRestoresNonce),
        new TestCase("StateStore_RoundTripsFullUlongNonce", TestStateStoreRoundTripsFullUlongNonce),
        new TestCase("KeyStore_EncryptsAtRest", TestKeyStoreEncryptsAtRest),
        new TestCase("Merkle_DomainSeparation_Profile", TestMerkleDomainSeparationProfile),
        new TestCase("TxId_CommitsSignature", TestTxIdCommitsSignature),
        new TestCase("ExchangeApi_TxLookup_BlockRefDisambiguatesDeterministicCoinbase", TestExchangeApiTxLookupBlockRefDisambiguatesDeterministicCoinbase),
        new TestCase("ExchangeApi_MiningJobs_SameMinerCanHoldMultipleOutstandingJobs", TestExchangeApiMiningJobsSameMinerCanHoldMultipleOutstandingJobs),
        new TestCase("BlockSerializer_RejectsInvalidTarget", TestBlockSerializerRejectsInvalidTarget),
        new TestCase("BlockValidator_TipValidation_WorksInsideTransaction", TestBlockValidatorTipValidationWorksInsideTransaction),
        new TestCase("BlockValidator_AcceptsLegacyLongMaxNonceBoundary", TestBlockValidatorAcceptsLegacyLongMaxNonceBoundary),
        new TestCase("BlockValidator_RejectsExhaustedSenderNonce", TestBlockValidatorRejectsExhaustedSenderNonce),
        new TestCase("ChainSelector_Reorg_MempoolReconcile", TestChainSelectorReorgMempoolReconcile),
        new TestCase("MempoolSelection_RespectsSenderNonceOrder", TestMempoolSelectionRespectsSenderNonceOrder),
        new TestCase("PendingTransactionBuffer_RejectsExhaustedSenderNonce", TestPendingTransactionBufferRejectsExhaustedSenderNonce),
        new TestCase("BlockLocator_FindsForkpoint", TestBlockLocatorFindsForkpoint),
        new TestCase("BlockSyncProtocol_Chunks4096Blocks", TestBlockSyncProtocolChunks4096Blocks),
        new TestCase("BlockSyncServer_UsesSmallNetBatchFrames", TestBlockSyncServerUsesSmallNetBatchFrames),
        new TestCase("SmallNetSyncProtocol_RoundTripsCoreFrames", TestSmallNetSyncProtocolRoundTripsCoreFrames),
        new TestCase("SmallNetPeerState_ClassifiesGapPragmatically", TestSmallNetPeerStateClassifiesGapPragmatically),
        new TestCase("SmallNetPeerFlow_PushesCanonicalTailToLaggingPeer", TestSmallNetPeerFlowPushesCanonicalTailToLaggingPeer),
        new TestCase("SmallNetPeerFlow_ReconnectHandshakePushesTail", TestSmallNetPeerFlowReconnectHandshakePushesTail),
        new TestCase("SmallNetPeerFlow_CompetingTipRequestsAncestorPack", TestSmallNetPeerFlowCompetingTipRequestsAncestorPack),
        new TestCase("SmallNetPeerFlow_ActiveBlockSyncSuppressesCoordinatorCatchUpRequests", TestSmallNetPeerFlowActiveBlockSyncSuppressesCoordinatorCatchUpRequests),
        new TestCase("SmallNetCoordinator_ProducesAggressiveRecoveryActions", TestSmallNetCoordinatorProducesAggressiveRecoveryActions),
        new TestCase("BulkSyncRuntime_CommitsChunkIntoCanonicalChain", TestBulkSyncRuntimeCommitsChunkIntoCanonicalChain),
        new TestCase("BulkSyncRuntime_MoreAvailable_KeepsGateAndContinuesBatch", TestBulkSyncRuntimeMoreAvailableKeepsGateAndContinuesBatch),
        new TestCase("BulkSyncRuntime_AbortAfterReorgChunk_RestoresPriorCanonicalChain", TestBulkSyncRuntimeAbortAfterReorgChunkRestoresPriorCanonicalChain),
        new TestCase("BlockIngressFlow_LiveOrphanRequestsParentAndPromotesAcceptedParent", TestBlockIngressFlowLiveOrphanRequestsParentAndPromotesAcceptedParent),
        new TestCase("BlockIngressFlow_AlreadyBufferedOrphanDoesNotReRequestParent", TestBlockIngressFlowAlreadyBufferedOrphanDoesNotReRequestParent),
        new TestCase("ValidationWorker_PrioritizesLivePush", TestValidationWorkerPrioritizesLivePush),
        new TestCase("BlockDownloadManager_RequestsRecoveryViaAncestorPack", TestBlockDownloadManagerRequestsRecoveryViaAncestorPack),
        new TestCase("BlockSyncClient_TipStateStartsSyncWithoutLegacyGetTip", TestBlockSyncClientTipStateStartsSyncWithoutLegacyGetTip),
        new TestCase("BlockSyncClient_PrefersHigherChainworkOverHeight", TestBlockSyncClientPrefersHigherChainworkOverHeight),
        new TestCase("BlockSyncClient_MoreAvailable_ContinuesFromCurrentLocalTip", TestBlockSyncClientMoreAvailableContinuesFromCurrentLocalTip),
        new TestCase("BlockSyncClient_Disconnect_ResumesFromLastCommitted", TestBlockSyncClientDisconnectResumesFromLastCommitted),
        new TestCase("BlockSyncClient_InvalidBlock_PenalizesAndFallsBack", TestBlockSyncClientInvalidBlockPenalizesAndFallsBack)
    };

    public static int Main(string[] args)
    {
        if (args != null &&
            args.Length == 1 &&
            string.Equals(args[0], "--list", StringComparison.OrdinalIgnoreCase))
        {
            for (int i = 0; i < Tests.Count; i++)
                Console.WriteLine(Tests[i].Name);
            return 0;
        }

        var selectedTests = SelectTests(args ?? Array.Empty<string>());
        if (selectedTests.Count == 0)
        {
            Console.WriteLine("SelfTest selection matched no tests.");
            return 1;
        }

        for (int i = 0; i < selectedTests.Count; i++)
            Run(selectedTests[i]);

        Console.WriteLine($"SelfTest complete: passed={_passed} failed={_failed}");
        return _failed == 0 ? 0 : 1;
    }

    private static void SetupRuntime(string tempRoot)
    {
        _tempRoot = tempRoot;
        Directory.CreateDirectory(tempRoot);
        string dbPath = Path.Combine(tempRoot, "qado.db");

        ChainSelector.ResetForTests();
        Difficulty.SetPowLimitOverrideForTests(SelfTestEasyTarget);
        DifficultyCalculator.SetFixedTargetOverrideForTests(SelfTestEasyTarget);
        Db.Initialize(dbPath);
        GenesisBlockProvider.EnsureGenesisBlockStored();
    }

    private static void Run(TestCase testCase)
    {
        string tempRoot = Path.Combine(Path.GetTempPath(), "qado-selftest-" + Guid.NewGuid().ToString("N"));

        try
        {
            TraceTestStep($"Run:{testCase.Name}:before-setup");
            SetupRuntime(tempRoot);
            TraceTestStep($"Run:{testCase.Name}:after-setup");
            TraceTestStep($"Run:{testCase.Name}:before-body");
            testCase.Body();
            TraceTestStep($"Run:{testCase.Name}:after-body");
            _passed++;
            Console.WriteLine($"[PASS] {testCase.Name}");
        }
        catch (Exception ex)
        {
            _failed++;
            Console.WriteLine($"[FAIL] {testCase.Name}: {ex.Message}");
        }
        finally
        {
            try { Db.Shutdown(); } catch { }
            try { DifficultyCalculator.SetFixedTargetOverrideForTests(null); } catch { }
            try { Difficulty.SetPowLimitOverrideForTests(null); } catch { }
            try { if (Directory.Exists(tempRoot)) Directory.Delete(tempRoot, true); } catch { }
        }
    }

    private static List<TestCase> SelectTests(string[] args)
    {
        if (args == null || args.Length == 0)
            return new List<TestCase>(Tests);

        var selected = new List<TestCase>();
        for (int i = 0; i < Tests.Count; i++)
        {
            var test = Tests[i];
            for (int j = 0; j < args.Length; j++)
            {
                string filter = args[j] ?? string.Empty;
                if (test.Name.IndexOf(filter, StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    selected.Add(test);
                    break;
                }
            }
        }

        return selected;
    }

    private static void TestGenesisStored()
    {
        var h0 = BlockStore.GetCanonicalHashAtHeight(0);
        Assert(h0 is { Length: 32 }, "canonical height 0 hash missing");

        var b0 = BlockStore.GetBlockByHeight(0);
        Assert(b0 != null, "genesis block not readable by height");
        Assert(b0!.BlockHeight == 0, "genesis height must be 0");
        Assert(b0.Transactions.Count >= 1, "genesis must contain coinbase");
        Assert(TransactionValidator.IsCoinbase(b0.Transactions[0]), "genesis tx[0] must be coinbase");
    }

    private static void TestStartupIntegrityAuditValidCanonicalChainPasses()
    {
        var genesis = BlockStore.GetBlockByHeight(0) ?? throw new InvalidOperationException("missing genesis block");
        var genesisHash = genesis.BlockHash ?? throw new InvalidOperationException("missing genesis hash");
        var miner = KeyGenerator.GenerateKeypairHex();

        var block1 = BuildMinedBlock(
            height: 1UL,
            prevHash: genesisHash,
            timestamp: genesis.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);
        BlockPersistHelper.Persist(block1);

        var block2 = BuildMinedBlock(
            height: 2UL,
            prevHash: block1.BlockHash!,
            timestamp: block1.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);
        BlockPersistHelper.Persist(block2);

        var audit = StartupIntegrityAudit.Run();
        Assert(audit.Disposition == StartupAuditDisposition.Continue, $"valid canonical chain should pass startup audit, got {audit.Disposition}");
        Assert(audit.TipHeight == 2UL, $"startup audit tip height mismatch: got {audit.TipHeight}");
        Assert(audit.TipHashHex == Convert.ToHexString(block2.BlockHash!).ToLowerInvariant(), "startup audit tip hash mismatch");
        Assert(audit.Issues.Count == 0, "valid canonical chain must not produce startup audit findings");
    }

    private static void TestStartupIntegrityAuditCanonGapBlocksStartup()
    {
        var genesis = BlockStore.GetBlockByHeight(0) ?? throw new InvalidOperationException("missing genesis block");
        var genesisHash = genesis.BlockHash ?? throw new InvalidOperationException("missing genesis hash");
        var miner = KeyGenerator.GenerateKeypairHex();

        var block1 = BuildMinedBlock(
            height: 1UL,
            prevHash: genesisHash,
            timestamp: genesis.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);
        BlockPersistHelper.Persist(block1);

        var block2 = BuildMinedBlock(
            height: 2UL,
            prevHash: block1.BlockHash!,
            timestamp: block1.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);
        BlockPersistHelper.Persist(block2);

        lock (Db.Sync)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = "DELETE FROM canon WHERE height=$h;";
            cmd.Parameters.AddWithValue("$h", 1L);
            cmd.ExecuteNonQuery();
        }

        var audit = StartupIntegrityAudit.Run();
        Assert(audit.Disposition == StartupAuditDisposition.HardStopCanonCorruption, "canon gap must hard-stop startup");
        Assert(audit.Issues.Count == 1, $"canon gap should produce exactly one startup finding, got {audit.Issues.Count}");
        Assert(audit.Issues[0].Code == StartupAuditIssueCode.CanonGap, $"unexpected startup audit issue code: {audit.Issues[0].Code}");
        Assert(audit.Issues[0].Height == 1UL, $"canon gap finding must point to height 1, got {audit.Issues[0].Height}");

        bool threw = false;
        try
        {
            StartupIntegrityAudit.ThrowIfBlocking(audit);
        }
        catch (InvalidOperationException ex)
        {
            threw = true;
            Assert(ex.Message.Contains("chain resync", StringComparison.OrdinalIgnoreCase),
                "hard-stop startup audit message should guide the user toward resync");
        }

        Assert(threw, "canon gap must throw when startup audit blocking result is enforced");
    }

    private static void TestStartupIntegrityAuditMissingCanonicalPayloadBlocksStartup()
    {
        var genesis = BlockStore.GetBlockByHeight(0) ?? throw new InvalidOperationException("missing genesis block");
        var genesisHash = genesis.BlockHash ?? throw new InvalidOperationException("missing genesis hash");
        var miner = KeyGenerator.GenerateKeypairHex();

        var block1 = BuildMinedBlock(
            height: 1UL,
            prevHash: genesisHash,
            timestamp: genesis.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);
        BlockPersistHelper.Persist(block1);

        lock (Db.Sync)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = "DELETE FROM block_payloads WHERE hash=$h;";
            cmd.Parameters.AddWithValue("$h", block1.BlockHash!);
            cmd.ExecuteNonQuery();
        }

        var audit = StartupIntegrityAudit.Run();
        Assert(audit.Disposition == StartupAuditDisposition.HardStopCanonCorruption, "missing canonical payload must hard-stop startup");
        Assert(audit.Issues.Count == 1, $"missing canonical payload should produce exactly one startup finding, got {audit.Issues.Count}");
        Assert(audit.Issues[0].Code == StartupAuditIssueCode.MissingCanonicalPayload,
            $"unexpected startup audit issue code: {audit.Issues[0].Code}");
        Assert(audit.Issues[0].Height == 1UL, $"missing payload finding must point to height 1, got {audit.Issues[0].Height}");
    }

    private static void TestStartupIntegrityAuditCanonPrevMismatchBlocksStartup()
    {
        var genesis = BlockStore.GetBlockByHeight(0) ?? throw new InvalidOperationException("missing genesis block");
        var genesisHash = genesis.BlockHash ?? throw new InvalidOperationException("missing genesis hash");
        var miner = KeyGenerator.GenerateKeypairHex();

        var block1 = BuildMinedBlock(
            height: 1UL,
            prevHash: genesisHash,
            timestamp: genesis.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);
        BlockPersistHelper.Persist(block1);

        var block2 = BuildMinedBlock(
            height: 2UL,
            prevHash: block1.BlockHash!,
            timestamp: block1.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);
        BlockPersistHelper.Persist(block2);

        var badCanonBlock = BuildLooseMinedBlock(
            height: 2UL,
            prevHash: genesisHash,
            timestamp: block2.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);

        lock (Db.Sync)
        {
            using var tx = Db.Connection.BeginTransaction();
            BlockStore.SaveBlock(badCanonBlock, tx, BlockIndexStore.StatusHaveBlockPayload);
            BlockStore.SetCanonicalHashAtHeight(2UL, badCanonBlock.BlockHash!, tx);
            tx.Commit();
        }

        var audit = StartupIntegrityAudit.Run();
        Assert(audit.Disposition == StartupAuditDisposition.HardStopCanonCorruption, "canonical prev mismatch must hard-stop startup");
        Assert(audit.Issues.Count == 1, $"canonical prev mismatch should produce exactly one startup finding, got {audit.Issues.Count}");
        Assert(audit.Issues[0].Code == StartupAuditIssueCode.CanonPrevMismatch,
            $"unexpected startup audit issue code: {audit.Issues[0].Code}");
        Assert(audit.Issues[0].Height == 2UL, $"canonical prev mismatch finding must point to height 2, got {audit.Issues[0].Height}");
    }

    private static void TestStartupIntegrityAuditMissingCanonicalUndoRebuildsDerivedState()
    {
        var genesis = BlockStore.GetBlockByHeight(0) ?? throw new InvalidOperationException("missing genesis block");
        var genesisHash = genesis.BlockHash ?? throw new InvalidOperationException("missing genesis hash");
        var miner1 = KeyGenerator.GenerateKeypairHex();
        var miner2 = KeyGenerator.GenerateKeypairHex();

        var block1 = BuildMinedBlock(
            height: 1UL,
            prevHash: genesisHash,
            timestamp: genesis.Header!.Timestamp + 61UL,
            minerPubHex: miner1.pubHex);
        BlockPersistHelper.Persist(block1);

        var block2 = BuildMinedBlock(
            height: 2UL,
            prevHash: block1.BlockHash!,
            timestamp: block1.Header!.Timestamp + 61UL,
            minerPubHex: miner2.pubHex);
        BlockPersistHelper.Persist(block2);

        byte[] block1TxId = block1.Transactions[0].ComputeTransactionHash();

        lock (Db.Sync)
        {
            using var tx = Db.Connection.BeginTransaction();

            using (var delUndo = tx.Connection!.CreateCommand())
            {
                delUndo.Transaction = tx;
                delUndo.CommandText = "DELETE FROM state_undo WHERE block_hash=$h;";
                delUndo.Parameters.AddWithValue("$h", block1.BlockHash!);
                delUndo.ExecuteNonQuery();
            }

            using (var delAccounts = tx.Connection!.CreateCommand())
            {
                delAccounts.Transaction = tx;
                delAccounts.CommandText = "DELETE FROM accounts;";
                delAccounts.ExecuteNonQuery();
            }

            using (var delTxIndex = tx.Connection!.CreateCommand())
            {
                delTxIndex.Transaction = tx;
                delTxIndex.CommandText = "DELETE FROM tx_index;";
                delTxIndex.ExecuteNonQuery();
            }

            tx.Commit();
        }

        var audit = StartupIntegrityAudit.Run();
        Assert(audit.Disposition == StartupAuditDisposition.RepairableDerivedState,
            $"missing canonical undo should be repairable, got {audit.Disposition}");
        Assert(audit.Issues.Count == 1, $"repairable startup audit should report one issue, got {audit.Issues.Count}");
        Assert(audit.Issues[0].Code == StartupAuditIssueCode.MissingCanonicalUndo,
            $"unexpected repairable startup audit issue code: {audit.Issues[0].Code}");
        Assert(audit.Issues[0].Height == 1UL,
            $"missing canonical undo should be reported at height 1, got {audit.Issues[0].Height}");

        var repaired = StartupIntegrityAudit.RepairDerivedStateIfNeeded(audit);
        Assert(repaired.Disposition == StartupAuditDisposition.Continue,
            $"derived-state repair should clear the startup audit, got {repaired.Disposition}");
        Assert(repaired.Issues.Count == 0, "derived-state repair should leave no startup audit findings");

        Assert(StateStore.GetBalanceU64(miner1.pubHex) == RewardCalculator.GetBlockSubsidy(1UL),
            "derived-state repair must restore the height-1 miner balance");
        Assert(StateStore.GetBalanceU64(miner2.pubHex) == RewardCalculator.GetBlockSubsidy(2UL),
            "derived-state repair must restore the height-2 miner balance");

        lock (Db.Sync)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = "SELECT COUNT(1) FROM state_undo WHERE block_hash=$h;";
            cmd.Parameters.AddWithValue("$h", block1.BlockHash!);
            long undoRows = Convert.ToInt64(cmd.ExecuteScalar() ?? 0L);
            Assert(undoRows == 1L, "derived-state repair must recreate state_undo for canonical blocks");
        }

        var indexed = TxIndexStore.Get(block1TxId) ?? throw new InvalidOperationException("derived-state repair must recreate tx_index rows");
        Assert(indexed.height == 1UL, $"recreated tx_index row should resolve to height 1, got {indexed.height}");
        Assert(BytesEqual(indexed.blockHash, block1.BlockHash!), "recreated tx_index row resolved to the wrong block");
    }

    private static void TestStartupRecoveryBackupAndChainResyncPreservesKeysAndResetsChain()
    {
        var genesis = BlockStore.GetBlockByHeight(0) ?? throw new InvalidOperationException("missing genesis block");
        var genesisHash = genesis.BlockHash ?? throw new InvalidOperationException("missing genesis hash");
        var miner = KeyGenerator.GenerateKeypairHex();
        var wallet = KeyGenerator.GenerateKeypairHex();

        KeyStore.AddKey(wallet.privHex, wallet.pubHex);

        var block1 = BuildMinedBlock(
            height: 1UL,
            prevHash: genesisHash,
            timestamp: genesis.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);
        BlockPersistHelper.Persist(block1);

        var block2 = BuildMinedBlock(
            height: 2UL,
            prevHash: block1.BlockHash!,
            timestamp: block1.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);
        BlockPersistHelper.Persist(block2);

        Assert(BlockStore.GetLatestHeight() == 2UL, "test setup must extend canonical chain before backup+resync");

        string dbPath = Path.Combine(_tempRoot, "qado.db");
        var result = StartupRecovery.PerformBackupAndChainResync(_tempRoot, dbPath);

        Assert(Directory.Exists(result.BackupDirectoryPath), "startup recovery must create a backup directory");
        Assert(File.Exists(Path.Combine(result.BackupDirectoryPath, "qado.db")), "startup recovery must move the old database into the backup directory");
        Assert(result.PreservedKeyCount == 1, $"startup recovery preserved key count mismatch: got {result.PreservedKeyCount}");

        Assert(BlockStore.GetLatestHeight() == 0UL, "startup recovery must reset the local chain back to genesis");
        var audit = StartupIntegrityAudit.Run();
        Assert(audit.Disposition == StartupAuditDisposition.Continue, $"fresh chain after startup recovery should pass audit, got {audit.Disposition}");

        var keys = KeyStore.GetAllKeys();
        Assert(keys.Count == 1, $"startup recovery should preserve exactly one wallet key, got {keys.Count}");
        Assert(string.Equals(keys[0].PubHex, wallet.pubHex, StringComparison.Ordinal), "startup recovery preserved the wrong wallet public key");
        Assert(string.Equals(keys[0].PrivHex, wallet.privHex, StringComparison.Ordinal), "startup recovery preserved the wrong wallet private key");
    }

    private static void TestBlockPayloadsStoredInSqlite()
    {
        var h0 = BlockStore.GetCanonicalHashAtHeight(0);
        Assert(h0 is { Length: 32 }, "canonical height 0 hash missing");

        byte[]? payload;
        lock (Db.Sync)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = "SELECT payload FROM block_payloads WHERE hash=$h LIMIT 1;";
            cmd.Parameters.AddWithValue("$h", h0);
            payload = cmd.ExecuteScalar() as byte[];
        }

        Assert(payload is { Length: > 0 }, "serialized genesis payload missing from sqlite");
        Assert(!File.Exists(Path.Combine(_tempRoot, "blocks.dat")), "blocks.dat must not be created");
    }

    private static void TestBlockValidatorTipValidationWorksInsideTransaction()
    {
        var genesisHash = BlockStore.GetCanonicalHashAtHeight(0) ?? throw new InvalidOperationException("missing genesis hash");
        var miner = KeyGenerator.GenerateKeypairHex();

        var block = BuildMinedBlock(
            height: 1,
            prevHash: genesisHash,
            timestamp: (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            minerPubHex: miner.pubHex);

        lock (Db.Sync)
        {
            using var tx = Db.Connection.BeginTransaction();
            bool ok = BlockValidator.ValidateNetworkTipBlock(block, out var reason, tx);
            tx.Rollback();
            Assert(ok, $"tip validation inside transaction failed: {reason}");
        }
    }

    private static void TestBlockValidatorRejectsExhaustedSenderNonce()
    {
        var sender = KeyGenerator.GenerateKeypairHex();
        var recipient = KeyGenerator.GenerateKeypairHex();
        var miner = KeyGenerator.GenerateKeypairHex();

        byte[] prev = BlockStore.GetCanonicalHashAtHeight(0) ?? throw new InvalidOperationException("missing genesis hash");
        var tx = BuildSignedTx(
            senderPrivHex: sender.privHex,
            senderPubHex: sender.pubHex,
            recipientPubHex: recipient.pubHex,
            amount: 1UL,
            fee: 1UL,
            nonce: NonceRules.MaxAccountNonce);
        var block = BuildMinedBlock(
            height: 1UL,
            prevHash: prev,
            timestamp: (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            minerPubHex: miner.pubHex,
            txs: new[] { tx });

        lock (Db.Sync)
        {
            using var sqlTx = Db.Connection.BeginTransaction();
            StateStore.Set(sender.pubHex, balance: 10_000UL, nonce: NonceRules.MaxAccountNonce, sqlTx);

            bool ok = BlockValidator.ValidateNetworkTipBlock(block, out var reason, sqlTx);
            Assert(!ok, "tip validation must reject exhausted sender nonce");
            Assert(reason.Contains("exhausted", StringComparison.OrdinalIgnoreCase) ||
                   reason.Contains("out of range", StringComparison.OrdinalIgnoreCase),
                $"unexpected rejection reason: {reason}");
            sqlTx.Rollback();
        }
    }

    private static void TestBlockValidatorAcceptsLegacyLongMaxNonceBoundary()
    {
        var sender = KeyGenerator.GenerateKeypairHex();
        var recipient = KeyGenerator.GenerateKeypairHex();
        var miner = KeyGenerator.GenerateKeypairHex();

        byte[] prev = BlockStore.GetCanonicalHashAtHeight(0) ?? throw new InvalidOperationException("missing genesis hash");
        ulong legacyMaxNonce = (ulong)long.MaxValue;
        var tx = BuildSignedTx(
            senderPrivHex: sender.privHex,
            senderPubHex: sender.pubHex,
            recipientPubHex: recipient.pubHex,
            amount: 1UL,
            fee: 1UL,
            nonce: legacyMaxNonce);
        var block = BuildMinedBlock(
            height: 1UL,
            prevHash: prev,
            timestamp: (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            minerPubHex: miner.pubHex,
            txs: new[] { tx });

        lock (Db.Sync)
        {
            using var sqlTx = Db.Connection.BeginTransaction();
            StateStore.Set(sender.pubHex, balance: 10_000UL, nonce: legacyMaxNonce - 1UL, sqlTx);

            bool ok = BlockValidator.ValidateNetworkTipBlock(block, out var reason, sqlTx);
            Assert(ok, $"tip validation must accept legacy nonce boundary: {reason}");

            StateApplier.ApplyBlock(block, sqlTx);
            Assert(StateStore.GetNonce(sender.pubHex, sqlTx) == legacyMaxNonce,
                "state apply must persist the legacy nonce boundary");

            sqlTx.Rollback();
        }
    }

    private static void TestStateApplierRejectsNonceGap()
    {
        var sender = KeyGenerator.GenerateKeypairHex();
        var recipient = KeyGenerator.GenerateKeypairHex();
        var miner = KeyGenerator.GenerateKeypairHex();

        byte[] prev = BlockStore.GetCanonicalHashAtHeight(0) ?? throw new InvalidOperationException("missing genesis hash");

        const ulong amount = 1_000UL;
        const ulong fee = 10UL;

        var tx = BuildSignedTx(sender.privHex, sender.pubHex, recipient.pubHex, amount, fee, nonce: 2);
        var coinbase = BuildCoinbase(miner.pubHex, RewardCalculator.GetBlockSubsidy(1) + fee);
        var block = BuildBlock(height: 1, prevHash: prev, minerPubHex: miner.pubHex, txs: new[] { coinbase, tx });

        lock (Db.Sync)
        {
            using var sqlTx = Db.Connection.BeginTransaction();
            StateStore.Set(sender.pubHex, balance: 20_000UL, nonce: 0UL, sqlTx);

            bool rejectedForNonce = false;
            try
            {
                StateApplier.ApplyBlock(block, sqlTx);
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("Nonce mismatch", StringComparison.OrdinalIgnoreCase))
            {
                rejectedForNonce = true;
            }

            Assert(rejectedForNonce, "expected nonce-gap rejection");
            sqlTx.Rollback();
        }
    }

    private static void TestStateApplierAcceptsSequentialNonce()
    {
        var sender = KeyGenerator.GenerateKeypairHex();
        var recipient = KeyGenerator.GenerateKeypairHex();
        var miner = KeyGenerator.GenerateKeypairHex();

        byte[] prev = BlockStore.GetCanonicalHashAtHeight(0) ?? throw new InvalidOperationException("missing genesis hash");

        const ulong startBalance = 20_000UL;
        const ulong amount = 1_000UL;
        const ulong fee = 10UL;

        var tx = BuildSignedTx(sender.privHex, sender.pubHex, recipient.pubHex, amount, fee, nonce: 1);
        ulong expectedCoinbase = RewardCalculator.GetBlockSubsidy(1) + fee;
        var coinbase = BuildCoinbase(miner.pubHex, expectedCoinbase);
        var block = BuildBlock(height: 1, prevHash: prev, minerPubHex: miner.pubHex, txs: new[] { coinbase, tx });

        lock (Db.Sync)
        {
            using var sqlTx = Db.Connection.BeginTransaction();
            StateStore.Set(sender.pubHex, balance: startBalance, nonce: 0UL, sqlTx);

            StateApplier.ApplyBlock(block, sqlTx);

            ulong senderBal = StateStore.GetBalance(sender.pubHex, sqlTx);
            ulong senderNonce = StateStore.GetNonce(sender.pubHex, sqlTx);
            ulong recipientBal = StateStore.GetBalance(recipient.pubHex, sqlTx);
            ulong minerBal = StateStore.GetBalance(miner.pubHex, sqlTx);

            Assert(senderBal == startBalance - amount - fee, "sender balance mismatch");
            Assert(senderNonce == 1UL, "sender nonce mismatch");
            Assert(recipientBal == amount, "recipient balance mismatch");
            Assert(minerBal == expectedCoinbase, "miner balance mismatch");

            sqlTx.Rollback();
        }
    }

    private static void TestStateUndoStoreRollbackRestoresNonce()
    {
        var sender = KeyGenerator.GenerateKeypairHex();
        var recipient = KeyGenerator.GenerateKeypairHex();
        var miner = KeyGenerator.GenerateKeypairHex();

        byte[] prev = BlockStore.GetCanonicalHashAtHeight(0) ?? throw new InvalidOperationException("missing genesis hash");

        const ulong startBalance = 20_000UL;
        const ulong amount = 1_000UL;
        const ulong fee = 10UL;

        var tx = BuildSignedTx(sender.privHex, sender.pubHex, recipient.pubHex, amount, fee, nonce: 1UL);
        var coinbase = BuildCoinbase(miner.pubHex, RewardCalculator.GetBlockSubsidy(1) + fee);
        var block = BuildBlock(height: 1, prevHash: prev, minerPubHex: miner.pubHex, txs: new[] { coinbase, tx });
        block.BlockHash = HashFromU64(90_001UL);

        lock (Db.Sync)
        {
            using var sqlTx = Db.Connection.BeginTransaction();
            StateStore.Set(sender.pubHex, balance: startBalance, nonce: 0UL, sqlTx);

            StateApplier.ApplyBlockWithUndo(block, sqlTx);
            Assert(StateStore.GetNonce(sender.pubHex, sqlTx) == 1UL, "sender nonce must advance before rollback");

            StateUndoStore.RollbackBlock(block.BlockHash, sqlTx);

            Assert(StateStore.GetNonce(sender.pubHex, sqlTx) == 0UL, "sender nonce must be restored by rollback");
            Assert(StateStore.GetBalance(sender.pubHex, sqlTx) == startBalance, "sender balance must be restored by rollback");
            sqlTx.Rollback();
        }
    }

    private static void TestStateStoreRoundTripsFullUlongNonce()
    {
        var account = KeyGenerator.GenerateKeypairHex();

        lock (Db.Sync)
        {
            using var sqlTx = Db.Connection.BeginTransaction();
            StateStore.Set(account.pubHex, balance: 123UL, nonce: ulong.MaxValue, sqlTx);

            ulong storedNonce = StateStore.GetNonce(account.pubHex, sqlTx);
            Assert(storedNonce == ulong.MaxValue, "state store must round-trip ulong.MaxValue nonce");

            using var cmd = sqlTx.Connection!.CreateCommand();
            cmd.Transaction = sqlTx;
            cmd.CommandText = "SELECT nonce FROM accounts WHERE addr=$a LIMIT 1;";
            cmd.Parameters.AddWithValue("$a", Convert.FromHexString(account.pubHex));
            object raw = cmd.ExecuteScalar() ?? throw new InvalidOperationException("nonce row missing");
            Assert(raw is byte[] { Length: 8 }, "nonce must be stored as 8-byte blob");

            sqlTx.Rollback();
        }
    }

    private static void TestStateApplierSelfTransferMinedBySenderIsNotInflationary()
    {
        var self = KeyGenerator.GenerateKeypairHex();

        byte[] prev = BlockStore.GetCanonicalHashAtHeight(0) ?? throw new InvalidOperationException("missing genesis hash");

        const ulong startBalance = 900_000UL;
        const ulong amount = 123_456UL;
        const ulong fee = 789UL;

        var tx = BuildSignedTx(self.privHex, self.pubHex, self.pubHex, amount, fee, nonce: 1);
        ulong expectedCoinbase = RewardCalculator.GetBlockSubsidy(1) + fee;
        var coinbase = BuildCoinbase(self.pubHex, expectedCoinbase);
        var block = BuildBlock(height: 1, prevHash: prev, minerPubHex: self.pubHex, txs: new[] { coinbase, tx });

        lock (Db.Sync)
        {
            using var sqlTx = Db.Connection.BeginTransaction();
            StateStore.Set(self.pubHex, balance: startBalance, nonce: 0UL, sqlTx);

            StateApplier.ApplyBlock(block, sqlTx);

            ulong balance = StateStore.GetBalance(self.pubHex, sqlTx);
            ulong nonce = StateStore.GetNonce(self.pubHex, sqlTx);

            Assert(balance == startBalance + RewardCalculator.GetBlockSubsidy(1),
                "self-transfer must only change balance by the block subsidy");
            Assert(nonce == 1UL, "self-transfer sender nonce mismatch");

            sqlTx.Rollback();
        }
    }

    private static void TestKeyStoreEncryptsAtRest()
    {
        var key = KeyGenerator.GenerateKeypairHex();
        var legacy = KeyGenerator.GenerateKeypairHex();

        lock (Db.Sync)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = "DELETE FROM keys;";
            cmd.ExecuteNonQuery();
        }

        KeyStore.AddKey(key.privHex, key.pubHex);

        byte[] storedBlob;
        lock (Db.Sync)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = "SELECT priv FROM keys WHERE pub=$p LIMIT 1;";
            cmd.Parameters.AddWithValue("$p", Convert.FromHexString(key.pubHex));
            storedBlob = cmd.ExecuteScalar() as byte[] ?? throw new InvalidOperationException("encrypted key row missing");
        }

        byte[] rawPriv = Convert.FromHexString(key.privHex);
        Assert(!storedBlob.SequenceEqual(rawPriv), "private key must not be stored in plaintext");
        Assert(storedBlob.Length > 3 && storedBlob[0] == (byte)'Q' && storedBlob[1] == (byte)'K',
            "encrypted key blob header missing");

        lock (Db.Sync)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = "INSERT OR REPLACE INTO keys(pub,priv) VALUES($p,$k);";
            cmd.Parameters.AddWithValue("$p", Convert.FromHexString(legacy.pubHex));
            cmd.Parameters.AddWithValue("$k", Convert.FromHexString(legacy.privHex));
            cmd.ExecuteNonQuery();
        }

        var keys = KeyStore.GetAllKeys();
        bool foundEncrypted = keys.Any(k => string.Equals(k.PubHex, key.pubHex, StringComparison.Ordinal) &&
                                            string.Equals(k.PrivHex, key.privHex, StringComparison.Ordinal));
        bool foundLegacy = keys.Any(k => string.Equals(k.PubHex, legacy.pubHex, StringComparison.Ordinal) &&
                                         string.Equals(k.PrivHex, legacy.privHex, StringComparison.Ordinal));

        Assert(foundEncrypted, "encrypted key must roundtrip");
        Assert(foundLegacy, "legacy key must remain readable");
    }

    private static void TestMerkleDomainSeparationProfile()
    {
        byte[] a = Blake3Util.Hash(new byte[] { 0xA1 });
        byte[] b = Blake3Util.Hash(new byte[] { 0xB2 });
        byte[] c = Blake3Util.Hash(new byte[] { 0xC3 });

        var root1 = MerkleUtil.ComputeMerkleRoot(new List<byte[]> { a });
        var want1 = HashLeaf(a);
        Assert(root1.SequenceEqual(want1), "single-leaf root must be domain-separated leaf hash");

        var root2 = MerkleUtil.ComputeMerkleRoot(new List<byte[]> { a, b });
        var want2 = HashNode(HashLeaf(a), HashLeaf(b));
        Assert(root2.SequenceEqual(want2), "two-leaf root mismatch");

        var root3 = MerkleUtil.ComputeMerkleRoot(new List<byte[]> { a, b, c });
        var n1 = HashNode(HashLeaf(a), HashLeaf(b));
        var n2 = HashNode(HashLeaf(c), HashLeaf(c));
        var want3 = HashNode(n1, n2);
        Assert(root3.SequenceEqual(want3), "odd-leaf root mismatch");
    }

    private static void TestTxIdCommitsSignature()
    {
        var sender = KeyGenerator.GenerateKeypairHex();
        var recipient = KeyGenerator.GenerateKeypairHex();

        var tx = BuildSignedTx(
            senderPrivHex: sender.privHex,
            senderPubHex: sender.pubHex,
            recipientPubHex: recipient.pubHex,
            amount: 1234UL,
            fee: 5UL,
            nonce: 1UL);

        var txMut = new Transaction
        {
            ChainId = tx.ChainId,
            Sender = (byte[])tx.Sender.Clone(),
            Recipient = (byte[])tx.Recipient.Clone(),
            Amount = tx.Amount,
            Fee = tx.Fee,
            TxNonce = tx.TxNonce,
            Signature = (byte[])tx.Signature.Clone()
        };
        txMut.Signature[0] ^= 0x01;

        Assert(tx.ToHashBytes().SequenceEqual(txMut.ToHashBytes()), "signing preimage must ignore signature");
        Assert(!tx.ComputeTransactionHash().SequenceEqual(txMut.ComputeTransactionHash()),
            "txid must change when signature changes");

        var rootA = MerkleUtil.ComputeMerkleRootFromTransactions(new List<Transaction> { tx });
        var rootB = MerkleUtil.ComputeMerkleRootFromTransactions(new List<Transaction> { txMut });
        Assert(!rootA.SequenceEqual(rootB), "merkle root must commit signature bytes via txid");
    }

    private static void TestExchangeApiTxLookupBlockRefDisambiguatesDeterministicCoinbase()
    {
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);

        var genesis = BlockStore.GetBlockByHeight(0) ?? throw new InvalidOperationException("missing genesis block");
        var genesisHash = genesis.BlockHash ?? throw new InvalidOperationException("missing genesis hash");
        ulong genesisTimestamp = genesis.Header?.Timestamp ?? (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        var miner = KeyGenerator.GenerateKeypairHex();

        var block1 = BuildMinedBlock(
            height: 1,
            prevHash: genesisHash,
            timestamp: genesisTimestamp + 60UL,
            minerPubHex: miner.pubHex);
        BlockPersistHelper.Persist(block1, mempool: mempool);

        var block1Hash = block1.BlockHash ?? throw new InvalidOperationException("missing block1 hash");
        var block2 = BuildMinedBlock(
            height: 2,
            prevHash: block1Hash,
            timestamp: genesisTimestamp + 120UL,
            minerPubHex: miner.pubHex);
        BlockPersistHelper.Persist(block2, mempool: mempool);

        Assert(BlockStore.GetLatestHeight() == 2UL, "expected canonical tip height 2");

        var coinbase1 = block1.Transactions[0];
        var coinbase2 = block2.Transactions[0];
        var txid1 = coinbase1.ComputeTransactionHash();
        var txid2 = coinbase2.ComputeTransactionHash();
        Assert(txid1.SequenceEqual(txid2), "test setup must create the same deterministic coinbase txid twice");

        string txidHex = Convert.ToHexString(txid1).ToLowerInvariant();
        string block1HashHex = Convert.ToHexString(block1Hash).ToLowerInvariant();
        string block2HashHex = Convert.ToHexString(block2.BlockHash ?? throw new InvalidOperationException("missing block2 hash")).ToLowerInvariant();

        int port = GetFreeTcpPort();
        using var host = new ExchangeApiHost(mempool, () => null, log: null);
        using var client = new HttpClient
        {
            BaseAddress = new Uri($"http://127.0.0.1:{port}"),
            Timeout = TimeSpan.FromSeconds(10)
        };

        host.StartAsync(port).GetAwaiter().GetResult();
        try
        {
            using var defaultLookup = GetJson(client, $"/v1/tx/{txidHex}");
            Assert(
                defaultLookup.RootElement.GetProperty("block_height").GetString() == "2",
                "txid-only lookup should continue returning the newest indexed canonical occurrence");
            Assert(
                defaultLookup.RootElement.GetProperty("block_hash").GetString() == block2HashHex,
                "txid-only lookup resolved the wrong canonical occurrence");

            using var heightScopedLookup = GetJson(client, $"/v1/tx/{txidHex}?block_ref=1");
            Assert(
                heightScopedLookup.RootElement.GetProperty("block_height").GetString() == "1",
                "block_ref height should resolve the specific height-1 occurrence");
            Assert(
                heightScopedLookup.RootElement.GetProperty("block_hash").GetString() == block1HashHex,
                "block_ref height resolved the wrong block hash");

            using var hashScopedConfirmations = GetJson(client, $"/v1/tx/{txidHex}/confirmations?block_ref={block1HashHex}");
            Assert(
                hashScopedConfirmations.RootElement.GetProperty("block_height").GetString() == "1",
                "block_ref hash should resolve confirmations for the requested block");
            Assert(
                hashScopedConfirmations.RootElement.GetProperty("confirmations").GetString() == "2",
                "height-1 occurrence should have two confirmations at tip height 2");
            Assert(
                hashScopedConfirmations.RootElement.GetProperty("tip_height").GetString() == "2",
                "confirmations response must report the current tip height");
        }
        finally
        {
            host.StopAsync().GetAwaiter().GetResult();
        }
    }

    private static void TestExchangeApiMiningJobsSameMinerCanHoldMultipleOutstandingJobs()
    {
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);

        var miner = KeyGenerator.GenerateKeypairHex();

        int port = GetFreeTcpPort();
        using var host = new ExchangeApiHost(mempool, () => null, log: null);
        using var client = new HttpClient
        {
            BaseAddress = new Uri($"http://127.0.0.1:{port}"),
            Timeout = TimeSpan.FromSeconds(10)
        };

        host.StartAsync(port).GetAwaiter().GetResult();
        try
        {
            using var job1 = PostJson(client, "/v1/mining/job", new { miner = miner.pubHex });
            using var job2 = PostJson(client, "/v1/mining/job", new { miner = miner.pubHex });

            string jobId1 = job1.RootElement.GetProperty("job_id").GetString()
                ?? throw new InvalidOperationException("first mining job id missing");
            string jobId2 = job2.RootElement.GetProperty("job_id").GetString()
                ?? throw new InvalidOperationException("second mining job id missing");
            Assert(!string.Equals(jobId1, jobId2, StringComparison.Ordinal), "mining job ids must be unique per request");

            ulong timestamp1 = ulong.Parse(job1.RootElement.GetProperty("timestamp").GetString() ?? "0");
            ulong timestamp2 = ulong.Parse(job2.RootElement.GetProperty("timestamp").GetString() ?? "0");
            Assert(timestamp1 > 0UL && timestamp2 > 0UL, "mining jobs must expose a positive timestamp");

            using var submit1 = PostJson(client, "/v1/mining/submit", new
            {
                job_id = jobId1,
                nonce = "0",
                timestamp = (timestamp1 - 1UL).ToString()
            });
            Assert(!submit1.RootElement.GetProperty("accepted").GetBoolean(), "first stale-timestamp submit must not be accepted");
            Assert(
                submit1.RootElement.GetProperty("reason").GetString() == "invalid_timestamp",
                "first job should still be found after requesting a second job for the same miner");

            using var submit2 = PostJson(client, "/v1/mining/submit", new
            {
                job_id = jobId2,
                nonce = "0",
                timestamp = (timestamp2 - 1UL).ToString()
            });
            Assert(!submit2.RootElement.GetProperty("accepted").GetBoolean(), "second stale-timestamp submit must not be accepted");
            Assert(
                submit2.RootElement.GetProperty("reason").GetString() == "invalid_timestamp",
                "second job should remain independently addressable by job id");
        }
        finally
        {
            host.StopAsync().GetAwaiter().GetResult();
        }
    }

    private static void TestBlockSerializerRejectsInvalidTarget()
    {
        var miner = KeyGenerator.GenerateKeypairHex();
        var block = BuildBlock(
            height: 1,
            prevHash: new byte[32],
            minerPubHex: miner.pubHex,
            txs: new[] { BuildCoinbase(miner.pubHex, 1UL) });

        int size = BlockBinarySerializer.GetSize(block);
        var payload = new byte[size];
        _ = BlockBinarySerializer.Write(payload, block);

        const int targetOffset = 1 + 32 + 32 + 8;
        for (int i = 0; i < 32; i++)
            payload[targetOffset + i] = 0xFF;

        bool rejected = false;
        try
        {
            _ = BlockBinarySerializer.Read(payload);
        }
        catch (ArgumentException ex) when (ex.Message.Contains("target", StringComparison.OrdinalIgnoreCase))
        {
            rejected = true;
        }

        Assert(rejected, "serializer must reject out-of-range block target");
    }

    private static void TestChainSelectorReorgMempoolReconcile()
    {
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);

        var genesis = BlockStore.GetBlockByHeight(0) ?? throw new InvalidOperationException("missing genesis block");
        var genesisHash = genesis.BlockHash ?? throw new InvalidOperationException("missing genesis hash");

        var senderResurrect = KeyGenerator.GenerateKeypairHex();
        var recipientResurrect = KeyGenerator.GenerateKeypairHex();
        var senderDrop = KeyGenerator.GenerateKeypairHex();
        var recipientDrop = KeyGenerator.GenerateKeypairHex();
        var recipientConflict = KeyGenerator.GenerateKeypairHex();
        var minerA1 = KeyGenerator.GenerateKeypairHex();
        var minerB1 = KeyGenerator.GenerateKeypairHex();
        var minerB2 = KeyGenerator.GenerateKeypairHex();

        SeedAccount(senderResurrect.pubHex, balance: 1_000_000UL, nonce: 0UL);
        SeedAccount(senderDrop.pubHex, balance: 1_000_000UL, nonce: 0UL);

        var txResurrect = BuildSignedTx(
            senderPrivHex: senderResurrect.privHex,
            senderPubHex: senderResurrect.pubHex,
            recipientPubHex: recipientResurrect.pubHex,
            amount: 25_000UL,
            fee: 100UL,
            nonce: 1UL);

        var txDrop = BuildSignedTx(
            senderPrivHex: senderDrop.privHex,
            senderPubHex: senderDrop.pubHex,
            recipientPubHex: recipientDrop.pubHex,
            amount: 30_000UL,
            fee: 150UL,
            nonce: 1UL);

        var a1 = BuildMinedBlock(
            height: 1,
            prevHash: genesisHash,
            timestamp: genesis.Header!.Timestamp + 61UL,
            minerPubHex: minerA1.pubHex,
            txs: new[] { txResurrect, txDrop });
        BlockPersistHelper.Persist(a1, mempool: mempool);

        Assert(!MempoolContainsTx(mempool, txResurrect), "tx should not be pending while canonical");
        Assert(!MempoolContainsTx(mempool, txDrop), "tx should not be pending while canonical");

        var b1 = BuildMinedBlock(
            height: 1,
            prevHash: genesisHash,
            timestamp: genesis.Header.Timestamp + 62UL,
            minerPubHex: minerB1.pubHex);
        BlockPersistHelper.Persist(b1, mempool: mempool);

        var txConflict = BuildSignedTx(
            senderPrivHex: senderDrop.privHex,
            senderPubHex: senderDrop.pubHex,
            recipientPubHex: recipientConflict.pubHex,
            amount: 12_000UL,
            fee: 200UL,
            nonce: 1UL);

        var b2 = BuildMinedBlock(
            height: 2,
            prevHash: b1.BlockHash!,
            timestamp: b1.Header!.Timestamp + 61UL,
            minerPubHex: minerB2.pubHex,
            txs: new[] { txConflict });
        BlockPersistHelper.Persist(b2, mempool: mempool);

        var canonH1 = BlockStore.GetCanonicalHashAtHeight(1) ?? throw new InvalidOperationException("missing canonical h=1");
        var canonH2 = BlockStore.GetCanonicalHashAtHeight(2) ?? throw new InvalidOperationException("missing canonical h=2");
        Assert(BytesEqual(canonH1, b1.BlockHash!), "reorg must replace canonical h=1 with side branch block");
        Assert(BytesEqual(canonH2, b2.BlockHash!), "stronger side-chain must become canonical");
        Assert(MempoolContainsTx(mempool, txResurrect), "tx from reorged-out block should be requeued");
        Assert(!MempoolContainsTx(mempool, txDrop), "conflicting tx must not be requeued after reorg");
        Assert(!MempoolContainsTx(mempool, txConflict), "tx included in new canonical chain must not be in mempool");
    }

    private static void TestMempoolSelectionRespectsSenderNonceOrder()
    {
        var senderA = KeyGenerator.GenerateKeypairHex();
        var senderB = KeyGenerator.GenerateKeypairHex();
        var recipient = KeyGenerator.GenerateKeypairHex();

        string senderAHex = senderA.pubHex.ToLowerInvariant();
        string senderBHex = senderB.pubHex.ToLowerInvariant();

        var confirmedBySender = new Dictionary<string, ulong>(StringComparer.Ordinal)
        {
            [senderAHex] = 0UL,
            [senderBHex] = 0UL
        };

        var balanceBySender = new Dictionary<string, ulong>(StringComparer.Ordinal)
        {
            [senderAHex] = 10_000_000UL,
            [senderBHex] = 10_000_000UL
        };

        var buffer = new PendingTransactionBuffer(
            getConfirmedNonce: sender =>
            {
                sender = sender.ToLowerInvariant();
                return confirmedBySender.TryGetValue(sender, out var n) ? n : 0UL;
            },
            getBalance: sender =>
            {
                sender = sender.ToLowerInvariant();
                return balanceBySender.TryGetValue(sender, out var b) ? b : 0UL;
            });

        for (ulong nonce = 1; nonce <= 101UL; nonce++)
        {
            ulong fee = nonce == 101UL ? 100_000UL : 1UL;
            var tx = BuildSignedTx(
                senderPrivHex: senderA.privHex,
                senderPubHex: senderA.pubHex,
                recipientPubHex: recipient.pubHex,
                amount: 1UL,
                fee: fee,
                nonce: nonce);

            Assert(buffer.Add(tx), $"failed to add sender A nonce={nonce}");
        }

        var txB1 = BuildSignedTx(
            senderPrivHex: senderB.privHex,
            senderPubHex: senderB.pubHex,
            recipientPubHex: recipient.pubHex,
            amount: 1UL,
            fee: 50_000UL,
            nonce: 1UL);
        var txB2 = BuildSignedTx(
            senderPrivHex: senderB.privHex,
            senderPubHex: senderB.pubHex,
            recipientPubHex: recipient.pubHex,
            amount: 1UL,
            fee: 1UL,
            nonce: 2UL);

        Assert(buffer.Add(txB1), "failed to add sender B nonce=1");
        Assert(buffer.Add(txB2), "failed to add sender B nonce=2");

        var selected = buffer.GetAllReadyTransactionsSortedByFee();
        Assert(selected.Count == 103, $"expected 103 selected tx, got {selected.Count}");

        var lastNonceBySender = new Dictionary<string, ulong>(StringComparer.Ordinal);
        for (int i = 0; i < selected.Count; i++)
        {
            var tx = selected[i];
            string sender = Convert.ToHexString(tx.Sender).ToLowerInvariant();

            ulong confirmed = confirmedBySender.TryGetValue(sender, out var cn) ? cn : 0UL;
            ulong expected = lastNonceBySender.TryGetValue(sender, out var prev)
                ? checked(prev + 1UL)
                : checked(confirmed + 1UL);

            Assert(tx.TxNonce == expected,
                $"nonce order violated for {sender}: got {tx.TxNonce}, expected {expected} at index {i}");

            lastNonceBySender[sender] = tx.TxNonce;
        }
    }

    private static void TestPendingTransactionBufferRejectsExhaustedSenderNonce()
    {
        var sender = KeyGenerator.GenerateKeypairHex();
        var recipient = KeyGenerator.GenerateKeypairHex();

        var buffer = new PendingTransactionBuffer(
            getConfirmedNonce: _ => NonceRules.MaxAccountNonce,
            getBalance: _ => 1_000_000UL);

        var tx = BuildSignedTx(
            senderPrivHex: sender.privHex,
            senderPubHex: sender.pubHex,
            recipientPubHex: recipient.pubHex,
            amount: 1UL,
            fee: 1UL,
            nonce: 1UL);

        Assert(!buffer.Add(tx), "buffer must reject transactions once sender nonce is exhausted");
    }

    private static void TestBlockLocatorFindsForkpoint()
    {
        ulong tipHeight = BlockStore.GetLatestHeight();
        ulong forkHeight = tipHeight > 0 ? tipHeight - 1UL : 0UL;
        byte[] forkHash = BlockStore.GetCanonicalHashAtHeight(forkHeight)
            ?? throw new InvalidOperationException("canonical fork hash missing");

        var locator = new List<byte[]>
        {
            HashFromU64(999_999UL),
            forkHash,
            BlockStore.GetCanonicalHashAtHeight(0) ?? throw new InvalidOperationException("missing genesis hash")
        };

        bool found = BlockSyncServer.TryFindForkPoint(locator, out var foundHash, out var foundHeight);
        Assert(found, "expected locator forkpoint to be found");
        Assert(foundHeight == forkHeight, $"fork height mismatch: got {foundHeight}, expected {forkHeight}");
        Assert(BytesEqual(foundHash, forkHash), "fork hash mismatch");
    }

    private static void TestBlockSyncProtocolChunks4096Blocks()
    {
        var blocks = new List<byte[]>(4096);
        for (int i = 0; i < 4096; i++)
        {
            var blob = new byte[96];
            blob[0] = (byte)i;
            blob[95] = (byte)(255 - (i & 0xFF));
            blocks.Add(blob);
        }

        var payloads = BlockSyncProtocol.BuildChunkPayloads(blocks, startHeight: 777UL);
        Assert(payloads.Count == 64, $"expected 64 chunk payloads, got {payloads.Count}");

        ulong expectedHeight = 777UL;
        int totalBlocks = 0;

        for (int i = 0; i < payloads.Count; i++)
        {
            Assert(BlockSyncProtocol.TryParseBlockChunk(payloads[i], out var frame), $"chunk {i} did not parse");
            Assert(frame.FirstHeight == expectedHeight, $"chunk {i} first height mismatch");
            Assert(frame.Blocks.Count > 0 && frame.Blocks.Count <= BlockSyncProtocol.ChunkBlocks,
                $"chunk {i} block count out of range");

            totalBlocks += frame.Blocks.Count;
            expectedHeight += (ulong)frame.Blocks.Count;
        }

        Assert(totalBlocks == 4096, $"expected 4096 chunked blocks, got {totalBlocks}");
    }

    private static void TestBlockSyncServerUsesSmallNetBatchFrames()
    {
        ulong tipHeight = BlockStore.GetLatestHeight();
        var tipBlock = BlockStore.GetBlockByHeight(tipHeight) ?? throw new InvalidOperationException("missing current tip block");
        byte[] oldTipHash = tipBlock.BlockHash ?? throw new InvalidOperationException("missing current tip hash");
        var miner = KeyGenerator.GenerateKeypairHex();

        var nextBlock = BuildMinedBlock(
            height: tipHeight + 1UL,
            prevHash: oldTipHash,
            timestamp: tipBlock.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);
        BlockPersistHelper.Persist(nextBlock);

        var sent = new List<(MsgType type, byte[] payload)>();
        var peer = CreateFakePeer("smallnet-sync-peer");
        byte[] request = BlockSyncProtocol.BuildGetBlocksFrom(oldTipHash, maxBlocks: 1);

        BlockSyncServer.HandleGetBlocksFromAsync(
                request,
                peer,
                (p, type, payload, ct) =>
                {
                    sent.Add((type, (byte[])payload.Clone()));
                    return Task.CompletedTask;
                },
                log: null,
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count >= 3, $"expected at least 3 frames, got {sent.Count}");
        Assert(sent[0].type == MsgType.BlocksBatchStart, $"expected BlocksBatchStart first, got {sent[0].type}");
        Assert(sent[1].type == MsgType.BlocksChunk, $"expected BlocksChunk second, got {sent[1].type}");
        Assert(sent[^1].type == MsgType.BlocksBatchEnd, $"expected BlocksBatchEnd last, got {sent[^1].type}");

        Assert(SmallNetSyncProtocol.TryParseBlocksBatchStart(sent[0].payload, out var batchStart), "batch start payload did not parse");
        Assert(batchStart.TotalBlocks == 1, $"expected 1 block in batch, got {batchStart.TotalBlocks}");
        Assert(batchStart.ForkHeight == tipHeight, "fork height mismatch");

        Assert(SmallNetSyncProtocol.TryParseBlocksChunk(sent[1].payload, out var chunk), "blocks chunk payload did not parse");
        Assert(chunk.Blocks.Count == 1, $"expected exactly one chunk block, got {chunk.Blocks.Count}");

        Assert(SmallNetSyncProtocol.TryParseBlocksBatchEnd(sent[^1].payload, out var batchEnd), "batch end payload did not parse");
        Assert(!batchEnd.MoreAvailable, "single-block test batch should finish at tip");
    }

    private static void TestSmallNetSyncProtocolRoundTripsCoreFrames()
    {
        byte[] nodeId = HashFromU64(1UL);
        byte[] tipHash = HashFromU64(2UL);
        byte[] stateDigest = HashFromU64(3UL);
        var recent = new List<byte[]> { HashFromU64(10UL), HashFromU64(11UL), HashFromU64(12UL) };

        var hello = new SmallNetHelloFrame(
            SmallNetSyncProtocol.CurrentProtocolVersion,
            nodeId,
            tipHash,
            123UL,
            ((UInt128)1 << 70) + 99UL,
            stateDigest);
        byte[] helloPayload = SmallNetSyncProtocol.BuildHello(hello);
        Assert(SmallNetSyncProtocol.TryParseHello(helloPayload, out var helloRoundTrip), "hello did not roundtrip");
        Assert(helloRoundTrip.ProtocolVersion == hello.ProtocolVersion, "hello version mismatch");
        Assert(BytesEqual(helloRoundTrip.NodeId, nodeId), "hello node id mismatch");
        Assert(BytesEqual(helloRoundTrip.TipHash, tipHash), "hello tip hash mismatch");
        Assert(helloRoundTrip.TipHeight == 123UL, "hello tip height mismatch");
        Assert(helloRoundTrip.TipChainwork == hello.TipChainwork, "hello chainwork mismatch");
        Assert(BytesEqual(helloRoundTrip.StateDigest!, stateDigest), "hello state digest mismatch");

        var tipState = new SmallNetTipStateFrame(124UL, tipHash, 999UL, stateDigest, recent);
        byte[] tipPayload = SmallNetSyncProtocol.BuildTipState(tipState);
        Assert(SmallNetSyncProtocol.TryParseTipState(tipPayload, out var tipRoundTrip), "tip state did not roundtrip");
        Assert(tipRoundTrip.Height == 124UL, "tip state height mismatch");
        Assert(BytesEqual(tipRoundTrip.TipHash, tipHash), "tip state hash mismatch");
        Assert(tipRoundTrip.Chainwork == 999UL, "tip state chainwork mismatch");
        Assert(tipRoundTrip.RecentCanonical.Count == recent.Count, "tip state recent canonical count mismatch");

        byte[] ancestorRequestPayload = SmallNetSyncProtocol.BuildGetAncestorPack(tipHash, 8);
        Assert(
            SmallNetSyncProtocol.TryParseGetAncestorPack(ancestorRequestPayload, out var ancestorRequest),
            "ancestor request did not roundtrip");
        Assert(BytesEqual(ancestorRequest.StartHash, tipHash), "ancestor request hash mismatch");
        Assert(ancestorRequest.MaxBlocks == 8, "ancestor request count mismatch");

        var packBlocks = new List<byte[]> { new byte[] { 0x01, 0x02 }, new byte[] { 0x03, 0x04, 0x05 } };
        byte[] packPayload = SmallNetSyncProtocol.BuildAncestorPack(packBlocks);
        Assert(SmallNetSyncProtocol.TryParseAncestorPack(packPayload, out var packRoundTrip), "ancestor pack did not roundtrip");
        Assert(packRoundTrip.Blocks.Count == 2, "ancestor pack count mismatch");

        Guid batchId = Guid.NewGuid();
        var batchStart = new SmallNetBlocksBatchStartFrame(batchId, HashFromU64(50UL), 55UL, 512, tipHash, 12345UL);
        byte[] batchStartPayload = SmallNetSyncProtocol.BuildBlocksBatchStart(batchStart);
        Assert(
            SmallNetSyncProtocol.TryParseBlocksBatchStart(batchStartPayload, out var batchStartRoundTrip),
            "batch start did not roundtrip");
        Assert(batchStartRoundTrip.BatchId == batchId, "batch start batch id mismatch");
        Assert(batchStartRoundTrip.TotalBlocks == 512, "batch start total blocks mismatch");

        var batchEnd = new SmallNetBlocksBatchEndFrame(batchId, HashFromU64(99UL), 777UL, MoreAvailable: true);
        byte[] batchEndPayload = SmallNetSyncProtocol.BuildBlocksBatchEnd(batchEnd);
        Assert(
            SmallNetSyncProtocol.TryParseBlocksBatchEnd(batchEndPayload, out var batchEndRoundTrip),
            "batch end did not roundtrip");
        Assert(batchEndRoundTrip.BatchId == batchId, "batch end batch id mismatch");
        Assert(batchEndRoundTrip.MoreAvailable, "batch end moreAvailable mismatch");
    }

    private static void TestSmallNetPeerStateClassifiesGapPragmatically()
    {
        var peer = new SmallNetPeerState("peer-smallnet");
        byte[] sharedRecent = HashFromU64(222UL);
        byte[] remoteTip = HashFromU64(223UL);
        byte[] localTip = HashFromU64(224UL);

        peer.UpdateTipState(
            new SmallNetTipStateFrame(
                Height: 101UL,
                TipHash: remoteTip,
                Chainwork: 5_100UL,
                StateDigest: null,
                RecentCanonical: new[] { sharedRecent }),
            DateTime.UtcNow);

        var local = new SmallNetLocalChainView(
            Height: 100UL,
            TipHash: localTip,
            Chainwork: 5_000UL,
            RecentCanonical: new[] { sharedRecent },
            StateDigest: null);

        var decision = peer.ClassifyGap(local, smallGapThreshold: 8);
        Assert(decision.Kind == SmallNetGapKind.RemoteAheadSmall, $"unexpected gap kind: {decision.Kind}");
        Assert(decision.BlockGap == 1, $"unexpected gap size: {decision.BlockGap}");
        Assert(decision.LikelyFork, "shared recent canonical must mark the situation as likely fork/catchup");
    }

    private static void TestSmallNetPeerFlowPushesCanonicalTailToLaggingPeer()
    {
        var peer = CreateFakePeer("peer-tail");
        byte[] nodeId = HashFromU64(9_001UL);
        byte[] hash1 = HashFromU64(1UL);
        byte[] hash2 = HashFromU64(2UL);
        byte[] hash3 = HashFromU64(3UL);
        var hashByHeight = new Dictionary<ulong, byte[]>
        {
            [1UL] = hash1,
            [2UL] = hash2,
            [3UL] = hash3
        };
        var payloadByHeight = new Dictionary<ulong, byte[]>
        {
            [2UL] = new byte[] { 0xB2 },
            [3UL] = new byte[] { 0xB3 }
        };
        var sent = new List<(MsgType type, byte[] payload)>();

        var client = new BlockSyncClient(
            sessionSnapshot: () => new[] { peer },
            sendFrameAsync: (targetPeer, type, payload, ct) => Task.CompletedTask,
            getLocalTipChainwork: () => 300UL,
            getLocalTipHash: () => (byte[])hash3.Clone(),
            prepareBatchAsync: (startHash, startHeight, advertisedTipChainwork, targetPeer, ct) =>
                Task.FromResult(new BlockSyncPrepareResult(true, string.Empty)),
            commitChunkAsync: (payloads, height, prevHash, targetPeer, ct) =>
                Task.FromResult(new BlockSyncChunkCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            log: null);

        var flow = new SmallNetPeerFlow(
            nodeId: nodeId,
            peers: new SmallNetPeerDirectory(),
            coordinator: new SmallNetCoordinator(),
            blockSyncClient: client,
            getLocalChainView: () => new SmallNetLocalChainView(3UL, hash3, 300UL, new[] { hash1, hash2, hash3 }, null),
            getCanonicalHashAtHeight: height =>
            {
                return hashByHeight.TryGetValue(height, out var hash)
                    ? (byte[])hash.Clone()
                    : null;
            },
            getCanonicalPayloadAtHeight: height =>
            {
                return payloadByHeight.TryGetValue(height, out var payload)
                    ? (byte[])payload.Clone()
                    : null;
            },
            sendFrameAsync: (targetPeer, type, payload, ct) =>
            {
                sent.Add((type, (byte[])payload.Clone()));
                return Task.CompletedTask;
            },
            dropSession: (targetPeer, reason) => throw new InvalidOperationException($"unexpected drop: {reason}"),
            log: null);

        var remoteTip = new SmallNetTipStateFrame(1UL, hash1, 100UL, null, new[] { hash1 });
        flow.HandleTipStateAsync(SmallNetSyncProtocol.BuildTipState(remoteTip), peer, CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 3, $"expected 3 frames for tail push, got {sent.Count}");
        Assert(sent[0].type == MsgType.Block, $"expected first frame Block, got {sent[0].type}");
        Assert(BytesEqual(sent[0].payload, payloadByHeight[2UL]), "first pushed block payload mismatch");
        Assert(sent[1].type == MsgType.Block, $"expected second frame Block, got {sent[1].type}");
        Assert(BytesEqual(sent[1].payload, payloadByHeight[3UL]), "second pushed block payload mismatch");
        Assert(sent[2].type == MsgType.TipState, $"expected last frame TipState, got {sent[2].type}");
        Assert(SmallNetSyncProtocol.TryParseTipState(sent[2].payload, out var pushedTip), "tail push tipstate did not parse");
        Assert(pushedTip.Height == 3UL, $"tail push tipstate height mismatch: {pushedTip.Height}");
        Assert(BytesEqual(pushedTip.TipHash, hash3), "tail push tip hash mismatch");
    }

    private static void TestSmallNetPeerFlowReconnectHandshakePushesTail()
    {
        var peerA = CreateFakePeer("node-a");
        var peerB = CreateFakePeer("node-b");
        byte[] hash1 = HashFromU64(101UL);
        byte[] hash2 = HashFromU64(102UL);
        byte[] hash3 = HashFromU64(103UL);
        var chainAHashes = new Dictionary<ulong, byte[]>
        {
            [1UL] = hash1,
            [2UL] = hash2,
            [3UL] = hash3
        };
        var chainAPayloads = new Dictionary<ulong, byte[]>
        {
            [2UL] = new byte[] { 0xC2 },
            [3UL] = new byte[] { 0xC3 }
        };
        var nodeBReceivedBlocks = new List<byte[]>();
        var nodeBRequested = new List<(MsgType type, byte[] payload)>();

        SmallNetPeerFlow? flowB = null;
        var flowA = new SmallNetPeerFlow(
            nodeId: HashFromU64(11UL),
            peers: new SmallNetPeerDirectory(),
            coordinator: new SmallNetCoordinator(),
            blockSyncClient: CreateNoopBlockSyncClient(peerA, 300UL, hash3),
            getLocalChainView: () => new SmallNetLocalChainView(3UL, hash3, 300UL, new[] { hash1, hash2, hash3 }, null),
            getCanonicalHashAtHeight: height => chainAHashes.TryGetValue(height, out var hash) ? (byte[])hash.Clone() : null,
            getCanonicalPayloadAtHeight: height => chainAPayloads.TryGetValue(height, out var payload) ? (byte[])payload.Clone() : null,
            sendFrameAsync: (targetPeer, type, payload, ct) =>
            {
                var copy = (byte[])payload.Clone();
                if (type == MsgType.Block)
                {
                    nodeBReceivedBlocks.Add(copy);
                }
                else if (type == MsgType.TipState && flowB != null)
                {
                    flowB.HandleTipStateAsync(copy, peerA, ct).GetAwaiter().GetResult();
                }

                return Task.CompletedTask;
            },
            dropSession: (targetPeer, reason) => throw new InvalidOperationException($"unexpected drop on node A: {reason}"),
            log: null);

        flowB = new SmallNetPeerFlow(
            nodeId: HashFromU64(12UL),
            peers: new SmallNetPeerDirectory(),
            coordinator: new SmallNetCoordinator(),
            blockSyncClient: CreateNoopBlockSyncClient(peerB, 100UL, hash1),
            getLocalChainView: () => new SmallNetLocalChainView(1UL, hash1, 100UL, new[] { hash1 }, null),
            getCanonicalHashAtHeight: height => height == 1UL ? (byte[])hash1.Clone() : null,
            getCanonicalPayloadAtHeight: height => null,
            sendFrameAsync: (targetPeer, type, payload, ct) =>
            {
                nodeBRequested.Add((type, (byte[])payload.Clone()));
                return Task.CompletedTask;
            },
            dropSession: (targetPeer, reason) => throw new InvalidOperationException($"unexpected drop on node B: {reason}"),
            log: null);

        flowA.HandleHelloAsync(flowB.BuildLocalHelloPayload(), peerB, CancellationToken.None)
            .GetAwaiter().GetResult();
        flowA.HandleTipStateAsync(flowB.BuildLocalTipStatePayload(), peerB, CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(nodeBRequested.Count >= 1, "lagging peer should request catch-up after reconnect");
        Assert(nodeBRequested[0].type == MsgType.GetBlocksFrom, $"expected GetBlocksFrom request, got {nodeBRequested[0].type}");
        Assert(nodeBReceivedBlocks.Count == 2, $"expected 2 pushed tail blocks, got {nodeBReceivedBlocks.Count}");
        Assert(BytesEqual(nodeBReceivedBlocks[0], chainAPayloads[2UL]), "reconnect tail push first block mismatch");
        Assert(BytesEqual(nodeBReceivedBlocks[1], chainAPayloads[3UL]), "reconnect tail push second block mismatch");
    }

    private static void TestSmallNetPeerFlowCompetingTipRequestsAncestorPack()
    {
        var peer = CreateFakePeer("peer-competing-tip");
        byte[] localTip = HashFromU64(401UL);
        byte[] remoteTip = HashFromU64(402UL);
        byte[] sharedRecent = HashFromU64(399UL);
        var sent = new List<(MsgType type, byte[] payload)>();

        var flow = new SmallNetPeerFlow(
            nodeId: HashFromU64(13UL),
            peers: new SmallNetPeerDirectory(),
            coordinator: new SmallNetCoordinator(),
            blockSyncClient: CreateNoopBlockSyncClient(peer, 800UL, localTip),
            getLocalChainView: () => new SmallNetLocalChainView(20UL, localTip, 800UL, new[] { sharedRecent, localTip }, null),
            getCanonicalHashAtHeight: _ => null,
            getCanonicalPayloadAtHeight: _ => null,
            sendFrameAsync: (targetPeer, type, payload, ct) =>
            {
                sent.Add((type, (byte[])payload.Clone()));
                return Task.CompletedTask;
            },
            dropSession: (targetPeer, reason) => throw new InvalidOperationException($"unexpected drop: {reason}"),
            log: null);

        var competingTip = new SmallNetTipStateFrame(20UL, remoteTip, 800UL, null, new[] { sharedRecent, remoteTip });
        flow.HandleTipStateAsync(SmallNetSyncProtocol.BuildTipState(competingTip), peer, CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1, $"expected one competing-tip recovery frame, got {sent.Count}");
        Assert(sent[0].type == MsgType.GetAncestorPack, $"expected GetAncestorPack, got {sent[0].type}");
        Assert(SmallNetSyncProtocol.TryParseGetAncestorPack(sent[0].payload, out var request), "ancestor-pack payload did not parse");
        Assert(BytesEqual(request.StartHash, remoteTip), "ancestor-pack request targeted the wrong competing tip");
    }

    private static void TestSmallNetPeerFlowActiveBlockSyncSuppressesCoordinatorCatchUpRequests()
    {
        var peer = CreateFakePeer("peer-active-sync");
        byte[] localTip = HashFromU64(500UL);
        byte[] remoteTip = HashFromU64(900UL);
        UInt128 localChainwork = 500UL;
        UInt128 remoteChainwork = 900UL;
        var clientSent = new List<(MsgType type, byte[] payload)>();
        var flowSent = new List<(MsgType type, byte[] payload)>();

        var client = new BlockSyncClient(
            sessionSnapshot: () => new[] { peer },
            sendFrameAsync: (targetPeer, type, payload, ct) =>
            {
                clientSent.Add((type, (byte[])payload.Clone()));
                return Task.CompletedTask;
            },
            getLocalTipChainwork: () => localChainwork,
            getLocalTipHash: () => (byte[])localTip.Clone(),
            prepareBatchAsync: (startHash, startHeight, advertisedTipChainwork, targetPeer, ct) =>
                Task.FromResult(new BlockSyncPrepareResult(true, string.Empty)),
            commitChunkAsync: (payloads, height, prevHash, targetPeer, ct) =>
                Task.FromResult(new BlockSyncChunkCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            log: null);

        var flow = new SmallNetPeerFlow(
            nodeId: HashFromU64(99UL),
            peers: new SmallNetPeerDirectory(),
            coordinator: new SmallNetCoordinator(),
            blockSyncClient: client,
            getLocalChainView: () => new SmallNetLocalChainView(500UL, localTip, localChainwork, new[] { HashFromU64(499UL), localTip }, null),
            getCanonicalHashAtHeight: _ => null,
            getCanonicalPayloadAtHeight: _ => null,
            sendFrameAsync: (targetPeer, type, payload, ct) =>
            {
                flowSent.Add((type, (byte[])payload.Clone()));
                return Task.CompletedTask;
            },
            dropSession: (targetPeer, reason) => throw new InvalidOperationException($"unexpected drop: {reason}"),
            log: null);

        flow.HandleTipStateAsync(
                SmallNetSyncProtocol.BuildTipState(
                    new SmallNetTipStateFrame(
                        Height: 900UL,
                        TipHash: remoteTip,
                        Chainwork: remoteChainwork,
                        StateDigest: null,
                        RecentCanonical: new[] { HashFromU64(898UL), HashFromU64(899UL), remoteTip })),
                peer,
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(clientSent.Count == 1, $"expected block sync client to emit exactly one sync request, got {clientSent.Count}");
        Assert(clientSent[0].type == MsgType.GetBlocksByLocator, $"expected client locator request, got {clientSent[0].type}");
        Assert(flowSent.Count == 0, $"coordinator catch-up requests must be suppressed while block sync is active, got {flowSent.Count}");
    }

    private static void TestSmallNetCoordinatorProducesAggressiveRecoveryActions()
    {
        var coordinator = new SmallNetCoordinator();
        byte[] localTip = HashFromU64(300UL);
        byte[] remoteTip = HashFromU64(301UL);
        byte[] sharedRecent = HashFromU64(299UL);
        var local = new SmallNetLocalChainView(
            Height: 100UL,
            TipHash: localTip,
            Chainwork: 10_000UL,
            RecentCanonical: new[] { sharedRecent, localTip },
            StateDigest: null);

        var peer = new SmallNetPeerState("peer-coordinator");
        peer.UpdateTipState(
            new SmallNetTipStateFrame(
                Height: 103UL,
                TipHash: remoteTip,
                Chainwork: 10_300UL,
                StateDigest: null,
                RecentCanonical: new[] { sharedRecent, remoteTip }),
            DateTime.UtcNow);

        var actions = coordinator.OnPeerTipState(peer, local);
        Assert(actions.Count == 1, $"expected one catch-up action, got {actions.Count}");
        Assert(actions[0].Kind == SmallNetActionKind.RequestBlocksFrom, $"unexpected action kind: {actions[0].Kind}");
        Assert(actions[0].MessageType == MsgType.GetBlocksFrom, $"unexpected message type: {actions[0].MessageType}");
        Assert(BlockSyncProtocol.TryParseGetBlocksFrom(actions[0].Payload, out var fromHash, out var maxBlocks), "GetBlocksFrom payload did not parse");
        Assert(BytesEqual(fromHash, localTip), "small-gap catch-up must request from local tip");
        Assert(maxBlocks == 3, $"expected request window of 3 blocks, got {maxBlocks}");
    }

    private static void TestBulkSyncRuntimeCommitsChunkIntoCanonicalChain()
    {
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        using var gate = new SemaphoreSlim(1, 1);
        bool notified = false;
        var runtime = new BulkSyncRuntime(gate, mempool, () => notified = true, log: null);
        var peer = CreateFakePeer("bulk-sync-peer");

        var genesis = BlockStore.GetBlockByHeight(0) ?? throw new InvalidOperationException("missing genesis block");
        var genesisHash = genesis.BlockHash ?? throw new InvalidOperationException("missing genesis hash");
        UInt128 localChainwork = BlockIndexStore.GetChainwork(genesisHash);
        UInt128 advertisedTipChainwork = localChainwork == 0 ? 1UL : localChainwork + 1UL;

        var miner = KeyGenerator.GenerateKeypairHex();
        var block = BuildMinedBlock(
            height: 1UL,
            prevHash: genesisHash,
            timestamp: genesis.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);

        int size = BlockBinarySerializer.GetSize(block);
        var payload = new byte[size];
        _ = BlockBinarySerializer.Write(payload, block);

        var prepare = runtime.PrepareBatchAsync(genesisHash, 1UL, advertisedTipChainwork, peer, CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(prepare.Success, $"bulk sync prepare failed: {prepare.Error}");

        var commit = runtime.CommitChunkAsync(new[] { payload }, 1UL, genesisHash, peer, CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(commit.Success, $"bulk sync commit failed: {commit.Error}");
        Assert(BytesEqual(commit.LastBlockHash, block.BlockHash!), "bulk sync returned wrong last block hash");

        runtime.CompleteBatchAsync(peer, BlocksEndStatus.TipReached, CancellationToken.None)
            .GetAwaiter().GetResult();

        var canonicalHash = BlockStore.GetCanonicalHashAtHeight(1UL) ?? throw new InvalidOperationException("missing canonical hash at height 1");
        Assert(BytesEqual(canonicalHash, block.BlockHash!), "bulk sync did not install committed block as canonical");
        Assert(notified, "bulk sync finalize should notify after canonical change");

        bool gateReleased = gate.Wait(0);
        Assert(gateReleased, "bulk sync finalize did not release validation gate");
        if (gateReleased)
            gate.Release();
    }

    private static void TestBulkSyncRuntimeMoreAvailableKeepsGateAndContinuesBatch()
    {
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        using var gate = new SemaphoreSlim(1, 1);
        bool notified = false;
        var runtime = new BulkSyncRuntime(gate, mempool, () => notified = true, log: null);
        var peer = CreateFakePeer("bulk-sync-continue-peer");

        var genesis = BlockStore.GetBlockByHeight(0) ?? throw new InvalidOperationException("missing genesis block");
        var genesisHash = genesis.BlockHash ?? throw new InvalidOperationException("missing genesis hash");
        UInt128 localChainwork = BlockIndexStore.GetChainwork(genesisHash);
        UInt128 advertisedTipChainwork = localChainwork == 0 ? 2UL : localChainwork + 2UL;

        var miner = KeyGenerator.GenerateKeypairHex();
        var block1 = BuildMinedBlock(
            height: 1UL,
            prevHash: genesisHash,
            timestamp: genesis.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);
        var payload1 = SerializeBlock(block1);

        var prepare1 = runtime.PrepareBatchAsync(genesisHash, 1UL, advertisedTipChainwork, peer, CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(prepare1.Success, $"first batch prepare failed: {prepare1.Error}");

        var commit1 = runtime.CommitChunkAsync(new[] { payload1 }, 1UL, genesisHash, peer, CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(commit1.Success, $"first batch commit failed: {commit1.Error}");
        Assert(BytesEqual(commit1.LastBlockHash, block1.BlockHash!), "first continuation batch returned wrong last block hash");

        runtime.CompleteBatchAsync(peer, BlocksEndStatus.MoreAvailable, CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(!gate.Wait(0), "validation gate must remain held across more-available continuation");

        var prepare2 = runtime.PrepareBatchAsync(block1.BlockHash!, 2UL, advertisedTipChainwork + 10UL, peer, CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(prepare2.Success, $"continuation batch prepare failed: {prepare2.Error}");

        var block2 = BuildLooseMinedBlock(
            height: 2UL,
            prevHash: block1.BlockHash!,
            timestamp: block1.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);
        var payload2 = SerializeBlock(block2);

        var commit2 = runtime.CommitChunkAsync(new[] { payload2 }, 2UL, block1.BlockHash!, peer, CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(commit2.Success, $"continuation batch commit failed: {commit2.Error}");
        Assert(BytesEqual(commit2.LastBlockHash, block2.BlockHash!), "continuation batch returned wrong last block hash");

        runtime.CompleteBatchAsync(peer, BlocksEndStatus.TipReached, CancellationToken.None)
            .GetAwaiter().GetResult();

        var canonicalHash1 = BlockStore.GetCanonicalHashAtHeight(1UL) ?? throw new InvalidOperationException("missing canonical hash at height 1");
        var canonicalHash2 = BlockStore.GetCanonicalHashAtHeight(2UL) ?? throw new InvalidOperationException("missing canonical hash at height 2");
        Assert(BytesEqual(canonicalHash1, block1.BlockHash!), "first continuation block not canonical");
        Assert(BytesEqual(canonicalHash2, block2.BlockHash!), "second continuation block not canonical");
        Assert(notified, "continuation sync should notify after canonical progress/finalization");

        bool gateReleased = gate.Wait(0);
        Assert(gateReleased, "validation gate must be released after final tip-reached completion");
        if (gateReleased)
            gate.Release();
    }

    private static void TestBulkSyncRuntimeAbortAfterReorgChunkRestoresPriorCanonicalChain()
    {
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        using var gate = new SemaphoreSlim(1, 1);
        int notified = 0;
        var log = new ListLogSink();
        var runtime = new BulkSyncRuntime(gate, mempool, () => notified++, log);
        var peer = CreateFakePeer("bulk-sync-abort-peer");

        var genesis = BlockStore.GetBlockByHeight(0) ?? throw new InvalidOperationException("missing genesis block");
        var genesisHash = genesis.BlockHash ?? throw new InvalidOperationException("missing genesis hash");

        var minerA = KeyGenerator.GenerateKeypairHex();
        var minerB = KeyGenerator.GenerateKeypairHex();

        var blockA1 = BuildMinedBlock(
            height: 1UL,
            prevHash: genesisHash,
            timestamp: genesis.Header!.Timestamp + 61UL,
            minerPubHex: minerA.pubHex);
        BlockPersistHelper.Persist(blockA1);

        var blockA2 = BuildMinedBlock(
            height: 2UL,
            prevHash: blockA1.BlockHash!,
            timestamp: blockA1.Header!.Timestamp + 61UL,
            minerPubHex: minerA.pubHex);
        BlockPersistHelper.Persist(blockA2);

        Assert(BlockStore.GetLatestHeight() == 2UL, "test setup must extend canonical chain to height 2");

        UInt128 localChainwork = BlockIndexStore.GetChainwork(blockA2.BlockHash!);
        UInt128 advertisedTipChainwork = localChainwork == 0 ? 1UL : localChainwork + 1UL;

        var blockB1 = BuildMinedBlock(
            height: 1UL,
            prevHash: genesisHash,
            timestamp: genesis.Header.Timestamp + 122UL,
            minerPubHex: minerB.pubHex);

        var prepare = runtime.PrepareBatchAsync(genesisHash, 1UL, advertisedTipChainwork, peer, CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(prepare.Success, $"abort-restore batch prepare failed: {prepare.Error}");

        var commit = runtime.CommitChunkAsync(new[] { SerializeBlock(blockB1) }, 1UL, genesisHash, peer, CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(commit.Success, $"abort-restore chunk commit failed: {commit.Error}");
        Assert(BytesEqual(commit.LastBlockHash, blockB1.BlockHash!), "abort-restore batch returned wrong last block hash");

        Assert(BlockStore.GetLatestHeight() == 1UL, "partial reorg chunk must temporarily shorten canonical tip");
        Assert(BytesEqual(
                BlockStore.GetCanonicalHashAtHeight(1UL) ?? throw new InvalidOperationException("missing temporary canonical hash at height 1"),
                blockB1.BlockHash!),
            "partial reorg chunk must temporarily install the new branch block");

        runtime.AbortBatchAsync(peer, "selftest abort after partial reorg", CancellationToken.None)
            .GetAwaiter().GetResult();

        var canonicalHash1 = BlockStore.GetCanonicalHashAtHeight(1UL) ?? throw new InvalidOperationException("missing canonical hash at height 1 after abort restore");
        var canonicalHash2 = BlockStore.GetCanonicalHashAtHeight(2UL) ?? throw new InvalidOperationException("missing canonical hash at height 2 after abort restore");
        Assert(BytesEqual(canonicalHash1, blockA1.BlockHash!), "abort restore must reinstall original canonical block at height 1");
        Assert(BytesEqual(canonicalHash2, blockA2.BlockHash!), "abort restore must reinstall original canonical block at height 2");
        Assert(BlockStore.GetLatestHeight() == 2UL, "abort restore must recover the original canonical tip height");

        Assert(StateStore.GetBalanceU64(minerA.pubHex) ==
               RewardCalculator.GetBlockSubsidy(1UL) + RewardCalculator.GetBlockSubsidy(2UL),
            "abort restore must recover balances from the original canonical chain");
        Assert(StateStore.GetBalanceU64(minerB.pubHex) == 0UL,
            "abort restore must roll back balances from the partially applied branch");

        Assert(BlockIndexStore.TryGetStatus(blockB1.BlockHash!, out var branchStatus), "alternative branch block status missing after abort restore");
        Assert(branchStatus == BlockIndexStore.StatusHaveBlockPayload,
            "partially applied branch block must remain stored as non-canonical payload after abort restore");
        Assert(BlockIndexStore.TryGetStatus(blockA1.BlockHash!, out var a1Status), "original h=1 block status missing after abort restore");
        Assert(BlockIndexStore.TryGetStatus(blockA2.BlockHash!, out var a2Status), "original h=2 block status missing after abort restore");
        Assert(a1Status == BlockIndexStore.StatusCanonicalStateValidated, "original h=1 block must be canonical again after abort restore");
        Assert(a2Status == BlockIndexStore.StatusCanonicalStateValidated, "original h=2 block must be canonical again after abort restore");

        lock (Db.Sync)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = "SELECT COUNT(1) FROM state_undo WHERE block_hash=$h;";
            cmd.Parameters.AddWithValue("$h", blockB1.BlockHash!);
            long undoRows = Convert.ToInt64(cmd.ExecuteScalar() ?? 0L);
            Assert(undoRows == 0L, "partially applied branch block undo rows must be removed after abort restore");
        }

        Assert(notified == 1, $"abort restore should notify exactly once, got {notified}");
        Assert(log.Lines.Any(line => line.Contains("abort restored prior canonical chain", StringComparison.OrdinalIgnoreCase)),
            "abort restore should emit a sync restore log line");

        bool gateReleased = gate.Wait(0);
        Assert(gateReleased, "abort restore must release the validation gate");
        if (gateReleased)
            gate.Release();
    }

    private static void TestBlockIngressFlowLiveOrphanRequestsParentAndPromotesAcceptedParent()
    {
        var peer = CreateFakePeer("peer-ingress-orphan");
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        var log = new ListLogSink();
        byte[] requestedParent = Array.Empty<byte>();
        byte[] promotedParent = Array.Empty<byte>();
        string requestSource = string.Empty;
        bool orphanStored = false;
        string invalidReason = string.Empty;

        var client = CreateNoopBlockSyncClient(peer, 0UL, new byte[32]);
        var downloadManager = new BlockDownloadManager(
            sessionSnapshot: () => new[] { peer },
            sendFrameAsync: (targetPeer, type, payload, ct) => Task.CompletedTask,
            haveBlock: _ => false,
            enqueueValidator: _ => true,
            revalidateStoredBlock: _ => { },
            log: log);
        var flow = new BlockIngressFlow(
            blockDownloadManager: downloadManager,
            blockSyncClient: client,
            mempool: mempool,
            getPeerKey: targetPeer => targetPeer.SessionKey,
            allowInboundBlock: _ => true,
            shouldRequestMissingHeaderResync: () => false,
            requestSyncNow: _ => { },
            tryStoreOrphan: (byte[] payload, Block block, PeerSession targetPeer, string peerKey, BlockIngressKind ingress, out string reason) =>
            {
                orphanStored = true;
                reason = string.Empty;
                return true;
            },
            requestParentFromPeer: (targetPeer, hash, source) =>
            {
                requestedParent = (byte[])hash.Clone();
                requestSource = source;
            },
            markStoredBlockInvalid: (_, _, reason) => invalidReason = reason,
            passesSidechainAdmission: (Block block, byte[]? canonicalTipHash, ulong prevHeight, int payloadBytes, out string reason) =>
            {
                reason = string.Empty;
                return true;
            },
            logKnownBlockAlready: _ => { },
            notifyUiAfterAcceptedBlock: () => { },
            relayValidatedBlockAsync: (_, _, _) => Task.CompletedTask,
            promoteOrphansForParentAsync: (parentHash, targetPeer, ct) =>
            {
                promotedParent = (byte[])parentHash.Clone();
                return Task.CompletedTask;
            },
            log: log);

        byte[] missingParentHash = HashFromU64(70_001UL);
        var orphanMiner = KeyGenerator.GenerateKeypairHex();
        var orphan = BuildLooseMinedBlock(
            height: 2UL,
            prevHash: missingParentHash,
            timestamp: (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            minerPubHex: orphanMiner.pubHex);
        Assert(
            BlockValidator.ValidateNetworkOrphanBlockStateless(orphan, out var orphanReason),
            $"loose orphan helper built invalid block: {orphanReason}");

        flow.HandleBlockAsync(SerializeBlock(orphan), peer, CancellationToken.None, BlockIngressKind.LivePush, enforceRateLimitOverride: null)
            .GetAwaiter().GetResult();

        Assert(orphanStored, $"live orphan should be buffered; invalid={invalidReason}; logs={string.Join(" || ", log.Lines)}");
        Assert(BytesEqual(requestedParent, missingParentHash), "live orphan must request the missing parent from the same peer");
        Assert(string.Equals(requestSource, "live-orphan-parent", StringComparison.Ordinal), $"unexpected parent request source: {requestSource}");
        Assert(downloadManager.IsOutOfPlanFallbackHash(missingParentHash), "missing parent should be armed as out-of-plan fallback");

        var genesis = BlockStore.GetBlockByHeight(0) ?? throw new InvalidOperationException("missing genesis block");
        var genesisHash = genesis.BlockHash ?? throw new InvalidOperationException("missing genesis hash");
        var parentMiner = KeyGenerator.GenerateKeypairHex();
        var parent = BuildMinedBlock(
            height: 1UL,
            prevHash: genesisHash,
            timestamp: genesis.Header!.Timestamp + 61UL,
            minerPubHex: parentMiner.pubHex);

        flow.HandleBlockAsync(SerializeBlock(parent), peer, CancellationToken.None, BlockIngressKind.LivePush, enforceRateLimitOverride: null)
            .GetAwaiter().GetResult();

        Assert(BytesEqual(promotedParent, parent.BlockHash!), "accepted parent must trigger orphan promotion callback");
    }

    private static void TestBlockIngressFlowAlreadyBufferedOrphanDoesNotReRequestParent()
    {
        var peer = CreateFakePeer("peer-ingress-orphan-duplicate");
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        var log = new ListLogSink();
        int parentRequests = 0;
        int syncRequests = 0;
        string invalidReason = string.Empty;

        var client = CreateNoopBlockSyncClient(peer, 0UL, new byte[32]);
        var downloadManager = new BlockDownloadManager(
            sessionSnapshot: () => new[] { peer },
            sendFrameAsync: (targetPeer, type, payload, ct) => Task.CompletedTask,
            haveBlock: _ => false,
            enqueueValidator: _ => true,
            revalidateStoredBlock: _ => { },
            log: log);
        var flow = new BlockIngressFlow(
            blockDownloadManager: downloadManager,
            blockSyncClient: client,
            mempool: mempool,
            getPeerKey: targetPeer => targetPeer.SessionKey,
            allowInboundBlock: _ => true,
            shouldRequestMissingHeaderResync: () => true,
            requestSyncNow: _ => syncRequests++,
            tryStoreOrphan: (byte[] payload, Block block, PeerSession targetPeer, string peerKey, BlockIngressKind ingress, out string reason) =>
            {
                reason = "already buffered";
                return true;
            },
            requestParentFromPeer: (_, _, _) => parentRequests++,
            markStoredBlockInvalid: (_, _, reason) => invalidReason = reason,
            passesSidechainAdmission: (Block block, byte[]? canonicalTipHash, ulong prevHeight, int payloadBytes, out string reason) =>
            {
                reason = string.Empty;
                return true;
            },
            logKnownBlockAlready: _ => { },
            notifyUiAfterAcceptedBlock: () => { },
            relayValidatedBlockAsync: (_, _, _) => Task.CompletedTask,
            promoteOrphansForParentAsync: (_, _, _) => Task.CompletedTask,
            log: log);

        byte[] missingParentHash = HashFromU64(80_001UL);
        var orphanMiner = KeyGenerator.GenerateKeypairHex();
        var orphan = BuildLooseMinedBlock(
            height: 2UL,
            prevHash: missingParentHash,
            timestamp: (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            minerPubHex: orphanMiner.pubHex);
        Assert(
            BlockValidator.ValidateNetworkOrphanBlockStateless(orphan, out var orphanReason),
            $"loose duplicate-orphan helper built invalid block: {orphanReason}");

        flow.HandleBlockAsync(SerializeBlock(orphan), peer, CancellationToken.None, BlockIngressKind.Recovery, enforceRateLimitOverride: false)
            .GetAwaiter().GetResult();

        Assert(parentRequests == 0, "already buffered orphan must not request the parent again");
        Assert(syncRequests == 0, "already buffered orphan must not request another resync");
        Assert(string.IsNullOrEmpty(invalidReason), $"duplicate orphan path should stay non-invalid, got: {invalidReason}");
        Assert(!downloadManager.IsOutOfPlanFallbackHash(missingParentHash), "already buffered orphan must not re-arm out-of-plan fallback");
        Assert(!log.Lines.Any(line => line.Contains("buffered", StringComparison.OrdinalIgnoreCase)),
            $"duplicate orphan path must stay quiet, logs={string.Join(" || ", log.Lines)}");
    }

    private static void TestValidationWorkerPrioritizesLivePush()
    {
        var peer = CreateFakePeer("validation-priority-peer");
        var seen = new List<BlockIngressKind>(2);
        using var release = new ManualResetEventSlim(false);
        using var done = new ManualResetEventSlim(false);
        using var cts = new CancellationTokenSource();
        using var worker = new ValidationWorker(
            async (item, ct) =>
            {
                release.Wait(ct);
                lock (seen)
                {
                    seen.Add(item.Ingress);
                    if (seen.Count >= 2)
                        done.Set();
                }

                await Task.CompletedTask;
            },
            log: null);

        Assert(worker.Enqueue(new ValidationWorkItem(new byte[] { 0x01 }, peer, BlockIngressKind.SyncPlan)),
            "failed to enqueue sync-plan work item");
        Assert(worker.Enqueue(new ValidationWorkItem(new byte[] { 0x02 }, peer, BlockIngressKind.LivePush)),
            "failed to enqueue live-push work item");

        worker.Start(1, cts.Token);
        release.Set();

        Assert(done.Wait(TimeSpan.FromSeconds(2)), "validation worker did not process both test items");
        lock (seen)
        {
            Assert(seen.Count == 2, $"expected 2 processed items, got {seen.Count}");
            Assert(seen[0] == BlockIngressKind.LivePush, "live push must preempt queued sync-plan work");
            Assert(seen[1] == BlockIngressKind.SyncPlan, "sync-plan work should run after live push");
        }
    }

    private static void TestBlockDownloadManagerRequestsRecoveryViaAncestorPack()
    {
        var peer = CreateFakePeer("recovery-request-peer");
        byte[] wanted = HashFromU64(777UL);
        var sent = new List<(MsgType type, byte[] payload)>();
        using var requested = new ManualResetEventSlim(false);
        using var manager = new BlockDownloadManager(
            sessionSnapshot: () => new[] { peer },
            sendFrameAsync: (p, type, payload, ct) =>
            {
                lock (sent)
                    sent.Add((type, (byte[])payload.Clone()));
                if (type == MsgType.GetAncestorPack)
                    requested.Set();
                return Task.CompletedTask;
            },
            haveBlock: _ => false,
            enqueueValidator: _ => true,
            revalidateStoredBlock: _ => { },
            log: null);

        Assert(manager.QueueOutOfPlanFallback(wanted, "selftest") == 1, "recovery hash was not queued");
        manager.Start(CancellationToken.None);
        manager.OnPeerReady(peer);

        Assert(requested.Wait(TimeSpan.FromSeconds(10)), "queued recovery hash was not requested");

        lock (sent)
        {
            Assert(sent.Count == 1, $"expected exactly one immediate request, got {sent.Count}");
            Assert(sent[0].type == MsgType.GetAncestorPack, "recovery request must use GetAncestorPack");
            Assert(SmallNetSyncProtocol.TryParseGetAncestorPack(sent[0].payload, out var frame), "ancestor-pack request did not parse");
            Assert(frame.MaxBlocks == 1, $"expected single-block recovery request, got {frame.MaxBlocks}");
            Assert(BytesEqual(frame.StartHash, wanted), "ancestor-pack request targeted the wrong hash");
        }
    }

    private static void TestBlockSyncClientPrefersHigherChainworkOverHeight()
    {
        byte[] localTip = HashFromU64(100UL);
        UInt128 localChainwork = 1_300UL;
        var sent = new List<(string peer, MsgType type, byte[] payload)>();
        var sessions = new List<PeerSession>();
        var peerTall = CreateFakePeer("peer-tall");
        var peerStrong = CreateFakePeer("peer-strong");
        sessions.Add(peerTall);
        sessions.Add(peerStrong);

        var client = new BlockSyncClient(
            sessionSnapshot: () => sessions.ToArray(),
            sendFrameAsync: (peer, type, payload, ct) =>
            {
                sent.Add((peer.SessionKey, type, (byte[])payload.Clone()));
                return Task.CompletedTask;
            },
            getLocalTipChainwork: () => localChainwork,
            getLocalTipHash: () => (byte[])localTip.Clone(),
            prepareBatchAsync: (startHash, startHeight, advertisedTipChainwork, peer, ct) =>
                Task.FromResult(new BlockSyncPrepareResult(true, string.Empty)),
            commitChunkAsync: (payloads, height, prevHash, peer, ct) =>
            {
                ulong lastHeight = height + (ulong)payloads.Count - 1UL;
                return Task.FromResult(new BlockSyncChunkCommitResult(true, HashFromU64(lastHeight), string.Empty));
            },
            completeBatchAsync: (peer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (peer, reason, ct) => Task.CompletedTask,
            log: null);

        client.OnTipStateAsync(peerTall, new SmallNetTipStateFrame(200UL, HashFromU64(200UL), 1_100UL, null, new[] { HashFromU64(199UL), HashFromU64(200UL) }), CancellationToken.None)
            .GetAwaiter().GetResult();
        client.OnTipStateAsync(peerStrong, new SmallNetTipStateFrame(150UL, HashFromU64(150UL), 1_200UL, null, new[] { HashFromU64(149UL), HashFromU64(150UL) }), CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(sent.Count == 0, $"expected no sync request while local chainwork stays ahead, got {sent.Count}");

        localChainwork = 1_000UL;
        client.OnTipStateAsync(peerTall, new SmallNetTipStateFrame(200UL, HashFromU64(200UL), 1_100UL, null, new[] { HashFromU64(199UL), HashFromU64(200UL) }), CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1, $"expected one sync request after lowering local chainwork, got {sent.Count}");
        Assert(sent[0].peer == peerStrong.SessionKey, "client must prefer the higher-chainwork peer even at lower height");
        Assert(sent[0].type == MsgType.GetBlocksByLocator, "initial sync request must use locator-based sync");
    }

    private static void TestBlockSyncClientTipStateStartsSyncWithoutLegacyGetTip()
    {
        byte[] localTip = HashFromU64(100UL);
        UInt128 localChainwork = 1_000UL;
        var sent = new List<(string peer, MsgType type, byte[] payload)>();
        var sessions = new List<PeerSession>();
        var peer = CreateFakePeer("peer-tipstate");
        sessions.Add(peer);

        var client = new BlockSyncClient(
            sessionSnapshot: () => sessions.ToArray(),
            sendFrameAsync: (targetPeer, type, payload, ct) =>
            {
                sent.Add((targetPeer.SessionKey, type, (byte[])payload.Clone()));
                return Task.CompletedTask;
            },
            getLocalTipChainwork: () => localChainwork,
            getLocalTipHash: () => (byte[])localTip.Clone(),
            prepareBatchAsync: (startHash, startHeight, advertisedTipChainwork, targetPeer, ct) =>
                Task.FromResult(new BlockSyncPrepareResult(true, string.Empty)),
            commitChunkAsync: (payloads, height, prevHash, targetPeer, ct) =>
            {
                ulong lastHeight = height + (ulong)payloads.Count - 1UL;
                return Task.FromResult(new BlockSyncChunkCommitResult(true, HashFromU64(lastHeight), string.Empty));
            },
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            log: null);

        client.OnPeerReadyAsync(peer, CancellationToken.None).GetAwaiter().GetResult();
        Assert(sent.Count == 0, $"peer-ready must not emit legacy tip requests, got {sent.Count} frame(s)");

        client.OnTipStateAsync(
                peer,
                new SmallNetTipStateFrame(
                    Height: 120UL,
                    TipHash: HashFromU64(120UL),
                    Chainwork: 1_200UL,
                    StateDigest: null,
                    RecentCanonical: new[] { HashFromU64(119UL), HashFromU64(120UL) }),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1, $"tip-state should trigger one sync request, got {sent.Count}");
        Assert(sent[0].peer == peer.SessionKey, "tip-state sync request must target the announcing peer");
        Assert(sent[0].type == MsgType.GetBlocksByLocator, "tip-state sync must start with locator-based batch request");
    }

    private static void TestBlockSyncClientDisconnectResumesFromLastCommitted()
    {
        byte[] localTip = HashFromU64(10UL);
        ulong localHeight = 10UL;
        UInt128 localChainwork = 10UL;
        var sent = new List<(string peer, MsgType type, byte[] payload)>();
        var sessions = new List<PeerSession>();
        var peer1 = CreateFakePeer("peer-1");
        var peer2 = CreateFakePeer("peer-2");
        sessions.Add(peer1);

        var client = new BlockSyncClient(
            sessionSnapshot: () => sessions.ToArray(),
            sendFrameAsync: (peer, type, payload, ct) =>
            {
                sent.Add((peer.SessionKey, type, (byte[])payload.Clone()));
                return Task.CompletedTask;
            },
            getLocalTipChainwork: () => localChainwork,
            getLocalTipHash: () => (byte[])localTip.Clone(),
            prepareBatchAsync: (startHash, startHeight, advertisedTipChainwork, peer, ct) =>
                Task.FromResult(new BlockSyncPrepareResult(true, string.Empty)),
            commitChunkAsync: (payloads, height, prevHash, peer, ct) =>
            {
                ulong lastHeight = height + (ulong)payloads.Count - 1UL;
                localHeight = lastHeight;
                localChainwork = (UInt128)lastHeight;
                localTip = HashFromU64(lastHeight + 10_000UL);
                return Task.FromResult(new BlockSyncChunkCommitResult(true, (byte[])localTip.Clone(), string.Empty));
            },
            completeBatchAsync: (peer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (peer, reason, ct) => Task.CompletedTask,
            log: null);

        byte[] startHash = (byte[])localTip.Clone();
        byte[] remoteTipHash = HashFromU64(20UL);

        client.OnTipStateAsync(peer1, new SmallNetTipStateFrame(20UL, remoteTipHash, 20UL, null, new[] { HashFromU64(19UL), remoteTipHash }), CancellationToken.None).GetAwaiter().GetResult();
        Assert(sent.Count == 1, $"expected 1 initial sync request, got {sent.Count}");
        Assert(sent[0].peer == peer1.SessionKey && sent[0].type == MsgType.GetBlocksByLocator,
            "first sync request must be locator-based");

        client.OnBlocksBatchStartAsync(peer1, new SmallNetBlocksBatchStartFrame(Guid.NewGuid(), startHash, 10UL, 2, remoteTipHash, 20UL), CancellationToken.None)
            .GetAwaiter().GetResult();
        client.OnBlocksChunkAsync(
                peer1,
                SmallNetSyncProtocol.BuildBlocksChunk(11UL, new[] { new byte[] { 0x01 } }),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(localHeight == 11UL, $"expected local height 11 after first committed sync block, got {localHeight}");

        sent.Clear();
        sessions.Clear();
        client.OnPeerDisconnectedAsync(peer1, CancellationToken.None).GetAwaiter().GetResult();

        sessions.Add(peer2);
        client.OnTipStateAsync(peer2, new SmallNetTipStateFrame(20UL, remoteTipHash, 20UL, null, new[] { HashFromU64(19UL), remoteTipHash }), CancellationToken.None).GetAwaiter().GetResult();

        Assert(sent.Count == 1, $"expected exactly one resume request, got {sent.Count}");
        Assert(sent[0].peer == peer2.SessionKey && sent[0].type == MsgType.GetBlocksFrom,
            "resume request must use GetBlocksFrom");
        Assert(BlockSyncProtocol.TryParseGetBlocksFrom(sent[0].payload, out var fromHash, out var maxBlocks),
            "resume request payload did not parse");
        Assert(BytesEqual(fromHash, localTip), "resume request did not use last committed hash");
        Assert(maxBlocks == BlockSyncProtocol.BatchMaxBlocks, "resume request max_blocks mismatch");
    }

    private static void TestBlockSyncClientMoreAvailableContinuesFromCurrentLocalTip()
    {
        byte[] localTip = HashFromU64(10UL);
        UInt128 localChainwork = 10UL;
        var sent = new List<(string peer, MsgType type, byte[] payload)>();
        var peer = CreateFakePeer("peer-more-available");
        var sessions = new List<PeerSession> { peer };

        var client = new BlockSyncClient(
            sessionSnapshot: () => sessions.ToArray(),
            sendFrameAsync: (targetPeer, type, payload, ct) =>
            {
                sent.Add((targetPeer.SessionKey, type, (byte[])payload.Clone()));
                return Task.CompletedTask;
            },
            getLocalTipChainwork: () => localChainwork,
            getLocalTipHash: () => (byte[])localTip.Clone(),
            prepareBatchAsync: (startHash, startHeight, advertisedTipChainwork, targetPeer, ct) =>
                Task.FromResult(new BlockSyncPrepareResult(true, string.Empty)),
            commitChunkAsync: (payloads, height, prevHash, targetPeer, ct) =>
            {
                localTip = HashFromU64(height + 10_000UL);
                localChainwork = height;
                return Task.FromResult(new BlockSyncChunkCommitResult(true, (byte[])localTip.Clone(), string.Empty));
            },
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            log: null);

        byte[] batchForkHash = (byte[])localTip.Clone();
        byte[] remoteTipHash = HashFromU64(25UL);

        client.OnTipStateAsync(peer, new SmallNetTipStateFrame(25UL, remoteTipHash, 25UL, null, new[] { HashFromU64(24UL), remoteTipHash }), CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(sent.Count == 1, $"expected initial locator sync request, got {sent.Count}");
        Assert(sent[0].type == MsgType.GetBlocksByLocator, $"expected locator request first, got {sent[0].type}");

        sent.Clear();
        client.OnBlocksBatchStartAsync(peer, new SmallNetBlocksBatchStartFrame(Guid.NewGuid(), batchForkHash, 10UL, 1, remoteTipHash, 25UL), CancellationToken.None)
            .GetAwaiter().GetResult();
        client.OnBlocksChunkAsync(
                peer,
                SmallNetSyncProtocol.BuildBlocksChunk(11UL, new[] { new byte[] { 0x01 } }),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        byte[] committedTip = (byte[])localTip.Clone();
        localTip = HashFromU64(12_000UL);
        localChainwork = 12UL;

        client.OnBlocksBatchEndAsync(
                peer,
                new SmallNetBlocksBatchEndFrame(Guid.NewGuid(), committedTip, 11UL, MoreAvailable: true),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1, $"expected one continuation request after more-available, got {sent.Count}");
        Assert(sent[0].type == MsgType.GetBlocksFrom, $"expected GetBlocksFrom continuation, got {sent[0].type}");
        Assert(BlockSyncProtocol.TryParseGetBlocksFrom(sent[0].payload, out var fromHash, out var maxBlocks),
            "continuation request payload did not parse");
        Assert(BytesEqual(fromHash, localTip), "continuation request must use the current local canonical tip");
        Assert(!BytesEqual(fromHash, committedTip), "continuation request must not reuse the stale batch end hash when local tip advanced");
        Assert(maxBlocks == BlockSyncProtocol.BatchMaxBlocks, "continuation request max_blocks mismatch");
    }

    private static void TestBlockSyncClientInvalidBlockPenalizesAndFallsBack()
    {
        byte[] localTip = HashFromU64(10UL);
        UInt128 localChainwork = 10UL;
        var sent = new List<(string peer, MsgType type, byte[] payload)>();
        var penalized = new List<string>();
        var sessions = new List<PeerSession>();
        var peer1 = CreateFakePeer("peer-1");
        var peer2 = CreateFakePeer("peer-2");
        sessions.Add(peer1);
        sessions.Add(peer2);

        var client = new BlockSyncClient(
            sessionSnapshot: () => sessions.ToArray(),
            sendFrameAsync: (peer, type, payload, ct) =>
            {
                sent.Add((peer.SessionKey, type, (byte[])payload.Clone()));
                return Task.CompletedTask;
            },
            getLocalTipChainwork: () => localChainwork,
            getLocalTipHash: () => (byte[])localTip.Clone(),
            prepareBatchAsync: (startHash, startHeight, advertisedTipChainwork, peer, ct) =>
                Task.FromResult(new BlockSyncPrepareResult(true, string.Empty)),
            commitChunkAsync: (payloads, height, prevHash, peer, ct) =>
                Task.FromResult(new BlockSyncChunkCommitResult(false, Array.Empty<byte>(), "invalid block")),
            completeBatchAsync: (peer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (peer, reason, ct) => Task.CompletedTask,
            penalizePeer: (peer, reason) => penalized.Add(peer.SessionKey),
            log: null);

        byte[] startHash = (byte[])localTip.Clone();
        byte[] remoteTipHash = HashFromU64(25UL);
        var tipState = new SmallNetTipStateFrame(25UL, remoteTipHash, 25UL, null, new[] { HashFromU64(24UL), remoteTipHash });

        client.OnTipStateAsync(peer1, tipState, CancellationToken.None).GetAwaiter().GetResult();
        client.OnTipStateAsync(peer2, tipState, CancellationToken.None).GetAwaiter().GetResult();
        client.OnBlocksBatchStartAsync(peer1, new SmallNetBlocksBatchStartFrame(Guid.NewGuid(), startHash, 10UL, 1, remoteTipHash, 25UL), CancellationToken.None)
            .GetAwaiter().GetResult();

        sent.Clear();
        client.OnBlocksChunkAsync(
                peer1,
                SmallNetSyncProtocol.BuildBlocksChunk(11UL, new[] { new byte[] { 0xFF } }),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(penalized.Count == 1 && penalized[0] == peer1.SessionKey, "invalid block must penalize the active peer");
        Assert(sent.Any(frame => frame.peer == peer2.SessionKey && frame.type == MsgType.GetBlocksByLocator),
            "client must fall back to locator sync on another peer after invalid block");
    }

    private static Transaction BuildCoinbase(string minerPubHex, ulong amount)
        => new()
        {
            ChainId = NetworkParams.ChainId,
            Sender = new byte[32],
            Recipient = Convert.FromHexString(minerPubHex),
            Amount = amount,
            Fee = 0UL,
            TxNonce = 0UL,
            Signature = Array.Empty<byte>()
        };

    private static Transaction BuildSignedTx(
        string senderPrivHex,
        string senderPubHex,
        string recipientPubHex,
        ulong amount,
        ulong fee,
        ulong nonce)
    {
        var tx = new Transaction
        {
            ChainId = NetworkParams.ChainId,
            Sender = Convert.FromHexString(senderPubHex),
            Recipient = Convert.FromHexString(recipientPubHex),
            Amount = amount,
            Fee = fee,
            TxNonce = nonce,
            Signature = Array.Empty<byte>()
        };

        using var key = Key.Import(SignatureAlgorithm.Ed25519, Convert.FromHexString(senderPrivHex), KeyBlobFormat.RawPrivateKey);
        tx.Signature = SignatureAlgorithm.Ed25519.Sign(key, tx.ToHashBytes());
        return tx;
    }

    private static Block BuildBlock(ulong height, byte[] prevHash, string minerPubHex, IEnumerable<Transaction> txs)
    {
        var header = new BlockHeader
        {
            Version = 1,
            PreviousBlockHash = (byte[])prevHash.Clone(),
            MerkleRoot = new byte[32],
            Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            Target = Difficulty.PowLimit.ToArray(),
            Nonce = 0,
            Miner = Convert.FromHexString(minerPubHex)
        };

        var block = new Block
        {
            BlockHeight = height,
            Header = header,
            Transactions = new List<Transaction>(txs),
            BlockHash = new byte[32]
        };

        block.RecomputeAndSetMerkleRoot();
        return block;
    }

    private static Block BuildMinedBlock(
        ulong height,
        byte[] prevHash,
        ulong timestamp,
        string minerPubHex,
        IEnumerable<Transaction>? txs = null)
    {
        txs ??= Array.Empty<Transaction>();

        ulong totalFees = 0;
        var list = new List<Transaction>();
        foreach (var tx in txs)
        {
            if (tx == null) continue;
            if (TransactionValidator.IsCoinbase(tx))
                throw new InvalidOperationException("non-coinbase tx list contains coinbase");

            checked { totalFees += tx.Fee; }
            list.Add(tx);
        }

        ulong subsidy = RewardCalculator.GetBlockSubsidy(height);
        ulong coinbaseAmount = checked(subsidy + totalFees);

        var blockTxs = new List<Transaction>(1 + list.Count)
        {
            BuildCoinbase(minerPubHex, coinbaseAmount)
        };
        blockTxs.AddRange(list);

        var target = ComputeExpectedTargetForHeight(height, prevHash);

        var block = new Block
        {
            BlockHeight = height,
            Header = new BlockHeader
            {
                Version = 1,
                PreviousBlockHash = (byte[])prevHash.Clone(),
                MerkleRoot = new byte[32],
                Timestamp = timestamp,
                Target = target,
                Nonce = 0,
                Miner = Convert.FromHexString(minerPubHex)
            },
            Transactions = blockTxs,
            BlockHash = new byte[32]
        };

        block.RecomputeAndSetMerkleRoot();
        MineBlockInPlace(block);

        if (!BlockValidator.ValidateNetworkSideBlockStateless(block, out var reason))
            throw new InvalidOperationException($"built block failed validation at h={height}: {reason}");

        return block;
    }

    private static Block BuildLooseMinedBlock(
        ulong height,
        byte[] prevHash,
        ulong timestamp,
        string minerPubHex,
        IEnumerable<Transaction>? txs = null)
    {
        txs ??= Array.Empty<Transaction>();

        ulong totalFees = 0;
        var list = new List<Transaction>();
        foreach (var tx in txs)
        {
            if (tx == null) continue;
            if (TransactionValidator.IsCoinbase(tx))
                throw new InvalidOperationException("non-coinbase tx list contains coinbase");

            checked { totalFees += tx.Fee; }
            list.Add(tx);
        }

        ulong subsidy = RewardCalculator.GetBlockSubsidy(height);
        ulong coinbaseAmount = checked(subsidy + totalFees);
        var blockTxs = new List<Transaction>(1 + list.Count)
        {
            BuildCoinbase(minerPubHex, coinbaseAmount)
        };
        blockTxs.AddRange(list);

        var block = new Block
        {
            BlockHeight = height,
            Header = new BlockHeader
            {
                Version = 1,
                PreviousBlockHash = (byte[])prevHash.Clone(),
                MerkleRoot = new byte[32],
                Timestamp = timestamp,
                Target = (byte[])SelfTestEasyTarget.Clone(),
                Nonce = 0,
                Miner = Convert.FromHexString(minerPubHex)
            },
            Transactions = blockTxs,
            BlockHash = new byte[32]
        };

        block.RecomputeAndSetMerkleRoot();
        MineBlockInPlace(block);
        return block;
    }

    private static byte[] SerializeBlock(Block block)
    {
        int size = BlockBinarySerializer.GetSize(block);
        var payload = new byte[size];
        _ = BlockBinarySerializer.Write(payload, block);
        return payload;
    }

    private static byte[] ComputeExpectedTargetForHeight(ulong nextHeight, byte[] prevHash)
    {
        if (nextHeight == 0)
            return Difficulty.PowLimit.ToArray();

        if (prevHash is not { Length: 32 })
            throw new InvalidOperationException("prevHash must be 32 bytes");

        var byHeight = new Dictionary<ulong, Block>();
        byte[] walk = (byte[])prevHash.Clone();

        for (int guard = 0; guard < 1_000_000; guard++)
        {
            var b = BlockStore.GetBlockByHash(walk);
            if (b == null)
                throw new InvalidOperationException("cannot compute target: missing prev chain payload");

            byHeight[b.BlockHeight] = b;
            if (b.BlockHeight == 0) break;

            var ph = b.Header?.PreviousBlockHash;
            if (ph is not { Length: 32 })
                throw new InvalidOperationException("cannot compute target: missing previous hash in ancestor");

            walk = ph;
        }

        return DifficultyCalculator.GetNextTarget(nextHeight, h => byHeight.TryGetValue(h, out var b) ? b : null);
    }

    private static void MineBlockInPlace(Block block)
    {
        const int NonceBatchSize = 4096;
        var target = block.Header?.Target ?? throw new InvalidOperationException("target missing");
        var header = block.Header!;
        int workers = Math.Max(1, Environment.ProcessorCount);
        int found = 0;
        ulong winningNonce = 0;
        byte[]? winningHash = null;
        object winnerGate = new();

        Parallel.For(
            fromInclusive: 0,
            toExclusive: workers,
            body: (workerIndex, state) =>
            {
                ulong nonce = (ulong)workerIndex * (ulong)NonceBatchSize;
                ulong step = (ulong)workers * (ulong)NonceBatchSize;

                while (Volatile.Read(ref found) == 0)
                {
                    ulong limit = nonce;
                    if (ulong.MaxValue - limit < (ulong)NonceBatchSize - 1UL)
                        limit = ulong.MaxValue - ((ulong)NonceBatchSize - 1UL);

                    for (ulong candidate = nonce; candidate <= limit; candidate++)
                    {
                        if (Volatile.Read(ref found) != 0)
                        {
                            state.Stop();
                            return;
                        }

                        var hash = Blake3Util.Hash(header.ToHashBytesWithNonce(candidate));
                        if (!Difficulty.Meets(hash, target))
                            continue;

                        lock (winnerGate)
                        {
                            if (winningHash == null)
                            {
                                winningNonce = candidate;
                                winningHash = hash;
                                Volatile.Write(ref found, 1);
                            }
                        }

                        state.Stop();
                        return;
                    }

                    if (ulong.MaxValue - nonce < step)
                        break;

                    nonce += step;
                }
            });

        if (winningHash is not { Length: 32 })
            throw new InvalidOperationException("mining failed: nonce exhausted");

        header.Nonce = winningNonce;
        block.BlockHash = winningHash;
    }

    private static void SeedAccount(string addrHex, ulong balance, ulong nonce)
    {
        lock (Db.Sync)
        {
            using var tx = Db.Connection.BeginTransaction();
            StateStore.Set(addrHex, balance, nonce, tx);
            tx.Commit();
        }
    }

    private static bool MempoolContainsTx(MempoolManager mempool, Transaction tx)
    {
        var wanted = tx.ComputeTransactionHash();
        return mempool.GetAll().Any(t => t != null && t.ComputeTransactionHash().SequenceEqual(wanted));
    }

    private static bool BytesEqual(byte[] a, byte[] b)
    {
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++)
            if (a[i] != b[i]) return false;
        return true;
    }

    private static int GetFreeTcpPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private static JsonDocument GetJson(HttpClient client, string path)
    {
        using var response = client.GetAsync(path).GetAwaiter().GetResult();
        string body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException($"GET {path} failed with {(int)response.StatusCode}: {body}");

        return JsonDocument.Parse(body);
    }

    private static JsonDocument PostJson(HttpClient client, string path, object payload)
    {
        string json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var response = client.PostAsync(path, content).GetAwaiter().GetResult();
        string body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException($"POST {path} failed with {(int)response.StatusCode}: {body}");

        return JsonDocument.Parse(body);
    }

    private static BlockSyncClient CreateNoopBlockSyncClient(PeerSession peer, UInt128 localTipChainwork, byte[] localTipHash)
        => new(
            sessionSnapshot: () => new[] { peer },
            sendFrameAsync: (targetPeer, type, payload, ct) => Task.CompletedTask,
            getLocalTipChainwork: () => localTipChainwork,
            getLocalTipHash: () => (byte[])localTipHash.Clone(),
            prepareBatchAsync: (startHash, startHeight, advertisedTipChainwork, targetPeer, ct) =>
                Task.FromResult(new BlockSyncPrepareResult(true, string.Empty)),
            commitChunkAsync: (payloads, height, prevHash, targetPeer, ct) =>
                Task.FromResult(new BlockSyncChunkCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            log: null);

    private static PeerSession CreateFakePeer(string key)
        => new()
        {
            Client = null!,
            Stream = null!,
            RemoteEndpoint = key,
            RemoteBanKey = key,
            HandshakeOk = true,
            IsInbound = false
        };

    private static byte[] HashFromU64(ulong value)
    {
        Span<byte> buf = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(buf, value);
        return Blake3Util.Hash(buf.ToArray());
    }

    private static void Assert(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }

    private static void TraceTestStep(string message)
    {
        string? trace = Environment.GetEnvironmentVariable("QADO_SELFTEST_TRACE");
        if (!string.Equals(trace, "1", StringComparison.Ordinal))
            return;

        string line = $"[TRACE] {DateTime.UtcNow:O} {message}{Environment.NewLine}";
        Console.Write(line);
        Console.Out.Flush();

        try
        {
            File.AppendAllText(Path.Combine(Environment.CurrentDirectory, "selftest_trace_steps.log"), line);
        }
        catch
        {
        }
    }

    private static byte[] HashLeaf(byte[] leaf)
    {
        byte[] buf = new byte[1 + 32];
        buf[0] = MerkleLeafTag;
        Buffer.BlockCopy(leaf, 0, buf, 1, 32);
        return Blake3Util.Hash(buf);
    }

    private static byte[] HashNode(byte[] left, byte[] right)
    {
        byte[] buf = new byte[1 + 32 + 32];
        buf[0] = MerkleNodeTag;
        Buffer.BlockCopy(left, 0, buf, 1, 32);
        Buffer.BlockCopy(right, 0, buf, 1 + 32, 32);
        return Blake3Util.Hash(buf);
    }
}

