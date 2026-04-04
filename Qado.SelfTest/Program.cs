using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
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
        new TestCase("ChainSelector_TipExtensionFastPath_AdoptsWithoutDeepAncestorMetadata", TestChainSelectorTipExtensionFastPathAdoptsWithoutDeepAncestorMetadata),
        new TestCase("ChainSelector_TipExtensionFastPath_UsesPrefetchedBlocksWithoutPayloadReload", TestChainSelectorTipExtensionFastPathUsesPrefetchedBlocksWithoutPayloadReload),
        new TestCase("MempoolSelection_RespectsSenderNonceOrder", TestMempoolSelectionRespectsSenderNonceOrder),
        new TestCase("PendingTransactionBuffer_RejectsExhaustedSenderNonce", TestPendingTransactionBufferRejectsExhaustedSenderNonce),
        new TestCase("BlockLocator_FindsForkpoint", TestBlockLocatorFindsForkpoint),
        new TestCase("BlockSyncProtocol_Batches128Blocks", TestBlockSyncProtocolBatches128Blocks),
        new TestCase("HandshakeCapabilities_ExtendedPayloadKeepsLegacyCompatibility", TestHandshakeCapabilitiesExtendedPayloadKeepsLegacyCompatibility),
        new TestCase("P2PNode_DuplicateSessionPreference_UsesNodeIdTieBreak", TestP2PNodeDuplicateSessionPreferenceUsesNodeIdTieBreak),
        new TestCase("P2PNode_ParentPackTargets_UseSenderFirstAndBoundedBackups", TestP2PNodeParentPackTargetsUseSenderFirstAndBoundedBackups),
        new TestCase("BlockIngressFlow_PostPromotionReevaluatesBestKnownTip", TestBlockIngressFlowPostPromotionReevaluatesBestKnownTip),
        new TestCase("BlockSyncClient_ExtendedSyncWindow_Requests512ForCapablePeers", TestBlockSyncClientExtendedSyncWindowRequests512ForCapablePeers),
        new TestCase("BlockSyncClient_SyncWindowPreview_PipelinesNextPeerBeforeBatchEnd", TestBlockSyncClientSyncWindowPreviewPipelinesNextPeerBeforeBatchEnd),
        new TestCase("BlockSyncClient_FromHashContinuation_DoesNotRequireImmediatePrepare", TestBlockSyncClientFromHashContinuationDoesNotRequireImmediatePrepare),
        new TestCase("BlockSyncClient_EqualChainworkPrepare_SkipsWithoutPenalty", TestBlockSyncClientEqualChainworkPrepareSkipsWithoutPenalty),
        new TestCase("BlockSyncClient_CompletedContinuation_WaitsForEarlierPreparedWindow", TestBlockSyncClientCompletedContinuationWaitsForEarlierPreparedWindow),
        new TestCase("BlockSyncClient_PreviewContinuation_RetriesAfterCommitWhenNoPeerWasInitiallyFree", TestBlockSyncClientPreviewContinuationRetriesAfterCommitWhenNoPeerWasInitiallyFree),
        new TestCase("BlockSyncClient_IdleBehindWatchdog_RevivesLocatorSync", TestBlockSyncClientIdleBehindWatchdogRevivesLocatorSync),
        new TestCase("BlockSyncClient_ActiveStallWatchdog_RevivesLocatorSync", TestBlockSyncClientActiveStallWatchdogRevivesLocatorSync),
        new TestCase("BlockSyncClient_ResetPipeline_ClearsCommitLoopLatch", TestBlockSyncClientResetPipelineClearsCommitLoopLatch),
        new TestCase("BlockSyncClient_ResumeHash_PrefersLocalTipWhenCommittedCursorIsStale", TestBlockSyncClientResumeHashPrefersLocalTipWhenCommittedCursorIsStale),
        new TestCase("PeerDiscovery_HandlePeersPayload_SkipsIpv6WhenDisabled_AndNormalizesMappedIpv4", TestPeerDiscoveryHandlePeersPayloadSkipsIpv6WhenDisabledAndNormalizesMappedIpv4),
        new TestCase("PeerDiscovery_BuildPeersPayload_LegacyPeerExcludesIpv6", TestPeerDiscoveryBuildPeersPayloadLegacyPeerExcludesIpv6),
        new TestCase("PeerDiscovery_BuildPeersPayload_CapablePeerSkipsIpv6WhenDisabled", TestPeerDiscoveryBuildPeersPayloadCapablePeerSkipsIpv6WhenDisabled),
        new TestCase("PeerDiscovery_BuildPeersPayload_MixesVerifiedAndUnverifiedRoutables", TestPeerDiscoveryBuildPeersPayloadMixesVerifiedAndUnverifiedRoutables),
        new TestCase("PeerAddress_OrderDialAddresses_SkipsIpv6WhenDisabled", TestPeerAddressOrderDialAddressesSkipsIpv6WhenDisabled),
        new TestCase("BlockSyncServer_UsesCapabilityGatedBatchFrames", TestBlockSyncServerUsesCapabilityGatedBatchFrames),
        new TestCase("SmallNetSyncProtocol_RoundTripsCoreFrames", TestSmallNetSyncProtocolRoundTripsCoreFrames),
        new TestCase("SmallNetPeerState_ClassifiesGapPragmatically", TestSmallNetPeerStateClassifiesGapPragmatically),
        new TestCase("SmallNetPeerFlow_PushesCanonicalTailToLaggingPeer", TestSmallNetPeerFlowPushesCanonicalTailToLaggingPeer),
        new TestCase("SmallNetPeerFlow_ReconnectHandshakePushesTail", TestSmallNetPeerFlowReconnectHandshakePushesTail),
        new TestCase("SmallNetPeerFlow_CompetingTipRequestsAncestorPack", TestSmallNetPeerFlowCompetingTipRequestsAncestorPack),
        new TestCase("SmallNetPeerFlow_ActiveBlockSyncSuppressesCoordinatorCatchUpRequests", TestSmallNetPeerFlowActiveBlockSyncSuppressesCoordinatorCatchUpRequests),
        new TestCase("SmallNetCoordinator_ProducesAggressiveRecoveryActions", TestSmallNetCoordinatorProducesAggressiveRecoveryActions),
        new TestCase("BulkSyncRuntime_CommitsBatchAndAdoptsCanonicalChain", TestBulkSyncRuntimeCommitsBatchAndAdoptsCanonicalChain),
        new TestCase("BulkSyncRuntime_MoreAvailable_ReleasesGateAndContinuesBatch", TestBulkSyncRuntimeMoreAvailableReleasesGateAndContinuesBatch),
        new TestCase("BulkSyncRuntime_ReusesRollingChainWindowAcrossContinuationSlices", TestBulkSyncRuntimeReusesRollingChainWindowAcrossContinuationSlices),
        new TestCase("BulkSyncRuntime_AbortLeavesStoredSidechainAndCanonicalUntouched", TestBulkSyncRuntimeAbortLeavesStoredSidechainAndCanonicalUntouched),
        new TestCase("BlockIngressFlow_LiveOrphanRequestsParentAndPromotesAcceptedParent", TestBlockIngressFlowLiveOrphanRequestsParentAndPromotesAcceptedParent),
        new TestCase("BlockIngressFlow_AlreadyBufferedOrphanDoesNotReRequestParent", TestBlockIngressFlowAlreadyBufferedOrphanDoesNotReRequestParent),
        new TestCase("BlockIngressFlow_LiveUnknownParentDefersUntilRecoveryArmed", TestBlockIngressFlowLiveUnknownParentDefersUntilRecoveryArmed),
        new TestCase("BlockIngressFlow_LiveFarBehindKnownParentDefersWithoutPersist", TestBlockIngressFlowLiveFarBehindKnownParentDefersWithoutPersist),
        new TestCase("BlockIngressFlow_RecoveryOrphanDuringActiveSyncKeepsBoundedSidePathRecovery", TestBlockIngressFlowRecoveryOrphanDuringActiveSyncKeepsBoundedSidePathRecovery),
        new TestCase("ValidationWorker_PrioritizesLivePush", TestValidationWorkerPrioritizesLivePush),
        new TestCase("BlockDownloadManager_RequestsRecoveryViaAncestorPack", TestBlockDownloadManagerRequestsRecoveryViaAncestorPack),
        new TestCase("BlockDownloadManager_ParallelizesRecoveryAcrossPeers", TestBlockDownloadManagerParallelizesRecoveryAcrossPeers),
        new TestCase("BlockSyncClient_TipStateStartsSyncWithoutLegacyGetTip", TestBlockSyncClientTipStateStartsSyncWithoutLegacyGetTip),
        new TestCase("BlockSyncClient_PrefersHigherChainworkOverHeight", TestBlockSyncClientPrefersHigherChainworkOverHeight),
        new TestCase("BlockSyncClient_ReconnectedPeerInheritsCooldownByBanKey", TestBlockSyncClientReconnectedPeerInheritsCooldownByBanKey),
        new TestCase("BlockSyncClient_ReconnectedPeerRetainsFailureBiasAfterCooldown", TestBlockSyncClientReconnectedPeerRetainsFailureBiasAfterCooldown),
        new TestCase("BlockSyncClient_MoreAvailable_ContinuesFromLastCommittedSyncPoint", TestBlockSyncClientMoreAvailableContinuesFromLastCommittedSyncPoint),
        new TestCase("BlockSyncClient_RemoteTipLatch_SuppressesDuplicateFinishContinuation", TestBlockSyncClientRemoteTipLatchSuppressesDuplicateFinishContinuation),
        new TestCase("BlockSyncClient_StaleBatchEndBeforeStart_IsIgnored", TestBlockSyncClientStaleBatchEndBeforeStartIsIgnored),
        new TestCase("BlockSyncClient_DisconnectDuringPrepare_DoesNotLeaveStaleBulkSyncBatch", TestBlockSyncClientDisconnectDuringPrepareDoesNotLeaveStaleBulkSyncBatch),
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

    private static void TestChainSelectorTipExtensionFastPathAdoptsWithoutDeepAncestorMetadata()
    {
        var canonical = BuildCanonicalChain(2UL);
        var canonTip = canonical[^1];
        var missingAncestor = canonical[0];
        var miner = KeyGenerator.GenerateKeypairHex();

        var block3 = BuildMinedBlock(
            height: 3UL,
            prevHash: canonTip.BlockHash!,
            timestamp: canonTip.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);

        lock (Db.Sync)
        {
            using var tx = Db.Connection.BeginTransaction();
            BlockStore.SaveBlock(block3, tx, BlockIndexStore.StatusHaveBlockPayload);
            tx.Commit();
        }

        lock (Db.Sync)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = "DELETE FROM block_index WHERE hash=$h;";
            cmd.Parameters.AddWithValue("$h", missingAncestor.BlockHash!);
            int deleted = cmd.ExecuteNonQuery();
            Assert(deleted == 1, $"test setup must remove exactly one deep ancestor metadata row, got {deleted}");
        }

        ChainSelector.MaybeAdoptNewTip(block3.BlockHash!, log: null, mempool: null);

        Assert(BlockStore.GetLatestHeight() == 3UL, "tip-extension fast path must adopt the appended block");
        var canonH3 = BlockStore.GetCanonicalHashAtHeight(3UL) ?? throw new InvalidOperationException("missing canonical hash at height 3");
        Assert(BytesEqual(canonH3, block3.BlockHash!), "appended block must become canonical even when deep ancestor metadata is missing");
    }

    private static void TestChainSelectorTipExtensionFastPathUsesPrefetchedBlocksWithoutPayloadReload()
    {
        var canonical = BuildCanonicalChain(2UL);
        var canonTip = canonical[^1];
        var miner = KeyGenerator.GenerateKeypairHex();

        var block3 = BuildMinedBlock(
            height: 3UL,
            prevHash: canonTip.BlockHash!,
            timestamp: canonTip.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);

        lock (Db.Sync)
        {
            using var tx = Db.Connection.BeginTransaction();
            BlockStore.SaveBlock(block3, tx, BlockIndexStore.StatusHaveBlockPayload);
            tx.Commit();
        }

        lock (Db.Sync)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = "DELETE FROM block_payloads WHERE hash=$h;";
            cmd.Parameters.AddWithValue("$h", block3.BlockHash!);
            int deleted = cmd.ExecuteNonQuery();
            Assert(deleted == 1, $"test setup must remove exactly one candidate payload row, got {deleted}");
        }

        ChainSelector.MaybeAdoptNewTip(block3.BlockHash!, new[] { block3 }, log: null, mempool: null);

        Assert(BlockStore.GetLatestHeight() == 3UL, "tip-extension fast path must adopt appended block from prefetched objects");
        var canonH3 = BlockStore.GetCanonicalHashAtHeight(3UL) ?? throw new InvalidOperationException("missing canonical hash at height 3");
        Assert(BytesEqual(canonH3, block3.BlockHash!), "prefetched appended block must become canonical without payload reload");
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

    private static void TestBlockSyncProtocolBatches128Blocks()
    {
        var blocks = new List<byte[]>(4096);
        for (int i = 0; i < 4096; i++)
        {
            var blob = new byte[96];
            blob[0] = (byte)i;
            blob[95] = (byte)(255 - (i & 0xFF));
            blocks.Add(blob);
        }

        ulong expectedHeight = 777UL;
        int expectedPayloads = 0;
        int totalBlocks = 0;
        for (int offset = 0; offset < blocks.Count; offset += BlockSyncProtocol.BatchMaxBlocks)
        {
            int take = Math.Min(BlockSyncProtocol.BatchMaxBlocks, blocks.Count - offset);
            var slice = new byte[take][];
            for (int i = 0; i < take; i++)
                slice[i] = blocks[offset + i];

            byte[] payload = SmallNetSyncProtocol.BuildBlocksBatchData(expectedHeight, slice);
            Assert(SmallNetSyncProtocol.TryParseBlocksBatchData(payload, out var frame), $"batch {expectedPayloads} did not parse");
            Assert(frame.FirstHeight == expectedHeight, $"batch {expectedPayloads} first height mismatch");
            Assert(frame.Blocks.Count > 0 && frame.Blocks.Count <= BlockSyncProtocol.BatchMaxBlocks,
                $"batch {expectedPayloads} block count out of range");

            totalBlocks += frame.Blocks.Count;
            expectedHeight += (ulong)frame.Blocks.Count;
            expectedPayloads++;
        }

        Assert(expectedPayloads == 32, $"expected 32 batch payloads, got {expectedPayloads}");
        Assert(totalBlocks == 4096, $"expected 4096 batched blocks, got {totalBlocks}");
    }

    private static void TestHandshakeCapabilitiesExtendedPayloadKeepsLegacyCompatibility()
    {
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        var node = new P2PNode(mempool, log: null);

        try
        {
            MethodInfo buildHandshake = typeof(P2PNode).GetMethod("BuildHandshakePayload", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("BuildHandshakePayload reflection lookup failed");
            MethodInfo parseCapabilities = typeof(P2PNode).GetMethod("ParseHandshakeCapabilities", BindingFlags.Static | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("ParseHandshakeCapabilities reflection lookup failed");

            byte[] fullPayload = (byte[])buildHandshake.Invoke(node, null)!;
            Assert(fullPayload.Length == 40, $"extended handshake payload length mismatch: {fullPayload.Length}");

            var fullCaps = (HandshakeCapabilities)parseCapabilities.Invoke(null, new object[] { fullPayload })!;
            Assert(fullCaps.HasFlag(HandshakeCapabilities.BlocksBatchData), "extended handshake must advertise batch-data capability");
            Assert(fullCaps.HasFlag(HandshakeCapabilities.PeerExchangeIpv6), "extended handshake must advertise IPv6 PEX capability");
            Assert(fullCaps.HasFlag(HandshakeCapabilities.DualStack), "extended handshake must advertise dual-stack capability");
            Assert(fullCaps.HasFlag(HandshakeCapabilities.ExtendedSyncWindow), "extended handshake must advertise extended sync-window capability");
            Assert(fullCaps.HasFlag(HandshakeCapabilities.SyncWindowPreview), "extended handshake must advertise sync-window preview capability");

            var legacyPayload = new byte[36];
            Buffer.BlockCopy(fullPayload, 0, legacyPayload, 0, legacyPayload.Length);
            var legacyCaps = (HandshakeCapabilities)parseCapabilities.Invoke(null, new object[] { legacyPayload })!;
            Assert(legacyCaps == HandshakeCapabilities.None, "legacy-length handshake must parse without optional capabilities");
        }
        finally
        {
            node.Stop();
        }
    }

    private static void TestP2PNodeDuplicateSessionPreferenceUsesNodeIdTieBreak()
    {
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        var node = new P2PNode(mempool, log: null);

        try
        {
            FieldInfo nodeIdField = typeof(P2PNode).GetField("_nodeId", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("_nodeId reflection lookup failed");
            MethodInfo selectPreferred = typeof(P2PNode).GetMethod("SelectPreferredDuplicateSession", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("SelectPreferredDuplicateSession reflection lookup failed");

            byte[] localNodeId = ((byte[])nodeIdField.GetValue(node)!).ToArray();
            Assert(localNodeId.Length == 32, "local node id must be 32 bytes");

            byte[] remoteGreater = MakeLexicographicNeighbor(localNodeId, makeGreater: true);
            byte[] remoteSmaller = MakeLexicographicNeighbor(localNodeId, makeGreater: false);

            var inbound = new PeerSession
            {
                Client = null!,
                Stream = null!,
                RemoteEndpoint = "dup-in",
                RemoteBanKey = "dup-peer",
                HandshakeOk = true,
                IsInbound = true
            };

            var outbound = new PeerSession
            {
                Client = null!,
                Stream = null!,
                RemoteEndpoint = "dup-out",
                RemoteBanKey = "dup-peer",
                HandshakeOk = true,
                IsInbound = false
            };

            var preferOutbound = (PeerSession?)selectPreferred.Invoke(node, new object[] { new[] { inbound, outbound }, remoteGreater });
            Assert(ReferenceEquals(preferOutbound, outbound),
                "when local nodeId is lexicographically smaller, outbound session should win");

            var preferInbound = (PeerSession?)selectPreferred.Invoke(node, new object[] { new[] { inbound, outbound }, remoteSmaller });
            Assert(ReferenceEquals(preferInbound, inbound),
                "when local nodeId is lexicographically greater, inbound session should win");
        }
        finally
        {
            node.Stop();
        }

        static byte[] MakeLexicographicNeighbor(byte[] source, bool makeGreater)
        {
            var copy = source.ToArray();

            if (makeGreater)
            {
                for (int i = copy.Length - 1; i >= 0; i--)
                {
                    if (copy[i] == byte.MaxValue)
                        continue;

                    copy[i]++;
                    for (int j = i + 1; j < copy.Length; j++)
                        copy[j] = 0;
                    return copy;
                }
            }
            else
            {
                for (int i = copy.Length - 1; i >= 0; i--)
                {
                    if (copy[i] == byte.MinValue)
                        continue;

                    copy[i]--;
                    for (int j = i + 1; j < copy.Length; j++)
                        copy[j] = byte.MaxValue;
                    return copy;
                }
            }

            throw new InvalidOperationException("unable to derive neighboring node id for duplicate-session test");
        }
    }

    private static void TestP2PNodeParentPackTargetsUseSenderFirstAndBoundedBackups()
    {
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        var node = new P2PNode(mempool, log: null);

        try
        {
            MethodInfo getTargets = typeof(P2PNode).GetMethod("GetParentPackRequestTargets", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("GetParentPackRequestTargets reflection lookup failed");
            FieldInfo sessionsField = typeof(P2PNode).GetField("_sessions", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("_sessions reflection lookup failed");

            var primary = CreateFakePeer("peer-primary");
            primary.LastLatencyMs = 75;

            var modernFast = CreateFakePeer(
                "peer-modern-fast",
                HandshakeCapabilities.BlocksBatchData |
                HandshakeCapabilities.ExtendedSyncWindow |
                HandshakeCapabilities.SyncWindowPreview);
            modernFast.LastLatencyMs = 10;

            var modernSlow = CreateFakePeer(
                "peer-modern-slow",
                HandshakeCapabilities.BlocksBatchData |
                HandshakeCapabilities.ExtendedSyncWindow);
            modernSlow.LastLatencyMs = 90;

            var legacyFast = CreateFakePeer("peer-legacy-fast");
            legacyFast.LastLatencyMs = 5;

            object sessions = sessionsField.GetValue(node)
                ?? throw new InvalidOperationException("_sessions value lookup failed");
            MethodInfo tryAdd = sessions.GetType().GetMethod("TryAdd")
                ?? throw new InvalidOperationException("_sessions.TryAdd reflection lookup failed");

            _ = (bool)tryAdd.Invoke(sessions, new object[] { primary.RemoteEndpoint, primary })!;
            _ = (bool)tryAdd.Invoke(sessions, new object[] { modernFast.RemoteEndpoint, modernFast })!;
            _ = (bool)tryAdd.Invoke(sessions, new object[] { modernSlow.RemoteEndpoint, modernSlow })!;
            _ = (bool)tryAdd.Invoke(sessions, new object[] { legacyFast.RemoteEndpoint, legacyFast })!;

            var targets = ((IReadOnlyList<PeerSession>)getTargets.Invoke(node, new object[] { primary, 3 })!).ToArray();
            Assert(targets.Length == 3, $"parent recovery must stay bounded to three peers, got {targets.Length}");
            Assert(ReferenceEquals(targets[0], primary), "parent recovery must keep the sender as the first target");
            Assert(ReferenceEquals(targets[1], modernFast), "capable low-latency backup should be chosen before weaker peers");
            Assert(ReferenceEquals(targets[2], modernSlow), "bounded backup set should prefer another capable peer before a legacy fallback");
        }
        finally
        {
            node.Stop();
        }
    }

    private static void TestPeerDiscoveryHandlePeersPayloadSkipsIpv6WhenDisabledAndNormalizesMappedIpv4()
    {
        const string ipv6 = "2001:4860:4860::8888";
        const string mappedIpv4 = "::ffff:93.184.216.34";
        const string normalizedIpv4 = "93.184.216.34";
        const int port6 = 18444;
        const int port4 = 18445;

        var payloadParts = new List<byte[]>();
        void AddPeer(string host, int port)
        {
            var hostBytes = Encoding.UTF8.GetBytes(host);
            var buf = new byte[1 + hostBytes.Length + 2];
            buf[0] = (byte)hostBytes.Length;
            Buffer.BlockCopy(hostBytes, 0, buf, 1, hostBytes.Length);
            BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(1 + hostBytes.Length, 2), (ushort)port);
            payloadParts.Add(buf);
        }

        AddPeer(ipv6, port6);
        AddPeer(mappedIpv4, port4);

        int totalLength = 2;
        for (int i = 0; i < payloadParts.Count; i++)
            totalLength += payloadParts[i].Length;

        var payload = new byte[totalLength];
        BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(0, 2), (ushort)payloadParts.Count);
        int offset = 2;
        for (int i = 0; i < payloadParts.Count; i++)
        {
            Buffer.BlockCopy(payloadParts[i], 0, payload, offset, payloadParts[i].Length);
            offset += payloadParts[i].Length;
        }

        PeerDiscovery.HandlePeersPayload(payload, log: null);

        lock (Db.Sync)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = "SELECT COUNT(1) FROM peers WHERE (ip=$ipv6 AND port=$port6) OR (ip=$ipv4 AND port=$port4);";
            cmd.Parameters.AddWithValue("$ipv6", IPAddress.Parse(ipv6).ToString().ToLowerInvariant());
            cmd.Parameters.AddWithValue("$port6", port6);
            cmd.Parameters.AddWithValue("$ipv4", normalizedIpv4);
            cmd.Parameters.AddWithValue("$port4", port4);
            long matches = Convert.ToInt64(cmd.ExecuteScalar() ?? 0L);
            Assert(matches == 1L, $"expected IPv6 peer to be skipped and mapped IPv4 peer to be stored, got {matches}");
        }
    }

    private static void TestPeerDiscoveryBuildPeersPayloadLegacyPeerExcludesIpv6()
    {
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        var node = new P2PNode(mempool, log: null);

        try
        {
            const string ipv4 = "93.184.216.40";
            const string ipv6 = "2001:4860:4860::8844";
            const int port4 = 18450;
            const int port6 = 18451;
            ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            PeerStore.MarkSeen(ipv4, port4, now, GenesisConfig.NetworkId);
            PeerStore.MarkSeen(ipv6, port6, now, GenesisConfig.NetworkId);
            MarkPeerPublicForTest(node, ipv4, port4);
            MarkPeerPublicForTest(node, ipv6, port6);

            var payload = PeerDiscovery.BuildPeersPayload(CreateFakePeer("legacy-pex", HandshakeCapabilities.None), maxPeers: 8);
            var peers = ParsePeersPayloadForTest(payload);

            Assert(peers.Any(p => string.Equals(p.ip, ipv4, StringComparison.OrdinalIgnoreCase) && p.port == port4),
                "legacy PEX payload must include verified IPv4 peer");
            Assert(!peers.Any(p => string.Equals(NormalizePeerHostForTest(p.ip), NormalizePeerHostForTest(ipv6), StringComparison.OrdinalIgnoreCase) && p.port == port6),
                "legacy PEX payload must exclude IPv6 peer");
        }
        finally
        {
            node.Stop();
        }
    }

    private static void TestPeerDiscoveryBuildPeersPayloadCapablePeerSkipsIpv6WhenDisabled()
    {
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        var node = new P2PNode(mempool, log: null);

        try
        {
            const string ipv4 = "93.184.216.41";
            const string ipv6 = "2001:4860:4860::8845";
            const int port4 = 18452;
            const int port6 = 18453;
            ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            PeerStore.MarkSeen(ipv4, port4, now, GenesisConfig.NetworkId);
            PeerStore.MarkSeen(ipv6, port6, now, GenesisConfig.NetworkId);
            MarkPeerPublicForTest(node, ipv4, port4);
            MarkPeerPublicForTest(node, ipv6, port6);

            var capablePeer = CreateFakePeer("capable-pex", HandshakeCapabilities.PeerExchangeIpv6);
            var payload = PeerDiscovery.BuildPeersPayload(capablePeer, maxPeers: 8);
            var peers = ParsePeersPayloadForTest(payload);

            Assert(peers.Any(p => string.Equals(p.ip, ipv4, StringComparison.OrdinalIgnoreCase) && p.port == port4),
                "capable PEX payload must include IPv4 peer");
            Assert(!peers.Any(p => string.Equals(NormalizePeerHostForTest(p.ip), NormalizePeerHostForTest(ipv6), StringComparison.OrdinalIgnoreCase) && p.port == port6),
                "capable PEX payload must skip IPv6 peer while IPv6 is disabled");
        }
        finally
        {
            node.Stop();
        }
    }

    private static void TestPeerDiscoveryBuildPeersPayloadMixesVerifiedAndUnverifiedRoutables()
    {
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        var node = new P2PNode(mempool, log: null);

        try
        {
            const string verifiedIpv4 = "93.184.216.42";
            const int verifiedPort = 18454;
            const string unverifiedIpv4 = "93.184.216.43";
            const int unverifiedPort = 18455;
            ulong now = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            PeerStore.MarkSeen(verifiedIpv4, verifiedPort, now, GenesisConfig.NetworkId);
            PeerStore.MarkSeen(unverifiedIpv4, unverifiedPort, now, GenesisConfig.NetworkId);
            MarkPeerPublicForTest(node, verifiedIpv4, verifiedPort);

            var payload = PeerDiscovery.BuildPeersPayload(CreateFakePeer("legacy-mix", HandshakeCapabilities.None), maxPeers: 8);
            var peers = ParsePeersPayloadForTest(payload);

            Assert(peers.Any(p => string.Equals(p.ip, verifiedIpv4, StringComparison.OrdinalIgnoreCase) && p.port == verifiedPort),
                "PEX payload must include verified routable peer");
            Assert(peers.Any(p => string.Equals(p.ip, unverifiedIpv4, StringComparison.OrdinalIgnoreCase) && p.port == unverifiedPort),
                "PEX payload should include at least one unverified routable peer from PeerStore");
        }
        finally
        {
            node.Stop();
        }
    }

    private static void TestBlockSyncServerUsesCapabilityGatedBatchFrames()
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

        byte[] request = BlockSyncProtocol.BuildGetBlocksFrom(oldTipHash, maxBlocks: 1);

        var batchPeer = CreateFakePeer("smallnet-sync-batch");
        batchPeer.Capabilities = HandshakeCapabilities.BlocksBatchData | HandshakeCapabilities.SyncWindowPreview;
        var batchSent = new List<(MsgType type, byte[] payload)>();
        BlockSyncServer.HandleGetBlocksFromAsync(
                request,
                batchPeer,
                (p, type, payload, ct) =>
                {
                    batchSent.Add((type, (byte[])payload.Clone()));
                    return Task.CompletedTask;
                },
                log: null,
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(batchSent.Count >= 3, $"expected at least 3 frames for capable peer, got {batchSent.Count}");
        Assert(batchSent[0].type == MsgType.BlocksBatchStart, $"expected BlocksBatchStart first, got {batchSent[0].type}");
        Assert(batchSent[1].type == MsgType.BlocksBatchData, $"expected BlocksBatchData second, got {batchSent[1].type}");
        Assert(batchSent[^1].type == MsgType.BlocksBatchEnd, $"expected BlocksBatchEnd last, got {batchSent[^1].type}");

        Assert(SmallNetSyncProtocol.TryParseBlocksBatchStart(batchSent[0].payload, out var batchStart), "batch start payload did not parse");
        Assert(batchStart.TotalBlocks == 1, $"expected 1 block in batch, got {batchStart.TotalBlocks}");
        Assert(batchStart.ForkHeight == tipHeight, "fork height mismatch");
        Assert(batchStart.PreviewLastHash is { Length: 32 }, "preview-capable peer must receive preview last hash");
        Assert(batchStart.PreviewLastHeight == tipHeight + 1UL, $"preview last height mismatch: {batchStart.PreviewLastHeight}");

        Assert(SmallNetSyncProtocol.TryParseBlocksBatchData(batchSent[1].payload, out var batchData), "blocks batch-data payload did not parse");
        Assert(batchData.Blocks.Count == 1, $"expected exactly one batch-data block, got {batchData.Blocks.Count}");

        Assert(SmallNetSyncProtocol.TryParseBlocksBatchEnd(batchSent[^1].payload, out var batchEnd), "batch end payload did not parse");
        Assert(!batchEnd.MoreAvailable, "single-block test batch should finish at tip");

        var legacyPeer = CreateFakePeer("smallnet-sync-legacy");
        var legacySent = new List<(MsgType type, byte[] payload)>();
        BlockSyncServer.HandleGetBlocksFromAsync(
                request,
                legacyPeer,
                (p, type, payload, ct) =>
                {
                    legacySent.Add((type, (byte[])payload.Clone()));
                    return Task.CompletedTask;
                },
                log: null,
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(legacySent.Count >= 3, $"expected at least 3 frames for legacy peer, got {legacySent.Count}");
        Assert(legacySent[1].type == MsgType.BlocksChunk, $"expected legacy peer to receive BlocksChunk, got {legacySent[1].type}");
        Assert(SmallNetSyncProtocol.TryParseBlocksBatchStart(legacySent[0].payload, out var legacyStart), "legacy batch start payload did not parse");
        Assert(legacyStart.PreviewLastHash == null, "legacy peer must not receive preview metadata");
        Assert(SmallNetSyncProtocol.TryParseBlocksChunk(legacySent[1].payload, out var chunk), "legacy blocks chunk payload did not parse");
        Assert(chunk.Blocks.Count == 1, $"expected exactly one legacy chunk block, got {chunk.Blocks.Count}");
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
        byte[] previewLastHash = HashFromU64(60UL);
        var batchStart = new SmallNetBlocksBatchStartFrame(
            batchId,
            HashFromU64(50UL),
            55UL,
            BlockSyncProtocol.ExtendedSyncWindowBlocks,
            tipHash,
            12345UL,
            previewLastHash,
            55UL + (ulong)BlockSyncProtocol.ExtendedSyncWindowBlocks);
        byte[] batchStartPayload = SmallNetSyncProtocol.BuildBlocksBatchStart(batchStart);
        Assert(
            SmallNetSyncProtocol.TryParseBlocksBatchStart(batchStartPayload, out var batchStartRoundTrip),
            "batch start did not roundtrip");
        Assert(batchStartRoundTrip.BatchId == batchId, "batch start batch id mismatch");
        Assert(batchStartRoundTrip.TotalBlocks == BlockSyncProtocol.ExtendedSyncWindowBlocks, "batch start total blocks mismatch");
        Assert(BytesEqual(batchStartRoundTrip.PreviewLastHash!, previewLastHash), "batch start preview hash mismatch");
        Assert(batchStartRoundTrip.PreviewLastHeight == 55UL + (ulong)BlockSyncProtocol.ExtendedSyncWindowBlocks, "batch start preview height mismatch");

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
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
                Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
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
            shouldPushBlockToPeer: (targetPeer, blockHash) => true,
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
            shouldPushBlockToPeer: (targetPeer, blockHash) => true,
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
            shouldPushBlockToPeer: (targetPeer, blockHash) => true,
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
            shouldPushBlockToPeer: (targetPeer, blockHash) => true,
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
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
                Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
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
            shouldPushBlockToPeer: (targetPeer, blockHash) => true,
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

    private static void TestBulkSyncRuntimeCommitsBatchAndAdoptsCanonicalChain()
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

        var commit = runtime.CommitBlocksAsync(new[] { payload }, 1UL, genesisHash, peer, CancellationToken.None)
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

    private static void TestBulkSyncRuntimeMoreAvailableReleasesGateAndContinuesBatch()
    {
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        using var gate = new SemaphoreSlim(1, 1);
        int notified = 0;
        var runtime = new BulkSyncRuntime(gate, mempool, () => notified++, log: null);
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

        var commit1 = runtime.CommitBlocksAsync(new[] { payload1 }, 1UL, genesisHash, peer, CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(commit1.Success, $"first batch commit failed: {commit1.Error}");
        Assert(BytesEqual(commit1.LastBlockHash, block1.BlockHash!), "first continuation batch returned wrong last block hash");

        runtime.CompleteBatchAsync(peer, BlocksEndStatus.MoreAvailable, CancellationToken.None)
            .GetAwaiter().GetResult();

        bool gateReleasedAfterFirstCommit = gate.Wait(0);
        Assert(gateReleasedAfterFirstCommit, "validation gate must be released after each committed batch");
        if (gateReleasedAfterFirstCommit)
            gate.Release();

        var prepare2 = runtime.PrepareBatchAsync(block1.BlockHash!, 2UL, advertisedTipChainwork + 10UL, peer, CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(prepare2.Success, $"continuation batch prepare failed: {prepare2.Error}");

        var block2 = BuildLooseMinedBlock(
            height: 2UL,
            prevHash: block1.BlockHash!,
            timestamp: block1.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);
        var payload2 = SerializeBlock(block2);

        var commit2 = runtime.CommitBlocksAsync(new[] { payload2 }, 2UL, block1.BlockHash!, peer, CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(commit2.Success, $"continuation batch commit failed: {commit2.Error}");
        Assert(BytesEqual(commit2.LastBlockHash, block2.BlockHash!), "continuation batch returned wrong last block hash");

        runtime.CompleteBatchAsync(peer, BlocksEndStatus.TipReached, CancellationToken.None)
            .GetAwaiter().GetResult();

        var canonicalHash1 = BlockStore.GetCanonicalHashAtHeight(1UL) ?? throw new InvalidOperationException("missing canonical hash at height 1");
        var canonicalHash2 = BlockStore.GetCanonicalHashAtHeight(2UL) ?? throw new InvalidOperationException("missing canonical hash at height 2");
        Assert(BytesEqual(canonicalHash1, block1.BlockHash!), "first continuation block not canonical");
        Assert(BytesEqual(canonicalHash2, block2.BlockHash!), "second continuation block not canonical");
        Assert(notified >= 1, "continuation sync should notify after canonical progress");

        bool gateReleased = gate.Wait(0);
        Assert(gateReleased, "validation gate must be released after final tip-reached completion");
        if (gateReleased)
            gate.Release();
    }

    private static void TestBulkSyncRuntimeReusesRollingChainWindowAcrossContinuationSlices()
    {
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        using var gate = new SemaphoreSlim(1, 1);
        var runtime = new BulkSyncRuntime(gate, mempool, () => { }, log: null);
        var peer = CreateFakePeer("bulk-sync-rolling-window-peer");

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
        Assert(prepare1.Success, $"first rolling-window prepare failed: {prepare1.Error}");

        var commit1 = runtime.CommitBlocksAsync(new[] { payload1 }, 1UL, genesisHash, peer, CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(commit1.Success, $"first rolling-window commit failed: {commit1.Error}");

        runtime.CompleteBatchAsync(peer, BlocksEndStatus.MoreAvailable, CancellationToken.None)
            .GetAwaiter().GetResult();

        lock (Db.Sync)
        {
            using var cmd = Db.Connection!.CreateCommand();
            cmd.CommandText = "DELETE FROM block_payloads WHERE hash=$h;";
            cmd.Parameters.AddWithValue("$h", block1.BlockHash!);
            _ = cmd.ExecuteNonQuery();
        }

        var prepare2 = runtime.PrepareBatchAsync(block1.BlockHash!, 2UL, advertisedTipChainwork + 10UL, peer, CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(prepare2.Success, $"second rolling-window prepare failed: {prepare2.Error}");

        var block2 = BuildLooseMinedBlock(
            height: 2UL,
            prevHash: block1.BlockHash!,
            timestamp: block1.Header!.Timestamp + 61UL,
            minerPubHex: miner.pubHex);
        var payload2 = SerializeBlock(block2);

        var commit2 = runtime.CommitBlocksAsync(new[] { payload2 }, 2UL, block1.BlockHash!, peer, CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(commit2.Success, $"rolling window should reuse cached prev block across continuation slices: {commit2.Error}");
        Assert(BytesEqual(commit2.LastBlockHash, block2.BlockHash!), "rolling-window continuation returned wrong last block hash");
    }

    private static void TestBulkSyncRuntimeAbortLeavesStoredSidechainAndCanonicalUntouched()
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
        Assert(prepare.Success, $"sidechain batch prepare failed: {prepare.Error}");

        var commit = runtime.CommitBlocksAsync(new[] { SerializeBlock(blockB1) }, 1UL, genesisHash, peer, CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(commit.Success, $"sidechain batch commit failed: {commit.Error}");
        Assert(BytesEqual(commit.LastBlockHash, blockB1.BlockHash!), "sidechain batch returned wrong last block hash");

        Assert(BlockStore.GetLatestHeight() == 2UL, "sidechain commit must not shorten canonical tip");
        Assert(BytesEqual(
                BlockStore.GetCanonicalHashAtHeight(1UL) ?? throw new InvalidOperationException("missing canonical hash at height 1"),
                blockA1.BlockHash!),
            "sidechain commit must not replace canonical height 1");

        runtime.AbortBatchAsync(peer, "selftest abort after partial reorg", CancellationToken.None)
            .GetAwaiter().GetResult();

        var canonicalHash1 = BlockStore.GetCanonicalHashAtHeight(1UL) ?? throw new InvalidOperationException("missing canonical hash at height 1 after abort");
        var canonicalHash2 = BlockStore.GetCanonicalHashAtHeight(2UL) ?? throw new InvalidOperationException("missing canonical hash at height 2 after abort");
        Assert(BytesEqual(canonicalHash1, blockA1.BlockHash!), "abort must leave original canonical block at height 1 intact");
        Assert(BytesEqual(canonicalHash2, blockA2.BlockHash!), "abort must leave original canonical block at height 2 intact");
        Assert(BlockStore.GetLatestHeight() == 2UL, "abort must leave original canonical tip height intact");

        Assert(StateStore.GetBalanceU64(minerA.pubHex) ==
               RewardCalculator.GetBlockSubsidy(1UL) + RewardCalculator.GetBlockSubsidy(2UL),
            "abort must preserve balances from the original canonical chain");
        Assert(StateStore.GetBalanceU64(minerB.pubHex) == 0UL,
            "sidechain-only commit must not apply branch balances to canonical state");

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
            Assert(undoRows == 0L, "non-canonical sidechain block must not create undo rows");
        }

        Assert(notified == 0, $"non-adopted sidechain commit must not notify canonical changes, got {notified}");
        Assert(!log.Lines.Any(line => line.Contains("abort restored prior canonical chain", StringComparison.OrdinalIgnoreCase)),
            "abort no longer restores in-flight canonical changes because it should not create them");

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

        downloadManager.QueueOutOfPlanFallback(missingParentHash, "test-live-orphan");
        flow.HandleBlockAsync(SerializeBlock(orphan), peer, CancellationToken.None, BlockIngressKind.LivePush, enforceRateLimitOverride: null)
            .GetAwaiter().GetResult();

        Assert(orphanStored, $"live orphan should be buffered; invalid={invalidReason}; logs={string.Join(" || ", log.Lines)}");
        Assert(BytesEqual(requestedParent, missingParentHash), "live orphan must request the missing parent from the same peer");
        Assert(
            string.Equals(requestSource, "live-orphan-parent", StringComparison.Ordinal) ||
            string.Equals(requestSource, "orphan-parent", StringComparison.Ordinal),
            $"unexpected parent request source: {requestSource}");
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

    private static void TestBlockIngressFlowPostPromotionReevaluatesBestKnownTip()
    {
        var peer = CreateFakePeer("peer-ingress-post-promotion");
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        var log = new ListLogSink();

        var genesis = BlockStore.GetBlockByHeight(0) ?? throw new InvalidOperationException("missing genesis block");
        var genesisHash = genesis.BlockHash ?? throw new InvalidOperationException("missing genesis hash");
        var canonicalMiner = KeyGenerator.GenerateKeypairHex();
        var canonical1 = BuildLooseMinedBlock(
            height: 1UL,
            prevHash: genesisHash,
            timestamp: genesis.Header!.Timestamp + 61UL,
            minerPubHex: canonicalMiner.pubHex);
        BlockPersistHelper.Persist(canonical1);
        var canonical2 = BuildLooseMinedBlock(
            height: 2UL,
            prevHash: canonical1.BlockHash!,
            timestamp: canonical1.Header!.Timestamp + 61UL,
            minerPubHex: canonicalMiner.pubHex);
        BlockPersistHelper.Persist(canonical2);

        var branchMiner = KeyGenerator.GenerateKeypairHex();

        var branch1 = BuildLooseMinedBlock(
            height: 1UL,
            prevHash: genesisHash,
            timestamp: genesis.Header!.Timestamp + 62UL,
            minerPubHex: branchMiner.pubHex);
        var branch2 = BuildLooseMinedBlock(
            height: 2UL,
            prevHash: branch1.BlockHash!,
            timestamp: branch1.Header!.Timestamp + 61UL,
            minerPubHex: branchMiner.pubHex);
        var branch3 = BuildLooseMinedBlock(
            height: 3UL,
            prevHash: branch2.BlockHash!,
            timestamp: branch2.Header!.Timestamp + 61UL,
            minerPubHex: branchMiner.pubHex);

        var client = CreateNoopBlockSyncClient(peer, 0UL, canonical2.BlockHash!);
        var downloadManager = new BlockDownloadManager(
            sessionSnapshot: () => new[] { peer },
            sendFrameAsync: (_, _, _, _) => Task.CompletedTask,
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
            tryStoreOrphan: (byte[] _, Block _, PeerSession _, string _, BlockIngressKind _, out string reason) =>
            {
                reason = string.Empty;
                return true;
            },
            requestParentFromPeer: (_, _, _) => { },
            markStoredBlockInvalid: (_, _, _) => { },
            passesSidechainAdmission: (Block _, byte[]? _, ulong _, int _, out string reason) =>
            {
                reason = string.Empty;
                return true;
            },
            logKnownBlockAlready: _ => { },
            notifyUiAfterAcceptedBlock: () => { },
            relayValidatedBlockAsync: (_, _, _) => Task.CompletedTask,
            promoteOrphansForParentAsync: (_, _, _) =>
            {
                lock (Db.Sync)
                {
                    using var tx = Db.Connection.BeginTransaction();
                    BlockStore.SaveBlock(branch2, tx, BlockIndexStore.StatusHaveBlockPayload);
                    BlockStore.SaveBlock(branch3, tx, BlockIndexStore.StatusHaveBlockPayload);
                    tx.Commit();
                }

                return Task.CompletedTask;
            },
            log: log);

        flow.HandleBlockAsync(SerializeBlock(branch1), peer, CancellationToken.None, BlockIngressKind.LivePush, enforceRateLimitOverride: null)
            .GetAwaiter().GetResult();

        Assert(BlockStore.GetLatestHeight() == 3UL, "post-promotion reevaluation must adopt the stronger promoted branch tip");
        Assert(BytesEqual(
                BlockStore.GetCanonicalHashAtHeight(1UL) ?? throw new InvalidOperationException("missing canonical hash at height 1"),
                branch1.BlockHash!),
            "post-promotion reevaluation must reorg canonical height 1 onto the promoted branch");
        Assert(BytesEqual(
                BlockStore.GetCanonicalHashAtHeight(2UL) ?? throw new InvalidOperationException("missing canonical hash at height 2"),
                branch2.BlockHash!),
            "post-promotion reevaluation must pull the promoted height 2 block into canonical");
        Assert(BytesEqual(
                BlockStore.GetCanonicalHashAtHeight(3UL) ?? throw new InvalidOperationException("missing canonical hash at height 3"),
                branch3.BlockHash!),
            "post-promotion reevaluation must pull the promoted height 3 block into canonical");
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

    private static void TestBlockIngressFlowLiveUnknownParentDefersUntilRecoveryArmed()
    {
        var peer = CreateFakePeer("peer-ingress-unarmed-orphan");
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        var log = new ListLogSink();
        int orphanStores = 0;
        int parentRequests = 0;
        int syncRequests = 0;

        BuildCanonicalChain(12UL);

        var client = CreateNoopBlockSyncClient(peer, 12UL, BlockStore.GetCanonicalHashAtHeight(12UL)!);
        var downloadManager = new BlockDownloadManager(
            sessionSnapshot: () => new[] { peer },
            sendFrameAsync: (_, _, _, _) => Task.CompletedTask,
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
            tryStoreOrphan: (byte[] _, Block _, PeerSession _, string _, BlockIngressKind _, out string reason) =>
            {
                orphanStores++;
                reason = string.Empty;
                return true;
            },
            requestParentFromPeer: (_, _, _) => parentRequests++,
            markStoredBlockInvalid: (_, _, _) => { },
            passesSidechainAdmission: (Block _, byte[]? _, ulong _, int _, out string reason) =>
            {
                reason = string.Empty;
                return true;
            },
            logKnownBlockAlready: _ => { },
            notifyUiAfterAcceptedBlock: () => { },
            relayValidatedBlockAsync: (_, _, _) => Task.CompletedTask,
            promoteOrphansForParentAsync: (_, _, _) => Task.CompletedTask,
            log: log);

        byte[] missingParentHash = HashFromU64(90_001UL);
        var orphanMiner = KeyGenerator.GenerateKeypairHex();
        var orphan = BuildLooseMinedBlock(
            height: 2UL,
            prevHash: missingParentHash,
            timestamp: (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            minerPubHex: orphanMiner.pubHex);

        flow.HandleBlockAsync(SerializeBlock(orphan), peer, CancellationToken.None, BlockIngressKind.LivePush, enforceRateLimitOverride: null)
            .GetAwaiter().GetResult();

        Assert(orphanStores == 0, "unarmed live orphan must not be buffered");
        Assert(parentRequests == 1, $"unarmed live orphan must request parent exactly once, got {parentRequests}");
        Assert(syncRequests == 1, $"unarmed live orphan must trigger one sync request, got {syncRequests}");
        Assert(downloadManager.IsOutOfPlanFallbackHash(missingParentHash), "unarmed live orphan must arm fallback for the missing parent");
        Assert(log.Lines.Any(line => line.Contains("Live orphan deferred until recovery is armed", StringComparison.Ordinal)),
            $"expected deferral log for unarmed live orphan, logs={string.Join(" || ", log.Lines)}");
    }

    private static void TestBlockIngressFlowLiveFarBehindKnownParentDefersWithoutPersist()
    {
        var peer = CreateFakePeer("peer-ingress-far-side");
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        var log = new ListLogSink();
        int sideAdmissionCalls = 0;
        int syncRequests = 0;

        var canonical = BuildCanonicalChain(12UL);
        var parent = canonical[1];

        var client = CreateNoopBlockSyncClient(peer, 12UL, BlockStore.GetCanonicalHashAtHeight(12UL)!);
        var downloadManager = new BlockDownloadManager(
            sessionSnapshot: () => new[] { peer },
            sendFrameAsync: (_, _, _, _) => Task.CompletedTask,
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
            tryStoreOrphan: (byte[] _, Block _, PeerSession _, string _, BlockIngressKind _, out string reason) =>
            {
                reason = string.Empty;
                return true;
            },
            requestParentFromPeer: (_, _, _) => { },
            markStoredBlockInvalid: (_, _, _) => { },
            passesSidechainAdmission: (Block _, byte[]? _, ulong _, int _, out string reason) =>
            {
                sideAdmissionCalls++;
                reason = string.Empty;
                return true;
            },
            logKnownBlockAlready: _ => { },
            notifyUiAfterAcceptedBlock: () => { },
            relayValidatedBlockAsync: (_, _, _) => Task.CompletedTask,
            promoteOrphansForParentAsync: (_, _, _) => Task.CompletedTask,
            log: log);

        var sideMiner = KeyGenerator.GenerateKeypairHex();
        var sideBlock = BuildMinedBlock(
            height: 3UL,
            prevHash: parent.BlockHash!,
            timestamp: parent.Header!.Timestamp + 61UL,
            minerPubHex: sideMiner.pubHex);

        flow.HandleBlockAsync(SerializeBlock(sideBlock), peer, CancellationToken.None, BlockIngressKind.LivePush, enforceRateLimitOverride: null)
            .GetAwaiter().GetResult();

        Assert(sideAdmissionCalls == 0, "far-behind live sidechain should be deferred before side admission/validation");
        Assert(syncRequests == 1, $"far-behind live sidechain should trigger a single sync request, got {syncRequests}");
        Assert(downloadManager.IsOutOfPlanFallbackHash(parent.BlockHash!), "far-behind live sidechain should arm fallback for the known parent");
        Assert(!BlockIndexStore.ContainsHash(sideBlock.BlockHash!), "far-behind live sidechain must not be persisted to block index");
        Assert(!BlockIndexStore.HasPayload(sideBlock.BlockHash!), "far-behind live sidechain must not persist payload");
        Assert(log.Lines.Any(line => line.Contains("Live sidechain deferred to request/response path", StringComparison.Ordinal)),
            $"expected defer log for far-behind live sidechain, logs={string.Join(" || ", log.Lines)}");
    }

    private static void TestBlockIngressFlowRecoveryOrphanDuringActiveSyncKeepsBoundedSidePathRecovery()
    {
        var peer = CreateFakePeer("peer-ingress-recovery-active-sync");
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        var log = new ListLogSink();
        int orphanStores = 0;
        int parentRequests = 0;
        int syncRequests = 0;

        BuildCanonicalChain(12UL);

        var client = CreateNoopBlockSyncClient(peer, 12UL, BlockStore.GetCanonicalHashAtHeight(12UL)!);
        SetBlockSyncClientStateForTest(client, BlockSyncState.AwaitingBatchBegin);
        var downloadManager = new BlockDownloadManager(
            sessionSnapshot: () => new[] { peer },
            sendFrameAsync: (_, _, _, _) => Task.CompletedTask,
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
            tryStoreOrphan: (byte[] _, Block _, PeerSession _, string _, BlockIngressKind _, out string reason) =>
            {
                orphanStores++;
                reason = string.Empty;
                return true;
            },
            requestParentFromPeer: (_, _, _) => parentRequests++,
            markStoredBlockInvalid: (_, _, _) => { },
            passesSidechainAdmission: (Block _, byte[]? _, ulong _, int _, out string reason) =>
            {
                reason = string.Empty;
                return true;
            },
            logKnownBlockAlready: _ => { },
            notifyUiAfterAcceptedBlock: () => { },
            relayValidatedBlockAsync: (_, _, _) => Task.CompletedTask,
            promoteOrphansForParentAsync: (_, _, _) => Task.CompletedTask,
            log: log);

        byte[] missingParentHash = HashFromU64(91_001UL);
        var orphanMiner = KeyGenerator.GenerateKeypairHex();
        var orphan = BuildLooseMinedBlock(
            height: 2UL,
            prevHash: missingParentHash,
            timestamp: (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            minerPubHex: orphanMiner.pubHex);
        Assert(
            BlockValidator.ValidateNetworkOrphanBlockStateless(orphan, out var orphanReason),
            $"recovery orphan helper built invalid block: {orphanReason}");

        flow.HandleBlockAsync(SerializeBlock(orphan), peer, CancellationToken.None, BlockIngressKind.Recovery, enforceRateLimitOverride: false)
            .GetAwaiter().GetResult();

        Assert(orphanStores == 1, $"recovery orphan should still be buffered once, got {orphanStores}");
        Assert(parentRequests == 1, $"active historical sync should keep one direct parent-pack chase alive, got {parentRequests}");
        Assert(syncRequests == 0, $"active historical sync must not trigger nested resync for recovery orphan, got {syncRequests}");
        Assert(downloadManager.IsOutOfPlanFallbackHash(missingParentHash), "recovery orphan during active sync should arm bounded fallback side-path work");
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

    private static void TestPeerAddressOrderDialAddressesSkipsIpv6WhenDisabled()
    {
        Assert(!NetworkParams.EnableIpv6, "test expects IPv6 to be disabled in NetworkParams");

        Type peerAddressType = typeof(P2PNode).Assembly.GetType("Qado.Networking.PeerAddress")
            ?? throw new InvalidOperationException("PeerAddress reflection lookup failed");
        MethodInfo orderDialAddresses = peerAddressType.GetMethod("OrderDialAddresses", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("PeerAddress.OrderDialAddresses reflection lookup failed");

        var ordered = (IReadOnlyList<IPAddress>)orderDialAddresses.Invoke(null, new object[]
        {
            new[]
            {
                IPAddress.Parse("2001:4860:4860::8888"),
                IPAddress.Parse("93.184.216.44")
            }
        })!;

        Assert(ordered.Count == 1, $"expected only IPv4 dial address when IPv6 is disabled, got {ordered.Count}");
        Assert(ordered[0].AddressFamily == AddressFamily.InterNetwork, "ordered dial addresses should retain only IPv4 entries");
        Assert(string.Equals(ordered[0].ToString(), "93.184.216.44", StringComparison.Ordinal), "unexpected IPv4 address ordering result");
    }

    private static void TestBlockDownloadManagerParallelizesRecoveryAcrossPeers()
    {
        var peer1 = CreateFakePeer("recovery-parallel-peer-1");
        peer1.LastLatencyMs = 5;
        peer1.LastLatencyUpdatedUtc = DateTime.UtcNow;

        var peer2 = CreateFakePeer("recovery-parallel-peer-2");
        peer2.LastLatencyMs = 15;
        peer2.LastLatencyUpdatedUtc = DateTime.UtcNow;

        byte[] wanted1 = HashFromU64(881UL);
        byte[] wanted2 = HashFromU64(882UL);
        var sent = new List<(string peer, MsgType type, byte[] payload)>();
        using var requested = new ManualResetEventSlim(false);

        using var manager = new BlockDownloadManager(
            sessionSnapshot: () => new[] { peer1, peer2 },
            sendFrameAsync: (peer, type, payload, ct) =>
            {
                lock (sent)
                {
                    sent.Add((peer.SessionKey, type, (byte[])payload.Clone()));
                    if (sent.Count >= 2)
                        requested.Set();
                }

                return Task.CompletedTask;
            },
            haveBlock: _ => false,
            enqueueValidator: _ => true,
            revalidateStoredBlock: _ => { },
            log: null);

        Assert(manager.QueueOutOfPlanFallback(wanted1, "selftest-parallel-1") == 1, "first recovery hash was not queued");
        Assert(manager.QueueOutOfPlanFallback(wanted2, "selftest-parallel-2") == 1, "second recovery hash was not queued");
        manager.Start(CancellationToken.None);
        manager.OnPeerReady(peer1);
        manager.OnPeerReady(peer2);

        Assert(requested.Wait(TimeSpan.FromSeconds(10)), "queued recovery hashes were not dispatched across multiple peers");

        lock (sent)
        {
            Assert(sent.Count >= 2, $"expected at least two recovery requests, got {sent.Count}");
            Assert(sent[0].type == MsgType.GetAncestorPack, "first recovery request must use GetAncestorPack");
            Assert(sent[1].type == MsgType.GetAncestorPack, "second recovery request must use GetAncestorPack");
            Assert(!string.Equals(sent[0].peer, sent[1].peer, StringComparison.Ordinal), "parallel recovery should use a backup peer instead of waiting on the first peer");

            Assert(SmallNetSyncProtocol.TryParseGetAncestorPack(sent[0].payload, out var frame1), "first ancestor-pack request did not parse");
            Assert(SmallNetSyncProtocol.TryParseGetAncestorPack(sent[1].payload, out var frame2), "second ancestor-pack request did not parse");
            Assert(frame1.MaxBlocks == 1, $"expected single-block recovery request for first frame, got {frame1.MaxBlocks}");
            Assert(frame2.MaxBlocks == 1, $"expected single-block recovery request for second frame, got {frame2.MaxBlocks}");

            var requestedHashes = new HashSet<string>(StringComparer.Ordinal)
            {
                Convert.ToHexString(frame1.StartHash),
                Convert.ToHexString(frame2.StartHash)
            };
            Assert(requestedHashes.Contains(Convert.ToHexString(wanted1)), "parallel recovery must include the first wanted hash");
            Assert(requestedHashes.Contains(Convert.ToHexString(wanted2)), "parallel recovery must include the second wanted hash");
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
            commitBlocksAsync: (payloads, height, prevHash, peer, ct) =>
            {
                ulong lastHeight = height + (ulong)payloads.Count - 1UL;
                return Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(lastHeight), string.Empty));
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

    private static void TestBlockSyncClientReconnectedPeerInheritsCooldownByBanKey()
    {
        byte[] localTip = HashFromU64(100UL);
        UInt128 localChainwork = 1_000UL;
        var sent = new List<(string peer, MsgType type)>();
        var sessions = new List<PeerSession>();

        var peerA1 = CreateFakePeer("peer-a-session-1");
        peerA1.RemoteBanKey = "peer-a";
        sessions.Add(peerA1);

        var client = new BlockSyncClient(
            sessionSnapshot: () => sessions.ToArray(),
            sendFrameAsync: (peer, type, payload, ct) =>
            {
                sent.Add((peer.SessionKey, type));
                return Task.CompletedTask;
            },
            getLocalTipChainwork: () => localChainwork,
            getLocalTipHash: () => (byte[])localTip.Clone(),
            prepareBatchAsync: (startHash, startHeight, advertisedTipChainwork, peer, ct) =>
                Task.FromResult(new BlockSyncPrepareResult(true, string.Empty)),
            commitBlocksAsync: (payloads, height, prevHash, peer, ct) =>
                Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
            completeBatchAsync: (peer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (peer, reason, ct) => Task.CompletedTask,
            log: null);

        byte[] tipHash = HashFromU64(250UL);
        var tipState = new SmallNetTipStateFrame(250UL, tipHash, 2_500UL, null, new[] { HashFromU64(249UL), tipHash });

        client.OnTipStateAsync(peerA1, tipState, CancellationToken.None).GetAwaiter().GetResult();
        Assert(sent.Count == 1, $"expected initial sync request to first session, got {sent.Count}");
        Assert(sent[0].peer == peerA1.SessionKey, "initial request must target first peer session");

        client.OnNoCommonAncestorAsync(peerA1, CancellationToken.None).GetAwaiter().GetResult();

        var peerA2 = CreateFakePeer("peer-a-session-2");
        peerA2.RemoteBanKey = "peer-a";
        sessions.Clear();
        sessions.Add(peerA2);

        client.OnTipStateAsync(peerA2, tipState, CancellationToken.None).GetAwaiter().GetResult();

        Assert(sent.Count == 1,
            $"reconnected peer with same ban key should stay on cooldown instead of being retried immediately; got {sent.Count} sends");
    }

    private static void TestBlockSyncClientReconnectedPeerRetainsFailureBiasAfterCooldown()
    {
        byte[] localTip = HashFromU64(100UL);
        UInt128 localChainwork = 1_000UL;
        var sent = new List<(string peer, MsgType type)>();
        var sessions = new List<PeerSession>();

        var peerA1 = CreateFakePeer("peer-a-session-1");
        peerA1.RemoteBanKey = "peer-a";
        sessions.Add(peerA1);

        var client = new BlockSyncClient(
            sessionSnapshot: () => sessions.ToArray(),
            sendFrameAsync: (peer, type, payload, ct) =>
            {
                sent.Add((peer.SessionKey, type));
                return Task.CompletedTask;
            },
            getLocalTipChainwork: () => localChainwork,
            getLocalTipHash: () => (byte[])localTip.Clone(),
            prepareBatchAsync: (startHash, startHeight, advertisedTipChainwork, peer, ct) =>
                Task.FromResult(new BlockSyncPrepareResult(true, string.Empty)),
            commitBlocksAsync: (payloads, height, prevHash, peer, ct) =>
                Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
            completeBatchAsync: (peer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (peer, reason, ct) => Task.CompletedTask,
            log: null);

        byte[] tipHash = HashFromU64(250UL);
        var tipState = new SmallNetTipStateFrame(250UL, tipHash, 2_500UL, null, new[] { HashFromU64(249UL), tipHash });

        client.OnTipStateAsync(peerA1, tipState, CancellationToken.None).GetAwaiter().GetResult();
        Assert(sent.Count == 1, $"expected initial sync request to first session, got {sent.Count}");

        client.OnNoCommonAncestorAsync(peerA1, CancellationToken.None).GetAwaiter().GetResult();

        FieldInfo cooldownField = typeof(BlockSyncClient).GetField("_cooldownUntilByPeerKey", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BlockSyncClient cooldown dictionary reflection lookup failed");
        var cooldownByPeer = (Dictionary<string, DateTime>)cooldownField.GetValue(client)!;
        cooldownByPeer.Clear();

        FieldInfo planningReadyField = typeof(BlockSyncClient).GetField("_planningReady", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BlockSyncClient planningReady reflection lookup failed");
        planningReadyField.SetValue(client, false);

        var peerA2 = CreateFakePeer("peer-a-session-2");
        peerA2.RemoteBanKey = "peer-a";
        var peerB = CreateFakePeer("peer-b-session-1");
        peerB.RemoteBanKey = "peer-b";

        sessions.Clear();
        sessions.Add(peerA2);
        sessions.Add(peerB);

        client.OnTipStateAsync(peerA2, tipState, CancellationToken.None).GetAwaiter().GetResult();
        client.OnTipStateAsync(peerB, tipState, CancellationToken.None).GetAwaiter().GetResult();

        planningReadyField.SetValue(client, true);
        MethodInfo ensureMethod = typeof(BlockSyncClient).GetMethod("EnsurePipelineAsync", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BlockSyncClient EnsurePipelineAsync reflection lookup failed");
        ((Task)ensureMethod.Invoke(client, new object[] { CancellationToken.None })!).GetAwaiter().GetResult();

        Assert(sent.Count >= 2, $"expected a replacement sync request after reconnect, got {sent.Count}");
        Assert(sent[^1].peer == peerB.SessionKey,
            "peer selection should prefer a fresh peer over a reconnected peer with prior sync failures");
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
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
            {
                ulong lastHeight = height + (ulong)payloads.Count - 1UL;
                return Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(lastHeight), string.Empty));
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

    private static void TestBlockSyncClientExtendedSyncWindowRequests512ForCapablePeers()
    {
        byte[] localTip = HashFromU64(100UL);
        UInt128 localChainwork = 1_000UL;
        var sent = new List<(string peer, MsgType type, byte[] payload)>();
        var sessions = new List<PeerSession>();
        var peer = CreateFakePeer(
            "peer-fast-window",
            HandshakeCapabilities.BlocksBatchData | HandshakeCapabilities.ExtendedSyncWindow);
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
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
            {
                ulong lastHeight = height + (ulong)payloads.Count - 1UL;
                localTip = HashFromU64(lastHeight + 10_000UL);
                return Task.FromResult(new BlockSyncCommitResult(true, (byte[])localTip.Clone(), string.Empty));
            },
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            log: null);

        byte[] remoteTipHash = HashFromU64(400UL);
        client.OnTipStateAsync(
                peer,
                new SmallNetTipStateFrame(400UL, remoteTipHash, 1_400UL, null, new[] { HashFromU64(399UL), remoteTipHash }),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1, $"expected one locator request, got {sent.Count}");
        Assert(sent[0].type == MsgType.GetBlocksByLocator, "initial fast-window request must use locator sync");
        Assert(BlockSyncProtocol.TryParseGetBlocksByLocator(sent[0].payload, out _, out var locatorMaxBlocks),
            "fast-window locator request did not parse");
        Assert(locatorMaxBlocks == BlockSyncProtocol.ExtendedSyncWindowBlocks,
            $"fast-window locator request max_blocks mismatch: got {locatorMaxBlocks}");

        Guid batchId = Guid.NewGuid();
        sent.Clear();
        client.OnBlocksBatchStartAsync(
                peer,
                new SmallNetBlocksBatchStartFrame(batchId, HashFromU64(100UL), 100UL, 1, remoteTipHash, 1_400UL),
                CancellationToken.None)
            .GetAwaiter().GetResult();
        client.OnBlocksBatchDataAsync(
                peer,
                SmallNetSyncProtocol.BuildBlocksBatchData(101UL, new[] { new byte[] { 0x01 } }),
                CancellationToken.None)
            .GetAwaiter().GetResult();
        client.OnBlocksBatchEndAsync(
                peer,
                new SmallNetBlocksBatchEndFrame(batchId, HashFromU64(101UL), 101UL, MoreAvailable: true),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1, $"expected one continuation request, got {sent.Count}");
        Assert(sent[0].type == MsgType.GetBlocksFrom, "continuation request must use GetBlocksFrom");
        Assert(BlockSyncProtocol.TryParseGetBlocksFrom(sent[0].payload, out _, out var fromMaxBlocks),
            "fast-window continuation request did not parse");
        Assert(fromMaxBlocks == BlockSyncProtocol.ExtendedSyncWindowBlocks,
            $"fast-window continuation request max_blocks mismatch: got {fromMaxBlocks}");
    }

    private static void TestBlockSyncClientStaleBatchEndBeforeStartIsIgnored()
    {
        byte[] localTip = HashFromU64(10UL);
        UInt128 localChainwork = 10UL;
        var sent = new List<(MsgType type, byte[] payload)>();
        var penalized = new List<string>();
        var peer = CreateFakePeer("peer-stale-end");
        var sessions = new List<PeerSession> { peer };

        var client = new BlockSyncClient(
            sessionSnapshot: () => sessions.ToArray(),
            sendFrameAsync: (targetPeer, type, payload, ct) =>
            {
                sent.Add((type, (byte[])payload.Clone()));
                return Task.CompletedTask;
            },
            getLocalTipChainwork: () => localChainwork,
            getLocalTipHash: () => (byte[])localTip.Clone(),
            prepareBatchAsync: (startHash, startHeight, advertisedTipChainwork, targetPeer, ct) =>
                Task.FromResult(new BlockSyncPrepareResult(true, string.Empty)),
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
                Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            penalizePeer: (targetPeer, reason) => penalized.Add($"{targetPeer.SessionKey}:{reason}"),
            log: null);

        byte[] remoteTipHash = HashFromU64(25UL);
        client.OnTipStateAsync(peer, new SmallNetTipStateFrame(25UL, remoteTipHash, 25UL, null, new[] { HashFromU64(24UL), remoteTipHash }), CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1 && sent[0].type == MsgType.GetBlocksByLocator,
            $"expected initial locator request before stale end test, got {sent.Count}");

        client.OnBlocksBatchEndAsync(
                peer,
                new SmallNetBlocksBatchEndFrame(Guid.NewGuid(), remoteTipHash, 11UL, MoreAvailable: true),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(penalized.Count == 0, $"stale batch end before start must not penalize peer, got {string.Join(", ", penalized)}");
        Assert(client.State == BlockSyncState.AwaitingBatchBegin,
            $"stale batch end before start must leave request waiting for batch start, got state={client.State}");
        Assert(sent.Count == 1, $"stale batch end before start must not trigger resync/reset traffic, got {sent.Count} sends");
    }

    private static void TestBlockSyncClientEqualChainworkPrepareSkipsWithoutPenalty()
    {
        byte[] localTip = HashFromU64(10UL);
        UInt128 localChainwork = 10UL;
        var sent = new List<(MsgType type, byte[] payload)>();
        var aborted = new List<string>();
        var penalized = new List<string>();
        var peer = CreateFakePeer("peer-equal-work");
        var sessions = new List<PeerSession> { peer };
        var log = new ListLogSink();

        var client = new BlockSyncClient(
            sessionSnapshot: () => sessions.ToArray(),
            sendFrameAsync: (targetPeer, type, payload, ct) =>
            {
                sent.Add((type, (byte[])payload.Clone()));
                return Task.CompletedTask;
            },
            getLocalTipChainwork: () => localChainwork,
            getLocalTipHash: () => (byte[])localTip.Clone(),
            prepareBatchAsync: (startHash, startHeight, advertisedTipChainwork, targetPeer, ct) =>
            {
                if (advertisedTipChainwork <= localChainwork)
                {
                    return Task.FromResult(new BlockSyncPrepareResult(
                        false,
                        $"peer tip chainwork not better than local chain (peer={advertisedTipChainwork}, local={localChainwork})"));
                }

                return Task.FromResult(new BlockSyncPrepareResult(true, string.Empty));
            },
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
                Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) =>
            {
                aborted.Add(reason);
                return Task.CompletedTask;
            },
            penalizePeer: (targetPeer, reason) => penalized.Add($"{targetPeer.SessionKey}:{reason}"),
            log: log);

        byte[] remoteTipHash = HashFromU64(25UL);
        client.OnTipStateAsync(
                peer,
                new SmallNetTipStateFrame(25UL, remoteTipHash, 25UL, null, new[] { HashFromU64(24UL), remoteTipHash }),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1 && sent[0].type == MsgType.GetBlocksByLocator,
            $"expected one initial locator request, got {sent.Count}");

        localChainwork = 25UL;
        localTip = (byte[])remoteTipHash.Clone();

        client.OnBlocksBatchStartAsync(
                peer,
                new SmallNetBlocksBatchStartFrame(Guid.NewGuid(), HashFromU64(10UL), 10UL, 1, remoteTipHash, 25UL),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(penalized.Count == 0, $"equal-chainwork prepare must not penalize peer, got {string.Join(", ", penalized)}");
        Assert(aborted.Count == 1, $"equal-chainwork prepare should abort the stale batch once, got {aborted.Count}");
        Assert(client.State == BlockSyncState.Idle,
            $"equal-chainwork prepare should leave client idle when no better peer remains, got state={client.State}");
        Assert(sent.Count == 1, $"equal-chainwork prepare must not trigger an immediate replacement request, got {sent.Count} sends");
        Assert(!log.Lines.Any(l => l.Contains("Block sync reset", StringComparison.Ordinal)),
            "equal-chainwork prepare must not emit a sync reset log");
    }

    private static void TestBlockSyncClientSyncWindowPreviewPipelinesNextPeerBeforeBatchEnd()
    {
        byte[] localTip = HashFromU64(100UL);
        UInt128 localChainwork = 1_000UL;
        var sent = new List<(string peer, MsgType type, byte[] payload)>();
        var sessions = new List<PeerSession>();
        var caps =
            HandshakeCapabilities.BlocksBatchData |
            HandshakeCapabilities.ExtendedSyncWindow |
            HandshakeCapabilities.SyncWindowPreview;
        var peer1 = CreateFakePeer("peer-preview-a", caps);
        var peer2 = CreateFakePeer("peer-preview-b", caps);
        sessions.Add(peer1);
        sessions.Add(peer2);

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
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
                Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            log: null);

        byte[] remoteTipHash = HashFromU64(900UL);
        var tipState = new SmallNetTipStateFrame(900UL, remoteTipHash, 9_000UL, null, new[] { HashFromU64(899UL), remoteTipHash });

        client.OnTipStateAsync(peer1, tipState, CancellationToken.None).GetAwaiter().GetResult();
        client.OnTipStateAsync(peer2, tipState, CancellationToken.None).GetAwaiter().GetResult();

        Assert(sent.Count == 1, $"expected initial locator request, got {sent.Count}");
        Assert(sent[0].peer == peer1.SessionKey, "first preview sync request must target the first peer");
        Assert(sent[0].type == MsgType.GetBlocksByLocator, "first preview sync request must use locator");

        sent.Clear();
        byte[] previewLastHash = HashFromU64(612UL);
        client.OnBlocksBatchStartAsync(
                peer1,
                new SmallNetBlocksBatchStartFrame(
                    Guid.NewGuid(),
                    localTip,
                    100UL,
                    BlockSyncProtocol.ExtendedSyncWindowBlocks,
                    remoteTipHash,
                    9_000UL,
                    previewLastHash,
                    100UL + (ulong)BlockSyncProtocol.ExtendedSyncWindowBlocks),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1, $"expected preview to pipeline exactly one next-peer request, got {sent.Count}");
        Assert(sent[0].peer == peer2.SessionKey, "preview pipeline must move the next window to another peer");
        Assert(sent[0].type == MsgType.GetBlocksFrom, "preview pipeline must use GetBlocksFrom for the next window");
        Assert(BlockSyncProtocol.TryParseGetBlocksFrom(sent[0].payload, out var fromHash, out var maxBlocks),
            "preview pipeline continuation request did not parse");
        Assert(BytesEqual(fromHash, previewLastHash), "preview pipeline must request from the preview hash");
        Assert(maxBlocks == BlockSyncProtocol.ExtendedSyncWindowBlocks,
            $"preview pipeline max_blocks mismatch: got {maxBlocks}");
    }

    private static void TestBlockSyncClientFromHashContinuationDoesNotRequireImmediatePrepare()
    {
        byte[] localTip = HashFromU64(100UL);
        UInt128 localChainwork = 1_000UL;
        var sent = new List<(string peer, MsgType type, byte[] payload)>();
        var penalized = new List<string>();
        var sessions = new List<PeerSession>();
        var caps =
            HandshakeCapabilities.BlocksBatchData |
            HandshakeCapabilities.ExtendedSyncWindow |
            HandshakeCapabilities.SyncWindowPreview;
        var peer1 = CreateFakePeer("peer-prepare-a", caps);
        var peer2 = CreateFakePeer("peer-prepare-b", caps);
        sessions.Add(peer1);
        sessions.Add(peer2);

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
            {
                if (startHeight == 101UL)
                    return Task.FromResult(new BlockSyncPrepareResult(true, string.Empty));

                return Task.FromResult(new BlockSyncPrepareResult(false, "start hash unknown"));
            },
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
                Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            penalizePeer: (targetPeer, reason) => penalized.Add($"{targetPeer.SessionKey}:{reason}"),
            log: null);

        byte[] remoteTipHash = HashFromU64(900UL);
        var tipState = new SmallNetTipStateFrame(900UL, remoteTipHash, 9_000UL, null, new[] { HashFromU64(899UL), remoteTipHash });

        client.OnTipStateAsync(peer1, tipState, CancellationToken.None).GetAwaiter().GetResult();
        client.OnTipStateAsync(peer2, tipState, CancellationToken.None).GetAwaiter().GetResult();

        sent.Clear();
        byte[] previewLastHash = HashFromU64(612UL);
        Guid batch1Id = Guid.NewGuid();
        client.OnBlocksBatchStartAsync(
                peer1,
                new SmallNetBlocksBatchStartFrame(
                    batch1Id,
                    localTip,
                    100UL,
                    BlockSyncProtocol.ExtendedSyncWindowBlocks,
                    remoteTipHash,
                    9_000UL,
                    previewLastHash,
                    100UL + (ulong)BlockSyncProtocol.ExtendedSyncWindowBlocks),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1 && sent[0].peer == peer2.SessionKey && sent[0].type == MsgType.GetBlocksFrom,
            $"expected preview continuation request to peer2, got {sent.Count}");

        Guid batch2Id = Guid.NewGuid();
        client.OnBlocksBatchStartAsync(
                peer2,
                new SmallNetBlocksBatchStartFrame(
                    batch2Id,
                    previewLastHash,
                    100UL + (ulong)BlockSyncProtocol.ExtendedSyncWindowBlocks,
                    BlockSyncProtocol.ExtendedSyncWindowBlocks,
                    remoteTipHash,
                    9_000UL),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(penalized.Count == 0,
            $"from-hash continuation must not fail prepare just because start hash is not local yet: {string.Join(", ", penalized)}");
        Assert(client.State == BlockSyncState.ReceivingBatch,
            $"from-hash continuation should stay active after batch start, got state={client.State}");
    }

    private static void TestBlockSyncClientCompletedContinuationWaitsForEarlierPreparedWindow()
    {
        byte[] localTip = HashFromU64(10UL);
        UInt128 localChainwork = 10UL;
        var sent = new List<(string peer, MsgType type, byte[] payload)>();
        var penalized = new List<string>();
        var sessions = new List<PeerSession>();
        var caps =
            HandshakeCapabilities.BlocksBatchData |
            HandshakeCapabilities.ExtendedSyncWindow |
            HandshakeCapabilities.SyncWindowPreview;
        var peer1 = CreateFakePeer("peer-order-a", caps);
        var peer2 = CreateFakePeer("peer-order-b", caps);
        sessions.Add(peer1);
        sessions.Add(peer2);

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
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
            {
                if (height != 11UL)
                    return Task.FromResult(new BlockSyncCommitResult(false, Array.Empty<byte>(), "expected prev hash unknown"));

                return Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(11UL), string.Empty));
            },
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            penalizePeer: (targetPeer, reason) => penalized.Add($"{targetPeer.SessionKey}:{reason}"),
            log: null);

        byte[] remoteTipHash = HashFromU64(900UL);
        var tipState = new SmallNetTipStateFrame(900UL, remoteTipHash, 9_000UL, null, new[] { HashFromU64(899UL), remoteTipHash });

        client.OnTipStateAsync(peer1, tipState, CancellationToken.None).GetAwaiter().GetResult();
        client.OnTipStateAsync(peer2, tipState, CancellationToken.None).GetAwaiter().GetResult();

        sent.Clear();
        byte[] previewLastHash = HashFromU64(522UL);
        Guid batch1Id = Guid.NewGuid();
        client.OnBlocksBatchStartAsync(
                peer1,
                new SmallNetBlocksBatchStartFrame(
                    batch1Id,
                    localTip,
                    10UL,
                    BlockSyncProtocol.ExtendedSyncWindowBlocks,
                    remoteTipHash,
                    9_000UL,
                    previewLastHash,
                    10UL + (ulong)BlockSyncProtocol.ExtendedSyncWindowBlocks),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1 && sent[0].peer == peer2.SessionKey && sent[0].type == MsgType.GetBlocksFrom,
            $"expected preview continuation request to peer2, got {sent.Count}");

        Guid batch2Id = Guid.NewGuid();
        client.OnBlocksBatchStartAsync(
                peer2,
                new SmallNetBlocksBatchStartFrame(
                    batch2Id,
                    previewLastHash,
                    10UL + (ulong)BlockSyncProtocol.ExtendedSyncWindowBlocks,
                    1,
                    remoteTipHash,
                    9_000UL),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        client.OnBlocksBatchDataAsync(
                peer2,
                SmallNetSyncProtocol.BuildBlocksBatchData(11UL + (ulong)BlockSyncProtocol.ExtendedSyncWindowBlocks, new[] { new byte[] { 0x99 } }),
                CancellationToken.None)
            .GetAwaiter().GetResult();
        client.OnBlocksBatchEndAsync(
                peer2,
                new SmallNetBlocksBatchEndFrame(batch2Id, HashFromU64(523UL), 11UL + (ulong)BlockSyncProtocol.ExtendedSyncWindowBlocks, MoreAvailable: false),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(penalized.Count == 0,
            $"completed continuation must wait for earlier prepared windows instead of committing out of order: {string.Join(", ", penalized)}");
        Assert(client.State == BlockSyncState.ReceivingBatch,
            $"client should stay active while the earlier window is still pending, got state={client.State}");
    }

    private static void TestBlockSyncClientPreviewContinuationRetriesAfterCommitWhenNoPeerWasInitiallyFree()
    {
        byte[] localTip = HashFromU64(10UL);
        UInt128 localChainwork = 10UL;
        var sent = new List<(string peer, MsgType type, byte[] payload)>();
        var caps =
            HandshakeCapabilities.BlocksBatchData |
            HandshakeCapabilities.ExtendedSyncWindow |
            HandshakeCapabilities.SyncWindowPreview;
        var peer = CreateFakePeer("peer-preview-retry", caps);
        var sessions = new List<PeerSession> { peer };
        byte[] previewLastHash = HashFromU64(11UL);

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
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
            {
                localTip = (byte[])previewLastHash.Clone();
                localChainwork = height + (ulong)payloads.Count - 1UL;
                return Task.FromResult(new BlockSyncCommitResult(true, (byte[])previewLastHash.Clone(), string.Empty));
            },
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            log: null);

        byte[] remoteTipHash = HashFromU64(25UL);
        client.OnTipStateAsync(peer, new SmallNetTipStateFrame(25UL, remoteTipHash, 25UL, null, new[] { HashFromU64(24UL), remoteTipHash }), CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1, $"expected initial locator request, got {sent.Count}");
        Assert(sent[0].type == MsgType.GetBlocksByLocator, $"expected locator request first, got {sent[0].type}");

        sent.Clear();
        Guid batchId = Guid.NewGuid();
        client.OnBlocksBatchStartAsync(
                peer,
                new SmallNetBlocksBatchStartFrame(
                    batchId,
                    HashFromU64(10UL),
                    10UL,
                    1,
                    remoteTipHash,
                    25UL,
                    previewLastHash,
                    11UL),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 0, $"no second peer is available, so preview must not send immediately, got {sent.Count} sends");

        client.OnBlocksBatchDataAsync(
                peer,
                SmallNetSyncProtocol.BuildBlocksBatchData(11UL, new[] { new byte[] { 0x42 } }),
                CancellationToken.None)
            .GetAwaiter().GetResult();
        client.OnBlocksBatchEndAsync(
                peer,
                new SmallNetBlocksBatchEndFrame(batchId, previewLastHash, 11UL, MoreAvailable: true),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1, $"expected continuation retry after commit freed the only peer, got {sent.Count}");
        Assert(sent[0].peer == peer.SessionKey && sent[0].type == MsgType.GetBlocksFrom,
            "preview retry must continue from the same peer via GetBlocksFrom");
        Assert(BlockSyncProtocol.TryParseGetBlocksFrom(sent[0].payload, out var fromHash, out var maxBlocks),
            "preview retry continuation payload did not parse");
        Assert(BytesEqual(fromHash, previewLastHash),
            "preview retry must resume from the preview last hash");
        Assert(maxBlocks == BlockSyncProtocol.ExtendedSyncWindowBlocks,
            $"preview retry continuation max_blocks mismatch: got {maxBlocks}");
    }

    private static void TestBlockSyncClientIdleBehindWatchdogRevivesLocatorSync()
    {
        byte[] localTip = HashFromU64(10UL);
        UInt128 localChainwork = 10UL;
        var sent = new List<(string peer, MsgType type, byte[] payload)>();
        var peer = CreateFakePeer("peer-watchdog");
        var sessions = new List<PeerSession> { peer };
        var log = new ListLogSink();

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
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
                Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            log: log);

        byte[] remoteTipHash = HashFromU64(200UL);
        client.OnTipStateAsync(peer, new SmallNetTipStateFrame(200UL, remoteTipHash, 200UL, null, new[] { HashFromU64(199UL), remoteTipHash }), CancellationToken.None)
            .GetAwaiter().GetResult();

        sent.Clear();

        FieldInfo requestsField = typeof(BlockSyncClient).GetField("_requestsByPeerKey", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BlockSyncClient _requestsByPeerKey reflection lookup failed");
        object requests = requestsField.GetValue(client)
            ?? throw new InvalidOperationException("BlockSyncClient _requestsByPeerKey reflection value missing");
        requests.GetType().GetMethod("Clear")!.Invoke(requests, null);

        FieldInfo planningReadyField = typeof(BlockSyncClient).GetField("_planningReady", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BlockSyncClient _planningReady reflection lookup failed");
        planningReadyField.SetValue(client, false);

        FieldInfo lastProgressField = typeof(BlockSyncClient).GetField("_lastProgressUtc", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BlockSyncClient _lastProgressUtc reflection lookup failed");
        lastProgressField.SetValue(client, DateTime.UtcNow - TimeSpan.FromSeconds(30));

        SetBlockSyncClientStateForTest(client, BlockSyncState.Idle);

        MethodInfo recoverMethod = typeof(BlockSyncClient).GetMethod("RecoverIdleBehindAsync", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BlockSyncClient RecoverIdleBehindAsync reflection lookup failed");
        ((Task)recoverMethod.Invoke(client, new object[] { CancellationToken.None })!)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1, $"watchdog should re-arm exactly one sync request, got {sent.Count}");
        Assert(sent[0].peer == peer.SessionKey && sent[0].type == MsgType.GetBlocksByLocator,
            "watchdog recovery must restart with a locator request");
        Assert(log.Lines.Exists(line => line.Contains("watchdog revived idle-planner", StringComparison.Ordinal)),
            "watchdog recovery should emit a diagnostic log line");
    }

    private static void TestBlockSyncClientResetPipelineClearsCommitLoopLatch()
    {
        byte[] localTip = HashFromU64(10UL);
        var peer = CreateFakePeer("peer-reset");

        var client = new BlockSyncClient(
            sessionSnapshot: () => new[] { peer },
            sendFrameAsync: (targetPeer, type, payload, ct) => Task.CompletedTask,
            getLocalTipChainwork: () => 10UL,
            getLocalTipHash: () => (byte[])localTip.Clone(),
            prepareBatchAsync: (startHash, startHeight, advertisedTipChainwork, targetPeer, ct) =>
                Task.FromResult(new BlockSyncPrepareResult(true, string.Empty)),
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
                Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            log: null);

        FieldInfo commitLoopField = typeof(BlockSyncClient).GetField("_commitLoopRunning", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BlockSyncClient _commitLoopRunning reflection lookup failed");
        commitLoopField.SetValue(client, true);

        MethodInfo resetMethod = typeof(BlockSyncClient).GetMethod("ResetPipeline_NoLock", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BlockSyncClient ResetPipeline_NoLock reflection lookup failed");

        object gate = typeof(BlockSyncClient).GetField("_gate", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(client)
            ?? throw new InvalidOperationException("BlockSyncClient _gate reflection value missing");

        lock (gate)
        {
            resetMethod.Invoke(client, new object[] { true });
        }

        bool commitLoopRunning = (bool)(commitLoopField.GetValue(client)
            ?? throw new InvalidOperationException("BlockSyncClient _commitLoopRunning reflection value missing"));
        Assert(!commitLoopRunning, "pipeline reset must clear the commit-loop latch so sync can restart cleanly");
    }

    private static void TestBlockSyncClientActiveStallWatchdogRevivesLocatorSync()
    {
        byte[] localTip = HashFromU64(10UL);
        UInt128 localChainwork = 10UL;
        var sent = new List<(string peer, MsgType type, byte[] payload)>();
        var peer = CreateFakePeer("peer-stall-watchdog");
        var sessions = new List<PeerSession> { peer };
        var log = new ListLogSink();

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
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
                Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            log: log);

        byte[] remoteTipHash = HashFromU64(200UL);
        client.OnTipStateAsync(peer, new SmallNetTipStateFrame(200UL, remoteTipHash, 200UL, null, new[] { HashFromU64(199UL), remoteTipHash }), CancellationToken.None)
            .GetAwaiter().GetResult();

        sent.Clear();

        FieldInfo commitLoopField = typeof(BlockSyncClient).GetField("_commitLoopRunning", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BlockSyncClient _commitLoopRunning reflection lookup failed");
        commitLoopField.SetValue(client, true);

        FieldInfo lastProgressField = typeof(BlockSyncClient).GetField("_lastProgressUtc", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BlockSyncClient _lastProgressUtc reflection lookup failed");
        lastProgressField.SetValue(client, DateTime.UtcNow - TimeSpan.FromSeconds(45));

        MethodInfo recoverMethod = typeof(BlockSyncClient).GetMethod("RecoverIdleBehindAsync", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BlockSyncClient RecoverIdleBehindAsync reflection lookup failed");
        ((Task)recoverMethod.Invoke(client, new object[] { CancellationToken.None })!)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1, $"stalled-sync watchdog should re-arm exactly one sync request, got {sent.Count}");
        Assert(sent[0].peer == peer.SessionKey && sent[0].type == MsgType.GetBlocksByLocator,
            "stalled-sync watchdog recovery must restart with a locator request");
        Assert(log.Lines.Exists(line => line.Contains("watchdog revived stalled-sync", StringComparison.Ordinal)),
            "stalled-sync watchdog should emit a diagnostic log line");
    }

    private static void TestBlockSyncClientResumeHashPrefersLocalTipWhenCommittedCursorIsStale()
    {
        byte[] localTip = HashFromU64(99UL);
        byte[] staleCommitted = HashFromU64(42UL);
        var peer = CreateFakePeer("peer-resume");

        var client = new BlockSyncClient(
            sessionSnapshot: () => new[] { peer },
            sendFrameAsync: (targetPeer, type, payload, ct) => Task.CompletedTask,
            getLocalTipChainwork: () => 99UL,
            getLocalTipHash: () => (byte[])localTip.Clone(),
            prepareBatchAsync: (startHash, startHeight, advertisedTipChainwork, targetPeer, ct) =>
                Task.FromResult(new BlockSyncPrepareResult(true, string.Empty)),
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
                Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            log: null);

        typeof(BlockSyncClient).GetField("_haveCommittedCursor", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(client, true);
        typeof(BlockSyncClient).GetField("_committedHash", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(client, (byte[])staleCommitted.Clone());

        MethodInfo resumeMethod = typeof(BlockSyncClient).GetMethod("GetResumeHash_NoLock", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BlockSyncClient GetResumeHash_NoLock reflection lookup failed");
        object gate = typeof(BlockSyncClient).GetField("_gate", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(client)
            ?? throw new InvalidOperationException("BlockSyncClient _gate reflection value missing");

        byte[] resumeHash;
        lock (gate)
        {
            resumeHash = (byte[])resumeMethod.Invoke(client, null)!
                ?? throw new InvalidOperationException("BlockSyncClient GetResumeHash_NoLock reflection value missing");
        }

        Assert(BytesEqual(resumeHash, localTip), "resume hash should prefer the actual local tip when the committed cursor is stale");
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
            commitBlocksAsync: (payloads, height, prevHash, peer, ct) =>
            {
                ulong lastHeight = height + (ulong)payloads.Count - 1UL;
                localHeight = lastHeight;
                localChainwork = (UInt128)lastHeight;
                localTip = HashFromU64(lastHeight + 10_000UL);
                return Task.FromResult(new BlockSyncCommitResult(true, (byte[])localTip.Clone(), string.Empty));
            },
            completeBatchAsync: (peer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (peer, reason, ct) => Task.CompletedTask,
            log: null);

        byte[] startHash = (byte[])localTip.Clone();
        byte[] remoteTipHash = HashFromU64(200UL);

        client.OnTipStateAsync(peer1, new SmallNetTipStateFrame(200UL, remoteTipHash, 200UL, null, new[] { HashFromU64(199UL), remoteTipHash }), CancellationToken.None).GetAwaiter().GetResult();
        Assert(sent.Count == 1, $"expected 1 initial sync request, got {sent.Count}");
        Assert(sent[0].peer == peer1.SessionKey && sent[0].type == MsgType.GetBlocksByLocator,
            "first sync request must be locator-based");

        Guid peer1BatchId = Guid.NewGuid();
        client.OnBlocksBatchStartAsync(peer1, new SmallNetBlocksBatchStartFrame(peer1BatchId, startHash, 10UL, 129, remoteTipHash, 200UL), CancellationToken.None)
            .GetAwaiter().GetResult();
        var batchPayloads = new List<byte[]>(BlockSyncProtocol.BatchMaxBlocks);
        for (int i = 0; i < BlockSyncProtocol.BatchMaxBlocks; i++)
            batchPayloads.Add(new byte[] { (byte)(i + 1) });

        client.OnBlocksBatchDataAsync(
                peer1,
                SmallNetSyncProtocol.BuildBlocksBatchData(11UL, batchPayloads),
                CancellationToken.None)
            .GetAwaiter().GetResult();
        ulong expectedCommittedHeight = 10UL + (ulong)BlockSyncProtocol.BatchMaxBlocks;

        Assert(localHeight == expectedCommittedHeight, $"expected local height {expectedCommittedHeight} after committed sync batch, got {localHeight}");

        sent.Clear();
        sessions.Clear();
        client.OnPeerDisconnectedAsync(peer1, CancellationToken.None).GetAwaiter().GetResult();

        sessions.Add(peer2);
        client.OnTipStateAsync(peer2, new SmallNetTipStateFrame(200UL, remoteTipHash, 200UL, null, new[] { HashFromU64(199UL), remoteTipHash }), CancellationToken.None).GetAwaiter().GetResult();

        Assert(sent.Count == 1, $"expected exactly one resume request, got {sent.Count}");
        Assert(sent[0].peer == peer2.SessionKey && sent[0].type == MsgType.GetBlocksFrom,
            "resume request must use GetBlocksFrom");
        Assert(BlockSyncProtocol.TryParseGetBlocksFrom(sent[0].payload, out var fromHash, out var maxBlocks),
            "resume request payload did not parse");
        Assert(BytesEqual(fromHash, localTip), "resume request did not use last committed hash");
        Assert(maxBlocks == BlockSyncProtocol.BatchMaxBlocks, "resume request max_blocks mismatch");
    }

    private static void TestBlockSyncClientDisconnectDuringPrepareDoesNotLeaveStaleBulkSyncBatch()
    {
        var mempool = new MempoolManager(
            senderHex => StateStore.GetBalanceU64(senderHex),
            senderHex => StateStore.GetNonceU64(senderHex),
            log: null);
        using var gate = new SemaphoreSlim(1, 1);
        var runtime = new BulkSyncRuntime(gate, mempool, () => { }, log: null);

        var peer1 = CreateFakePeer("peer-race-1");
        var peer2 = CreateFakePeer("peer-race-2");
        var sessions = new List<PeerSession> { peer1, peer2 };
        var sent = new List<(string peer, MsgType type)>();

        var genesis = BlockStore.GetBlockByHeight(0) ?? throw new InvalidOperationException("missing genesis block");
        var genesisHash = genesis.BlockHash ?? throw new InvalidOperationException("missing genesis hash");
        UInt128 localTipChainwork = BlockIndexStore.GetChainwork(genesisHash);
        UInt128 peer1TipChainwork = localTipChainwork == 0 ? 2UL : localTipChainwork + 2UL;
        UInt128 peer2TipChainwork = peer1TipChainwork + 1UL;
        byte[] remoteTipHash = HashFromU64(999UL);

        BlockSyncClient? client = null;
        bool injectedDisconnect = false;

        client = new BlockSyncClient(
            sessionSnapshot: () => sessions.ToArray(),
            sendFrameAsync: (peer, type, payload, ct) =>
            {
                sent.Add((peer.SessionKey, type));
                return Task.CompletedTask;
            },
            getLocalTipChainwork: () =>
            {
                lock (Db.Sync)
                {
                    var tipHash = BlockStore.GetCanonicalHashAtHeight(BlockStore.GetLatestHeight()) ?? Array.Empty<byte>();
                    return tipHash is { Length: 32 } ? BlockIndexStore.GetChainwork(tipHash) : 0;
                }
            },
            getLocalTipHash: () =>
            {
                lock (Db.Sync)
                    return BlockStore.GetCanonicalHashAtHeight(BlockStore.GetLatestHeight()) ?? Array.Empty<byte>();
            },
            prepareBatchAsync: async (startHash, startHeight, advertisedTipChainwork, peer, ct) =>
            {
                var prepare = await runtime.PrepareBatchAsync(startHash, startHeight, advertisedTipChainwork, peer, ct).ConfigureAwait(false);
                if (prepare.Success && !injectedDisconnect && string.Equals(peer.SessionKey, peer1.SessionKey, StringComparison.Ordinal))
                {
                    injectedDisconnect = true;
                    await client!.OnPeerDisconnectedAsync(peer1, CancellationToken.None).ConfigureAwait(false);
                }

                return prepare;
            },
            commitBlocksAsync: runtime.CommitBlocksAsync,
            completeBatchAsync: runtime.CompleteBatchAsync,
            abortBatchAsync: runtime.AbortBatchAsync,
            log: null);

        client.OnTipStateAsync(peer1, new SmallNetTipStateFrame(1UL, remoteTipHash, peer1TipChainwork, null, new[] { genesisHash, remoteTipHash }), CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(sent.Count == 1 && sent[0].peer == peer1.SessionKey && sent[0].type == MsgType.GetBlocksByLocator,
            "initial sync request must target the first peer via locator");

        client.OnBlocksBatchStartAsync(
                peer1,
                new SmallNetBlocksBatchStartFrame(Guid.NewGuid(), genesisHash, 0UL, 1, remoteTipHash, peer1TipChainwork),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        sent.Clear();
        client.OnTipStateAsync(peer2, new SmallNetTipStateFrame(1UL, remoteTipHash, peer2TipChainwork, null, new[] { genesisHash, remoteTipHash }), CancellationToken.None)
            .GetAwaiter().GetResult();
        Assert(sent.Count == 1 && sent[0].peer == peer2.SessionKey && sent[0].type == MsgType.GetBlocksByLocator,
            "after disconnect the client must retry sync against the second peer");

        var peer2Prepare = runtime.PrepareBatchAsync(genesisHash, 1UL, peer2TipChainwork, peer2, CancellationToken.None)
            .GetAwaiter().GetResult();

        if (peer2Prepare.Success)
        {
            runtime.AbortBatchAsync(peer2, "selftest cleanup", CancellationToken.None)
                .GetAwaiter().GetResult();
        }
        else
        {
            runtime.AbortBatchAsync(null, "selftest cleanup stale batch", CancellationToken.None)
                .GetAwaiter().GetResult();
        }

        Assert(peer2Prepare.Success,
            $"disconnect during batch prepare left a stale bulk-sync batch behind: {peer2Prepare.Error}");

        bool gateReleased = gate.Wait(0);
        Assert(gateReleased, "bulk-sync validation gate must be released after cleanup");
        if (gateReleased)
            gate.Release();
    }

    private static void TestBlockSyncClientMoreAvailableContinuesFromLastCommittedSyncPoint()
    {
        byte[] localTip = HashFromU64(10UL);
        UInt128 localChainwork = 10UL;
        var sent = new List<(string peer, MsgType type, byte[] payload)>();
        var peer = CreateFakePeer("peer-more-available");
        var sessions = new List<PeerSession> { peer };
        byte[] lastCommittedTip = Array.Empty<byte>();

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
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
            {
                localTip = HashFromU64(height + 10_000UL);
                lastCommittedTip = (byte[])localTip.Clone();
                localChainwork = height;
                return Task.FromResult(new BlockSyncCommitResult(true, (byte[])localTip.Clone(), string.Empty));
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

        Guid moreAvailableBatchId = Guid.NewGuid();
        sent.Clear();
        client.OnBlocksBatchStartAsync(peer, new SmallNetBlocksBatchStartFrame(moreAvailableBatchId, batchForkHash, 10UL, 1, remoteTipHash, 25UL), CancellationToken.None)
            .GetAwaiter().GetResult();
        client.OnBlocksChunkAsync(
                peer,
                SmallNetSyncProtocol.BuildBlocksChunk(11UL, new[] { new byte[] { 0x01 } }),
                CancellationToken.None)
            .GetAwaiter().GetResult();
        localTip = HashFromU64(12_000UL);
        localChainwork = 12UL;

        client.OnBlocksBatchEndAsync(
                peer,
                new SmallNetBlocksBatchEndFrame(moreAvailableBatchId, remoteTipHash, 11UL, MoreAvailable: true),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1, $"expected one continuation request after more-available, got {sent.Count}");
        Assert(sent[0].type == MsgType.GetBlocksFrom, $"expected GetBlocksFrom continuation, got {sent[0].type}");
        Assert(BlockSyncProtocol.TryParseGetBlocksFrom(sent[0].payload, out var fromHash, out var maxBlocks),
            "continuation request payload did not parse");
        Assert(BytesEqual(fromHash, lastCommittedTip), "continuation request must resume from the last committed sync hash");
        Assert(maxBlocks == BlockSyncProtocol.BatchMaxBlocks, "continuation request max_blocks mismatch");
    }

    private static void TestBlockSyncClientRemoteTipLatchSuppressesDuplicateFinishContinuation()
    {
        byte[] localTip = HashFromU64(10UL);
        UInt128 localChainwork = 10UL;
        var sent = new List<(string peer, MsgType type, byte[] payload)>();
        var log = new ListLogSink();
        var caps =
            HandshakeCapabilities.BlocksBatchData |
            HandshakeCapabilities.ExtendedSyncWindow |
            HandshakeCapabilities.SyncWindowPreview;
        var peer = CreateFakePeer("peer-tip-latch", caps);
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
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
                Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            log: log);

        byte[] previewHash = HashFromU64(11UL);
        byte[] remoteTipHash = HashFromU64(12UL);
        var remoteTip = new SmallNetTipStateFrame(12UL, remoteTipHash, 12UL, null, new[] { previewHash, remoteTipHash });

        client.OnTipStateAsync(peer, remoteTip, CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1 && sent[0].type == MsgType.GetBlocksByLocator,
            $"expected initial locator request, got {sent.Count}");

        sent.Clear();
        Guid locatorBatchId = Guid.NewGuid();
        client.OnBlocksBatchStartAsync(
                peer,
                new SmallNetBlocksBatchStartFrame(
                    locatorBatchId,
                    localTip,
                    10UL,
                    1,
                    remoteTipHash,
                    12UL,
                    previewHash,
                    11UL),
                CancellationToken.None)
            .GetAwaiter().GetResult();
        client.OnBlocksChunkAsync(
                peer,
                SmallNetSyncProtocol.BuildBlocksChunk(11UL, new[] { new byte[] { 0x01 } }),
                CancellationToken.None)
            .GetAwaiter().GetResult();
        client.OnBlocksBatchEndAsync(
                peer,
                new SmallNetBlocksBatchEndFrame(locatorBatchId, previewHash, 11UL, MoreAvailable: true),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1 && sent[0].type == MsgType.GetBlocksFrom,
            $"expected preview continuation request, got {sent.Count}");
        Assert(BlockSyncProtocol.TryParseGetBlocksFrom(sent[0].payload, out var fromHash, out _),
            "preview continuation payload did not parse");
        Assert(BytesEqual(fromHash, previewHash), "preview continuation must resume from the preview hash");

        sent.Clear();
        Guid finishBatchId = Guid.NewGuid();
        client.OnBlocksBatchStartAsync(
                peer,
                new SmallNetBlocksBatchStartFrame(
                    finishBatchId,
                    previewHash,
                    11UL,
                    0,
                    remoteTipHash,
                    12UL),
                CancellationToken.None)
            .GetAwaiter().GetResult();
        client.OnBlocksBatchEndAsync(
                peer,
                new SmallNetBlocksBatchEndFrame(finishBatchId, previewHash, 11UL, MoreAvailable: false),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        client.OnTipStateAsync(peer, remoteTip, CancellationToken.None)
            .GetAwaiter().GetResult();
        client.OnTipStateAsync(peer, remoteTip, CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 0,
            $"same exhausted from-hash must not be requested again while peer tip is unchanged, got {sent.Count}");
        Assert(log.Lines.Count(l => l.Contains("Block sync reached remote tip", StringComparison.Ordinal)) == 1,
            "remote tip should only be logged once for the same exhausted tip");

        sent.Clear();
        byte[] newerTipHash = HashFromU64(13UL);
        client.OnTipStateAsync(
                peer,
                new SmallNetTipStateFrame(13UL, newerTipHash, 13UL, null, new[] { remoteTipHash, newerTipHash }),
                CancellationToken.None)
            .GetAwaiter().GetResult();

        Assert(sent.Count == 1 && sent[0].type == MsgType.GetBlocksFrom,
            "a newer peer tip must clear the finish latch and allow a fresh continuation request");
        Assert(BlockSyncProtocol.TryParseGetBlocksFrom(sent[0].payload, out var resumedFromHash, out _),
            "refreshed continuation payload did not parse");
        Assert(BytesEqual(resumedFromHash, previewHash),
            "fresh continuation after a newer tip should resume from the last exhausted sync point");
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
            commitBlocksAsync: (payloads, height, prevHash, peer, ct) =>
                Task.FromResult(new BlockSyncCommitResult(false, Array.Empty<byte>(), "invalid block")),
            completeBatchAsync: (peer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (peer, reason, ct) => Task.CompletedTask,
            penalizePeer: (peer, reason) => penalized.Add(peer.SessionKey),
            log: null);

        byte[] startHash = (byte[])localTip.Clone();
        byte[] remoteTipHash = HashFromU64(25UL);
        var tipState = new SmallNetTipStateFrame(25UL, remoteTipHash, 25UL, null, new[] { HashFromU64(24UL), remoteTipHash });

        client.OnTipStateAsync(peer1, tipState, CancellationToken.None).GetAwaiter().GetResult();
        client.OnTipStateAsync(peer2, tipState, CancellationToken.None).GetAwaiter().GetResult();
        Guid invalidBatchId = Guid.NewGuid();
        client.OnBlocksBatchStartAsync(peer1, new SmallNetBlocksBatchStartFrame(invalidBatchId, startHash, 10UL, 1, remoteTipHash, 25UL), CancellationToken.None)
            .GetAwaiter().GetResult();

        sent.Clear();
        client.OnBlocksChunkAsync(
                peer1,
                SmallNetSyncProtocol.BuildBlocksChunk(11UL, new[] { new byte[] { 0xFF } }),
                CancellationToken.None)
            .GetAwaiter().GetResult();
        client.OnBlocksBatchEndAsync(
                peer1,
                new SmallNetBlocksBatchEndFrame(invalidBatchId, remoteTipHash, 11UL, MoreAvailable: false),
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

    private static List<Block> BuildCanonicalChain(ulong tipHeight)
    {
        var result = new List<Block>();
        if (tipHeight == 0)
            return result;

        var genesis = BlockStore.GetBlockByHeight(0) ?? throw new InvalidOperationException("missing genesis block");
        byte[] prevHash = genesis.BlockHash ?? throw new InvalidOperationException("missing genesis hash");
        ulong ts = genesis.Header!.Timestamp;
        var miner = KeyGenerator.GenerateKeypairHex();

        for (ulong height = 1; height <= tipHeight; height++)
        {
            var block = BuildMinedBlock(
                height: height,
                prevHash: prevHash,
                timestamp: ts + 61UL,
                minerPubHex: miner.pubHex);
            BlockPersistHelper.Persist(block);
            result.Add(block);
            prevHash = block.BlockHash!;
            ts = block.Header!.Timestamp;
        }

        return result;
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
            commitBlocksAsync: (payloads, height, prevHash, targetPeer, ct) =>
                Task.FromResult(new BlockSyncCommitResult(true, HashFromU64(height + (ulong)payloads.Count - 1UL), string.Empty)),
            completeBatchAsync: (targetPeer, status, ct) => Task.CompletedTask,
            abortBatchAsync: (targetPeer, reason, ct) => Task.CompletedTask,
            log: null);

    private static void SetBlockSyncClientStateForTest(BlockSyncClient client, BlockSyncState state)
    {
        FieldInfo field = typeof(BlockSyncClient).GetField("_state", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BlockSyncClient _state reflection lookup failed");
        field.SetValue(client, state);
    }

    private static PeerSession CreateFakePeer(string key, HandshakeCapabilities capabilities = HandshakeCapabilities.None)
        => new()
        {
            Client = null!,
            Stream = null!,
            RemoteEndpoint = key,
            RemoteBanKey = key,
            HandshakeOk = true,
            IsInbound = false,
            Capabilities = capabilities
        };

    private static void MarkPeerPublicForTest(P2PNode node, string ip, int port)
    {
        MethodInfo method = typeof(P2PNode).GetMethod("MarkPeerAsPublic", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("MarkPeerAsPublic reflection lookup failed");
        method.Invoke(node, new object[] { ip, port });
    }

    private static List<(string ip, int port)> ParsePeersPayloadForTest(byte[] payload)
    {
        var result = new List<(string ip, int port)>();
        if (payload == null || payload.Length < 2)
            return result;

        int idx = 0;
        ushort declared = BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(idx, 2));
        idx += 2;

        for (int i = 0; i < declared && idx < payload.Length; i++)
        {
            int len = payload[idx++];
            if (len <= 0 || idx + len + 2 > payload.Length)
                break;

            string ip = Encoding.UTF8.GetString(payload, idx, len);
            idx += len;
            int port = BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(idx, 2));
            idx += 2;
            result.Add((NormalizePeerHostForTest(ip), port));
        }

        return result;
    }

    private static string NormalizePeerHostForTest(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        string s = value.Trim();
        int zone = s.IndexOf('%');
        if (zone > 0)
            s = s[..zone];

        s = s.Trim().ToLowerInvariant();
        if (s.StartsWith("[", StringComparison.Ordinal) && s.EndsWith("]", StringComparison.Ordinal) && s.Length > 2)
            s = s[1..^1];

        if (!IPAddress.TryParse(s, out var addr))
            return s;

        if (addr.IsIPv4MappedToIPv6)
            addr = addr.MapToIPv4();

        return addr.ToString().ToLowerInvariant();
    }

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

