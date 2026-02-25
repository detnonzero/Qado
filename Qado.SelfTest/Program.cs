using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using NSec.Cryptography;
using Qado.Blockchain;
using Qado.Mempool;
using Qado.Networking;
using Qado.Serialization;
using Qado.Storage;
using Qado.Utils;

internal static class Program
{
    private const byte MerkleLeafTag = 0x00;
    private const byte MerkleNodeTag = 0x01;

    private static int _passed;
    private static int _failed;

    public static int Main()
    {
        string tempRoot = Path.Combine(Path.GetTempPath(), "qado-selftest-" + Guid.NewGuid().ToString("N"));

        try
        {
            SetupRuntime(tempRoot);

            Run("Genesis_Stored", TestGenesisStored);
            Run("StateApplier_RejectsNonceGap", TestStateApplierRejectsNonceGap);
            Run("StateApplier_AcceptsSequentialNonce", TestStateApplierAcceptsSequentialNonce);
            Run("KeyStore_EncryptsAtRest", TestKeyStoreEncryptsAtRest);
            Run("Merkle_DomainSeparation_Profile", TestMerkleDomainSeparationProfile);
            Run("TxId_CommitsSignature", TestTxIdCommitsSignature);
            Run("BlockSerializer_RejectsInvalidTarget", TestBlockSerializerRejectsInvalidTarget);
            Run("ChainSelector_Reorg_MempoolReconcile", TestChainSelectorReorgMempoolReconcile);
            Run("MempoolSelection_RespectsSenderNonceOrder", TestMempoolSelectionRespectsSenderNonceOrder);
        }
        catch (Exception ex)
        {
            _failed++;
            Console.WriteLine($"[FAIL] Harness_Setup: {ex.Message}");
        }
        finally
        {
            try { BlockLog.Shutdown(); } catch { }
            try { Db.Shutdown(); } catch { }
            try { if (Directory.Exists(tempRoot)) Directory.Delete(tempRoot, true); } catch { }
        }

        Console.WriteLine($"SelfTest complete: passed={_passed} failed={_failed}");
        return _failed == 0 ? 0 : 1;
    }

    private static void SetupRuntime(string tempRoot)
    {
        Directory.CreateDirectory(tempRoot);
        string dbPath = Path.Combine(tempRoot, "qado.db");
        string blocksPath = Path.Combine(tempRoot, "blocks.dat");

        Db.Initialize(dbPath);
        BlockLog.Initialize(blocksPath);
        GenesisBlockProvider.EnsureGenesisBlockStored();
    }

    private static void Run(string name, Action test)
    {
        try
        {
            test();
            _passed++;
            Console.WriteLine($"[PASS] {name}");
        }
        catch (Exception ex)
        {
            _failed++;
            Console.WriteLine($"[FAIL] {name}: {ex.Message}");
        }
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
        byte[] a = SHA256.HashData(new byte[] { 0xA1 });
        byte[] b = SHA256.HashData(new byte[] { 0xB2 });
        byte[] c = SHA256.HashData(new byte[] { 0xC3 });

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
        var target = block.Header?.Target ?? throw new InvalidOperationException("target missing");

        for (uint nonce = 0; ; nonce++)
        {
            block.Header!.Nonce = nonce;
            var hash = block.ComputeBlockHash();
            if (Difficulty.Meets(hash, target))
            {
                block.BlockHash = hash;
                return;
            }

            if (nonce == uint.MaxValue)
                throw new InvalidOperationException("mining failed: nonce exhausted");
        }
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

    private static void Assert(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }

    private static byte[] HashLeaf(byte[] leaf)
    {
        byte[] buf = new byte[1 + 32];
        buf[0] = MerkleLeafTag;
        Buffer.BlockCopy(leaf, 0, buf, 1, 32);
        return SHA256.HashData(buf);
    }

    private static byte[] HashNode(byte[] left, byte[] right)
    {
        byte[] buf = new byte[1 + 32 + 32];
        buf[0] = MerkleNodeTag;
        Buffer.BlockCopy(left, 0, buf, 1, 32);
        Buffer.BlockCopy(right, 0, buf, 1 + 32, 32);
        return SHA256.HashData(buf);
    }
}

