using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using NSec.Cryptography;
using Qado.Blockchain;
using Qado.Networking;
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

