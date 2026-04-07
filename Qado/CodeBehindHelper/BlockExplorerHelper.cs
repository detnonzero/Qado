using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Numerics;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Qado.Blockchain;
using Qado.Serialization;
using Qado.Storage;
using Qado.Utils;

namespace Qado.CodeBehindHelper
{
    public static class BlockExplorerHelper
    {
        public sealed class BlockSummary
        {
            public ulong Height { get; set; }
            public byte[] Hash { get; set; } = Array.Empty<byte>();
            public byte[] PreviousHash { get; set; } = Array.Empty<byte>();
            public ulong Timestamp { get; set; }
            public byte[] Miner { get; set; } = Array.Empty<byte>();
            public ulong Nonce { get; set; }
            public byte[] Target { get; set; } = Array.Empty<byte>();
            public byte[] MerkleRoot { get; set; } = Array.Empty<byte>();
            public byte Version { get; set; }
            public int TxCount { get; set; }
        }

        public sealed class BlockUiTag
        {
            public ulong Height { get; }
            public byte[] Hash { get; } // canonical block hash (32 bytes)

            public BlockUiTag(ulong height, byte[] hash)
            {
                if (hash is not { Length: 32 })
                    throw new ArgumentException("hash must be 32 bytes.", nameof(hash));

                Height = height;
                Hash = (byte[])hash.Clone();
            }
        }

        public static List<Block> ReadBlocksDescendingFrom(ulong startHeight, int count)
        {
            var list = new List<Block>(Math.Max(0, count));
            if (count <= 0) return list;

            ulong h = startHeight;

            while (list.Count < count)
            {
                var hash = BlockStore.GetCanonicalHashAtHeight(h);
                if (hash is { Length: 32 })
                {
                    var b = BlockStore.GetBlockByHash(hash);
                    if (b != null)
                    {
                        b.BlockHeight = h;
                        b.BlockHash = (byte[])hash.Clone();
                        list.Add(b);
                    }
                }

                if (h == 0) break;
                h--;
            }

            return list;
        }

        public static List<BlockSummary> ReadBlockSummariesDescendingFrom(ulong startHeight, int count)
        {
            var list = new List<BlockSummary>(Math.Max(0, count));
            if (count <= 0) return list;

            lock (Db.Sync)
            {
                using var cmd = Db.Connection.CreateCommand();
                cmd.CommandText = @"
SELECT c.height, c.hash, bi.prev_hash, bi.ts, bi.target, bi.miner, p.payload
FROM canon c
JOIN block_index bi ON bi.hash = c.hash
JOIN block_payloads p ON p.hash = c.hash
WHERE c.height <= $startHeight
ORDER BY c.height DESC
LIMIT $count;";
                cmd.Parameters.AddWithValue("$startHeight", (long)startHeight);
                cmd.Parameters.AddWithValue("$count", count);

                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    long heightRaw = reader.GetInt64(0);
                    var hash = reader[1] as byte[];
                    var prevHash = reader[2] as byte[];
                    long tsRaw = reader.GetInt64(3);
                    var target = reader[4] as byte[];
                    var miner = reader[5] as byte[];
                    var payload = reader[6] as byte[];

                    if (heightRaw < 0 ||
                        hash is not { Length: 32 } ||
                        prevHash is not { Length: 32 } ||
                        target is not { Length: 32 } ||
                        miner is not { Length: 32 } ||
                        payload is not { Length: > 0 })
                    {
                        continue;
                    }

                    if (!TryReadPayloadSummary(payload, out var version, out var merkleRoot, out var nonce, out var txCount))
                        continue;

                    list.Add(new BlockSummary
                    {
                        Height = (ulong)heightRaw,
                        Hash = (byte[])hash.Clone(),
                        PreviousHash = (byte[])prevHash.Clone(),
                        Timestamp = tsRaw < 0 ? 0UL : (ulong)tsRaw,
                        Miner = (byte[])miner.Clone(),
                        Nonce = nonce,
                        Target = (byte[])target.Clone(),
                        MerkleRoot = merkleRoot,
                        Version = version,
                        TxCount = txCount
                    });
                }
            }

            return list;
        }

        public static StackPanel BuildBlockUi(Block block)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            if (block.Header == null) throw new ArgumentException("Block.Header is required.", nameof(block));
            if (block.BlockHash is not { Length: 32 }) throw new ArgumentException("Block.BlockHash must be 32 bytes.", nameof(block));

            var header = block.Header;

            var sortedTxs = BuildDisplayTxOrder(block.Transactions);

            var txPanel = new StackPanel
            {
                Visibility = Visibility.Collapsed,
                Margin = new Thickness(0, 2, 0, 0)
            };

            for (int i = 0; i < sortedTxs.Count; i++)
            {
                var tx = sortedTxs[i];
                if (tx == null) continue;

                var txText = new TextBlock
                {
                    Text =
                        $"TX[{i}] | From: {Hex(tx.Sender, 8)} → To: {Hex(tx.Recipient, 8)} | " +
                        $"Amount: {QadoAmountParser.FormatNanoToQado(tx.Amount)} | Fee: {QadoAmountParser.FormatNanoToQado(tx.Fee)} | " +
                        $"Nonce: {tx.TxNonce} | ChainId: {tx.ChainId}",
                    FontFamily = new FontFamily("Consolas"),
                    Margin = new Thickness(18, 2, 0, 2),
                    TextWrapping = TextWrapping.Wrap
                };

                txPanel.Children.Add(txText);
            }

            var ts = SafeUnixToLocal(header.Timestamp);

            BigInteger diff;
            try { diff = Difficulty.TargetToDifficulty(header.Target); }
            catch { diff = BigInteger.One; }

            var headerText = new TextBlock
            {
                Text =
                    $"Block {block.BlockHeight} | Hash: {Hex(block.BlockHash, 16)} | Prev: {Hex(header.PreviousBlockHash, 16)} | " +
                    $"Time: {ts} | Miner: {Hex(header.Miner, 8)} | Nonce: {header.Nonce} | Diff: {diff} | " +
                    $"Ver: {header.Version} | Merkle: {Hex(header.MerkleRoot, 16)}",
                FontFamily = new FontFamily("Consolas"),
                TextWrapping = TextWrapping.Wrap,
                Margin = new Thickness(5),
                Cursor = System.Windows.Input.Cursors.Hand
            };

            headerText.MouseLeftButtonUp += (_, __) =>
            {
                try
                {
                    if (Application.Current?.MainWindow is MainWindow mw)
                    {
                        mw.ShowBlockTransactions(block);
                        return;
                    }
                }
                catch
                {
                }

                txPanel.Visibility = txPanel.Visibility == Visibility.Visible
                    ? Visibility.Collapsed
                    : Visibility.Visible;
            };

            var container = new StackPanel
            {
                Margin = new Thickness(0, 0, 0, 10),
                Tag = new BlockUiTag(block.BlockHeight, block.BlockHash)
            };

            container.Children.Add(headerText);
            container.Children.Add(txPanel);

            return container;
        }

        public static bool HasGenesisMissing(Panel blockExplorerPanel)
        {
            if (blockExplorerPanel.Children.Count == 0) return true;

            for (int i = 0; i < blockExplorerPanel.Children.Count; i++)
            {
                if (TryGetContainerInfo(blockExplorerPanel.Children[i], out var h, out _))
                {
                    if (h == 0UL) return false;
                }
            }

            return true;
        }

        public static bool TryGetContainerInfo(UIElement container, out ulong height, out byte[]? hash)
        {
            height = 0;
            hash = null;

            if (container is not FrameworkElement fe)
                return false;

            if (fe.Tag is BlockUiTag tag)
            {
                height = tag.Height;
                hash = (byte[])tag.Hash.Clone();
                return hash is { Length: 32 };
            }

            if (fe.Tag is ulong u)
            {
                height = u;
                hash = null;
                return true;
            }

            return false;
        }

        public static ulong TryGetContainerBlockHeight(UIElement container)
            => TryGetContainerInfo(container, out var h, out _) ? h : 0UL;

        public static byte[]? TryGetContainerBlockHash(UIElement container)
            => TryGetContainerInfo(container, out _, out var hash) ? hash : null;


        private static List<Transaction> BuildDisplayTxOrder(List<Transaction> txs)
        {
            if (txs == null || txs.Count == 0)
                return new List<Transaction>(0);

            var result = new List<Transaction>(txs.Count);

            result.Add(txs[0]);

            if (txs.Count == 1)
                return result;

            var rest = new List<Transaction>(txs.Count - 1);
            for (int i = 1; i < txs.Count; i++)
            {
                if (txs[i] != null) rest.Add(txs[i]);
            }

            rest.Sort(static (a, b) =>
            {
                int c = b.Fee.CompareTo(a.Fee);
                if (c != 0) return c;

                c = CompareBytesLex(a.Sender, b.Sender);
                if (c != 0) return c;

                c = CompareBytesLex(a.Recipient, b.Recipient);
                if (c != 0) return c;

                return a.TxNonce.CompareTo(b.TxNonce);
            });

            result.AddRange(rest);
            return result;
        }

        private static bool TryReadPayloadSummary(
            byte[] payload,
            out byte version,
            out byte[] merkleRoot,
            out ulong nonce,
            out int txCount)
        {
            version = 0;
            merkleRoot = Array.Empty<byte>();
            nonce = 0;
            txCount = 0;

            if (payload == null || payload.Length < BlockBinarySerializer.HeaderSize)
                return false;

            int o = 0;
            version = payload[o++];

            o += 32; // prev
            merkleRoot = payload.AsSpan(o, 32).ToArray();
            o += 32;

            o += 8;  // timestamp
            o += 32; // target

            nonce = BinaryPrimitives.ReadUInt64BigEndian(payload.AsSpan(o, 8));
            o += 8;

            o += 32; // miner

            uint rawTxCount = BinaryPrimitives.ReadUInt32BigEndian(payload.AsSpan(o, 4));
            txCount = rawTxCount > int.MaxValue ? int.MaxValue : (int)rawTxCount;
            return true;
        }

        private static int CompareBytesLex(byte[]? a, byte[]? b)
        {
            if (ReferenceEquals(a, b)) return 0;
            if (a == null) return -1;
            if (b == null) return 1;

            int n = a.Length < b.Length ? a.Length : b.Length;
            for (int i = 0; i < n; i++)
            {
                int d = a[i].CompareTo(b[i]);
                if (d != 0) return d;
            }
            return a.Length.CompareTo(b.Length);
        }


        private static string Hex(byte[]? data, int takeBytes = -1)
        {
            if (data == null || data.Length == 0) return "";
            string hex = Convert.ToHexString(data).ToLowerInvariant();

            if (takeBytes > 0)
            {
                int takeChars = takeBytes * 2;
                return (hex.Length > takeChars) ? hex[..takeChars] + "…" : hex;
            }

            return hex;
        }

        private static string SafeUnixToLocal(ulong unixSeconds)
        {
            try
            {
                return DateTimeOffset.FromUnixTimeSeconds((long)unixSeconds)
                    .ToLocalTime()
                    .ToString("yyyy-MM-dd HH:mm:ss");
            }
            catch
            {
                return unixSeconds.ToString();
            }
        }
    }
}

