using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using Qado.Blockchain;

namespace Qado.Utils
{
    public static class MerkleUtil
    {
        private const int HashSize = 32;

        public static byte[] ComputeMerkleRoot(IReadOnlyList<byte[]> leafHashes)
        {
            if (leafHashes is null || leafHashes.Count == 0)
                return new byte[HashSize];

            var current = new List<byte[]>(leafHashes.Count);
            for (int i = 0; i < leafHashes.Count; i++)
            {
                current.Add(HashLeaf(EnsureHash32(leafHashes[i], $"leafHashes[{i}]")));
            }

            while (current.Count > 1)
            {
                var next = new List<byte[]>((current.Count + 1) / 2);

                for (int i = 0; i < current.Count; i += 2)
                {
                    var left = current[i];
                    var right = (i + 1 < current.Count) ? current[i + 1] : left;

                    next.Add(HashNode(left, right));
                }

                current = next;
            }

            return current[0];
        }

        public static byte[] ComputeMerkleRootFromTransactions(IReadOnlyList<Transaction> transactions)
        {
            if (transactions is null || transactions.Count == 0)
                return new byte[HashSize];

            var leaves = new List<byte[]>(transactions.Count);
            for (int i = 0; i < transactions.Count; i++)
            {
                if (transactions[i] is null)
                    throw new InvalidOperationException($"transactions[{i}] is null.");
                leaves.Add(transactions[i].ComputeTransactionHash());
            }

            return ComputeMerkleRoot(leaves);
        }

        public static string ComputeMerkleRootHex(IReadOnlyList<byte[]> leafHashes)
        {
            var root = ComputeMerkleRoot(leafHashes);
            return Convert.ToHexString(root).ToLowerInvariant();
        }

        private static byte[] HashLeaf(byte[] leafHash)
        {
            if (leafHash.Length != HashSize)
                throw new InvalidOperationException("leaf hash must be 32 bytes.");

            Span<byte> input = stackalloc byte[1 + HashSize];
            input[0] = ConsensusRules.MerkleLeafDomainTag;
            leafHash.AsSpan(0, HashSize).CopyTo(input.Slice(1, HashSize));
            return SHA256.HashData(input);
        }

        private static byte[] HashNode(byte[] left, byte[] right)
        {
            if (left is not { Length: HashSize } || right is not { Length: HashSize })
                throw new InvalidOperationException("Merkle node input hashes must be 32 bytes.");

            Span<byte> input = stackalloc byte[1 + HashSize + HashSize];
            input[0] = ConsensusRules.MerkleNodeDomainTag;
            left.AsSpan(0, HashSize).CopyTo(input.Slice(1, HashSize));
            right.AsSpan(0, HashSize).CopyTo(input.Slice(1 + HashSize, HashSize));
            return SHA256.HashData(input);
        }

        private static byte[] EnsureHash32(byte[]? value, string name)
        {
            if (value is null)
                throw new InvalidOperationException($"{name} is null.");
            if (value.Length != HashSize)
                throw new InvalidOperationException($"{name} must be {HashSize} bytes.");
            return value;
        }
    }
}

