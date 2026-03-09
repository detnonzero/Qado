using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Qado.Networking
{
    public sealed class SmallNetPeerState
    {
        public string SessionKey { get; }
        public byte ProtocolVersion { get; private set; }
        public byte[] NodeId { get; private set; } = Array.Empty<byte>();
        public ulong TipHeight { get; private set; }
        public byte[] TipHash { get; private set; } = Array.Empty<byte>();
        public UInt128 TipChainwork { get; private set; }
        public byte[]? StateDigest { get; private set; }
        public IReadOnlyList<byte[]> RecentCanonical { get; private set; } = Array.Empty<byte[]>();
        public DateTime LastHelloUtc { get; private set; } = DateTime.MinValue;
        public DateTime LastTipStateUtc { get; private set; } = DateTime.MinValue;
        public DateTime LastBlockUtc { get; private set; } = DateTime.MinValue;

        public SmallNetPeerState(string sessionKey)
        {
            SessionKey = sessionKey ?? string.Empty;
        }

        public void UpdateHello(SmallNetHelloFrame frame, DateTime nowUtc)
        {
            ProtocolVersion = frame.ProtocolVersion;
            NodeId = CloneOrEmpty(frame.NodeId);
            TipHeight = frame.TipHeight;
            TipHash = CloneOrEmpty(frame.TipHash);
            TipChainwork = frame.TipChainwork;
            StateDigest = CloneOrNull(frame.StateDigest);
            LastHelloUtc = nowUtc;
            if (LastTipStateUtc == DateTime.MinValue)
                LastTipStateUtc = nowUtc;
        }

        public void UpdateTipState(SmallNetTipStateFrame frame, DateTime nowUtc)
        {
            TipHeight = frame.Height;
            TipHash = CloneOrEmpty(frame.TipHash);
            TipChainwork = frame.Chainwork;
            StateDigest = CloneOrNull(frame.StateDigest);
            RecentCanonical = CloneHashes(frame.RecentCanonical);
            LastTipStateUtc = nowUtc;
        }

        public void NoteBlockSeen(byte[] blockHash, ulong blockHeight, DateTime nowUtc)
        {
            if (blockHash is { Length: 32 })
                TipHash = (byte[])blockHash.Clone();
            if (blockHeight >= TipHeight)
                TipHeight = blockHeight;
            LastBlockUtc = nowUtc;
        }

        public SmallNetGapDecision ClassifyGap(SmallNetLocalChainView localView, int smallGapThreshold = 8)
        {
            if (localView.TipHash is not { Length: 32 } || TipHash is not { Length: 32 })
                return new SmallNetGapDecision(SmallNetGapKind.Unknown, 0, SameTip: false, LikelyFork: false);

            bool sameTip = BytesEqual(localView.TipHash, TipHash);
            if (sameTip)
                return new SmallNetGapDecision(SmallNetGapKind.InSync, 0, SameTip: true, LikelyFork: false);

            int gap = Math.Abs((int)Math.Min(int.MaxValue, (long)localView.Height - (long)TipHeight));
            bool sharesRecentCanon = RecentCanonical.Any(remoteHash =>
                                         remoteHash is { Length: 32 } &&
                                         localView.RecentCanonical.Any(localHash =>
                                             localHash is { Length: 32 } &&
                                             BytesEqual(remoteHash, localHash))) ||
                                     RecentCanonical.Any(hash => hash is { Length: 32 } && BytesEqual(hash, localView.TipHash)) ||
                                     localView.RecentCanonical.Any(hash => hash is { Length: 32 } && BytesEqual(hash, TipHash));
            bool likelyFork = sharesRecentCanon || (localView.Height == TipHeight && localView.Chainwork != TipChainwork);

            if (localView.Chainwork > TipChainwork)
            {
                return new SmallNetGapDecision(
                    gap <= smallGapThreshold ? SmallNetGapKind.LocalAheadSmall : SmallNetGapKind.LocalAheadLarge,
                    gap,
                    SameTip: false,
                    LikelyFork: likelyFork);
            }

            if (localView.Chainwork < TipChainwork)
            {
                return new SmallNetGapDecision(
                    gap <= smallGapThreshold ? SmallNetGapKind.RemoteAheadSmall : SmallNetGapKind.RemoteAheadLarge,
                    gap,
                    SameTip: false,
                    LikelyFork: likelyFork);
            }

            return new SmallNetGapDecision(SmallNetGapKind.CompetingTip, gap, SameTip: false, LikelyFork: true);
        }

        private static byte[] CloneOrEmpty(byte[]? value)
            => value is { Length: > 0 } ? (byte[])value.Clone() : Array.Empty<byte>();

        private static byte[]? CloneOrNull(byte[]? value)
            => value is { Length: > 0 } ? (byte[])value.Clone() : null;

        private static IReadOnlyList<byte[]> CloneHashes(IReadOnlyList<byte[]>? hashes)
        {
            hashes ??= Array.Empty<byte[]>();
            var copy = new List<byte[]>(hashes.Count);
            for (int i = 0; i < hashes.Count; i++)
                copy.Add(CloneOrEmpty(hashes[i]));
            return copy;
        }

        private static bool BytesEqual(byte[] left, byte[] right)
        {
            if (left.Length != right.Length)
                return false;

            for (int i = 0; i < left.Length; i++)
            {
                if (left[i] != right[i])
                    return false;
            }

            return true;
        }
    }

    public sealed class SmallNetPeerDirectory
    {
        private readonly ConcurrentDictionary<string, SmallNetPeerState> _peers = new(StringComparer.Ordinal);

        public SmallNetPeerState GetOrCreate(string sessionKey)
            => _peers.GetOrAdd(sessionKey ?? string.Empty, key => new SmallNetPeerState(key));

        public SmallNetPeerState UpdateHello(string sessionKey, SmallNetHelloFrame frame, DateTime nowUtc)
        {
            var state = GetOrCreate(sessionKey);
            state.UpdateHello(frame, nowUtc);
            return state;
        }

        public SmallNetPeerState UpdateTipState(string sessionKey, SmallNetTipStateFrame frame, DateTime nowUtc)
        {
            var state = GetOrCreate(sessionKey);
            state.UpdateTipState(frame, nowUtc);
            return state;
        }

        public bool TryGet(string sessionKey, out SmallNetPeerState state)
            => _peers.TryGetValue(sessionKey ?? string.Empty, out state!);

        public IReadOnlyList<SmallNetPeerState> Snapshot()
            => _peers.Values.ToArray();
    }
}
