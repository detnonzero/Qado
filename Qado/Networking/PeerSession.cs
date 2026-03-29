using System;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading;

namespace Qado.Networking
{
    public readonly record struct QueuedRelayFrame(MsgType Type, byte[] Payload);

    public sealed class PeerSession
    {
        public TcpClient Client { get; init; } = null!;
        public NetworkStream Stream { get; init; } = null!;
        public string RemoteEndpoint { get; init; } = string.Empty;
        public string RemoteBanKey { get; set; } = string.Empty;
        public string? RemoteIpAdvertised { get; set; }
        public int? RemotePortAdvertised { get; set; }
        public byte[] RemoteNodeId { get; set; } = Array.Empty<byte>();
        public bool HandshakeOk { get; set; }
        public bool IsInbound { get; init; }
        public bool RemoteClaimsListening { get; set; }
        public bool RemoteIsPublic { get; set; }
        public HandshakeCapabilities Capabilities { get; set; }
        public DateTime ConnectedUtc { get; } = DateTime.UtcNow;
        public DateTime LastMessageUtc { get; set; } = DateTime.UtcNow;
        public long LastPingSentUnixMs { get; set; }
        public int LastLatencyMs { get; set; } = -1;
        public DateTime LastLatencyUpdatedUtc { get; set; } = DateTime.MinValue;
        public int PingTimeoutStreak { get; set; }
        public SemaphoreSlim SendLock { get; } = new(1, 1);
        public ConcurrentQueue<QueuedRelayFrame> RelayQueue { get; } = new();
        public SemaphoreSlim RelaySignal { get; } = new(0);
        public int RelayQueuedCount;
        public int RelayPumpStarted;
        public int RelayStopRequested;
        public object RelayOverflowLogGate { get; } = new();
        public DateTime RelayOverflowLastLogUtc { get; set; } = DateTime.MinValue;
        public int RelayOverflowSuppressed { get; set; }

        public string SessionKey => RemoteEndpoint;

        public bool Supports(HandshakeCapabilities capability)
            => capability == HandshakeCapabilities.None || (Capabilities & capability) == capability;
    }
}
