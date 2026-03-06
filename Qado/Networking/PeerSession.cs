using System;
using System.Net.Sockets;
using System.Threading;

namespace Qado.Networking
{
    public sealed class PeerSession
    {
        public TcpClient Client { get; init; } = null!;
        public NetworkStream Stream { get; init; } = null!;
        public string RemoteEndpoint { get; init; } = string.Empty;
        public string RemoteBanKey { get; set; } = string.Empty;
        public string? RemoteIpAdvertised { get; set; }
        public int? RemotePortAdvertised { get; set; }
        public bool HandshakeOk { get; set; }
        public bool IsInbound { get; init; }
        public bool RemoteClaimsListening { get; set; }
        public bool RemoteIsPublic { get; set; }
        public DateTime ConnectedUtc { get; } = DateTime.UtcNow;
        public DateTime LastMessageUtc { get; set; } = DateTime.UtcNow;
        public long LastPingSentUnixMs { get; set; }
        public int LastLatencyMs { get; set; } = -1;
        public DateTime LastLatencyUpdatedUtc { get; set; } = DateTime.MinValue;
        public int PingTimeoutStreak { get; set; }
        public SemaphoreSlim SendLock { get; } = new(1, 1);

        public string SessionKey => RemoteEndpoint;
    }
}
