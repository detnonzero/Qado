using System.Threading;
using System.Threading.Tasks;
using Qado.Logging;
using Qado.Mempool;
using Qado.Storage;

namespace Qado.Networking
{
    public static class GenesisBootstrapper
    {
        public static Task<P2PNode> BootstrapAsync(MempoolManager mempool, ILogSink? log, CancellationToken ct)
        {
            try { PeerStore.PruneAndEnforceLimits(); } catch { }
            log?.Info("Bootstrap",
                $"Peer limits: maxPortsPerIp={PeerStore.MaxPortsPerIp}, maxTotal={PeerStore.MaxPeersTotal}, ttlDays={PeerStore.PeerTtlSeconds / 86400UL}");

            var node = new P2PNode(mempool, log);
            node.Start(GenesisConfig.P2PPort, ct);

            _ = node.ConnectKnownPeersAsync(ct);

            _ = Task.Run(() => BlockSyncStarter.StartAsync(mempool, log, ct), ct);

            node.StartPeerExchangeLoop(ct);

            log?.Info("Bootstrap", "P2P started, known peers dialed (if any), blocksync triggered.");
            return Task.FromResult(node);
        }
    }
}

