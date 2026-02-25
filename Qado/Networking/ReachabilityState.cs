using System;

namespace Qado.Networking
{
    public sealed class ReachabilityState
    {
        private readonly object _gate = new();

        public bool IsPublic { get; private set; }
        public DateTime LastInboundUtc { get; private set; } = DateTime.MinValue;

        public void MarkInbound()
        {
            lock (_gate)
            {
                IsPublic = true;
                LastInboundUtc = DateTime.UtcNow;
            }
        }

        public void DecayIfExpired(TimeSpan timeout)
        {
            if (timeout <= TimeSpan.Zero)
                return;

            lock (_gate)
            {
                if (!IsPublic)
                    return;

                if (DateTime.UtcNow - LastInboundUtc > timeout)
                    IsPublic = false;
            }
        }
    }
}
