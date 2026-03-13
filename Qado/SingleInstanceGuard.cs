using System;
using System.Threading;
using Qado.Networking;

namespace Qado
{
    internal sealed class SingleInstanceGuard : IDisposable
    {
        private readonly Mutex _mutex;
        private readonly bool _ownsMutex;

        private SingleInstanceGuard(Mutex mutex, bool ownsMutex)
        {
            _mutex = mutex;
            _ownsMutex = ownsMutex;
        }

        public bool HasHandle => _ownsMutex;

        public static SingleInstanceGuard AcquireForCurrentNetwork()
        {
            string networkName = string.IsNullOrWhiteSpace(NetworkParams.Name)
                ? "unknown"
                : NetworkParams.Name.Trim().ToLowerInvariant();

            string mutexName = $@"Global\Qado-{networkName}-{NetworkParams.NetworkId:x2}";
            var mutex = new Mutex(initiallyOwned: false, name: mutexName);

            bool ownsMutex;
            try
            {
                ownsMutex = mutex.WaitOne(0, false);
            }
            catch (AbandonedMutexException)
            {
                ownsMutex = true;
            }

            return new SingleInstanceGuard(mutex, ownsMutex);
        }

        public void Dispose()
        {
            try
            {
                if (_ownsMutex)
                    _mutex.ReleaseMutex();
            }
            catch (ApplicationException)
            {
            }
            finally
            {
                _mutex.Dispose();
            }
        }
    }
}
