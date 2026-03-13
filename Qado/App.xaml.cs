using System;
using System.IO;
using System.Windows;
using Qado.Blockchain;
using Qado.CodeBehindHelper;
using Qado.Networking;
using Qado.Storage;

namespace Qado
{
    public partial class App : Application
    {
        private SingleInstanceGuard? _singleInstanceGuard;

        protected override void OnStartup(StartupEventArgs e)
        {
            base.OnStartup(e);
            ShutdownMode = ShutdownMode.OnMainWindowClose;

            try
            {
                _singleInstanceGuard = SingleInstanceGuard.AcquireForCurrentNetwork();
                if (!_singleInstanceGuard.HasHandle)
                {
                    ReleaseSingleInstanceGuard();
                    MessageBox.Show(
                        $"QADO is already running on this machine for {NetworkParams.Name}.",
                        "QADO", MessageBoxButton.OK, MessageBoxImage.Information);

                    Shutdown();
                    return;
                }

                SelfPeerGuard.InitializeAtStartup(GenesisConfig.P2PPort);

                var dataDir = Path.Combine(AppContext.BaseDirectory, NetworkParams.DataDirectoryName);
                Directory.CreateDirectory(dataDir);

                var dbPath = Path.Combine(dataDir, NetworkParams.DbFileName);
                Db.Initialize(dbPath);

                GenesisBlockProvider.EnsureGenesisBlockStored();
            }
            catch (Exception ex)
            {
                ReleaseSingleInstanceGuard();
                MessageBox.Show(
                    $"Startup failed:\n{ex.Message}",
                    "QADO", MessageBoxButton.OK, MessageBoxImage.Error);

                Shutdown();
                return;
            }
        }

        protected override void OnExit(ExitEventArgs e)
        {
            try { MiningHelper.StopMiningForShutdown(); } catch { }
            try { Db.Shutdown(); } catch { }
            ReleaseSingleInstanceGuard();

            base.OnExit(e);
        }

        private void ReleaseSingleInstanceGuard()
        {
            try { _singleInstanceGuard?.Dispose(); } catch { }
            _singleInstanceGuard = null;
        }
    }
}

