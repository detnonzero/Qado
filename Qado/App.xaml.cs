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
        protected override void OnStartup(StartupEventArgs e)
        {
            base.OnStartup(e);
            ShutdownMode = ShutdownMode.OnMainWindowClose;

            try
            {
                SelfPeerGuard.InitializeAtStartup(GenesisConfig.P2PPort);

                var dataDir = Path.Combine(AppContext.BaseDirectory, NetworkParams.DataDirectoryName);
                Directory.CreateDirectory(dataDir);

                var dbPath = Path.Combine(dataDir, NetworkParams.DbFileName);
                Db.Initialize(dbPath);

                var blocksPath = Path.Combine(dataDir, NetworkParams.BlockLogFileName);
                BlockLog.Initialize(blocksPath);

                GenesisBlockProvider.EnsureGenesisBlockStored();
            }
            catch (Exception ex)
            {
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
            try { BlockLog.Shutdown(); } catch { }
            try { Db.Shutdown(); } catch { }

            base.OnExit(e);
        }
    }
}

