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
                var startupAudit = StartupIntegrityAudit.Run();
                startupAudit = StartupIntegrityAudit.RepairDerivedStateIfNeeded(startupAudit);
                if (startupAudit.Disposition == StartupAuditDisposition.HardStopCanonCorruption)
                {
                    var recoveryChoice = MessageBox.Show(
                        $"Startup integrity audit failed:\n\n{startupAudit.Issues[0].Message}\n\n" +
                        "Yes: Back up the current chain database and start a fresh chain resync.\n" +
                        "No: Open the data directory and exit.\n" +
                        "Cancel: Exit without changing anything.",
                        "QADO Recovery",
                        MessageBoxButton.YesNoCancel,
                        MessageBoxImage.Warning);

                    if (recoveryChoice == MessageBoxResult.Yes)
                    {
                        var resync = StartupRecovery.PerformBackupAndChainResync(dataDir, dbPath);
                        startupAudit = StartupIntegrityAudit.Run();
                        startupAudit = StartupIntegrityAudit.RepairDerivedStateIfNeeded(startupAudit);
                        StartupIntegrityAudit.ThrowIfBlocking(startupAudit);

                        MessageBox.Show(
                            $"Chain database backed up to:\n{resync.BackupDirectoryPath}\n\n" +
                            $"Preserved wallet keys: {resync.PreservedKeyCount}\n\n" +
                            "QADO will continue startup and resync the chain from genesis.",
                            "QADO Recovery",
                            MessageBoxButton.OK,
                            MessageBoxImage.Information);
                    }
                    else
                    {
                        if (recoveryChoice == MessageBoxResult.No)
                        {
                            try { StartupRecovery.OpenDataDirectory(dataDir); } catch { }
                        }

                        ReleaseSingleInstanceGuard();
                        Shutdown();
                        return;
                    }
                }

                StartupIntegrityAudit.ThrowIfBlocking(startupAudit);
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

