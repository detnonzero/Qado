using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Qado.Blockchain;

namespace Qado.Storage
{
    public sealed record StartupResyncResult(
        string BackupDirectoryPath,
        int PreservedKeyCount);

    public static class StartupRecovery
    {
        public static StartupResyncResult PerformBackupAndChainResync(string dataDirectoryPath, string dbPath)
        {
            if (string.IsNullOrWhiteSpace(dataDirectoryPath))
                throw new ArgumentNullException(nameof(dataDirectoryPath));
            if (string.IsNullOrWhiteSpace(dbPath))
                throw new ArgumentNullException(nameof(dbPath));

            string fullDataDir = Path.GetFullPath(dataDirectoryPath);
            string fullDbPath = Path.GetFullPath(dbPath);
            if (!fullDbPath.StartsWith(fullDataDir, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("database path must live inside the selected data directory");

            var preservedKeys = ReadKeysFromCurrentDatabase();
            string backupDirectoryPath = CreateBackupDirectory(fullDataDir);

            Db.Shutdown();

            try
            {
                MoveDbArtifactsToBackup(fullDbPath, backupDirectoryPath);

                Db.Initialize(fullDbPath);
                GenesisBlockProvider.EnsureGenesisBlockStored();

                if (preservedKeys.Count > 0)
                    KeyStore.ReplaceAll(preservedKeys);

                return new StartupResyncResult(backupDirectoryPath, preservedKeys.Count);
            }
            catch
            {
                try { Db.Shutdown(); } catch { }
                throw;
            }
        }

        public static void OpenDataDirectory(string dataDirectoryPath)
        {
            if (string.IsNullOrWhiteSpace(dataDirectoryPath))
                throw new ArgumentNullException(nameof(dataDirectoryPath));

            string fullDataDir = Path.GetFullPath(dataDirectoryPath);
            Directory.CreateDirectory(fullDataDir);

            Process.Start(new ProcessStartInfo
            {
                FileName = fullDataDir,
                UseShellExecute = true
            });
        }

        private static List<(string PrivHex, string PubHex)> ReadKeysFromCurrentDatabase()
        {
            try
            {
                return KeyStore.GetAllKeys();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to read wallet keys before chain resync: {ex.Message}",
                    ex);
            }
        }

        private static string CreateBackupDirectory(string dataDirectoryPath)
        {
            string backupsRoot = Path.Combine(dataDirectoryPath, "startup-recovery");
            Directory.CreateDirectory(backupsRoot);

            for (int attempt = 0; attempt < 1000; attempt++)
            {
                string suffix = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
                string candidate = Path.Combine(
                    backupsRoot,
                    attempt == 0 ? $"backup-{suffix}" : $"backup-{suffix}-{attempt:000}");

                if (Directory.Exists(candidate))
                    continue;

                Directory.CreateDirectory(candidate);
                return candidate;
            }

            throw new InvalidOperationException("Failed to allocate a unique startup recovery backup directory.");
        }

        private static void MoveDbArtifactsToBackup(string dbPath, string backupDirectoryPath)
        {
            foreach (var artifactPath in EnumerateDbArtifacts(dbPath))
            {
                if (!File.Exists(artifactPath))
                    continue;

                string destination = Path.Combine(backupDirectoryPath, Path.GetFileName(artifactPath));
                File.Move(artifactPath, destination, overwrite: false);
            }
        }

        private static IEnumerable<string> EnumerateDbArtifacts(string dbPath)
        {
            yield return dbPath;
            yield return dbPath + "-wal";
            yield return dbPath + "-shm";
        }
    }
}
