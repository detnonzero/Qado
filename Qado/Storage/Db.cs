using System;
using System.IO;
using Microsoft.Data.Sqlite;

namespace Qado.Storage
{
    public static class Db
    {
        public static SqliteConnection Connection { get; private set; } = null!;
        public static readonly object Sync = new();

        public static void Initialize(string dbPath)
        {
            if (string.IsNullOrWhiteSpace(dbPath))
                throw new ArgumentNullException(nameof(dbPath));

            var full = Path.GetFullPath(dbPath);
            Directory.CreateDirectory(Path.GetDirectoryName(full)!);

            var cs = new SqliteConnectionStringBuilder
            {
                DataSource = full,
                Mode = SqliteOpenMode.ReadWriteCreate,
                Cache = SqliteCacheMode.Shared
            }.ToString();

            Connection = new SqliteConnection(cs);
            Connection.Open();

            using (var cmd = Connection.CreateCommand())
            {
                cmd.CommandText = @"
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;
PRAGMA page_size=8192;
PRAGMA mmap_size=268435456;
PRAGMA cache_size=-524288;
";
                cmd.ExecuteNonQuery();
            }

            Schema.Ensure();
        }

        public static void Shutdown()
        {
            lock (Sync)
            {
                try { Connection?.Close(); } catch { }
                try { Connection?.Dispose(); } catch { }
                Connection = null!;
            }
        }
    }
}

