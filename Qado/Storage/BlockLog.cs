using System;
using System.Buffers.Binary;
using System.IO;
using Qado.Blockchain;

namespace Qado.Storage
{
    public static class BlockLog
    {
        private static readonly byte[] Magic = { (byte)'Q', (byte)'B', (byte)'L', (byte)'K' };

        public const int HeaderSize = 48;

        private const ushort CurrentVersion = 1;
        private const ushort FlagsNone = 0;

        public const int MaxPayloadSize = ConsensusRules.MaxBlockSizeBytes;

        private static readonly object _fileSync = new();
        private static FileStream? _fs;
        private static string _path = "";

        public static string PathOnDisk => _path;

        public static void Initialize(string blocksDatPath)
        {
            if (string.IsNullOrWhiteSpace(blocksDatPath))
                throw new ArgumentNullException(nameof(blocksDatPath));

            lock (_fileSync)
            {
                var full = System.IO.Path.GetFullPath(blocksDatPath);

                if (_fs != null)
                {
                    if (!string.Equals(_path, full, StringComparison.OrdinalIgnoreCase))
                        throw new InvalidOperationException($"BlockLog already initialized with '{_path}', cannot re-init with '{full}'.");
                    return;
                }

                Directory.CreateDirectory(System.IO.Path.GetDirectoryName(full)!);

                _path = full;
                _fs = new FileStream(
                    full,
                    FileMode.OpenOrCreate,
                    FileAccess.ReadWrite,
                    FileShare.ReadWrite,
                    bufferSize: 1024 * 1024,
                    options: FileOptions.SequentialScan);

                _fs.Seek(0, SeekOrigin.End);
            }
        }

        public static void Shutdown()
        {
            lock (_fileSync)
            {
                try { _fs?.Flush(true); } catch { }
                try { _fs?.Dispose(); } catch { }
                _fs = null;
                _path = "";
            }
        }

        public static (long RecordOffset, int RecordSize, uint Crc32) Append(byte[] payload, byte[] headerHash)
        {
            if (payload == null) throw new ArgumentNullException(nameof(payload));
            if (headerHash is not { Length: 32 }) throw new ArgumentException("headerHash must be 32 bytes", nameof(headerHash));
            EnsureInitialized();

            if (payload.Length < 0 || payload.Length > MaxPayloadSize)
                throw new IOException($"payload too large: {payload.Length} bytes (max {MaxPayloadSize})");

            lock (_fileSync)
            {
                var fs = _fs!;

                long recordOffset = fs.Length;
                fs.Seek(0, SeekOrigin.End);

                uint crc = Crc32(payload);

                Span<byte> hdr = stackalloc byte[HeaderSize];
                hdr.Clear();

                Magic.CopyTo(hdr[..4]);
                BinaryPrimitives.WriteUInt16LittleEndian(hdr.Slice(4, 2), CurrentVersion);
                BinaryPrimitives.WriteUInt16LittleEndian(hdr.Slice(6, 2), FlagsNone);
                BinaryPrimitives.WriteUInt32LittleEndian(hdr.Slice(8, 4), (uint)payload.Length);

                headerHash.CopyTo(hdr.Slice(12, 32));
                BinaryPrimitives.WriteUInt32LittleEndian(hdr.Slice(44, 4), crc);

                fs.Write(hdr);
                if (payload.Length > 0)
                    fs.Write(payload, 0, payload.Length);

                fs.Flush(true);

                int recordSize = checked(HeaderSize + payload.Length);
                return (recordOffset, recordSize, crc);
            }
        }

        public static byte[] ReadPayload(long recordOffset, out byte[] headerHash)
        {
            EnsureInitialized();

            lock (_fileSync)
            {
                var fs = _fs!;
                if (recordOffset < 0 || recordOffset > fs.Length - HeaderSize)
                    throw new IOException("recordOffset out of range");

                fs.Seek(recordOffset, SeekOrigin.Begin);

                var hdr = new byte[HeaderSize];
                ReadExact(fs, hdr, 0, HeaderSize);

                if (hdr[0] != Magic[0] || hdr[1] != Magic[1] || hdr[2] != Magic[2] || hdr[3] != Magic[3])
                    throw new IOException("bad magic");

                ushort ver = BinaryPrimitives.ReadUInt16LittleEndian(hdr.AsSpan(4, 2));
                if (ver != CurrentVersion)
                    throw new IOException($"unsupported blocklog record version: {ver}");

                uint payloadLenU32 = BinaryPrimitives.ReadUInt32LittleEndian(hdr.AsSpan(8, 4));
                if (payloadLenU32 > MaxPayloadSize)
                    throw new IOException("payload too large");

                int payloadLen = (int)payloadLenU32;

                headerHash = hdr.AsSpan(12, 32).ToArray();
                uint expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(hdr.AsSpan(44, 4));

                long endPos = recordOffset + HeaderSize + payloadLen;
                if (endPos > fs.Length)
                    throw new IOException("truncated record");

                var payload = new byte[payloadLen];
                if (payloadLen > 0)
                    ReadExact(fs, payload, 0, payloadLen);

                uint actualCrc = Crc32(payload);
                if (actualCrc != expectedCrc)
                    throw new IOException("crc mismatch");

                return payload;
            }
        }

        public static bool TryReadPayload(long recordOffset, out byte[] payload, out byte[] headerHash, out string error)
        {
            try
            {
                payload = ReadPayload(recordOffset, out headerHash);
                error = "";
                return true;
            }
            catch (Exception ex)
            {
                payload = Array.Empty<byte>();
                headerHash = Array.Empty<byte>();
                error = ex.Message;
                return false;
            }
        }

        private static void EnsureInitialized()
        {
            if (_fs == null)
                throw new InvalidOperationException("BlockLog not initialized. Call BlockLog.Initialize(path) on startup.");
        }

        private static void ReadExact(FileStream fs, byte[] buf, int off, int len)
        {
            int read = 0;
            while (read < len)
            {
                int r = fs.Read(buf, off + read, len - read);
                if (r <= 0) throw new IOException("unexpected EOF");
                read += r;
            }
        }

        private static readonly uint[] _crcTable = BuildCrcTable();

        private static uint Crc32(byte[] data)
        {
            uint crc = 0xFFFFFFFFu;
            for (int i = 0; i < data.Length; i++)
            {
                uint idx = (crc ^ data[i]) & 0xFF;
                crc = (crc >> 8) ^ _crcTable[idx];
            }
            return ~crc;
        }

        private static uint[] BuildCrcTable()
        {
            const uint poly = 0xEDB88320u;
            var table = new uint[256];
            for (uint i = 0; i < table.Length; i++)
            {
                uint c = i;
                for (int k = 0; k < 8; k++)
                    c = (c & 1) != 0 ? (poly ^ (c >> 1)) : (c >> 1);
                table[i] = c;
            }
            return table;
        }
    }
}

