using System;
using System.Collections.Generic;
using Qado.Storage;

namespace Qado.Networking
{
    public static class BlockLocatorBuilder
    {
        public static List<byte[]> BuildCanonicalLocator(int maxHashes = BlockSyncProtocol.MaxLocatorHashes)
        {
            return BuildLocator(
                BlockStore.GetLatestHeight(),
                height => BlockStore.GetCanonicalHashAtHeight(height),
                maxHashes);
        }

        public static List<byte[]> BuildLocator(
            ulong tipHeight,
            Func<ulong, byte[]?> getHashAtHeight,
            int maxHashes = BlockSyncProtocol.MaxLocatorHashes)
        {
            if (getHashAtHeight == null)
                throw new ArgumentNullException(nameof(getHashAtHeight));

            if (maxHashes <= 0)
                maxHashes = BlockSyncProtocol.MaxLocatorHashes;

            var locator = new List<byte[]>(Math.Min(maxHashes, BlockSyncProtocol.MaxLocatorHashes));
            var seen = new HashSet<string>(StringComparer.Ordinal);

            void AddHeight(ulong height)
            {
                if (locator.Count >= maxHashes)
                    return;

                var hash = getHashAtHeight(height);
                if (hash is not { Length: 32 })
                    return;

                string key = Convert.ToHexString(hash).ToLowerInvariant();
                if (!seen.Add(key))
                    return;

                locator.Add((byte[])hash.Clone());
            }

            AddHeight(tipHeight);
            if (tipHeight == 0)
                return locator;

            ulong height = tipHeight;

            for (int i = 0; i < 3 && height > 0 && locator.Count < maxHashes; i++)
            {
                height--;
                AddHeight(height);
            }

            ulong step = 2;
            while (height > 0 && locator.Count < maxHashes)
            {
                height = height > step ? height - step : 0;
                AddHeight(height);
                if (step <= ulong.MaxValue / 2)
                    step *= 2;
                else
                    step = ulong.MaxValue;
            }

            return locator;
        }
    }
}
