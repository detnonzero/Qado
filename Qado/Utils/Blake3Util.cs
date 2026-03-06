using Blake3;

namespace Qado.Utils
{
    public static class Blake3Util
    {
        public const int HashSize = 32;

        public static byte[] Hash(ReadOnlySpan<byte> input)
        {
            var output = new byte[HashSize];
            Hasher.Hash(input, output);
            return output;
        }
    }
}
