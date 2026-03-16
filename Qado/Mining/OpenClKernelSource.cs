namespace Qado.Mining
{
    internal static class OpenClKernelSource
    {
        public const string Source = """
__constant uint IV[8] = {
    0x6A09E667u, 0xBB67AE85u, 0x3C6EF372u, 0xA54FF53Au,
    0x510E527Fu, 0x9B05688Cu, 0x1F83D9ABu, 0x5BE0CD19u
};

#define CHUNK_START 1u
#define CHUNK_END 2u
#define ROOT 8u

inline uint rotr32(uint x, uint n)
{
    return (x >> n) | (x << (32u - n));
}

inline void g(__private uint* a, __private uint* b, __private uint* c, __private uint* d, uint mx, uint my)
{
    *a = *a + *b + mx;
    *d = rotr32(*d ^ *a, 16u);
    *c = *c + *d;
    *b = rotr32(*b ^ *c, 12u);
    *a = *a + *b + my;
    *d = rotr32(*d ^ *a, 8u);
    *c = *c + *d;
    *b = rotr32(*b ^ *c, 7u);
}

inline void round_fn(__private uint v[16], __private uint m[16])
{
    g(&v[0], &v[4], &v[8], &v[12], m[0], m[1]);
    g(&v[1], &v[5], &v[9], &v[13], m[2], m[3]);
    g(&v[2], &v[6], &v[10], &v[14], m[4], m[5]);
    g(&v[3], &v[7], &v[11], &v[15], m[6], m[7]);

    g(&v[0], &v[5], &v[10], &v[15], m[8], m[9]);
    g(&v[1], &v[6], &v[11], &v[12], m[10], m[11]);
    g(&v[2], &v[7], &v[8], &v[13], m[12], m[13]);
    g(&v[3], &v[4], &v[9], &v[14], m[14], m[15]);
}

inline void permute(__private uint m[16])
{
    uint t[16];
    t[0] = m[2]; t[1] = m[6]; t[2] = m[3]; t[3] = m[10];
    t[4] = m[7]; t[5] = m[0]; t[6] = m[4]; t[7] = m[13];
    t[8] = m[1]; t[9] = m[11]; t[10] = m[12]; t[11] = m[5];
    t[12] = m[9]; t[13] = m[14]; t[14] = m[15]; t[15] = m[8];
    for (int i = 0; i < 16; i++) m[i] = t[i];
}

inline void compress_words(
    __private const uint cv[8],
    __private uint blockWords[16],
    uint counterLow,
    uint counterHigh,
    uint blockLen,
    uint flags,
    __private uint outWords[16])
{
    uint v[16];
    for (int i = 0; i < 8; i++) v[i] = cv[i];
    v[8] = IV[0];
    v[9] = IV[1];
    v[10] = IV[2];
    v[11] = IV[3];
    v[12] = counterLow;
    v[13] = counterHigh;
    v[14] = blockLen;
    v[15] = flags;

    round_fn(v, blockWords);
    permute(blockWords);
    round_fn(v, blockWords);
    permute(blockWords);
    round_fn(v, blockWords);
    permute(blockWords);
    round_fn(v, blockWords);
    permute(blockWords);
    round_fn(v, blockWords);
    permute(blockWords);
    round_fn(v, blockWords);
    permute(blockWords);
    round_fn(v, blockWords);

    for (int i = 0; i < 8; i++)
    {
        outWords[i] = v[i] ^ v[i + 8];
        outWords[i + 8] = v[i + 8] ^ cv[i];
    }
}

inline void inject_nonce(__global const uint* block1Base, ulong nonce, __private uint block1[16])
{
    for (int i = 0; i < 16; i++) block1[i] = block1Base[i];

    uint b56 = (uint)((nonce >> 56) & 0xFFul);
    uint b48 = (uint)((nonce >> 48) & 0xFFul);
    uint b40 = (uint)((nonce >> 40) & 0xFFul);
    uint b32 = (uint)((nonce >> 32) & 0xFFul);
    uint b24 = (uint)((nonce >> 24) & 0xFFul);
    uint b16 = (uint)((nonce >> 16) & 0xFFul);
    uint b8 = (uint)((nonce >> 8) & 0xFFul);
    uint b0 = (uint)(nonce & 0xFFul);

    block1[10] = (block1[10] & 0x000000FFu) | (b56 << 8) | (b48 << 16) | (b40 << 24);
    block1[11] = b32 | (b24 << 8) | (b16 << 16) | (b8 << 24);
    block1[12] = (block1[12] & 0xFFFFFF00u) | b0;
}

inline void blake3_header_hash(
    __global const uint* block0,
    __global const uint* block1Base,
    __global const uint* block2,
    ulong nonce,
    __private uchar outHash[32])
{
    uint cv[8];
    uint blockWords[16];
    uint outWords[16];

    for (int i = 0; i < 8; i++) cv[i] = IV[i];

    for (int i = 0; i < 16; i++) blockWords[i] = block0[i];
    compress_words(cv, blockWords, 0u, 0u, 64u, CHUNK_START, outWords);
    for (int i = 0; i < 8; i++) cv[i] = outWords[i];

    inject_nonce(block1Base, nonce, blockWords);
    compress_words(cv, blockWords, 0u, 0u, 64u, 0u, outWords);
    for (int i = 0; i < 8; i++) cv[i] = outWords[i];

    for (int i = 0; i < 16; i++) blockWords[i] = block2[i];
    compress_words(cv, blockWords, 0u, 0u, 17u, CHUNK_END | ROOT, outWords);

    for (int i = 0; i < 8; i++)
    {
        uint w = outWords[i];
        outHash[(i * 4) + 0] = (uchar)(w & 0xFFu);
        outHash[(i * 4) + 1] = (uchar)((w >> 8) & 0xFFu);
        outHash[(i * 4) + 2] = (uchar)((w >> 16) & 0xFFu);
        outHash[(i * 4) + 3] = (uchar)((w >> 24) & 0xFFu);
    }
}

inline int meets_target(__private const uchar hash[32], __global const uchar* target)
{
    for (int i = 0; i < 32; i++)
    {
        if (hash[i] < target[i]) return 1;
        if (hash[i] > target[i]) return 0;
    }

    return 1;
}

__kernel void search_nonce(
    __global const uint* block0,
    __global const uint* block1Base,
    __global const uint* block2,
    __global const uchar* target,
    ulong nonceBase,
    __global int* foundFlag,
    __global ulong* foundNonce,
    __global uchar* foundHash)
{
    if (*foundFlag != 0)
        return;

    ulong nonce = nonceBase + (ulong)get_global_id(0);
    uchar digest[32];
    blake3_header_hash(block0, block1Base, block2, nonce, digest);

    if (!meets_target(digest, target))
        return;

    if (atomic_cmpxchg(foundFlag, 0, 1) == 0)
    {
        foundNonce[0] = nonce;
        for (int i = 0; i < 32; i++)
            foundHash[i] = digest[i];
    }
}
""";
    }
}
