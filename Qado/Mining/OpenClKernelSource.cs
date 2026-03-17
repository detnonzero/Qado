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

inline uint bswap32(uint x)
{
    return ((x & 0x000000FFu) << 24)
         | ((x & 0x0000FF00u) << 8)
         | ((x & 0x00FF0000u) >> 8)
         | ((x & 0xFF000000u) >> 24);
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

inline void round_fn(
    __private uint* v0, __private uint* v1, __private uint* v2, __private uint* v3,
    __private uint* v4, __private uint* v5, __private uint* v6, __private uint* v7,
    __private uint* v8, __private uint* v9, __private uint* v10, __private uint* v11,
    __private uint* v12, __private uint* v13, __private uint* v14, __private uint* v15,
    uint m0, uint m1, uint m2, uint m3, uint m4, uint m5, uint m6, uint m7,
    uint m8, uint m9, uint m10, uint m11, uint m12, uint m13, uint m14, uint m15)
{
    g(v0, v4, v8, v12, m0, m1);
    g(v1, v5, v9, v13, m2, m3);
    g(v2, v6, v10, v14, m4, m5);
    g(v3, v7, v11, v15, m6, m7);

    g(v0, v5, v10, v15, m8, m9);
    g(v1, v6, v11, v12, m10, m11);
    g(v2, v7, v8, v13, m12, m13);
    g(v3, v4, v9, v14, m14, m15);
}

inline void permute(
    __private uint* m0, __private uint* m1, __private uint* m2, __private uint* m3,
    __private uint* m4, __private uint* m5, __private uint* m6, __private uint* m7,
    __private uint* m8, __private uint* m9, __private uint* m10, __private uint* m11,
    __private uint* m12, __private uint* m13, __private uint* m14, __private uint* m15)
{
    uint t0 = *m2;
    uint t1 = *m6;
    uint t2 = *m3;
    uint t3 = *m10;
    uint t4 = *m7;
    uint t5 = *m0;
    uint t6 = *m4;
    uint t7 = *m13;
    uint t8 = *m1;
    uint t9 = *m11;
    uint t10 = *m12;
    uint t11 = *m5;
    uint t12 = *m9;
    uint t13 = *m14;
    uint t14 = *m15;
    uint t15 = *m8;

    *m0 = t0; *m1 = t1; *m2 = t2; *m3 = t3;
    *m4 = t4; *m5 = t5; *m6 = t6; *m7 = t7;
    *m8 = t8; *m9 = t9; *m10 = t10; *m11 = t11;
    *m12 = t12; *m13 = t13; *m14 = t14; *m15 = t15;
}

inline void compress_words(
    uint cv0, uint cv1, uint cv2, uint cv3, uint cv4, uint cv5, uint cv6, uint cv7,
    uint m0, uint m1, uint m2, uint m3, uint m4, uint m5, uint m6, uint m7,
    uint m8, uint m9, uint m10, uint m11, uint m12, uint m13, uint m14, uint m15,
    uint counterLow,
    uint counterHigh,
    uint blockLen,
    uint flags,
    __private uint* o0, __private uint* o1, __private uint* o2, __private uint* o3,
    __private uint* o4, __private uint* o5, __private uint* o6, __private uint* o7,
    __private uint* o8, __private uint* o9, __private uint* o10, __private uint* o11,
    __private uint* o12, __private uint* o13, __private uint* o14, __private uint* o15)
{
    uint v0 = cv0;
    uint v1 = cv1;
    uint v2 = cv2;
    uint v3 = cv3;
    uint v4 = cv4;
    uint v5 = cv5;
    uint v6 = cv6;
    uint v7 = cv7;
    uint v8 = IV[0];
    uint v9 = IV[1];
    uint v10 = IV[2];
    uint v11 = IV[3];
    uint v12 = counterLow;
    uint v13 = counterHigh;
    uint v14 = blockLen;
    uint v15 = flags;

    round_fn(&v0, &v1, &v2, &v3, &v4, &v5, &v6, &v7, &v8, &v9, &v10, &v11, &v12, &v13, &v14, &v15, m0, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15);
    permute(&m0, &m1, &m2, &m3, &m4, &m5, &m6, &m7, &m8, &m9, &m10, &m11, &m12, &m13, &m14, &m15);
    round_fn(&v0, &v1, &v2, &v3, &v4, &v5, &v6, &v7, &v8, &v9, &v10, &v11, &v12, &v13, &v14, &v15, m0, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15);
    permute(&m0, &m1, &m2, &m3, &m4, &m5, &m6, &m7, &m8, &m9, &m10, &m11, &m12, &m13, &m14, &m15);
    round_fn(&v0, &v1, &v2, &v3, &v4, &v5, &v6, &v7, &v8, &v9, &v10, &v11, &v12, &v13, &v14, &v15, m0, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15);
    permute(&m0, &m1, &m2, &m3, &m4, &m5, &m6, &m7, &m8, &m9, &m10, &m11, &m12, &m13, &m14, &m15);
    round_fn(&v0, &v1, &v2, &v3, &v4, &v5, &v6, &v7, &v8, &v9, &v10, &v11, &v12, &v13, &v14, &v15, m0, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15);
    permute(&m0, &m1, &m2, &m3, &m4, &m5, &m6, &m7, &m8, &m9, &m10, &m11, &m12, &m13, &m14, &m15);
    round_fn(&v0, &v1, &v2, &v3, &v4, &v5, &v6, &v7, &v8, &v9, &v10, &v11, &v12, &v13, &v14, &v15, m0, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15);
    permute(&m0, &m1, &m2, &m3, &m4, &m5, &m6, &m7, &m8, &m9, &m10, &m11, &m12, &m13, &m14, &m15);
    round_fn(&v0, &v1, &v2, &v3, &v4, &v5, &v6, &v7, &v8, &v9, &v10, &v11, &v12, &v13, &v14, &v15, m0, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15);
    permute(&m0, &m1, &m2, &m3, &m4, &m5, &m6, &m7, &m8, &m9, &m10, &m11, &m12, &m13, &m14, &m15);
    round_fn(&v0, &v1, &v2, &v3, &v4, &v5, &v6, &v7, &v8, &v9, &v10, &v11, &v12, &v13, &v14, &v15, m0, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15);

    *o0 = v0 ^ v8;
    *o1 = v1 ^ v9;
    *o2 = v2 ^ v10;
    *o3 = v3 ^ v11;
    *o4 = v4 ^ v12;
    *o5 = v5 ^ v13;
    *o6 = v6 ^ v14;
    *o7 = v7 ^ v15;
    *o8 = v8 ^ cv0;
    *o9 = v9 ^ cv1;
    *o10 = v10 ^ cv2;
    *o11 = v11 ^ cv3;
    *o12 = v12 ^ cv4;
    *o13 = v13 ^ cv5;
    *o14 = v14 ^ cv6;
    *o15 = v15 ^ cv7;
}

inline void inject_nonce(__constant const uint* block1Base, ulong nonce, __private uint block1[16])
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

inline int meets_target_words(
    uint h0, uint h1, uint h2, uint h3,
    uint h4, uint h5, uint h6, uint h7,
    __constant const uint* targetWords)
{
    uint x0 = bswap32(h0);
    uint t0 = targetWords[0];
    if (x0 < t0) return 1;
    if (x0 > t0) return 0;

    uint x1 = bswap32(h1);
    uint t1 = targetWords[1];
    if (x1 < t1) return 1;
    if (x1 > t1) return 0;

    uint x2 = bswap32(h2);
    uint t2 = targetWords[2];
    if (x2 < t2) return 1;
    if (x2 > t2) return 0;

    uint x3 = bswap32(h3);
    uint t3 = targetWords[3];
    if (x3 < t3) return 1;
    if (x3 > t3) return 0;

    uint x4 = bswap32(h4);
    uint t4 = targetWords[4];
    if (x4 < t4) return 1;
    if (x4 > t4) return 0;

    uint x5 = bswap32(h5);
    uint t5 = targetWords[5];
    if (x5 < t5) return 1;
    if (x5 > t5) return 0;

    uint x6 = bswap32(h6);
    uint t6 = targetWords[6];
    if (x6 < t6) return 1;
    if (x6 > t6) return 0;

    uint x7 = bswap32(h7);
    uint t7 = targetWords[7];
    if (x7 < t7) return 1;
    if (x7 > t7) return 0;

    return 1;
}

inline void write_hash(
    __global uchar* foundHash,
    uint h0, uint h1, uint h2, uint h3,
    uint h4, uint h5, uint h6, uint h7)
{
    foundHash[0] = (uchar)(h0 & 0xFFu);
    foundHash[1] = (uchar)((h0 >> 8) & 0xFFu);
    foundHash[2] = (uchar)((h0 >> 16) & 0xFFu);
    foundHash[3] = (uchar)((h0 >> 24) & 0xFFu);
    foundHash[4] = (uchar)(h1 & 0xFFu);
    foundHash[5] = (uchar)((h1 >> 8) & 0xFFu);
    foundHash[6] = (uchar)((h1 >> 16) & 0xFFu);
    foundHash[7] = (uchar)((h1 >> 24) & 0xFFu);
    foundHash[8] = (uchar)(h2 & 0xFFu);
    foundHash[9] = (uchar)((h2 >> 8) & 0xFFu);
    foundHash[10] = (uchar)((h2 >> 16) & 0xFFu);
    foundHash[11] = (uchar)((h2 >> 24) & 0xFFu);
    foundHash[12] = (uchar)(h3 & 0xFFu);
    foundHash[13] = (uchar)((h3 >> 8) & 0xFFu);
    foundHash[14] = (uchar)((h3 >> 16) & 0xFFu);
    foundHash[15] = (uchar)((h3 >> 24) & 0xFFu);
    foundHash[16] = (uchar)(h4 & 0xFFu);
    foundHash[17] = (uchar)((h4 >> 8) & 0xFFu);
    foundHash[18] = (uchar)((h4 >> 16) & 0xFFu);
    foundHash[19] = (uchar)((h4 >> 24) & 0xFFu);
    foundHash[20] = (uchar)(h5 & 0xFFu);
    foundHash[21] = (uchar)((h5 >> 8) & 0xFFu);
    foundHash[22] = (uchar)((h5 >> 16) & 0xFFu);
    foundHash[23] = (uchar)((h5 >> 24) & 0xFFu);
    foundHash[24] = (uchar)(h6 & 0xFFu);
    foundHash[25] = (uchar)((h6 >> 8) & 0xFFu);
    foundHash[26] = (uchar)((h6 >> 16) & 0xFFu);
    foundHash[27] = (uchar)((h6 >> 24) & 0xFFu);
    foundHash[28] = (uchar)(h7 & 0xFFu);
    foundHash[29] = (uchar)((h7 >> 8) & 0xFFu);
    foundHash[30] = (uchar)((h7 >> 16) & 0xFFu);
    foundHash[31] = (uchar)((h7 >> 24) & 0xFFu);
}

__kernel void search_nonce(
    __constant const uint* precomputedCv,
    __constant const uint* block1Base,
    __constant const uint* block2,
    __constant const uint* targetWords,
    ulong nonceBase,
    __global int* foundFlag,
    __global ulong* foundNonce,
    __global uchar* foundHash)
{
    if (*foundFlag != 0)
        return;

    ulong nonce = nonceBase + (ulong)get_global_id(0);

    uint cv0 = precomputedCv[0];
    uint cv1 = precomputedCv[1];
    uint cv2 = precomputedCv[2];
    uint cv3 = precomputedCv[3];
    uint cv4 = precomputedCv[4];
    uint cv5 = precomputedCv[5];
    uint cv6 = precomputedCv[6];
    uint cv7 = precomputedCv[7];

    uint blockWords[16];
    uint outWords[16];

    inject_nonce(block1Base, nonce, blockWords);
    compress_words(
        cv0, cv1, cv2, cv3, cv4, cv5, cv6, cv7,
        blockWords[0], blockWords[1], blockWords[2], blockWords[3], blockWords[4], blockWords[5], blockWords[6], blockWords[7],
        blockWords[8], blockWords[9], blockWords[10], blockWords[11], blockWords[12], blockWords[13], blockWords[14], blockWords[15],
        0u, 0u, 64u, 0u,
        &outWords[0], &outWords[1], &outWords[2], &outWords[3], &outWords[4], &outWords[5], &outWords[6], &outWords[7],
        &outWords[8], &outWords[9], &outWords[10], &outWords[11], &outWords[12], &outWords[13], &outWords[14], &outWords[15]);

    cv0 = outWords[0];
    cv1 = outWords[1];
    cv2 = outWords[2];
    cv3 = outWords[3];
    cv4 = outWords[4];
    cv5 = outWords[5];
    cv6 = outWords[6];
    cv7 = outWords[7];

    compress_words(
        cv0, cv1, cv2, cv3, cv4, cv5, cv6, cv7,
        block2[0], block2[1], block2[2], block2[3], block2[4], block2[5], block2[6], block2[7],
        block2[8], block2[9], block2[10], block2[11], block2[12], block2[13], block2[14], block2[15],
        0u, 0u, 17u, CHUNK_END | ROOT,
        &outWords[0], &outWords[1], &outWords[2], &outWords[3], &outWords[4], &outWords[5], &outWords[6], &outWords[7],
        &outWords[8], &outWords[9], &outWords[10], &outWords[11], &outWords[12], &outWords[13], &outWords[14], &outWords[15]);

    uint h0 = outWords[0];
    uint h1 = outWords[1];
    uint h2 = outWords[2];
    uint h3 = outWords[3];
    uint h4 = outWords[4];
    uint h5 = outWords[5];
    uint h6 = outWords[6];
    uint h7 = outWords[7];

    if (!meets_target_words(h0, h1, h2, h3, h4, h5, h6, h7, targetWords))
        return;

    if (atomic_cmpxchg(foundFlag, 0, 1) == 0)
    {
        foundNonce[0] = nonce;
        write_hash(foundHash, h0, h1, h2, h3, h4, h5, h6, h7);
    }
}
""";
    }
}
