/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.BitSet;

import com.google.common.primitives.Longs;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ for more details.
 *
 * hash32() and hash64() are MurmurHash 2.0.
 *
 * hash3_x64_128() is *almost* MurmurHash 3.0.  It was supposed to match, but we didn't catch a sign bug with
 * the result that it doesn't.  Unfortunately, we can't change it now without breaking Murmur3Partitioner. *
 * 
 * <p>
 * The C version of MurmurHash 2.0 found at that site was ported to Java by
 * Andrzej Bialecki (ab at getopt org).
 * </p>
 */
public class MurmurHash
{
    public static int hash32(ByteBuffer data, int offset, int length, int seed)
    {
        int m = 0x5bd1e995;
        int r = 24;

        int h = seed ^ length;

        int len_4 = length >> 2;

        for (int i = 0; i < len_4; i++)
        {
            int i_4 = i << 2;
            int k = data.get(offset + i_4 + 3);
            k = k << 8;
            k = k | (data.get(offset + i_4 + 2) & 0xff);
            k = k << 8;
            k = k | (data.get(offset + i_4 + 1) & 0xff);
            k = k << 8;
            k = k | (data.get(offset + i_4 + 0) & 0xff);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // avoid calculating modulo
        int len_m = len_4 << 2;
        int left = length - len_m;

        if (left != 0)
        {
            if (left >= 3)
            {
                h ^= (int) data.get(offset + length - 3) << 16;
            }
            if (left >= 2)
            {
                h ^= (int) data.get(offset + length - 2) << 8;
            }
            if (left >= 1)
            {
                h ^= (int) data.get(offset + length - 1);
            }

            h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    public static long hash2_64(ByteBuffer key, int offset, int length, long seed)
    {
        long m64 = 0xc6a4a7935bd1e995L;
        int r64 = 47;

        long h64 = (seed & 0xffffffffL) ^ (m64 * length);

        int lenLongs = length >> 3;

        for (int i = 0; i < lenLongs; ++i)
        {
            int i_8 = i << 3;

            long k64 =  ((long)  key.get(offset+i_8+0) & 0xff)      + (((long) key.get(offset+i_8+1) & 0xff)<<8)  +
                        (((long) key.get(offset+i_8+2) & 0xff)<<16) + (((long) key.get(offset+i_8+3) & 0xff)<<24) +
                        (((long) key.get(offset+i_8+4) & 0xff)<<32) + (((long) key.get(offset+i_8+5) & 0xff)<<40) +
                        (((long) key.get(offset+i_8+6) & 0xff)<<48) + (((long) key.get(offset+i_8+7) & 0xff)<<56);

            k64 *= m64;
            k64 ^= k64 >>> r64;
            k64 *= m64;

            h64 ^= k64;
            h64 *= m64;
        }

        int rem = length & 0x7;

        switch (rem)
        {
        case 0:
            break;
        case 7:
            h64 ^= (long) key.get(offset + length - rem + 6) << 48;
        case 6:
            h64 ^= (long) key.get(offset + length - rem + 5) << 40;
        case 5:
            h64 ^= (long) key.get(offset + length - rem + 4) << 32;
        case 4:
            h64 ^= (long) key.get(offset + length - rem + 3) << 24;
        case 3:
            h64 ^= (long) key.get(offset + length - rem + 2) << 16;
        case 2:
            h64 ^= (long) key.get(offset + length - rem + 1) << 8;
        case 1:
            h64 ^= (long) key.get(offset + length - rem);
            h64 *= m64;
        }

        h64 ^= h64 >>> r64;
        h64 *= m64;
        h64 ^= h64 >>> r64;

        return h64;
    }

    protected static long getBlock(ByteBuffer key, int offset, int index)
    {
        int i_8 = index << 3;
        int blockOffset = offset + i_8;
        return ((long) key.get(blockOffset + 0) & 0xff) + (((long) key.get(blockOffset + 1) & 0xff) << 8) +
               (((long) key.get(blockOffset + 2) & 0xff) << 16) + (((long) key.get(blockOffset + 3) & 0xff) << 24) +
               (((long) key.get(blockOffset + 4) & 0xff) << 32) + (((long) key.get(blockOffset + 5) & 0xff) << 40) +
               (((long) key.get(blockOffset + 6) & 0xff) << 48) + (((long) key.get(blockOffset + 7) & 0xff) << 56);
    }

    protected static long rotl64(long v, int n)
    {
        return ((v << n) | (v >>> (64 - n)));
    }

    protected static long fmix(long k)
    {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;

        return k;
    }

    public static void hash3_x64_128(ByteBuffer key, int offset, int length, long seed, long[] result)
    {
        final int nblocks = length >> 4; // Process as 128-bit blocks.

        long h1 = seed;
        long h2 = seed;

        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;

        //----------
        // body

        for(int i = 0; i < nblocks; i++)
        {
            long k1 = getBlock(key, offset, i * 2 + 0);
            long k2 = getBlock(key, offset, i * 2 + 1);

            k1 *= c1; k1 = rotl64(k1,31); k1 *= c2; h1 ^= k1;

            h1 = rotl64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;

            k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;

            h2 = rotl64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
        }

        //----------
        // tail

        // Advance offset to the unprocessed tail of the data.
        offset += nblocks * 16;

        long k1 = 0;
        long k2 = 0;

        switch(length & 15)
        {
            case 15: k2 ^= ((long) key.get(offset+14)) << 48;
            case 14: k2 ^= ((long) key.get(offset+13)) << 40;
            case 13: k2 ^= ((long) key.get(offset+12)) << 32;
            case 12: k2 ^= ((long) key.get(offset+11)) << 24;
            case 11: k2 ^= ((long) key.get(offset+10)) << 16;
            case 10: k2 ^= ((long) key.get(offset+9)) << 8;
            case  9: k2 ^= ((long) key.get(offset+8)) << 0;
                k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;

            case  8: k1 ^= ((long) key.get(offset+7)) << 56;
            case  7: k1 ^= ((long) key.get(offset+6)) << 48;
            case  6: k1 ^= ((long) key.get(offset+5)) << 40;
            case  5: k1 ^= ((long) key.get(offset+4)) << 32;
            case  4: k1 ^= ((long) key.get(offset+3)) << 24;
            case  3: k1 ^= ((long) key.get(offset+2)) << 16;
            case  2: k1 ^= ((long) key.get(offset+1)) << 8;
            case  1: k1 ^= ((long) key.get(offset));
                k1 *= c1; k1  = rotl64(k1,31); k1 *= c2; h1 ^= k1;
        };

        //----------
        // finalization

        h1 ^= length; h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix(h1);
        h2 = fmix(h2);

        h1 += h2;
        h2 += h1;

        result[0] = h1;
        result[1] = h2;
    }

    protected static long invRotl64(long v, int n)
    {
        return ((v >>> n) | (v << (64 - n)));
    }

    protected static long invRShiftXor(long value, int shift)
    {
        long output = 0;
        long i = 0;
        while (i * shift < 64)
        {
            long c = (0xffffffffffffffffL << (64 - shift)) >>> (shift * i);
            long partOutput = value & c;
            value ^= partOutput >>> shift;
            output |= partOutput;
            i += 1;
        }
        return output;
    }

    protected static long invFmix(long k)
    {
        k = invRShiftXor(k, 33);
        k *= 0x9cb4b2f8129337dbL;
        k = invRShiftXor(k, 33);
        k *= 0x4f74430c22a54005L;
        k = invRShiftXor(k, 33);
        return k;
    }

    /**
     * This gives a correct reversal of the tail byte flip which is needed if want a non mod16==0 byte hash inv or to
     * target a hash for a given schema.
     */
    public static long invTailReverse(long num)
    {
        byte[] v = Longs.toByteArray(Long.reverseBytes(num));
        for (int i = 0; i < 8; i++)
        {
            if (v[i] < 0 && i < 7)
            {
                BitSet bits = BitSet.valueOf(v);
                bits.flip(8 * (i + 1), 64);
                v = bits.toByteArray();
            }
        }
        return Longs.fromByteArray(v);
    }

    public static long[] inv_hash3_x64_128(long[] result)
    {
        long c1 = 0xa98409e882ce4d7dL;
        long c2 = 0xa81e14edd9de2c7fL;

        long k1 = 0;
        long k2 = 0;
        long h1 = result[0];
        long h2 = result[1];

        //----------
        // reverse finalization
        h2 -= h1;
        h1 -= h2;

        h1 = invFmix(h1);
        h2 = invFmix(h2);

        h2 -= h1;
        h1 -= h2;

        h1 ^= 16;
        h2 ^= 16;

        //----------
        // reverse body
        h2 -= 0x38495ab5;
        h2 *= 0xcccccccccccccccdL;
        h2 -= h1;
        h2 = invRotl64(h2, 31);
        k2 = h2;
        h2 = 0;

        k2 *= c1;
        k2 = invRotl64(k2, 33);
        k2 *= c2;

        h1 -= 0x52dce729;
        h1 *= 0xcccccccccccccccdL;
        //h1 -= h2;
        h1 = invRotl64(h1, 27);

        k1 = h1;

        k1 *= c2;
        k1 = invRotl64(k1, 31);
        k1 *= c1;

        // note that while this works for body block reversing the tail reverse requires `invTailReverse`
        k1 = Long.reverseBytes(k1);
        k2 = Long.reverseBytes(k2);

        return new long[] {k1, k2};
    }

}
