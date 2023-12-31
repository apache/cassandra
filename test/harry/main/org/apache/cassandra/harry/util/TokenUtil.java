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

package org.apache.cassandra.harry.util;

import java.nio.ByteBuffer;

/**
 * Copied verbatim from Apache Cassandra codebase to avoid having a dependency on Cassandra.
 */
public class TokenUtil
{
    public static long token(ByteBuffer key)
    {
        if (key.remaining() == 0)
            return Long.MIN_VALUE;

        return normalize(getHash(key)[0]);
    }

    private static long normalize(long v)
    {
        // We exclude the MINIMUM value; see getToken()
        return v == Long.MIN_VALUE ? Long.MAX_VALUE : v;
    }

    private static long[] getHash(ByteBuffer key)
    {
        long[] hash = new long[2];
        hash3_x64_128(key, key.position(), key.remaining(), 0, hash);
        return hash;
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
}
