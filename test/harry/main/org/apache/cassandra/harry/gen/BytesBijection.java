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

package org.apache.cassandra.harry.gen;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.TreeMap;

import org.apache.cassandra.harry.gen.rng.RngUtils;

/**
 * A nibble-based bijection for ByteBuffers, similar to {@link StringBijection}.
 * <p>
 * Each byte of the descriptor is used as an index into a nibble table of 256 pre-generated
 * byte[] fragments. The 8 nibbles form a unique prefix that guarantees bijectivity;
 * additional random bytes are appended for variable-length output.
 */
public class BytesBijection implements Bijections.Bijection<ByteBuffer>
{
    public static final int NIBBLES_SIZE = 256;
    private final byte[][] nibbles;
    private final int nibbleSize;
    private final int maxRandomBytes;

    public BytesBijection()
    {
        this(8, 10);
    }

    public BytesBijection(int nibbleSize, int maxRandomBytes)
    {
        this(generateNibbles(nibbleSize), nibbleSize, maxRandomBytes);
    }

    public BytesBijection(byte[][] nibbles, int nibbleSize, int maxRandomBytes)
    {
        assert nibbles.length == NIBBLES_SIZE;
        this.nibbles = nibbles;
        this.nibbleSize = nibbleSize;
        this.maxRandomBytes = maxRandomBytes;

        for (int i = 0; i < nibbles.length; i++)
            assert nibbles[i].length == nibbleSize;
    }

    public ByteBuffer inflate(long descriptor)
    {
        // Prefix: 8 nibbles selected by each byte of the descriptor
        int prefixLen = Long.BYTES * nibbleSize;

        // Determine suffix length
        int suffixLen = suffixLength(descriptor);

        // Determine extra length (subclasses may override)
        int extraLen = extraLength(descriptor);

        byte[] result = new byte[prefixLen + suffixLen + extraLen];

        // Write prefix nibbles
        for (int i = 0; i < Long.BYTES; i++)
        {
            int idx = getByte(descriptor, i);
            System.arraycopy(nibbles[idx], 0, result, i * nibbleSize, nibbleSize);
        }

        // Append suffix bytes
        appendSuffix(result, prefixLen, suffixLen, descriptor);

        // Append extra bytes (subclasses may override)
        appendExtra(result, prefixLen + suffixLen, extraLen, descriptor);

        return ByteBuffer.wrap(result);
    }

    protected int suffixLength(long descriptor)
    {
        long rnd = RngUtils.next(descriptor);
        return RngUtils.asInt(rnd, 0, maxRandomBytes);
    }

    protected int extraLength(long descriptor)
    {
        return 0;
    }

    protected void appendSuffix(byte[] result, int offset, int length, long descriptor)
    {
        long rnd = RngUtils.next(descriptor);
        // skip past the rnd used for length
        rnd = RngUtils.next(rnd);

        int pos = offset;
        int remaining = length;
        while (remaining > 0)
        {
            rnd = RngUtils.next(rnd);
            for (int i = 0; i < remaining && i < Long.BYTES; i++)
            {
                result[pos++] = (byte) ((rnd >> (i * 8)) & 0xff);
                remaining--;
            }
        }
    }

    protected void appendExtra(byte[] result, int offset, int length, long descriptor)
    {
    }

    public long deflate(ByteBuffer value)
    {
        long res = 0;
        for (int i = 0; i < Long.BYTES; i++)
        {
            int idx = findNibble(value, value.position() + i * nibbleSize);
            long v = idx;
            if (i == 0)
                v ^= 0x80;
            res |= v << (Long.BYTES - i - 1) * Byte.SIZE;
        }
        return res;
    }

    private int findNibble(ByteBuffer value, int offset)
    {
        for (int n = 0; n < NIBBLES_SIZE; n++)
        {
            boolean match = true;
            for (int j = 0; j < nibbleSize; j++)
            {
                if (value.get(offset + j) != nibbles[n][j])
                {
                    match = false;
                    break;
                }
            }
            if (match)
                return n;
        }
        throw new IllegalArgumentException("No matching nibble found at offset " + offset);
    }

    public static int getByte(long l, int idx)
    {
        int b = (int) ((l >> (Long.BYTES - idx - 1) * Byte.SIZE) & 0xff);
        if (idx == 0)
            b ^= 0x80;
        return b;
    }

    public int compare(long l, long r)
    {
        for (int i = 0; i < Long.BYTES; i++)
        {
            int cmp = Integer.compare(getByte(l, i), getByte(r, i));
            if (cmp != 0)
                return cmp;
        }
        return 0;
    }

    public int byteSize()
    {
        return Long.BYTES;
    }

    @Override
    public String toString()
    {
        return "bytes(" +
               "nibbleSize=" + nibbleSize +
               ", maxRandomBytes=" + maxRandomBytes +
               ')';
    }

    /**
     * Generate 256 unique byte[] nibbles of the given size, sorted lexicographically.
     * Uses a deterministic RNG so nibbles are stable across runs.
     */
    public static byte[][] generateNibbles(int nibbleSize)
    {
        TreeMap<ByteArrayWrapper, byte[]> sorted = new TreeMap<>();
        long seed = 0xdeadbeefcafeL;

        while (sorted.size() < NIBBLES_SIZE)
        {
            seed = RngUtils.next(seed);
            byte[] nibble = new byte[nibbleSize];
            long s = seed;
            for (int j = 0; j < nibbleSize; j++)
            {
                nibble[j] = (byte) (s & 0xff);
                s = RngUtils.next(s);
            }
            sorted.put(new ByteArrayWrapper(nibble), nibble);
        }

        byte[][] nibbles = new byte[NIBBLES_SIZE][];
        int i = 0;
        for (byte[] nibble : sorted.values())
            nibbles[i++] = nibble;

        return nibbles;
    }

    /**
     * Wrapper for byte[] that provides equals/hashCode/compareTo for use in sorted collections.
     */
    private static class ByteArrayWrapper implements Comparable<ByteArrayWrapper>
    {
        private final byte[] data;

        ByteArrayWrapper(byte[] data)
        {
            this.data = data;
        }

        @Override
        public int compareTo(ByteArrayWrapper other)
        {
            int len = Math.min(data.length, other.data.length);
            for (int i = 0; i < len; i++)
            {
                int cmp = Integer.compare(data[i] & 0xff, other.data[i] & 0xff);
                if (cmp != 0)
                    return cmp;
            }
            return Integer.compare(data.length, other.data.length);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof ByteArrayWrapper)) return false;
            return Arrays.equals(data, ((ByteArrayWrapper) o).data);
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(data);
        }
    }
}
