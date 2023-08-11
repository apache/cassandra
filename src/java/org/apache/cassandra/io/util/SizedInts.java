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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Utility class for sizing, writing and reading ints with length stored separately.
 * Used for trie payloads.
 */
public class SizedInts
{
    /**
     * Returns the number of bytes we need to store the given position.
     * This method understands 0 to need 1 byte.
     * <p>
     * If your use case permits 0 to be encoded in 0 length, use {@link #sizeAllowingZero} below.
     */
    public static int nonZeroSize(long value)
    {
        if (value < 0)
            value = ~value;
        int lz = Long.numberOfLeadingZeros(value);       // 1 <= lz <= 64
        return (64 - lz + 1 + 7) / 8;   // significant bits, +1 for sign, rounded up. At least 1, at most 8.
    }

    /**
     * Returns the number of bytes we need to store the given position. Returns 0 for 0 argument.
     */
    public static int sizeAllowingZero(long value)
    {
        if (value == 0)
            return 0;
        return nonZeroSize(value);
    }

    public static long read(ByteBuffer src, int startPos, int bytes)
    {
        switch (bytes)
        {
            case 0:
                return 0;
            case 1:
                return src.get(startPos);
            case 2:
                return src.getShort(startPos);
            case 3:
            {
                long high = src.get(startPos);
                return (high << 16L) | (src.getShort(startPos + 1) & 0xFFFFL);
            }
            case 4:
                return src.getInt(startPos);
            case 5:
            {
                long high = src.get(startPos);
                return (high << 32L) | (src.getInt(startPos + 1) & 0xFFFFFFFFL);
            }
            case 6:
            {
                long high = src.getShort(startPos);
                return (high << 32L) | (src.getInt(startPos + 2) & 0xFFFFFFFFL);
            }
            case 7:
            {
                long high = src.get(startPos);
                high = (high << 16L) | (src.getShort(startPos + 1) & 0xFFFFL);
                return (high << 32L) | (src.getInt(startPos + 3) & 0xFFFFFFFFL);
            }
            case 8:
                return src.getLong(startPos);
            default:
                throw new AssertionError();
        }
    }

    public static long readUnsigned(ByteBuffer src, int startPos, int bytes)
    {
        if (bytes == 8)
            return src.getLong(startPos);
        else
            return read(src, startPos, bytes) & ((1L << (bytes * 8)) - 1);
    }

    public static void write(DataOutputPlus dest, long value, int size) throws IOException
    {
        dest.writeMostSignificantBytes(value << ((8 - size) * 8), size);
    }
}