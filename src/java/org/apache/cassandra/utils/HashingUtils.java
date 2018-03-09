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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class HashingUtils
{
    public static final HashFunction CURRENT_HASH_FUNCTION = Hashing.md5();

    public static MessageDigest newMessageDigest(String algorithm)
    {
        try
        {
            return MessageDigest.getInstance(algorithm);
        }
        catch (NoSuchAlgorithmException nsae)
        {
            throw new RuntimeException("the requested digest algorithm (" + algorithm + ") is not available", nsae);
        }
    }

    public static void updateBytes(Hasher hasher, ByteBuffer input)
    {
        if (!input.hasRemaining())
            return;

        if (input.hasArray())
        {
            byte[] b = input.array();
            int ofs = input.arrayOffset();
            int pos = input.position();
            int lim = input.limit();
            hasher.putBytes(b, ofs + pos, lim - pos);
            input.position(lim);
        }
        else
        {
            int len = input.remaining();
            int n = Math.min(len, 1 << 12); // either the remaining amount or 4kb
            byte[] tempArray = new byte[n];
            while (len > 0)
            {
                int chunk = Math.min(len, tempArray.length);
                input.get(tempArray, 0, chunk);
                hasher.putBytes(tempArray, 0, chunk);
                len -= chunk;
            }
        }
    }

    public static void updateWithShort(Hasher hasher, int val)
    {
        hasher.putByte((byte) ((val >> 8) & 0xFF));
        hasher.putByte((byte) (val & 0xFF));
    }

    public static void updateWithByte(Hasher hasher, int val)
    {
        hasher.putByte((byte) (val & 0xFF));
    }

    public static void updateWithInt(Hasher hasher, int val)
    {
        hasher.putByte((byte) ((val >>> 24) & 0xFF));
        hasher.putByte((byte) ((val >>> 16) & 0xFF));
        hasher.putByte((byte) ((val >>>  8) & 0xFF));
        hasher.putByte((byte) ((val >>> 0) & 0xFF));
    }

    public static void updateWithLong(Hasher hasher, long val)
    {
        hasher.putByte((byte) ((val >>> 56) & 0xFF));
        hasher.putByte((byte) ((val >>> 48) & 0xFF));
        hasher.putByte((byte) ((val >>> 40) & 0xFF));
        hasher.putByte((byte) ((val >>> 32) & 0xFF));
        hasher.putByte((byte) ((val >>> 24) & 0xFF));
        hasher.putByte((byte) ((val >>> 16) & 0xFF));
        hasher.putByte((byte) ((val >>>  8) & 0xFF));
        hasher.putByte((byte)  ((val >>> 0) & 0xFF));
    }

    public static void updateWithBoolean(Hasher hasher, boolean val)
    {
        updateWithByte(hasher, val ? 0 : 1);
    }
}
