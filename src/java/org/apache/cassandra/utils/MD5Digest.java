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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

/**
 * The result of the computation of an MD5 digest.
 *
 * A MD5 is really just a byte[] but arrays are a no go as map keys. We could
 * wrap it in a ByteBuffer but:
 *   1. MD5Digest is a more explicit name than ByteBuffer to represent a md5.
 *   2. Using our own class allows to use our FastByteComparison for equals.
 */
public class MD5Digest
{
    /**
     * In the interest not breaking things, we're consciously keeping this single remaining instance
     * of MessageDigest around for usage by GuidGenerator (which is only ever used by RandomPartitioner)
     * and some client native transport methods, where we're tied to the usage of MD5 in the protocol.
     * As RandomPartitioner will always be MD5 and cannot be changed, we can switch over all our
     * other digest usage to Guava's Hasher to make switching the hashing function used during message
     * digests etc possible, but not regress on performance or bugs in RandomPartitioner's usage of
     * MD5 and MessageDigest.
     */
    private static final ThreadLocal<MessageDigest> localMD5Digest = new ThreadLocal<MessageDigest>()
    {
        @Override
        protected MessageDigest initialValue()
        {
            return HashingUtils.newMessageDigest("MD5");
        }

        @Override
        public MessageDigest get()
        {
            MessageDigest digest = super.get();
            digest.reset();
            return digest;
        }
    };

    public final byte[] bytes;
    private final int hashCode;

    private MD5Digest(byte[] bytes)
    {
        this.bytes = bytes;
        hashCode = Arrays.hashCode(bytes);
    }

    public static MD5Digest wrap(byte[] digest)
    {
        return new MD5Digest(digest);
    }

    public static MD5Digest compute(byte[] toHash)
    {
        return new MD5Digest(localMD5Digest.get().digest(toHash));
    }

    public static MD5Digest compute(String toHash)
    {
        return compute(toHash.getBytes(StandardCharsets.UTF_8));
    }

    public ByteBuffer byteBuffer()
    {
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public final int hashCode()
    {
        return hashCode;
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof MD5Digest))
            return false;
        MD5Digest that = (MD5Digest)o;
        // handles nulls properly
        return FBUtilities.compareUnsigned(this.bytes, that.bytes, 0, 0, this.bytes.length, that.bytes.length) == 0;
    }

    @Override
    public String toString()
    {
        return Hex.bytesToHex(bytes);
    }

    public static MessageDigest threadLocalMD5Digest()
    {
        return localMD5Digest.get();
    }
}
