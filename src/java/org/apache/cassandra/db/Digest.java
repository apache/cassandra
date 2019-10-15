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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.utils.FastByteOperations;

public class Digest
{
    private static final ThreadLocal<byte[]> localBuffer = ThreadLocal.withInitial(() -> new byte[4096]);

    private final Hasher hasher;
    private long inputBytes = 0;

    @SuppressWarnings("deprecation")
    private static Hasher md5()
    {
        return Hashing.md5().newHasher();
    }

    public static Digest forReadResponse()
    {
        return new Digest(md5());
    }

    public static Digest forSchema()
    {
        return new Digest(md5());
    }

    public static Digest forValidator()
    {
        // Uses a Hasher that concatenates the hash code from 2 hash functions
        // (murmur3_128) with different seeds to produce a 256 bit hashcode
        return new Digest(Hashing.concatenating(Hashing.murmur3_128(1000),
                                                Hashing.murmur3_128(2000))
                                 .newHasher());
    }

    public static Digest forRepairedDataTracking()
    {
        return new Digest(Hashing.crc32c().newHasher())
        {
            @Override
            public <V> Digest updateWithCounterContext(V context, ValueAccessor<V> accessor)
            {
                // for the purposes of repaired data tracking on the read path, exclude
                // contexts with legacy shards as these may be irrevocably different on
                // different replicas
                if (CounterContext.instance().hasLegacyShards(context, accessor))
                    return this;

                return super.updateWithCounterContext(context, accessor);
            }
        };
    }

    Digest(Hasher hasher)
    {
        this.hasher = hasher;
    }

    public Digest update(byte[] input, int offset, int len)
    {
        hasher.putBytes(input, offset, len);
        inputBytes += len;
        return this;
    }

    public <V> Digest update(V input, ValueAccessor<V> accessor)
    {
        accessor.digest(input, this);
        return this;
    }

    /**
     * Update the digest with the bytes from the supplied buffer. This does
     * not modify the position of the supplied buffer, so callers are not
     * required to duplicate() the source buffer before calling
     */
    public Digest update(ByteBuffer input)
    {
        return update(input, input.position(), input.remaining());
    }

    /**
     * Update the digest with the bytes sliced from the supplied buffer. This does
     * not modify the position of the supplied buffer, so callers are not
     * required to duplicate() the source buffer before calling
     */
    public Digest update(ByteBuffer input, int pos, int len)
    {
        if (len <= 0)
            return this;

        if (input.hasArray())
        {
            byte[] b = input.array();
            int ofs = input.arrayOffset();
            hasher.putBytes(b, ofs + pos, len);
            inputBytes += len;
        }
        else
        {
            byte[] tempArray = localBuffer.get();
            while (len > 0)
            {
                int chunk = Math.min(len, tempArray.length);
                FastByteOperations.copy(input, pos, tempArray, 0, chunk);
                hasher.putBytes(tempArray, 0, chunk);
                len -= chunk;
                pos += chunk;
                inputBytes += chunk;
            }
        }
        return this;
    }

    /**
     * Update the digest with the content of a counter context.
     * Note that this skips the header entirely since the header information
     * has local meaning only, while digests are meant for comparison across
     * nodes. This means in particular that we always have:
     *  updateDigest(ctx) == updateDigest(clearAllLocal(ctx))
     */
    public <V> Digest updateWithCounterContext(V context, ValueAccessor<V> accessor)
    {
        // context can be empty due to the optimization from CASSANDRA-10657
        if (accessor.isEmpty(context))
            return this;

        int pos = CounterContext.headerLength(context, accessor);
        int len = accessor.size(context) - pos;
        accessor.digest(context, pos, len, this);
        return this;
    }

    public Digest updateWithByte(int val)
    {
        hasher.putByte((byte) (val & 0xFF));
        inputBytes++;
        return this;
    }

    public Digest updateWithInt(int val)
    {
        hasher.putByte((byte) ((val >>> 24) & 0xFF));
        hasher.putByte((byte) ((val >>> 16) & 0xFF));
        hasher.putByte((byte) ((val >>>  8) & 0xFF));
        hasher.putByte((byte) ((val >>> 0) & 0xFF));
        inputBytes += 4;
        return this;
    }

    public Digest updateWithLong(long val)
    {
        hasher.putByte((byte) ((val >>> 56) & 0xFF));
        hasher.putByte((byte) ((val >>> 48) & 0xFF));
        hasher.putByte((byte) ((val >>> 40) & 0xFF));
        hasher.putByte((byte) ((val >>> 32) & 0xFF));
        hasher.putByte((byte) ((val >>> 24) & 0xFF));
        hasher.putByte((byte) ((val >>> 16) & 0xFF));
        hasher.putByte((byte) ((val >>>  8) & 0xFF));
        hasher.putByte((byte)  ((val >>> 0) & 0xFF));
        inputBytes += 8;
        return this;
    }

    public Digest updateWithBoolean(boolean val)
    {
        updateWithByte(val ? 0 : 1);
        return this;
    }

    public byte[] digest()
    {
        return hasher.hash().asBytes();
    }

    public long inputBytes()
    {
        return inputBytes;
    }
}

