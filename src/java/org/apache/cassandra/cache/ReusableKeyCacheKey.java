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

package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A {@link KeyCacheKey} for lookups only: the caller replaces its partition key bytes before each
 * lookup, so one instance serves every partition. It equals and hashes the same as a
 * {@link KeyCacheKey} with the same table, sstable and key bytes. It is never put in the cache,
 * since the cache would then hold a key whose bytes change.
 * <p>
 * The key cache map calls {@code equals} on the key it is given, so a lookup with this key
 * finds the stored {@link KeyCacheKey}.
 */
public final class ReusableKeyCacheKey extends KeyCacheKey
{
    /** The hash of the table, index and sstable, the part of {@link KeyCacheKey#hashCode} before the key bytes. */
    private final int prefixHash;

    private byte[] bytes;
    private int offset;
    private int length;
    private int bytesHash;

    public ReusableKeyCacheKey(TableMetadata tableMetadata, Descriptor desc)
    {
        super(tableMetadata, desc);
        int result = tableId.hashCode();
        result = 31 * result + Objects.hashCode(indexName);
        result = 31 * result + desc.hashCode();
        this.prefixHash = result;
    }

    /**
     * Sets the key bytes for the next lookup. The array is not copied, so it must not change while
     * the lookup runs.
     *
     * @param bytesHash {@link #hashBytes} of the same range, so a caller that looks one key up in
     *                  many sstables hashes its bytes once
     */
    public void setKey(byte[] bytes, int offset, int length, int bytesHash)
    {
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        this.bytesHash = bytesHash;
    }

    /** The same value as {@link Arrays#hashCode(byte[])} of the range. */
    public static int hashBytes(byte[] bytes, int offset, int length)
    {
        int result = 1;
        for (int i = offset; i < offset + length; i++)
            result = 31 * result + bytes[i];
        return result;
    }

    @Override
    public int hashCode()
    {
        return 31 * prefixHash + bytesHash;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof KeyCacheKey)) return false;

        KeyCacheKey that = (KeyCacheKey) o;
        if (!tableId.equals(that.tableId) || !Objects.equals(indexName, that.indexName) || !desc.equals(that.desc))
            return false;
        if (that instanceof ReusableKeyCacheKey)
        {
            ReusableKeyCacheKey other = (ReusableKeyCacheKey) that;
            return Arrays.equals(bytes, offset, offset + length, other.bytes, other.offset, other.offset + other.length);
        }
        return Arrays.equals(bytes, offset, offset + length, that.key, 0, that.key.length);
    }

    /** The cache weighs only the keys it stores, and this one is never stored. */
    @Override
    public long unsharedHeapSize()
    {
        return 0;
    }

    @Override
    public String toString()
    {
        return String.format("ReusableKeyCacheKey(%s, %s)", desc, ByteBufferUtil.bytesToHex(ByteBuffer.wrap(bytes, offset, length)));
    }
}
