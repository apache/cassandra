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
import java.util.UUID;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;

public class RowCacheKey implements CacheKey, Comparable<RowCacheKey>
{
    public final UUID cfId;
    public final byte[] key;

    private static final long EMPTY_SIZE = ObjectSizes.measure(new RowCacheKey(null, ByteBufferUtil.EMPTY_BYTE_BUFFER));

    public RowCacheKey(UUID cfId, DecoratedKey key)
    {
        this(cfId, key.getKey());
    }

    public RowCacheKey(UUID cfId, ByteBuffer key)
    {
        this.cfId = cfId;
        this.key = ByteBufferUtil.getArray(key);
        assert this.key != null;
    }

    public UUID getCFId()
    {
        return cfId;
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOfArray(key);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RowCacheKey that = (RowCacheKey) o;

        return cfId.equals(that.cfId) && Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode()
    {
        int result = cfId.hashCode();
        result = 31 * result + (key != null ? Arrays.hashCode(key) : 0);
        return result;
    }

    public int compareTo(RowCacheKey otherKey)
    {
        return (cfId.compareTo(otherKey.cfId) < 0) ? -1 : ((cfId.equals(otherKey.cfId)) ?  FBUtilities.compareUnsigned(key, otherKey.key, 0, 0, key.length, otherKey.key.length) : 1);
    }

    @Override
    public String toString()
    {
        return String.format("RowCacheKey(cfId:%s, key:%s)", cfId, Arrays.toString(key));
    }
}
