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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;

public final class RowCacheKey extends CacheKey
{
    public final byte[] key;

    private static final long EMPTY_SIZE = ObjectSizes.measure(new RowCacheKey(null, ByteBufferUtil.EMPTY_BYTE_BUFFER));

    public RowCacheKey(Pair<String, String> ksAndCFName, byte[] key)
    {
        super(ksAndCFName);
        this.key = key;
    }

    public RowCacheKey(Pair<String, String> ksAndCFName, DecoratedKey key)
    {
        this(ksAndCFName, key.getKey());
    }

    public RowCacheKey(Pair<String, String> ksAndCFName, ByteBuffer key)
    {
        super(ksAndCFName);
        this.key = ByteBufferUtil.getArray(key);
        assert this.key != null;
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

        return ksAndCFName.equals(that.ksAndCFName) && Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode()
    {
        int result = ksAndCFName.hashCode();
        result = 31 * result + (key != null ? Arrays.hashCode(key) : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return String.format("RowCacheKey(ksAndCFName:%s, key:%s)", ksAndCFName, Arrays.toString(key));
    }
}
