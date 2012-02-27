/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cache;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class RowCacheKey implements CacheKey, Comparable<RowCacheKey>
{
    public final int cfId;
    public final ByteBuffer key;

    public RowCacheKey(int cfId, DecoratedKey key)
    {
        this.cfId = cfId;
        this.key = key.key;
    }

    public ByteBuffer serializeForStorage()
    {
        ByteBuffer bytes = ByteBuffer.allocate(serializedSize());

        bytes.put(key.slice());
        bytes.rewind();

        return bytes;
    }

    public Pair<String, String> getPathInfo()
    {
        return Schema.instance.getCF(cfId);
    }

    public int serializedSize()
    {
        return key.remaining();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(131, 56337)
                .append(cfId)
                .append(key).toHashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        RowCacheKey otherKey = (RowCacheKey) obj;

        return cfId == otherKey.cfId && key.equals(otherKey.key);
    }

    @Override
    public int compareTo(RowCacheKey otherKey)
    {
        return (cfId < otherKey.cfId) ? -1 : ((cfId == otherKey.cfId) ? ByteBufferUtil.compareUnsigned(key, otherKey.key) : 1);
    }

    @Override
    public String toString()
    {
        return String.format("RowCacheKey(cfId:%d, key:%s)", cfId, key);
    }
}
