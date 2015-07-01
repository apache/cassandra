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

import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class SerializingCacheProvider implements CacheProvider<RowCacheKey, IRowCacheEntry>
{
    public ICache<RowCacheKey, IRowCacheEntry> create()
    {
        return SerializingCache.create(DatabaseDescriptor.getRowCacheSizeInMB() * 1024 * 1024, new RowCacheSerializer());
    }

    // Package protected for tests
    static class RowCacheSerializer implements ISerializer<IRowCacheEntry>
    {
        public void serialize(IRowCacheEntry entry, DataOutputPlus out) throws IOException
        {
            assert entry != null; // unlike CFS we don't support nulls, since there is no need for that in the cache
            boolean isSentinel = entry instanceof RowCacheSentinel;
            out.writeBoolean(isSentinel);
            if (isSentinel)
                out.writeLong(((RowCacheSentinel) entry).sentinelId);
            else
                CachedPartition.cacheSerializer.serialize((CachedPartition)entry, out);
        }

        public IRowCacheEntry deserialize(DataInputPlus in) throws IOException
        {
            boolean isSentinel = in.readBoolean();
            if (isSentinel)
                return new RowCacheSentinel(in.readLong());

            return CachedPartition.cacheSerializer.deserialize(in);
        }

        public long serializedSize(IRowCacheEntry entry)
        {
            int size = TypeSizes.sizeof(true);
            if (entry instanceof RowCacheSentinel)
                size += TypeSizes.sizeof(((RowCacheSentinel) entry).sentinelId);
            else
                size += CachedPartition.cacheSerializer.serializedSize((CachedPartition) entry);
            return size;
        }
    }
}
