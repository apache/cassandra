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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.net.MessagingService;

public class SerializingCacheProvider implements IRowCacheProvider
{
    public ICache<RowCacheKey, IRowCacheEntry> create(long capacity, boolean useMemoryWeigher)
    {
        return new SerializingCache<RowCacheKey, IRowCacheEntry>(capacity, useMemoryWeigher, new RowCacheSerializer());
    }

    // Package protected for tests
    static class RowCacheSerializer implements ISerializer<IRowCacheEntry>
    {
        public void serialize(IRowCacheEntry cf, DataOutput out)
        {
            assert cf != null; // unlike CFS we don't support nulls, since there is no need for that in the cache
            try
            {
                out.writeBoolean(cf instanceof RowCacheSentinel);
                if (cf instanceof RowCacheSentinel)
                    out.writeLong(((RowCacheSentinel) cf).sentinelId);
                else
                    ColumnFamily.serializer.serialize((ColumnFamily) cf, out, MessagingService.current_version);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        public IRowCacheEntry deserialize(DataInput in) throws IOException
        {
            boolean isSentinel = in.readBoolean();
            if (isSentinel)
                return new RowCacheSentinel(in.readLong());
            return ColumnFamily.serializer.deserialize(in, MessagingService.current_version);
        }

        public long serializedSize(IRowCacheEntry cf, TypeSizes typeSizes)
        {
            int size = typeSizes.sizeof(true);
            if (cf instanceof RowCacheSentinel)
                size += typeSizes.sizeof(((RowCacheSentinel) cf).sentinelId);
            else
                size += ColumnFamily.serializer.serializedSize((ColumnFamily) cf, typeSizes, MessagingService.current_version);
            return size;
        }
    }
}
