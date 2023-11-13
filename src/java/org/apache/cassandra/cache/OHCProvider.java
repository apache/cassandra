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
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.schema.TableId;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;

public class OHCProvider implements CacheProvider<RowCacheKey, IRowCacheEntry>
{
    public ICache<RowCacheKey, IRowCacheEntry> create()
    {
        OHCacheBuilder<RowCacheKey, IRowCacheEntry> builder = OHCacheBuilder.newBuilder();
        builder.capacity(DatabaseDescriptor.getRowCacheSizeInMiB() * 1024 * 1024)
               .keySerializer(KeySerializer.instance)
               .valueSerializer(ValueSerializer.instance)
               .throwOOME(true);

        return new OHCacheAdapter(builder.build());
    }

    private static class OHCacheAdapter implements ICache<RowCacheKey, IRowCacheEntry>
    {
        private final OHCache<RowCacheKey, IRowCacheEntry> ohCache;

        public OHCacheAdapter(OHCache<RowCacheKey, IRowCacheEntry> ohCache)
        {
            this.ohCache = ohCache;
        }

        public long capacity()
        {
            return ohCache.capacity();
        }

        public void setCapacity(long capacity)
        {
            ohCache.setCapacity(capacity);
        }

        public void put(RowCacheKey key, IRowCacheEntry value)
        {
            ohCache.put(key,  value);
        }

        public boolean putIfAbsent(RowCacheKey key, IRowCacheEntry value)
        {
            return ohCache.putIfAbsent(key, value);
        }

        public boolean replace(RowCacheKey key, IRowCacheEntry old, IRowCacheEntry value)
        {
            return ohCache.addOrReplace(key, old, value);
        }

        public IRowCacheEntry get(RowCacheKey key)
        {
            return ohCache.get(key);
        }

        public void remove(RowCacheKey key)
        {
            ohCache.remove(key);
        }

        public int size()
        {
            return (int) ohCache.size();
        }

        public long weightedSize()
        {
            return ohCache.memUsed();
        }

        public void clear()
        {
            ohCache.clear();
        }

        public Iterator<RowCacheKey> hotKeyIterator(int n)
        {
            return ohCache.hotKeyIterator(n);
        }

        public Iterator<RowCacheKey> keyIterator()
        {
            return ohCache.keyIterator();
        }

        public boolean containsKey(RowCacheKey key)
        {
            return ohCache.containsKey(key);
        }
    }

    private static class KeySerializer implements org.caffinitas.ohc.CacheSerializer<RowCacheKey>
    {
        private static KeySerializer instance = new KeySerializer();
        public void serialize(RowCacheKey rowCacheKey, ByteBuffer buf)
        {
            try (DataOutputBuffer dataOutput = new DataOutputBufferFixed(buf))
            {
                rowCacheKey.tableId.serialize(dataOutput);
                dataOutput.writeUTF(rowCacheKey.indexName != null ? rowCacheKey.indexName : "");
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            buf.putInt(rowCacheKey.key.length);
            buf.put(rowCacheKey.key);
        }

        public RowCacheKey deserialize(ByteBuffer buf)
        {
            TableId tableId = null;
            String indexName = null;
            try (DataInputBuffer dataInput = new DataInputBuffer(buf, false))
            {
                tableId = TableId.deserialize(dataInput);
                indexName = dataInput.readUTF();
                if (indexName.isEmpty())
                    indexName = null;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            byte[] key = new byte[buf.getInt()];
            buf.get(key);
            return new RowCacheKey(tableId, indexName, key);
        }

        public int serializedSize(RowCacheKey rowCacheKey)
        {
            return rowCacheKey.tableId.serializedSize()
                   + TypeSizes.sizeof(rowCacheKey.indexName != null ? rowCacheKey.indexName : "")
                   + 4
                   + rowCacheKey.key.length;
        }
    }

    private static class ValueSerializer implements org.caffinitas.ohc.CacheSerializer<IRowCacheEntry>
    {
        private static ValueSerializer instance = new ValueSerializer();
        public void serialize(IRowCacheEntry entry, ByteBuffer buf)
        {
            assert entry != null; // unlike CFS we don't support nulls, since there is no need for that in the cache
            try (DataOutputBufferFixed out = new DataOutputBufferFixed(buf))
            {
                boolean isSentinel = entry instanceof RowCacheSentinel;
                out.writeBoolean(isSentinel);
                if (isSentinel)
                    out.writeLong(((RowCacheSentinel) entry).sentinelId);
                else
                    CachedPartition.cacheSerializer.serialize((CachedPartition)entry, out);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public IRowCacheEntry deserialize(ByteBuffer buf)
        {
            try (RebufferingInputStream in = new DataInputBuffer(buf, false))
            {
                boolean isSentinel = in.readBoolean();
                if (isSentinel)
                    return new RowCacheSentinel(in.readLong());
                return CachedPartition.cacheSerializer.deserialize(in);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public int serializedSize(IRowCacheEntry entry)
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
