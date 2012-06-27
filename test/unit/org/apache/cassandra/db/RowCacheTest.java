/**
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

import java.util.Collection;

import org.junit.AfterClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.db.filter.QueryPath;

public class RowCacheTest extends SchemaLoader
{
    private String KEYSPACE = "RowCacheSpace";
    private String COLUMN_FAMILY = "CachedCF";

    @AfterClass
    public static void cleanup()
    {
        cleanupSavedCaches();
    }

    @Test
    public void testRowCache() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open(KEYSPACE);
        ColumnFamilyStore cachedStore  = table.getColumnFamilyStore(COLUMN_FAMILY);

        // empty the row cache
        CacheService.instance.invalidateRowCache();

        // set global row cache size to 1 MB
        CacheService.instance.setRowCacheCapacityInMB(1);

        // inserting 100 rows into both column families
        insertData(KEYSPACE, COLUMN_FAMILY, 0, 100);

        // now reading rows one by one and checking if row change grows
        for (int i = 0; i < 100; i++)
        {
            DecoratedKey key = Util.dk("key" + i);
            QueryPath path = new QueryPath(COLUMN_FAMILY, null, ByteBufferUtil.bytes("col" + i));

            cachedStore.getColumnFamily(key, path, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
            assert CacheService.instance.rowCache.size() == i + 1;
            assert cachedStore.containsCachedRow(key); // current key should be stored in the cache

            // checking if column is read correctly after cache
            ColumnFamily cf = cachedStore.getColumnFamily(key, path, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
            Collection<IColumn> columns = cf.getSortedColumns();

            IColumn column = columns.iterator().next();

            assert columns.size() == 1;
            assert column.name().equals(ByteBufferUtil.bytes("col" + i));
            assert column.value().equals(ByteBufferUtil.bytes("val" + i));
        }

        // insert 10 more keys
        insertData(KEYSPACE, COLUMN_FAMILY, 100, 10);

        for (int i = 100; i < 110; i++)
        {
            DecoratedKey key = Util.dk("key" + i);
            QueryPath path = new QueryPath(COLUMN_FAMILY, null, ByteBufferUtil.bytes("col" + i));

            cachedStore.getColumnFamily(key, path, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
            assert cachedStore.containsCachedRow(key); // cache should be populated with the latest rows read (old ones should be popped)

            // checking if column is read correctly after cache
            ColumnFamily cf = cachedStore.getColumnFamily(key, path, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
            Collection<IColumn> columns = cf.getSortedColumns();

            IColumn column = columns.iterator().next();

            assert columns.size() == 1;
            assert column.name().equals(ByteBufferUtil.bytes("col" + i));
            assert column.value().equals(ByteBufferUtil.bytes("val" + i));
        }

        // clear 100 rows from the cache
        int keysLeft = 109;
        for (int i = 109; i >= 10; i--)
        {
            cachedStore.invalidateCachedRow(Util.dk("key" + i));
            assert CacheService.instance.rowCache.size() == keysLeft;
            keysLeft--;
        }

        CacheService.instance.setRowCacheCapacityInMB(0);
    }

    @Test
    public void testRowCacheLoad() throws Exception
    {
        CacheService.instance.setRowCacheCapacityInMB(1);
        rowCacheLoad(100, Integer.MAX_VALUE, false);
        CacheService.instance.setRowCacheCapacityInMB(0);
    }

    @Test
    public void testRowCachePartialLoad() throws Exception
    {
        CacheService.instance.setRowCacheCapacityInMB(1);
        rowCacheLoad(100, 50, true);
        CacheService.instance.setRowCacheCapacityInMB(0);
    }

    public void rowCacheLoad(int totalKeys, int keysToSave, boolean reduceLoadCapacity) throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        ColumnFamilyStore store = Table.open(KEYSPACE).getColumnFamilyStore(COLUMN_FAMILY);

        // empty the cache
        CacheService.instance.invalidateRowCache();
        assert CacheService.instance.rowCache.size() == 0;

        // insert data and fill the cache
        insertData(KEYSPACE, COLUMN_FAMILY, 0, totalKeys);
        readData(KEYSPACE, COLUMN_FAMILY, 0, totalKeys);
        assert CacheService.instance.rowCache.size() == totalKeys;

        // force the cache to disk
        CacheService.instance.rowCache.submitWrite(keysToSave).get();

        if (reduceLoadCapacity)
            CacheService.instance.reduceRowCacheSize();

        // empty the cache again to make sure values came from disk
        CacheService.instance.invalidateRowCache();
        assert CacheService.instance.rowCache.size() == 0;
        assert CacheService.instance.rowCache.loadSaved(store) == (keysToSave == Integer.MAX_VALUE ? totalKeys : keysToSave);
    }
}
