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
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.utils.ByteBufferUtil;

public class RowCacheTest extends CleanupHelper
{
    private String KEYSPACE = "RowCacheSpace";
    private String COLUMN_FAMILY_WITH_CACHE = "CachedCF";
    private String COLUMN_FAMILY_WITHOUT_CACHE = "CFWithoutCache";

    @Test
    public void testRowCache() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open(KEYSPACE);
        ColumnFamilyStore cachedStore  = table.getColumnFamilyStore(COLUMN_FAMILY_WITH_CACHE);
        ColumnFamilyStore noCacheStore = table.getColumnFamilyStore(COLUMN_FAMILY_WITHOUT_CACHE);

        // empty the row cache
        cachedStore.invalidateRowCache();

        // inserting 100 rows into both column families
        insertData(KEYSPACE, COLUMN_FAMILY_WITH_CACHE, 0, 100);
        insertData(KEYSPACE, COLUMN_FAMILY_WITHOUT_CACHE, 0, 100);

        // now reading rows one by one and checking if row change grows
        for (int i = 0; i < 100; i++)
        {
            DecoratedKey key = Util.dk("key" + i);
            QueryPath path = new QueryPath(COLUMN_FAMILY_WITH_CACHE, null, ByteBufferUtil.bytes("col" + i));

            cachedStore.getColumnFamily(key, path, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
            assert cachedStore.getRowCacheSize() == i + 1;
            assert cachedStore.getRawCachedRow(key) != null; // current key should be stored in the cache

            // checking if column is read correctly after cache
            ColumnFamily cf = cachedStore.getColumnFamily(key, path, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
            Collection<IColumn> columns = cf.getSortedColumns();

            IColumn column = columns.iterator().next();

            assert columns.size() == 1;
            assert column.name().equals(ByteBufferUtil.bytes("col" + i));
            assert column.value().equals(ByteBufferUtil.bytes("val" + i));

            path = new QueryPath(COLUMN_FAMILY_WITHOUT_CACHE, null, ByteBufferUtil.bytes("col" + i));

            // row cache should not get populated for the second store
            noCacheStore.getColumnFamily(key, path, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
            assert noCacheStore.getRowCacheSize() == 0;
        }

        // insert 10 more keys and check that row cache is still at store.getRowCacheCapacity()
        insertData(KEYSPACE, COLUMN_FAMILY_WITH_CACHE, 100, 10);

        for (int i = 100; i < 110; i++)
        {
            DecoratedKey key = Util.dk("key" + i);
            QueryPath path = new QueryPath(COLUMN_FAMILY_WITH_CACHE, null, ByteBufferUtil.bytes("col" + i));

            cachedStore.getColumnFamily(key, path, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
            assert cachedStore.getRowCacheSize() == cachedStore.getRowCacheCapacity();
            assert cachedStore.getRawCachedRow(key) != null; // cache should be populated with the latest rows read (old ones should be popped)

            // checking if column is read correctly after cache
            ColumnFamily cf = cachedStore.getColumnFamily(key, path, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
            Collection<IColumn> columns = cf.getSortedColumns();

            IColumn column = columns.iterator().next();

            assert columns.size() == 1;
            assert column.name().equals(ByteBufferUtil.bytes("col" + i));
            assert column.value().equals(ByteBufferUtil.bytes("val" + i));
        }

        // clear all 100 rows from the cache
        int keysLeft = 99;
        for (int i = 109; i >= 10; i--)
        {
            cachedStore.invalidateCachedRow(Util.dk("key" + i));
            assert cachedStore.getRowCacheSize() == keysLeft;
            keysLeft--;
        }
    }

    @Test
    public void testRowCacheLoad() throws Exception
    {
        rowCacheLoad(100, 100, Integer.MAX_VALUE, false);
    }

    @Test
    public void testRowCachePartialLoad() throws Exception
    {
        rowCacheLoad(100, 50, 50, false);
    }

    @Test
    public void testRowCacheCapacityLoad() throws Exception
    {
        // 60 is default from DatabaseDescriptor
        rowCacheLoad(100, 60, Integer.MAX_VALUE, true);
    }


    public void rowCacheLoad(int totalKeys, int expectedKeys, int keysToSave, boolean reduceLoadCapacity) throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        ColumnFamilyStore store = Table.open(KEYSPACE).getColumnFamilyStore(COLUMN_FAMILY_WITH_CACHE);

        // empty the cache
        store.invalidateRowCache();
        assert store.getRowCacheSize() == 0;

        // insert data and fill the cache
        insertData(KEYSPACE, COLUMN_FAMILY_WITH_CACHE, 0, totalKeys);
        readData(KEYSPACE, COLUMN_FAMILY_WITH_CACHE, 0, totalKeys);
        assert store.getRowCacheSize() == totalKeys;

        // force the cache to disk
        store.rowCache.submitWrite(keysToSave).get();

        if (reduceLoadCapacity)
            store.reduceCacheSizes();

        // empty the cache again to make sure values came from disk
        store.invalidateRowCache();
        assert store.getRowCacheSize() == 0;

        // load the cache from disk
        store.initCaches();
        assert store.getRowCacheSize() == expectedKeys;

        // If we are loading less than the entire cache back, we can't
        // be sure which rows we will get if all rows are equally hot.
        int nulls = 0;
        int nonNull = 0;
        for (int i = 0; i < expectedKeys; i++)
        {
            // verify the correct data was found when we expect to get
            // back the entire cache.  Otherwise only make assertions
            // about how many items are read back.
            ColumnFamily row = store.getRawCachedRow(Util.dk("key" + i));
            if (expectedKeys == totalKeys)
            {
                assert row != null;
                assert row.getColumn(ByteBufferUtil.bytes("col" + i)).value().equals(ByteBufferUtil.bytes("val" + i));
            }
            if (row == null)
                nulls++;
            else
                nonNull++;
        }
        assert nulls + nonNull == expectedKeys;
    }
}
