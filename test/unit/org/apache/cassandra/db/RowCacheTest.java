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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.cassandra.CleanupHelper;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.junit.Test;

public class RowCacheTest extends CleanupHelper
{
    @Test
    public void testRowCache() throws Exception
    {
        String KEYSPACE = "RowCacheSpace";
        String COLUMN_FAMILY_WITH_CACHE = "CachedCF";
        String COLUMN_FAMILY_WITHOUT_CACHE = "CFWithoutCache";

        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open(KEYSPACE);
        ColumnFamilyStore cachedStore  = table.getColumnFamilyStore(COLUMN_FAMILY_WITH_CACHE);
        ColumnFamilyStore noCacheStore = table.getColumnFamilyStore(COLUMN_FAMILY_WITHOUT_CACHE);

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

    private void insertData(String keyspace, String columnFamily, int offset, int numberOfRows) throws IOException
    {
        for (int i = offset; i < offset + numberOfRows; i++)
        {
            ByteBuffer key = ByteBufferUtil.bytes("key" + i);
            RowMutation rowMutation = new RowMutation(keyspace, key);
            QueryPath path = new QueryPath(columnFamily, null, ByteBufferUtil.bytes("col" + i));

            rowMutation.add(path, ByteBufferUtil.bytes("val" + i), System.currentTimeMillis());
            rowMutation.applyUnsafe();
        }
    }

}
