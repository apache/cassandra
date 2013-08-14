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

import java.net.InetAddress;
import java.util.Collection;

import org.junit.AfterClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import static org.junit.Assert.assertEquals;

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

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cachedStore  = keyspace.getColumnFamilyStore(COLUMN_FAMILY);

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

            cachedStore.getColumnFamily(key,
                                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                        false,
                                        1,
                                        System.currentTimeMillis());
            assert CacheService.instance.rowCache.size() == i + 1;
            assert cachedStore.containsCachedRow(key); // current key should be stored in the cache

            // checking if column is read correctly after cache
            ColumnFamily cf = cachedStore.getColumnFamily(key,
                                                          ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                          ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                          false,
                                                          1,
                                                          System.currentTimeMillis());
            Collection<Column> columns = cf.getSortedColumns();

            Column column = columns.iterator().next();

            assert columns.size() == 1;
            assert column.name().equals(ByteBufferUtil.bytes("col" + i));
            assert column.value().equals(ByteBufferUtil.bytes("val" + i));
        }

        // insert 10 more keys
        insertData(KEYSPACE, COLUMN_FAMILY, 100, 10);

        for (int i = 100; i < 110; i++)
        {
            DecoratedKey key = Util.dk("key" + i);

            cachedStore.getColumnFamily(key,
                                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                        false,
                                        1,
                                        System.currentTimeMillis());
            assert cachedStore.containsCachedRow(key); // cache should be populated with the latest rows read (old ones should be popped)

            // checking if column is read correctly after cache
            ColumnFamily cf = cachedStore.getColumnFamily(key,
                                                          ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                          ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                          false,
                                                          1,
                                                          System.currentTimeMillis());
            Collection<Column> columns = cf.getSortedColumns();

            Column column = columns.iterator().next();

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
        rowCacheLoad(100, Integer.MAX_VALUE, 0);
        CacheService.instance.setRowCacheCapacityInMB(0);
    }

    @Test
    public void testRowCacheCleanup() throws Exception
    {
        StorageService.instance.initServer(0);
        CacheService.instance.setRowCacheCapacityInMB(1);
        rowCacheLoad(100, Integer.MAX_VALUE, 1000);

        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(COLUMN_FAMILY);
        assertEquals(CacheService.instance.rowCache.getKeySet().size(), 100);
        store.cleanupCache();
        assertEquals(CacheService.instance.rowCache.getKeySet().size(), 100);
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        byte[] tk1, tk2;
        tk1 = "key1000".getBytes();
        tk2 = "key1050".getBytes();
        tmd.updateNormalToken(new BytesToken(tk1), InetAddress.getByName("127.0.0.1"));
        tmd.updateNormalToken(new BytesToken(tk2), InetAddress.getByName("127.0.0.2"));
        store.cleanupCache();
        assertEquals(CacheService.instance.rowCache.getKeySet().size(), 50);
        CacheService.instance.setRowCacheCapacityInMB(0);
    }

    @Test
    public void testRowCachePartialLoad() throws Exception
    {
        CacheService.instance.setRowCacheCapacityInMB(1);
        rowCacheLoad(100, 50, 0);
        CacheService.instance.setRowCacheCapacityInMB(0);
    }

    public void rowCacheLoad(int totalKeys, int keysToSave, int offset) throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(COLUMN_FAMILY);

        // empty the cache
        CacheService.instance.invalidateRowCache();
        assert CacheService.instance.rowCache.size() == 0;

        // insert data and fill the cache
        insertData(KEYSPACE, COLUMN_FAMILY, offset, totalKeys);
        readData(KEYSPACE, COLUMN_FAMILY, offset, totalKeys);
        assert CacheService.instance.rowCache.size() == totalKeys;

        // force the cache to disk
        CacheService.instance.rowCache.submitWrite(keysToSave).get();

        // empty the cache again to make sure values came from disk
        CacheService.instance.invalidateRowCache();
        assert CacheService.instance.rowCache.size() == 0;
        assert CacheService.instance.rowCache.loadSaved(store) == (keysToSave == Integer.MAX_VALUE ? totalKeys : keysToSave);
    }
}
