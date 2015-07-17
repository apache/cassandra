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

package org.apache.cassandra.db;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.*;

public class RowCacheTest
{
    private static final String KEYSPACE_CACHED = "RowCacheTest";
    private static final String CF_CACHED = "CachedCF";
    private static final String CF_CACHEDINT = "CachedIntCF";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE_CACHED,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE_CACHED, CF_CACHED).caching(CachingParams.CACHE_EVERYTHING),
                                    SchemaLoader.standardCFMD(KEYSPACE_CACHED, CF_CACHEDINT, 1, IntegerType.instance)
                                                .caching(new CachingParams(true, 100)));
    }

    @AfterClass
    public static void cleanup()
    {
        SchemaLoader.cleanupSavedCaches();
    }

    @Test
    public void testRoundTrip() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE_CACHED);
        String cf = "CachedIntCF";
        ColumnFamilyStore cachedStore  = keyspace.getColumnFamilyStore(cf);
        long startRowCacheHits = cachedStore.metric.rowCacheHit.getCount();
        long startRowCacheOutOfRange = cachedStore.metric.rowCacheHitOutOfRange.getCount();
        // empty the row cache
        CacheService.instance.invalidateRowCache();

        // set global row cache size to 1 MB
        CacheService.instance.setRowCacheCapacityInMB(1);

        ByteBuffer key = ByteBufferUtil.bytes("rowcachekey");
        DecoratedKey dk = cachedStore.decorateKey(key);
        RowCacheKey rck = new RowCacheKey(cachedStore.metadata.cfId, dk);

        RowUpdateBuilder rub = new RowUpdateBuilder(cachedStore.metadata, System.currentTimeMillis(), key);
        rub.clustering(String.valueOf(0));
        rub.add("val", ByteBufferUtil.bytes("val" + 0));
        rub.build().applyUnsafe();

        // populate row cache, we should not get a row cache hit;
        Util.getAll(Util.cmd(cachedStore, dk).withLimit(1).build());
        assertEquals(startRowCacheHits, cachedStore.metric.rowCacheHit.getCount());

        // do another query, limit is 20, which is < 100 that we cache, we should get a hit and it should be in range
        Util.getAll(Util.cmd(cachedStore, dk).withLimit(1).build());
        assertEquals(++startRowCacheHits, cachedStore.metric.rowCacheHit.getCount());
        assertEquals(startRowCacheOutOfRange, cachedStore.metric.rowCacheHitOutOfRange.getCount());

        CachedPartition cachedCf = (CachedPartition)CacheService.instance.rowCache.get(rck);
        assertEquals(1, cachedCf.rowCount());
        for (Unfiltered unfiltered : Util.once(cachedCf.unfilteredIterator(ColumnFilter.selection(cachedCf.columns()), Slices.ALL, false)))
        {
            Row r = (Row) unfiltered;
            for (ColumnData c : r)
            {
                assertEquals(((Cell)c).value(), ByteBufferUtil.bytes("val" + 0));
            }
        }
        cachedStore.truncateBlocking();
    }

    @Test
    public void testRowCache() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE_CACHED);
        ColumnFamilyStore cachedStore  = keyspace.getColumnFamilyStore(CF_CACHED);

        // empty the row cache
        CacheService.instance.invalidateRowCache();

        // set global row cache size to 1 MB
        CacheService.instance.setRowCacheCapacityInMB(1);

        // inserting 100 rows into both column families
        SchemaLoader.insertData(KEYSPACE_CACHED, CF_CACHED, 0, 100);

        // now reading rows one by one and checking if row change grows
        for (int i = 0; i < 100; i++)
        {
            DecoratedKey key = Util.dk("key" + i);

            Util.getAll(Util.cmd(cachedStore, key).build());
            assert CacheService.instance.rowCache.size() == i + 1;
            assert cachedStore.containsCachedParition(key); // current key should be stored in the cache

            // checking if cell is read correctly after cache
            CachedPartition cp = cachedStore.getRawCachedPartition(key);
            try (UnfilteredRowIterator ai = cp.unfilteredIterator(ColumnFilter.selection(cp.columns()), Slices.ALL, false))
            {
                assert ai.hasNext();
                Row r = (Row)ai.next();
                assertFalse(ai.hasNext());

                Iterator<Cell> ci = r.cells().iterator();
                assert(ci.hasNext());
                Cell cell = ci.next();

                assert cell.column().name.bytes.equals(ByteBufferUtil.bytes("val"));
                assert cell.value().equals(ByteBufferUtil.bytes("val" + i));
            }
        }

        // insert 10 more keys
        SchemaLoader.insertData(KEYSPACE_CACHED, CF_CACHED, 100, 10);

        for (int i = 100; i < 110; i++)
        {
            DecoratedKey key = Util.dk("key" + i);

            Util.getAll(Util.cmd(cachedStore, key).build());
            assert cachedStore.containsCachedParition(key); // cache should be populated with the latest rows read (old ones should be popped)

            // checking if cell is read correctly after cache
            CachedPartition cp = cachedStore.getRawCachedPartition(key);
            try (UnfilteredRowIterator ai = cp.unfilteredIterator(ColumnFilter.selection(cp.columns()), Slices.ALL, false))
            {
                assert ai.hasNext();
                Row r = (Row)ai.next();
                assertFalse(ai.hasNext());

                Iterator<Cell> ci = r.cells().iterator();
                assert(ci.hasNext());
                Cell cell = ci.next();

                assert cell.column().name.bytes.equals(ByteBufferUtil.bytes("val"));
                assert cell.value().equals(ByteBufferUtil.bytes("val" + i));
            }
        }

        // clear 100 rows from the cache
        int keysLeft = 109;
        for (int i = 109; i >= 10; i--)
        {
            cachedStore.invalidateCachedPartition(Util.dk("key" + i));
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

        ColumnFamilyStore store = Keyspace.open(KEYSPACE_CACHED).getColumnFamilyStore(CF_CACHED);
        assertEquals(CacheService.instance.rowCache.size(), 100);
        store.cleanupCache();
        assertEquals(CacheService.instance.rowCache.size(), 100);
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        byte[] tk1, tk2;
        tk1 = "key1000".getBytes();
        tk2 = "key1050".getBytes();
        tmd.updateNormalToken(new BytesToken(tk1), InetAddress.getByName("127.0.0.1"));
        tmd.updateNormalToken(new BytesToken(tk2), InetAddress.getByName("127.0.0.2"));
        store.cleanupCache();
        assertEquals(50, CacheService.instance.rowCache.size());
        CacheService.instance.setRowCacheCapacityInMB(0);
    }

    @Test
    public void testRowCachePartialLoad() throws Exception
    {
        CacheService.instance.setRowCacheCapacityInMB(1);
        rowCacheLoad(100, 50, 0);
        CacheService.instance.setRowCacheCapacityInMB(0);
    }

    @Test
    public void testRowCacheRange()
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE_CACHED);
        String cf = "CachedIntCF";
        ColumnFamilyStore cachedStore  = keyspace.getColumnFamilyStore(cf);
        long startRowCacheHits = cachedStore.metric.rowCacheHit.getCount();
        long startRowCacheOutOfRange = cachedStore.metric.rowCacheHitOutOfRange.getCount();
        // empty the row cache
        CacheService.instance.invalidateRowCache();

        // set global row cache size to 1 MB
        CacheService.instance.setRowCacheCapacityInMB(1);

        ByteBuffer key = ByteBufferUtil.bytes("rowcachekey");
        DecoratedKey dk = cachedStore.decorateKey(key);
        RowCacheKey rck = new RowCacheKey(cachedStore.metadata.cfId, dk);
        String values[] = new String[200];
        for (int i = 0; i < 200; i++)
        {
            RowUpdateBuilder rub = new RowUpdateBuilder(cachedStore.metadata, System.currentTimeMillis(), key);
            rub.clustering(String.valueOf(i));
            values[i] = "val" + i;
            rub.add("val", ByteBufferUtil.bytes(values[i]));
            rub.build().applyUnsafe();
        }
        Arrays.sort(values);

        // populate row cache, we should not get a row cache hit;
        Util.getAll(Util.cmd(cachedStore, dk).withLimit(10).build());
        assertEquals(startRowCacheHits, cachedStore.metric.rowCacheHit.getCount());

        // do another query, limit is 20, which is < 100 that we cache, we should get a hit and it should be in range
        Util.getAll(Util.cmd(cachedStore, dk).withLimit(10).build());
        assertEquals(++startRowCacheHits, cachedStore.metric.rowCacheHit.getCount());
        assertEquals(startRowCacheOutOfRange, cachedStore.metric.rowCacheHitOutOfRange.getCount());

        // get a slice from 95 to 105, 95->99 are in cache, we should not get a hit and then row cache is out of range
        Util.getAll(Util.cmd(cachedStore, dk).fromIncl(String.valueOf(210)).toExcl(String.valueOf(215)).build());
        assertEquals(startRowCacheHits, cachedStore.metric.rowCacheHit.getCount());
        assertEquals(++startRowCacheOutOfRange, cachedStore.metric.rowCacheHitOutOfRange.getCount());

        // get a slice with limit > 100, we should get a hit out of range.
        Util.getAll(Util.cmd(cachedStore, dk).withLimit(101).build());
        assertEquals(startRowCacheHits, cachedStore.metric.rowCacheHit.getCount());
        assertEquals(++startRowCacheOutOfRange, cachedStore.metric.rowCacheHitOutOfRange.getCount());


        CacheService.instance.invalidateRowCache();

        // try to populate row cache with a limit > rows to cache, we should still populate row cache;
        Util.getAll(Util.cmd(cachedStore, dk).withLimit(105).build());
        assertEquals(startRowCacheHits, cachedStore.metric.rowCacheHit.getCount());

        // validate the stuff in cache;
        CachedPartition cachedCf = (CachedPartition)CacheService.instance.rowCache.get(rck);
        assertEquals(cachedCf.rowCount(), 100);
        int i = 0;

        for (Unfiltered unfiltered : Util.once(cachedCf.unfilteredIterator(ColumnFilter.selection(cachedCf.columns()), Slices.ALL, false)))
        {
            Row r = (Row) unfiltered;

            assertEquals(r.clustering().get(0), ByteBufferUtil.bytes(values[i].substring(3)));

            for (ColumnData c : r)
            {
                assertEquals(((Cell)c).value(), ByteBufferUtil.bytes(values[i]));
            }
            i++;
        }

        cachedStore.truncateBlocking();
    }

    public void rowCacheLoad(int totalKeys, int keysToSave, int offset) throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        ColumnFamilyStore store = Keyspace.open(KEYSPACE_CACHED).getColumnFamilyStore(CF_CACHED);

        // empty the cache
        CacheService.instance.invalidateRowCache();
        assertEquals(0, CacheService.instance.rowCache.size());

        // insert data and fill the cache
        SchemaLoader.insertData(KEYSPACE_CACHED, CF_CACHED, offset, totalKeys);
        readData(KEYSPACE_CACHED, CF_CACHED, offset, totalKeys);
        assertEquals(totalKeys, CacheService.instance.rowCache.size());

        // force the cache to disk
        CacheService.instance.rowCache.submitWrite(keysToSave).get();

        // empty the cache again to make sure values came from disk
        CacheService.instance.invalidateRowCache();
        assertEquals(0, CacheService.instance.rowCache.size());
        assertEquals(keysToSave == Integer.MAX_VALUE ? totalKeys : keysToSave, CacheService.instance.rowCache.loadSaved(store));
    }

    private static void readData(String keyspace, String columnFamily, int offset, int numberOfRows)
    {
        ColumnFamilyStore store = Keyspace.open(keyspace).getColumnFamilyStore(columnFamily);
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace, columnFamily);

        for (int i = offset; i < offset + numberOfRows; i++)
        {
            DecoratedKey key = Util.dk("key" + i);
            Clustering cl = new Clustering(ByteBufferUtil.bytes("col" + i));
            Util.getAll(Util.cmd(store, key).build());
        }
    }
}
