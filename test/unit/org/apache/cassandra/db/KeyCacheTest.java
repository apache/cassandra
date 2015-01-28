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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.apache.cassandra.utils.concurrent.Refs;
import static org.junit.Assert.assertEquals;

public class KeyCacheTest
{
    private static final String KEYSPACE1 = "KeyCacheTest1";
    private static final String COLUMN_FAMILY1 = "Standard1";
    private static final String COLUMN_FAMILY2 = "Standard2";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY2));
    }

    @AfterClass
    public static void cleanup()
    {
        SchemaLoader.cleanupSavedCaches();
    }

    @Test
    public void testKeyCacheLoad() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        ColumnFamilyStore store = Keyspace.open(KEYSPACE1).getColumnFamilyStore(COLUMN_FAMILY2);

        // empty the cache
        CacheService.instance.invalidateKeyCache();
        assertKeyCacheSize(0, KEYSPACE1, COLUMN_FAMILY2);

        // insert data and force to disk
        SchemaLoader.insertData(KEYSPACE1, COLUMN_FAMILY2, 0, 100);
        store.forceBlockingFlush();

        // populate the cache
        SchemaLoader.readData(KEYSPACE1, COLUMN_FAMILY2, 0, 100);
        assertKeyCacheSize(100, KEYSPACE1, COLUMN_FAMILY2);

        // really? our caches don't implement the map interface? (hence no .addAll)
        Map<KeyCacheKey, RowIndexEntry> savedMap = new HashMap<KeyCacheKey, RowIndexEntry>();
        for (Iterator<KeyCacheKey> iter = CacheService.instance.keyCache.keyIterator();
             iter.hasNext();)
        {
            KeyCacheKey k = iter.next();
            if (k.desc.ksname.equals(KEYSPACE1) && k.desc.cfname.equals(COLUMN_FAMILY2))
                savedMap.put(k, CacheService.instance.keyCache.get(k));
        }

        // force the cache to disk
        CacheService.instance.keyCache.submitWrite(Integer.MAX_VALUE).get();

        CacheService.instance.invalidateKeyCache();
        assertKeyCacheSize(0, KEYSPACE1, COLUMN_FAMILY2);

        CacheService.instance.keyCache.loadSaved(store);
        assertKeyCacheSize(savedMap.size(), KEYSPACE1, COLUMN_FAMILY2);

        // probably it's better to add equals/hashCode to RowIndexEntry...
        for (Map.Entry<KeyCacheKey, RowIndexEntry> entry : savedMap.entrySet())
        {
            RowIndexEntry expected = entry.getValue();
            RowIndexEntry actual = CacheService.instance.keyCache.get(entry.getKey());
            assertEquals(expected.position, actual.position);
            assertEquals(expected.columnsIndex(), actual.columnsIndex());
            if (expected.isIndexed())
            {
                assertEquals(expected.deletionTime(), actual.deletionTime());
            }
        }
    }

    @Test
    public void testKeyCache() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COLUMN_FAMILY1);

        // just to make sure that everything is clean
        CacheService.instance.invalidateKeyCache();

        // KeyCache should start at size 0 if we're caching X% of zero data.
        assertKeyCacheSize(0, KEYSPACE1, COLUMN_FAMILY1);

        DecoratedKey key1 = Util.dk("key1");
        DecoratedKey key2 = Util.dk("key2");
        Mutation rm;

        // inserts
        rm = new Mutation(KEYSPACE1, key1.getKey());
        rm.add(COLUMN_FAMILY1, Util.cellname("1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.applyUnsafe();
        rm = new Mutation(KEYSPACE1, key2.getKey());
        rm.add(COLUMN_FAMILY1, Util.cellname("2"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.applyUnsafe();

        // to make sure we have SSTable
        cfs.forceBlockingFlush();

        // reads to cache key position
        cfs.getColumnFamily(QueryFilter.getSliceFilter(key1,
                                                       COLUMN_FAMILY1,
                                                       Composites.EMPTY,
                                                       Composites.EMPTY,
                                                       false,
                                                       10,
                                                       System.currentTimeMillis()));

        cfs.getColumnFamily(QueryFilter.getSliceFilter(key2,
                                                       COLUMN_FAMILY1,
                                                       Composites.EMPTY,
                                                       Composites.EMPTY,
                                                       false,
                                                       10,
                                                       System.currentTimeMillis()));

        assertKeyCacheSize(2, KEYSPACE1, COLUMN_FAMILY1);

        Set<SSTableReader> readers = cfs.getDataTracker().getSSTables();
        Refs<SSTableReader> refs = Refs.tryRef(readers);
        if (refs == null)
            throw new IllegalStateException();

        Util.compactAll(cfs, Integer.MAX_VALUE).get();
        // after compaction cache should have entries for new SSTables,
        // but since we have kept a reference to the old sstables,
        // if we had 2 keys in cache previously it should become 4
        assertKeyCacheSize(4, KEYSPACE1, COLUMN_FAMILY1);

        refs.release();

        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);;
        while (ScheduledExecutors.nonPeriodicTasks.getActiveCount() + ScheduledExecutors.nonPeriodicTasks.getQueue().size() > 0);

        // after releasing the reference this should drop to 2
        assertKeyCacheSize(2, KEYSPACE1, COLUMN_FAMILY1);

        // re-read same keys to verify that key cache didn't grow further
        cfs.getColumnFamily(QueryFilter.getSliceFilter(key1,
                                                       COLUMN_FAMILY1,
                                                       Composites.EMPTY,
                                                       Composites.EMPTY,
                                                       false,
                                                       10,
                                                       System.currentTimeMillis()));

        cfs.getColumnFamily(QueryFilter.getSliceFilter(key2,
                                                       COLUMN_FAMILY1,
                                                       Composites.EMPTY,
                                                       Composites.EMPTY,
                                                       false,
                                                       10,
                                                       System.currentTimeMillis()));

        assertKeyCacheSize(2, KEYSPACE1, COLUMN_FAMILY1);
    }

    private void assertKeyCacheSize(int expected, String keyspace, String columnFamily)
    {
        int size = 0;
        for (Iterator<KeyCacheKey> iter = CacheService.instance.keyCache.keyIterator();
             iter.hasNext();)
        {
            KeyCacheKey k = iter.next();
            if (k.desc.ksname.equals(keyspace) && k.desc.cfname.equals(columnFamily))
                size++;
        }
        assertEquals(expected, size);
    }
}
