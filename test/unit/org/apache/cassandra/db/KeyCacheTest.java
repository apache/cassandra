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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.junit.Assert.assertEquals;

public class KeyCacheTest
{
    private static final String KEYSPACE1 = "KeyCacheTest1";
    private static final String COLUMN_FAMILY1 = "Standard1";
    private static final String COLUMN_FAMILY2 = "Standard2";
    private static final String COLUMN_FAMILY3 = "Standard3";


    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY2),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY3));
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
        readData(KEYSPACE1, COLUMN_FAMILY2, 0, 100);
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

        CacheService.instance.keyCache.loadSaved();
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
    public void testKeyCacheLoadWithLostTable() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        ColumnFamilyStore store = Keyspace.open(KEYSPACE1).getColumnFamilyStore(COLUMN_FAMILY3);

        // empty the cache
        CacheService.instance.invalidateKeyCache();
        assertKeyCacheSize(0, KEYSPACE1, COLUMN_FAMILY3);

        // insert data and force to disk
        SchemaLoader.insertData(KEYSPACE1, COLUMN_FAMILY3, 0, 100);
        store.forceBlockingFlush();

        Collection<SSTableReader> firstFlushTables = ImmutableList.copyOf(store.getLiveSSTables());

        // populate the cache
        readData(KEYSPACE1, COLUMN_FAMILY3, 0, 100);
        assertKeyCacheSize(100, KEYSPACE1, COLUMN_FAMILY3);

        // insert some new data and force to disk
        SchemaLoader.insertData(KEYSPACE1, COLUMN_FAMILY3, 100, 50);
        store.forceBlockingFlush();

        // check that it's fine
        readData(KEYSPACE1, COLUMN_FAMILY3, 100, 50);
        assertKeyCacheSize(150, KEYSPACE1, COLUMN_FAMILY3);

        // force the cache to disk
        CacheService.instance.keyCache.submitWrite(Integer.MAX_VALUE).get();

        CacheService.instance.invalidateKeyCache();
        assertKeyCacheSize(0, KEYSPACE1, COLUMN_FAMILY3);

        // check that the content is written correctly
        CacheService.instance.keyCache.loadSaved();
        assertKeyCacheSize(150, KEYSPACE1, COLUMN_FAMILY3);

        CacheService.instance.invalidateKeyCache();
        assertKeyCacheSize(0, KEYSPACE1, COLUMN_FAMILY3);

        // now remove the first sstable from the store to simulate losing the file
        store.markObsolete(firstFlushTables, OperationType.UNKNOWN);

        // check that reading now correctly skips over lost table and reads the rest (CASSANDRA-10219)
        CacheService.instance.keyCache.loadSaved();
        assertKeyCacheSize(50, KEYSPACE1, COLUMN_FAMILY3);
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

        Mutation rm;

        // inserts
        new RowUpdateBuilder(cfs.metadata, 0, "key1").clustering("1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "key2").clustering("2").build().applyUnsafe();

        // to make sure we have SSTable
        cfs.forceBlockingFlush();

        // reads to cache key position
        Util.getAll(Util.cmd(cfs, "key1").build());
        Util.getAll(Util.cmd(cfs, "key2").build());

        assertKeyCacheSize(2, KEYSPACE1, COLUMN_FAMILY1);

        Set<SSTableReader> readers = cfs.getLiveSSTables();
        Refs<SSTableReader> refs = Refs.tryRef(readers);
        if (refs == null)
            throw new IllegalStateException();

        Util.compactAll(cfs, Integer.MAX_VALUE).get();
        boolean noEarlyOpen = DatabaseDescriptor.getSSTablePreempiveOpenIntervalInMB() < 0;

        // after compaction cache should have entries for new SSTables,
        // but since we have kept a reference to the old sstables,
        // if we had 2 keys in cache previously it should become 4
        assertKeyCacheSize(noEarlyOpen ? 2 : 4, KEYSPACE1, COLUMN_FAMILY1);

        refs.release();

        LifecycleTransaction.waitForDeletions();

        // after releasing the reference this should drop to 2
        assertKeyCacheSize(2, KEYSPACE1, COLUMN_FAMILY1);

        // re-read same keys to verify that key cache didn't grow further
        Util.getAll(Util.cmd(cfs, "key1").build());
        Util.getAll(Util.cmd(cfs, "key2").build());

        assertKeyCacheSize(noEarlyOpen ? 4 : 2, KEYSPACE1, COLUMN_FAMILY1);
    }

    private static void readData(String keyspace, String columnFamily, int startRow, int numberOfRows)
    {
        ColumnFamilyStore store = Keyspace.open(keyspace).getColumnFamilyStore(columnFamily);
        for (int i = 0; i < numberOfRows; i++)
            Util.getAll(Util.cmd(store, "key" + (i + startRow)).includeRow("col" + (i + startRow)).build());
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
