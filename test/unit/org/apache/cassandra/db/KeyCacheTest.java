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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.cache.ICache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Refs;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.AnswersWithDelay;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class KeyCacheTest
{
    private static final String KEYSPACE1 = "KeyCacheTest1";
    private static final String KEYSPACE2 = "KeyCacheTest2";
    private static final String COLUMN_FAMILY1 = "Standard1";
    private static final String COLUMN_FAMILY2 = "Standard2";
    private static final String COLUMN_FAMILY3 = "Standard3";
    private static final String COLUMN_FAMILY7 = "Standard7";
    private static final String COLUMN_FAMILY8 = "Standard8";
    private static final String COLUMN_FAMILY9 = "Standard9";

    private static final String COLUMN_FAMILY_K2_1 = "Standard1";


    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY2),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY3),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY7),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY8),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY9));

        SchemaLoader.createKeyspace(KEYSPACE2,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE2, COLUMN_FAMILY_K2_1));

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

    @Test
    public void testKeyCacheLoadNegativeCacheLoadTime() throws Exception
    {
        DatabaseDescriptor.setCacheLoadTimeout(-1);
        String cf = COLUMN_FAMILY7;

        createAndInvalidateCache(Collections.singletonList(Pair.create(KEYSPACE1, cf)), 100);

        CacheService.instance.keyCache.loadSaved();

        // Here max time to load cache is negative which means no time left to load cache. So the keyCache size should
        // be zero after loadSaved().
        assertKeyCacheSize(0, KEYSPACE1, cf);
        assertEquals(0, CacheService.instance.keyCache.size());
    }

    @Test
    public void testKeyCacheLoadTwoTablesTime() throws Exception
    {
        DatabaseDescriptor.setCacheLoadTimeout(60);
        String columnFamily1 = COLUMN_FAMILY8;
        String columnFamily2 = COLUMN_FAMILY_K2_1;
        int numberOfRows = 100;
        List<Pair<String, String>> tables = new ArrayList<>(2);
        tables.add(Pair.create(KEYSPACE1, columnFamily1));
        tables.add(Pair.create(KEYSPACE2, columnFamily2));

        createAndInvalidateCache(tables, numberOfRows);

        CacheService.instance.keyCache.loadSaved();

        // Here max time to load cache is negative which means no time left to load cache. So the keyCache size should
        // be zero after load.
        assertKeyCacheSize(numberOfRows, KEYSPACE1, columnFamily1);
        assertKeyCacheSize(numberOfRows, KEYSPACE2, columnFamily2);
        assertEquals(numberOfRows * tables.size(), CacheService.instance.keyCache.size());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testKeyCacheLoadCacheLoadTimeExceedingLimit() throws Exception
    {
        DatabaseDescriptor.setCacheLoadTimeout(2);
        int delayMillis = 1000;
        int numberOfRows = 100;

        String cf = COLUMN_FAMILY9;

        createAndInvalidateCache(Collections.singletonList(Pair.create(KEYSPACE1, cf)), numberOfRows);

        // Testing cache load. Here using custom built AutoSavingCache instance as simulating delay is not possible with
        // 'CacheService.instance.keyCache'. 'AutoSavingCache.loadSaved()' is returning no.of entries loaded so we don't need
        // to instantiate ICache.class.
        CacheService.KeyCacheSerializer keyCacheSerializer = new CacheService.KeyCacheSerializer();
        CacheService.KeyCacheSerializer keyCacheSerializerSpy = Mockito.spy(keyCacheSerializer);
        AutoSavingCache autoSavingCache = new AutoSavingCache(mock(ICache.class),
                                                              CacheService.CacheType.KEY_CACHE,
                                                              keyCacheSerializerSpy);

        doAnswer(new AnswersWithDelay(delayMillis, answer -> keyCacheSerializer.deserialize(answer.getArgument(0),
                                                                                            answer.getArgument(1)) ))
               .when(keyCacheSerializerSpy).deserialize(any(DataInputPlus.class), any(ColumnFamilyStore.class));

        long maxExpectedKeyCache = Math.min(numberOfRows,
                                            1 + TimeUnit.SECONDS.toMillis(DatabaseDescriptor.getCacheLoadTimeout()) / delayMillis);

        long keysLoaded = autoSavingCache.loadSaved();
        assertTrue(keysLoaded < maxExpectedKeyCache);
        assertTrue(0 != keysLoaded);
        Mockito.verify(keyCacheSerializerSpy, Mockito.times(1)).cleanupAfterDeserialize();
    }

    private void createAndInvalidateCache(List<Pair<String, String>> tables, int numberOfRows) throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        // empty the cache
        CacheService.instance.invalidateKeyCache();
        assertEquals(0, CacheService.instance.keyCache.size());

        for(Pair<String, String> entry : tables)
        {
            String keyspace = entry.left;
            String cf = entry.right;
            ColumnFamilyStore store = Keyspace.open(keyspace).getColumnFamilyStore(cf);

            // insert data and force to disk
            SchemaLoader.insertData(keyspace, cf, 0, numberOfRows);
            store.forceBlockingFlush();
        }
        for(Pair<String, String> entry : tables)
        {
            String keyspace = entry.left;
            String cf = entry.right;
            // populate the cache
            readData(keyspace, cf, 0, numberOfRows);
            assertKeyCacheSize(numberOfRows, keyspace, cf);
        }

        // force the cache to disk
        CacheService.instance.keyCache.submitWrite(CacheService.instance.keyCache.size()).get();

        CacheService.instance.invalidateKeyCache();
        assertEquals(0, CacheService.instance.keyCache.size());
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
