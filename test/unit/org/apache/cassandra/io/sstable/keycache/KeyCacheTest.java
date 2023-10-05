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
package org.apache.cassandra.io.sstable.keycache;

import java.io.IOException;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.cache.ICache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Refs;
import org.assertj.core.api.Assertions;
import org.hamcrest.Matchers;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.AnswersWithDelay;
import org.mockito.invocation.InvocationOnMock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class KeyCacheTest
{
    private static boolean sstableImplCachesKeys;

    private static final String KEYSPACE1 = "KeyCacheTest1";
    private static final String KEYSPACE2 = "KeyCacheTest2";
    private static final String COLUMN_FAMILY1 = "Standard1";
    private static final String COLUMN_FAMILY2 = "Standard2";
    private static final String COLUMN_FAMILY3 = "Standard3";
    private static final String COLUMN_FAMILY4 = "Standard4";
    private static final String COLUMN_FAMILY5 = "Standard5";
    private static final String COLUMN_FAMILY6 = "Standard6";
    private static final String COLUMN_FAMILY7 = "Standard7";
    private static final String COLUMN_FAMILY8 = "Standard8";
    private static final String COLUMN_FAMILY9 = "Standard9";
    private static final String COLUMN_FAMILY10 = "Standard10";

    private static final String COLUMN_FAMILY_K2_1 = "Standard1";


    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
        sstableImplCachesKeys = KeyCacheSupport.isSupportedBy(DatabaseDescriptor.getSelectedSSTableFormat());
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY2),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY3),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY4),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY5),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY6),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY7),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY8),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY9),
                                    SchemaLoader.standardCFMD(KEYSPACE1, COLUMN_FAMILY10));

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
    public void testKeyCacheLoadShallowIndexEntry() throws Exception
    {
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        testKeyCacheLoad(COLUMN_FAMILY2);
    }

    @Test
    public void testKeyCacheLoadIndexInfoOnHeap() throws Exception
    {
        DatabaseDescriptor.setColumnIndexCacheSize(8);
        testKeyCacheLoad(COLUMN_FAMILY5);
    }

    private void testKeyCacheLoad(String cf) throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        ColumnFamilyStore store = Keyspace.open(KEYSPACE1).getColumnFamilyStore(cf);

        // empty the cache
        CacheService.instance.invalidateKeyCache();
        assertKeyCacheSize(0, KEYSPACE1, cf);

        // insert data and force to disk
        SchemaLoader.insertData(KEYSPACE1, cf, 0, 100);
        Util.flush(store);

        // populate the cache
        readData(KEYSPACE1, cf, 0, 100);
        assertKeyCacheSize(100, KEYSPACE1, cf);

        // really? our caches don't implement the map interface? (hence no .addAll)
        Map<KeyCacheKey, AbstractRowIndexEntry> savedMap = new HashMap<>();
        Map<KeyCacheKey, RowIndexEntry.IndexInfoRetriever> savedInfoMap = new HashMap<>();
        for (Iterator<KeyCacheKey> iter = CacheService.instance.keyCache.keyIterator();
             iter.hasNext();)
        {
            KeyCacheKey k = iter.next();
            if (k.desc.ksname.equals(KEYSPACE1) && k.desc.cfname.equals(cf))
            {
                AbstractRowIndexEntry rie = CacheService.instance.keyCache.get(k);
                savedMap.put(k, rie);
                if (rie instanceof RowIndexEntry)
                {
                    BigTableReader sstr = (BigTableReader) readerForKey(k);
                    savedInfoMap.put(k, ((RowIndexEntry) rie).openWithIndex(sstr.getIndexFile()));
                }
            }
        }

        // force the cache to disk
        CacheService.instance.keyCache.submitWrite(Integer.MAX_VALUE).get();

        CacheService.instance.invalidateKeyCache();
        assertKeyCacheSize(0, KEYSPACE1, cf);

        CacheService.instance.keyCache.loadSaved();
        assertKeyCacheSize(savedMap.size(), KEYSPACE1, cf);

        // probably it's better to add equals/hashCode to RowIndexEntry...
        for (Map.Entry<KeyCacheKey, AbstractRowIndexEntry> entry : savedMap.entrySet())
        {
            AbstractRowIndexEntry expected = entry.getValue();
            AbstractRowIndexEntry actual = CacheService.instance.keyCache.get(entry.getKey());
            assertEquals(expected.position, actual.position);
            assertEquals(expected.blockCount(), actual.blockCount());
            assertEquals(expected.getSSTableFormat(), actual.getSSTableFormat());
            for (int i = 0; i < expected.blockCount(); i++)
            {
                SSTableReader actualSstr = readerForKey(entry.getKey());
                Assertions.assertThat(actualSstr.descriptor.version.format).isEqualTo(expected.getSSTableFormat());
                if (actual instanceof RowIndexEntry)
                {
                    try (RowIndexEntry.IndexInfoRetriever actualIir = ((RowIndexEntry) actual).openWithIndex(((BigTableReader) actualSstr).getIndexFile()))
                    {
                        RowIndexEntry.IndexInfoRetriever expectedIir = savedInfoMap.get(entry.getKey());
                        assertEquals(expectedIir.columnsIndex(i), actualIir.columnsIndex(i));
                    }
                }
            }
            if (expected.isIndexed())
            {
                assertEquals(expected.deletionTime(), actual.deletionTime());
            }
        }

        savedInfoMap.values().forEach(iir -> {
            try
            {
                if (iir != null)
                    iir.close();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    private static SSTableReader readerForKey(KeyCacheKey k)
    {
        return ColumnFamilyStore.getIfExists(k.desc.ksname, k.desc.cfname).getLiveSSTables()
                                .stream()
                                .filter(sstreader -> sstreader.descriptor.id == k.desc.id)
                                .findFirst().get();
    }

    @Test
    public void testKeyCacheLoadWithLostTableShallowIndexEntry() throws Exception
    {
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        testKeyCacheLoadWithLostTable(COLUMN_FAMILY3);
    }

    @Test
    public void testKeyCacheLoadWithLostTableIndexInfoOnHeap() throws Exception
    {
        DatabaseDescriptor.setColumnIndexCacheSize(8);
        testKeyCacheLoadWithLostTable(COLUMN_FAMILY6);
    }

    private void testKeyCacheLoadWithLostTable(String cf) throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        ColumnFamilyStore store = Keyspace.open(KEYSPACE1).getColumnFamilyStore(cf);

        // empty the cache
        CacheService.instance.invalidateKeyCache();
        assertKeyCacheSize(0, KEYSPACE1, cf);

        // insert data and force to disk
        SchemaLoader.insertData(KEYSPACE1, cf, 0, 100);
        Util.flush(store);

        Collection<SSTableReader> firstFlushTables = ImmutableList.copyOf(store.getLiveSSTables());

        // populate the cache
        readData(KEYSPACE1, cf, 0, 100);
        assertKeyCacheSize(100, KEYSPACE1, cf);

        // insert some new data and force to disk
        SchemaLoader.insertData(KEYSPACE1, cf, 100, 50);
        Util.flush(store);

        // check that it's fine
        readData(KEYSPACE1, cf, 100, 50);
        assertKeyCacheSize(150, KEYSPACE1, cf);

        // force the cache to disk
        CacheService.instance.keyCache.submitWrite(Integer.MAX_VALUE).get();

        CacheService.instance.invalidateKeyCache();
        assertKeyCacheSize(0, KEYSPACE1, cf);

        // check that the content is written correctly
        CacheService.instance.keyCache.loadSaved();
        assertKeyCacheSize(150, KEYSPACE1, cf);

        CacheService.instance.invalidateKeyCache();
        assertKeyCacheSize(0, KEYSPACE1, cf);

        // now remove the first sstable from the store to simulate losing the file
        store.markObsolete(firstFlushTables, OperationType.UNKNOWN);

        // check that reading now correctly skips over lost table and reads the rest (CASSANDRA-10219)
        CacheService.instance.keyCache.loadSaved();
        assertKeyCacheSize(50, KEYSPACE1, cf);
    }

    @Test
    public void testKeyCacheShallowIndexEntry() throws ExecutionException, InterruptedException
    {
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        testKeyCache(COLUMN_FAMILY1);
    }

    @Test
    public void testKeyCacheIndexInfoOnHeap() throws ExecutionException, InterruptedException
    {
        DatabaseDescriptor.setColumnIndexCacheSize(8);
        testKeyCache(COLUMN_FAMILY4);
    }

    private void testKeyCache(String cf) throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cf);

        // just to make sure that everything is clean
        CacheService.instance.invalidateKeyCache();

        // KeyCache should start at size 0 if we're caching X% of zero data.
        assertKeyCacheSize(0, KEYSPACE1, cf);

        Mutation rm;

        // inserts
        new RowUpdateBuilder(cfs.metadata(), 0, "key1").clustering("1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, "key2").clustering("2").build().applyUnsafe();

        // to make sure we have SSTable
        Util.flush(cfs);

        // reads to cache key position
        Util.getAll(Util.cmd(cfs, "key1").build());
        Util.getAll(Util.cmd(cfs, "key2").build());

        assertKeyCacheSize(2, KEYSPACE1, cf);

        Set<SSTableReader> readers = cfs.getLiveSSTables();
        Refs<SSTableReader> refs = Refs.tryRef(readers);
        if (refs == null)
            throw new IllegalStateException();

        Util.compactAll(cfs, Integer.MAX_VALUE).get();

        // after compaction cache should have entries for new SSTables,
        // but since we have kept a reference to the old sstables,
        // if we had 2 keys in cache previously it should become 4
        assertKeyCacheSize(4, KEYSPACE1, cf);

        refs.release();

        LifecycleTransaction.waitForDeletions();

        // after releasing the reference this should drop to 2
        assertKeyCacheSize(2, KEYSPACE1, cf);

        // re-read same keys to verify that key cache didn't grow further
        Util.getAll(Util.cmd(cfs, "key1").build());
        Util.getAll(Util.cmd(cfs, "key2").build());

        assertKeyCacheSize( 2, KEYSPACE1, cf);
    }

    @Test
    public void testKeyCacheLoadZeroCacheLoadTime() throws Exception
    {
        DatabaseDescriptor.setCacheLoadTimeout(0);
        String cf = COLUMN_FAMILY7;

        createAndInvalidateCache(Collections.singletonList(Pair.create(KEYSPACE1, cf)), 100);

        CacheService.instance.keyCache.loadSaved();

        // Here max time to load cache is zero which means no time left to load cache. So the keyCache size should
        // be zero after loadSaved().
        assertKeyCacheSize(0, KEYSPACE1, cf);
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

        doAnswer(new AnswersWithDelay(delayMillis, InvocationOnMock::callRealMethod)).when(keyCacheSerializerSpy)
                                                                                     .deserialize(any(DataInputPlus.class));

        long maxExpectedKeyCache = Math.min(numberOfRows,
                                            1 + TimeUnit.SECONDS.toMillis(DatabaseDescriptor.getCacheLoadTimeout()) / delayMillis);

        long keysLoaded = autoSavingCache.loadSaved();
        assertThat(keysLoaded, Matchers.lessThanOrEqualTo(maxExpectedKeyCache));
        if (sstableImplCachesKeys)
        {
            assertNotEquals(0, keysLoaded);
            Mockito.verify(keyCacheSerializerSpy, Mockito.times(1)).cleanupAfterDeserialize();
        }
        else
        {
            assertEquals(0, keysLoaded);
            Mockito.verify(keyCacheSerializerSpy, Mockito.never()).cleanupAfterDeserialize();
        }
    }

    private void createAndInvalidateCache(List<Pair<String, String>> tables, int numberOfRows) throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        // empty the cache
        CacheService.instance.invalidateKeyCache();
        assertEquals(0, CacheService.instance.keyCache.size());

        for(Pair<String, String> entry : tables)
        {
            String keyspace = entry.left();
            String cf = entry.right();
            ColumnFamilyStore store = Keyspace.open(keyspace).getColumnFamilyStore(cf);

            // insert data and force to disk
            SchemaLoader.insertData(keyspace, cf, 0, numberOfRows);
            Util.flush(store);
        }
        for(Pair<String, String> entry : tables)
        {
            String keyspace = entry.left();
            String cf = entry.right();
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
        assertEquals(sstableImplCachesKeys ? expected : 0, size);
    }
}
