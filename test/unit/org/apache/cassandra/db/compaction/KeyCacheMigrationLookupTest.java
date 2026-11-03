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

package org.apache.cassandra.db.compaction;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.keycache.KeyCacheSupport;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Key cache migration during compaction looks a key up only in the originals whose key cache can hold it.
 * With caching keys NONE no lookup is made, on either path. With a key cache of capacity 0 the cursor path
 * makes no lookup, since every lookup would miss. The cursor path builds the key cache keys it looks up
 * with once per output sstable, not once per partition. BIG format only.
 * <p>
 * Each compaction runs on the test thread, and only that thread is counted, so background compactions of
 * other tables do not count.
 */
@RunWith(BMUnitRunner.class)
@BMRules(rules = {
    @BMRule(name = "count key cache lookups",
            targetClass = "org.apache.cassandra.io.sstable.keycache.KeyCache",
            targetMethod = "get",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.compaction.KeyCacheMigrationLookupTest.isCounting()",
            action = "org.apache.cassandra.db.compaction.KeyCacheMigrationLookupTest.lookups.incrementAndGet()"),
    @BMRule(name = "count key cache keys built",
            targetClass = "org.apache.cassandra.cache.KeyCacheKey",
            targetMethod = "<init>",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.compaction.KeyCacheMigrationLookupTest.isCounting()",
            action = "org.apache.cassandra.db.compaction.KeyCacheMigrationLookupTest.keysBuilt.incrementAndGet()")
})
public class KeyCacheMigrationLookupTest extends CQLTester
{
    public static final AtomicLong lookups = new AtomicLong();
    public static final AtomicLong keysBuilt = new AtomicLong();
    private static volatile Thread countingThread;

    private static final int PARTITIONS = 100;
    /** Every Nth partition is read before the compaction, so its key is in the originals' key cache. */
    private static final int HOT_STRIDE = 3;

    private SSTableFormat<?, ?> originalFormat;
    private boolean originalMigrate;
    private boolean originalCursor;
    private long originalKeyCacheCapacity;

    public static boolean isCounting()
    {
        return Thread.currentThread() == countingThread;
    }

    @Before
    public void selectBigAndEnableMigration()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("big");
        originalMigrate = DatabaseDescriptor.shouldMigrateKeycacheOnCompaction();
        DatabaseDescriptor.setMigrateKeycacheOnCompaction(true);
        originalCursor = DatabaseDescriptor.cursorCompactionEnabled();
        originalKeyCacheCapacity = CacheService.instance.keyCache.getCapacity();
        if (originalKeyCacheCapacity == 0)
            CacheService.instance.keyCache.setCapacity(1L << 20);
        CacheService.instance.invalidateKeyCache();
    }

    @After
    public void restore()
    {
        countingThread = null;
        DatabaseDescriptor.setMigrateKeycacheOnCompaction(originalMigrate);
        DatabaseDescriptor.setCursorCompactionEnabled(originalCursor);
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        CacheService.instance.keyCache.setCapacity(originalKeyCacheCapacity);
        CacheService.instance.invalidateKeyCache();
    }

    @Test
    public void keysNoneMakesNoLookupOnCursorPath() throws Throwable
    {
        assertEquals("a compaction of a table with caching keys NONE looked up the key cache",
                     0, compact("NONE", PARTITIONS, true).lookups);
    }

    @Test
    public void keysNoneMakesNoLookupOnIteratorPath() throws Throwable
    {
        assertEquals("a compaction of a table with caching keys NONE looked up the key cache",
                     0, compact("NONE", PARTITIONS, false).lookups);
    }

    /** The same compaction with keys ALL does look up, or the zero results above would prove nothing. */
    @Test
    public void keysAllLooksUpOnCursorPath() throws Throwable
    {
        assertTrue(compact("ALL", PARTITIONS, true).lookups > 0);
    }

    @Test
    public void keysAllLooksUpOnIteratorPath() throws Throwable
    {
        assertTrue(compact("ALL", PARTITIONS, false).lookups > 0);
    }

    /**
     * With a key cache of capacity 0 (key_cache_size: 0MiB) and keys ALL, the cursor path makes no lookup
     * and builds no key cache key. Every lookup would miss, so the cached keys are the same as on the
     * iterator path: none.
     */
    @Test
    public void zeroCapacityMakesNoLookupOnCursorPath() throws Throwable
    {
        CacheService.instance.keyCache.setCapacity(0);
        Compaction cursor = compact("ALL", PARTITIONS, true);
        assertEquals("a compaction with a key cache of capacity 0 looked up the key cache", 0, cursor.lookups);
        assertEquals("a compaction with a key cache of capacity 0 built key cache keys", 0, cursor.keysBuilt);
        assertTrue("the cursor path cached a key in a key cache of capacity 0", cursor.cachedKeys.isEmpty());

        Compaction iterator = compact("ALL", PARTITIONS, false);
        assertTrue("the iterator path cached a key in a key cache of capacity 0", iterator.cachedKeys.isEmpty());
    }

    /**
     * The cursor path builds one key cache key per original to look up with, for the whole compaction. A new key per original
     * per partition would make the count grow with the partition count.
     */
    @Test
    public void cursorPathBuildsNoKeyCacheKeyPerPartition() throws Throwable
    {
        Compaction small = compact("ALL", PARTITIONS, true);
        Compaction large = compact("ALL", 10 * PARTITIONS, true);
        assertTrue("the compaction looked nothing up, so this proves nothing", small.lookups > 0 && large.lookups > 0);
        // the stored keys of the hot partitions are built when the output caches them
        long smallPerOriginalOrHot = small.keysBuilt - small.cachedKeys.size();
        long largePerOriginalOrHot = large.keysBuilt - large.cachedKeys.size();
        assertEquals(String.format("the cursor path built %d key cache keys for %d partitions and %d for %d partitions, " +
                                   "beyond one for each key it cached",
                                   small.keysBuilt, PARTITIONS, large.keysBuilt, 10 * PARTITIONS),
                     smallPerOriginalOrHot, largePerOriginalOrHot);
        assertTrue("the cursor path built more than one key cache key per original to look up with: " + smallPerOriginalOrHot,
                   smallPerOriginalOrHot <= small.originals);
    }

    /** The output caches the same keys on both paths, when only some keys are hot in the originals. */
    @Test
    public void cachedKeysMatchIteratorPath() throws Throwable
    {
        Compaction iterator = compact("ALL", PARTITIONS, false);
        Compaction cursor = compact("ALL", PARTITIONS, true);
        assertFalse("the iterator path cached no key, so this proves nothing", iterator.cachedKeys.isEmpty());
        assertEquals("the cursor path cached different keys than the iterator path", iterator.cachedKeys, cursor.cachedKeys);
    }

    private static final class Compaction
    {
        long lookups;
        long keysBuilt;
        int originals;
        /** The partition keys, as longs, that the output has a cached position for. */
        final Set<Long> cachedKeys = new TreeSet<>();
    }

    /**
     * Writes {@code partitions} partitions into two sstables, reads every {@link #HOT_STRIDE}th partition
     * so its key is cached, and compacts the two on this thread.
     */
    private Compaction compact(String keys, int partitions, boolean cursor) throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint PRIMARY KEY, v int) WITH caching = {'keys': '" + keys + "'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < partitions; pk++)
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, round);
            flush();
        }
        for (long pk = 0; pk < partitions; pk += HOT_STRIDE)
            execute("SELECT v FROM %s WHERE pk = ?", pk);

        DatabaseDescriptor.setCursorCompactionEnabled(cursor);
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        Compaction result = new Compaction();
        result.originals = inputs.size();
        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        assertNotNull("unable to mark the inputs compacting", txn);
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        lookups.set(0);
        keysBuilt.set(0);
        countingThread = Thread.currentThread();
        try
        {
            new CompactionTask(cfs, txn, cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()), false).execute(ActiveCompactionsTracker.NOOP);
        }
        finally
        {
            countingThread = null;
        }
        CompactionPipelineCounts.assertPipelineRan(cursor, before);
        result.lookups = lookups.get();
        result.keysBuilt = keysBuilt.get();

        Set<SSTableReader> outputs = cfs.getLiveSSTables();
        assertEquals("expected one compaction output", 1, outputs.size());
        KeyCacheSupport<?> output = (KeyCacheSupport<?>) outputs.iterator().next();
        for (long pk = 0; pk < partitions; pk++)
        {
            DecoratedKey key = cfs.decorateKey(ByteBufferUtil.bytes(pk));
            if (output.getCachedPosition(key, false) != null)
                result.cachedKeys.add(pk);
        }
        return result;
    }
}
