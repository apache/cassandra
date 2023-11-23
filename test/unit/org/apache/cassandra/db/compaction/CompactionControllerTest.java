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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.LongPredicate;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

@RunWith(BMUnitRunner.class)
public class CompactionControllerTest extends SchemaLoader
{
    private static final String KEYSPACE = "CompactionControllerTest";
    private static final String CF1 = "Standard1";
    private static final String CF2 = "Standard2";
    private static final int TTL_SECONDS = 10;
    private static CountDownLatch compaction2FinishLatch = new CountDownLatch(1);
    private static CountDownLatch createCompactionControllerLatch = new CountDownLatch(1);
    private static CountDownLatch compaction1RefreshLatch = new CountDownLatch(1);
    private static CountDownLatch refreshCheckLatch = new CountDownLatch(1);
    private static int overlapRefreshCounter = 0;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    TableMetadata.builder(KEYSPACE, CF1)
                                                 .addPartitionKeyColumn("pk", AsciiType.instance)
                                                 .addClusteringColumn("ck", AsciiType.instance)
                                                 .addRegularColumn("val", AsciiType.instance),
                                    TableMetadata.builder(KEYSPACE, CF2)
                                                 .addPartitionKeyColumn("pk", AsciiType.instance)
                                                 .addClusteringColumn("ck", AsciiType.instance)
                                                 .addRegularColumn("val", AsciiType.instance));
    }

    @Test
    public void testMaxPurgeableTimestamp()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        DecoratedKey key = Util.dk("k1");

        long timestamp1 = FBUtilities.timestampMicros(); // latest timestamp
        long timestamp2 = timestamp1 - 5;
        long timestamp3 = timestamp2 - 5; // oldest timestamp

        // add to first memtable
        applyMutation(cfs.metadata(), key, timestamp1);

        // check max purgeable timestamp without any sstables
        try(CompactionController controller = new CompactionController(cfs, null, 0))
        {
            assertPurgeBoundary(controller.getPurgeEvaluator(key), timestamp1); //memtable only

            Util.flush(cfs);
            assertTrue(controller.getPurgeEvaluator(key).test(Long.MAX_VALUE)); //no memtables and no sstables
        }

        Set<SSTableReader> compacting = Sets.newHashSet(cfs.getLiveSSTables()); // first sstable is compacting

        // create another sstable
        applyMutation(cfs.metadata(), key, timestamp2);
        Util.flush(cfs);

        // check max purgeable timestamp when compacting the first sstable with and without a memtable
        try (CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
            assertPurgeBoundary(controller.getPurgeEvaluator(key), timestamp2);

            applyMutation(cfs.metadata(), key, timestamp3);

            assertPurgeBoundary(controller.getPurgeEvaluator(key), timestamp3); //second sstable and second memtable
        }

        // check max purgeable timestamp again without any sstables but with different insertion orders on the memtable
        Util.flush(cfs);

        //newest to oldest
        try (CompactionController controller = new CompactionController(cfs, null, 0))
        {
            applyMutation(cfs.metadata(), key, timestamp1);
            applyMutation(cfs.metadata(), key, timestamp2);
            applyMutation(cfs.metadata(), key, timestamp3);

            assertPurgeBoundary(controller.getPurgeEvaluator(key), timestamp3); //memtable only
        }

        Util.flush(cfs);

        //oldest to newest
        try (CompactionController controller = new CompactionController(cfs, null, 0))
        {
            applyMutation(cfs.metadata(), key, timestamp3);
            applyMutation(cfs.metadata(), key, timestamp2);
            applyMutation(cfs.metadata(), key, timestamp1);

            assertPurgeBoundary(controller.getPurgeEvaluator(key), timestamp3);
        }
    }

    @Test
    public void testGetFullyExpiredSSTables()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF2);
        cfs.truncateBlocking();

        DecoratedKey key = Util.dk("k1");

        long timestamp1 = FBUtilities.timestampMicros(); // latest timestamp
        long timestamp2 = timestamp1 - 5;
        long timestamp3 = timestamp2 - 5; // oldest timestamp

        // create sstable with tombstone that should be expired in no older timestamps
        applyDeleteMutation(cfs.metadata(), key, timestamp2);
        Util.flush(cfs);

        // first sstable with tombstone is compacting
        Set<SSTableReader> compacting = Sets.newHashSet(cfs.getLiveSSTables());

        // create another sstable with more recent timestamp
        applyMutation(cfs.metadata(), key, timestamp1);
        Util.flush(cfs);

        // second sstable is overlapping
        Set<SSTableReader> overlapping = Sets.difference(Sets.newHashSet(cfs.getLiveSSTables()), compacting);

        // the first sstable should be expired because the overlapping sstable is newer and the gc period is later
        long gcBefore = (System.currentTimeMillis() / 1000) + 5;
        Set<SSTableReader> expired = CompactionController.getFullyExpiredSSTables(cfs, compacting, overlapping, gcBefore);
        assertNotNull(expired);
        assertEquals(1, expired.size());
        assertEquals(compacting.iterator().next(), expired.iterator().next());

        // however if we add an older mutation to the memtable then the sstable should not be expired
        applyMutation(cfs.metadata(), key, timestamp3);
        expired = CompactionController.getFullyExpiredSSTables(cfs, compacting, overlapping, gcBefore);
        assertNotNull(expired);
        assertEquals(0, expired.size());

        // Now if we explicitly ask to ignore overlaped sstables, we should get back our expired sstable
        expired = CompactionController.getFullyExpiredSSTables(cfs, compacting, overlapping, gcBefore, true);
        assertNotNull(expired);
        assertEquals(1, expired.size());
    }

    @Test
    @BMRules(rules = {
    @BMRule(name = "Pause compaction",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "INVOKE getCompactionAwareWriter",
    condition = "Thread.currentThread().getName().equals(\"compaction1\")",
    action = "org.apache.cassandra.db.compaction.CompactionControllerTest.createCompactionControllerLatch.countDown();" +
             "com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly" +
             "(org.apache.cassandra.db.compaction.CompactionControllerTest.compaction2FinishLatch);"),
    @BMRule(name = "Check overlaps",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "INVOKE finish",
    condition = "Thread.currentThread().getName().equals(\"compaction1\")",
    action = "org.apache.cassandra.db.compaction.CompactionControllerTest.compaction1RefreshLatch.countDown();" +
             "com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly" +
             "(org.apache.cassandra.db.compaction.CompactionControllerTest.refreshCheckLatch);"),
    @BMRule(name = "Increment overlap refresh counter",
    targetClass = "ColumnFamilyStore",
    targetMethod = "getAndReferenceOverlappingLiveSSTables",
    condition = "Thread.currentThread().getName().equals(\"compaction1\")",
    action = "org.apache.cassandra.db.compaction.CompactionControllerTest.incrementOverlapRefreshCounter();")
    })
    public void testIgnoreOverlapsTrue() throws Exception
    {
        resetCounters();
        testOverlapIterator(true);
    }

    @Test
    @BMRules(rules = {
    @BMRule(name = "Pause compaction",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "INVOKE getCompactionAwareWriter",
    condition = "Thread.currentThread().getName().equals(\"compaction1\")",
    action = "org.apache.cassandra.db.compaction.CompactionControllerTest.createCompactionControllerLatch.countDown();" +
             "com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly" +
             "(org.apache.cassandra.db.compaction.CompactionControllerTest.compaction2FinishLatch);"),
    @BMRule(name = "Check overlaps",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "INVOKE finish",
    condition = "Thread.currentThread().getName().equals(\"compaction1\")",
    action = "org.apache.cassandra.db.compaction.CompactionControllerTest.compaction1RefreshLatch.countDown();" +
             "com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly" +
             "(org.apache.cassandra.db.compaction.CompactionControllerTest.refreshCheckLatch);"),
    @BMRule(name = "Increment overlap refresh counter",
    targetClass = "ColumnFamilyStore",
    targetMethod = "getAndReferenceOverlappingLiveSSTables",
    condition = "Thread.currentThread().getName().equals(\"compaction1\")",
    action = "org.apache.cassandra.db.compaction.CompactionControllerTest.incrementOverlapRefreshCounter();")
    })
    public void testIgnoreOverlapsFalse() throws Exception
    {
        resetCounters();
        testOverlapIterator(false);
    }

    @Test
    @BMRules(rules = {
    @BMRule(name = "Pause compaction",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "INVOKE getCompactionAwareWriter",
    condition = "Thread.currentThread().getName().equals(\"compaction1\")",
    action = "org.apache.cassandra.db.compaction.CompactionControllerTest.createCompactionControllerLatch.countDown();" +
             "com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly" +
             "(org.apache.cassandra.db.compaction.CompactionControllerTest.compaction2FinishLatch);"),
    @BMRule(name = "Check overlaps",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "INVOKE finish",
    condition = "Thread.currentThread().getName().equals(\"compaction1\")",
    action = "org.apache.cassandra.db.compaction.CompactionControllerTest.compaction1RefreshLatch.countDown();" +
             "com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly" +
             "(org.apache.cassandra.db.compaction.CompactionControllerTest.refreshCheckLatch);"),
    @BMRule(name = "Increment overlap refresh counter",
    targetClass = "ColumnFamilyStore",
    targetMethod = "getAndReferenceOverlappingLiveSSTables",
    condition = "Thread.currentThread().getName().equals(\"compaction1\")",
    action = "org.apache.cassandra.db.compaction.CompactionControllerTest.incrementOverlapRefreshCounter();")
    })
    public void testIgnoreOverlapsUCSTrue() throws Exception
    {
        resetCounters();
        testOverlapIteratorUCS(true);
    }

    @Test
    @BMRules(rules = {
    @BMRule(name = "Pause compaction",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "INVOKE getCompactionAwareWriter",
    condition = "Thread.currentThread().getName().equals(\"compaction1\")",
    action = "org.apache.cassandra.db.compaction.CompactionControllerTest.createCompactionControllerLatch.countDown();" +
             "com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly" +
             "(org.apache.cassandra.db.compaction.CompactionControllerTest.compaction2FinishLatch);"),
    @BMRule(name = "Check overlaps",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "INVOKE finish",
    condition = "Thread.currentThread().getName().equals(\"compaction1\")",
    action = "org.apache.cassandra.db.compaction.CompactionControllerTest.compaction1RefreshLatch.countDown();" +
             "com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly" +
             "(org.apache.cassandra.db.compaction.CompactionControllerTest.refreshCheckLatch);"),
    @BMRule(name = "Increment overlap refresh counter",
    targetClass = "ColumnFamilyStore",
    targetMethod = "getAndReferenceOverlappingLiveSSTables",
    condition = "Thread.currentThread().getName().equals(\"compaction1\")",
    action = "org.apache.cassandra.db.compaction.CompactionControllerTest.incrementOverlapRefreshCounter();")
    })
    public void testIgnoreOverlapsUCSFalse() throws Exception
    {
        resetCounters();
        testOverlapIteratorUCS(false);
    }

    private void resetCounters()
    {
        overlapRefreshCounter = 0;
        compaction2FinishLatch = new CountDownLatch(1);
        createCompactionControllerLatch = new CountDownLatch(1);
        compaction1RefreshLatch = new CountDownLatch(1);
        refreshCheckLatch = new CountDownLatch(1);
    }

    public void testOverlapIterator(boolean ignoreOverlaps) throws Exception
    {

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        //create 2 overlapping sstables
        DecoratedKey key = Util.dk("k1");
        long timestamp1 = FBUtilities.timestampMicros();
        long timestamp2 = timestamp1 - 5;
        applyMutation(cfs.metadata(), key, timestamp1);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        assertEquals(cfs.getLiveSSTables().size(), 1);
        Set<SSTableReader> sstables = cfs.getLiveSSTables();

        applyMutation(cfs.metadata(), key, timestamp2);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        assertEquals(cfs.getLiveSSTables().size(), 2);
        String sstable2 = cfs.getLiveSSTables().iterator().next().getFilename();

        CassandraRelevantProperties.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION.setBoolean(true);
        Map<String, String> options = new HashMap<>();
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "30");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "SECONDS");
        options.put(TimeWindowCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY, "MILLISECONDS");
        options.put(TimeWindowCompactionStrategyOptions.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY, "0");
        options.put(TimeWindowCompactionStrategyOptions.UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY, Boolean.toString(ignoreOverlaps));
        TimeWindowCompactionStrategy twcs = new TimeWindowCompactionStrategy(cfs, options);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            twcs.addSSTable(sstable);

        twcs.startup();

        CompactionTask task = (CompactionTask)twcs.getUserDefinedTask(sstables, 0);
        assertFalse(task.keepOriginals);

        assertNotNull(task);
        assertEquals(1, Iterables.size(task.transaction.originals()));

        //start a compaction for the first sstable (compaction1)
        //the overlap iterator should contain sstable2
        //this compaction will be paused by the BMRule
        Thread t = new Thread(() -> {
            task.execute(null);
        });

        //start a compaction for the second sstable (compaction2)
        //the overlap iterator should contain sstable1
        //this compaction should complete as normal
        Thread t2 = new Thread(() -> {
            Uninterruptibles.awaitUninterruptibly(createCompactionControllerLatch);
            assertEquals(1, overlapRefreshCounter);
            CompactionManager.instance.forceUserDefinedCompaction(sstable2);

            //after compaction2 is finished, wait 1 minute and then resume compaction1 (this gives enough time for the overlapIterator to be refreshed)
            //after resuming, the overlap iterator for compaction1 should be updated to include the new sstable created by compaction2,
            //and it should not contain sstable2
            try
            {
                TimeUnit.MINUTES.sleep(1);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            compaction2FinishLatch.countDown();
        });

        t.setName("compaction1");
        t.start();
        t2.start();

        compaction1RefreshLatch.await();
        //at this point, the overlap iterator for compaction1 should be refreshed

        //verify that the overlap iterator for compaction1 is refreshed twice, (once during the constructor, and again after compaction2 finishes)
        assertEquals(2, overlapRefreshCounter);

        refreshCheckLatch.countDown();
        t.join();
    }

    public void testOverlapIteratorUCS(boolean ignoreOverlaps) throws Exception
    {

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        //create 2 overlapping sstables
        DecoratedKey key = Util.dk("k1");
        long timestamp1 = FBUtilities.timestampMicros();
        long timestamp2 = timestamp1 - 5;
        applyMutation(cfs.metadata(), key, timestamp1);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        assertEquals(cfs.getLiveSSTables().size(), 1);
        Set<SSTableReader> sstables = cfs.getLiveSSTables();

        applyMutation(cfs.metadata(), key, timestamp2);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        assertEquals(cfs.getLiveSSTables().size(), 2);
        String sstable2 = cfs.getLiveSSTables().iterator().next().getFilename();

        CassandraRelevantProperties.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION.setBoolean(true);
        Map<String, String> options = new HashMap<>();
        options.put(TimeWindowCompactionStrategyOptions.UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY, Boolean.toString(ignoreOverlaps));
        UnifiedCompactionStrategy ucs = new UnifiedCompactionStrategy(cfs, options);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            ucs.addSSTable(sstable);

        ucs.startup();

        CompactionTask task = (CompactionTask)ucs.getUserDefinedTask(sstables, 0);
        assertFalse(task.keepOriginals);

        assertNotNull(task);
        assertEquals(1, Iterables.size(task.transaction.originals()));

        //start a compaction for the first sstable (compaction1)
        //the overlap iterator should contain sstable2
        //this compaction will be paused by the BMRule
        Thread t = new Thread(() -> {
            task.execute(null);
        });

        //start a compaction for the second sstable (compaction2)
        //the overlap iterator should contain sstable1
        //this compaction should complete as normal
        Thread t2 = new Thread(() -> {
            Uninterruptibles.awaitUninterruptibly(createCompactionControllerLatch);
            assertEquals(1, overlapRefreshCounter);
            CompactionManager.instance.forceUserDefinedCompaction(sstable2);

            //after compaction2 is finished, wait 1 minute and then resume compaction1 (this gives enough time for the overlapIterator to be refreshed)
            //after resuming, the overlap iterator for compaction1 should be updated to include the new sstable created by compaction2,
            //and it should not contain sstable2
            try
            {
                TimeUnit.MINUTES.sleep(1);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            compaction2FinishLatch.countDown();
        });

        t.setName("compaction1");
        t.start();
        t2.start();

        compaction1RefreshLatch.await();
        //at this point, the overlap iterator for compaction1 should be refreshed

        //verify that the overlap iterator for compaction1 is refreshed twice, (once during the constructor, and again after compaction2 finishes)
        assertEquals(2, overlapRefreshCounter);

        refreshCheckLatch.countDown();
        t.join();
    }

    private void applyMutation(TableMetadata cfm, DecoratedKey key, long timestamp)
    {
        ByteBuffer val = ByteBufferUtil.bytes(1L);

        new RowUpdateBuilder(cfm, timestamp, key)
        .clustering("ck")
        .add("val", val)
        .build()
        .applyUnsafe();
    }

    private void applyDeleteMutation(TableMetadata cfm, DecoratedKey key, long timestamp)
    {
        new Mutation(PartitionUpdate.fullPartitionDelete(cfm, key, timestamp, FBUtilities.nowInSeconds()))
        .applyUnsafe();
    }

    private void assertPurgeBoundary(LongPredicate evaluator, long boundary)
    {
        assertFalse(evaluator.test(boundary));
        assertTrue(evaluator.test(boundary - 1));
    }

    public static void incrementOverlapRefreshCounter()
    {
        overlapRefreshCounter++;
    }

    @Test
    public void testDisableNeverPurgeTombstones()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF2);
        cfs.truncateBlocking();

        DecoratedKey key = Util.dk("k1");
        long timestamp = System.currentTimeMillis();
        applyMutation(cfs.metadata(), key, timestamp);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        Set<SSTableReader> toCompact = Sets.newHashSet(cfs.getLiveSSTables());
        cfs.setNeverPurgeTombstones(true);
        applyMutation(cfs.metadata(), key, timestamp + 1);

        try (CompactionController cc = new CompactionController(cfs, toCompact, (int)(System.currentTimeMillis()/1000)))
        {
            assertFalse(cc.getPurgeEvaluator(key).test(timestamp));
            assertFalse(cc.getPurgeEvaluator(key).test(timestamp + 1));
            assertTrue(cc.getFullyExpiredSSTables().isEmpty());

            cfs.setNeverPurgeTombstones(false);
            assertFalse(cc.getPurgeEvaluator(key).test(timestamp));
            assertFalse(cc.getPurgeEvaluator(key).test(timestamp + 1));
            assertTrue(cc.getFullyExpiredSSTables().isEmpty());

            cc.maybeRefreshOverlaps();
            assertTrue(cc.getPurgeEvaluator(key).test(timestamp));
            assertFalse(cc.getPurgeEvaluator(key).test(timestamp + 1));
            assertTrue(cc.getFullyExpiredSSTables().isEmpty());
        }
    }
}
