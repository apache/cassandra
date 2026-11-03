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

package org.apache.cassandra.db.compaction.differential;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.CompactionPipelineCounts;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.ClusteringDescriptor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableCursorWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Work done once per output partition on the compaction write path, counted on the thread that runs the
 * compaction, on both paths.
 */
@RunWith(BMUnitRunner.class)
@BMRules(rules = {
    @BMRule(name = "count partitions prepared",
            targetClass = "org.apache.cassandra.db.compaction.writers.CompactionAwareWriter",
            targetMethod = "prepareForPartition",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.compaction.differential.CompactionPerPartitionWorkTest.isCounting()",
            action = "org.apache.cassandra.db.compaction.differential.CompactionPerPartitionWorkTest.prepared.incrementAndGet()"),
    @BMRule(name = "count file cache skips",
            targetClass = "org.apache.cassandra.io.sstable.format.SSTableReader",
            targetMethod = "trySkipFileCacheBefore",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.compaction.differential.CompactionPerPartitionWorkTest.isCounting()",
            action = "org.apache.cassandra.db.compaction.differential.CompactionPerPartitionWorkTest.cacheSkips.incrementAndGet()"),
    @BMRule(name = "count key hashes",
            targetClass = "org.apache.cassandra.utils.MurmurHash",
            targetMethod = "hash3_x64_128",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.compaction.differential.CompactionPerPartitionWorkTest.isCounting()",
            action = "org.apache.cassandra.db.compaction.differential.CompactionPerPartitionWorkTest.hashes.incrementAndGet()"),
    @BMRule(name = "count pre-hashed key copies",
            targetClass = "org.apache.cassandra.db.PreHashedDecoratedKey",
            targetMethod = "<init>",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.compaction.differential.CompactionPerPartitionWorkTest.isCounting()",
            action = "org.apache.cassandra.db.compaction.differential.CompactionPerPartitionWorkTest.keyCopies.incrementAndGet()"),
    @BMRule(name = "check the max clustering when a partition ends",
            targetClass = "org.apache.cassandra.io.sstable.SSTableCursorWriter",
            targetMethod = "writePartitionEnd",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.compaction.differential.CompactionPerPartitionWorkTest.isCounting()",
            action = "org.apache.cassandra.db.compaction.differential.CompactionPerPartitionWorkTest.checkMaxClusteringAtPartitionEnd($0, $4)")
})
public class CompactionPerPartitionWorkTest extends DifferentialCompactionTester
{
    public static final AtomicLong prepared = new AtomicLong();
    public static final AtomicLong cacheSkips = new AtomicLong();
    public static final AtomicLong hashes = new AtomicLong();
    public static final AtomicLong keyCopies = new AtomicLong();
    /** Partitions that ended with a last clustering above the collector's max clustering. */
    public static final AtomicLong maxBelowLastName = new AtomicLong();
    public static final AtomicLong partitionEndsChecked = new AtomicLong();
    private static volatile Thread countingThread;

    private static final int PARTITIONS = 4000;
    private static final String KEY_PREFIX = "k".repeat(200);
    private static final String VALUE = "x".repeat(300);
    private static final int OPEN_INTERVAL_MIB = 1;

    public static boolean isCounting()
    {
        return Thread.currentThread() == countingThread;
    }

    @Before
    public void saveFormat()
    {
        selectSSTableFormat(DatabaseDescriptor.getSelectedSSTableFormat().name());
    }

    @After
    public void tearDown()
    {
        countingThread = null;
        restoreSelectedFormat();
    }

    @Test
    public void bothPathsPrepareEachPartitionOnce() throws Throwable
    {
        for (boolean cursor : new boolean[]{ false, true })
        {
            ColumnFamilyStore cfs = twoSSTables(100);
            startCounting();
            commitThroughFactory(cfs, cursor, (c, txn, gcBefore) -> new CompactionTask(c, txn, gcBefore, false));
            countingThread = null;
            assertEquals("cursor=" + cursor + ": each output partition must go through prepareForPartition once",
                         100, prepared.get());
        }
    }

    @Test
    public void cursorPathHashesNoMoreThanIteratorBig() throws Throwable
    {
        assertCursorHashesNoMoreThanIterator("big");
    }

    @Test
    public void cursorPathHashesNoMoreThanIteratorBti() throws Throwable
    {
        assertCursorHashesNoMoreThanIterator("bti");
    }

    /** Compares the hashes each extra output partition costs, so work done once per compaction does not count. */
    private void assertCursorHashesNoMoreThanIterator(String format) throws Throwable
    {
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        double iterator = hashesPerExtraPartition(false);
        double cursor = hashesPerExtraPartition(true);
        logger.info("{} key hashes per output partition: iterator {}, cursor {}", format, iterator, cursor);
        assertTrue("the iterator path hashed no key, so the comparison proves nothing", iterator > 0);
        assertTrue(String.format("the cursor path hashes %.3f times per output partition, the iterator path %.3f",
                                 cursor, iterator),
                   cursor <= iterator);
    }

    private double hashesPerExtraPartition(boolean cursor) throws Throwable
    {
        long[] counted = new long[2];
        int[] partitions = { 1000, 2000 };
        for (int i = 0; i < 2; i++)
        {
            ColumnFamilyStore cfs = twoSSTables(partitions[i]);
            startCounting();
            commitThroughFactory(cfs, cursor, (c, txn, gcBefore) -> new CompactionTask(c, txn, gcBefore, false));
            countingThread = null;
            counted[i] = hashes.get();
        }
        return (double) (counted[1] - counted[0]) / (partitions[1] - partitions[0]);
    }

    /**
     * On BTI the cursor path reuses three keys for the partition index and copies only the keys the index
     * keeps, so the count of key copies does not grow with the partition count. A copy of each partition's
     * key ({@code key.retainable()}) would add one per partition.
     */
    @Test
    public void btiCursorPathCopiesNoKeyPerPartition() throws Throwable
    {
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
        int small = 1000;
        int large = 10 * small;
        long smallCopies = keyCopiesDuringCursorCompaction(small);
        long largeCopies = keyCopiesDuringCursorCompaction(large);
        logger.info("BTI cursor compaction key copies: {} for {} partitions, {} for {} partitions",
                    smallCopies, small, largeCopies, large);
        assertTrue(String.format("the BTI cursor path copied %d keys for %d partitions and %d for %d partitions; " +
                                 "the count grows with the partitions, so a key is copied per partition",
                                 smallCopies, small, largeCopies, large),
                   largeCopies - smallCopies < (large - small) / 100);
    }

    private long keyCopiesDuringCursorCompaction(int partitions) throws Throwable
    {
        ColumnFamilyStore cfs = twoSSTables(partitions);
        startCounting();
        commitThroughFactory(cfs, true, (c, txn, gcBefore) -> new CompactionTask(c, txn, gcBefore, false));
        countingThread = null;
        return keyCopies.get();
    }

    /**
     * The partition's last clustering is in the max clustering of the output before the end of partition
     * is written, as on the iterator path, which updates it with each unfiltered. An sstable opened early
     * during that write would otherwise carry a max clustering below the partition's last row.
     */
    @Test
    public void lastClusteringIsInMaxClusteringBeforePartitionEnd() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 2; round++)
        {
            // each partition's clusterings are above every earlier partition's, so each last clustering raises the max
            for (long pk = 0; pk < 10; pk++)
                for (long ck = 0; ck < 5; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, pk * 5 + ck, "v" + round);
            flush();
        }
        startCounting();
        commitThroughFactory(cfs, true, (c, txn, gcBefore) -> new CompactionTask(c, txn, gcBefore, false));
        countingThread = null;
        assertEquals("every output partition must be checked", 10, partitionEndsChecked.get());
        assertEquals("partitions whose last clustering was above the max clustering when the end of partition was written",
                     0, maxBelowLastName.get());
    }

    /** Called by Byteman at the start of {@link SSTableCursorWriter#writePartitionEnd}. */
    public static void checkMaxClusteringAtPartitionEnd(SSTableCursorWriter writer, ClusteringDescriptor lastName) throws Exception
    {
        if (lastName == null)
            return;
        Field collectorField = SSTableCursorWriter.class.getDeclaredField("metadataCollector");
        collectorField.setAccessible(true);
        Object collector = collectorField.get(writer);
        Field maxField = MetadataCollector.class.getDeclaredField("maxClusteringDescriptor");
        maxField.setAccessible(true);
        ClusteringDescriptor max = (ClusteringDescriptor) maxField.get(collector);
        partitionEndsChecked.incrementAndGet();
        if (ClusteringComparator.compare(max, lastName) < 0)
            maxBelowLastName.incrementAndGet();
    }

    /** Offline, the originals' file cache is skipped once per open interval, not on every partition after it. */
    @Test
    public void offlineSkipsFileCacheOncePerIntervalCursor() throws Throwable
    {
        assertOfflineSkipsOncePerInterval(true);
    }

    @Test
    public void offlineSkipsFileCacheOncePerIntervalIterator() throws Throwable
    {
        assertOfflineSkipsOncePerInterval(false);
    }

    private void assertOfflineSkipsOncePerInterval(boolean cursor) throws Throwable
    {
        int originalInterval = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(OPEN_INTERVAL_MIB);
        try
        {
            ColumnFamilyStore cfs = twoSSTables(PARTITIONS);
            DatabaseDescriptor.setCursorCompactionEnabled(cursor);
            long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());

            // like sstablesplit: readers outside the tracker, in an offline transaction
            Set<Descriptor> inputDescs = new HashSet<>();
            File dataDir = null;
            long inputBytes = 0;
            for (SSTableReader live : cfs.getLiveSSTables())
            {
                inputDescs.add(live.descriptor);
                dataDir = live.descriptor.directory;
                inputBytes += live.uncompressedLength();
            }
            Set<SSTableReader> fresh = new HashSet<>();
            for (Descriptor d : inputDescs)
                fresh.add(SSTableReader.open(cfs, d));
            if (cursor)
                assertCursorPathWillRun(cfs, fresh, gcBefore);

            try (LifecycleTransaction offline = LifecycleTransaction.offline(OperationType.COMPACTION, fresh))
            {
                CompactionPipelineCounts before = CompactionPipelineCounts.mark();
                startCounting();
                new CompactionTask(cfs, offline, gcBefore, true).execute(ActiveCompactionsTracker.NOOP);
                countingThread = null;
                CompactionPipelineCounts.assertPipelineRan(cursor, before);
            }

            long intervals = inputBytes / (OPEN_INTERVAL_MIB << 20) + 1;
            assertTrue("the compaction never crossed the open interval, so this proves nothing", cacheSkips.get() > 0);
            assertTrue(String.format("cursor=%s: %d file cache skips for %d originals over at most %d intervals",
                                     cursor, cacheSkips.get(), inputDescs.size(), intervals),
                       cacheSkips.get() <= inputDescs.size() * intervals);

            deleteOutputs(dataDir, inputDescs);
        }
        finally
        {
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalInterval);
        }
    }

    private static void deleteOutputs(File dataDir, Set<Descriptor> inputDescs)
    {
        File[] files = dataDir.tryList();
        if (files == null)
            return;
        for (File f : files)
        {
            if (!f.name().endsWith("-Data.db"))
                continue;
            Descriptor desc = Descriptor.fromFile(f);
            if (inputDescs.contains(desc))
                continue;
            for (Component c : desc.discoverComponents())
                desc.fileFor(c).deleteIfExists();
        }
    }

    private static void startCounting()
    {
        prepared.set(0);
        cacheSkips.set(0);
        hashes.set(0);
        keyCopies.set(0);
        maxBelowLastName.set(0);
        partitionEndsChecked.set(0);
        countingThread = Thread.currentThread();
    }

    private ColumnFamilyStore twoSSTables(int partitions) throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 2; round++)
        {
            for (int pk = 0; pk < partitions; pk++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", KEY_PREFIX + pk, 0L, VALUE + round);
            flush();
        }
        return cfs;
    }
}
