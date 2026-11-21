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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class UnifiedCompactionTombstoneTest extends CQLTester
{
    private static final int PARTITIONS = 32;
    private static final int TOMBSTONES_PER_PARTITION = 16;
    private static final int LIVE_ROW = 100000;
    private static final int MARKERS = PARTITIONS * TOMBSTONES_PER_PARTITION * 2;

    @Test
    public void testLCSSingleSSTableTombstoneCompaction() throws Throwable
    {
        testSingleSSTableTombstoneCompaction(LeveledCompactionStrategy.class, null);
    }

    @Test
    public void testTieredUCSSingleSSTableTombstoneCompaction() throws Throwable
    {
        testSingleSSTableTombstoneCompaction(UnifiedCompactionStrategy.class, "T4");
    }

    @Test
    public void testLeveledUCSSingleSSTableTombstoneCompaction() throws Throwable
    {
        testSingleSSTableTombstoneCompaction(UnifiedCompactionStrategy.class, "L10");
    }

    @Test
    public void testOptIn()
    {
        ColumnFamilyStore cfs = createTable();
        writeMixedSSTable(cfs);
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        Map<String, String> options = options(UnifiedCompactionStrategy.class, "T4");
        for (Map<String, String> overrides : List.of(Map.<String, String>of(),
                                                     Map.of("unchecked_tombstone_compaction", "false")))
        {
            options.putAll(overrides);
            cfs.setCompactionParameters(options);
            AbstractCompactionStrategy strategy = cfs.getCompactionStrategyManager().getCompactionStrategyFor(sstable);
            assertTrue(strategy.disableTombstoneCompactions);
            assertTrue(strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).isEmpty());
        }

        for (Map<String, String> overrides : List.of(Map.of("tombstone_threshold", "0.2"),
                                                     Map.of("tombstone_compaction_interval", "0"),
                                                     Map.of("unchecked_tombstone_compaction", "true")))
        {
            options = options(UnifiedCompactionStrategy.class, "T4");
            options.putAll(overrides);
            cfs.setCompactionParameters(options);
            assertFalse(cfs.getCompactionStrategyManager().getCompactionStrategyFor(sstable).disableTombstoneCompactions);
        }
    }

    @Test
    public void testThresholdIntervalAndGcBefore() throws Throwable
    {
        for (boolean unchecked : new boolean[]{ false, true })
        {
            ColumnFamilyStore cfs = createTable();
            writeMixedSSTable(cfs);
            SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
            Map<String, String> options = tombstoneOptions(unchecked);
            options.put("tombstone_compaction_interval", "86400");
            cfs.setCompactionParameters(options);
            assertTrue(strategy(cfs, sstable).getNextBackgroundTasks(FBUtilities.nowInSeconds()).isEmpty());

            options.put("tombstone_compaction_interval", "0");
            options.put("tombstone_threshold", "1000");
            cfs.setCompactionParameters(options);
            assertTrue(strategy(cfs, sstable).getNextBackgroundTasks(FBUtilities.nowInSeconds()).isEmpty());

            options.put("tombstone_threshold", "0.2");
            cfs.setCompactionParameters(options);
            assertTrue(strategy(cfs, sstable).getNextBackgroundTasks(0).isEmpty());
            executeTasks(strategy(cfs, sstable).getNextBackgroundTasks(FBUtilities.nowInSeconds()),
                         OperationType.TOMBSTONE_COMPACTION, Collections.singleton(sstable));
            assertEquals(0, countRangeTombstoneMarkers(cfs));
            assertLiveRows();
        }
    }

    @Test
    public void testCompactingAndSuspectSSTablesAreExcluded()
    {
        ColumnFamilyStore cfs = createTable();
        cfs.setCompactionParameters(tombstoneOptions(true));
        writeMixedSSTable(cfs);
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        AbstractCompactionStrategy strategy = strategy(cfs, sstable);
        try (LifecycleTransaction transaction = cfs.getTracker().tryModify(Collections.singleton(sstable), OperationType.COMPACTION))
        {
            assertNotNull(transaction);
            assertTrue(strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).isEmpty());
        }
        sstable.markSuspect();
        try
        {
            assertTrue(strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).isEmpty());
        }
        finally
        {
            sstable.unmarkSuspect();
        }
        Collection<AbstractCompactionTask> tasks = strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds());
        try
        {
            assertEquals(1, tasks.size());
        }
        finally
        {
            tasks.forEach(AbstractCompactionTask::rejected);
        }
    }

    @Test
    public void testNeverPurgeTombstones()
    {
        ColumnFamilyStore cfs = createTable();
        cfs.setCompactionParameters(tombstoneOptions(true));
        writeMixedSSTable(cfs);
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        cfs.setNeverPurgeTombstones(true);
        try
        {
            assertTrue(strategy(cfs, sstable).getNextBackgroundTasks(FBUtilities.nowInSeconds()).isEmpty());
        }
        finally
        {
            cfs.setNeverPurgeTombstones(false);
        }
    }

    @Test
    public void testOverlappingSSTableSafety() throws Throwable
    {
        ColumnFamilyStore cfs = createTable();
        cfs.setCompactionParameters(tombstoneOptions(false));
        writeOlderRows(cfs);
        Util.flush(cfs);
        SSTableReader older = Iterables.getOnlyElement(cfs.getLiveSSTables());
        writeMixedSSTable(cfs);
        SSTableReader mixed = cfs.getLiveSSTables().stream().filter(s -> s != older).findFirst().get();
        assertTrue(strategy(cfs, mixed).getNextBackgroundTasks(FBUtilities.nowInSeconds()).isEmpty());

        // Unchecked skips candidate overlap checks, but must not resurrect the older SSTable's data.
        cfs.setCompactionParameters(tombstoneOptions(true));
        executeTasks(strategy(cfs, mixed).getNextBackgroundTasks(FBUtilities.nowInSeconds()),
                     OperationType.TOMBSTONE_COMPACTION, Collections.singleton(mixed));
        assertEquals(MARKERS, countRangeTombstoneMarkers(cfs));
        assertTrue(cfs.getLiveSSTables().contains(older));
        assertLiveRows();
    }

    @Test
    public void testOverlappingMemtableSafety() throws Throwable
    {
        ColumnFamilyStore cfs = createTable();
        cfs.setCompactionParameters(tombstoneOptions(true));
        writeMixedSSTable(cfs);
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        writeOlderRows(cfs);
        executeTasks(strategy(cfs, sstable).getNextBackgroundTasks(FBUtilities.nowInSeconds()),
                     OperationType.TOMBSTONE_COMPACTION, Collections.singleton(sstable));
        assertEquals(MARKERS, countRangeTombstoneMarkers(cfs));
        assertLiveRows();
    }

    @Test
    public void testOnlyPurgeRepairedTombstones() throws Throwable
    {
        for (boolean unchecked : new boolean[]{ false, true })
        {
            ColumnFamilyStore cfs = createTable();
            Map<String, String> options = tombstoneOptions(unchecked);
            options.put("only_purge_repaired_tombstones", "true");
            cfs.setCompactionParameters(options);
            writeMixedSSTable(cfs);
            SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
            assertFalse(sstable.isRepaired());
            Collection<AbstractCompactionTask> tasks = strategy(cfs, sstable).getNextBackgroundTasks(FBUtilities.nowInSeconds());
            try
            {
                assertTrue("Unrepaired SSTables cannot purge tombstones", tasks.isEmpty());
            }
            finally
            {
                tasks.forEach(AbstractCompactionTask::rejected);
            }
            assertEquals(MARKERS, countRangeTombstoneMarkers(cfs));
            assertLiveRows();

            cfs.getCompactionStrategyManager().mutateRepaired(Collections.singleton(sstable), 1L, null, false);
            assertTrue(sstable.isRepaired());
            executeTasks(strategy(cfs, sstable).getNextBackgroundTasks(FBUtilities.nowInSeconds()),
                         OperationType.TOMBSTONE_COMPACTION, Collections.singleton(sstable));
            assertEquals(0, countRangeTombstoneMarkers(cfs));
            assertLiveRows();
        }
    }

    @Test
    public void testNormalCompactionTakesPrecedence() throws Throwable
    {
        ColumnFamilyStore cfs = createTable();
        cfs.setCompactionParameters(tombstoneOptions(true));
        for (int i = 0; i < 4; ++i)
            writeMixedSSTable(cfs);
        assertEquals(4, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        executeTasks(strategy(cfs, sstable).getNextBackgroundTasks(FBUtilities.nowInSeconds()),
                     OperationType.COMPACTION, cfs.getLiveSSTables());
        assertEquals(0, countRangeTombstoneMarkers(cfs));
        assertLiveRows();
    }

    @Test
    public void testParallelOutputShards() throws Throwable
    {
        ColumnFamilyStore cfs = createTable();
        Map<String, String> options = tombstoneOptions(false);
        cfs.setCompactionParameters(options);
        writeMixedSSTable(cfs);
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        options.put("base_shard_count", "4");
        options.put("parallelize_output_shards", "true");
        cfs.setCompactionParameters(options);
        Collection<AbstractCompactionTask> tasks = strategy(cfs, sstable).getNextBackgroundTasks(FBUtilities.nowInSeconds());
        try
        {
            assertTrue(tasks.size() > 1);
            executeTasks(tasks, OperationType.TOMBSTONE_COMPACTION, Collections.singleton(sstable));
        }
        finally
        {
            tasks.forEach(AbstractCompactionTask::rejected);
        }
        assertEquals(0, countRangeTombstoneMarkers(cfs));
        assertLiveRows();
    }

    @Test
    public void testWholeFileExpirationTakesPrecedence() throws Throwable
    {
        ColumnFamilyStore cfs = createTable();
        Map<String, String> options = tombstoneOptions(true);
        cfs.setCompactionParameters(options);
        writeMixedSSTable(cfs);
        SSTableReader mixed = Iterables.getOnlyElement(cfs.getLiveSSTables());

        long localDeletionTime = FBUtilities.nowInSeconds() - 86400;
        for (int shard = 0; shard < PARTITIONS; ++shard)
            new RowUpdateBuilder(cfs.metadata(), localDeletionTime, 50L, shard)
                .addRangeTombstone(0, 1).build().applyUnsafe();
        Util.flush(cfs);
        SSTableReader expired = cfs.getLiveSSTables().stream().filter(s -> s != mixed).findFirst().get();
        // Force the expiration check without waiting for the configured polling interval.
        Controller controller = spy(Controller.fromOptions(cfs, options));
        doReturn(-1L).when(controller).getExpiredSSTableCheckFrequency();
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, options, controller);
        strategy.addSSTables(cfs.getLiveSSTables());
        strategy.startup();
        try
        {
            executeTasks(strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()),
                         OperationType.COMPACTION, Collections.singleton(expired));
        }
        finally
        {
            strategy.shutdown();
        }
        assertEquals(Collections.singleton(mixed), cfs.getLiveSSTables());
        assertEquals(MARKERS, countRangeTombstoneMarkers(cfs));
        assertLiveRows();
    }

    @Test
    public void testHighestDroppableRatioIsSelected()
    {
        ColumnFamilyStore cfs = createTable();
        cfs.setCompactionParameters(tombstoneOptions(true));
        writeMixedSSTable(cfs, 2);
        SSTableReader lowerRatio = Iterables.getOnlyElement(cfs.getLiveSSTables());
        writeMixedSSTable(cfs);
        SSTableReader higherRatio = cfs.getLiveSSTables().stream().filter(s -> s != lowerRatio).findFirst().get();
        long gcBefore = FBUtilities.nowInSeconds();
        assertTrue(higherRatio.getEstimatedDroppableTombstoneRatio(gcBefore) > lowerRatio.getEstimatedDroppableTombstoneRatio(gcBefore));
        Collection<AbstractCompactionTask> tasks = strategy(cfs, higherRatio).getNextBackgroundTasks(gcBefore);
        try
        {
            AbstractCompactionTask task = Iterables.getOnlyElement(tasks);
            assertEquals(Collections.singleton(higherRatio), task.transaction.originals());
            assertEquals(OperationType.TOMBSTONE_COMPACTION, task.compactionType);
        }
        finally
        {
            tasks.forEach(AbstractCompactionTask::rejected);
        }
    }

    private void testSingleSSTableTombstoneCompaction(Class<? extends AbstractCompactionStrategy> strategyClass,
                                                    String scalingParameter) throws Throwable
    {
        for (boolean unchecked : new boolean[]{ false, true })
        {
            ColumnFamilyStore cfs = createTable();
            Map<String, String> options = options(strategyClass, scalingParameter);
            options.put("tombstone_compaction_interval", "0");
            options.put("unchecked_tombstone_compaction", Boolean.toString(unchecked));
            cfs.setCompactionParameters(options);

            writeMixedSSTable(cfs);
            SSTableReader original = Iterables.getOnlyElement(cfs.getLiveSSTables());
            assertEquals(MARKERS, countRangeTombstoneMarkers(cfs));

            AbstractCompactionStrategy strategy = cfs.getCompactionStrategyManager().getCompactionStrategyFor(original);
            Collection<AbstractCompactionTask> tasks = strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds());
            assertEquals("Expected a tombstone compaction for " + options, 1, tasks.size());
            executeTasks(tasks, OperationType.TOMBSTONE_COMPACTION, Collections.singleton(original));

            assertEquals(0, countRangeTombstoneMarkers(cfs));
            assertLiveRows();
            assertTrue(strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).isEmpty());
        }
    }

    private static AbstractCompactionStrategy strategy(ColumnFamilyStore cfs, SSTableReader sstable)
    {
        return cfs.getCompactionStrategyManager().getCompactionStrategyFor(sstable);
    }

    private static Map<String, String> tombstoneOptions(boolean unchecked)
    {
        Map<String, String> options = options(UnifiedCompactionStrategy.class, "T4");
        options.put("tombstone_compaction_interval", "0");
        options.put("unchecked_tombstone_compaction", Boolean.toString(unchecked));
        return options;
    }

    private static void executeTasks(Collection<AbstractCompactionTask> tasks, OperationType type, Collection<SSTableReader> originals)
    {
        try
        {
            assertFalse("Expected a background compaction", tasks.isEmpty());
            for (AbstractCompactionTask task : tasks)
            {
                assertEquals(type, task.compactionType);
                assertEquals(originals, task.transaction.originals());
                task.execute(ActiveCompactionsTracker.NOOP);
            }
        }
        finally
        {
            tasks.forEach(AbstractCompactionTask::rejected);
        }
    }

    private static void writeOlderRows(ColumnFamilyStore cfs)
    {
        for (int shard = 0; shard < PARTITIONS; ++shard)
            new RowUpdateBuilder(cfs.metadata(), 100L, shard).clustering(0).add("value", "old").build().applyUnsafe();
    }

    private ColumnFamilyStore createTable()
    {
        createTable("CREATE TABLE %s (shard int, task int, value text, PRIMARY KEY (shard, task)) " +
                    "WITH gc_grace_seconds = 0 AND compaction = {'class': 'UnifiedCompactionStrategy', 'enabled': 'false'}");
        return getCurrentColumnFamilyStore();
    }

    private static Map<String, String> options(Class<? extends AbstractCompactionStrategy> strategyClass, String scalingParameter)
    {
        Map<String, String> options = new HashMap<>();
        options.put("class", strategyClass.getSimpleName());
        options.put("enabled", "false");
        if (scalingParameter != null)
        {
            options.put("scaling_parameters", scalingParameter);
            options.put("base_shard_count", "1");
            options.put("min_sstable_size", "0B");
            options.put("parallelize_output_shards", "false");
        }
        return options;
    }

    private static void writeMixedSSTable(ColumnFamilyStore cfs)
    {
        writeMixedSSTable(cfs, TOMBSTONES_PER_PARTITION);
    }

    private static void writeMixedSSTable(ColumnFamilyStore cfs, int tombstonesPerPartition)
    {
        // Use old local deletion times to avoid sleeping or depending on histogram rounding.
        long localDeletionTime = FBUtilities.nowInSeconds() - 86400;
        for (int shard = 0; shard < PARTITIONS; ++shard)
        {
            for (int row = 0; row < tombstonesPerPartition; ++row)
            {
                int task = row * 3;
                new RowUpdateBuilder(cfs.metadata(), 100L, shard).clustering(task).add("value", "deleted").build().applyUnsafe();
                new RowUpdateBuilder(cfs.metadata(), localDeletionTime, 200L, shard)
                    .addRangeTombstone(task, task + 1).build().applyUnsafe();
            }
            new RowUpdateBuilder(cfs.metadata(), 300L, shard).clustering(LIVE_ROW).add("value", "live").build().applyUnsafe();
        }
        Util.flush(cfs);
    }

    private static int countRangeTombstoneMarkers(ColumnFamilyStore cfs)
    {
        int markers = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        while (partition.hasNext())
                        {
                            if (partition.next().isRangeTombstoneMarker())
                                ++markers;
                        }
                    }
                }
            }
        }
        return markers;
    }

    private void assertLiveRows() throws Throwable
    {
        for (int shard = 0; shard < PARTITIONS; ++shard)
            assertRows(execute("SELECT task, value FROM %s WHERE shard = ?", shard), row(LIVE_ROW, "live"));
    }
}
