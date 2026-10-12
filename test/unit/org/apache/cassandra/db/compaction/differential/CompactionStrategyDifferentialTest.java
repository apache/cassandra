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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.LeveledCompactionTask;
import org.apache.cassandra.db.compaction.ShardManager;
import org.apache.cassandra.db.compaction.ShardManagerNoDisks;
import org.apache.cassandra.db.compaction.ShardTracker;
import org.apache.cassandra.db.compaction.TimeWindowCompactionController;
import org.apache.cassandra.db.compaction.TimeWindowCompactionTask;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.compaction.unified.ShardedCompactionWriter;
import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MajorLeveledCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.ColumnFamilyStore.RING_VERSION_IRRELEVANT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * LCS, TWCS and UCS on the cursor path, through the real strategy tasks and their writers,
 * match the iterator path.
 */
public class CompactionStrategyDifferentialTest extends DifferentialCompactionTester
{
    private static final String TWCS = " AND compaction = {'class': 'TimeWindowCompactionStrategy', " +
                                       "'compaction_window_unit': 'MINUTES', 'compaction_window_size': '1'}";

    private static final String UCS = " AND compaction = {'class': 'UnifiedCompactionStrategy'}";

    /** Files of inputs a run is expected to drop whole, mapped to their copies, put back before each restore. */
    private final Map<Path, Path> droppedInputBackups = new HashMap<>();

    private ColumnFamilyStore table(String options) throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}" + options);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        return cfs;
    }

    // LCS

    private static TaskFactory leveled(int level, long maxSSTableBytes, boolean major, boolean retainOriginals)
    {
        return (cfs, txn, gcBefore) -> new LeveledCompactionTask(cfs, txn, level, gcBefore, maxSSTableBytes, major)
        {
            @Override
            public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                                  Directories directories,
                                                                  ILifecycleTransaction transaction,
                                                                  Set<SSTableReader> nonExpiredSSTables)
            {
                if (major)
                    return new MajorLeveledCompactionWriter(cfs, directories, transaction, nonExpiredSSTables,
                                                            maxSSTableBytes, retainOriginals);
                return new MaxSSTableSizeWriter(cfs, directories, transaction, nonExpiredSSTables,
                                                maxSSTableBytes, getLevel(), retainOriginals);
            }
        };
    }

    /** Each partition holds about 1KB, so an 8KB cap splits the output several times. */
    private void populateLeveled(int partitions, int rounds) throws Throwable
    {
        String padding = "x".repeat(100);
        for (int round = 0; round < rounds; round++)
        {
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < 10; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, padding + round + "-" + ck);
            flush();
        }
    }

    @Test
    public void majorLeveledWriter() throws Throwable
    {
        ColumnFamilyStore cfs = table("");
        populateLeveled(40, 2);

        CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), leveled(0, 8 * 1024, true, true));
        assertTrue("scenario must produce multiple outputs to test anything, got " + out.sstables.size(),
                   out.sstables.size() >= 2);
    }

    /** Committed outputs carry the task level and honour the size cap on the cursor path. */
    @Test
    public void committedOutputsCarryTheTaskLevelOnCursorPath() throws Throwable
    {
        ColumnFamilyStore cfs = table("");
        populateLeveled(40, 2);

        long maxSSTableBytes = 8 * 1024;
        commitThroughFactory(cfs, true, leveled(3, maxSSTableBytes, false, false));

        Set<SSTableReader> outputs = cfs.getLiveSSTables();
        assertTrue("scenario must produce multiple outputs to test anything, got " + outputs.size(),
                   outputs.size() >= 2);
        long largest = 0;
        for (SSTableReader sstable : outputs)
        {
            assertEquals("output was not written at the task's level", 3, sstable.getSSTableLevel());
            largest = Math.max(largest, sstable.onDiskLength());
        }
        // The cap is honoured to within one partition.
        assertTrue("an output overshot the cap by more than one partition: " + largest,
                   largest <= maxSSTableBytes * 2);
    }

    // TWCS

    /**
     * The real TWCS task with a fixed now. Each run adds the inputs its controller drops whole to
     * {@code droppedPerRun}.
     */
    private static TaskFactory timeWindow(boolean ignoreOverlaps, boolean retainOriginals, long nowInSeconds,
                                          List<Set<Descriptor>> droppedPerRun)
    {
        return (cfs, txn, gcBefore) -> new TimeWindowCompactionTask(cfs, txn, gcBefore, ignoreOverlaps)
        {
            @Override
            public CompactionController getCompactionController(Set<SSTableReader> toCompact, long gcBefore)
            {
                return new TimeWindowCompactionController(cfs, toCompact, gcBefore, ignoreOverlaps)
                {
                    @Override
                    public Set<SSTableReader> getFullyExpiredSSTables()
                    {
                        Set<SSTableReader> expired = super.getFullyExpiredSSTables();
                        Set<Descriptor> descriptors = new HashSet<>();
                        for (SSTableReader sstable : expired)
                            descriptors.add(sstable.descriptor);
                        droppedPerRun.add(descriptors);
                        return expired;
                    }
                };
            }

            @Override
            public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                                  Directories directories,
                                                                  ILifecycleTransaction transaction,
                                                                  Set<SSTableReader> nonExpiredSSTables)
            {
                return new DefaultCompactionWriter(cfs, directories, transaction, nonExpiredSSTables,
                                                   retainOriginals, 0);
            }
        }.setNowInSecondsSupplier(() -> nowInSeconds);
    }

    private ColumnFamilyStore twoWindows() throws Throwable
    {
        ColumnFamilyStore cfs = table(TWCS);

        // Two flushes with separated timestamps, so the inputs land in different windows.
        String padding = "x".repeat(200);
        for (long pk = 0; pk < 300; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000000",
                    pk, 0L, padding + "-old");
        flush();
        for (long pk = 150; pk < 450; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 90000000",
                    pk, 0L, padding + "-new");
        flush();

        assertTrue("the fixture needs inputs", cfs.getLiveSSTables().size() >= 2);
        return cfs;
    }

    /**
     * With ignoreOverlaps, TWCS drops an sstable whole once all of it is past gcBefore, even when it
     * overlaps older live data. Without ignoreOverlaps neither input below would be dropped, since
     * both are newer than the live data. Both paths must drop the same inputs and write the same bytes.
     */
    @Test
    public void timeWindowTaskIgnoringOverlapsMatchesIterator() throws Throwable
    {
        ColumnFamilyStore cfs = table(TWCS);

        // Live data, older than everything else.
        for (long pk = 0; pk < 100; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, 'live') USING TIMESTAMP 1000000", pk, 0L);
        flush();
        Set<SSTableReader> before = new HashSet<>(cfs.getLiveSSTables());

        // Every cell expires one second after the write.
        for (long pk = 200; pk < 300; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, 'ttl') USING TIMESTAMP 2000000 AND TTL 1", pk, 0L);
        flush();
        SSTableReader expired = newSSTable(cfs, before);
        before.add(expired);

        // Only partition deletes, of the first 10 live partitions.
        for (long pk = 0; pk < 10; pk++)
            execute("DELETE FROM %s USING TIMESTAMP 3000000 WHERE pk = ?", pk);
        flush();
        SSTableReader tombstones = newSSTable(cfs, before);

        // An hour ahead, so the TTL cells and the deletes are all before gcBefore.
        long now = FBUtilities.nowInSeconds() + 3600;
        Path backups = Files.createTempDirectory("dropped-inputs");
        try
        {
            backUpDroppedInput(expired, backups);
            backUpDroppedInput(tombstones, backups);

            List<Set<Descriptor>> droppedPerRun = new ArrayList<>();
            CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(),
                                                             timeWindow(true, true, now, droppedPerRun), now);

            Set<Descriptor> expectedDropped = Set.of(expired.descriptor, tombstones.descriptor);
            assertEquals("expected one run per path", 2, droppedPerRun.size());
            assertEquals("the iterator path did not drop the fully expired inputs", expectedDropped, droppedPerRun.get(0));
            assertEquals("the cursor path did not drop the fully expired inputs", expectedDropped, droppedPerRun.get(1));
            // The deletes were dropped without being applied, so the 10 rows they covered come back.
            assertEquals("the output must hold every live row", 100, outputRows(out));
        }
        finally
        {
            FileUtils.deleteDirectory(backups.toFile());
        }
    }

    /** The one sstable in the live set that is not in {@code before}. */
    private static SSTableReader newSSTable(ColumnFamilyStore cfs, Set<SSTableReader> before)
    {
        Set<SSTableReader> added = new HashSet<>(cfs.getLiveSSTables());
        added.removeAll(before);
        assertEquals("expected one new sstable from the flush", 1, added.size());
        return added.iterator().next();
    }

    private void backUpDroppedInput(SSTableReader sstable, Path backups) throws Exception
    {
        for (Component c : sstable.descriptor.discoverComponents())
        {
            Path original = sstable.descriptor.fileFor(c).toPath();
            Path copy = backups.resolve(original.getFileName());
            Files.copy(original, copy);
            droppedInputBackups.put(original, copy);
        }
    }

    /** A dropped input's files are deleted by the run, so put them back before the inputs are reopened. */
    @Override
    protected void restoreAfterCompaction(ColumnFamilyStore cfs,
                                          List<SSTableReader> outputs,
                                          List<SSTableReader> retainedInputClones,
                                          List<Descriptor> inputDescriptors,
                                          int liveBeforeCount) throws Exception
    {
        LifecycleTransaction.waitForDeletions();
        for (Map.Entry<Path, Path> backup : droppedInputBackups.entrySet())
            if (!Files.exists(backup.getKey()))
                Files.copy(backup.getValue(), backup.getKey());
        super.restoreAfterCompaction(cfs, outputs, retainedInputClones, inputDescriptors, liveBeforeCount);
    }

    /** The committed output's timestamp range spans both windows. */
    @Test
    public void committedOutputCarriesTheSpanningTimestampRange() throws Throwable
    {
        ColumnFamilyStore cfs = twoWindows();
        commitThroughFactory(cfs, true, timeWindow(false, false, FBUtilities.nowInSeconds(), new ArrayList<>()));

        List<SSTableReader> outputs = List.copyOf(cfs.getLiveSSTables());
        assertEquals("expected one compaction output", 1, outputs.size());
        SSTableReader output = outputs.get(0);

        assertEquals("the output's minTimestamp must be the oldest cell it carries",
                     1000000L, output.getSSTableMetadata().minTimestamp);
        assertEquals("the output's maxTimestamp must be the newest cell it carries",
                     90000000L, output.getSSTableMetadata().maxTimestamp);
    }

    // UCS

    /** The real UCS task building a {@link ShardedCompactionWriter} over the given shard count. */
    private static TaskFactory sharded(ColumnFamilyStore cfs, int numShards, boolean retainOriginals)
    {
        UnifiedCompactionStrategy strategy = unifiedStrategy(cfs);
        ShardManager shardManager = new ShardManagerNoDisks(ColumnFamilyStore.fullWeightedRange(RING_VERSION_IRRELEVANT,
                                                                                               cfs.getPartitioner()));
        return (c, txn, gcBefore) -> new UnifiedCompactionTask(c, strategy, txn, gcBefore, shardManager)
        {
            @Override
            public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                                  Directories directories,
                                                                  ILifecycleTransaction transaction,
                                                                  Set<SSTableReader> nonExpiredSSTables)
            {
                // A fresh tracker per writer, since it is stateful.
                return new ShardedCompactionWriter(cfs, directories, transaction, nonExpiredSSTables,
                                                   retainOriginals, true /* earlyOpenAllowed */,
                                                   shardManager.boundaries(numShards));
            }
        };
    }

    private static UnifiedCompactionStrategy unifiedStrategy(ColumnFamilyStore cfs)
    {
        for (List<AbstractCompactionStrategy> perRepairState : cfs.getCompactionStrategyManager().getStrategies())
            for (AbstractCompactionStrategy strategy : perRepairState)
                if (strategy instanceof UnifiedCompactionStrategy)
                    return (UnifiedCompactionStrategy) strategy;
        throw new AssertionError("the table is not on UnifiedCompactionStrategy");
    }

    @Test
    public void shardedWriterMatchesIterator() throws Throwable
    {
        ColumnFamilyStore cfs = table(UCS);

        String padding = "x".repeat(120);
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < 200; pk++)
                for (long ck = 0; ck < 4; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, padding + round + "-" + ck);
            flush();
        }

        CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), sharded(cfs, 4, true));
        assertTrue("sharding must produce several outputs to test anything, got " + out.sstables.size(),
                   out.sstables.size() >= 2);
    }

    /** Every output stays inside one shard on the cursor path. */
    @Test
    public void everyOutputStaysInsideOneShardOnCursorPath() throws Throwable
    {
        ColumnFamilyStore cfs = table(UCS);

        String padding = "y".repeat(200);
        for (long pk = 0; pk < 120; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0L, padding);
        flush();
        for (long pk = 0; pk < 120; pk += 2)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 1L, padding);
        flush();

        int numShards = 8;
        ShardManager shardManager = new ShardManagerNoDisks(ColumnFamilyStore.fullWeightedRange(RING_VERSION_IRRELEVANT,
                                                                                               cfs.getPartitioner()));
        // Commit, so the live set is the sharded output.
        commitThroughFactory(cfs, true, sharded(cfs, numShards, false));

        Set<SSTableReader> outputs = cfs.getLiveSSTables();
        assertTrue("sharding must produce several outputs to test anything, got " + outputs.size(),
                   outputs.size() >= 2);
        for (SSTableReader sstable : outputs)
            assertInsideOneShard(shardManager.boundaries(numShards), sstable);
    }

    /** Asserts the sstable's last key has not crossed the end of the shard its first key falls in. */
    private static void assertInsideOneShard(ShardTracker tracker, SSTableReader sstable)
    {
        DecoratedKey first = sstable.getFirst();
        DecoratedKey last = sstable.getLast();
        tracker.advanceTo(first.getToken());
        Token shardEnd = tracker.shardEnd();
        assertTrue(sstable + " spans shard boundary " + shardEnd + " (" + first + " to " + last + ')',
                   shardEnd == null || last.getToken().compareTo(shardEnd) <= 0);
    }
}
