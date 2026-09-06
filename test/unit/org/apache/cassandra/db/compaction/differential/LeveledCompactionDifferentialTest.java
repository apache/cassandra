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

import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.LeveledCompactionTask;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.MajorLeveledCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * LCS on the cursor path, through the real {@link LeveledCompactionTask}, which picks the writer and
 * the output level. {@code MultiOutputDifferentialCompactionTest} builds a
 * {@link MaxSSTableSizeWriter} by hand at level 0, so neither the task nor level assignment nor
 * {@link MajorLeveledCompactionWriter} was covered against the cursor writer before this.
 */
public class LeveledCompactionDifferentialTest extends DifferentialCompactionTester
{
    /**
     * The parameter cannot be named keepOriginals: inside the subclass that name resolves to
     * CompactionTask's inherited field, which {@link LeveledCompactionTask} always leaves false.
     */
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

    /**
     * Deliberately NOT on LeveledCompactionStrategy: the task carries the level and the size cap, and an
     * LCS manifest would demote the second run's outputs to L0 through
     * {@code LeveledGenerations.sendToL0} because the first run's outputs still occupy the level. That
     * rewrites the level in the sstable metadata and shows up as a stats divergence that belongs to the
     * harness, not to either compaction path.
     */
    private ColumnFamilyStore table() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        return cfs;
    }

    /** Each partition holds about 1KB, so an 8KB cap splits the output several times. */
    private void populate(int partitions, int rounds) throws Throwable
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
    public void maxSizeWriterAtLevelTwo() throws Throwable
    {
        ColumnFamilyStore cfs = table();
        populate(40, 2);

        CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), leveled(2, 8 * 1024, false, true));
        assertTrue("scenario must produce multiple outputs to test anything, got " + out.sstables.size(),
                   out.sstables.size() >= 2);
    }

    @Test
    public void majorLeveledWriter() throws Throwable
    {
        ColumnFamilyStore cfs = table();
        populate(40, 2);

        CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), leveled(0, 8 * 1024, true, true));
        assertTrue("scenario must produce multiple outputs to test anything, got " + out.sstables.size(),
                   out.sstables.size() >= 2);
    }

    /**
     * Level and size of the committed outputs, which the differential comparison cannot pin: both
     * paths would have to get them wrong in the same way to still match.
     */
    @Test
    public void committedOutputsCarryTheTaskLevelOnCursorPath() throws Throwable
    {
        assertCommittedOutputsCarryTheTaskLevel(true);
    }

    /** The same expectation on the iterator path, so a failure above is read as a cursor defect. */
    @Test
    public void committedOutputsCarryTheTaskLevelOnIteratorPath() throws Throwable
    {
        assertCommittedOutputsCarryTheTaskLevel(false);
    }

    private void assertCommittedOutputsCarryTheTaskLevel(boolean cursor) throws Throwable
    {
        ColumnFamilyStore cfs = table();
        populate(40, 2);

        long maxSSTableBytes = 8 * 1024;
        commitThroughFactory(cfs, cursor, leveled(3, maxSSTableBytes, false, false));

        Set<SSTableReader> outputs = cfs.getLiveSSTables();
        assertTrue("scenario must produce multiple outputs to test anything, got " + outputs.size(),
                   outputs.size() >= 2);
        long largest = 0;
        for (SSTableReader sstable : outputs)
        {
            assertEquals("output was not written at the task's level", 3, sstable.getSSTableLevel());
            largest = Math.max(largest, sstable.onDiskLength());
        }
        // The cap is honoured to within one partition, exactly as the iterator path overshoots it.
        assertTrue("an output overshot the cap by more than one partition: " + largest,
                   largest <= maxSSTableBytes * 2);
    }
}
