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

import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.TimeWindowCompactionTask;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * TWCS on the cursor path, through the real {@link TimeWindowCompactionTask}.
 *
 * TWCS was the one strategy with no cursor coverage. STCS is covered throughout the corpus, LCS by
 * {@link LeveledCompactionDifferentialTest} and UCS by {@link UnifiedCompactionDifferentialTest};
 * nothing drove a {@link TimeWindowCompactionTask} against the cursor writer.
 *
 * What TWCS adds over the others is its dependence on cell timestamps rather than on size or level:
 * it buckets by the sstable's {@code maxTimestamp}, and it sets {@code ignoreOverlaps}, which
 * changes what the controller will purge. Timestamp metadata is written by the compaction path
 * itself, so a cursor-side divergence in {@code minTimestamp} or {@code maxTimestamp} would feed
 * back into the next bucketing decision.
 */
public class TimeWindowCompactionDifferentialTest extends DifferentialCompactionTester
{
    /** The parameter cannot be named keepOriginals; see {@link TaskFactory}. */
    private static TaskFactory timeWindow(boolean ignoreOverlaps, boolean retainOriginals)
    {
        return (cfs, txn, gcBefore) -> new TimeWindowCompactionTask(cfs, txn, gcBefore, ignoreOverlaps)
        {
            @Override
            public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                                  Directories directories,
                                                                  ILifecycleTransaction transaction,
                                                                  Set<SSTableReader> nonExpiredSSTables)
            {
                return new DefaultCompactionWriter(cfs, directories, transaction, nonExpiredSSTables,
                                                   retainOriginals, 0);
            }
        };
    }

    private ColumnFamilyStore twoWindows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} " +
                    "AND compaction = {'class': 'TimeWindowCompactionStrategy', " +
                    "'compaction_window_unit': 'MINUTES', 'compaction_window_size': '1'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Two flushes with explicitly separated timestamps, so the inputs land in different windows
        // and the merged output's timestamp range spans both.
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

    /** Both paths must produce byte-identical output through the TWCS task. */
    @Test
    public void timeWindowTaskMatchesIterator() throws Throwable
    {
        ColumnFamilyStore cfs = twoWindows();
        assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), timeWindow(false, true));
    }

    /**
     * ignoreOverlaps changes what the controller may purge, so it is a distinct scenario rather than
     * a flag on the previous one.
     */
    @Test
    public void timeWindowTaskIgnoringOverlapsMatchesIterator() throws Throwable
    {
        ColumnFamilyStore cfs = twoWindows();
        assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), timeWindow(true, true));
    }

    /**
     * The committed output's timestamp range is what TWCS buckets on next time, so it is asserted
     * absolutely rather than only compared between the paths.
     */
    @Test
    public void committedOutputCarriesTheSpanningTimestampRange() throws Throwable
    {
        ColumnFamilyStore cfs = twoWindows();
        commitThroughFactory(cfs, true, timeWindow(false, false));

        List<SSTableReader> outputs = List.copyOf(cfs.getLiveSSTables());
        assertEquals("expected one compaction output", 1, outputs.size());
        SSTableReader output = outputs.get(0);

        assertEquals("the output's minTimestamp must be the oldest cell it carries",
                     1000000L, output.getSSTableMetadata().minTimestamp);
        assertEquals("the output's maxTimestamp must be the newest cell it carries",
                     90000000L, output.getSSTableMetadata().maxTimestamp);
    }
}
