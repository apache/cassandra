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

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.differential.DifferentialCompactionTester;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * {@code getMergedRowCounts} counts PARTITIONS, not rows: index i holds the number of partitions that
 * were merged from i+1 sources. It reaches {@code compaction_history.rows_merged} and the "total
 * partitions merged" log line, so a cursor path returning row counts would inflate both by the width
 * of every partition.
 * <p>
 * Lives in {@code org.apache.cassandra.db.compaction} for {@link AbstractCompactionPipeline}, which is
 * package-private.
 */
public class MergedPartitionCountsDifferentialTest extends DifferentialCompactionTester
{
    private static final int PARTITIONS = 8;
    private static final int ROWS_PER_PARTITION = 50;

    /** Captures the pipeline's merged counts at the point CompactionTask reads them. */
    private static TaskFactory capturing(AtomicReference<long[]> sink)
    {
        return (cfs, txn, gcBefore) -> new CompactionTask(cfs, txn, gcBefore, false)
        {
            @Override
            protected Collection<SSTableReader> finish(AbstractCompactionPipeline pipeline)
            {
                Collection<SSTableReader> result = super.finish(pipeline);
                sink.set(pipeline.getMergedRowCounts());
                return result;
            }
        };
    }

    private long[] mergedCountsFromOneCompaction(boolean cursor) throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Every partition appears in both sstables, and every row of it in both, so a row count and a
        // partition count differ by exactly ROWS_PER_PARTITION.
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < PARTITIONS; pk++)
                for (long ck = 0; ck < ROWS_PER_PARTITION; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + round + '-' + ck);
            flush();
        }
        assertEquals("the fixture needs two sstables to merge", 2, cfs.getLiveSSTables().size());

        AtomicReference<long[]> sink = new AtomicReference<>();
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        commitThroughFactory(cfs, cursor, capturing(sink));
        long[] counts = sink.get();
        assertNotNull("the task never reported merged counts", counts);
        assertEquals("one counter per input sstable", inputs.size(), counts.length);
        return counts;
    }

    @Test
    public void bothPipelinesCountPartitionsNotRows() throws Exception
    {
        assumeCursorSupportedFormatSelected();

        long[] iterator = mergedCountsFromOneCompaction(false);
        long[] cursor = mergedCountsFromOneCompaction(true);

        // Every partition came from both sources, so the two-source bucket holds them all.
        assertArrayEquals("the iterator path must count partitions, or this says nothing",
                          new long[]{ 0, PARTITIONS }, iterator);
        assertArrayEquals("the cursor path counted " + Arrays.toString(cursor) +
                          " where the iterator path counted " + Arrays.toString(iterator),
                          iterator, cursor);
    }
}
