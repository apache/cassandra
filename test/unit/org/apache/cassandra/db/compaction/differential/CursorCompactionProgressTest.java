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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionTask;

import static org.junit.Assert.assertTrue;

/**
 * Pins that compaction progress advances inside one large partition on the cursor path.
 *
 * {@code CursorCompactor} used to update {@code totalBytesRead} only when a partition header was
 * read or a cursor reset, so {@code nodetool compactionstats} showed nothing moving while a single
 * large partition was merged, and partition N's bytes only landed once partition N+1's header was
 * read. The iterator path refreshes {@code CompactionInfo.bytesRead} every 100 unfiltereds
 * ({@code CompactionIterator}); the cursor path now refreshes every
 * {@code CursorCompactor.UNFILTERED_TO_UPDATE_PROGRESS}.
 *
 * The scenario is one partition with many rows, so a per-partition update produces exactly one
 * distinct intermediate value and this test fails, while a per-unfiltered update produces many.
 *
 * The cadence itself is deliberately not asserted. It is a private constant on both paths and a
 * test that pins it would break on any retuning without anything being wrong.
 */
public class CursorCompactionProgressTest extends DifferentialCompactionTester
{
    /** One partition, enough rows that a per-unfiltered cadence has many chances to fire. */
    private static final int ROWS = 120_000;

    /**
     * The iterator path's granularity is the yardstick, so both paths are measured in one run: the
     * sampler is timing-sensitive and two separate runs would compare counts taken under different
     * machine load.
     *
     * The margin is an order of magnitude, which is far wider than the two cadences differ (128
     * against 100) and far narrower than the defect. Measured on this fixture: with the in-partition
     * refresh the cursor path reports 917 distinct values against the iterator's 1079; with it
     * disabled, 2 against 1663.
     */
    private static final int GRANULARITY_MARGIN = 10;

    @Test
    public void progressAdvancesWithinOnePartitionAsOftenAsTheIteratorPath() throws Throwable
    {
        long iterator = distinctProgressValues(false);
        long cursor = distinctProgressValues(true);

        assertTrue("the iterator path is the yardstick and it reported no progress inside the " +
                   "partition, so this scenario cannot judge the cursor path", iterator > 2);

        assertTrue("compaction progress barely moved inside the partition on the cursor path: " +
                   cursor + " distinct values against the iterator path's " + iterator + ". A reader " +
                   "of nodetool compactionstats would watch one large partition merge with the " +
                   "counter stuck.",
                   cursor >= iterator / GRANULARITY_MARGIN);
    }

    private long distinctProgressValues(boolean cursor) throws Throwable
    {
        ColumnFamilyStore cfs = oneLargePartitionInTwoSSTables();

        SamplingTracker tracker = new SamplingTracker();
        commitThroughFactory(cfs, cursor,
                             (store, txn, gcBefore) -> new CompactionTask(store, txn, gcBefore, false),
                             tracker);

        return tracker.distinctIntermediateValues();
    }

    private ColumnFamilyStore oneLargePartitionInTwoSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "x".repeat(60);
        for (int round = 0; round < 2; round++)
        {
            for (int ck = 0; ck < ROWS; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1, ck + round * ROWS, padding);
            flush();
        }
        assertTrue("the fixture needs inputs", cfs.getLiveSSTables().size() >= 2);
        return cfs;
    }

    /**
     * Samples {@code getCompleted()} from a thread while the compaction runs, and keeps the values
     * that fall strictly between zero and the total. A path that only updates per partition reports
     * one such value for a single-partition compaction; a path that updates per unfiltered reports
     * many.
     */
    private static final class SamplingTracker implements ActiveCompactionsTracker
    {
        private final List<Long> samples = new ArrayList<>();
        private volatile boolean running;
        private volatile long total;
        private Thread sampler;

        @Override
        public void beginCompaction(CompactionInfo.Holder holder)
        {
            total = holder.getCompactionInfo().getTotal();
            running = true;
            sampler = new Thread(() -> {
                while (running)
                {
                    long completed = holder.getCompactionInfo().getCompleted();
                    synchronized (samples)
                    {
                        samples.add(completed);
                    }
                    Thread.onSpinWait();
                }
            }, "compaction-progress-sampler");
            sampler.setDaemon(true);
            sampler.start();
        }

        @Override
        public void finishCompaction(CompactionInfo.Holder holder)
        {
            running = false;
            try
            {
                if (sampler != null)
                    sampler.join(10_000);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }

        /** Sampling can outlive the join timeout, so the lock stays. */
        long distinctIntermediateValues()
        {
            synchronized (samples)
            {
                return samples.stream().filter(v -> v > 0 && v < total).distinct().count();
            }
        }
    }
}
