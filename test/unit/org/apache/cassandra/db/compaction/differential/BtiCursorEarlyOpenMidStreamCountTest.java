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

import java.util.Collection;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.WrappedLifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertTrue;

/**
 * Counts, under BTI, the OpenReason.EARLY readers the cursor compaction path publishes before
 * prepareToCommit (mid-stream) and at prepare time. Uses a self-contained fixture so the count does not
 * depend on the parent's private helpers.
 */
public class BtiCursorEarlyOpenMidStreamCountTest extends DifferentialCompactionTester
{
    private static final int OPEN_INTERVAL_MIB = 1;
    private static final int PARTITIONS_PER_ROUND = 4000;
    private static final int KEY_PADDING = 200;
    private static final int VALUE_PADDING = 300;
    private static final String KEY_PREFIX = "k".repeat(KEY_PADDING);

    private SSTableFormat<?, ?> originalFormat;
    private int originalInterval;

    @Before
    public void selectBtiAndShrinkOpenInterval()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
        originalInterval = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(OPEN_INTERVAL_MIB);
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalInterval);
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    @Test
    public void reportsMidStreamEarlyReaderCountUnderBti() throws Throwable
    {
        ColumnFamilyStore cfs = severalMegabytesInTwoSSTables();

        int[] counts = new int[2]; // [0] = mid-stream EARLY, [1] = prepare-time EARLY
        commitThroughFactory(cfs, true, counting(counts));

        long outputBytes = cfs.getLiveSSTables().stream().mapToLong(SSTableReader::onDiskLength).sum();
        System.out.println("BTI-EARLY-PROBE cursor path: midStream EARLY readers=" + counts[0] +
                           ", prepare-time EARLY readers=" + counts[1] +
                           ", interval MiB=" + OPEN_INTERVAL_MIB +
                           ", output bytes=" + outputBytes);

        assertTrue("BTI published no EARLY reader mid-stream on the cursor path; midStream=" + counts[0] +
                   ", prepare-time=" + counts[1] + ", output bytes=" + outputBytes,
                   counts[0] > 0);
    }

    private ColumnFamilyStore severalMegabytesInTwoSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String valuePadding = "x".repeat(VALUE_PADDING);
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < PARTITIONS_PER_ROUND; pk++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)",
                        KEY_PREFIX + pk, 0L, valuePadding + round);
            flush();
        }
        assertTrue("the fixture needs inputs", cfs.getLiveSSTables().size() >= 2);
        return cfs;
    }

    private static TaskFactory counting(int[] counts)
    {
        return (cfs, txn, gcBefore) -> new CompactionTask(cfs, txn, gcBefore, false)
        {
            @Override
            public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                                  Directories directories,
                                                                  ILifecycleTransaction transaction,
                                                                  Set<SSTableReader> nonExpiredSSTables)
            {
                return new DefaultCompactionWriter(cfs, directories,
                                                   new Counting(transaction, counts),
                                                   nonExpiredSSTables, false, 0);
            }
        };
    }

    private static final class Counting extends WrappedLifecycleTransaction
    {
        private final int[] counts;
        private boolean preparing;

        Counting(ILifecycleTransaction delegate, int[] counts)
        {
            super(delegate);
            this.counts = counts;
        }

        private void tally(SSTableReader reader)
        {
            if (reader.openReason == SSTableReader.OpenReason.EARLY)
                counts[preparing ? 1 : 0]++;
        }

        @Override
        public void prepareToCommit()
        {
            preparing = true;
            super.prepareToCommit();
        }

        @Override
        public void update(SSTableReader reader, boolean original)
        {
            tally(reader);
            super.update(reader, original);
        }

        @Override
        public void update(Collection<SSTableReader> readers, boolean original)
        {
            readers.forEach(this::tally);
            super.update(readers, original);
        }
    }
}
