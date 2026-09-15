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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.WrappedLifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Pins the key an early-opened sstable carries as its {@code last}, on the BIG format: it must be
 *  a stable copy, not a reused key or token the next partition overwrites. */
public class CursorEarlyOpenBoundaryTest extends DifferentialCompactionTester
{
    /** The smallest preemptive-open interval the config accepts. */
    private static final int OPEN_INTERVAL_MIB = 1;

    private SSTableFormat<?, ?> originalFormat;
    private int originalInterval;

    /** The format this scenario runs under. */
    protected String formatName()
    {
        return "big";
    }

    /** True when the preemptive reopen must fire inside the output. */
    protected boolean requiresMidStreamReopen()
    {
        return true;
    }

    @Before
    public void selectBigAndShrinkOpenInterval()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(formatName());
        originalInterval = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(OPEN_INTERVAL_MIB);
    }

    @After
    public void restoreFormatAndOpenInterval()
    {
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalInterval);
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    @Test
    public void earlyOpenedBoundaryIsDetachedOnCursorPath() throws Throwable
    {
        assertBoundaryIsDetached(true);
    }

    /** The same expectation on the iterator path, so a failure above reads as a cursor defect. */
    @Test
    public void earlyOpenedBoundaryIsDetachedOnIteratorPath() throws Throwable
    {
        assertBoundaryIsDetached(false);
    }

    private void assertBoundaryIsDetached(boolean cursor) throws Throwable
    {
        ColumnFamilyStore cfs = severalMegabytesInTwoSSTables();

        List<CapturedBoundary> boundaries = new ArrayList<>();
        commitThroughFactory(cfs, cursor, singleOutputCapturing(boundaries));

        long midStream = boundaries.stream().filter(b -> b.midStream).count();
        if (requiresMidStreamReopen())
            assertTrue("the preemptive reopen never fired inside the output, so this scenario asserted " +
                       "nothing; mid-stream reopens=" + midStream + " of " + boundaries.size() +
                       " early opens, interval MiB=" + OPEN_INTERVAL_MIB + ", output bytes=" +
                       cfs.getLiveSSTables().stream().mapToLong(SSTableReader::onDiskLength).sum(),
                       midStream > 0);

        // @Before forces BIG, so pin what the scenario needs directly
        cfs.getLiveSSTables().forEach(DifferentialCompactionTester::assertOutputFormatIsSelected);

        for (CapturedBoundary boundary : boundaries)
            boundary.assertDetached();

        assertCommittedBoundsAreTheExtremes(cfs);
    }

    /** The committed output must span the whole key range it was given. */
    private void assertCommittedBoundsAreTheExtremes(ColumnFamilyStore cfs)
    {
        List<DecoratedKey> written = everyKeyWritten(cfs);
        DecoratedKey min = written.stream().min(Comparator.naturalOrder()).orElseThrow();
        DecoratedKey max = written.stream().max(Comparator.naturalOrder()).orElseThrow();

        for (SSTableReader output : cfs.getLiveSSTables())
        {
            assertEquals("the committed output's first key is not the lowest key written", min, output.getFirst());
            assertEquals("the committed output's last key is not the highest key written", max, output.getLast());
        }
    }

    /** Records what an early-opened reader carried as its {@code last} when published, holding the
     *  reader so it can be read again after the compaction. */
    private static final class CapturedBoundary
    {
        private final SSTableReader reader;
        private final ByteBuffer keyAtPublication;
        /** False for the reader published at prepare time, which never reopens. */
        private final boolean midStream;
        /** Where the reader could find its own last key when it was published; negative is a miss. */
        private final long positionOfLast;
        /** The reader's first bound. */
        private final ByteBuffer firstAtPublication;
        private final long positionOfFirst;

        CapturedBoundary(SSTableReader reader, boolean midStream)
        {
            this.reader = reader;
            this.midStream = midStream;
            DecoratedKey last = reader.getLast();
            this.keyAtPublication = ByteBufferUtil.clone(last.getKey());
            // updateStats false so the probe does not warm the key cache
            this.positionOfLast = reader.getPosition(last, SSTableReader.Operator.EQ, false);
            DecoratedKey first = reader.getFirst();
            this.firstAtPublication = ByteBufferUtil.clone(first.getKey());
            this.positionOfFirst = reader.getPosition(first, SSTableReader.Operator.EQ, false);
        }

        void assertDetached()
        {
            DecoratedKey last = reader.getLast();

            // the partial sstable must be able to serve the key it claims as its last
            assertTrue("the early-opened sstable cannot find the key it published as its last, so " +
                       "moveStarts trimmed the originals to a key this sstable cannot serve; reads in " +
                       "that window return nothing until the compaction commits",
                       positionOfLast >= 0);

            // first is not derived from the boundary, so it must stay stable and reachable
            assertTrue("the early-opened sstable cannot find the key it published as its first",
                       positionOfFirst >= 0);
            assertEquals("the early-opened sstable's first key changed after publication",
                         firstAtPublication, reader.getFirst().getKey());

            // Catches a retained reusable Token: the key's own bytes and its token disagree.
            assertEquals("the early-opened sstable's last key carries a token that does not belong to " +
                         "its own bytes, so it was built from a reusable token that has since moved; " +
                         "moveStarts trimmed the originals past partitions this sstable cannot serve",
                         reader.getPartitioner().getToken(last.getKey()), last.getToken());

            // catches a retained reusable key buffer, where bytes and token move together
            assertEquals("the early-opened sstable's last key changed after publication, so the " +
                         "boundary retained the writer's reusable key rather than a copy",
                         keyAtPublication, last.getKey());
        }
    }

    /** One output, with the transaction wrapped so every early-opened reader is captured. */
    private static TaskFactory singleOutputCapturing(List<CapturedBoundary> boundaries)
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
                                                   new EarlyOpenCapturing(transaction, boundaries),
                                                   nonExpiredSSTables, false, 0);
            }
        };
    }

    // both files must outgrow a buffer for openEarly to publish a readable boundary; padding the
    // partition key makes Index.db do so
    /** Partitions per flushed sstable. Two rounds, so the compaction has two inputs. */
    private static final int PARTITIONS_PER_ROUND = 4000;
    /** Long enough that Index.db outgrows the index writer's buffer several times over. */
    private static final int KEY_PADDING = 200;
    private static final String KEY_PREFIX = "k".repeat(KEY_PADDING);
    /** Long enough that Data.db outgrows the preemptive open interval several times over. */
    private static final int VALUE_PADDING = 300;

    /** Every partition key the fixture wrote, decorated for comparison. */
    private List<DecoratedKey> everyKeyWritten(ColumnFamilyStore cfs)
    {
        List<DecoratedKey> keys = new ArrayList<>(PARTITIONS_PER_ROUND);
        for (long pk = 0; pk < PARTITIONS_PER_ROUND; pk++)
            keys.add(cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(KEY_PREFIX + pk)));
        return keys;
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

    /** Captures the readers published with {@code OpenReason.EARLY}; delegates everything else. */
    private static final class EarlyOpenCapturing extends WrappedLifecycleTransaction
    {
        private final List<CapturedBoundary> boundaries;
        private boolean preparing;

        EarlyOpenCapturing(ILifecycleTransaction delegate, List<CapturedBoundary> boundaries)
        {
            super(delegate);
            this.boundaries = boundaries;
        }

        private void capture(SSTableReader reader)
        {
            if (reader.openReason == SSTableReader.OpenReason.EARLY)
                boundaries.add(new CapturedBoundary(reader, !preparing));
        }

        /** Marks the prepare phase, so the final EARLY reader published here is not counted as a
         *  mid-stream reopen. */
        @Override
        public void prepareToCommit()
        {
            preparing = true;
            super.prepareToCommit();
        }

        @Override
        public void update(SSTableReader reader, boolean original)
        {
            capture(reader);
            super.update(reader, original);
        }

        @Override
        public void update(Collection<SSTableReader> readers, boolean original)
        {
            readers.forEach(this::capture);
            super.update(readers, original);
        }
    }
}
