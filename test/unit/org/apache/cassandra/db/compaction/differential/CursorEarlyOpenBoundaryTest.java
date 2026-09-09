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
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Pins the key an early-opened sstable carries as its {@code last}, on the BIG format.
 *
 * {@code sstable_preemptive_open_interval} publishes a partial reader every so many bytes.
 * {@code BigTableWriter.openEarly} takes that reader's bounds from
 * {@code IndexSummaryBuilder.ReadableBoundary.lastKey}, which the index writer stashes per sampled
 * entry through {@code retainable()}. {@code retainable()} clones the bytes but keeps the caller's
 * {@code Token} object, and returns the caller's key untouched when the buffer is already an exact
 * fit. The cursor path's key and token are both reusable instances that the next partition
 * overwrites, so a boundary that retains either one reports the partition the compaction has
 * reached now, not the flushed boundary.
 *
 * That matters because {@code SSTableRewriter.maybeReopenEarly} calls
 * {@code moveStarts(reader.getLast())}. A key that runs ahead trims the originals past partitions
 * the partial sstable cannot serve yet, and reads in that window return nothing until the
 * compaction commits.
 *
 * BIG only. {@code BtiTableWriter.openEarly} takes first and last from the partition index, and its
 * publication is deferred until the data, row index and partition index writers have all flushed
 * past the recorded ends, so it does not reach this boundary at all.
 */
public class CursorEarlyOpenBoundaryTest extends DifferentialCompactionTester
{
    /** The smallest interval the config accepts. The scenario writes several times this. */
    private static final int OPEN_INTERVAL_MIB = 1;

    private SSTableFormat<?, ?> originalFormat;
    private int originalInterval;

    /** The format this scenario runs under. */
    protected String formatName()
    {
        return "big";
    }

    /**
     * True when the preemptive reopen must fire inside the output. BIG publishes synchronously from
     * BigTableWriter.openEarly. BTI defers publication through PartitionIndexBuilder.buildPartial
     * until the data, row index and partition index writers have all flushed past the recorded ends,
     * and openFinalEarly cancels whatever is still pending, so zero is a legitimate outcome there.
     */
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

        // Not assumeCursorSupportedFormatSelected: @Before forces BIG, which always supports the
        // cursor path, so that guard can never fire. This pins what the scenario actually needs.
        cfs.getLiveSSTables().forEach(DifferentialCompactionTester::assertOutputFormatIsSelected);

        for (CapturedBoundary boundary : boundaries)
            boundary.assertDetached();

        assertCommittedBoundsAreTheExtremes(cfs);
    }

    /**
     * The committed output must span the whole key range it was given.
     *
     * On BTI this is where a regressed key shows first: PartitionIndexBuilder holds firstKey and the
     * previous key across addEntry to compute each separator, and writes both bounds into the
     * Partitions.db footer. Handed a key the next partition overwrites, both bounds collapse onto the
     * final partition and every separator is derived from a key that has since moved.
     */
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

    /**
     * Records what an early-opened reader carried as its {@code last} at the moment it was
     * published, and holds the reader so the same field can be read again after the compaction.
     */
    private static final class CapturedBoundary
    {
        private final SSTableReader reader;
        private final ByteBuffer keyAtPublication;
        private final Token tokenAtPublication;
        private final IPartitioner partitioner;
        /** False for the reader openFinalEarly publishes at prepare time, which never reopens. */
        private final boolean midStream;
        /** Where the reader could find its own last key when it was published; negative is a miss. */
        private final long positionOfLast;
        /** The reader's other bound, which nothing else in the tree asserts. */
        private final ByteBuffer firstAtPublication;
        private final long positionOfFirst;

        CapturedBoundary(SSTableReader reader, boolean midStream)
        {
            this.reader = reader;
            this.midStream = midStream;
            this.partitioner = reader.getPartitioner();
            DecoratedKey last = reader.getLast();
            this.keyAtPublication = ByteBufferUtil.clone(last.getKey());
            this.tokenAtPublication = partitioner.getToken(keyAtPublication);
            // Taken here, one line before SSTableRewriter.maybeReopenEarly hands this same key to
            // moveStarts. updateStats false so the probe does not warm the key cache.
            this.positionOfLast = reader.getPosition(last, SSTableReader.Operator.EQ, false);
            DecoratedKey first = reader.getFirst();
            this.firstAtPublication = ByteBufferUtil.clone(first.getKey());
            this.positionOfFirst = reader.getPosition(first, SSTableReader.Operator.EQ, false);
        }

        void assertDetached()
        {
            DecoratedKey last = reader.getLast();

            // Absolute, not a consistency check: the partial sstable must be able to serve the key it
            // claims as its last, because moveStarts trims the originals to exactly that key. A
            // boundary that ran ahead sits past the reader's own indexLength override and misses here.
            assertTrue("the early-opened sstable cannot find the key it published as its last, so " +
                       "moveStarts trimmed the originals to a key this sstable cannot serve; reads in " +
                       "that window return nothing until the compaction commits",
                       positionOfLast >= 0);

            // ReadableBoundary also carries indexLength, dataLength, summaryCount and entriesLength,
            // which become this reader's length overrides. first is not derived from the boundary at
            // all, so it must be both stable and reachable under those overrides.
            assertTrue("the early-opened sstable cannot find the key it published as its first",
                       positionOfFirst >= 0);
            assertEquals("the early-opened sstable's first key changed after publication",
                         firstAtPublication, reader.getFirst().getKey());

            // Catches a retained reusable Token: the key's own bytes and its token disagree.
            assertEquals("the early-opened sstable's last key carries a token that does not belong to " +
                         "its own bytes, so it was built from a reusable token that has since moved; " +
                         "moveStarts trimmed the originals past partitions this sstable cannot serve",
                         partitioner.getToken(last.getKey()), last.getToken());

            // Catches a retained reusable key buffer, where bytes and token move together and so
            // agree with each other while both describe the wrong partition.
            assertEquals("the early-opened sstable's last key changed after publication, so the " +
                         "boundary retained the writer's reusable key rather than a copy",
                         keyAtPublication, last.getKey());
            assertEquals("the early-opened sstable's last token changed after publication, so the " +
                         "boundary retained the writer's reusable token rather than a copy",
                         tokenAtPublication, last.getToken());
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

    /** Partitions per flushed sstable. Two rounds, so the compaction has two inputs. */
    private static final int PARTITIONS_PER_ROUND = 4000;
    /** Long enough that Index.db outgrows the index writer's buffer several times over. */
    private static final int KEY_PADDING = 200;
    /** Long enough that Data.db outgrows the preemptive open interval several times over. */
    private static final int VALUE_PADDING = 300;

    /**
     * Both files must outgrow a buffer, not just Data.db.
     * <p>
     * {@code IndexSummaryBuilder.refreshReadableBoundary} takes the lower of the boundaries below
     * the data and the index sync positions, and a sync position only advances when that writer
     * flushes a buffer. A table with short partition keys writes a Index.db smaller than one
     * buffer however large Data.db grows, so the index sync position stays at zero, no boundary is
     * ever readable, and {@code openEarly} publishes nothing on either path. Padding the partition
     * key is what makes this scenario exercise the reopen at all.
     */
    /** Every partition key the fixture wrote, decorated for comparison. */
    private List<DecoratedKey> everyKeyWritten(ColumnFamilyStore cfs)
    {
        String keyPadding = "k".repeat(KEY_PADDING);
        List<DecoratedKey> keys = new ArrayList<>(PARTITIONS_PER_ROUND);
        for (long pk = 0; pk < PARTITIONS_PER_ROUND; pk++)
            keys.add(cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(keyPadding + pk)));
        return keys;
    }

    private ColumnFamilyStore severalMegabytesInTwoSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String keyPadding = "k".repeat(KEY_PADDING);
        String valuePadding = "x".repeat(VALUE_PADDING);
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < PARTITIONS_PER_ROUND; pk++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)",
                        keyPadding + pk, 0L, valuePadding + round);
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

        /**
         * SSTableRewriter.switchWriter publishes one last EARLY reader here through openFinalEarly.
         * Its last comes from SSTableWriter.setLast, which was already a copy before this change, so
         * counting it as a reopen would let the scenario pass with no reopen at all.
         */
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
