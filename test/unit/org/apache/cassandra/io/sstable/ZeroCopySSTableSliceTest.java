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
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compression.CompressionDictionaryManager;
import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
import org.apache.cassandra.db.compression.TrainingState;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSlice.Plan;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSlice.Reason;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.OutputHandler;

import static org.apache.cassandra.Util.spinUntilTrue;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * {@link ZeroCopySSTableSlice} produces the components of an sstable it does not have the Data.db of. Every test
 * here therefore MATERIALISES the slice -- copies the planned byte range of the parent's Data.db into place, which
 * is exactly what the receiving node does with the bytes off the wire -- and then asserts against an ordinary
 * {@link SSTableReader} opened on the result.
 *
 * <p>The load bearing assertions are that the materialised slice holds exactly the requested partitions and
 * nothing else, that the partitions read back identical to the parent's, and that the dead space (the head of the
 * first compression chunk, and anything between sections less than a chunk apart) is unreachable rather than
 * merely unlikely to be reached.
 */
public class ZeroCopySSTableSliceTest extends CQLTester
{
    /**
     * The core test: one range out of the middle of an sstable, which is the shape a subrange repair or a
     * decommission produces.
     */
    @Test
    public void middleRangeSliceReadsBackExactly() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertTrue(parent.compression);
        assertEquals(4096, parent.getCompressionMetadata().chunkLength());

        List<DecoratedKey> keys = keysInOrder(parent);
        assertEquals(80, keys.size());

        // (keys[19], keys[59]] -- Range excludes its left endpoint, so 40 partitions.
        List<PartitionPositionBounds> sections = positionsFor(parent, keys.get(19), keys.get(59));
        assertEquals(1, sections.size());
        List<DecoratedKey> expected = keys.subList(20, 60);

        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 0.25);
        assertTrue(plan.toString(), plan.isEligible());
        assertEquals(sections.get(0).upperPosition - sections.get(0).lowerPosition, plan.usefulBytes);
        // The slice does not begin on a chunk boundary here, so it carries a dead prefix and only that.
        assertEquals(sections.get(0).lowerPosition % 4096, plan.deadBytes);
        assertTrue("expected a dead prefix to exercise", plan.deadBytes > 0);

        Materialised materialised = materialise(cfs, parent, plan);
        try
        {
            SSTableReader slice = materialised.reader;

            assertEquals(expected.size(), materialised.slice.partitionCount);
            assertEquals(expected.get(0), slice.getFirst());
            assertEquals(expected.get(expected.size() - 1), slice.getLast());

            // The uncompressed length includes the dead prefix, which is what makes the rebased index positions
            // land where they do; the physical length is the chunk run and nothing more.
            assertEquals(plan.dataLength, slice.uncompressedLength());
            assertEquals(plan.physicalBytes, slice.onDiskLength());
            assertEquals(parent.getCompressionMetadata().chunkLength(),
                         slice.getCompressionMetadata().chunkLength());

            assertContentMatches(parent, slice, expected);
            assertOnlyTheseKeysArePresent(slice, keys, expected);
        }
        finally
        {
            materialised.close();
        }
    }

    /**
     * A slice that starts at the very first partition has no dead prefix at all, so its {@code offsets[0]} is 0
     * and its first partition is at data position 0 -- the ordinary sstable shape, reached by a different route.
     */
    @Test
    public void prefixSliceHasNoDeadSpace() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(60, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(parent);

        List<PartitionPositionBounds> sections = parent.getPositionsForRanges(
            Collections.singletonList(new Range<>(parent.getPartitioner().getMinimumToken(), keys.get(29).getToken())));
        assertEquals(1, sections.size());
        assertEquals(0, sections.get(0).lowerPosition);

        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 0.0);
        assertTrue("a slice from position 0 has no dead space at all: " + plan, plan.isEligible());
        assertEquals(0, plan.deadBytes);

        Materialised materialised = materialise(cfs, parent, plan);
        try
        {
            assertEquals(keys.get(0), materialised.reader.getFirst());
            assertEquals(keys.get(29), materialised.reader.getLast());
            assertContentMatches(parent, materialised.reader, keys.subList(0, 30));
            assertOnlyTheseKeysArePresent(materialised.reader, keys, keys.subList(0, 30));
        }
        finally
        {
            materialised.close();
        }
    }

    /**
     * A slice reaching the end of a COMPACTION-produced parent, which is the shape production sstables actually
     * have and the one that hides a silent corruption.
     *
     * <p>{@code SSTableRewriter.doPrepare} syncs the data file twice, and {@code CompressedSequentialWriter}
     * appends a chunk unconditionally on each, so such an sstable carries a trailing zero-uncompressed-length
     * chunk past its last real one. A slice that took its end from the physical file length rather than from its
     * last chunk would copy that slack, and the receiver -- which derives the last chunk's compressed length as
     * {@code compressedFileLength - offsets[C-1] - 4} -- would read the final chunk with an inflated length: a
     * CRC failure, or worse, compressed bytes handed back as row data once the length crossed
     * {@code maxCompressedLength}. Nothing upstream would notice, because the digest is computed over whatever
     * was written.
     *
     * <p>Two things have to be arranged for the slack to exist at all: {@code sstable_preemptive_open_interval}
     * has to be set, or {@code switchWriter(null)} never triggers the second sync; and the parent has to be
     * REOPENED, because {@code CompressionMetadata.Writer.open} trims the offsets table and resets
     * {@code compressedLength}, so the reader compaction hands back cannot see the trailing chunk. A streaming
     * sender on a node that has restarted since the compaction is looking at the untrimmed view.
     *
     * <p>Setting the interval is redundant on trunk -- {@code Config.sstable_preemptive_open_interval} already
     * defaults to 50MiB and {@code test/conf/cassandra.yaml} does not override it -- but it is kept explicit so
     * the test does not silently stop covering the slack if that default ever changes. The
     * {@code assertTrue(... srcEnd < dataFileLength)} below is the guard that would catch it.
     */
    @Test
    public void sliceToTheEndOfACompactedParentExcludesTrailingSlack() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        int previousInterval = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        SSTableReader parent = null;
        try
        {
            // What conf/cassandra.yaml ships, and what Config already defaults to on trunk.
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(50);

            createCompressedTable(4);
            disableCompaction();
            insertPartitions(60, 4, 400);
            flush();
            insertPartitions(60, 4, 400);
            flush();

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            assertEquals("need two sstables to have something to compact", 2, cfs.getLiveSSTables().size());
            cfs.forceMajorCompaction();
            SSTableReader compacted = onlySSTable(cfs);
            parent = SSTableReader.open(cfs, compacted.descriptor, compacted.getComponents(), cfs.metadata);

            List<DecoratedKey> keys = keysInOrder(parent);
            long dataFileLength = parent.descriptor.fileFor(Components.DATA).length();
            assertEquals("the parent must be the on-disk view, whose length includes the trailing chunk",
                         dataFileLength, parent.getCompressionMetadata().compressedFileLength);

            // (keys[19], +infinity]: the section's upper bound is the parent's uncompressedLength.
            List<PartitionPositionBounds> sections = parent.getPositionsForRanges(
                Collections.singletonList(new Range<>(keys.get(19).getToken(),
                                                     parent.getPartitioner().getMinimumToken())));
            assertEquals(1, sections.size());
            assertEquals(parent.uncompressedLength(), sections.get(0).upperPosition);

            Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 0.25);
            assertTrue(plan.toString(), plan.isEligible());

            // Guard the guard: without the slack this test silently stops covering what it is named for.
            assertTrue("expected a compaction-produced parent to carry trailing slack past its last data chunk",
                       onlyRun(plan).srcEnd < dataFileLength);

            Materialised materialised = materialise(cfs, parent, plan);
            try
            {
                List<DecoratedKey> expected = keys.subList(20, keys.size());
                assertEquals(keys.get(keys.size() - 1), materialised.reader.getLast());
                // Reads every partition to the last byte of the last chunk, which is what an inflated final
                // chunk length breaks.
                assertContentMatches(parent, materialised.reader, expected);
                assertOnlyTheseKeysArePresent(materialised.reader, keys, expected);
            }
            finally
            {
                materialised.close();
            }
        }
        finally
        {
            if (parent != null)
                parent.selfRef().release();
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(previousInterval);
        }
    }

    /**
     * Two ranges less than a compression chunk apart still make ONE run, and the partitions between them come
     * along inside the boundary chunks. They must be physically present and completely unreachable: absent from
     * the index, the summary and the filter, and skipped by every scan.
     */
    @Test
    public void interiorGapPartitionsAreCarriedButUnreachable() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(64);   // one 64 KiB chunk holds many partitions, so a gap cannot split the run
        disableCompaction();
        insertPartitions(60, 4, 300);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(parent);

        // (keys[9], keys[24]] and (keys[29], keys[49]]: keys[25..29] are the gap.
        List<PartitionPositionBounds> sections = new ArrayList<>();
        sections.addAll(positionsFor(parent, keys.get(9), keys.get(24)));
        sections.addAll(positionsFor(parent, keys.get(29), keys.get(49)));
        assertEquals(2, sections.size());

        int chunkLength = parent.getCompressionMetadata().chunkLength();
        assertTrue("the sections must be closer than a chunk for this to be one run",
                   isSingleRun(sections, chunkLength));

        List<DecoratedKey> expected = new ArrayList<>();
        expected.addAll(keys.subList(10, 25));
        expected.addAll(keys.subList(30, 50));

        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 1.0);
        assertTrue(plan.toString(), plan.isEligible());
        long gap = sections.get(1).lowerPosition - sections.get(0).upperPosition;
        assertTrue("expected a real interior gap", gap > 0);
        assertEquals(plan.deadBytes, gap + sections.get(0).lowerPosition % chunkLength);

        Materialised materialised = materialise(cfs, parent, plan);
        try
        {
            assertEquals(expected.size(), materialised.slice.partitionCount);
            assertContentMatches(parent, materialised.reader, expected);
            assertOnlyTheseKeysArePresent(materialised.reader, keys, expected);

            // The gap's bytes really are in the file: the slice is longer than the partitions it exposes.
            assertTrue(materialised.reader.uncompressedLength() > plan.usefulBytes);
        }
        finally
        {
            materialised.close();
        }
    }

    /**
     * Sections a whole cell or more apart become SEPARATE runs, and the cells between them are not sent at all.
     * The slice is those ranges concatenated, which works because cell ordinals stay consecutive across the join:
     * every run but the last contributes whole cells, so the grid the index and CompressionInfo.db are addressed
     * against survives.
     *
     * <p>This is the case a single-run slice has to fall back for, and the one whose arithmetic is not the
     * splitter's: each run gets its own rebase.
     */
    @Test
    public void separateRunsAreConcatenatedAndSkipWhatIsBetweenThem() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(120, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(parent);

        // Three ranges with wide gaps: (keys[9], keys[29]], (keys[59], keys[79]], (keys[99], keys[114]].
        List<PartitionPositionBounds> sections = new ArrayList<>();
        sections.addAll(positionsFor(parent, keys.get(9), keys.get(29)));
        sections.addAll(positionsFor(parent, keys.get(59), keys.get(79)));
        sections.addAll(positionsFor(parent, keys.get(99), keys.get(114)));
        assertEquals(3, sections.size());

        int chunkLength = parent.getCompressionMetadata().chunkLength();
        assertEquals("the gaps are far wider than a chunk, so this is three runs",
                     3, ZeroCopySSTableSlice.runCount(sections, chunkLength));

        List<DecoratedKey> expected = new ArrayList<>();
        expected.addAll(keys.subList(10, 30));
        expected.addAll(keys.subList(60, 80));
        expected.addAll(keys.subList(100, 115));

        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 0.25);
        assertTrue(plan.toString(), plan.isEligible());
        assertEquals(3, plan.runs.size());

        // What the runs say about themselves: consecutive cell ordinals, contiguous physical bases, per-run
        // rebases, and nothing overlapping in the parent.
        long cellBase = 0;
        long physicalBase = 0;
        for (int r = 0; r < plan.runs.size(); r++)
        {
            ZeroCopySSTableSlice.Run run = plan.runs.get(r);
            assertEquals("run " + r + " child cell base", cellBase, run.childCellBase);
            assertEquals("run " + r + " child physical base", physicalBase, run.childPhysicalBase);
            assertEquals("run " + r + " shift", (run.firstCell - run.childCellBase) * chunkLength, run.shift);
            if (r > 0)
                assertTrue("run " + r + " must start past the previous one",
                           run.firstCell > plan.runs.get(r - 1).lastCell);
            cellBase += run.cellCount();
            physicalBase += run.physicalBytes();
        }
        assertEquals(plan.cellCount(), cellBase);
        assertEquals(plan.physicalBytes, physicalBase);

        // The whole point: the bytes between the runs are never sent.
        long span = sections.get(2).upperPosition - sections.get(0).lowerPosition;
        assertTrue("a three-run slice must be materially smaller than the span it covers",
                   plan.dataLength < span);

        Materialised materialised = materialise(cfs, parent, plan);
        try
        {
            assertEquals(expected.size(), materialised.slice.partitionCount);
            assertEquals(expected.get(0), materialised.reader.getFirst());
            assertEquals(expected.get(expected.size() - 1), materialised.reader.getLast());
            assertEquals(plan.dataLength, materialised.reader.uncompressedLength());
            assertEquals(plan.physicalBytes, materialised.reader.onDiskLength());
            assertContentMatches(parent, materialised.reader, expected);
            assertOnlyTheseKeysArePresent(materialised.reader, keys, expected);
        }
        finally
        {
            materialised.close();
        }
    }

    /**
     * Runs and interior gaps at the same time: two runs, each made of two sections that are closer together than a
     * chunk. Both kinds of dead space in one slice, and both kinds of rebase.
     */
    @Test
    public void runsAndInteriorGapsTogether() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(16);
        disableCompaction();
        insertPartitions(160, 2, 300);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(parent);
        int chunkLength = parent.getCompressionMetadata().chunkLength();

        List<PartitionPositionBounds> sections = new ArrayList<>();
        sections.addAll(positionsFor(parent, keys.get(4), keys.get(14)));    // run 0, section 0
        sections.addAll(positionsFor(parent, keys.get(16), keys.get(29)));   // run 0, section 1 (small gap)
        sections.addAll(positionsFor(parent, keys.get(99), keys.get(119)));  // run 1, section 0
        sections.addAll(positionsFor(parent, keys.get(121), keys.get(139))); // run 1, section 1 (small gap)
        assertEquals(4, sections.size());
        assertEquals(2, ZeroCopySSTableSlice.runCount(sections, chunkLength));

        List<DecoratedKey> expected = new ArrayList<>();
        expected.addAll(keys.subList(5, 15));
        expected.addAll(keys.subList(17, 30));
        expected.addAll(keys.subList(100, 120));
        expected.addAll(keys.subList(122, 140));

        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 1.0);
        assertTrue(plan.toString(), plan.isEligible());
        assertEquals(2, plan.runs.size());
        assertTrue("expected dead space from both the prefixes and the interior gaps", plan.deadBytes > 0);

        Materialised materialised = materialise(cfs, parent, plan);
        try
        {
            assertEquals(expected.size(), materialised.slice.partitionCount);
            assertContentMatches(parent, materialised.reader, expected);
            assertOnlyTheseKeysArePresent(materialised.reader, keys, expected);
        }
        finally
        {
            materialised.close();
        }
    }

    /**
     * Verifier walks Data.db by seeking to each next index position, and Scrubber walks it linearly. Both must
     * accept a slice whose data has holes in the middle, and Scrubber must recover every partition without
     * reporting one as bad.
     */
    @Test
    public void verifierAndScrubberAcceptASliceWithAnInteriorGap() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(64);
        disableCompaction();
        insertPartitions(40, 4, 300);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(parent);

        List<PartitionPositionBounds> sections = new ArrayList<>();
        sections.addAll(positionsFor(parent, keys.get(4), keys.get(14)));
        sections.addAll(positionsFor(parent, keys.get(19), keys.get(34)));

        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 1.0);
        assertTrue(plan.toString(), plan.isEligible());
        assertTrue(plan.deadBytes > 0);

        Materialised materialised = materialise(cfs, parent, plan);
        int partitionCount = materialised.slice.partitionCount;
        SSTableReader slice = materialised.reader;
        boolean consumedByTxn = false;
        try
        {
            // No Digest.crc32 is produced for a slice, so this is a full extended verification either way.
            verify(cfs, slice);

            consumedByTxn = true;
            IScrubber.ScrubResult result;
            try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.SCRUB, slice);
                 IScrubber scrubber = scrubber(cfs, slice, txn))
            {
                result = scrubber.scrubWithResult();
            }
            assertEquals(partitionCount, result.goodPartitions);
            assertEquals(0, result.badPartitions);
            assertEquals(0, result.emptyPartitions);
        }
        finally
        {
            if (!consumedByTxn)
                materialised.close();
            else
                materialised.deleteFiles();
            LifecycleTransaction.waitForDeletions();
        }
    }

    /** A range narrow enough to sit inside a couple of chunks is mostly dead space, and is refused for it. */
    @Test
    public void refusesWhenDeadSpaceExceedsTheRatio() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(64);
        disableCompaction();
        insertPartitions(80, 4, 400);
        flush();

        SSTableReader parent = onlySSTable(getCurrentColumnFamilyStore());
        List<DecoratedKey> keys = keysInOrder(parent);

        // One partition out of a 64 KiB chunk: the run is the whole chunk, the partition is a fraction of it.
        List<PartitionPositionBounds> sections = positionsFor(parent, keys.get(40), keys.get(41));
        Plan refused = ZeroCopySSTableSlice.plan(parent, sections, 0.25);
        assertEquals(Reason.DEAD_SPACE, refused.reason);

        // ... and the same sections are accepted when nothing is being bounded, which is what proves the refusal
        // was the ratio and not the shape.
        Plan accepted = ZeroCopySSTableSlice.plan(parent, sections, 1.0);
        assertTrue(accepted.isEligible());
        assertTrue("expected mostly dead space, got " + accepted, accepted.deadRatio() > 0.25);
    }

    /**
     * Trunk-only refusal (CEP-49): the parent's chunks were compressed against a trained dictionary, which the
     * slice's own CompressionInfo.db has no proven round trip for yet, so the whole shape is declined rather than
     * risk handing the receiver undecompressible bytes. Mirrors {@code ZeroCopySSTableSplitter.isSupported()}.
     */
    @Test
    public void refusesADictionaryCompressedParent() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        Config.FlushCompression previousFlush = DatabaseDescriptor.getFlushCompression();
        try
        {
            // Otherwise the flush writes LZ4 and the sstable never sees the dictionary compressor at all.
            DatabaseDescriptor.setFlushCompression(Config.FlushCompression.table);

            createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) WITH compression = " +
                        "{'class': 'ZstdDictionaryCompressor', 'chunk_length_in_kb': '4'}");
            disableCompaction();

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            // Highly repetitive payload, which is what a dictionary is trained on. The trainer samples from
            // sstables, so this has to be enough rows across enough files to reach the sample size -- the
            // periodic flush is load-bearing, not incidental (cf. CompressionDictionaryIntegrationTest).
            for (int p = 0; p < 1000; p++)
            {
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", String.format("k%06d", p), 0,
                        "the quick brown fox jumps over the lazy dog, and does so repeatedly " + (p % 7));
                if (p % 200 == 0)
                    flush();
            }
            flush();

            CompressionDictionaryManager dictionaries = cfs.compressionDictionaryManager();
            dictionaries.train(true, Map.of(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME, "10KiB",
                                            TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME, "128KiB"));
            // Wait on the training state first: if training fails this reports WHY, instead of timing out on
            // getCurrent() with no explanation.
            spinUntilTrue(() -> TrainingState.fromCompositeData(dictionaries.getTrainingState()).status
                                == TrainingStatus.COMPLETED, 30);
            spinUntilTrue(() -> dictionaries.getCurrent() != null, 5);

            // Only an sstable written AFTER the dictionary exists carries it, so the second flush is the parent.
            Set<SSTableReader> beforeSecondFlush = new HashSet<>(cfs.getLiveSSTables());
            for (int p = 400; p < 800; p++)
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", String.format("k%06d", p), 0,
                        "the quick brown fox jumps over the lazy dog, and does so repeatedly " + (p % 7));
            flush();

            Set<SSTableReader> added = new HashSet<>(cfs.getLiveSSTables());
            added.removeAll(beforeSecondFlush);
            assertEquals("expected the second flush to produce exactly one sstable", 1, added.size());
            SSTableReader parent = added.iterator().next();
            assertNotNull("the fixture did not produce a dictionary-compressed sstable, so this test proves nothing",
                          parent.getCompressionMetadata().compressionDictionary());

            List<PartitionPositionBounds> sections = parent.getPositionsForRanges(
                Collections.singletonList(new Range<>(parent.getPartitioner().getMinimumToken(),
                                                      parent.getPartitioner().getMinimumToken())));
            Plan refused = ZeroCopySSTableSlice.plan(parent, sections, 1.0);
            assertEquals(Reason.COMPRESSION_DICTIONARY, refused.reason);
            assertFalse(refused.isEligible());
        }
        finally
        {
            DatabaseDescriptor.setFlushCompression(previousFlush);
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Uncompressed sstables: the grid is CRC.db's, and a cell CAN be cut
    // ----------------------------------------------------------------------------------------------------

    /**
     * An uncompressed slice. The grid is the chunk size in CRC.db's header rather than a compression chunk length,
     * physical and uncompressed positions are the same bytes, and the last cell is cut exactly at the last live
     * byte -- so there is no dead suffix, at the price of recomputing that one cell's CRC.
     */
    @Test
    public void uncompressedSliceReadsBackExactly() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        // CRC.db's grid is 64 KiB, so an uncompressed sstable has to be a good deal larger than a compressed one
        // before a slice of it is mostly live data. This one is sized to pass the DEFAULT dead space ratio.
        createUncompressedTable();
        disableCompaction();
        insertPartitions(400, 4, 500);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertFalse(parent.compression);
        assertTrue("an uncompressed sstable must have a CRC.db to slice",
                   parent.descriptor.fileFor(Components.CRC).exists());

        List<DecoratedKey> keys = keysInOrder(parent);
        List<PartitionPositionBounds> sections = positionsFor(parent, keys.get(99), keys.get(349));
        List<DecoratedKey> expected = keys.subList(100, 350);

        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 0.25);
        assertTrue(plan.toString(), plan.isEligible());
        assertFalse(plan.compressed);
        assertEquals("physical and uncompressed are the same bytes here", plan.dataLength, plan.physicalBytes);
        assertEquals(sections.get(0).lowerPosition % plan.cellLength, plan.deadBytes);
        assertTrue("expected a dead prefix to exercise", plan.deadBytes > 0);

        Materialised materialised = materialise(cfs, parent, plan);
        try
        {
            SSTableReader slice = materialised.reader;
            assertTrue(materialised.slice.components.contains(Components.CRC));
            assertFalse(materialised.slice.components.contains(Components.COMPRESSION_INFO));

            assertEquals(expected.size(), materialised.slice.partitionCount);
            assertEquals(expected.get(0), slice.getFirst());
            assertEquals(expected.get(expected.size() - 1), slice.getLast());
            assertEquals(plan.dataLength, slice.uncompressedLength());
            assertEquals(plan.dataLength, slice.descriptor.fileFor(Components.DATA).length());

            assertContentMatches(parent, slice, expected);
            assertOnlyTheseKeysArePresent(slice, keys, expected);
            assertCrcValidates(slice);
        }
        finally
        {
            materialised.close();
        }
    }

    /** Several ranges of an uncompressed sstable: separate runs, and CRC.db sliced across the join. */
    @Test
    public void uncompressedMultiRunSliceReadsBackExactly() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        // A gap has to exceed a whole 64 KiB cell to become a second run, so this needs a sstable of some size
        // and ranges that are genuinely far apart.
        createUncompressedTable();
        disableCompaction();
        insertPartitions(800, 4, 500);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(parent);

        List<PartitionPositionBounds> sections = new ArrayList<>();
        sections.addAll(positionsFor(parent, keys.get(49), keys.get(149)));
        sections.addAll(positionsFor(parent, keys.get(399), keys.get(499)));
        sections.addAll(positionsFor(parent, keys.get(699), keys.get(799)));

        List<DecoratedKey> expected = new ArrayList<>();
        expected.addAll(keys.subList(50, 150));
        expected.addAll(keys.subList(400, 500));
        expected.addAll(keys.subList(700, 800));

        // One dead prefix of up to a 64 KiB cell PER RUN, which dominates an sstable this size; the ratio itself
        // is covered by refusesWhenDeadSpaceExceedsTheRatio.
        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 1.0);
        assertTrue(plan.toString(), plan.isEligible());
        assertTrue("expected more than one run, got " + plan, plan.runs.size() > 1);
        assertEquals(plan.dataLength, plan.physicalBytes);

        Materialised materialised = materialise(cfs, parent, plan);
        try
        {
            assertEquals(expected.size(), materialised.slice.partitionCount);
            assertContentMatches(parent, materialised.reader, expected);
            assertOnlyTheseKeysArePresent(materialised.reader, keys, expected);
            assertCrcValidates(materialised.reader);
        }
        finally
        {
            materialised.close();
        }
    }

    /**
     * A slice whose last live byte lands exactly on a cell boundary needs no CRC recomputed, so its CRC.db is
     * every entry verbatim. Reached by taking the slice to the end of the parent, whose final cell is the parent's
     * own final cell.
     */
    @Test
    public void uncompressedSliceToTheEndKeepsEveryCrcVerbatim() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createUncompressedTable();
        disableCompaction();
        insertPartitions(80, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(parent);

        List<PartitionPositionBounds> sections = parent.getPositionsForRanges(
            Collections.singletonList(new Range<>(keys.get(39).getToken(), parent.getPartitioner().getMinimumToken())));
        assertEquals(parent.uncompressedLength(), sections.get(0).upperPosition);

        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 1.0);
        assertTrue(plan.toString(), plan.isEligible());

        Materialised materialised = materialise(cfs, parent, plan);
        try
        {
            List<DecoratedKey> expected = keys.subList(40, keys.size());
            assertEquals(keys.get(keys.size() - 1), materialised.reader.getLast());
            assertContentMatches(parent, materialised.reader, expected);
            assertCrcValidates(materialised.reader);
        }
        finally
        {
            materialised.close();
        }
    }

    /** Verifier and Scrubber over an uncompressed slice, which is a differently shaped file to a compressed one. */
    @Test
    public void verifierAndScrubberAcceptAnUncompressedSlice() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createUncompressedTable();
        disableCompaction();
        insertPartitions(60, 4, 300);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(parent);

        List<PartitionPositionBounds> sections = new ArrayList<>();
        sections.addAll(positionsFor(parent, keys.get(9), keys.get(29)));
        sections.addAll(positionsFor(parent, keys.get(39), keys.get(54)));

        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 1.0);
        assertTrue(plan.toString(), plan.isEligible());

        Materialised materialised = materialise(cfs, parent, plan);
        int partitionCount = materialised.slice.partitionCount;
        boolean consumedByTxn = false;
        try
        {
            verify(cfs, materialised.reader);

            consumedByTxn = true;
            IScrubber.ScrubResult result;
            try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.SCRUB, materialised.reader);
                 IScrubber scrubber = scrubber(cfs, materialised.reader, txn))
            {
                result = scrubber.scrubWithResult();
            }
            assertEquals(partitionCount, result.goodPartitions);
            assertEquals(0, result.badPartitions);
            assertEquals(0, result.emptyPartitions);
        }
        finally
        {
            if (!consumedByTxn)
                materialised.close();
            else
                materialised.deleteFiles();
            LifecycleTransaction.waitForDeletions();
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // NOTE: the fork's runAndDeadSpaceArithmetic() lived here. It needs no CFS, no schema and no disk, so it
    // was lifted verbatim into ZeroCopySSTableSliceArithmeticTest during the port to trunk.
    // ----------------------------------------------------------------------------------------------------

    // ----------------------------------------------------------------------------------------------------
    // Materialising a slice: what the receiving node does with the bytes
    // ----------------------------------------------------------------------------------------------------

    private static final class Materialised implements AutoCloseable
    {
        final ZeroCopySSTableSlice.Slice slice;
        final SSTableReader reader;
        final Set<Component> components;

        Materialised(ZeroCopySSTableSlice.Slice slice, SSTableReader reader, Set<Component> components)
        {
            this.slice = slice;
            this.reader = reader;
            this.components = components;
        }

        void deleteFiles()
        {
            ZeroCopySSTableSlice.delete(slice.descriptor, components);
        }

        @Override
        public void close()
        {
            reader.selfRef().release();
            deleteFiles();
        }
    }

    /**
     * Synthesise the slice's components, then copy the planned byte range of the parent's Data.db beside them and
     * open the lot. The copy is byte for byte what {@code CassandraEntireSSTableStreamWriter} sends and
     * {@code BigTableZeroCopyWriter} writes, so a reader that works here works there.
     */
    private static Materialised materialise(ColumnFamilyStore cfs, SSTableReader parent, Plan plan) throws IOException
    {
        Descriptor target = ZeroCopySSTableSlice.newDescriptor(parent);
        ZeroCopySSTableSlice.Slice slice = ZeroCopySSTableSlice.write(parent, plan, target);

        Set<Component> components = new HashSet<>(slice.components);
        components.add(Components.DATA);
        try
        {
            try (FileChannel in = parent.descriptor.fileFor(Components.DATA).newReadChannel();
                 FileChannel out = target.fileFor(Components.DATA).newWriteChannel(File.WriteMode.OVERWRITE))
            {
                for (ZeroCopySSTableSlice.Run run : plan.runs)
                {
                    long position = run.srcStart;
                    long remaining = run.physicalBytes();
                    while (remaining > 0)
                    {
                        long transferred = in.transferTo(position, remaining, out);
                        assertTrue("transferTo made no progress", transferred > 0);
                        position += transferred;
                        remaining -= transferred;
                    }
                }
            }
            assertEquals("the receiver writes exactly what the manifest declares",
                         plan.physicalBytes, target.fileFor(Components.DATA).length());

            components.add(Components.TOC);
            TOCComponent.updateTOC(target, components);

            SSTableReader reader = SSTableReader.open(cfs, target, components, cfs.metadata);
            return new Materialised(slice, reader, components);
        }
        catch (Throwable t)
        {
            ZeroCopySSTableSlice.delete(target, components);
            throw t;
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Assertions
    // ----------------------------------------------------------------------------------------------------

    /**
     * Every expected partition, in order, byte for byte what the parent holds for it. Both scanners run in token
     * order, so the parent's is advanced past the partitions that were not asked for rather than reopened per key.
     */
    private static void assertContentMatches(SSTableReader parent, SSTableReader slice, List<DecoratedKey> expected)
    {
        Set<DecoratedKey> wanted = new HashSet<>(expected);
        int compared = 0;
        try (ISSTableScanner parentScanner = parent.getScanner();
             ISSTableScanner sliceScanner = slice.getScanner())
        {
            while (sliceScanner.hasNext())
            {
                assertTrue("the slice yielded more partitions than were asked for", compared < expected.size());
                try (UnfilteredRowIterator actual = sliceScanner.next())
                {
                    assertEquals("partition " + compared, expected.get(compared), actual.partitionKey());

                    UnfilteredRowIterator wantedPartition = null;
                    try
                    {
                        while (parentScanner.hasNext())
                        {
                            UnfilteredRowIterator candidate = parentScanner.next();
                            if (wanted.contains(candidate.partitionKey()))
                            {
                                wantedPartition = candidate;
                                break;
                            }
                            candidate.close();
                        }
                        assertNotNull("the parent ran out of partitions at " + compared, wantedPartition);
                        assertSamePartition(wantedPartition, actual);
                    }
                    finally
                    {
                        if (wantedPartition != null)
                            wantedPartition.close();
                    }
                }
                compared++;
            }
        }
        assertEquals("the slice is missing partitions", expected.size(), compared);
    }

    /**
     * The point of the whole exercise: the partitions the slice was not asked for are physically in its Data.db
     * and cannot be reached by any means the read path offers.
     */
    private static void assertOnlyTheseKeysArePresent(SSTableReader slice, List<DecoratedKey> all,
                                                      List<DecoratedKey> expected)
    {
        Set<DecoratedKey> wanted = new HashSet<>(expected);
        for (DecoratedKey key : all)
        {
            // trunk's getPosition returns the data position, or a negative value for "not in this sstable".
            long position = slice.getPosition(key, SSTableReader.Operator.EQ);
            if (wanted.contains(key))
                assertTrue("the slice cannot find " + key, position >= 0);
            else
                assertTrue("the slice exposes " + key + ", which was not asked for", position < 0);
        }
    }

    /**
     * Validate the slice's Data.db against its own CRC.db, cell by cell, the way a legacy stream of it would --
     * {@code CassandraStreamWriter} is the one consumer of the component, and it validates every chunk it sends.
     * This is what catches a CRC.db whose entries do not line up with the cells they are supposed to describe.
     */
    private static void assertCrcValidates(SSTableReader slice) throws IOException
    {
        File data = slice.descriptor.fileFor(Components.DATA);
        // trunk has no DataIntegrityMetadata.checksumValidator(Descriptor); the two files are named explicitly.
        try (DataIntegrityMetadata.ChecksumValidator validator =
                 new DataIntegrityMetadata.ChecksumValidator(data, slice.descriptor.fileFor(Components.CRC));
             RandomAccessReader in = RandomAccessReader.open(data))
        {
            long length = data.length();
            assertTrue("expected more than one cell to validate", length > validator.chunkSize);
            validator.seek(0);
            long position = 0;
            while (position < length)
            {
                int toRead = (int) Math.min(validator.chunkSize, length - position);
                byte[] bytes = new byte[toRead];
                in.seek(position);
                in.readFully(bytes);
                validator.validate(ByteBuffer.wrap(bytes));
                position += toRead;
            }
        }
    }

    private static void assertSamePartition(UnfilteredRowIterator expected, UnfilteredRowIterator actual)
    {
        String context = "partition " + expected.partitionKey();
        assertEquals(context, expected.partitionKey(), actual.partitionKey());
        assertEquals(context + ": partition level deletion",
                     expected.partitionLevelDeletion(), actual.partitionLevelDeletion());
        assertEquals(context + ": static row", expected.staticRow(), actual.staticRow());
        assertEquals(context + ": columns", expected.columns(), actual.columns());

        int i = 0;
        while (expected.hasNext())
        {
            assertTrue(context + ": the slice ran out of rows after " + i, actual.hasNext());
            assertEquals(context + ": unfiltered " + i, expected.next(), actual.next());
            i++;
        }
        assertFalse(context + ": the slice has extra rows after " + i, actual.hasNext());
        assertTrue(context + ": expected at least one row", i > 0);
    }

    // ----------------------------------------------------------------------------------------------------
    // Scaffolding
    // ----------------------------------------------------------------------------------------------------

    /**
     * {@code nodetool verify}'s extended pass over a slice. The fork's {@code new Verifier(cfs, sstable, isOffline,
     * options)} is {@code SSTableReader.getVerifier} in trunk; a slice is not tracked by the cfs, hence offline.
     */
    private static void verify(ColumnFamilyStore cfs, SSTableReader slice)
    {
        try (IVerifier verifier = slice.getVerifier(cfs, new OutputHandler.LogOutput(), true,
                                                    IVerifier.options().extendedVerification(true).build()))
        {
            verifier.verify();
        }
    }

    /** The fork's {@code new Scrubber(cfs, txn, skipCorrupted = false, checkData = true)}. */
    private static IScrubber scrubber(ColumnFamilyStore cfs, SSTableReader slice, LifecycleTransaction txn)
    {
        return slice.descriptor.getFormat().getScrubber(cfs, txn, new OutputHandler.LogOutput(),
                                                        IScrubber.options().checkData().build());
    }

    /** Keys in the sstable's own order, read straight out of Index.db. */
    private static List<DecoratedKey> keysInOrder(SSTableReader sstable) throws IOException
    {
        List<DecoratedKey> keys = new ArrayList<>();
        try (RandomAccessReader in = RandomAccessReader.open(sstable.descriptor.fileFor(Components.PRIMARY_INDEX)))
        {
            long length = in.length();
            while (in.getFilePointer() != length)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                RowIndexEntry.Serializer.readPosition(in);
                int promotedSize = in.readUnsignedVInt32();
                if (promotedSize > 0)
                    in.skipBytesFully(promotedSize);
                keys.add(sstable.decorateKey(key));
            }
        }
        return keys;
    }

    /** The sections {@code createOutgoingStreams} would ask for, for the range {@code (left, right]}. */
    private static List<PartitionPositionBounds> positionsFor(SSTableReader sstable, DecoratedKey left, DecoratedKey right)
    {
        return sstable.getPositionsForRanges(Collections.singletonList(new Range<>(left.getToken(), right.getToken())));
    }

    /** Convenience for the many cases that are meant to be one contiguous range. */
    private static boolean isSingleRun(List<PartitionPositionBounds> sections, int cellLength)
    {
        return ZeroCopySSTableSlice.runCount(sections, cellLength) == 1;
    }

    private static ZeroCopySSTableSlice.Run onlyRun(Plan plan)
    {
        assertEquals("expected a single run", 1, plan.runs.size());
        return plan.runs.get(0);
    }

    private static SSTableReader onlySSTable(ColumnFamilyStore cfs)
    {
        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals("expected exactly one sstable", 1, live.size());
        return live.iterator().next();
    }

    private String createCompressedTable(int chunkLengthInKb) throws Throwable
    {
        return createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                           "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '" +
                           chunkLengthInKb + "'}");
    }

    private String createUncompressedTable() throws Throwable
    {
        return createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                           "WITH compression = {'enabled': 'false'}");
    }

    private void insertPartitions(int partitions, int rowsPerPartition, int valueBytes) throws Throwable
    {
        for (int p = 0; p < partitions; p++)
            for (int c = 0; c < rowsPerPartition; c++)
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", String.format("k%06d", p), c,
                        randomText(valueBytes));
    }

    /** Near-incompressible payload, so the sstable really does span many compression chunks. */
    private static String randomText(int length)
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = (char) ('!' + random.nextInt(94));
        return new String(chars);
    }
}
