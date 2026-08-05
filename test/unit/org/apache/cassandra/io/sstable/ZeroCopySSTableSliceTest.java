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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.AbstractCompactionController;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compression.CompressionDictionaryManager;
import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
import org.apache.cassandra.db.compression.TrainingState;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSlice.Plan;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSlice.Reason;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.SSTableSimpleScanner;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.Util.spinUntilTrue;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    private static final SSTableReadsListener NOOP = SSTableReadsListener.NOOP_LISTENER;

    /**
     * A streamable component type that is not one of the format's own, standing in for the storage-attached index
     * components, which are exactly that. Held in a static because {@code Component.Type}'s constructor registers
     * itself in a global registry, so it must be created once per JVM.
     */
    private static final Component.Type EXTRA_STREAMABLE_TYPE =
        Component.Type.create("SliceTestExtra", ".*-SliceTestExtra\\.db", true, null);

    private int savedColumnIndexCacheSizeInKiB;
    private int savedColumnIndexSizeInKiB;
    private int savedPreemptiveOpenIntervalInMiB;
    private Config.FlushCompression savedFlushCompression;

    /**
     * Several tests here move global configuration -- the column index cache size, the preemptive open interval,
     * the flush compression -- because the shapes they need cannot be produced any other way. Snapshotting it
     * around every test means a failure part way through one of them cannot leak that configuration into the next
     * class to run in the same JVM.
     */
    @Before
    public void saveConfigurationAndHooks()
    {
        savedColumnIndexCacheSizeInKiB = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
        savedColumnIndexSizeInKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        savedPreemptiveOpenIntervalInMiB = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        savedFlushCompression = DatabaseDescriptor.getFlushCompression();
    }

    @After
    public void restoreConfigurationAndHooks()
    {
        DatabaseDescriptor.setColumnIndexCacheSize(savedColumnIndexCacheSizeInKiB);
        DatabaseDescriptor.setColumnIndexSizeInKiB(savedColumnIndexSizeInKiB);
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(savedPreemptiveOpenIntervalInMiB);
        DatabaseDescriptor.setFlushCompression(savedFlushCompression);
        // Production statics that exist only for tests. This class drives ZeroCopySSTableSplitter (see
        // splittingASliceKeepsTheUnindexedRegionMark), so leaving either of them set would silently change what a
        // later test in the same JVM exercises.
        ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = false;
        ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
    }

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

            // A dead PREFIX on its own is not an unindexed region for scanning purposes: every scan's sections
            // begin at a position taken from the index, so the prefix is stepped over. Marking this would push
            // every full scan of the received sstable onto the index-driven path for no reason, so the marker is
            // conditioned on interior gaps only.
            //
            // LOAD BEARING, and deliberately an assertFalse: the marker is `interiorDeadBytes() > 0 ||
            // parent.hasUnindexedRegions`, and this pins the left half of that to "prefix does not count" for a
            // parent that carries no mark. CassandraOutgoingFile.contained() and SortedTableVerifier both rely on a
            // prefix-only slice being an ordinary linearly scannable sstable. The right half of the OR -- a marked
            // parent -- is slicingASliceKeepsTheUnindexedRegionMark, and it must stay a separate test.
            assertTrue("a middle range has a dead prefix", plan.deadBytes > 0);
            assertEquals("but no interior gap", 0, plan.interiorDeadBytes());
            assertFalse("the parent is a flushed sstable, so there is no mark to inherit either",
                        parent.hasUnindexedRegions());
            assertFalse("a prefix-only slice must not be marked", slice.hasUnindexedRegions());

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
    public void sliceFromPositionZeroHasNoDeadPrefixButStillCarriesItsFinalChunk() throws Throwable
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

        // No dead PREFIX and no interior gap, so nothing inside dataLength is unreachable...
        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 0.25);
        assertTrue("a single range from position 0 must be eligible: " + plan, plan.isEligible());
        assertEquals("no dead prefix and no interior gap", 0, plan.deadBytes);
        assertEquals(0, plan.interiorDeadBytes());

        // ...but the range does not end on a chunk boundary, and a COMPRESSED chunk cannot be cut, so the tail of
        // the final chunk is transferred and stored all the same. It is outside dataLength, hence not in deadBytes,
        // and it is why maxDeadSpaceRatio = 0 must refuse this: "no waste at all" is not what this slice is.
        // Compressed is the only path where this holds; the uncompressed one really does cut its last cell and
        // reports suffixBytes == 0 -- see uncompressedSliceWithNoDeadSpaceIsAcceptedEvenAtRatioZero.
        assertTrue("this test is about the compressed path", plan.compressed);
        assertTrue("the final chunk's tail is still carried: " + plan, plan.suffixBytes > 0);
        assertTrue(plan.deadRatio() > 0.0);
        assertFalse("a ratio of 0 permits only ranges that begin AND end on a cell boundary",
                    ZeroCopySSTableSlice.plan(parent, sections, 0.0).isEligible());

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
     * The RANGE-scoped scanners have to skip an interior gap too, and they cannot do it the way a full scan does.
     *
     * <p>{@code getPositionsForRanges} resolves only the ENDPOINTS of each requested range through the index, so
     * the section it hands back spans the gap: a linear reader starting at the section's lower position walks
     * straight into the carried partitions and emits them. Marking the sstable with
     * {@code StatsMetadata#hasUnindexedRegions} is what prevents that, and this asserts the marker is honoured by
     * every {@code getScanner} overload rather than only by the no-argument one -- {@code nodetool cleanup}
     * ({@code CleanupStrategy.Bounded}) and repair validation both come in through the ranges variant, and cleanup
     * would write whatever a scanner returned back out as this node's own data.
     */
    @Test
    public void rangeScopedScansOfASliceSkipTheCarriedPartitions() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(64);
        disableCompaction();
        insertPartitions(60, 4, 300);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(parent);

        List<PartitionPositionBounds> sections = new ArrayList<>();
        sections.addAll(positionsFor(parent, keys.get(9), keys.get(24)));
        sections.addAll(positionsFor(parent, keys.get(29), keys.get(49)));
        assertEquals(2, sections.size());
        assertTrue(isSingleRun(sections, parent.getCompressionMetadata().chunkLength()));

        List<DecoratedKey> expected = new ArrayList<>();
        expected.addAll(keys.subList(10, 25));
        expected.addAll(keys.subList(30, 50));

        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 1.0);
        assertTrue(plan.toString(), plan.isEligible());
        assertTrue("this test needs a real interior gap", plan.interiorDeadBytes() > 0);

        Materialised materialised = materialise(cfs, parent, plan);
        try
        {
            SSTableReader slice = materialised.reader;
            assertTrue("an interior gap must be marked", slice.hasUnindexedRegions());

            // A range covering everything the slice could possibly hold, so nothing is excluded by the range
            // itself and only the index can be what leaves the gap partitions out.
            Range<Token> everything = new Range<>(parent.getPartitioner().getMinimumToken(),
                                                  parent.getPartitioner().getMinimumToken());
            // Every overload, because CursorCompactor reaches the sstable through whichever one its strategy used
            // and only the no-argument one was ever covered here.
            assertKeysFromScanner("getScanner()", expected, slice.getScanner());
            assertKeysFromScanner("getScanner(diskAccessMode)", expected,
                                  slice.getScanner(Config.DiskAccessMode.standard));
            assertKeysFromScanner("getScanner(range)", expected, slice.getScanner(everything));
            assertKeysFromScanner("getScanner(ranges)", expected,
                                  slice.getScanner(Collections.singletonList(everything)));

            // A WRAPPING range, which is what a node owning a range across the ring's origin asks for, and the
            // shape that has to be split into its two non-wrapping pieces before a scanner sees it -- left
            // unsplit it would reach the scanner with left > right. (keys[45], keys[15]] covers the top and the
            // bottom of the ring, so of what the slice holds it selects keys[46..49] and keys[10..15]; the scanner
            // returns them in ascending token order, i.e. the low piece first.
            Range<Token> wrapping = new Range<>(keys.get(45).getToken(), keys.get(15).getToken());
            assertTrue("the range must actually wrap for this to test anything", wrapping.isWrapAround());
            List<DecoratedKey> wrapped = new ArrayList<>();
            wrapped.addAll(keys.subList(10, 16));
            wrapped.addAll(keys.subList(46, 50));
            assertKeysFromScanner("getScanner(wrapping ranges)", wrapped,
                                  slice.getScanner(Collections.singletonList(wrapping)));
            assertKeysFromScanner("getScanner(bounds)", expected,
                                  slice.getScanner(Range.makeRowRange(everything)));
            assertKeysFromScanner("getScanner(boundsIterator)", expected,
                                  slice.getScanner(Collections.<AbstractBounds<PartitionPosition>>singletonList(
                                      Range.makeRowRange(everything)).iterator()));

            // And both linear readers refuse outright, so no future call site can reintroduce the hazard by
            // constructing one directly.
            try
            {
                new SSTableSimpleScanner(slice, Collections.singletonList(new PartitionPositionBounds(0, slice.uncompressedLength())),
                                         Config.DiskAccessMode.standard);
                fail("SSTableSimpleScanner must refuse an sstable with unindexed regions");
            }
            catch (IllegalArgumentException expectedFailure)
            {
                assertTrue(expectedFailure.getMessage(), expectedFailure.getMessage().contains("unindexed regions"));
            }

            // The cursor is the other linear reader, and the more dangerous one: it walks Data.db from offset 0 and
            // its seekPartition() does not even check position 0, so it would emit the dead prefix's partial
            // partition and every carried one.
            try
            {
                new SSTableCursorReader(slice);
                fail("SSTableCursorReader must refuse an sstable with unindexed regions");
            }
            catch (IllegalArgumentException expectedFailure)
            {
                assertTrue(expectedFailure.getMessage(), expectedFailure.getMessage().contains("unindexed regions"));
            }

            assertCursorCompactionDeclines(cfs, slice, parent);
        }
        finally
        {
            materialised.close();
        }
    }

    /**
     * Cursor compaction must decline a marked sstable TWICE OVER, and the second refusal is the one that matters.
     *
     * <p>{@code CursorCompactor.convertScannersToCursors} closes the scanners it was given before it builds the
     * cursors, so a throw from {@code SSTableCursorReader}'s constructor at that point kills the compaction task
     * instead of falling back -- the refusal has to happen in {@code isSupported}, while falling back is still free.
     *
     * <p>The first refusal is {@code ISSTableScanner.isFullRange()}, which the index-driven scanner now answers
     * false. It used to answer TRUE: it inherited {@code dataRange == null} from the base class, which for every
     * OTHER scanner means "no clustering restriction and the whole sstable", but for this one means "bounds were
     * handed to me explicitly". A true there let {@code isSupported} accept, and cursor compaction then discarded
     * the scanners and read Data.db linearly from offset 0 -- returning the dead prefix and every carried partition
     * and writing them out as this node's own data.
     *
     * <p>The second is the per-backing-sstable {@code hasUnindexedRegions()} test, checked here with a scanner that
     * lies about its range: a wrapping scanner ({@code LeveledScanner}) answers {@code isFullRange()} from its own
     * bounds without consulting the per-sstable scanners it creates lazily, so the first refusal cannot be relied
     * on to catch every path to a marked sstable.
     *
     * @param unmarkedControl an ordinary sstable of the same table, which cursor compaction MUST accept. Without it
     *                        both refusals below would pass just as well if this table were unsupported for some
     *                        entirely unrelated reason, and {@code isSupported} has several.
     */
    private static void assertCursorCompactionDeclines(ColumnFamilyStore cfs, SSTableReader slice,
                                                      SSTableReader unmarkedControl)
    {
        // Nothing to close: it holds only the cfs and the two values isSupported reads off it.
        AbstractCompactionController controller = noPurgeController(cfs);

        assertFalse("the control must be an ordinary sstable", unmarkedControl.hasUnindexedRegions());
        try (ISSTableScanner scanner = unmarkedControl.getScanner())
        {
            assertTrue("a linear full scan of an unmarked sstable claims the full range", scanner.isFullRange());
            assertTrue("cursor compaction must be supported for this table, or the refusals below prove nothing",
                       CursorCompactor.isSupported(scannerList(scanner), controller));
        }

        try (ISSTableScanner scanner = slice.getScanner())
        {
            assertFalse("an index-driven scanner is restricted to its bounds and must never claim otherwise",
                        scanner.isFullRange());
            assertFalse("cursor compaction must decline a scanner that is not full-range",
                        CursorCompactor.isSupported(scannerList(scanner), controller));
        }

        try (ISSTableScanner scanner = slice.getScanner())
        {
            assertFalse("cursor compaction must decline a marked sstable even behind a scanner that claims to" +
                        " cover the whole range",
                        CursorCompactor.isSupported(scannerList(new ClaimsFullRange(scanner)), controller));
        }
    }

    private static AbstractCompactionStrategy.ScannerList scannerList(ISSTableScanner scanner)
    {
        return new AbstractCompactionStrategy.ScannerList(Collections.singletonList(scanner));
    }

    /**
     * The minimum {@code CursorCompactor.isSupported} reads: the table's metadata and the tombstone option. Built by
     * hand rather than as a {@code CompactionController} because a slice is not tracked by the cfs, so there are no
     * overlaps to compute and nothing to compute them from.
     */
    private static AbstractCompactionController noPurgeController(ColumnFamilyStore cfs)
    {
        return new AbstractCompactionController(cfs, FBUtilities.nowInSeconds(), CompactionParams.TombstoneOption.NONE)
        {
            public boolean compactingRepaired()
            {
                return false;
            }

            public java.util.function.LongPredicate getPurgeEvaluator(DecoratedKey key)
            {
                return time -> false;
            }

            public void close()
            {
            }
        };
    }

    /** A scanner that reports {@code isFullRange()} the way the index-driven scanner used to, and is otherwise real. */
    private static final class ClaimsFullRange implements ISSTableScanner
    {
        private final ISSTableScanner delegate;

        ClaimsFullRange(ISSTableScanner delegate)
        {
            this.delegate = delegate;
        }

        public boolean isFullRange()
        {
            return true;
        }

        public long getLengthInBytes()
        {
            return delegate.getLengthInBytes();
        }

        public long getCompressedLengthInBytes()
        {
            return delegate.getCompressedLengthInBytes();
        }

        public long getCurrentPosition()
        {
            return delegate.getCurrentPosition();
        }

        public long getBytesScanned()
        {
            return delegate.getBytesScanned();
        }

        public Set<SSTableReader> getBackingSSTables()
        {
            return delegate.getBackingSSTables();
        }

        public TableMetadata metadata()
        {
            return delegate.metadata();
        }

        public boolean hasNext()
        {
            return delegate.hasNext();
        }

        public UnfilteredRowIterator next()
        {
            return delegate.next();
        }

        public void close()
        {
            delegate.close();
        }
    }

    /**
     * A slice of WIDE partitions, read back through clustering-restricted queries with the promoted index forced
     * off heap.
     *
     * <p>Worth its own test because the promoted index is dead code in every other test here: they use 2-4 row
     * partitions, so {@code blockCount()} is 0 and nothing is ever copied or re-read. The position vint is the one
     * field the slice rewrites, and {@code ShallowIndexedEntry} finds its IndexInfo offsets table by re-deriving
     * {@code computeUnsignedVIntSize(position)} from that field -- so a rebased position that narrows, which is the
     * normal case for a slice, has to be encoded minimally or every block lookup seeks into the middle of an
     * IndexInfo. Only a clustering-restricted read reaches that: {@code IndexState.findBlockIndex} short-circuits
     * on BOTTOM/TOP, so a full-partition scan never asks for a block.
     */
    @Test
    public void widePartitionSliceServesTheShallowPromotedIndex() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        // 0 forces the ShallowIndexedEntry branch for every promoted entry, i.e. one that re-reads its IndexInfo
        // out of Index.db per block lookup instead of holding it on heap.
        int previousCacheSize = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        try
        {
            createCompressedTable(4);
            disableCompaction();
            int rowsPerPartition = 40;
            insertPartitions(20, rowsPerPartition, 1000);
            flush();

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            SSTableReader parent = onlySSTable(cfs);
            List<DecoratedKey> keys = keysInOrder(parent);

            Map<ByteBuffer, byte[]> parentPromoted = promotedByKey(parent.descriptor);
            for (DecoratedKey key : keys)
                assertNotNull("partition " + key + " has no promoted index; widen the payload",
                              parentPromoted.get(key.getKey()));

            List<PartitionPositionBounds> sections = positionsFor(parent, keys.get(7), keys.get(15));
            assertEquals(1, sections.size());
            List<DecoratedKey> expected = keys.subList(8, 16);

            Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 0.25);
            assertTrue(plan.toString(), plan.isEligible());
            long shift = onlyRun(plan).shift;

            Materialised materialised = materialise(cfs, parent, plan);
            try
            {
                SSTableReader slice = materialised.reader;
                assertEquals(expected.size(), materialised.slice.partitionCount);

                // The promoted part is copied verbatim; only the position ahead of it is rewritten.
                Map<ByteBuffer, byte[]> slicePromoted = promotedByKey(slice.descriptor);
                assertEquals(expected.size(), slicePromoted.size());
                for (DecoratedKey key : expected)
                    assertArrayEquals("promoted index of " + key + " was not copied verbatim",
                                      parentPromoted.get(key.getKey()), slicePromoted.get(key.getKey()));

                int narrowed = 0;
                for (DecoratedKey key : expected)
                {
                    RowIndexEntry parentEntry = entryFor(parent, key);
                    RowIndexEntry sliceEntry = entryFor(slice, key);
                    assertNotNull("the slice lost " + key, sliceEntry);
                    assertTrue("the slice's " + key + " lost its promoted index", sliceEntry.isIndexed());
                    // false for ShallowIndexedEntry, true for the on-heap one: this is what says the reads below
                    // are served by re-reading the slice's own Index.db.
                    assertFalse("the slice's " + key + " was materialised on heap, not re-read",
                                sliceEntry.indexOnHeap());
                    assertEquals("block count of " + key, parentEntry.blockCount(), sliceEntry.blockCount());
                    assertEquals("rebased position of " + key, parentEntry.position - shift, sliceEntry.position);

                    if (VIntCoding.computeUnsignedVIntSize(sliceEntry.position)
                        != VIntCoding.computeUnsignedVIntSize(parentEntry.position))
                        narrowed++;
                }
                assertTrue("no position vint changed width, so the shallow offset arithmetic is untested",
                           narrowed > 0);

                // The reads that actually walk the promoted index.
                ClusteringComparator comparator = cfs.metadata().comparator;
                ColumnFilter columns = ColumnFilter.all(cfs.metadata());
                List<Slices> bands = new ArrayList<>();
                bands.add(Slices.ALL);
                bands.add(Slices.with(comparator, Slice.make(comparator.make(0), comparator.make(3))));
                bands.add(Slices.with(comparator, Slice.make(comparator.make(13), comparator.make(26))));
                bands.add(Slices.with(comparator, Slice.make(comparator.make(rowsPerPartition - 4),
                                                             comparator.make(rowsPerPartition - 1))));
                bands.add(Slices.with(comparator, Slice.make(comparator.make(20), comparator.make(20))));

                for (DecoratedKey key : expected)
                {
                    for (Slices band : bands)
                    {
                        for (boolean reversed : new boolean[]{ false, true })
                        {
                            try (UnfilteredRowIterator want = parent.rowIterator(key, band, columns, reversed, NOOP);
                                 UnfilteredRowIterator got = slice.rowIterator(key, band, columns, reversed, NOOP))
                            {
                                assertSamePartition(want, got);
                            }
                        }
                    }
                }

                assertContentMatches(parent, slice, expected);
                assertOnlyTheseKeysArePresent(slice, keys, expected);
                // -extended walks every IndexInfo and checks partitionStart + info.offset begins at
                // info.firstName in the SLICE's Data.db; with narrow partitions that loop never runs.
                verify(cfs, slice);
            }
            finally
            {
                materialised.close();
            }
        }
        finally
        {
            DatabaseDescriptor.setColumnIndexCacheSize(previousCacheSize);
        }
    }

    /** Every Index.db record's promoted part by raw key, parsed by hand so it does not go through the code under test. */
    private static Map<ByteBuffer, byte[]> promotedByKey(Descriptor descriptor) throws IOException
    {
        Map<ByteBuffer, byte[]> promotedParts = new LinkedHashMap<>();
        try (RandomAccessReader in = RandomAccessReader.open(descriptor.fileFor(Components.PRIMARY_INDEX)))
        {
            long length = in.length();
            while (in.getFilePointer() != length)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                RowIndexEntry.Serializer.readPosition(in);
                int promotedSize = in.readUnsignedVInt32();
                byte[] promoted = null;
                if (promotedSize > 0)
                {
                    promoted = new byte[promotedSize];
                    in.readFully(promoted);
                }
                promotedParts.put(key, promoted);
            }
        }
        return promotedParts;
    }

    private static RowIndexEntry entryFor(SSTableReader reader, DecoratedKey key)
    {
        return ((BigTableReader) reader).getRowIndexEntry(key, SSTableReader.Operator.EQ, false, NOOP);
    }

    /**
     * Splitting a marked sstable keeps the mark. A split child is one contiguous chunk run, so the split adds no
     * unindexed region of its own -- but the run it copies can already contain one, and a child that cleared the
     * mark would be handed straight to the linear scanner.
     *
     * <p>Reachable as soon as both flags are on: a node bootstraps or rebuilds, receives a multi-section slice,
     * and later anticompacts it.
     */
    @Test
    public void splittingASliceKeepsTheUnindexedRegionMark() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(64);
        disableCompaction();
        insertPartitions(60, 4, 300);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(parent);

        List<PartitionPositionBounds> sections = new ArrayList<>();
        sections.addAll(positionsFor(parent, keys.get(9), keys.get(24)));
        sections.addAll(positionsFor(parent, keys.get(29), keys.get(49)));
        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 1.0);
        assertTrue(plan.toString(), plan.isEligible());
        assertTrue(plan.interiorDeadBytes() > 0);

        Materialised materialised = materialise(cfs, parent, plan);
        try
        {
            SSTableReader slice = materialised.reader;
            assertTrue(slice.hasUnindexedRegions());
            assertTrue("the slice must be splittable for this test to prove anything",
                       ZeroCopySSTableSplitter.isSupported(slice));

            ZeroCopySSTableSplitter.Result result = ZeroCopySSTableSplitter.split(slice, 2, null);
            try
            {
                assertEquals(2, result.children.size());
                for (ZeroCopySSTableSplitter.Child child : result.children)
                {
                    assertTrue("child " + child.descriptor + " lost the unindexed-region mark",
                               child.reader.hasUnindexedRegions());
                }
            }
            finally
            {
                for (ZeroCopySSTableSplitter.Child child : result.children)
                {
                    child.reader.selfRef().release();
                    ZeroCopySSTableSlice.delete(child.descriptor, child.components);
                }
            }
        }
        finally
        {
            materialised.close();
        }
    }

    /**
     * SLICING a marked sstable keeps the mark, and this is the case that cannot be got right by computing it:
     * the new slice is a SINGLE section, so its own {@code interiorDeadBytes()} is 0 by construction -- a single
     * section is one contiguous byte range, and the accounting calls every byte in it useful even though the
     * partitions the PARENT was carrying unindexed are sitting in the middle of it.
     *
     * <p>The reachable shape is the second streaming hop: node B is sent a multi-section slice, node C then
     * bootstraps or repairs a sub-range of what B holds, and B slices its own slice. A marker computed fresh from
     * this plan would be false, the received sstable would be handed to the linear scanner, and the next full scan
     * on C -- a compaction, a cleanup, a repair validation -- would return the partitions carried inside the
     * boundary chunk and write them out as C's own data. Silent resurrection of data C was never sent, including of
     * partitions that were deleted elsewhere.
     *
     * <p>{@code middleRangeSliceReadsBackExactly} is the complement: for a parent with NO mark, a prefix-only slice
     * must still come out unmarked. Both halves of the {@code interiorDeadBytes() > 0 || parentStats
     * .hasUnindexedRegions} disjunction, one test each.
     */
    @Test
    public void slicingASliceKeepsTheUnindexedRegionMark() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(64);   // one chunk holds many partitions, so two nearby ranges share one
        disableCompaction();
        insertPartitions(60, 4, 300);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<DecoratedKey> parentKeys = keysInOrder(parent);

        // Hop one: (keys[9], keys[24]] and (keys[29], keys[49]] in one run, so keys[25..29] come along unindexed.
        List<PartitionPositionBounds> firstHop = new ArrayList<>();
        firstHop.addAll(positionsFor(parent, parentKeys.get(9), parentKeys.get(24)));
        firstHop.addAll(positionsFor(parent, parentKeys.get(29), parentKeys.get(49)));
        assertTrue(isSingleRun(firstHop, parent.getCompressionMetadata().chunkLength()));

        Plan firstPlan = ZeroCopySSTableSlice.plan(parent, firstHop, 1.0);
        assertTrue(firstPlan.toString(), firstPlan.isEligible());
        assertTrue("hop one must have an interior gap, or there is no mark to inherit",
                   firstPlan.interiorDeadBytes() > 0);

        Materialised child = materialise(cfs, parent, firstPlan);
        try
        {
            assertTrue("hop one must be marked", child.reader.hasUnindexedRegions());
            List<DecoratedKey> childKeys = keysInOrder(child.reader);
            assertEquals(35, childKeys.size());

            // Hop two: ONE range, deliberately spanning the join between hop one's two sections, so the single
            // section it produces has the carried partitions inside it.
            List<PartitionPositionBounds> secondHop = positionsFor(child.reader, childKeys.get(4), childKeys.get(24));
            assertEquals("hop two must be a single section for this test to prove anything", 1, secondHop.size());
            List<DecoratedKey> expected = childKeys.subList(5, 25);

            Plan secondPlan = ZeroCopySSTableSlice.plan(child.reader, secondHop, 1.0);
            assertTrue(secondPlan.toString(), secondPlan.isEligible());
            assertEquals("a single section is a single run", 1, secondPlan.runs.size());
            // The two assertions this whole test hangs on: hop two's plan has no interior gap of its OWN, and yet
            // the sstable it produces must be marked. The marker cannot have come from anywhere but the parent.
            assertEquals("a single-section slice has no interior gap by construction",
                         0, secondPlan.interiorDeadBytes());
            assertTrue("hop two still has a dead prefix, which on its own must never mark",
                       secondPlan.deadBytes > 0);

            Materialised grandchild = materialise(cfs, child.reader, secondPlan);
            try
            {
                SSTableReader reader = grandchild.reader;
                assertTrue("a slice of a marked sstable must stay marked", reader.hasUnindexedRegions());

                assertEquals(expected.size(), grandchild.slice.partitionCount);
                assertEquals(expected.get(0), reader.getFirst());
                assertEquals(expected.get(expected.size() - 1), reader.getLast());
                assertContentMatches(child.reader, reader, expected);
                // Checked against EVERY key of the original parent, which is what says the partitions hop one
                // carried unindexed -- and which hop two's single section physically contains -- are still
                // unreachable two hops down.
                assertOnlyTheseKeysArePresent(reader, parentKeys, expected);
                assertKeysFromScanner("getScanner() two hops down", expected, reader.getScanner());

                // And the mark still does what it is for.
                assertCursorCompactionDeclines(cfs, reader, parent);
            }
            finally
            {
                grandchild.close();
            }
        }
        finally
        {
            child.close();
        }
    }

    private static void assertKeysFromScanner(String what, List<DecoratedKey> expected, ISSTableScanner scanner)
    {
        List<DecoratedKey> seen = new ArrayList<>();
        try (ISSTableScanner closeMe = scanner)
        {
            assertFalse(what + " must not be the linear scanner", closeMe instanceof SSTableSimpleScanner);
            // An index-driven scanner walks the bounds it was handed, so it covers a subset of the sstable whether
            // or not those bounds span it. Callers treat a true here as permission to bypass the scanner and read
            // Data.db themselves -- see assertCursorCompactionDeclines.
            assertFalse(what + " must not claim to cover the full range", closeMe.isFullRange());
            while (closeMe.hasNext())
            {
                try (UnfilteredRowIterator partition = closeMe.next())
                {
                    seen.add(partition.partitionKey());
                }
            }
        }
        assertEquals(what + " returned the wrong partitions", expected, seen);
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
     * The same slice over a partition shape that is not four plain rows: a partition-level tombstone, a static row,
     * TTLs, a collection, row / range / cell tombstones, and two clustering columns one of which is DESC.
     *
     * <p>Every other test here writes {@code INSERT}s only, which makes the {@code partitionLevelDeletion()} and
     * {@code staticRow()} comparisons in {@link #assertSamePartition} vacuous -- LIVE equals LIVE, EMPTY equals
     * EMPTY -- so a slice that dropped either would pass all of them. A lost partition tombstone on a receiver is
     * silent resurrection of every row that partition ever had anywhere in the cluster, which is the worst outcome
     * this code can produce, so at least one fixture has to be able to see it. {@link #assertRichShapesPresent}
     * fails if the fixture ever stops producing the shapes, rather than letting the comparison go quiet again.
     *
     * <p>Nothing about the shape can actually break the arithmetic -- the bytes are copied verbatim -- but the
     * SerializationHeader those bytes are read back through is inherited from the parent's Statistics.db, and it is
     * what carries the clustering types, the static columns and the encoding stats a TTL needs.
     */
    @Test
    public void richPartitionShapesSliceIdentically() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createTable("CREATE TABLE %s (pk text, ck0 int, ck1 text, val text, m map<int, text>, s text static, " +
                    "PRIMARY KEY (pk, ck0, ck1)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '16'} " +
                    "AND CLUSTERING ORDER BY (ck0 DESC, ck1 ASC)");
        disableCompaction();
        insertRichPartitions(60);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(parent);
        assertEquals(60, keys.size());

        // Two nearby ranges, so the slice has an interior gap as well as a dead prefix: the shape that carries
        // partitions it does not claim, now with tombstones in them.
        List<PartitionPositionBounds> sections = new ArrayList<>();
        sections.addAll(positionsFor(parent, keys.get(9), keys.get(24)));
        sections.addAll(positionsFor(parent, keys.get(27), keys.get(49)));

        List<DecoratedKey> expected = new ArrayList<>();
        expected.addAll(keys.subList(10, 25));
        expected.addAll(keys.subList(28, 50));

        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 1.0);
        assertTrue(plan.toString(), plan.isEligible());
        assertTrue("the gap is a few partitions and the cell is 16 KiB, so this must be one run with a gap in it",
                   plan.interiorDeadBytes() > 0);

        Materialised materialised = materialise(cfs, parent, plan);
        try
        {
            SSTableReader slice = materialised.reader;
            assertEquals(expected.size(), materialised.slice.partitionCount);
            assertTrue(slice.hasUnindexedRegions());
            // Compares partitionLevelDeletion(), staticRow(), columns() and every Unfiltered against the parent.
            assertContentMatches(parent, slice, expected);
            assertOnlyTheseKeysArePresent(slice, keys, expected);
            assertRichShapesPresent(slice);
            verify(cfs, slice);
        }
        finally
        {
            materialised.close();
        }
    }

    /**
     * That the slice really holds every shape {@link #insertRichPartitions} writes. Without this the fixture could
     * drift back to plain rows and {@link #richPartitionShapesSliceIdentically} would keep passing while comparing
     * nothing.
     */
    private static void assertRichShapesPresent(SSTableReader slice)
    {
        int partitionTombstones = 0;
        int statics = 0;
        int rowTombstones = 0;
        int rangeTombstoneMarkers = 0;
        int expiringCells = 0;
        int complexColumns = 0;

        try (ISSTableScanner scanner = slice.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    if (!partition.partitionLevelDeletion().isLive())
                        partitionTombstones++;
                    if (!partition.staticRow().isEmpty())
                        statics++;
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        if (unfiltered.isRangeTombstoneMarker())
                        {
                            rangeTombstoneMarkers++;
                            continue;
                        }
                        Row row = (Row) unfiltered;
                        if (!row.deletion().isLive())
                            rowTombstones++;
                        for (ColumnData data : row)
                        {
                            if (data.column().isComplex())
                                complexColumns++;
                        }
                        for (Cell<?> cell : row.cells())
                        {
                            if (cell.isExpiring())
                                expiringCells++;
                        }
                    }
                }
            }
        }

        assertTrue("no partition tombstone survived into the slice", partitionTombstones > 0);
        assertTrue("no static row survived into the slice", statics > 0);
        assertTrue("no row tombstone survived into the slice", rowTombstones > 0);
        assertTrue("no range tombstone marker survived into the slice", rangeTombstoneMarkers > 0);
        assertTrue("no TTL'd cell survived into the slice", expiringCells > 0);
        assertTrue("no collection survived into the slice", complexColumns > 0);
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

    /**
     * A slice synthesises a fixed list of components and sends only those, so a parent that would stream anything
     * outside its format's own component set is refused. In production that set is the storage-attached index
     * components, which register their own non-singleton {@code Component.Type}s: they are streamable, so a FULL
     * entire-sstable stream sends them verbatim and the receiver validates them, but a slice cannot produce them at
     * all. Sending the slice anyway makes the receiver's {@code validateSSTableAttachedIndexes(readers, true, true)}
     * throw and fails the whole session, so the refusal has to happen here -- inside {@code plan}, which
     * {@code CassandraOutgoingFile} calls in its constructor, before {@code getNumFiles()} promises the peer a count.
     * <p>
     * Uses a custom component rather than a real index, which is not a shortcut: with a real one the
     * {@code SSTABLE_ATTACHED_INDEXES} gate refuses FIRST and this backstop is never reached. What it covers is the
     * case the gate cannot -- an sstable whose {@code ColumnFamilyStore} is unreachable (offline tooling), or a
     * streamable component type nobody has thought of yet. The gate itself is
     * {@link #refusesATableWithStorageAttachedIndexes}.
     */
    @Test
    public void refusesWhenTheParentWouldStreamComponentsASliceCannotSynthesise() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(16);
        disableCompaction();
        insertPartitions(40, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(parent);
        List<PartitionPositionBounds> sections = positionsFor(parent, keys.get(0), keys.get(30));

        // Eligible before the extra component exists, which is what proves the refusal below is that component.
        assertTrue(ZeroCopySSTableSlice.plan(parent, sections, 1.0).isEligible());

        Component extra = EXTRA_STREAMABLE_TYPE.createComponent("SliceTestExtra.db");
        parent.descriptor.fileFor(extra).createFileIfNotExists();
        parent.registerComponents(Collections.singleton(extra), cfs.getTracker());

        assertTrue(parent.getStreamingComponents().contains(extra));
        assertEquals(Reason.EXTRA_STREAMING_COMPONENTS, ZeroCopySSTableSlice.plan(parent, sections, 1.0).reason);
    }

    /**
     * The AUTHORITATIVE storage-attached-index refusal, and the one the component backstop above cannot stand in
     * for: it is asked of the TABLE, not of this sstable's component set.
     *
     * <p>The window that matters is a {@code CREATE INDEX} on a populated table. The index exists, so the receiver
     * will run {@code validateSSTableAttachedIndexes(readers, true, true)} over what arrives, but the sstables do
     * not carry its components yet, so {@code getStreamingComponents() \ allComponents()} is EMPTY and the sstable
     * looks perfectly sliceable -- in exactly the window where slicing it is worst. Depending on which file of the
     * session lands last, the receiver either fails the session (taking down every repair and bootstrap for the
     * duration of the build) or publishes an sstable permanently missing its per-sstable completion marker, whose
     * rows are readable and answer no index predicate, silently and for ever.
     *
     * <p>That window is reproduced here by unregistering the index components from a finished sstable rather than by
     * racing a real build: the difference the backstop looks at is then empty, and the only thing left that can
     * refuse is the gate.
     */
    @Test
    public void refusesATableWithStorageAttachedIndexes() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createTable("CREATE TABLE %s (pk text, ck int, v int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        for (int p = 0; p < 60; p++)
            for (int c = 0; c < 4; c++)
                execute("INSERT INTO %s (pk, ck, v, val) VALUES (?, ?, ?, ?)",
                        String.format("k%06d", p), c, 7, randomText(400));
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertTrue("the table must have storage-attached indexes for this test to mean anything",
                   cfs.indexManager.hasSSTableAttachedIndexes());

        List<DecoratedKey> keys = keysInOrder(parent);
        List<PartitionPositionBounds> sections = positionsFor(parent, keys.get(9), keys.get(39));
        assertEquals(Reason.SSTABLE_ATTACHED_INDEXES, ZeroCopySSTableSlice.plan(parent, sections, 1.0).reason);

        // Now the mid-build shape: the index exists and the sstable does not carry its components. What the
        // component backstop can see is empty, so this refusal is the gate and nothing else.
        Set<Component> indexComponents =
            ImmutableSet.copyOf(Sets.difference(parent.getStreamingComponents(),
                                                parent.descriptor.getFormat().allComponents()));
        assertFalse("the flush should have written index components to inherit", indexComponents.isEmpty());
        parent.unregisterComponents(indexComponents, cfs.getTracker());
        try
        {
            assertTrue("the backstop must have nothing left to see, or it and not the gate is what refuses",
                       Sets.difference(parent.getStreamingComponents(),
                                       parent.descriptor.getFormat().allComponents()).isEmpty());

            assertEquals(Reason.SSTABLE_ATTACHED_INDEXES, ZeroCopySSTableSlice.plan(parent, sections, 1.0).reason);
        }
        finally
        {
            // The files were never removed, only forgotten; put them back so the table is not left lying to its own
            // index for the teardown that drops it.
            parent.registerComponents(indexComponents, cfs.getTracker());
        }
    }

    /**
     * A parent whose sstable version cannot carry {@code StatsMetadata#hasUnindexedRegions} is refused outright,
     * even for a shape that would need no marker at all.
     *
     * <p>A slice keeps the parent's version, so a version that drops the marker on serialisation is the same silent
     * corruption as computing the marker fresh: the receiver gets an sstable with unindexed regions and no way to
     * know. It has to be refused for EVERY shape and not only for the ones with an interior gap, because the parent
     * may already have been carrying the flag -- and if the version cannot hold it, that flag has already been lost
     * on the parent, so there is nothing to test for. Older sstables stay supported and streamable, just not
     * sliceable.
     */
    @Test
    public void refusesAParentWhoseVersionCannotCarryTheMarker() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        Assume.assumeTrue("this fixture needs a parent that CAN carry the marker",
                          parent.descriptor.version.hasUnindexedRegionsMarker());

        List<DecoratedKey> keys = keysInOrder(parent);
        List<PartitionPositionBounds> sections = positionsFor(parent, keys.get(19), keys.get(59));
        // The shape is the eligible one, which is what makes the refusal below about the version and nothing else.
        assertTrue(ZeroCopySSTableSlice.plan(parent, sections, 0.25).isEligible());

        SSTableReader older = reopenAtVersion(cfs, parent, "pa");
        try
        {
            assertFalse("'pa' must not carry the marker, or this test proves nothing",
                        older.descriptor.version.hasUnindexedRegionsMarker());
            assertFalse("the fixture must still be an ordinary readable sstable", older.hasUnindexedRegions());

            // Positions are the parent's -- Data.db is the same file -- so this is the same eligible shape.
            List<PartitionPositionBounds> olderSections = positionsFor(older, keys.get(19), keys.get(59));
            assertEquals(sections, olderSections);
            assertEquals(Reason.NO_UNINDEXED_REGIONS_MARKER,
                         ZeroCopySSTableSlice.plan(older, olderSections, 1.0).reason);

            // Not even the shape that needs no marker: one section, so no interior gap could ever arise.
            assertEquals(Reason.NO_UNINDEXED_REGIONS_MARKER,
                         ZeroCopySSTableSlice.plan(older, positionsFor(older, keys.get(0), keys.get(79)), 1.0).reason);
        }
        finally
        {
            Set<Component> components = older.getComponents();
            older.selfRef().release();
            ZeroCopySSTableSlice.delete(older.descriptor, components);
        }
    }

    /**
     * The parent's components under a descriptor of an OLDER sstable version, so the version is the only thing that
     * differs. Every component but Statistics.db is hardlinked -- between {@code pa} and {@code pb} nothing else
     * changed shape -- and Statistics.db is re-serialised AT the older version, which is precisely what drops
     * {@code hasUnindexedRegions} on the floor and why the planner refuses such a parent.
     */
    private static SSTableReader reopenAtVersion(ColumnFamilyStore cfs, SSTableReader parent, String version)
    throws IOException
    {
        Descriptor allocated = ZeroCopySSTableSlice.newDescriptor(parent);
        Descriptor older = new Descriptor(version, allocated.directory, allocated.ksname, allocated.cfname,
                                         allocated.id, allocated.getFormat());

        Set<Component> components = new HashSet<>(parent.getComponents());
        components.add(Components.TOC);
        try
        {
            for (Component component : components)
            {
                if (component != Components.STATS && component != Components.TOC)
                    FileUtils.createHardLinkWithConfirm(parent.descriptor.fileFor(component),
                                                        older.fileFor(component));
            }
            Map<MetadataType, MetadataComponent> metadata =
                parent.descriptor.getMetadataSerializer().deserialize(parent.descriptor,
                                                                     EnumSet.allOf(MetadataType.class));
            older.getMetadataSerializer().rewriteSSTableMetadata(older, metadata);
            TOCComponent.updateTOC(older, components);
            return SSTableReader.open(cfs, older, components, cfs.metadata);
        }
        catch (Throwable t)
        {
            ZeroCopySSTableSlice.delete(older, components);
            throw t;
        }
    }

    /**
     * A range that matches no partition of the parent leaves {@code getPositionsForRanges} with nothing to hand
     * back, and a slice of nothing is refused rather than planned as an empty one. Worth pinning because
     * {@code write} would otherwise reach {@code plan.lo()} -- an index into an empty section list -- and because
     * the reason is what {@code CassandraOutgoingFile} logs and counts.
     *
     * <p>The complementary shape is at the other end: sections that cover the whole sstable produce a plan whose
     * single run IS the parent's whole data file, which is why the caller must reach
     * {@code computeShouldStreamEntireSSTables()} first and never build a slice for it -- synthesising components
     * for bytes it could hardlink instead. See {@code CassandraPartialSSTableStreamTest}, where the routing itself
     * is asserted.
     */
    @Test
    public void refusesWhenNoSectionMatchesAndDegeneratesWhenAllOfThemDo() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(40, 4, 400);
        flush();

        SSTableReader parent = onlySSTable(getCurrentColumnFamilyStore());
        List<DecoratedKey> keys = keysInOrder(parent);

        // A range strictly between two adjacent tokens holds no partition of this sstable. Not (t, t], which is the
        // whole ring.
        Token present = keys.get(10).getToken();
        List<PartitionPositionBounds> none = parent.getPositionsForRanges(
            Collections.singletonList(new Range<>(present, present.increaseSlightly())));
        assertTrue("this range must select no partition", none.isEmpty());
        assertEquals(Reason.NO_SECTIONS, ZeroCopySSTableSlice.plan(parent, none, 1.0).reason);
        assertEquals(Reason.NO_SECTIONS, ZeroCopySSTableSlice.plan(parent, Collections.emptyList(), 1.0).reason);
        assertEquals(Reason.NO_SECTIONS, ZeroCopySSTableSlice.plan(parent, null, 1.0).reason);

        // And the whole sstable: eligible, but a degenerate slice -- one run, from byte 0 to the end of the last
        // chunk, with nothing dead in it. Exactly the bytes the entire-sstable path sends without synthesising
        // anything, hence a plan that must never be preferred to it.
        List<PartitionPositionBounds> everything = parent.getPositionsForRanges(
            Collections.singletonList(new Range<>(parent.getPartitioner().getMinimumToken(),
                                                  parent.getPartitioner().getMinimumToken())));
        Plan whole = ZeroCopySSTableSlice.plan(parent, everything, 0.0);
        assertTrue(whole.toString(), whole.isEligible());
        assertEquals(1, whole.runs.size());
        assertEquals(0, onlyRun(whole).srcStart);
        assertEquals(0, whole.deadBytes);
        assertEquals(0, whole.suffixBytes);
        assertEquals(parent.uncompressedLength(), whole.dataLength);
        assertEquals("nothing in the parent is left out", parent.uncompressedLength(), whole.usefulBytes);
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
        // Nothing past the last live byte is sent: the last run ends exactly at it and writeCrc recomputes that one
        // checksum. Charging a suffix here anyway inflated deadRatio() by up to a whole 64 KiB cell and refused
        // eligible slices at the default 0.25.
        assertEquals("an uncompressed slice cuts its last cell, so it has no dead suffix", 0, plan.suffixBytes);
        assertEquals((double) plan.deadBytes / plan.dataLength, plan.deadRatio(), 1e-12);

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

    /**
     * An uncompressed slice with NO dead space is refused by no ratio at all, not even 0.
     *
     * <p>The shape is a range starting at byte 0 -- so no dead prefix -- and ending mid-cell, which is the ordinary
     * case: a partition boundary lands where it lands. Because the uncompressed path cuts its last cell and
     * recomputes that cell's CRC, nothing past the last live byte is sent, and this slice carries literally nothing
     * it was not asked for.
     *
     * <p>{@code suffixBytes} used to be computed for both paths, so this slice was charged the rest of its final
     * 64 KiB cell -- bytes it never sends -- and a narrow one could be refused at the DEFAULT 0.25 for waste that
     * does not exist. The compressed complement, where the suffix is real because a chunk cannot be cut, is
     * {@code sliceFromPositionZeroHasNoDeadPrefixButStillCarriesItsFinalChunk}.
     */
    @Test
    public void uncompressedSliceWithNoDeadSpaceIsAcceptedEvenAtRatioZero() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createUncompressedTable();
        disableCompaction();
        insertPartitions(400, 4, 500);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(parent);

        List<PartitionPositionBounds> sections = parent.getPositionsForRanges(
            Collections.singletonList(new Range<>(parent.getPartitioner().getMinimumToken(),
                                                  keys.get(199).getToken())));
        assertEquals(1, sections.size());
        assertEquals("the slice must start at byte 0 to have no dead prefix", 0, sections.get(0).lowerPosition);

        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 0.0);
        assertTrue("a zero-dead-space slice must survive even maxDeadSpaceRatio = 0: " + plan, plan.isEligible());
        assertFalse(plan.compressed);
        assertTrue("this test needs the final cell to be CUT; move the range boundary if a partition ever lands" +
                   " exactly on a cell edge",
                   sections.get(0).upperPosition % plan.cellLength != 0);
        assertTrue("and it must not be the parent's own last cell, or there would be no suffix either way",
                   sections.get(0).upperPosition < parent.uncompressedLength());
        assertEquals(0, plan.deadBytes);
        assertEquals(0, plan.suffixBytes);
        assertEquals(0.0, plan.deadRatio(), 0.0);
        assertEquals("every byte carried was asked for", plan.usefulBytes, plan.dataLength);

        Materialised materialised = materialise(cfs, parent, plan);
        try
        {
            List<DecoratedKey> expected = keys.subList(0, 200);
            assertEquals("the data file stops at the last live byte",
                         sections.get(0).upperPosition,
                         materialised.reader.descriptor.fileFor(Components.DATA).length());
            assertContentMatches(parent, materialised.reader, expected);
            assertOnlyTheseKeysArePresent(materialised.reader, keys, expected);
            // The cut cell's recomputed checksum has to be right, or this slice is unreadable by a legacy stream.
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

    /** Older than {@link #DELETE_TS}, so those deletions really shadow the rows they name. */
    private static final long PAST_TS = 1_600_000_000_000_000L;
    /** Every deletion, explicit rather than wall-clock so the fixture's on-disk layout is the same every run. */
    private static final long DELETE_TS = PAST_TS + 1_000_000L;
    /** Newer than {@link #DELETE_TS}, so a partition tombstone is retained AND the rows after it survive it. */
    private static final long FUTURE_TS = 2_000_000_000_000_000L;

    /**
     * The fixture for {@link #richPartitionShapesSliceIdentically}: the shapes an {@code INSERT}-only fixture cannot
     * produce. Deletions run at {@link #DELETE_TS}, which sits between {@link #PAST_TS} and {@link #FUTURE_TS}, so a
     * partition tombstone shadows everything written before it and nothing written after.
     */
    private void insertRichPartitions(int partitions) throws Throwable
    {
        for (int p = 0; p < partitions; p++)
        {
            String pk = String.format("k%06d", p);

            boolean deletedPartition = p % 3 == 0;
            if (deletedPartition)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", DELETE_TS, pk);
            long ts = (deletedPartition ? FUTURE_TS : PAST_TS) + p * 100L;

            for (int c = 0; c < 4; c++)
            {
                if (c % 2 == 0)
                    execute("INSERT INTO %s (pk, ck0, ck1, val, m) VALUES (?, ?, ?, ?, ?) " +
                            "USING TIMESTAMP ? AND TTL ?",
                            pk, c, "c" + c, randomText(300), Collections.singletonMap(c, "m" + c), ts + c, 500_000);
                else
                    execute("INSERT INTO %s (pk, ck0, ck1, val) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                            pk, c, "c" + c, randomText(300), ts + c);
            }

            // Only some partitions, so an EMPTY static row is compared as well as a populated one.
            if (p % 2 == 0)
                execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP ?", pk, "s" + p, ts + 10);

            // A row tombstone, a range tombstone and a single-cell tombstone; all three end up in the copied bytes.
            if (p % 4 == 1)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 = ? AND ck1 = ?",
                        DELETE_TS, pk, 1, "c1");
            if (p % 4 == 2)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 > ? AND ck0 <= ?",
                        DELETE_TS, pk, 1, 2);
            if (p % 4 == 3)
                execute("DELETE val FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 = ? AND ck1 = ?",
                        DELETE_TS, pk, 0, "c0");
        }
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
