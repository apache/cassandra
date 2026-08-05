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
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TestDatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSlice.Plan;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Partial (sliced) zero-copy streaming of BTI sstables.
 *
 * <p>A slice is the harder of the two rebases for BTI, and it is harder in exactly one way: its runs are disjoint,
 * so its Rows.db is not one verbatim range of the parent's but a selection of entries copied out of it, each placed
 * wherever the writer happens to be. Two things can go wrong there and nowhere else:
 * <ul>
 *   <li>an entry placed so that one of its trie nodes crosses a page boundary -- {@code Walker} then reads past the
 *       end of a rebuffered page, so the failure is a garbage row position or an exception on lookup, never a clean
 *       error at write time; and</li>
 *   <li>row indexes of the partitions BETWEEN runs being carried along, which is correct but can put more bytes on
 *       the wire than the slice was asked for.</li>
 * </ul>
 * So the assertions here are a full round trip through a materialised slice -- exactly the bytes
 * {@code CassandraEntireSSTableStreamWriter} sends and {@code SSTableZeroCopyWriter} writes -- plus a direct check
 * that the row index entries of skipped partitions are not in it.
 */
public class ZeroCopySSTableSliceBtiTest extends CQLTester
{
    private static final int PARTITIONS = 60;
    /** Enough rows per partition that every one of them is well past {@code column_index_size}. */
    private static final int WIDE_ROWS = 200;
    private static final int VALUE_BYTES = 200;

    /** Pinned rather than inherited, so the narrow/wide split of the fixture cannot drift with the yaml. */
    private static final int COLUMN_INDEX_KB = 4;

    /** Older than the wall-clock timestamp of everything else, so a partition tombstone does not shadow its rows. */
    private static final long OLD_TS = 1_600_000_000_000_000L;
    private static final long NEW_TS = 1_900_000_000_000_000L;

    private SSTableFormat<?, ?> savedFormat;
    private int savedColumnIndexKb;

    @Before
    public void selectBtiFormat()
    {
        savedFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        savedColumnIndexKb = DatabaseDescriptor.getColumnIndexSizeInKiB();
        TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(BtiFormat.NAME);
        DatabaseDescriptor.setColumnIndexSizeInKiB(COLUMN_INDEX_KB);
    }

    /**
     * The slice path sets no production static of its own, but it reaches {@code ZeroCopySSTableSplitter} through
     * {@link #splittingAMarkedBtiSliceKeepsTheMark}, so the splitter's testing hooks are cleared here too: a test
     * that failed part way through would otherwise leave them set for every class that runs after it in this JVM.
     */
    @After
    public void restoreSettings()
    {
        ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = false;
        ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
        DatabaseDescriptor.setColumnIndexSizeInKiB(savedColumnIndexKb);
        if (savedFormat != null)
            TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(savedFormat);
    }

    // ----------------------------------------------------------------------------------------------------
    // The plan
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void planAcceptsCompressedBti() throws Throwable
    {
        SSTableReader parent = btiSSTable(true, WIDE_ROWS);
        List<DecoratedKey> keys = keysInOrder(parent);

        Plan plan = plan(parent, keys, 10, 40);

        assertTrue(plan.toString(), plan.isEligible());
        assertEquals(ZeroCopySSTableSlice.Reason.ELIGIBLE, plan.reason);
        assertEquals(parent.descriptor.getFormat(), plan.format);
        assertEquals(ZeroCopySSTableSlice.COMPRESSED_BTI_COMPONENTS, plan.components());
        assertFalse(plan.runs.isEmpty());
    }

    @Test
    public void planAcceptsUncompressedBti() throws Throwable
    {
        SSTableReader parent = btiSSTable(false, WIDE_ROWS);
        List<DecoratedKey> keys = keysInOrder(parent);

        Plan plan = plan(parent, keys, 10, 40);

        assertTrue(plan.toString(), plan.isEligible());
        assertEquals(ZeroCopySSTableSlice.UNCOMPRESSED_BTI_COMPONENTS, plan.components());
    }

    // ----------------------------------------------------------------------------------------------------
    // Round trips
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void slicesWidePartitionsCompressed() throws Throwable
    {
        sliceAndVerify(true, WIDE_ROWS, new int[][]{ { 10, 40 } });
    }

    @Test
    public void slicesWidePartitionsUncompressed() throws Throwable
    {
        sliceAndVerify(false, WIDE_ROWS, new int[][]{ { 10, 40 } });
    }

    @Test
    public void slicesNarrowPartitionsCompressed() throws Throwable
    {
        // Every partition below the row index granularity: Rows.db is empty in the parent and in the slice, and
        // every key has to be read out of Data.db.
        sliceAndVerify(true, 1, new int[][]{ { 10, 40 } });
    }

    @Test
    public void slicesNarrowPartitionsUncompressed() throws Throwable
    {
        sliceAndVerify(false, 1, new int[][]{ { 10, 40 } });
    }

    /** The whole sstable through the slice path, i.e. shift 0 and no dead space. */
    @Test
    public void slicesEverything() throws Throwable
    {
        sliceAndVerify(true, WIDE_ROWS, new int[][]{ { 0, PARTITIONS } });
    }

    /** A slice small enough that only one or two partitions are in it. */
    @Test
    public void slicesASinglePartition() throws Throwable
    {
        sliceAndVerify(true, WIDE_ROWS, new int[][]{ { 30, 31 } });
    }

    /**
     * The multi-run case, and the reason this class exists: three disjoint ranges spread across the parent, so the
     * slice's Rows.db is a selection rather than a range, and each entry's placement is decided independently.
     */
    @Test
    public void slicesSeveralDisjointRunsOfWidePartitions() throws Throwable
    {
        sliceAndVerify(true, WIDE_ROWS, new int[][]{ { 5, 12 }, { 25, 30 }, { 48, 55 } });
    }

    @Test
    public void slicesSeveralDisjointRunsUncompressed() throws Throwable
    {
        sliceAndVerify(false, WIDE_ROWS, new int[][]{ { 5, 12 }, { 25, 30 }, { 48, 55 } });
    }

    /**
     * A slice whose Rows.db must NOT contain the entries of the partitions it skipped. This is the thing the
     * selection exists for: the two runs are at opposite ends of the parent, so a contiguous copy would carry
     * almost all of its row indexes, and none of those bytes could ever be read.
     */
    @Test
    public void skippedPartitionsRowIndexesAreNotCarried() throws Throwable
    {
        SSTableReader parent = btiSSTable(true, WIDE_ROWS);
        List<DecoratedKey> keys = keysInOrder(parent);

        Plan plan = plan(parent, keys, new int[][]{ { 0, 3 }, { PARTITIONS - 3, PARTITIONS } });
        assertTrue(plan.toString(), plan.isEligible());
        assertTrue(plan.toString(), plan.runs.size() >= 2);

        long parentRows = parent.descriptor.fileFor(BtiFormat.Components.ROW_INDEX).length();
        assertTrue("the fixture produced no row indexes", parentRows > 0);

        try (Materialised materialised = materialise(getCurrentColumnFamilyStore(), parent, plan))
        {
            long sliceRows = materialised.reader.descriptor.fileFor(BtiFormat.Components.ROW_INDEX).length();
            assertTrue("slice Rows.db is " + sliceRows + " bytes of the parent's " + parentRows +
                       "; the entries between the runs are being carried",
                       sliceRows < parentRows / 2);
            assertTrue("slice Rows.db is empty, so nothing was copied at all", sliceRows > 0);
        }
    }

    /**
     * A slice with interior dead space has to be marked {@code hasUnindexedRegions} and then read through its
     * index, which for BTI means {@code BtiTableReader.indexDrivenScanner}. Without both halves a linear scan
     * would hand back the partitions between the runs -- data the receiving node was never sent.
     *
     * <p>This is also the only place a BTI sstable with an interior gap is put through {@code SortedTableVerifier},
     * which is changed on this branch: it now steps over the DEAD PREFIX rather than failing on a non-zero first
     * index position, bounds how large a prefix it will tolerate, reads the prefix so no byte goes unlooked-at, and
     * stops rather than seeking when the index reports the data length. Every one of those is reached by a slice and
     * by nothing else, and {@code nodetool verify} on a bootstrapped BTI node is where they land. BIG has the
     * equivalent in {@code ZeroCopySSTableSliceTest.verifierAndScrubberAcceptASliceWithAnInteriorGap}.
     */
    @Test
    public void interiorDeadSpaceIsMarkedAndScannedThroughTheIndex() throws Throwable
    {
        SSTableReader parent = btiSSTable(true, 1);
        List<DecoratedKey> keys = keysInOrder(parent);

        // Two ranges one partition apart in a narrow table: with 4 KiB cells they land in the same cell, so they
        // stay one run and the partition between them comes along inside it.
        Plan plan = plan(parent, keys, new int[][]{ { 20, 21 }, { 22, 23 } });
        assertTrue(plan.toString(), plan.isEligible());
        assertEquals(plan.toString(), 1, plan.runs.size());
        assertTrue("no interior dead space, so this test proves nothing: " + plan, plan.interiorDeadBytes() > 0);

        List<DecoratedKey> expected = new ArrayList<>();
        expected.add(keys.get(20));
        expected.add(keys.get(22));

        try (Materialised materialised = materialise(getCurrentColumnFamilyStore(), parent, plan))
        {
            SSTableReader slice = materialised.reader;
            assertTrue("the marker was not set", slice.hasUnindexedRegions());
            // getScanner() has to route to the index-driven scanner rather than the linear one, and must not throw
            assertEquals(expected, allKeys(slice));
            assertContentMatches(parent, slice, expected);
            // No Digest.crc32 is ever produced for a slice, so this is a full extended verification either way.
            verify(getCurrentColumnFamilyStore(), slice);
        }
    }

    /**
     * The marker is INHERITED, not recomputed. A single-section slice has {@code interiorDeadBytes() == 0} by
     * construction, so a literal answer about the slice itself would clear a marker its parent was already
     * carrying -- and its Data.db still holds the partitions the parent's own copied cells dragged along, which a
     * linear scanner would then hand back.
     *
     * <p>Reachable as soon as partial streaming is on: node B is sent a multi-section slice, and node C later takes a
     * sub-range of what B has. The BTI half matters on its own because a BTI grandchild's index is a trie rebuilt
     * from the parent's, so nothing about the copy would notice the carried partitions -- only the marker does.
     */
    @Test
    public void markerIsInheritedBySingleSectionSliceOfAMarkedParent() throws Throwable
    {
        SSTableReader parent = btiSSTable(true, 1);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<DecoratedKey> keys = keysInOrder(parent);

        Plan plan = plan(parent, keys, new int[][]{ { 20, 21 }, { 22, 23 } });
        assertTrue(plan.toString(), plan.isEligible());
        assertTrue(plan.toString(), plan.interiorDeadBytes() > 0);

        List<DecoratedKey> claimed = new ArrayList<>();
        claimed.add(keys.get(20));
        claimed.add(keys.get(22));

        try (Materialised materialised = materialise(cfs, parent, plan))
        {
            SSTableReader slice = materialised.reader;
            assertTrue("the fixture is not marked, so inheritance cannot be tested", slice.hasUnindexedRegions());
            assertEquals(claimed, allKeys(slice));

            // One contiguous section over everything the slice claims. It necessarily spans the partition the slice
            // carries but does not claim, which is what makes interiorDeadBytes() zero -- and what makes clearing
            // the marker unsafe.
            List<Range<Token>> everything =
                Collections.singletonList(new Range<>(parent.getPartitioner().getMinimumToken(),
                                                      parent.getPartitioner().getMinimumToken()));
            List<PartitionPositionBounds> sections = slice.getPositionsForRanges(everything);
            assertEquals("the sub-slice has to be single-section for this test to prove anything",
                         1, sections.size());

            Plan subPlan = ZeroCopySSTableSlice.plan(slice, sections, 1.0);
            assertTrue(subPlan.toString(), subPlan.isEligible());
            assertEquals("a single-section slice must have no interior dead space of its own",
                         0, subPlan.interiorDeadBytes());

            try (Materialised sub = materialise(cfs, slice, subPlan))
            {
                assertTrue("the sub-slice lost its parent's unindexed-region mark",
                           sub.reader.hasUnindexedRegions());
                assertEquals(claimed, allKeys(sub.reader));
                assertContentMatches(parent, sub.reader, claimed);
                verify(cfs, sub.reader);
            }
        }
    }

    /**
     * ...and so does a SPLIT of a marked BTI slice. A child is one contiguous chunk run and adds no unindexed region
     * of its own, but the run it copies can already contain one. BIG has
     * {@code ZeroCopySSTableSliceTest.splittingASliceKeepsTheUnindexedRegionMark}; this is its BTI twin, and it also
     * pins that a marked BTI sstable is splittable at all -- {@code isSupported} would have to say no if a BTI
     * child could not carry the marker.
     */
    @Test
    public void splittingAMarkedBtiSliceKeepsTheMark() throws Throwable
    {
        SSTableReader parent = btiSSTable(true, 1);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<DecoratedKey> keys = keysInOrder(parent);

        Plan plan = plan(parent, keys, new int[][]{ { 5, 20 }, { 30, 50 } });
        assertTrue(plan.toString(), plan.isEligible());
        assertTrue(plan.toString(), plan.interiorDeadBytes() > 0);

        try (Materialised materialised = materialise(cfs, parent, plan))
        {
            SSTableReader slice = materialised.reader;
            assertTrue("the fixture is not marked, so inheritance cannot be tested", slice.hasUnindexedRegions());
            assertTrue("the slice must be splittable for this test to prove anything",
                       ZeroCopySSTableSplitter.isSupported(slice));

            ZeroCopySSTableSplitter.Result result = ZeroCopySSTableSplitter.split(slice, 2, null);
            try
            {
                // Not exactly 2: the slice is small enough that the byte-share boundary search may collapse it to
                // one child, and the property under test is the marker, not the child count.
                assertFalse(result.toString(), result.children.isEmpty());
                for (ZeroCopySSTableSplitter.Child child : result.children)
                {
                    assertTrue("child " + child.descriptor + " lost the unindexed-region mark",
                               child.reader.hasUnindexedRegions());
                    // ...and the marker is honoured, so the children together are exactly what the slice claimed.
                    for (DecoratedKey key : allKeys(child.reader))
                        assertTrue("child cannot find its own key " + key,
                                   child.reader.getPosition(key, SSTableReader.Operator.EQ) >= 0);
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
    }

    // ----------------------------------------------------------------------------------------------------

    private void sliceAndVerify(boolean compressed, int rowsPerPartition, int[][] ranges) throws Throwable
    {
        SSTableReader parent = btiSSTable(compressed, rowsPerPartition);
        List<DecoratedKey> keys = keysInOrder(parent);
        assertEquals(PARTITIONS, keys.size());

        boolean wide = rowsPerPartition > 1;
        assertEquals("Rows.db for rowsPerPartition=" + rowsPerPartition,
                     wide, parent.descriptor.fileFor(BtiFormat.Components.ROW_INDEX).length() > 0);

        Plan plan = plan(parent, keys, ranges);
        assertTrue(plan.toString(), plan.isEligible());

        List<DecoratedKey> expected = new ArrayList<>();
        for (int[] range : ranges)
            for (int i = range[0]; i < range[1]; i++)
                expected.add(keys.get(i));

        try (Materialised materialised = materialise(getCurrentColumnFamilyStore(), parent, plan))
        {
            SSTableReader slice = materialised.reader;

            // Every component the slice wrote is one the plan named, so getNumFiles() cannot over-promise.
            assertTrue(materialised.slice.components + " is not a subset of " + plan.components(),
                       plan.components().containsAll(materialised.slice.components));
            assertTrue(materialised.slice.components.contains(BtiFormat.Components.PARTITION_INDEX));
            assertTrue(materialised.slice.components.contains(BtiFormat.Components.ROW_INDEX));
            assertFalse(materialised.slice.components.contains(BigFormat.Components.PRIMARY_INDEX));
            assertFalse(materialised.slice.components.contains(BigFormat.Components.SUMMARY));
            // Rows.db exists either way; it is zero length when nothing in the slice has a row index.
            assertTrue(slice.descriptor.fileFor(BtiFormat.Components.ROW_INDEX).exists());
            assertEquals(wide, slice.descriptor.fileFor(BtiFormat.Components.ROW_INDEX).length() > 0);

            assertEquals(expected.size(), materialised.slice.partitionCount);
            assertEquals(expected.get(0), slice.getFirst());
            assertEquals(expected.get(expected.size() - 1), slice.getLast());

            // Every key the slice claims resolves through its rebuilt Partitions.db -- which, for an indexed
            // partition, means the key read back out of the copied Rows.db entry matched.
            for (DecoratedKey key : expected)
                assertTrue("the slice cannot find its own key " + key,
                           slice.getPosition(key, SSTableReader.Operator.EQ) >= 0);

            // ... and nothing else does. A carried partition is physically in the slice's Data.db and must stay
            // unreachable, which is exactly what rebuilding the trie rather than copying it buys.
            Set<DecoratedKey> claimed = new HashSet<>(expected);
            for (DecoratedKey key : keys)
            {
                if (!claimed.contains(key) && key.compareTo(slice.getFirst()) >= 0
                    && key.compareTo(slice.getLast()) <= 0)
                {
                    assertTrue("the slice resolved a key it does not claim: " + key,
                               slice.getPosition(key, SSTableReader.Operator.EQ) < 0);
                }
            }

            assertEquals(expected, allKeys(slice));
            assertContentMatches(parent, slice, expected);
            verify(getCurrentColumnFamilyStore(), slice);
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Harness
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

        @Override
        public void close()
        {
            reader.selfRef().release();
            ZeroCopySSTableSlice.delete(slice.descriptor, components);
        }
    }

    /**
     * Synthesise the slice's components, copy the planned byte ranges of the parent's Data.db beside them, and open
     * the lot. The copy is byte for byte what {@code CassandraEntireSSTableStreamWriter} sends and
     * {@code SSTableZeroCopyWriter} writes, so a reader that works here works there.
     */
    private static Materialised materialise(ColumnFamilyStore cfs, SSTableReader parent, Plan plan) throws IOException
    {
        Descriptor target = ZeroCopySSTableSlice.newDescriptor(parent);
        ZeroCopySSTableSlice.Slice slice = ZeroCopySSTableSlice.write(parent, plan, target);

        Set<Component> components = new HashSet<>(slice.components);
        components.add(SSTableFormat.Components.DATA);
        try
        {
            try (FileChannel in = parent.descriptor.fileFor(SSTableFormat.Components.DATA).newReadChannel();
                 FileChannel out = target.fileFor(SSTableFormat.Components.DATA)
                                         .newWriteChannel(File.WriteMode.OVERWRITE))
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
                         plan.physicalBytes, target.fileFor(SSTableFormat.Components.DATA).length());

            components.add(SSTableFormat.Components.TOC);
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

    /** Every expected partition, in order, byte for byte what the parent holds for it. */
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

                    while (parentScanner.hasNext())
                    {
                        try (UnfilteredRowIterator candidate = parentScanner.next())
                        {
                            if (!wanted.contains(candidate.partitionKey()))
                                continue;
                            assertEquals("out of order", expected.get(compared), candidate.partitionKey());
                            assertSamePartition(candidate, actual);
                            break;
                        }
                    }
                    compared++;
                }
            }
        }
        assertEquals("the slice is missing partitions", expected.size(), compared);
    }

    private static void assertSamePartition(UnfilteredRowIterator expected, UnfilteredRowIterator actual)
    {
        String context = "partition " + expected.partitionKey();
        assertEquals(context + ": deletion", expected.partitionLevelDeletion(), actual.partitionLevelDeletion());
        assertEquals(context + ": static row", expected.staticRow(), actual.staticRow());
        assertEquals(context + ": columns", expected.columns(), actual.columns());
        int row = 0;
        while (expected.hasNext())
        {
            assertTrue(context + ": slice ran out at row " + row, actual.hasNext());
            assertEquals(context + " row " + row, expected.next(), actual.next());
            row++;
        }
        assertFalse(context + ": slice has extra rows", actual.hasNext());
    }

    /** {@code nodetool verify}'s extended pass over a slice, which is not tracked by the cfs, hence offline. */
    private static void verify(ColumnFamilyStore cfs, SSTableReader slice)
    {
        try (IVerifier verifier = slice.getVerifier(cfs, new OutputHandler.LogOutput(), true,
                                                    IVerifier.options().extendedVerification(true).build()))
        {
            verifier.verify();
        }
    }

    private static List<DecoratedKey> allKeys(SSTableReader sstable)
    {
        List<DecoratedKey> keys = new ArrayList<>();
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    keys.add(partition.partitionKey().retainable());
                }
            }
        }
        return keys;
    }

    private static List<DecoratedKey> keysInOrder(SSTableReader sstable) throws IOException
    {
        List<DecoratedKey> keys = new ArrayList<>();
        try (KeyReader reader = sstable.keyReader())
        {
            while (!reader.isExhausted())
            {
                keys.add(sstable.decorateKey(reader.key()).retainable());
                reader.advance();
            }
        }
        return keys;
    }

    /**
     * The sections {@code createOutgoingStreams} would ask for, for each {@code [from, to)} of key indices. Token
     * ranges are half-open on the left, so a range that is to start at key {@code from} is bounded by the token of
     * the key before it -- or by the minimum token when there is none.
     */
    private static Plan plan(SSTableReader parent, List<DecoratedKey> keys, int[][] ranges)
    {
        List<Range<Token>> tokenRanges = new ArrayList<>(ranges.length);
        for (int[] range : ranges)
        {
            Token left = range[0] == 0 ? parent.getPartitioner().getMinimumToken()
                                       : keys.get(range[0] - 1).getToken();
            tokenRanges.add(new Range<>(left, keys.get(range[1] - 1).getToken()));
        }
        List<PartitionPositionBounds> sections = parent.getPositionsForRanges(tokenRanges);
        assertNotNull(sections);
        assertFalse("no sections for " + Arrays.deepToString(ranges), sections.isEmpty());
        return ZeroCopySSTableSlice.plan(parent, sections, 1.0);
    }

    private static Plan plan(SSTableReader parent, List<DecoratedKey> keys, int from, int to)
    {
        return plan(parent, keys, new int[][]{ { from, to } });
    }

    /**
     * One flushed BTI sstable of {@link #PARTITIONS} partitions.
     *
     * <p>The shape is deliberately not flat. {@link #assertSamePartition} compares {@code partitionLevelDeletion()},
     * {@code staticRow()} and every {@code Unfiltered} with {@code equals}, and each of those comparisons is vacuous
     * unless the data carries the thing being compared -- so this writes a partition-level tombstone under live rows,
     * a static row on most but not all partitions, TTL'd cells, a collection, a row tombstone, a range tombstone
     * where there are enough rows for one, and a cell deletion, over TWO clustering columns (a clustering prefix of
     * more than one component is what the row index trie's keys are built from).
     *
     * @param rowsPerPartition 1 for the narrow shape -- no row index at all, so Rows.db is empty in the parent and in
     *                         every slice and every key has to come out of Data.db -- and {@link #WIDE_ROWS} for the
     *                         wide one, where every partition is well past {@code column_index_size}
     */
    private SSTableReader btiSSTable(boolean compressed, int rowsPerPartition) throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck0 int, ck1 text, sv text static, val text, m map<int, text>, " +
                    "t text, PRIMARY KEY (pk, ck0, ck1)) WITH compression = " +
                    (compressed ? "{'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}" : "{'enabled': 'false'}"));
        disableCompaction();

        for (int p = 0; p < PARTITIONS; p++)
        {
            String pk = String.format("k%06d", p);

            // Older than every row below, so the rows survive and the deletion stays in the partition header.
            if (p % 5 == 0)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", OLD_TS, pk);
            if (p % 3 != 0)
                execute("INSERT INTO %s (pk, sv) VALUES (?, ?) USING TIMESTAMP ?", pk, "static-" + p, NEW_TS);

            for (int c = 0; c < rowsPerPartition; c++)
            {
                // A TTL'd row carrying a collection. Reached in the narrow shape too, where c is always 0.
                if (c % 7 == 3 || (rowsPerPartition == 1 && p % 4 == 3))
                {
                    Map<Integer, String> collection = new HashMap<>();
                    collection.put(c, "m" + c);
                    collection.put(c + 1, "m" + (c + 1));
                    execute("INSERT INTO %s (pk, ck0, ck1, val, m) VALUES (?, ?, ?, ?, ?) " +
                            "USING TIMESTAMP ? AND TTL 8640000",
                            pk, c, "c" + c, randomText(VALUE_BYTES), collection, NEW_TS);
                }
                else
                {
                    execute("INSERT INTO %s (pk, ck0, ck1, val) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                            pk, c, "c" + c, randomText(VALUE_BYTES), NEW_TS);
                }
            }

            // A row tombstone over the last row, so no partition is ever emptied, plus a range tombstone wherever
            // there are enough rows for it to fall inside a row index block rather than at its edge.
            if (p % 4 == 1 && rowsPerPartition > 1)
            {
                int last = rowsPerPartition - 1;
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 = ? AND ck1 = ?",
                        NEW_TS + 1, pk, last, "c" + last);
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 > ? AND ck0 < ?",
                        NEW_TS + 1, pk, rowsPerPartition / 2, rowsPerPartition / 2 + 3);
            }
            if (p % 6 == 2)
                execute("DELETE t FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 = ? AND ck1 = ?",
                        NEW_TS + 1, pk, 0, "c0");
        }
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals("expected exactly one sstable", 1, live.size());
        SSTableReader sstable = live.iterator().next();

        assertTrue("expected a BTI sstable, got " + sstable.descriptor.getFormat().name(),
                   BtiFormat.is(sstable.descriptor.getFormat()));
        // Guards against the class degenerating into a suite of refusal tests: plan() answers
        // NO_UNINDEXED_REGIONS_MARKER for a version below 'eb', which would make every isEligible() assertion below
        // fail with no hint as to why.
        assertTrue("the fixture flushed version '" + sstable.descriptor.version.version + "', which cannot carry " +
                   "hasUnindexedRegions -- plan() refuses it, so nothing here tests what it says it does",
                   sstable.descriptor.version.hasUnindexedRegionsMarker());
        assertEquals(compressed, sstable.compression);
        return sstable;
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

    @SuppressWarnings("unused")
    private static List<PartitionPositionBounds> wholeFile(SSTableReader parent)
    {
        return Collections.singletonList(new PartitionPositionBounds(0, parent.uncompressedLength()));
    }
}
