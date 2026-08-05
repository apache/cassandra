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
package org.apache.cassandra.io.sstable.format.bti;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TestDatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PageAware;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The two primitives a BTI zero-copy rebase rests on, tested directly rather than through a split or a slice:
 * {@link BtiZeroCopySplit#writeUnsignedVIntOfWidth} (pure arithmetic) and the {@link BtiZeroCopySplit.Cursor} /
 * {@link BtiZeroCopySplit.RowIndexCopier} pair over a real sstable.
 *
 * <p>The Rows.db placement rule is what the second half is about, and it cannot be observed from the outside: a trie
 * node is only ever read out of one rebuffered page, so an entry placed such that one of its nodes straddles a page
 * boundary produces a garbage row position or an exception at LOOKUP time, long after the write that caused it. Two
 * of {@link BtiZeroCopySplit.RowIndexCopier#align}'s three placements are reached by any wide fixture; the third --
 * a partition whose row index trie is itself larger than {@link PageAware#PAGE_SIZE} -- needs a fixture built for it,
 * which is what {@link #everyRowIndexedSSTable} is.
 */
public class BtiZeroCopySplitTest extends CQLTester
{
    private SSTableFormat<?, ?> savedFormat;
    private int savedColumnIndexKb;

    @Before
    public void selectBtiFormat()
    {
        savedFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        savedColumnIndexKb = DatabaseDescriptor.getColumnIndexSizeInKiB();
        TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(BtiFormat.NAME);
    }

    /**
     * {@code forceAlignedLayoutForTesting} is package-private to {@code org.apache.cassandra.io.sstable} and cannot
     * be reached from here; nothing in this class sets it, and the two BTI tests that live in that package clear it.
     */
    @After
    public void restoreSettings()
    {
        ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
        DatabaseDescriptor.setColumnIndexSizeInKiB(savedColumnIndexKb);
        if (savedFormat != null)
            TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(savedFormat);
    }

    // ----------------------------------------------------------------------------------------------------
    // writeUnsignedVIntOfWidth
    // ----------------------------------------------------------------------------------------------------

    /**
     * {@link BtiZeroCopySplit#writeUnsignedVIntOfWidth} is the primitive that lets a child's Rows.db entry keep the
     * parent's byte length while its data position changes, which is what keeps every following entry -- and
     * therefore the trie writer's page geometry -- exactly where the parent put it.
     *
     * <p>Two claims are being pinned. That a deliberately over-long encoding decodes back to the value written, which
     * holds because {@code VIntCoding} takes the encoded width from the leading byte's set-bit count and never checks
     * minimality. And that the widths this is used with are always sufficient, which holds because a rebased position
     * is never larger than the position it replaces.
     */
    @Test
    public void paddedEncodingRoundTripsAtEveryWidth() throws IOException
    {
        for (int width = 1; width <= 9; width++)
        {
            for (long value : interestingValues(width))
            {
                if (VIntCoding.computeUnsignedVIntSize(value) > width)
                    continue;

                byte[] buffer = new byte[16];
                // Written at a non-zero offset, because that is how it is used: in place, inside a copied entry.
                BtiZeroCopySplit.writeUnsignedVIntOfWidth(value, width, buffer, 3);

                ByteBuffer wrapped = ByteBuffer.wrap(buffer);
                assertEquals("width " + width + " value " + value,
                             value, VIntCoding.getUnsignedVInt(wrapped, 3));
                // The whole point: the decoder takes exactly the width we chose, not the canonical one.
                assertEquals("width " + width + " value " + value,
                             width, VIntCoding.readLengthOfVInt(wrapped, 3));
            }
        }
    }

    /** The canonical encoder and this one agree whenever the requested width IS the canonical one. */
    @Test
    public void matchesTheCanonicalEncoderAtCanonicalWidth() throws IOException
    {
        Random random = new Random(0x5eed);
        for (int i = 0; i < 10_000; i++)
        {
            long value = Math.abs(random.nextLong()) >>> random.nextInt(64);
            int width = VIntCoding.computeUnsignedVIntSize(value);

            byte[] mine = new byte[16];
            BtiZeroCopySplit.writeUnsignedVIntOfWidth(value, width, mine, 0);

            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                out.writeUnsignedVInt(value);
                byte[] theirs = out.toByteArray();
                assertEquals(width, theirs.length);
                for (int b = 0; b < width; b++)
                    assertEquals("value " + value + " byte " + b, theirs[b], mine[b]);
            }
        }
    }

    /**
     * The rebasing invariant, which is what makes "same width always fits" true rather than merely usually true:
     * for any position and any shift no greater than it, the rebased value needs no more bytes than the original.
     */
    @Test
    public void rebasedPositionNeverNeedsMoreBytes()
    {
        Random random = new Random(0xd1ce);
        for (int i = 0; i < 100_000; i++)
        {
            long position = Math.abs(random.nextLong()) >>> random.nextInt(64);
            long shift = position == 0 ? 0 : Math.abs(random.nextLong()) % (position + 1);
            int before = VIntCoding.computeUnsignedVIntSize(position);
            int after = VIntCoding.computeUnsignedVIntSize(position - shift);
            assertTrue(position + " - " + shift + ": " + before + " -> " + after, after <= before);

            byte[] buffer = new byte[16];
            BtiZeroCopySplit.writeUnsignedVIntOfWidth(position - shift, before, buffer, 0);
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer)))
            {
                assertEquals(position - shift, VIntCoding.readUnsignedVInt(in));
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
        }
    }

    @Test
    public void rejectsAWidthThatCannotHoldTheValue()
    {
        try
        {
            BtiZeroCopySplit.writeUnsignedVIntOfWidth(1L << 20, 1, new byte[16], 0);
            fail("expected a refusal: a 21-bit value does not fit in one byte");
        }
        catch (IllegalArgumentException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("needs"));
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // The Rows.db placement rule
    // ----------------------------------------------------------------------------------------------------

    /**
     * CHARACTERISATION TEST OF A KNOWN, DELIBERATELY UNFIXED DEFECT. This test asserts the WRONG behaviour on
     * purpose, and fixing the defect is expected to make it fail -- at which point it should be inverted, not
     * deleted.
     *
     * <p>The defect is documented in full on {@link BtiZeroCopySplit.Cursor#rowIndexBlockStart()}, which is the
     * authority; the short form is that {@code previousRowIndexEntryEnd} has no initialiser and is only ever advanced
     * from records the cursor YIELDS, so a cursor built with a {@code left} bound -- which descends the trie straight
     * to that key and yields nothing below it -- reports {@code 0} as the first entry's node region start instead of
     * the true one. {@link BtiZeroCopySplit.RowIndexCopier#copy} then takes {@code [0, keyStart)} as that entry's node
     * region and copies the parent's ENTIRE Rows.db prefix into the target: every row index entry of every partition
     * below the bound.
     *
     * <p>It is amplification, not corruption, and that is exactly what is asserted below: the copy lands at
     * {@code delta == 0}, so the payload {@code copy} returns is the parent's own {@code keyStart}, and with a zero
     * shift the target's Rows.db is BYTE-IDENTICAL to the parent's prefix -- every self-relative node pointer, the
     * trailer's root pointer and the page geometry of everything after it are as correct as they would be with the
     * right block start. Nothing can reach the extra bytes, because the only way into a Rows.db entry is a
     * Partitions.db payload and the target's rebuilt trie has one for none of them. What it costs is unbounded: the
     * whole of the parent's Rows.db below the slice, regardless of the slice's size.
     *
     * <p>Only the SLICE path builds a bounded cursor ({@code ZeroCopySSTableSlice.walk} -> {@code BtiCursor}); the
     * split path walks from the beginning and is unaffected.
     *
     * <p>One further consequence is pinned here because it is a TEST-COVERAGE claim and nothing else records it: with
     * a prefix larger than a page -- which is what the fixture below guarantees, and what any real slice of a wide
     * table has -- {@code nodeBytes} exceeds {@link PageAware#PAGE_SIZE}, so {@code align} takes its MULTI-PAGE
     * branch and bumps {@code congruenceAlignments}. That is what currently satisfies
     * {@code ZeroCopyBtiFuzzTest}'s {@code congruence > 0} coverage assertion, so that assertion is not evidence that
     * the genuine multi-page-trie placement has been tested. It has its own test:
     * {@link #aGenuinelyMultiPageRowIndexTrieIsPlacedAtItsCongruentOffset}, which reaches the branch through the
     * SPLIT path where the cursor is unbounded and the block start is therefore right.
     */
    @Test
    public void boundedCursorFirstCopyCarriesTheParentsRowsDbPrefix() throws Throwable
    {
        SSTableReader parent = everyRowIndexedSSTable(10, 400);
        List<DecoratedKey> keys = keysInOrder(parent);
        long parentRowsLength = parent.descriptor.fileFor(BtiFormat.Components.ROW_INDEX).length();
        assertTrue("the fixture produced no row indexes", parentRowsLength > 0);

        Descriptor target = scratchDescriptor(parent);
        long congruenceBefore = BtiZeroCopySplit.RowIndexCopier.congruenceAlignments.sum();
        long blockStart;
        long keyStart;
        long entryEnd;
        long payload;

        // Bounded half way in, the way ZeroCopySSTableSlice.walk bounds its cursor at the slice's first key. Half
        // rather than one or two partitions in, so that the prefix is many pages wide even though the trie's bound is
        // on stored key PREFIXES and may therefore yield a record or two below the key asked for.
        try (BtiZeroCopySplit.Cursor cursor = BtiZeroCopySplit.cursor(parent, keys.get(keys.size() / 2));
             BtiZeroCopySplit.RowIndexCopier copier =
                 new BtiZeroCopySplit.RowIndexCopier(parent, target, SequentialWriterOption.DEFAULT))
        {
            assertTrue("the bounded cursor yielded nothing", cursor.advance());
            assertEquals("the first record of the walk is the one the defect applies to", 0, cursor.index());
            assertTrue("the fixture must index every row, so every partition has a row index", cursor.hasRowIndex());

            blockStart = cursor.rowIndexBlockStart();
            keyStart = cursor.rowIndexKeyStart();
            entryEnd = cursor.rowIndexEntryEnd();

            // THE DEFECT. The true node region of this entry starts just after the previous partition's entry, which
            // is tens of kilobytes in; the cursor says 0 because nothing ever advanced it past the entries below the
            // bound.
            assertEquals("the defect has been fixed -- see Cursor.rowIndexBlockStart()'s javadoc; invert this test",
                         0, blockStart);
            assertTrue("the bound is not far enough in for this to test anything: keyStart=" + keyStart,
                       keyStart > PageAware.PAGE_SIZE);

            // Shift zero, so the patched vint re-encodes to exactly the parent's bytes and the copy is byte-identical.
            payload = copier.copy(cursor, cursor.dataPosition());
            assertEquals("the copy did not land at delta == 0", keyStart, payload);
            assertEquals("the target's Rows.db is not the parent's prefix plus this one entry",
                         entryEnd, copier.finish());
        }

        // ...and the multi-page branch of align() is what the amplified node region took, which is the coverage claim
        // ZeroCopyBtiFuzzTest's `congruence > 0` assertion is currently resting on.
        assertEquals("the amplified node region should have taken align()'s multi-page branch",
                     1, BtiZeroCopySplit.RowIndexCopier.congruenceAlignments.sum() - congruenceBefore);

        byte[] parentRows = Files.readAllBytes(parent.descriptor
                                               .fileFor(BtiFormat.Components.ROW_INDEX).toPath());
        byte[] targetRows = Files.readAllBytes(target.fileFor(BtiFormat.Components.ROW_INDEX).toPath());
        assertEquals(entryEnd, targetRows.length);
        assertArrayEquals("with a zero shift the copy must be byte-identical to the parent's prefix",
                          Arrays.copyOfRange(parentRows, 0, (int) entryEnd),
                          targetRows);
        // Stated as a size, which is the cost, and it is the exact inverse of what
        // ZeroCopySSTableSliceBtiTest.skippedPartitionsRowIndexesAreNotCarried asserts for the entries ABOVE the
        // bound: a target holding ONE entry has a Rows.db that is a large fraction of the whole parent's.
        assertTrue("a one-entry target's Rows.db is " + targetRows.length + " of the parent's " + parentRowsLength +
                   " bytes; the amplification is gone, which means the defect is fixed -- invert this test",
                   targetRows.length > parentRowsLength / 3);

        target.fileFor(BtiFormat.Components.ROW_INDEX).tryDelete();
    }

    /**
     * The third placement in {@link BtiZeroCopySplit.RowIndexCopier#align}, reached GENUINELY: a single partition
     * whose row index trie is by itself larger than {@link PageAware#PAGE_SIZE}, so the entry cannot be made to fit
     * inside one page and has to be placed at the same offset within a page as it had in the parent.
     *
     * <p>This is the branch nothing else covers. {@code ZeroCopyBtiFuzzTest}'s {@code congruence > 0} assertion is
     * satisfied by the amplification defect above rather than by a real multi-page trie (see
     * {@link #boundedCursorFirstCopyCarriesTheParentsRowsDbPrefix}), and every ordinary fixture's per-partition trie
     * is far smaller than 4 KiB. So the fixture is built for it: {@code column_index_size = 0} indexes every row, and
     * 1200 rows per partition put each partition's trie past a page. That row count is empirical, not decorative --
     * an index entry costs roughly ten trie bytes, so 400 rows lands just UNDER {@link PageAware#PAGE_SIZE} and the
     * guard below fails; if it ever fails again, raise the row count rather than lowering the guard.
     *
     * <p>Driven through a real SPLIT, whose cursor is unbounded and whose block starts are therefore correct, and
     * into more than one child so that a child's writer starts at 0 while the entry's node region begins pages in --
     * which is what makes the congruence padding non-zero rather than incidentally zero. If the placement were wrong,
     * a node would straddle a page boundary in the child and the clustering-restricted reads below would return a
     * garbage position or throw; a linear scan alone would not notice.
     */
    @Test
    public void aGenuinelyMultiPageRowIndexTrieIsPlacedAtItsCongruentOffset() throws Throwable
    {
        SSTableReader parent = everyRowIndexedSSTable(6, 1200);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<DecoratedKey> keys = keysInOrder(parent);

        // Guard the fixture: without a node region wider than a page, align() can never take the branch under test
        // and this whole test is vacuous.
        int multiPage = 0;
        try (BtiZeroCopySplit.Cursor cursor = BtiZeroCopySplit.cursor(parent))
        {
            while (cursor.advance())
            {
                assertTrue("the fixture must index every row", cursor.hasRowIndex());
                long nodeBytes = cursor.rowIndexKeyStart() - cursor.rowIndexBlockStart();
                if (nodeBytes > PageAware.PAGE_SIZE)
                    multiPage++;
            }
        }
        assertTrue("no partition's row index trie exceeds " + PageAware.PAGE_SIZE + " bytes, so the multi-page " +
                   "placement cannot be reached; the fixture needs more rows per partition", multiPage >= 2);

        long congruenceBefore = BtiZeroCopySplit.RowIndexCopier.congruenceAlignments.sum();
        ZeroCopySSTableSplitter.Result result = ZeroCopySSTableSplitter.split(parent, 3, null);
        try
        {
            assertTrue(result.toString(), result.children.size() > 1);
            assertTrue("the split never entered align()'s multi-page branch, so the placement under test did not run",
                       BtiZeroCopySplit.RowIndexCopier.congruenceAlignments.sum() - congruenceBefore > 0);

            List<DecoratedKey> seen = new ArrayList<>();
            for (ZeroCopySSTableSplitter.Child child : result.children)
            {
                for (DecoratedKey key : allKeys(child.reader))
                {
                    seen.add(key);
                    assertTrue("child cannot find its own key " + key,
                               child.reader.getPosition(key, SSTableReader.Operator.EQ) >= 0);
                }
                assertRowIndexReadsMatch(cfs, parent, child.reader);
            }
            assertEquals("the children do not add up to the parent", keys, seen);
        }
        finally
        {
            for (ZeroCopySSTableSplitter.Child child : result.children)
                child.reader.selfRef().release();
        }
    }

    /**
     * Clustering-restricted reads of every partition a child holds, compared against the parent's. These are the
     * reads that descend the row index trie -- a full scan does not -- so a node placed across a page boundary shows
     * up here and nowhere else.
     */
    private static void assertRowIndexReadsMatch(ColumnFamilyStore cfs, SSTableReader parent, SSTableReader child)
    {
        ClusteringComparator comparator = cfs.metadata().comparator;
        ColumnFilter columns = ColumnFilter.all(cfs.metadata());
        List<Slices> bands = new ArrayList<>();
        bands.add(Slices.ALL);
        bands.add(Slices.with(comparator, Slice.make(comparator.make(0), comparator.make(7))));
        bands.add(Slices.with(comparator, Slice.make(comparator.make(150), comparator.make(190))));
        bands.add(Slices.with(comparator, Slice.make(comparator.make(396), comparator.make(399))));
        bands.add(Slices.with(comparator, Slice.make(comparator.make(200), comparator.make(200))));

        for (DecoratedKey key : allKeys(child))
        {
            for (Slices band : bands)
            {
                for (boolean reversed : new boolean[]{ false, true })
                {
                    try (UnfilteredRowIterator want = parent.rowIterator(key, band, columns, reversed,
                                                                         SSTableReadsListener.NOOP_LISTENER);
                         UnfilteredRowIterator got = child.rowIterator(key, band, columns, reversed,
                                                                       SSTableReadsListener.NOOP_LISTENER))
                    {
                        String context = "partition " + key + " reversed=" + reversed;
                        assertEquals(context + ": deletion",
                                     want.partitionLevelDeletion(), got.partitionLevelDeletion());
                        assertEquals(context + ": static row", want.staticRow(), got.staticRow());
                        int row = 0;
                        while (want.hasNext())
                        {
                            assertTrue(context + ": child ran out at row " + row, got.hasNext());
                            assertEquals(context + " row " + row, want.next(), got.next());
                            row++;
                        }
                        assertFalse(context + ": child has extra rows", got.hasNext());
                    }
                }
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Fixture
    // ----------------------------------------------------------------------------------------------------

    /**
     * One compressed BTI sstable in which EVERY row gets its own row index block ({@code column_index_size = 0}), so
     * a single partition's row index trie is a function of its row count alone and can be pushed past
     * {@link PageAware#PAGE_SIZE} without writing megabytes.
     */
    private SSTableReader everyRowIndexedSSTable(int partitions, int rowsPerPartition) throws Throwable
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(0);

        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
        disableCompaction();

        String value = repeat('v', 80);
        for (int p = 0; p < partitions; p++)
            for (int c = 0; c < rowsPerPartition; c++)
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", String.format("k%06d", p), c, value);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals("expected exactly one sstable", 1, live.size());
        SSTableReader sstable = live.iterator().next();
        assertTrue("expected a BTI sstable, got " + sstable.descriptor.getFormat().name(),
                   BtiFormat.is(sstable.descriptor.getFormat()));
        assertTrue(sstable.compression);
        return sstable;
    }

    /** A descriptor in a scratch directory, in the parent's version, so nothing here touches a live sstable. */
    private static Descriptor scratchDescriptor(SSTableReader parent) throws IOException
    {
        File directory = new File(Files.createTempDirectory("btiZeroCopySplit"));
        return new Descriptor(parent.descriptor.version, directory, parent.descriptor.ksname,
                              parent.descriptor.cfname, parent.descriptor.id);
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

    private static String repeat(char c, int n)
    {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++)
            sb.append(c);
        return sb.toString();
    }

    private static long[] interestingValues(int width)
    {
        // Boundaries of each width's capacity, plus a few arbitrary values, since an off-by-one in the shift or in
        // the leading byte's mask would only show up right at a capacity edge.
        int valueBits = width == 9 ? 64 : 8 * (width - 1) + (8 - width);
        long max = valueBits >= 64 ? Long.MAX_VALUE : (1L << valueBits) - 1;
        return new long[]{ 0, 1, 2, 127, 128, 255, 256, max - 1, max, max >>> 1, 0x0123456789ABCDEFL & max };
    }
}
