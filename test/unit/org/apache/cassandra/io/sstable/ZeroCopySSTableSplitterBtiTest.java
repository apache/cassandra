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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TestDatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.AntiCompactionRunPlanner;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Child;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Result;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.utils.BloomFilterSerializer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.IFilter;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The zero-copy split against BTI parents, which differ from BIG in every way that matters to it: there is no
 * Index.db to rebase, Partitions.db is one trie over every key and has to be rebuilt per child, Rows.db is copied
 * verbatim with one vint patched per entry, and the full keys of partitions without a row index exist only inside
 * Data.db.
 *
 * <p>Each case is run in both shapes, because they exercise disjoint halves of the implementation:
 * <ul>
 *   <li>NARROW -- every partition below the row index granularity, so every Partitions.db payload is
 *       {@code ~dataPosition}, Rows.db is empty in the parent and in every child, and every key is read out of
 *       Data.db;</li>
 *   <li>WIDE -- every partition above it, so every payload points into Rows.db, every child copies a range of it,
 *       and every key is read out of Rows.db without decompressing anything.</li>
 * </ul>
 * The assertion that matters most is the round trip: reading every child back and comparing its partitions and
 * rows against the parent's. A rebased position that is wrong by any amount, a Rows.db range placed at an offset
 * that breaks the trie writer's page geometry, or a patched vint whose width changed would all surface there and
 * nowhere earlier.
 *
 * <p>The fixture is deliberately not one flat shape. {@link #assertSamePartition} compares
 * {@code partitionLevelDeletion()}, {@code staticRow()} and every {@code Unfiltered} with {@code equals}, and each of
 * those comparisons is vacuous unless the data actually carries the thing being compared -- so
 * {@link #writeShapes} writes a partition-level tombstone under live rows, a static row, TTL'd cells, a collection,
 * row and range tombstones and a cell deletion, over two clustering columns.
 *
 * <p>Two things are asserted about every child beyond its content, both of which only ever fail on a restart or a
 * {@code nodetool refresh} otherwise: that its TOC.txt, its component set and the files actually on disk are the
 * same three sets ({@link #assertComponents}), and that it reads back identically after a COLD
 * {@code SSTableReader.open} rather than only through the reader {@code split()} handed back.
 */
public class ZeroCopySSTableSplitterBtiTest extends CQLTester
{
    /** Older than the wall-clock timestamp of everything else, so a partition tombstone does not shadow its rows. */
    private static final long OLD_TS = 1_600_000_000_000_000L;
    private static final long NEW_TS = 1_900_000_000_000_000L;

    /** Pinned rather than inherited, so the narrow/wide split of the fixture cannot drift with the yaml. */
    private static final int COLUMN_INDEX_KB = 4;

    private SSTableFormat<?, ?> savedFormat;
    private int savedColumnIndexKb;
    private boolean savedDigestEnabled;
    private int savedPreemptiveOpenInterval;

    @Before
    public void selectBtiFormat()
    {
        savedFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        savedColumnIndexKb = DatabaseDescriptor.getColumnIndexSizeInKiB();
        savedDigestEnabled = DatabaseDescriptor.getZeroCopySplitDigestEnabled();
        savedPreemptiveOpenInterval = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(BtiFormat.NAME);
        DatabaseDescriptor.setColumnIndexSizeInKiB(COLUMN_INDEX_KB);
    }

    /**
     * Every production static this class touches is reset here rather than in the test that set it: a test that fails
     * part way through would otherwise leave the flag on for every class that runs after it in the same JVM, and
     * {@code forceAlignedLayoutForTesting} in particular silently changes the layout of every split.
     */
    @After
    public void restoreSettings()
    {
        ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = false;
        ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
        DatabaseDescriptor.setColumnIndexSizeInKiB(savedColumnIndexKb);
        DatabaseDescriptor.setZeroCopySplitDigestEnabled(savedDigestEnabled);
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(savedPreemptiveOpenInterval);
        if (savedFormat != null)
            TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(savedFormat);
    }

    @Test
    public void isSupportedIsTrueForCompressedBti() throws Throwable
    {
        SSTableReader parent = compressedBtiSSTable(false);
        assertTrue(parent.compression);
        assertTrue(ZeroCopySSTableSplitter.isSupported(parent));
        assertTrue(ZeroCopySSTableSplitter.isSupportedFormat(parent.descriptor.getFormat()));
    }

    @Test
    public void splitsNarrowPartitionsByCount() throws Throwable
    {
        splitByCountAndVerify(false, 4);
    }

    @Test
    public void splitsWidePartitionsByCount() throws Throwable
    {
        splitByCountAndVerify(true, 4);
    }

    /** One child is the degenerate case that has to keep working: the whole parent, shift 0, no pad. */
    @Test
    public void splitsIntoOneChild() throws Throwable
    {
        splitByCountAndVerify(true, 1);
    }

    /** Enough children that most of them hold a single partition. */
    @Test
    public void splitsWidePartitionsIntoManyChildren() throws Throwable
    {
        splitByCountAndVerify(true, 8);
    }

    /**
     * The aligned layout is what a reflink-capable filesystem gets, and it is the case where a child's Data.db
     * starts with a head pad. Forced on so it is covered on a filesystem that cannot share extents.
     */
    @Test
    public void splitsWidePartitionsWithAlignedLayout() throws Throwable
    {
        ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = true;
        splitByCountAndVerify(true, 3);
    }

    @Test
    public void splitsNarrowPartitionsWithAlignedLayout() throws Throwable
    {
        ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = true;
        splitByCountAndVerify(false, 3);
    }

    /** The planner is now expected to say yes, and to hand back boundaries the splitter can use. */
    @Test
    public void plannerAcceptsBti() throws Throwable
    {
        SSTableReader parent = compressedBtiSSTable(true);

        List<DecoratedKey> keys = allKeys(parent);
        // A range that ends part way through the sstable, so the labelling produces at least two runs.
        Token middle = keys.get(keys.size() / 2).getToken();
        RangesAtEndpoint ranges = fullOnly(new Range<>(parent.getFirst().getToken(), middle));

        AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.plan(parent, ranges, nextTimeUUID());

        assertNotNull(plan);
        assertTrue("planner refused a BTI parent: " + plan.ineligibleReason, plan.eligible);
        assertTrue(plan.toString(), plan.runCount >= 2);
        assertFalse(plan.boundaries.isEmpty());
        assertEquals(plan.boundaries.size() + 1, plan.perChild.size());
    }

    /**
     * The streaming-side slice accepts BTI too; {@code ZeroCopySSTableSliceBtiTest} is where it is exercised. Kept
     * here so that the two entry points a BTI cluster reaches -- the anticompaction planner and the outgoing-file
     * planner -- are both pinned in one place.
     */
    @Test
    public void slicePlanAcceptsBti() throws Throwable
    {
        SSTableReader parent = compressedBtiSSTable(false);

        List<PartitionPositionBounds> wholeFile =
            Collections.singletonList(new PartitionPositionBounds(0, parent.uncompressedLength()));

        ZeroCopySSTableSlice.Plan plan = ZeroCopySSTableSlice.plan(parent, wholeFile, 1.0);

        assertTrue(plan.toString(), plan.isEligible());
        assertEquals(ZeroCopySSTableSlice.Reason.ELIGIBLE, plan.reason);
        assertEquals(ZeroCopySSTableSlice.COMPRESSED_BTI_COMPONENTS, plan.components());
        assertFalse(plan.runs.isEmpty());
    }

    // ------------------------------------------------------------------------------------------------
    // Digest.crc32
    // ------------------------------------------------------------------------------------------------

    /**
     * With {@code zero_copy_split_digest_enabled} on -- the default -- every BTI child gets a Digest.crc32 whose
     * content is the decimal CRC32 of every PHYSICAL byte of its Data.db, head pad included. The pad is what makes
     * this worth asserting separately from BIG: {@code Verifier} CRCs the whole file with no reference to
     * {@code dataLength}, so a digest computed over the live range only would make every aligned child fail
     * {@code nodetool verify}.
     */
    @Test
    public void digestCoversEveryPhysicalByteOfAChild() throws Throwable
    {
        DatabaseDescriptor.setZeroCopySplitDigestEnabled(true);
        ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = true;   // so that there is a head pad to cover

        SSTableReader parent = compressedBtiSSTable(true);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        Result result = ZeroCopySSTableSplitter.split(parent, 3, null);
        try
        {
            boolean anyPad = false;
            for (Child child : result.children)
            {
                String context = "child " + child.descriptor;
                assertTrue(context + ": DIGEST must be a component",
                           child.components.contains(SSTableFormat.Components.DIGEST));
                assertEquals(context + ": digest",
                             Long.toString(crc32Of(child.descriptor.fileFor(SSTableFormat.Components.DATA))),
                             readDigest(child.descriptor));
                anyPad |= child.headPadBytes > 0;
            }
            assertTrue("no child had a head pad, so the pad is not covered by this run", anyPad);
            assertComponents(cfs, result);
        }
        finally
        {
            release(result);
        }
    }

    /**
     * ...and with it off, the component is absent from all three places that have to agree -- the set, TOC.txt and
     * the directory -- while everything else about the children is unchanged. Digest.crc32 is the only component
     * whose cost is proportional to the DATA rather than to the index, so this is the flag that takes a BTI split
     * down to its Partitions.db/Rows.db pass, and skipping it has to be a supported state and not a broken one.
     */
    @Test
    public void digestIsOptionalForBti() throws Throwable
    {
        DatabaseDescriptor.setZeroCopySplitDigestEnabled(false);

        SSTableReader parent = compressedBtiSSTable(true);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        Result result = ZeroCopySSTableSplitter.split(parent, 3, null);
        try
        {
            for (Child child : result.children)
            {
                String context = "child " + child.descriptor;
                assertFalse(context + ": Digest.crc32 must not have been written",
                            child.descriptor.fileFor(SSTableFormat.Components.DIGEST).exists());
                assertFalse(context + ": DIGEST must not be a component",
                            child.components.contains(SSTableFormat.Components.DIGEST));
                assertFalse(context + ": TOC must not list DIGEST",
                            TOCComponent.loadTOC(child.descriptor, false).contains(SSTableFormat.Components.DIGEST));
                assertFalse(context + ": nothing on disk may claim DIGEST",
                            child.descriptor.discoverComponents().contains(SSTableFormat.Components.DIGEST));
            }

            assertComponents(cfs, result);
            assertConcatenatedContentEquals(parent, readers(result));
            assertColdReopenReadsTheSame(cfs, parent, result);
        }
        finally
        {
            release(result);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Refusals
    // ------------------------------------------------------------------------------------------------

    /** An uncompressed BTI parent is refused up front, exactly as an uncompressed BIG one is. */
    @Test
    public void uncompressedBtiParentIsRefused() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        disableCompaction();
        for (int p = 0; p < 20; p++)
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", String.format("k%06d", p), 0, repeat('v', 300));
        flush();

        SSTableReader parent = onlySSTable(getCurrentColumnFamilyStore());
        assertTrue(BtiFormat.is(parent.descriptor.getFormat()));
        assertFalse(parent.compression);
        assertFalse(ZeroCopySSTableSplitter.isSupported(parent));

        try
        {
            ZeroCopySSTableSplitter.split(parent, 2, null);
            fail("expected an uncompressed BTI parent to be refused");
        }
        catch (UnsupportedOperationException e)
        {
            assertTrue(e.getMessage(),
                       e.getMessage().startsWith(ZeroCopySSTableSplitter.UNCOMPRESSED_UNSUPPORTED_MESSAGE));
        }
    }

    /**
     * A BTI parent below {@code eb} is refused, by both the splitter and the slice planner.
     *
     * <p>{@code eb} is the version that can record {@code StatsMetadata.hasUnindexedRegions}, and a child or a slice
     * inherits its parent's version, so producing one in {@code ea} would write a marker that an {@code ea} reader
     * ignores -- and an ignored marker means a linear scan handing back partitions the sstable does not claim. The
     * refusal is therefore the whole safety property of the version gate, and nothing else covers it: every fixture
     * in this branch flushes at {@code current_version}.
     *
     * <p>{@code ea} and {@code eb} differ in nothing but this flag ({@code BtiFormat.BtiVersion}), which is what
     * makes relabelling a real sstable a faithful stand-in for one written by 6.0: every other version-gated reader
     * decision, including {@code DeletionTime}'s serializer, is identical, and the one extra byte an {@code eb}
     * Statistics.db carries is simply not read ({@code MetadataSerializer} hands each component its own bytes).
     */
    @Test
    public void btiParentBelowEbIsRefused() throws Throwable
    {
        SSTableReader parent = compressedBtiSSTable(false);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertTrue("a flush is expected to produce a version that can carry the marker",
                   parent.descriptor.version.hasUnindexedRegionsMarker());

        // 'ea' is the last BTI version without the marker. Hard-coded on purpose: it is a historical version, so it
        // stays a valid stand-in for a 6.0 sstable no matter how many times current_version is bumped after it.
        Set<Component> components = new LinkedHashSet<>(parent.descriptor.discoverComponents());
        Descriptor legacy = new Descriptor("ea",
                                           parent.descriptor.directory,
                                           parent.descriptor.ksname,
                                           parent.descriptor.cfname,
                                           SSTableIdFactory.instance.defaultBuilder()
                                                                    .generator(Stream.empty()).get(),
                                           parent.descriptor.getFormat());
        SSTableReader legacyReader = null;
        try
        {
            for (Component component : components)
            {
                Files.copy(parent.descriptor.fileFor(component).toPath(),
                           legacy.fileFor(component).toPath());
            }
            assertFalse(legacy.version.hasUnindexedRegionsMarker());

            legacyReader = SSTableReader.open(cfs, legacy, components, cfs.metadata);
            assertFalse("isSupported must reject a pre-eb BTI parent",
                        ZeroCopySSTableSplitter.isSupported(legacyReader));

            try
            {
                ZeroCopySSTableSplitter.split(legacyReader, 2, null);
                fail("expected a pre-eb BTI parent to be refused");
            }
            catch (UnsupportedOperationException e)
            {
                assertTrue(e.getMessage(),
                           e.getMessage().startsWith(ZeroCopySSTableSplitter.LEGACY_VERSION_UNSUPPORTED_MESSAGE));
            }

            // The slice planner refuses by returning a reason rather than throwing, since a refusal there only costs
            // the row-by-row stream.
            List<PartitionPositionBounds> wholeFile =
                Collections.singletonList(new PartitionPositionBounds(0, legacyReader.uncompressedLength()));
            ZeroCopySSTableSlice.Plan plan = ZeroCopySSTableSlice.plan(legacyReader, wholeFile, 1.0);
            assertFalse(plan.toString(), plan.isEligible());
            assertEquals(ZeroCopySSTableSlice.Reason.NO_UNINDEXED_REGIONS_MARKER, plan.reason);
        }
        finally
        {
            if (legacyReader != null)
                legacyReader.selfRef().release();
            // Not left behind: an sstable of an older version sitting in a live data directory is exactly the thing
            // a later refresh or scrub in the same JVM would trip over.
            for (Component component : components)
                legacy.fileFor(component).tryDelete();
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Compaction-produced parents
    // ------------------------------------------------------------------------------------------------

    /**
     * A BTI parent produced by COMPACTION rather than by a flush, which is the shape that caught the trailing-chunk
     * bug for BIG and is the shape anticompaction actually gets.
     *
     * <p>A compaction-produced Data.db carries one extra CompressionInfo.db offset past its last data chunk (the
     * writer's early-open bookkeeping), so {@code offsets.length == chunks + 1} and the physical file is longer than
     * the last data chunk's end. A split that derived its last child's extent from {@code offsets.length} rather than
     * from {@code dataLength} copied those trailing bytes as slack, which made the child's final chunk claim to be
     * longer than it was: every read of it then failed its inline CRC32, or -- once the inflated length crossed
     * {@code maxCompressedLength} -- took the raw-chunk branch and returned compressed bytes as row data.
     * Digest.crc32 cannot catch that, being computed over whatever bytes were written, and the parent has been
     * obsoleted by then.
     *
     * <p>Nothing about that arithmetic is format-specific, which is exactly why it needs a BTI case: the BIG test
     * asserts it through {@code Assume.assumeTrue(BigFormat.isSelected())} and so never runs here, and a BTI child
     * additionally has to have its Rows.db range and rebuilt Partitions.db line up with the corrected extent.
     */
    @Test
    public void splitOfCompactionProducedBtiParentDoesNotAbsorbTheTrailingChunk() throws Throwable
    {
        // What conf/cassandra.yaml ships and what Config defaults to; pinned so a future change to either yaml
        // cannot silently stop this test from producing a trailing chunk.
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(50);

        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
        disableCompaction();
        for (int round = 0; round < 2; round++)
        {
            for (int p = 0; p < 60; p++)
                for (int c = 0; c < 5; c++)
                    execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)",
                            String.format("k%06d", p), c + round * 5, repeat('v', 480));
            flush();
        }

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals("need two sstables to have something to compact", 2, cfs.getLiveSSTables().size());
        cfs.forceMajorCompaction();
        SSTableReader compacted = onlySSTable(cfs);

        // The on-disk view: the live reader of a just-finished compaction can be an early-open one whose
        // compressedFileLength stops short of the trailing chunk, and the trailing chunk is the whole point.
        SSTableReader parent = SSTableReader.open(cfs, compacted.descriptor, compacted.getComponents(), cfs.metadata);
        try
        {
            assertTrue(BtiFormat.is(parent.descriptor.getFormat()));

            long[] offsets = readChunkOffsets(parent.descriptor);
            CompressionMetadata meta = parent.getCompressionMetadata();
            int chunkLength = meta.chunkLength();
            int dataChunks = (int) ((meta.dataLength + chunkLength - 1) / chunkLength);
            long physical = parent.descriptor.fileFor(SSTableFormat.Components.DATA).length();

            // Guard the guard. If compaction ever stops emitting the trailing chunk, or the reopen stops exposing
            // it, this test silently stops testing anything -- so fail loudly instead.
            assertEquals("a compaction-produced sstable is expected to carry exactly one trailing " +
                         "zero-uncompressed-length chunk; without it this test cannot exercise the regression",
                         dataChunks + 1, offsets.length);
            assertEquals("the parent must be the on-disk view, whose length includes the trailing chunk",
                         physical, meta.compressedFileLength);
            assertTrue("the trailing chunk must put the physical end past the last data chunk",
                       physical > offsets[dataChunks]);
            assertTrue("more than one chunk, otherwise the whole exercise is trivial", dataChunks > 20);

            Result result = ZeroCopySSTableSplitter.split(parent, 3, null);
            try
            {
                assertEquals(3, result.children.size());

                Child last = result.children.get(result.children.size() - 1);
                assertEquals("the last child must end at the last DATA chunk", dataChunks - 1, last.lastChunk);
                assertEquals("the last child must stop at the end of the last data chunk",
                             offsets[dataChunks] - offsets[(int) last.firstChunk], last.physicalBytes);
                assertEquals("and that must be its exact on-disk length, head pad aside",
                             last.onDiskLength(), last.descriptor.fileFor(SSTableFormat.Components.DATA).length());
                assertTrue("the trailing slack must not have been copied",
                           last.physicalBytes < physical - offsets[(int) last.firstChunk]);

                // The failure mode was confined to the final chunk, so read it: a wrong derived length shows up as
                // a CorruptSSTableException here and nowhere else.
                try (RandomAccessReader in = last.reader.openDataReader())
                {
                    in.seek(last.reader.uncompressedLength() - 1);
                    in.readByte();
                }

                assertComponents(cfs, result);
                assertConcatenatedContentEquals(parent, readers(result));
                assertColdReopenReadsTheSame(cfs, parent, result);
            }
            finally
            {
                release(result);
            }
        }
        finally
        {
            parent.selfRef().release();
        }
    }

    // ------------------------------------------------------------------------------------------------

    private void splitByCountAndVerify(boolean wide, int numChildren) throws Throwable
    {
        SSTableReader parent = compressedBtiSSTable(wide);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<DecoratedKey> parentKeys = allKeys(parent);
        Set<DecoratedKey> expectedKeys = new HashSet<>(parentKeys);
        assertFalse(parentKeys.isEmpty());
        // A "wide" parent with no row index at all would make the interesting half of the test vacuous, and a
        // "narrow" one with a row index would make the other half untested.
        long rowsLength = parent.descriptor.fileFor(BtiFormat.Components.ROW_INDEX).length();
        assertEquals("Rows.db is " + rowsLength + " bytes for wide=" + wide, wide, rowsLength > 0);

        Result result = ZeroCopySSTableSplitter.split(parent, numChildren, null);
        try
        {
            assertTrue(result.toString(), !result.children.isEmpty());
            assertTrue(result.toString(), result.children.size() <= numChildren);

            int partitions = 0;
            Set<DecoratedKey> seen = new HashSet<>();
            DecoratedKey previousLast = null;
            for (Child child : result.children)
            {
                SSTableReader reader = child.reader;

                // Every child claims a contiguous, ascending, disjoint slice of the parent's keys.
                assertTrue(reader.getFirst().compareTo(reader.getLast()) <= 0);
                if (previousLast != null)
                    assertTrue(previousLast + " >= " + reader.getFirst(),
                               previousLast.compareTo(reader.getFirst()) < 0);
                previousLast = reader.getLast();

                // Rows.db exists either way; a child with no indexed partition has a zero-length one, which is
                // exactly what a flush of a narrow table produces.
                assertTrue(reader.descriptor.fileFor(BtiFormat.Components.ROW_INDEX).exists());
                assertTrue(reader.descriptor.fileFor(BtiFormat.Components.PARTITION_INDEX).length() > 0);

                for (DecoratedKey key : allKeys(reader))
                {
                    assertTrue("duplicate key across children: " + key, seen.add(key));
                    assertTrue("child claims a key the parent never had: " + key, expectedKeys.contains(key));
                    // Every key the child holds has to be findable through its rebuilt partition index -- and, for
                    // an indexed partition, that means the key it reads back out of the copied Rows.db matched.
                    assertTrue("child cannot find its own key " + key,
                               reader.getPosition(key, SSTableReader.Operator.EQ) >= 0);
                }

                partitions += child.partitionCount;
            }

            assertEquals(expectedKeys, seen);
            assertEquals(parentKeys.size(), partitions);

            assertConcatenatedContentEquals(parent, readers(result));
            assertComponents(cfs, result);
            assertColdReopenReadsTheSame(cfs, parent, result);
        }
        finally
        {
            release(result);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Component sanity, and the cold reopen
    // ------------------------------------------------------------------------------------------------

    /**
     * The three descriptions of a child's components -- the set {@code split()} returns, TOC.txt, and the files in
     * the directory -- have to be the same set, plus the per-component checks that only a standalone parse can make.
     *
     * <p>BIG has had this since the beginning; BTI had nothing, so a child whose TOC.txt disagreed with the
     * directory would have opened fine here (the set is handed to {@code SSTableReader.open} directly) and failed on
     * the next restart or {@code nodetool refresh}, which rediscover components from TOC.
     */
    private void assertComponents(ColumnFamilyStore cfs, Result result) throws IOException
    {
        for (Child child : result.children)
        {
            String context = "child " + child.descriptor;

            assertEquals(context + ": TOC", child.components, TOCComponent.loadTOC(child.descriptor, false));
            assertEquals(context + ": files on disk", child.components, child.descriptor.discoverComponents());
            for (Component component : child.components)
                assertTrue(context + ": missing " + component, child.descriptor.fileFor(component).exists());

            // The BTI index pair, and none of BIG's.
            assertTrue(context + ": no Partitions.db",
                       child.components.contains(BtiFormat.Components.PARTITION_INDEX));
            assertTrue(context + ": no Rows.db", child.components.contains(BtiFormat.Components.ROW_INDEX));
            assertFalse(context + ": a BTI child must not carry Index.db",
                        child.components.contains(BigFormat.Components.PRIMARY_INDEX));
            assertFalse(context + ": a BTI child must not carry Summary.db",
                        child.components.contains(BigFormat.Components.SUMMARY));
            assertTrue(context + ": no Filter.db", child.components.contains(SSTableFormat.Components.FILTER));
            assertTrue(context + ": no CompressionInfo.db",
                       child.components.contains(SSTableFormat.Components.COMPRESSION_INFO));
            assertFalse(context + ": a compressed sstable must not have a CRC.db",
                        child.components.contains(SSTableFormat.Components.CRC));

            // Digest.crc32 is optional (zero_copy_split_digest_enabled) and the two states must be exactly two
            // states: claimed and right, or claimed nowhere and existing nowhere.
            if (child.components.contains(SSTableFormat.Components.DIGEST))
            {
                assertEquals(context + ": digest",
                             Long.toString(crc32Of(child.descriptor.fileFor(SSTableFormat.Components.DATA))),
                             readDigest(child.descriptor));
            }
            else
            {
                assertFalse(context + ": Digest.crc32 must not exist when it is not a component",
                            child.descriptor.fileFor(SSTableFormat.Components.DIGEST).exists());
            }

            // Statistics.db: all four metadata components must deserialise standalone. It carries the
            // SerializationHeader every relocated row is decoded against, and it is written through a
            // SequentialWriter rather than through MetadataSerializer.rewriteSSTableMetadata.
            Map<MetadataType, MetadataComponent> childMetadata =
                child.descriptor.getMetadataSerializer()
                                .deserialize(child.descriptor, EnumSet.allOf(MetadataType.class));
            for (MetadataType type : MetadataType.values())
                assertNotNull(context + ": Statistics.db is missing " + type, childMetadata.get(type));
            assertFalse(context + ": leftover Statistics.db tmp file",
                        child.descriptor.tmpFileFor(SSTableFormat.Components.STATS).exists());

            // With hasKeyRange() the reader prefers Statistics.db to every other component for its bounds, so the
            // keys in it are what a cold open will claim -- and inheriting the parent's would claim its whole range.
            StatsMetadata stats = (StatsMetadata) childMetadata.get(MetadataType.STATS);
            assertTrue(context + ": BTI is expected to record a key range", child.descriptor.version.hasKeyRange());
            IPartitioner partitioner = cfs.metadata().partitioner;
            assertEquals(context + ": Statistics.db first key",
                         child.first, partitioner.decorateKey(stats.firstKey));
            assertEquals(context + ": Statistics.db last key",
                         child.last, partitioner.decorateKey(stats.lastKey));

            // Bloom filter: a false negative is data loss, so every owned key must be present. Deserialised
            // standalone, because that is how a restart gets it.
            try (FileInputStreamPlus in = child.descriptor.fileFor(SSTableFormat.Components.FILTER).newInputStream();
                 IFilter filter = BloomFilterSerializer.forVersion(child.descriptor.version.hasOldBfFormat())
                                                       .deserialize(in))
            {
                for (DecoratedKey key : allKeys(child.reader))
                    assertTrue(context + ": bloom filter false negative for " + key, filter.isPresent(key));
            }

        }
    }

    /**
     * Reopen every child from disk and read it again.
     *
     * <p>{@code split()} hands back a reader it opened itself, with its file handles and its in-memory index already
     * warm; a restart or a {@code nodetool refresh} does not. Everything a BTI child's readability depends on --
     * Partitions.db's footer and trie root, Rows.db's placement, Statistics.db's SerializationHeader and key range --
     * is only re-read on the cold path, so a child that is readable only through the handed-back reader is a child
     * that stops being readable at the next restart.
     */
    private void assertColdReopenReadsTheSame(ColumnFamilyStore cfs, SSTableReader parent, Result result)
    {
        List<SSTableReader> reopened = new ArrayList<>(result.children.size());
        try
        {
            for (Child child : result.children)
                reopened.add(SSTableReader.open(cfs, child.descriptor, child.components, cfs.metadata));

            for (int i = 0; i < reopened.size(); i++)
            {
                SSTableReader cold = reopened.get(i);
                Child child = result.children.get(i);
                assertEquals("cold first key of " + child.descriptor, child.first, cold.getFirst());
                assertEquals("cold last key of " + child.descriptor, child.last, cold.getLast());
                assertEquals("cold unindexed-region marker of " + child.descriptor,
                             child.reader.hasUnindexedRegions(), cold.hasUnindexedRegions());
                for (DecoratedKey key : allKeys(cold))
                    assertTrue("cold reader cannot find its own key " + key,
                               cold.getPosition(key, SSTableReader.Operator.EQ) >= 0);
            }

            assertConcatenatedContentEquals(parent, reopened);
        }
        finally
        {
            for (SSTableReader reader : reopened)
                reader.selfRef().release();
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Content equivalence
    // ------------------------------------------------------------------------------------------------

    /**
     * The children concatenated in token order are the parent, partition for partition and {@code Unfiltered} for
     * {@code Unfiltered}. Compared with {@code equals} rather than through {@code toString}, so cell timestamps,
     * TTLs, local deletion times, collection tombstones and range tombstone markers all count.
     */
    private static void assertConcatenatedContentEquals(SSTableReader parent, List<SSTableReader> children)
    {
        List<ISSTableScanner> scanners = new ArrayList<>(children.size());
        try (ISSTableScanner parentScanner = parent.getScanner())
        {
            for (SSTableReader child : children)
                scanners.add(child.getScanner());

            int index = 0;
            int compared = 0;
            while (parentScanner.hasNext())
            {
                try (UnfilteredRowIterator expected = parentScanner.next())
                {
                    while (index < scanners.size() && !scanners.get(index).hasNext())
                        index++;
                    assertTrue("the children ran out at the parent's partition " + expected.partitionKey(),
                               index < scanners.size());
                    try (UnfilteredRowIterator actual = scanners.get(index).next())
                    {
                        assertEquals("partition " + compared, expected.partitionKey(), actual.partitionKey());
                        assertSamePartition(expected, actual);
                    }
                    compared++;
                }
            }
            while (index < scanners.size())
            {
                assertFalse("a child yielded a partition the parent does not have",
                            scanners.get(index).hasNext());
                index++;
            }
            assertTrue("nothing was compared", compared > 0);
        }
        finally
        {
            for (ISSTableScanner scanner : scanners)
                scanner.close();
        }
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
            assertTrue(context + ": child ran out at row " + row, actual.hasNext());
            assertEquals(context + " row " + row, expected.next(), actual.next());
            row++;
        }
        assertFalse(context + ": child has extra rows", actual.hasNext());
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

    // ------------------------------------------------------------------------------------------------
    // Fixture
    // ------------------------------------------------------------------------------------------------

    /**
     * One compressed BTI sstable. Compression matters: it is the splitter's other precondition.
     *
     * @param wide when true every partition is well past {@code column_index_size}, so it gets a row index and its
     *             Partitions.db payload points into Rows.db; when false none of them does
     */
    private SSTableReader compressedBtiSSTable(boolean wide) throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck0 int, ck1 text, sv text static, val text, m map<int, text>, " +
                    "t text, PRIMARY KEY (pk, ck0, ck1)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
        disableCompaction();

        // Two rows even in the narrow shape, so a row tombstone can take one of them without emptying the partition.
        writeShapes(wide ? 24 : 400, wide ? 400 : 2, wide ? 300 : 20);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = onlySSTable(cfs);

        // Guards against the whole class silently degenerating into a second BIG-format test.
        assertTrue("expected a BTI sstable, got " + sstable.descriptor.getFormat().name(),
                   BtiFormat.is(sstable.descriptor.getFormat()));
        assertFalse(BigFormat.is(sstable.descriptor.getFormat()));
        // ...and against it degenerating into a suite of refusal tests: the splitter refuses a version below 'eb', so
        // a fixture pinned below it would turn every "was split" assertion below into an exception with no hint why.
        assertTrue("the fixture flushed version '" + sstable.descriptor.version.version + "', which cannot carry " +
                   "hasUnindexedRegions -- the splitter refuses it, so nothing in this class tests what it says it does",
                   sstable.descriptor.version.hasUnindexedRegionsMarker());
        return sstable;
    }

    /**
     * Data whose shape makes the oracle's comparisons mean something. Every branch below exists because
     * {@link #assertSamePartition} compares that thing and nothing else in this class produced it:
     * a partition-level tombstone under live rows (so {@code partitionLevelDeletion()} is not always
     * {@code LIVE}), a static row on most but not all partitions (so {@code staticRow()} is not always empty), TTL'd
     * cells and a collection (so cell {@code localDeletionTime}, {@code ttl} and the collection's complex deletion
     * are compared), row and range tombstones (so {@code RangeTombstoneMarker}s are in the stream), and a cell
     * deletion. Two clustering columns, so a clustering prefix is more than one component -- which is what the row
     * index trie's keys are built from.
     */
    private void writeShapes(int partitions, int rowsPerPartition, int valueBytes) throws Throwable
    {
        String value = repeat('v', valueBytes);
        for (int p = 0; p < partitions; p++)
        {
            String pk = String.format("k%06d", p);

            // Older than every row below, so the rows survive and the deletion is still in the partition header.
            if (p % 5 == 0)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", OLD_TS, pk);

            if (p % 3 != 0)
                execute("INSERT INTO %s (pk, sv) VALUES (?, ?) USING TIMESTAMP ?", pk, "static-" + p, NEW_TS);

            for (int c = 0; c < rowsPerPartition; c++)
            {
                // A TTL'd row carrying a collection. Reached in the narrow shape too, where c never gets past 1.
                boolean decorated = c % 7 == 3 || (rowsPerPartition <= 4 && p % 4 == 3 && c == 0);
                if (decorated)
                {
                    Map<Integer, String> collection = new HashMap<>();
                    collection.put(c, "m" + c);
                    collection.put(c + 1, "m" + (c + 1));
                    execute("INSERT INTO %s (pk, ck0, ck1, val, m) VALUES (?, ?, ?, ?, ?) " +
                            "USING TIMESTAMP ? AND TTL 8640000",
                            pk, c, "c" + c, value, collection, NEW_TS);
                }
                else
                {
                    execute("INSERT INTO %s (pk, ck0, ck1, val) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                            pk, c, "c" + c, value, NEW_TS);
                }
            }

            // A row tombstone, over the LAST row so the partition is never emptied, and -- where there are enough
            // rows for it to sit inside a row index block rather than at its edge -- a range tombstone too.
            if (p % 4 == 1)
            {
                int last = rowsPerPartition - 1;
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 = ? AND ck1 = ?",
                        NEW_TS + 1, pk, last, "c" + last);
                if (rowsPerPartition > 8)
                    execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 > ? AND ck0 < ?",
                            NEW_TS + 1, pk, rowsPerPartition / 2, rowsPerPartition / 2 + 3);
            }
            if (p % 6 == 2)
                execute("DELETE t FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 = ? AND ck1 = ?",
                        NEW_TS + 1, pk, 0, "c0");
        }
    }

    private static String repeat(char c, int n)
    {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++)
            sb.append(c);
        return sb.toString();
    }

    private static RangesAtEndpoint fullOnly(Range<Token> range)
    {
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        return RangesAtEndpoint.builder(local).add(Replica.fullReplica(local, range)).build();
    }

    private static SSTableReader onlySSTable(ColumnFamilyStore cfs)
    {
        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals("expected exactly one sstable", 1, live.size());
        return live.iterator().next();
    }

    private static List<SSTableReader> readers(Result result)
    {
        List<SSTableReader> readers = new ArrayList<>(result.children.size());
        for (Child child : result.children)
            readers.add(child.reader);
        return readers;
    }

    private static void release(Result result)
    {
        for (Child child : result.children)
            child.reader.selfRef().release();
    }

    private static long crc32Of(File file) throws IOException
    {
        CRC32 crc = new CRC32();
        byte[] buffer = new byte[8192];
        try (FileInputStreamPlus in = file.newInputStream())
        {
            int n;
            while ((n = in.read(buffer)) > 0)
                crc.update(buffer, 0, n);
        }
        return crc.getValue();
    }

    private static String readDigest(Descriptor descriptor) throws IOException
    {
        byte[] bytes = Files.readAllBytes(descriptor.fileFor(SSTableFormat.Components.DIGEST).toPath());
        return new String(bytes, StandardCharsets.UTF_8).trim();
    }

    /** CompressionInfo.db's offsets table, parsed by hand so it does not go through the code under test. */
    private static long[] readChunkOffsets(Descriptor descriptor) throws IOException
    {
        try (FileInputStreamPlus in = descriptor.fileFor(SSTableFormat.Components.COMPRESSION_INFO).newInputStream())
        {
            in.readUTF();                       // compressor class name
            int optionCount = in.readInt();
            for (int i = 0; i < optionCount; i++)
            {
                in.readUTF();
                in.readUTF();
            }
            in.readInt();                       // chunkLength
            if (descriptor.version.hasMaxCompressedLength())
                in.readInt();                   // maxCompressedLength
            in.readLong();                      // dataLength
            long[] offsets = new long[in.readInt()];
            for (int i = 0; i < offsets.length; i++)
                offsets[i] = in.readLong();
            return offsets;
        }
    }
}
