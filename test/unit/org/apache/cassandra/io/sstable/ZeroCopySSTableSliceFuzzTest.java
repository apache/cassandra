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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config.FlushCompression;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSlice.Plan;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSlice.Reason;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSlice.Run;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Randomised end-to-end test of {@link ZeroCopySSTableSlice}: for a random sstable shape and a random set of
 * ranges, the slice that gets planned and synthesised must be an sstable holding exactly the partitions in those
 * ranges and nothing else.
 *
 * <p>Every iteration MATERIALISES the slice -- copies the planned byte ranges of the parent's Data.db into place,
 * in order, which is exactly what the receiving node does with the bytes off the wire -- and then attacks the
 * result through the ordinary readers. The properties checked, in the order they would break:
 * <ul>
 *   <li>STRUCTURE: the slice's own CompressionInfo.db or CRC.db agrees with its data file, its chunk offsets are
 *       contiguous and start at 0, its declared length matches the plan, and there is no trailing slack -- a
 *       single spare byte at the end inflates the last chunk's derived length and can flip the reader into
 *       handing compressed bytes back as row data.</li>
 *   <li>CONTENT: every requested partition, in order, with every row, cell, timestamp and deletion identical to
 *       the parent's.</li>
 *   <li>UNREACHABILITY: every partition that was NOT requested -- including the ones physically carried inside a
 *       boundary cell -- cannot be found through the index, the summary or the filter.</li>
 *   <li>VERIFICATION: {@code nodetool verify}'s extended pass, which walks the data by index position and checks
 *       every key against the filter and the summary, accepts the slice.</li>
 * </ul>
 *
 * <p>The generator deliberately covers the shapes the arithmetic is most likely to get wrong: partitions much
 * larger than a cell (promoted index entries, one partition spanning many cells) and much smaller (many partitions
 * per cell), ranges that start and end mid-cell, ranges close enough together to share a cell, ranges far enough
 * apart to be separate runs, and both formats -- compressed, where a cell cannot be cut and the last one is short
 * of what it decompresses to, and uncompressed, where the grid is CRC.db's and the last cell is cut and its
 * checksum recomputed.
 *
 * <p>A failure prints the iteration's whole configuration and a command line that replays that one case.
 */
public class ZeroCopySSTableSliceFuzzTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(ZeroCopySSTableSliceFuzzTest.class);

    private static final String PROP_SEED = "cassandra.test.zerocopyslice.seed";
    private static final String PROP_ITERATIONS = "cassandra.test.zerocopyslice.iterations";
    private static final String PROP_REPLAY_SEED = "cassandra.test.zerocopyslice.replaySeed";

    static final int DEFAULT_ITERATIONS = 24;
    /** Range sets tried per generated sstable; the sstable is what is expensive, not the slicing. */
    private static final int RANGE_SETS_PER_ITERATION = 4;

    // These three are developer-facing replay knobs for this one class, not runtime configuration, so they are
    // deliberately not in CassandraRelevantProperties -- the same reason MockMessagingSpy and the simulator
    // suppress this rule rather than register their debug switches globally.
    // checkstyle: suppress below 'blockSystemPropertyUsage'
    private static final long BASE_SEED = Long.getLong(PROP_SEED, 20260731_0001L);
    private static final int ITERATIONS = Integer.getInteger(PROP_ITERATIONS, DEFAULT_ITERATIONS);
    /** When set, exactly one iteration runs, with this literal seed. */
    private static final Long REPLAY_SEED = Long.getLong(PROP_REPLAY_SEED);

    /** Explicit insert timestamps keep the on-disk layout stable across runs of the same seed. */
    private static final long BASE_TS = 1_600_000_000_000_000L;

    /** Keep a single iteration's sstable small enough that one flush is one sstable, and the test quick. */
    private static final long MAX_TABLE_BYTES = 2_000_000L;

    /** null means uncompressed, whose grid is CRC.db's rather than a compression chunk length. */
    private static final String[] COMPRESSORS = { "LZ4Compressor", "SnappyCompressor", "DeflateCompressor",
                                                  "ZstdCompressor", null };
    private static final int[] CHUNK_KB = { 4, 8, 16, 32, 64 };
    /** 0 disables the raw-chunk fallback; > 1 makes most chunks store raw, which is the sharpest edge case. */
    private static final double[] MIN_COMPRESS_RATIO = { 0.0, 0.0, 1.0, 2.0, 8.0 };
    private static final int[] COLUMN_INDEX_KB = { 1, 2, 4, 16, 64 };

    /**
     * What the run actually covered. A fuzz test that quietly stops generating the shapes it exists for is worse
     * than no fuzz test, so {@link #fuzz} fails if any of these stays at zero.
     */
    private int slicesVerified;
    private int refusedForDeadSpace;
    private int multiRunSlices;
    private int compressedSlices;
    private int uncompressedSlices;
    private int slicesWithInteriorGaps;
    private int slicesWithDeadPrefix;

    // ------------------------------------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------------------------------------

    @Test
    public void fuzz() throws Throwable
    {
        // keysInOrder() reads Index.db directly and the whole class asserts the BIG-only zero-copy slice ran.
        Assume.assumeTrue(BigFormat.isSelected());

        int savedIndexSize = DatabaseDescriptor.getColumnIndexSizeInKiB();
        FlushCompression savedFlush = DatabaseDescriptor.getFlushCompression();
        try
        {
            // Otherwise flush_compression: fast silently swaps any non-LZ4 compressor -- and its chunk length --
            // for LZ4 at 16 KiB, and the sweep over compressors and chunk sizes would only ever test one of them.
            DatabaseDescriptor.setFlushCompression(FlushCompression.table);

            if (REPLAY_SEED != null)
            {
                logger.info("Replaying a single ZeroCopySSTableSlice fuzz iteration, seed {}", REPLAY_SEED);
                runGuarded(REPLAY_SEED);
                return;
            }

            logger.info("ZeroCopySSTableSlice fuzz: {} iterations from base seed {}", ITERATIONS, BASE_SEED);
            for (int i = 0; i < ITERATIONS; i++)
                runGuarded(scramble(BASE_SEED + i));

            logger.info("ZeroCopySSTableSlice fuzz covered: {} slices ({} compressed, {} uncompressed, {} multi-run, " +
                        "{} with interior gaps, {} with a dead prefix), {} refused for dead space",
                        slicesVerified, compressedSlices, uncompressedSlices, multiRunSlices,
                        slicesWithInteriorGaps, slicesWithDeadPrefix, refusedForDeadSpace);

            // Guard the guard: if the generator drifts into producing only trivial shapes, say so rather than
            // passing on nothing.
            assertTrue("the fuzz verified almost no slices: " + slicesVerified, slicesVerified >= ITERATIONS);
            assertTrue("no compressed slice was verified", compressedSlices > 0);
            assertTrue("no uncompressed slice was verified", uncompressedSlices > 0);
            assertTrue("no multi-run slice was verified", multiRunSlices > 0);
            assertTrue("no slice with an interior gap was verified", slicesWithInteriorGaps > 0);
            assertTrue("no slice with a dead prefix was verified", slicesWithDeadPrefix > 0);
            assertTrue("nothing was ever refused for dead space", refusedForDeadSpace > 0);
        }
        finally
        {
            DatabaseDescriptor.setColumnIndexSizeInKiB(savedIndexSize);
            DatabaseDescriptor.setFlushCompression(savedFlush);
        }
    }

    /**
     * The deliberately adversarial shape: every partition sized to land exactly on, one byte before, or one byte
     * after a cell boundary, with a range boundary at every partition. That is where {@code (hi-1)/G} versus
     * {@code hi/G}, {@code lo mod G} and the physical length of a run are most likely to be off by one.
     */
    @Test
    public void straddlesCellBoundaries() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        int savedIndexSize = DatabaseDescriptor.getColumnIndexSizeInKiB();
        FlushCompression savedFlush = DatabaseDescriptor.getFlushCompression();
        try
        {
            DatabaseDescriptor.setFlushCompression(FlushCompression.table);
            DatabaseDescriptor.setColumnIndexSizeInKiB(64);   // one row per partition, so no promoted index anyway

            long seed = REPLAY_SEED != null ? REPLAY_SEED : BASE_SEED;
            for (int chunkKb : new int[]{ 4, 16 })
            {
                Case c = new Case(seed + chunkKb);
                c.compressor = "LZ4Compressor";
                c.chunkKb = chunkKb;
                c.minCompressRatio = 0.0;
                c.straddling = true;
                try
                {
                    runStraddling(c);
                }
                catch (Throwable t)
                {
                    throw new AssertionError("ZeroCopySSTableSlice straddling case FAILED\n" + c + '\n'
                                             + replayHint(c.seed, "straddlesCellBoundaries"), t);
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setColumnIndexSizeInKiB(savedIndexSize);
            DatabaseDescriptor.setFlushCompression(savedFlush);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // One iteration
    // ------------------------------------------------------------------------------------------------

    private void runGuarded(long seed) throws Throwable
    {
        Case c = new Case(seed);
        try
        {
            runIteration(c);
        }
        catch (Throwable t)
        {
            throw new AssertionError("ZeroCopySSTableSlice fuzz iteration FAILED\n" + c + '\n'
                                     + replayHint(seed, "fuzz"), t);
        }
    }

    private static String replayHint(long seed, String method)
    {
        // ai-ci-test has no method filter, so the seed alone pins the case down; -Dtest.methods still works
        // with a plain `ant testsome'.
        return "replay this case alone with:\n"
               + "  ant testsome"
               + " -Dtest.name=org.apache.cassandra.io.sstable.ZeroCopySSTableSliceFuzzTest"
               + " -Dtest.methods=" + method
               + " -D" + PROP_REPLAY_SEED + '=' + seed;
    }

    private void runIteration(Case c) throws Throwable
    {
        Random rnd = new Random(c.seed);

        c.compressor = COMPRESSORS[rnd.nextInt(COMPRESSORS.length)];
        c.chunkKb = CHUNK_KB[rnd.nextInt(CHUNK_KB.length)];
        c.minCompressRatio = MIN_COMPRESS_RATIO[rnd.nextInt(MIN_COMPRESS_RATIO.length)];
        c.columnIndexKb = COLUMN_INDEX_KB[rnd.nextInt(COLUMN_INDEX_KB.length)];
        c.clusterings = rnd.nextInt(4);              // 0 means one row per partition
        c.partitions = 20 + rnd.nextInt(180);
        c.maxDeadSpaceRatio = rnd.nextBoolean() ? 1.0 : 0.25;

        DatabaseDescriptor.setColumnIndexSizeInKiB(c.columnIndexKb);
        createTableFor(c);
        disableCompaction();
        writeRandomData(c, rnd);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> live = cfs.getLiveSSTables();
        if (live.size() != 1)
            return;   // a budget overshoot made two sstables; nothing to learn from this iteration
        SSTableReader parent = live.iterator().next();

        if (parent.compression)
            assertEquals("flush_compression silently replaced the table's chunk length",
                         c.chunkKb * 1024, parent.getCompressionMetadata().chunkLength());

        List<DecoratedKey> keys = keysInOrder(parent);
        if (keys.size() < 8)
            return;
        c.keyCount = keys.size();

        // Several independent range sets against the same parent: generating the data is what costs, so this is
        // most of the coverage per unit of time.
        for (int attempt = 0; attempt < RANGE_SETS_PER_ITERATION; attempt++)
        {
            // A random set of 1..5 disjoint, increasing key intervals, some adjacent and some far apart.
            c.intervals = randomIntervals(rnd, keys.size());
            verifySlice(cfs, parent, keys, c);
        }
    }

    /** Every partition is one cell wide give or take a byte, and every partition boundary is a range boundary. */
    private void runStraddling(Case c) throws Throwable
    {
        Random rnd = new Random(c.seed);
        int cellLength = c.chunkKb * 1024;

        c.clusterings = 0;
        c.columnIndexKb = 64;
        c.partitions = 24;
        c.maxDeadSpaceRatio = 1.0;

        createTableFor(c);
        disableCompaction();
        for (int p = 0; p < c.partitions; p++)
        {
            // -1, 0 or +1 byte around a whole cell, minus the fixed per-row overhead the writer adds.
            int target = cellLength + (p % 3) - 1 - 40;
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?) USING TIMESTAMP ?",
                    key(p), 0, incompressible(rnd, Math.max(1, target)), BASE_TS + p);
        }
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> live = cfs.getLiveSSTables();
        if (live.size() != 1)
            return;
        SSTableReader parent = live.iterator().next();
        List<DecoratedKey> keys = keysInOrder(parent);
        c.keyCount = keys.size();

        // Every single-partition range in turn, then every adjacent pair: maximum boundary coverage.
        for (int p = 0; p + 1 < keys.size(); p++)
        {
            c.intervals = Arrays.asList(new int[]{ p, p + 1 });
            verifySlice(cfs, parent, keys, c);
        }
        for (int p = 0; p + 3 < keys.size(); p += 2)
        {
            c.intervals = Arrays.asList(new int[]{ p, p + 1 }, new int[]{ p + 2, p + 3 });
            verifySlice(cfs, parent, keys, c);
        }
    }

    /**
     * Plan, synthesise, materialise and attack one slice.
     *
     * @param intervals key index pairs {@code {left, right}}: the range {@code (keys[left], keys[right]]}, so the
     *                  partitions {@code keys[left + 1 .. right]}
     */
    private void verifySlice(ColumnFamilyStore cfs, SSTableReader parent, List<DecoratedKey> keys, Case c)
    throws Throwable
    {
        List<Range<Token>> ranges = new ArrayList<>(c.intervals.size());
        List<DecoratedKey> expected = new ArrayList<>();
        for (int[] interval : c.intervals)
        {
            ranges.add(new Range<>(keys.get(interval[0]).getToken(), keys.get(interval[1]).getToken()));
            expected.addAll(keys.subList(interval[0] + 1, interval[1] + 1));
        }
        List<Range<Token>> normalized = Range.normalize(ranges);
        List<PartitionPositionBounds> sections = parent.getPositionsForRanges(normalized);
        if (sections.isEmpty() || expected.isEmpty())
            return;

        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, c.maxDeadSpaceRatio);
        if (!plan.isEligible())
        {
            // The only refusal a well-formed, flushed parent may produce. Anything else is a bug in the planner,
            // not a shape it is allowed to decline.
            assertEquals("unexpected refusal for " + c, Reason.DEAD_SPACE, plan.reason);
            assertTrue("refused for dead space at ratio " + c.maxDeadSpaceRatio,
                       c.maxDeadSpaceRatio < 1.0);
            c.refused++;
            refusedForDeadSpace++;
            return;
        }
        c.planned++;
        slicesVerified++;
        if (plan.compressed)
            compressedSlices++;
        else
            uncompressedSlices++;
        if (plan.runs.size() > 1)
            multiRunSlices++;
        if (plan.lo() % plan.cellLength != 0)
            slicesWithDeadPrefix++;
        for (Run run : plan.runs)
        {
            if (run.lastSection > run.firstSection)
            {
                slicesWithInteriorGaps++;
                break;
            }
        }

        assertPlanArithmetic(parent, plan, sections);

        Materialised materialised = materialise(cfs, parent, plan);
        try
        {
            SSTableReader slice = materialised.reader;

            assertEquals("partition count", expected.size(), materialised.slice.partitionCount);
            assertEquals("first key", expected.get(0), slice.getFirst());
            assertEquals("last key", expected.get(expected.size() - 1), slice.getLast());
            assertEquals("uncompressed length", plan.dataLength, slice.uncompressedLength());

            assertStructure(parent, slice, plan);
            assertContentMatches(parent, slice, expected);
            assertOnlyTheseKeysArePresent(slice, keys, expected);

            // No Digest.crc32 is synthesised for a slice -- the receiver computes that -- so this is always a full
            // extended verification: a walk of the data by index position, every key checked against the filter
            // and the summary. The fork's `new Verifier(cfs, sstable, isOffline, options)' is
            // SSTableReader.getVerifier on trunk; a slice is not tracked by the cfs, hence offline.
            try (IVerifier verifier = slice.getVerifier(cfs, new OutputHandler.LogOutput(), true,
                                                        IVerifier.options().extendedVerification(true).build()))
            {
                verifier.verify();
            }
        }
        finally
        {
            materialised.close();
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Assertions
    // ------------------------------------------------------------------------------------------------

    /** The plan against the parent it was computed from, recomputed independently of how it was built. */
    private static void assertPlanArithmetic(SSTableReader parent, Plan plan, List<PartitionPositionBounds> sections)
    {
        int cellLength = plan.cellLength;
        assertTrue("cell length must be positive", cellLength > 0);

        long useful = 0;
        for (PartitionPositionBounds section : sections)
            useful += section.upperPosition - section.lowerPosition;
        assertEquals("useful bytes", useful, plan.usefulBytes);
        assertEquals("dead bytes", plan.dataLength - useful, plan.deadBytes);
        assertEquals("run count", ZeroCopySSTableSlice.runCount(sections, cellLength), plan.runs.size());
        assertEquals("data length", ZeroCopySSTableSlice.dataLength(sections, cellLength), plan.dataLength);

        long cells = plan.cellCount();
        assertTrue("the last cell must hold at least one live byte and at most a full cell of them: " + plan,
                   (cells - 1) * (long) cellLength < plan.dataLength
                   && plan.dataLength <= cells * (long) cellLength);

        long cellBase = 0;
        long physicalBase = 0;
        long parentPhysical = parent.descriptor.fileFor(Components.DATA).length();
        for (int r = 0; r < plan.runs.size(); r++)
        {
            Run run = plan.runs.get(r);
            assertEquals("run " + r + " child cell base", cellBase, run.childCellBase);
            assertEquals("run " + r + " child physical base", physicalBase, run.childPhysicalBase);
            assertEquals("run " + r + " shift", (run.firstCell - run.childCellBase) * (long) cellLength, run.shift);
            assertTrue("run " + r + " must be non-empty", run.srcEnd > run.srcStart);
            assertTrue("run " + r + " must be inside the parent's data file", run.srcEnd <= parentPhysical);
            if (r > 0)
            {
                Run previous = plan.runs.get(r - 1);
                assertTrue("run " + r + " must start after the previous run's cells",
                           run.firstCell > previous.lastCell);
                assertTrue("runs must not overlap in the parent", run.srcStart >= previous.srcEnd);
            }
            cellBase += run.cellCount();
            physicalBase += run.physicalBytes();
        }
        assertEquals("total cells", cells, cellBase);
        assertEquals("total physical bytes", plan.physicalBytes, physicalBase);

        // Every requested position has to fall inside some run, or its partition would not be sent at all.
        for (PartitionPositionBounds section : sections)
        {
            boolean covered = false;
            for (Run run : plan.runs)
            {
                if (section.lowerPosition >= run.firstCell * (long) cellLength
                    && section.upperPosition <= (run.lastCell + 1) * (long) cellLength)
                {
                    covered = true;
                    break;
                }
            }
            assertTrue("section " + section.lowerPosition + ".." + section.upperPosition + " is in no run of " + plan,
                       covered);
        }
    }

    /** The slice's own components against its data file. */
    private static void assertStructure(SSTableReader parent, SSTableReader slice, Plan plan) throws IOException
    {
        long physical = slice.descriptor.fileFor(Components.DATA).length();
        assertEquals("the slice's data file must be exactly the planned bytes, with no trailing slack",
                     plan.physicalBytes, physical);

        if (plan.compressed)
        {
            long[] offsets = readChunkOffsets(slice.descriptor);
            assertEquals("chunk count", plan.cellCount(), offsets.length);
            assertEquals("offsets[0]", 0, offsets[0]);
            for (int k = 1; k < offsets.length; k++)
                assertTrue("chunk offsets must increase: " + offsets[k - 1] + " -> " + offsets[k],
                           offsets[k] > offsets[k - 1]);
            assertTrue("the last chunk must start inside the file", offsets[offsets.length - 1] < physical);

            assertEquals("chunk length", parent.getCompressionMetadata().chunkLength(),
                         slice.getCompressionMetadata().chunkLength());
            assertEquals("declared uncompressed length", plan.dataLength,
                         slice.getCompressionMetadata().dataLength);
            assertEquals("declared physical length", physical,
                         slice.getCompressionMetadata().compressedFileLength);
            assertFalse("a compressed slice has no CRC.db", slice.descriptor.fileFor(Components.CRC).exists());
        }
        else
        {
            assertEquals("an uncompressed slice's data file IS its uncompressed length",
                         plan.dataLength, physical);
            assertEquals("CRC.db is a header plus one checksum per cell",
                         4 + 4 * plan.cellCount(), slice.descriptor.fileFor(Components.CRC).length());
            assertFalse("an uncompressed slice has no CompressionInfo.db",
                        slice.descriptor.fileFor(Components.COMPRESSION_INFO).exists());

            // The one consumer of CRC.db is a legacy stream of this sstable, which validates every chunk it sends.
            // trunk has no DataIntegrityMetadata.checksumValidator(Descriptor); the two files are named explicitly.
            try (DataIntegrityMetadata.ChecksumValidator validator =
                     new DataIntegrityMetadata.ChecksumValidator(slice.descriptor.fileFor(Components.DATA),
                                                                 slice.descriptor.fileFor(Components.CRC));
                 RandomAccessReader in = RandomAccessReader.open(slice.descriptor.fileFor(Components.DATA)))
            {
                assertEquals("CRC.db chunk size", plan.cellLength, validator.chunkSize);
                validator.seek(0);
                long position = 0;
                while (position < physical)
                {
                    int toRead = (int) Math.min(validator.chunkSize, physical - position);
                    byte[] bytes = new byte[toRead];
                    in.seek(position);
                    in.readFully(bytes);
                    validator.validate(ByteBuffer.wrap(bytes));
                    position += toRead;
                }
            }
        }
    }

    /** Every expected partition, in order, identical to the parent's. */
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

                    UnfilteredRowIterator sent = null;
                    try
                    {
                        while (parentScanner.hasNext())
                        {
                            UnfilteredRowIterator candidate = parentScanner.next();
                            if (wanted.contains(candidate.partitionKey()))
                            {
                                sent = candidate;
                                break;
                            }
                            candidate.close();
                        }
                        assertNotNull("the parent ran out of partitions at " + compared, sent);
                        assertPartitionEquals(sent, actual);
                    }
                    finally
                    {
                        if (sent != null)
                            sent.close();
                    }
                }
                compared++;
            }
        }
        assertEquals("the slice is missing partitions", expected.size(), compared);
    }

    private static void assertPartitionEquals(UnfilteredRowIterator expected, UnfilteredRowIterator actual)
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
    }

    /** The partitions carried inside a boundary cell are physically there and must still be unreachable. */
    private static void assertOnlyTheseKeysArePresent(SSTableReader slice, List<DecoratedKey> all,
                                                     List<DecoratedKey> expected)
    {
        Set<DecoratedKey> wanted = new HashSet<>(expected);
        for (DecoratedKey key : all)
        {
            // trunk's getPosition returns the data position, or a negative value for "not in this sstable", so
            // "found" and "points inside the slice's data" collapse into one range check.
            long position = slice.getPosition(key, SSTableReader.Operator.EQ);
            if (wanted.contains(key))
            {
                assertTrue("the slice cannot find " + key, position >= 0);
                assertTrue("the index points outside the slice's data: " + position,
                           position < slice.uncompressedLength());
            }
            else
            {
                assertTrue("the slice exposes " + key + ", which was not asked for", position < 0);
            }
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Materialising a slice: what the receiving node does with the bytes
    // ------------------------------------------------------------------------------------------------

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
                for (Run run : plan.runs)
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

    // ------------------------------------------------------------------------------------------------
    // Generator and scaffolding
    // ------------------------------------------------------------------------------------------------

    /** One iteration's configuration, printed verbatim when it fails. */
    private static final class Case
    {
        final long seed;
        String compressor;
        int chunkKb;
        double minCompressRatio;
        int columnIndexKb;
        int clusterings;
        int partitions;
        int keyCount;
        double maxDeadSpaceRatio;
        boolean straddling;
        List<int[]> intervals = new ArrayList<>();
        int planned;
        int refused;

        Case(long seed)
        {
            this.seed = seed;
        }

        @Override
        public String toString()
        {
            StringBuilder ranges = new StringBuilder();
            for (int[] interval : intervals)
                ranges.append(ranges.length() == 0 ? "" : ", ").append('(').append(interval[0]).append(", ")
                      .append(interval[1]).append(']');
            return "Case{seed=" + seed
                   + ", compressor=" + (compressor == null ? "none" : compressor)
                   + ", chunkKb=" + chunkKb
                   + ", minCompressRatio=" + minCompressRatio
                   + ", columnIndexKb=" + columnIndexKb
                   + ", clusterings=" + clusterings
                   + ", partitions=" + partitions
                   + ", keys=" + keyCount
                   + ", maxDeadSpaceRatio=" + maxDeadSpaceRatio
                   + ", straddling=" + straddling
                   + ", planned=" + planned
                   + ", refused=" + refused
                   + ", intervals=[" + ranges + "]}";
        }
    }

    private void createTableFor(Case c) throws Throwable
    {
        String compression = c.compressor == null
                             ? "{'enabled': 'false'}"
                             : "{'class': '" + c.compressor + "', 'chunk_length_in_kb': '" + c.chunkKb + '\''
                               + (c.minCompressRatio > 0 ? ", 'min_compress_ratio': '" + c.minCompressRatio + '\'' : "")
                               + '}';
        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) WITH compression = "
                    + compression);
    }

    private void writeRandomData(Case c, Random rnd) throws Throwable
    {
        int cellLength = (c.compressor == null ? 64 : c.chunkKb) * 1024;
        long budget = MAX_TABLE_BYTES;

        for (int p = 0; p < c.partitions; p++)
        {
            int roll = rnd.nextInt(100);
            int valueSize;
            int rows;
            if (roll < 45)
            {
                valueSize = 1 + rnd.nextInt(200);              // many partitions per cell
                rows = c.clusterings == 0 ? 1 : 1 + rnd.nextInt(1 + c.clusterings);
            }
            else if (roll < 80)
            {
                valueSize = 200 + rnd.nextInt(2000);
                rows = c.clusterings == 0 ? 1 : 1 + rnd.nextInt(1 + c.clusterings);
            }
            else
            {
                valueSize = cellLength + rnd.nextInt(2 * cellLength);   // one partition over many cells
                rows = 1;
            }

            // Every partition has to fit, or the sstable ends up with a handful of partitions and the ranges have
            // nothing to cut. A partition may take up to four times its fair share of what is left.
            int remaining = c.partitions - p;
            long allowance = Math.max(256, (budget * 4) / remaining);
            if ((long) valueSize * rows > allowance)
            {
                rows = 1;
                valueSize = (int) Math.min(valueSize, allowance);
            }
            budget = Math.max(0, budget - (long) valueSize * rows);

            boolean compressible = rnd.nextBoolean();
            for (int r = 0; r < rows; r++)
            {
                String value = compressible ? compressible(valueSize) : incompressible(rnd, valueSize);
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?) USING TIMESTAMP ?",
                        key(p), r, value, BASE_TS + p * 100L + r);
            }
        }
    }

    /** 1..5 disjoint, increasing intervals, biased so that some are adjacent and some are far apart. */
    private static List<int[]> randomIntervals(Random rnd, int keyCount)
    {
        int wanted = 1 + rnd.nextInt(5);
        List<int[]> intervals = new ArrayList<>(wanted);
        int cursor = rnd.nextInt(Math.max(1, keyCount / 8));
        for (int i = 0; i < wanted && cursor + 2 < keyCount; i++)
        {
            int left = cursor;
            int width = 1 + rnd.nextInt(Math.max(1, keyCount / 4));
            int right = Math.min(keyCount - 1, left + width);
            if (right <= left)
                break;
            intervals.add(new int[]{ left, right });
            // Either butt up against the previous interval or leave a real gap.
            cursor = right + (rnd.nextBoolean() ? 0 : 1 + rnd.nextInt(Math.max(1, keyCount / 6)));
        }
        if (intervals.isEmpty())
            intervals.add(new int[]{ 0, keyCount - 1 });
        return intervals;
    }

    private static String key(int p)
    {
        return String.format("p%05d", p);
    }

    private static String compressible(int length)
    {
        char[] chars = new char[Math.max(1, length)];
        Arrays.fill(chars, 'v');
        return new String(chars);
    }

    private static String incompressible(Random rnd, int length)
    {
        char[] chars = new char[Math.max(1, length)];
        for (int i = 0; i < chars.length; i++)
            chars[i] = (char) ('!' + rnd.nextInt(94));
        return new String(chars);
    }

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

    private static long[] readChunkOffsets(Descriptor descriptor) throws IOException
    {
        try (FileInputStreamPlus in = descriptor.fileFor(Components.COMPRESSION_INFO).newInputStream())
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

    private static long scramble(long seed)
    {
        long z = seed + 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
