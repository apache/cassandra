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
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
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
 * <p>It also covers the partition shapes the CONTENT comparison needs to be worth running: partition-level
 * tombstones that later rows survive, static rows, TTLs, collections, one or two clustering columns with either
 * order, and row, range and cell tombstones. An {@code INSERT}-only fixture makes every
 * {@code partitionLevelDeletion()} and {@code staticRow()} comparison in {@link #assertPartitionEquals} vacuous, so
 * {@link #fuzz} fails if the run produced no slice carrying either.
 *
 * <p>The compressor and the chunk length come from the iteration's ORDINAL rather than from its seed, so the run
 * walks that matrix systematically and {@link #fuzz} can assert that every cell of it produced a verified slice
 * instead of hoping the dice covered them.
 *
 * <p>A failure prints the iteration's whole configuration and a command line that replays that one case.
 */
public class ZeroCopySSTableSliceFuzzTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(ZeroCopySSTableSliceFuzzTest.class);

    private static final String PROP_SEED = "cassandra.test.zerocopyslice.seed";
    private static final String PROP_ITERATIONS = "cassandra.test.zerocopyslice.iterations";
    private static final String PROP_REPLAY_SEED = "cassandra.test.zerocopyslice.replaySeed";
    private static final String PROP_REPLAY_INDEX = "cassandra.test.zerocopyslice.replayIndex";

    /** {@code COMPRESSORS.length * CHUNK_KB.length}: one iteration per cell of the matrix, see {@link #runIteration}. */
    static final int DEFAULT_ITERATIONS = 25;
    /** Range sets tried per generated sstable; the sstable is what is expensive, not the slicing. */
    private static final int RANGE_SETS_PER_ITERATION = 4;

    // These four are developer-facing replay knobs for this one class, not runtime configuration, so they are
    // deliberately not in CassandraRelevantProperties -- the same reason MockMessagingSpy and the simulator
    // suppress this rule rather than register their debug switches globally.
    // checkstyle: suppress below 'blockSystemPropertyUsage'
    private static final long BASE_SEED = Long.getLong(PROP_SEED, 20260731_0001L);
    private static final int ITERATIONS = Integer.getInteger(PROP_ITERATIONS, DEFAULT_ITERATIONS);
    /** When set, exactly one iteration runs, with this literal seed. */
    private static final Long REPLAY_SEED = Long.getLong(PROP_REPLAY_SEED);
    /** The replayed iteration's ordinal, which is what picks its compressor and chunk length. */
    private static final int REPLAY_INDEX = Integer.getInteger(PROP_REPLAY_INDEX, 0);

    /** Explicit insert timestamps keep the on-disk layout stable across runs of the same seed. */
    private static final long BASE_TS = 1_600_000_000_000_000L;
    /**
     * Every deletion's timestamp, explicit for the same reason the inserts' are: a wall-clock one varies in vint
     * WIDTH between runs, which moves every partition after it and makes a replayed seed a different case. Above
     * every timestamp {@link #writeRandomData} counts up from {@link #BASE_TS} and far below {@link #FUTURE_TS}.
     */
    private static final long DELETE_TS = BASE_TS + 1_000_000L;
    /**
     * Newer than {@link #DELETE_TS}, so a partition tombstone is retained AND the rows written after it survive it
     * -- a partition that is both deleted and non-empty, which is the shape whose loss on a receiver is silent data
     * resurrection.
     */
    private static final long FUTURE_TS = 2_000_000_000_000_000L;

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
     * The dead space ratio per range set of an iteration. The first two are 1.0, which a well-formed flushed parent
     * can never be refused at, so every iteration verifies at least two slices whatever the generator rolls -- that
     * is what makes the coverage assertions in {@link #fuzz} deterministic rather than probable. 0.25 is the
     * shipped default and 0.0 is what produces the {@code DEAD_SPACE} refusals.
     */
    private static final double[] DEAD_SPACE_RATIOS = { 1.0, 1.0, 0.25, 0.0 };

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
    private int slicesWithPartitionTombstones;
    private int slicesWithStaticRows;

    /**
     * The compressor/chunk-length matrix, attempted versus actually verified. {@code slicesVerified} alone cannot
     * say this: it was satisfied by a quarter of the iterations being productive, so a compressor that silently
     * stopped producing sliceable sstables -- or an iteration that abandoned itself before slicing anything -- left
     * no trace.
     */
    private final Set<String> compressorsAttempted = new TreeSet<>();
    private final Set<String> compressorsVerified = new TreeSet<>();
    private final Set<Integer> chunkKbAttempted = new TreeSet<>();
    private final Set<Integer> chunkKbVerified = new TreeSet<>();
    /** Iterations that verified nothing at all, with why, so the failure names the case instead of a count. */
    private final List<String> abandoned = new ArrayList<>();

    private int savedColumnIndexSizeInKiB;
    private FlushCompression savedFlushCompression;

    /**
     * Both tests here move the column index size and the flush compression per iteration, so the snapshot has to be
     * taken and put back outside them: a failure part way through a sweep must not leave either behind for the next
     * class to run in the same JVM.
     */
    @Before
    public void saveConfigurationAndHooks()
    {
        savedColumnIndexSizeInKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        savedFlushCompression = DatabaseDescriptor.getFlushCompression();
    }

    @After
    public void restoreConfigurationAndHooks()
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(savedColumnIndexSizeInKiB);
        DatabaseDescriptor.setFlushCompression(savedFlushCompression);
        // Production statics that exist only for tests; nothing here sets them, but a class that leaves one set
        // would change what this one exercises, and this is the cheapest place to be sure.
        ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = false;
        ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
    }

    // ------------------------------------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------------------------------------

    @Test
    public void fuzz() throws Throwable
    {
        // keysInOrder() reads Index.db directly and the whole class asserts the BIG-only zero-copy slice ran.
        Assume.assumeTrue(BigFormat.isSelected());

        // Otherwise flush_compression: fast silently swaps any non-LZ4 compressor -- and its chunk length --
        // for LZ4 at 16 KiB, and the sweep over compressors and chunk sizes would only ever test one of them.
        DatabaseDescriptor.setFlushCompression(FlushCompression.table);

        if (REPLAY_SEED != null)
        {
            logger.info("Replaying a single ZeroCopySSTableSlice fuzz iteration, seed {} index {}",
                        REPLAY_SEED, REPLAY_INDEX);
            runGuarded(REPLAY_SEED, REPLAY_INDEX);
            return;
        }

        logger.info("ZeroCopySSTableSlice fuzz: {} iterations from base seed {}", ITERATIONS, BASE_SEED);
        for (int i = 0; i < ITERATIONS; i++)
            runGuarded(scramble(BASE_SEED + i), i);

        logger.info("ZeroCopySSTableSlice fuzz covered: {} slices ({} compressed, {} uncompressed, {} multi-run, " +
                    "{} with interior gaps, {} with a dead prefix, {} with a partition tombstone, {} with a static " +
                    "row), {} refused for dead space; compressors {}, chunk lengths {}",
                    slicesVerified, compressedSlices, uncompressedSlices, multiRunSlices,
                    slicesWithInteriorGaps, slicesWithDeadPrefix, slicesWithPartitionTombstones,
                    slicesWithStaticRows, refusedForDeadSpace, compressorsVerified, chunkKbVerified);

        // Guard the guard: if the generator drifts into producing only trivial shapes, say so rather than
        // passing on nothing.
        assertTrue("iterations verified nothing at all: " + abandoned, abandoned.isEmpty());
        // Two of the four range sets per iteration run at maxDeadSpaceRatio 1.0, which a well-formed flushed parent
        // is never refused at, so this is a floor and not a hope. The old bound -- slicesVerified >= ITERATIONS --
        // was met by ONE of the four being productive, i.e. by three quarters of the matrix going untested.
        assertTrue("the fuzz verified too few slices: " + slicesVerified + " for " + ITERATIONS + " iterations",
                   slicesVerified >= 2 * ITERATIONS);
        // The matrix is walked by iteration ordinal, so what was attempted is exact and what was verified has to
        // match it: a compressor or chunk length that stops producing sliceable sstables cannot hide any more.
        assertEquals("the compressor sweep is not systematic any more",
                     Math.min(ITERATIONS, COMPRESSORS.length), compressorsAttempted.size());
        assertEquals("some compressor produced no verified slice",
                     compressorsAttempted, compressorsVerified);
        assertEquals("some chunk length produced no verified slice", chunkKbAttempted, chunkKbVerified);
        assertTrue("the chunk length sweep is too narrow: " + chunkKbVerified, chunkKbVerified.size() > 1);

        assertTrue("no compressed slice was verified", compressedSlices > 0);
        assertTrue("no uncompressed slice was verified", uncompressedSlices > 0);
        assertTrue("no multi-run slice was verified", multiRunSlices > 0);
        assertTrue("no slice with an interior gap was verified", slicesWithInteriorGaps > 0);
        assertTrue("no slice with a dead prefix was verified", slicesWithDeadPrefix > 0);
        assertTrue("nothing was ever refused for dead space", refusedForDeadSpace > 0);
        // Without these the oracle's partitionLevelDeletion() and staticRow() comparisons are LIVE == LIVE and
        // EMPTY == EMPTY, i.e. a slice that lost either would pass every iteration.
        assertTrue("no slice carried a partition tombstone, so the oracle never compared one",
                   slicesWithPartitionTombstones > 0);
        assertTrue("no slice carried a static row, so the oracle never compared one", slicesWithStaticRows > 0);
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

        DatabaseDescriptor.setFlushCompression(FlushCompression.table);
        DatabaseDescriptor.setColumnIndexSizeInKiB(64);   // one row per partition, so no promoted index anyway

        long seed = REPLAY_SEED != null ? REPLAY_SEED : BASE_SEED;
        for (int chunkKb : new int[]{ 4, 16 })
        {
            Case c = new Case(seed + chunkKb, 0);
            c.compressor = "LZ4Compressor";
            c.chunkKb = chunkKb;
            c.minCompressRatio = 0.0;
            c.straddling = true;
            // The narrowest possible shape: this case measures partitions to the byte against a cell boundary, so
            // every optional column would move the target it is aiming at.
            c.clusteringColumns = 1;
            try
            {
                runStraddling(c);
            }
            catch (Throwable t)
            {
                throw new AssertionError("ZeroCopySSTableSlice straddling case FAILED\n" + c + '\n'
                                         + replayHint(c.seed, 0, "straddlesCellBoundaries"), t);
            }
        }
    }

    // ------------------------------------------------------------------------------------------------
    // One iteration
    // ------------------------------------------------------------------------------------------------

    private void runGuarded(long seed, int index) throws Throwable
    {
        Case c = new Case(seed, index);
        try
        {
            runIteration(c);
        }
        catch (Throwable t)
        {
            throw new AssertionError("ZeroCopySSTableSlice fuzz iteration FAILED\n" + c + '\n'
                                     + replayHint(seed, index, "fuzz"), t);
        }
    }

    private static String replayHint(long seed, int index, String method)
    {
        // ai-ci-test has no method filter, so the seed and the ordinal pin the case down; -Dtest.methods still
        // works with a plain `ant testsome'. The ordinal is needed as well as the seed because it, and not the
        // seed, is what selects the compressor and the chunk length.
        return "replay this case alone with:\n"
               + "  ant testsome"
               + " -Dtest.name=org.apache.cassandra.io.sstable.ZeroCopySSTableSliceFuzzTest"
               + " -Dtest.methods=" + method
               + " -D" + PROP_REPLAY_SEED + '=' + seed
               + " -D" + PROP_REPLAY_INDEX + '=' + index;
    }

    private void runIteration(Case c) throws Throwable
    {
        Random rnd = new Random(c.seed);

        // The compressor and the chunk length come from the iteration's ORDINAL, not from the seed: 25 iterations
        // then walk all 5x5 of them exactly once, which is what lets fuzz() assert that every cell of the matrix
        // really produced a verified slice. Random selection left that to luck, and with a custom -Dseed or a
        // reduced -Diterations it silently tested a fraction of the matrix.
        c.compressor = COMPRESSORS[c.index % COMPRESSORS.length];
        c.chunkKb = CHUNK_KB[(c.index / COMPRESSORS.length) % CHUNK_KB.length];
        c.minCompressRatio = MIN_COMPRESS_RATIO[rnd.nextInt(MIN_COMPRESS_RATIO.length)];
        c.columnIndexKb = COLUMN_INDEX_KB[rnd.nextInt(COLUMN_INDEX_KB.length)];
        c.rowSpread = rnd.nextInt(4);                 // 0 means one row per partition
        c.partitions = 20 + rnd.nextInt(180);
        c.clusteringColumns = 1 + rnd.nextInt(2);
        c.reverseFirstClustering = rnd.nextBoolean();
        c.hasStatic = rnd.nextBoolean();
        c.hasMap = rnd.nextBoolean();

        compressorsAttempted.add(c.compressorName());
        if (c.compressor != null)
            chunkKbAttempted.add(c.chunkKb);

        DatabaseDescriptor.setColumnIndexSizeInKiB(c.columnIndexKb);
        createTableFor(c);
        disableCompaction();
        writeRandomData(c, rnd);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> live = cfs.getLiveSSTables();
        if (live.size() != 1)
        {
            // A budget overshoot made two sstables. Nothing can be learned from the iteration, and -- since the
            // matrix is walked by ordinal -- one cell of it went untested, so this is recorded rather than ignored.
            abandoned.add("iteration " + c.index + " produced " + live.size() + " sstables: " + c);
            return;
        }
        SSTableReader parent = live.iterator().next();

        if (parent.compression)
            assertEquals("flush_compression silently replaced the table's chunk length",
                         c.chunkKb * 1024, parent.getCompressionMetadata().chunkLength());

        List<DecoratedKey> keys = keysInOrder(parent);
        if (keys.size() < 8)
        {
            abandoned.add("iteration " + c.index + " produced only " + keys.size() + " partitions: " + c);
            return;
        }
        c.keyCount = keys.size();

        // Several independent range sets against the same parent: generating the data is what costs, so this is
        // most of the coverage per unit of time. The first two run at ratio 1.0, so at least two slices are
        // verified per iteration however the intervals fall out.
        for (int attempt = 0; attempt < RANGE_SETS_PER_ITERATION; attempt++)
        {
            c.maxDeadSpaceRatio = DEAD_SPACE_RATIOS[attempt % DEAD_SPACE_RATIOS.length];
            // A random set of 1..5 disjoint, increasing key intervals, some adjacent and some far apart.
            c.intervals = randomIntervals(rnd, keys.size());
            verifySlice(cfs, parent, keys, c);
        }

        if (c.planned == 0)
            abandoned.add("iteration " + c.index + " verified no slice at all: " + c);
    }

    /** Every partition is one cell wide give or take a byte, and every partition boundary is a range boundary. */
    private void runStraddling(Case c) throws Throwable
    {
        Random rnd = new Random(c.seed);
        int cellLength = c.chunkKb * 1024;

        c.rowSpread = 0;
        c.columnIndexKb = 64;
        c.partitions = 24;
        c.maxDeadSpaceRatio = 1.0;

        createTableFor(c);
        disableCompaction();
        for (int p = 0; p < c.partitions; p++)
        {
            // -1, 0 or +1 byte around a whole cell, minus the fixed per-row overhead the writer adds.
            int target = cellLength + (p % 3) - 1 - 40;
            execute("INSERT INTO %s (pk, ck0, val) VALUES (?, ?, ?) USING TIMESTAMP ?",
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
        compressorsVerified.add(c.compressorName());
        if (plan.compressed)
        {
            compressedSlices++;
            chunkKbVerified.add(c.chunkKb);
        }
        else
        {
            uncompressedSlices++;
        }
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
            countShapes(slice);

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

    /**
     * Whether the slice carries the shapes that make {@link #assertPartitionEquals}'s partition-level deletion and
     * static row comparisons mean anything. They are counted rather than asserted per slice, because a given range
     * set need not contain a deleted partition; {@link #fuzz} asserts that the run as a whole produced some.
     */
    private void countShapes(SSTableReader slice)
    {
        boolean tombstone = false;
        boolean staticRow = false;
        try (ISSTableScanner scanner = slice.getScanner())
        {
            while (scanner.hasNext() && !(tombstone && staticRow))
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    tombstone |= !partition.partitionLevelDeletion().isLive();
                    staticRow |= !partition.staticRow().isEmpty();
                }
            }
        }
        if (tombstone)
            slicesWithPartitionTombstones++;
        if (staticRow)
            slicesWithStaticRows++;
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
        /** The iteration's ordinal, which selects {@link #compressor} and {@link #chunkKb}; part of the replay key. */
        final int index;
        String compressor;
        int chunkKb;
        double minCompressRatio;
        int columnIndexKb;
        /** Upper bound on the extra rows a partition gets; 0 means one row per partition. */
        int rowSpread;
        int clusteringColumns;
        boolean reverseFirstClustering;
        boolean hasStatic;
        boolean hasMap;
        int partitions;
        int keyCount;
        double maxDeadSpaceRatio;
        boolean straddling;
        List<int[]> intervals = new ArrayList<>();
        int planned;
        int refused;

        Case(long seed, int index)
        {
            this.seed = seed;
            this.index = index;
        }

        String compressorName()
        {
            return compressor == null ? "none" : compressor;
        }

        @Override
        public String toString()
        {
            StringBuilder ranges = new StringBuilder();
            for (int[] interval : intervals)
                ranges.append(ranges.length() == 0 ? "" : ", ").append('(').append(interval[0]).append(", ")
                      .append(interval[1]).append(']');
            return "Case{seed=" + seed
                   + ", index=" + index
                   + ", compressor=" + compressorName()
                   + ", chunkKb=" + chunkKb
                   + ", minCompressRatio=" + minCompressRatio
                   + ", columnIndexKb=" + columnIndexKb
                   + ", rowSpread=" + rowSpread
                   + ", clusteringColumns=" + clusteringColumns
                   + ", reverseFirstClustering=" + reverseFirstClustering
                   + ", hasStatic=" + hasStatic
                   + ", hasMap=" + hasMap
                   + ", partitions=" + partitions
                   + ", keys=" + keyCount
                   + ", maxDeadSpaceRatio=" + maxDeadSpaceRatio
                   + ", straddling=" + straddling
                   + ", planned=" + planned
                   + ", refused=" + refused
                   + ", intervals=[" + ranges + "]}";
        }
    }

    /**
     * The table shape, which the slice's arithmetic is indifferent to but its inherited SerializationHeader is not:
     * the clustering types, their order, the static columns and the encoding stats all come out of the parent's
     * Statistics.db, and a slice that got any of them wrong would deserialise the copied bytes as something else.
     */
    private void createTableFor(Case c) throws Throwable
    {
        StringBuilder ddl = new StringBuilder("CREATE TABLE %s (pk text, ck0 int");
        if (c.clusteringColumns >= 2)
            ddl.append(", ck1 text");
        ddl.append(", val text");
        if (c.hasStatic)
            ddl.append(", s text static");
        if (c.hasMap)
            ddl.append(", m map<int, text>");
        ddl.append(", PRIMARY KEY (pk, ck0");
        if (c.clusteringColumns >= 2)
            ddl.append(", ck1");
        ddl.append("))");

        ddl.append(" WITH compression = ");
        if (c.compressor == null)
        {
            ddl.append("{'enabled': 'false'}");
        }
        else
        {
            ddl.append("{'class': '").append(c.compressor).append("', 'chunk_length_in_kb': '").append(c.chunkKb)
               .append('\'');
            if (c.minCompressRatio > 0)
                ddl.append(", 'min_compress_ratio': '").append(c.minCompressRatio).append('\'');
            ddl.append('}');
        }

        if (c.reverseFirstClustering)
        {
            ddl.append(" AND CLUSTERING ORDER BY (ck0 DESC");
            if (c.clusteringColumns >= 2)
                ddl.append(", ck1 ASC");
            ddl.append(')');
        }
        createTable(ddl.toString());
    }

    /**
     * Partition sizes spread around the cell length, plus the shapes an {@code INSERT}-only generator cannot
     * produce: partition-level tombstones that later rows survive, static rows, TTLs, collections, and row, range
     * and cell tombstones. Without them the oracle's {@code partitionLevelDeletion()} and {@code staticRow()}
     * comparisons are LIVE == LIVE and EMPTY == EMPTY, so a slice that dropped either would pass every iteration --
     * and a lost partition tombstone on a receiver is silent resurrection of everything that partition ever held.
     */
    private void writeRandomData(Case c, Random rnd) throws Throwable
    {
        int cellLength = (c.compressor == null ? 64 : c.chunkKb) * 1024;
        long budget = MAX_TABLE_BYTES;
        long pastTs = BASE_TS;

        for (int p = 0; p < c.partitions; p++)
        {
            // Deletions run at DELETE_TS, so they always shadow the BASE_TS data and never the FUTURE_TS data.
            // Either way the tombstones themselves land in the bytes the slice copies.
            boolean partitionTombstone = rnd.nextInt(6) == 0;
            if (partitionTombstone)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", DELETE_TS, key(p));
            long ts = partitionTombstone ? FUTURE_TS + p * 1000L : pastTs;

            int roll = rnd.nextInt(100);
            int valueSize;
            int rows;
            if (roll < 45)
            {
                valueSize = 1 + rnd.nextInt(200);              // many partitions per cell
                rows = c.rowSpread == 0 ? 1 : 1 + rnd.nextInt(1 + c.rowSpread);
            }
            else if (roll < 80)
            {
                valueSize = 200 + rnd.nextInt(2000);
                rows = c.rowSpread == 0 ? 1 : 1 + rnd.nextInt(1 + c.rowSpread);
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
                insertRow(c, rnd, p, r, valueSize, compressible, ts + r);

            if (c.hasStatic && rnd.nextBoolean())
                execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP ?",
                        key(p), incompressible(rnd, 1 + rnd.nextInt(40)), ts + rows);

            // Only the PAST counter advances; a resurrected partition's FUTURE timestamps must not leak into the
            // next partition, or the deletions there would stop biting.
            pastTs += rows + 2;

            if (rows > 1 && rnd.nextInt(4) == 0)                     // row tombstone
                deleteRow(c, p, rnd.nextInt(rows));
            if (rows > 2 && rnd.nextInt(4) == 0)                     // range tombstone
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 > ? AND ck0 <= ?",
                        DELETE_TS, key(p), 0, 1 + rnd.nextInt(rows - 1));
            if (rnd.nextInt(5) == 0)                                 // single cell tombstone
                deleteValue(c, p, rnd.nextInt(rows));
        }
    }

    private void insertRow(Case c, Random rnd, int p, int row, int valueSize, boolean compressible, long ts)
    throws Throwable
    {
        String value = compressible ? compressible(valueSize) : incompressible(rnd, valueSize);
        StringBuilder query = new StringBuilder("INSERT INTO %s (pk, ck0");
        List<Object> values = new ArrayList<>();
        values.add(key(p));
        values.add(row);
        if (c.clusteringColumns >= 2)
        {
            query.append(", ck1");
            values.add("c" + row);
        }
        query.append(", val");
        values.add(value);
        // A collection, which is a complex column: its own deletion plus one cell per element.
        boolean map = c.hasMap && rnd.nextInt(3) == 0;
        if (map)
        {
            query.append(", m");
            Map<Integer, String> m = new TreeMap<>();
            for (int i = 0, n = rnd.nextInt(4); i < n; i++)
                m.put(rnd.nextInt(100), "m" + i);
            values.add(m);
        }

        query.append(") VALUES (?, ?");
        for (int i = 2; i < values.size(); i++)
            query.append(", ?");
        query.append(") USING TIMESTAMP ?");
        values.add(ts);

        // Long enough that nothing can expire mid-test, but it still writes real expiry information, which is what
        // the inherited EncodingStats has to describe.
        if (rnd.nextInt(4) == 0)
        {
            query.append(" AND TTL ?");
            values.add(100_000 + rnd.nextInt(1_000_000));
        }

        execute(query.toString(), values.toArray());
    }

    private void deleteRow(Case c, int p, int row) throws Throwable
    {
        if (c.clusteringColumns >= 2)
            execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 = ? AND ck1 = ?",
                    DELETE_TS, key(p), row, "c" + row);
        else
            execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 = ?", DELETE_TS, key(p), row);
    }

    private void deleteValue(Case c, int p, int row) throws Throwable
    {
        if (c.clusteringColumns >= 2)
            execute("DELETE val FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 = ? AND ck1 = ?",
                    DELETE_TS, key(p), row, "c" + row);
        else
            execute("DELETE val FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 = ?", DELETE_TS, key(p), row);
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
