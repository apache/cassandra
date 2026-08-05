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
package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config.FlushCompression;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TestDatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.Reflink;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Randomised test of anticompaction over random <b>range sets</b>, covering both the
 * {@link ZeroCopySSTableSplitter}-based path (chosen by {@link AntiCompactionRunPlanner}) and the unchanged
 * three-rewriter fallback. Every iteration builds one compressed sstable, invents a full/transient range set,
 * runs {@code performAnticompaction} over it, and applies one oracle.
 *
 * <h2>The oracle</h2>
 * Whichever path ran:
 * <ol>
 *     <li>every parent partition appears <b>exactly once</b> across the resulting sstables, and its content is
 *         byte-for-byte the same (compared as the fully-detailed rendering of the partition-level deletion, the
 *         static row and every unfiltered); and</li>
 *     <li>each partition's {@code pendingRepair} / {@code isTransient} is exactly what its token implies from
 *         the range set -- FULL to {@code (sessionID, false)}, TRANSIENT to {@code (sessionID, true)},
 *         everything else to {@code (null, false)} -- and nothing is ever marked repaired.</li>
 * </ol>
 * That oracle is path-agnostic on purpose, so the test stays valid no matter which side of the gate an
 * iteration lands on. On top of it the test cross-checks the gate itself: the labels, the run count, the
 * boundaries and the per-child repair state are all recomputed here from {@code Range.contains} alone, and the
 * planner's verdict must agree; the {@code BytesZeroCopyAnticompaction} meter must move if and only if
 * the verdict was "eligible"; and the number of output sstables must be the run count when the split ran and
 * the number of distinct labels present when the rewrite ran. The recomputation deliberately uses the naive
 * {@code Range.contains} scan rather than the {@code OrderedRangeContainmentChecker} that both production paths
 * share, so agreement means something.
 *
 * <h2>Why the generator writes no tombstones and never overwrites</h2>
 * The fallback path pushes every partition through a {@link CompactionController}, so it legitimately drops
 * data shadowed by a deletion, whereas the zero-copy path copies compression chunks verbatim and retains it.
 * A single content oracle can only hold for both paths if the data contains nothing either path is allowed to
 * change: hence one INSERT per {@code (pk, ck)}, no DELETEs, no null values, no TTLs, and an explicit
 * {@code gc_grace_seconds}. Tombstone-retention behaviour is the splitter's own fuzz test's job
 * ({@code ZeroCopySSTableSplitterFuzzTest}); the subject here is routing and completeness.
 *
 * <h2>What is randomised</h2>
 * The sstable format (BIG or BTI), the compressor and {@code chunk_length_in_kb}, {@code column_index_size} (so wide
 * partitions really do carry a row index that the planner's index walk has to skip), the partition count, and wide
 * versus narrow partitions. The range set is randomised by <em>shape</em>: covering the whole sstable, a prefix, a suffix, a
 * middle span, exactly one partition, a range whose endpoint lands exactly on the first or last partition's
 * token, ranges that cover no partition at all (inside a token gap, or entirely below or above the sstable's
 * span), a full range abutting a transient one, a transient range nested inside a full one (full wins) and vice
 * versa, several disjoint full ranges, and a vnode-like alternating layout with more runs than the planner
 * retains detail for. Shapes are guaranteed to be exercised: iteration {@code i} always gets shape
 * {@code i % shapes}, encoded in the low digits of that iteration's seed so a seed alone still replays it.
 *
 * <h2>Randomised, and replayable anyway</h2>
 * The base seed is drawn fresh on every run, so successive runs really do sample the ~480-combination matrix
 * instead of re-testing the same 16 configurations forever; a hardcoded base seed made the "fuzz" deterministic
 * and, worse, permanently blind to whatever it happened not to pick. What is NOT random is the shape: iteration
 * {@code i} always gets shape {@code i % SHAPES.length}, so every shape is exercised on every run whatever the
 * seed, and the shape is encoded in the low digits of the iteration's seed so quoting that seed replays the shape
 * as well.
 * <p>
 * Every assertion message carries the whole configuration -- range set, format, free-space verdict -- plus the
 * iteration's seed, and the base seed is logged at INFO on the way in and repeated in the summary assertions.
 * A bare {@code -Dfoo=bar} on the ant command line does <b>not</b> reach the forked test JVM, so the properties
 * below must be passed through {@code -Dtest.jvm.args}:
 * <pre>
 *   ant testsome \
 *       -Dtest.name=org.apache.cassandra.db.compaction.ZeroCopyAntiCompactionFuzzTest \
 *       -Dtest.methods=fuzzRangeSets \
 *       -Dtest.jvm.args="-Dcassandra.test.zcanticompaction.replaySeed=&lt;seed from the failure message&gt;"
 * </pre>
 * To replay a whole run, or for a longer soak:
 * <pre>
 *   -Dtest.jvm.args="-Dcassandra.test.zcanticompaction.seed=&lt;base seed from the log&gt;"
 *   -Dtest.jvm.args="-Dcassandra.test.zcanticompaction.iterations=160"
 * </pre>
 * The default of one iteration per shape is deliberately modest so this stays inside a normal unit-test run.
 *
 * <h2>Both formats, in every run</h2>
 * BIG and BTI resolve a partition key from different places -- BIG reads it straight out of Index.db, BTI walks
 * Partitions.db and then reads the key from Rows.db or, for a partition with no row index, from Data.db -- so the
 * labelling oracle is testing genuinely different code on each. Waiting for the one CI job that selects BTI
 * ({@code ant test-latest}, whose yaml sets {@code sstable.selected_format: bti}) left that code untested in every
 * other run, so the format is part of the randomised matrix instead: each iteration flips it, which works because
 * the selected format is read at flush time. It is restored in {@code @After} with everything else.
 *
 * <h2>Preconditions, and how they fail</h2>
 * Two things can make the feature INERT rather than wrong, and both are diagnosed
 * explicitly rather than left to surface as a mysterious "the plan was eligible but the metric did not move":
 * <ul>
 *   <li>an sstable version that cannot carry {@code StatsMetadata.hasUnindexedRegions} (BIG {@code pb}+, BTI
 *       {@code eb}+), which is what {@code storage_compatibility_mode} produces; and</li>
 *   <li>a nearly full disk. The precheck in {@code CompactionManager.zeroCopyAntiCompact} goes through
 *       {@code Directories.hasDiskSpaceForCompactionsAndStreams}, so it honours {@code min_free_space_per_drive}
 *       and {@code max_space_usable_for_compactions_in_percentage} and DECLINES every eligible sstable on a full
 *       CI volume. The same question is asked here, before each run, and its verdict recorded in the
 *       configuration.</li>
 * </ul>
 * {@code zero_copy_anticompaction_enabled} defaults to false; it and the four other settings this test moves are
 * saved in {@code @Before} and restored in {@code @After}, so no {@code Assume} or early return can leak them.
 */
public class ZeroCopyAntiCompactionFuzzTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(ZeroCopyAntiCompactionFuzzTest.class);

    private static final String PROP_SEED = "cassandra.test.zcanticompaction.seed";
    private static final String PROP_ITERATIONS = "cassandra.test.zcanticompaction.iterations";
    private static final String PROP_REPLAY_SEED = "cassandra.test.zcanticompaction.replaySeed";

    /** How the full/transient ranges are placed relative to the sstable's token span. */
    private enum Shape
    {
        /** Every partition FULL; also the shape that makes {@code mutateFullyContainedSSTables} take over. */
        COVER_ALL,
        /** FULL over a token prefix: {@code F U}. */
        PREFIX,
        /** FULL over a token suffix: {@code U F}. */
        SUFFIX,
        /** FULL over a middle span: {@code U F U}. */
        MIDDLE,
        /** FULL over exactly one partition, both endpoints on real partition tokens: {@code U F U}. */
        SINGLE_PARTITION,
        /** Right endpoint exactly on the first partition's token: {@code F U}. */
        TOUCH_FIRST,
        /** Left endpoint exactly on the second-to-last partition's token: {@code U F}. */
        TOUCH_LAST,
        /** A range strictly inside a gap between two adjacent tokens: covers nothing. */
        GAP_MISS,
        /** A range entirely below the sstable's token span: covers nothing. */
        BELOW_MISS,
        /** A range entirely above the sstable's token span: covers nothing. */
        ABOVE_MISS,
        /** FULL immediately followed by TRANSIENT: {@code U F T U}, the widest eligible shape. */
        FULL_THEN_TRANSIENT,
        /** TRANSIENT over a middle span, no full ranges at all: {@code U T U}. */
        TRANSIENT_MIDDLE,
        /** A TRANSIENT range nested inside a FULL one: full wins, so still {@code U F U}. */
        TRANSIENT_INSIDE_FULL,
        /** A FULL range nested inside a TRANSIENT one: {@code U T F T U}, so TRANSIENT is not contiguous. */
        FULL_INSIDE_TRANSIENT,
        /** Two disjoint FULL ranges with unrepaired partitions between them. */
        INTERLEAVED_FULL,
        /** Alternating single-partition FULL ranges: the vnode layout, with more runs than are retained. */
        VNODE
    }

    private static final Shape[] SHAPES = Shape.values();

    // These three are read straight from the system properties rather than through
    // CassandraRelevantProperties: they are replay knobs for this one test class, never read by production
    // code, and adding them to the enum would put test scaffolding into the production configuration surface.
    /**
     * Fresh per run unless overridden, which is the difference between a fuzz test and sixteen fixed cases: with
     * {@code ITERATIONS == SHAPES.length} a hardcoded base seed pinned exactly one configuration per shape forever,
     * leaving most of the compressor x chunk x column-index x width matrix permanently untested. Randomising costs
     * nothing in reproducibility -- {@link #replayHint} quotes the per-iteration seed, and this value is logged and
     * repeated in every summary assertion so a whole run can be replayed too.
     */
    private static final long BASE_SEED =
        Long.getLong(PROP_SEED, System.nanoTime());    // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final int ITERATIONS =
        Integer.getInteger(PROP_ITERATIONS, SHAPES.length); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    /** When set, exactly one iteration runs, with this literal seed. */
    private static final Long REPLAY_SEED =
        Long.getLong(PROP_REPLAY_SEED);                // checkstyle: suppress nearby 'blockSystemPropertyUsage'

    /** Explicit insert timestamps keep the on-disk layout stable across runs of the same seed. */
    private static final long BASE_TS = 1_600_000_000_000_000L;

    private static final String[] COMPRESSORS = { "LZ4Compressor", "SnappyCompressor",
                                                  "DeflateCompressor", "ZstdCompressor" };
    private static final int[] CHUNK_KB = { 4, 8, 16, 32, 64 };
    /** Small values force a promoted index into Index.db, which the planner's walk has to skip over. */
    private static final int[] COLUMN_INDEX_KB = { 1, 2, 4, 64 };
    private static final int[] COLUMN_INDEX_CACHE_KB = { 0, 2, 99999 };

    /** Enough distinct tokens for every shape's index arithmetic to have room. */
    private static final int MIN_DISTINCT_TOKENS = 8;

    /** BTI is optional in a build; where it is registered, half the iterations use it. */
    private static final boolean BTI_AVAILABLE = DatabaseDescriptor.getSSTableFormats().containsKey(BtiFormat.NAME);

    private int eligibleIterations;
    private int ineligibleIterations;
    private final Map<String, String> verdictByShape = new TreeMap<>();
    /** The last free-space verdict computed, so the summary assertion can name an environmental cause. */
    private String lastFreeSpaceVerdict = "not evaluated";

    private int savedIndexSize;
    private int savedCacheSize;
    private FlushCompression savedFlushCompression;
    private boolean savedZeroCopy;
    private SSTableFormat<?, ?> savedFormat;

    @Before
    public void saveConfig()
    {
        savedIndexSize = DatabaseDescriptor.getColumnIndexSizeInKiB();
        savedCacheSize = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
        savedFlushCompression = DatabaseDescriptor.getFlushCompression();
        savedZeroCopy = DatabaseDescriptor.getZeroCopyAnticompactionEnabled();
        savedFormat = DatabaseDescriptor.getSelectedSSTableFormat();
    }

    /**
     * Unconditional and outside the test body, so nothing this class moves can leak into the rest of the JVM --
     * including via an {@code Assume} that skips before the old inline {@code finally} was reached. Five global
     * settings are in play: two column-index sizes, the flush compression mode, the feature flag and the selected
     * sstable format.
     */
    @After
    public void restoreConfig()
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(savedIndexSize);
        DatabaseDescriptor.setColumnIndexCacheSize(savedCacheSize);
        DatabaseDescriptor.setFlushCompression(savedFlushCompression);
        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(savedZeroCopy);
        TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(savedFormat);
    }

    @Test
    public void fuzzRangeSets() throws Throwable
    {
        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);

        if (REPLAY_SEED != null)
        {
            logger.info("Replaying a single zero-copy anticompaction fuzz iteration, seed {}", REPLAY_SEED);
            runGuarded(REPLAY_SEED);
        }
        else
        {
            logger.info("Zero-copy anticompaction fuzz on the {} format: {} iterations from base seed {} over {} " +
                        "range shapes. Replay this whole run with -D{}={}",
                        DatabaseDescriptor.getSelectedSSTableFormat().name(), ITERATIONS, BASE_SEED, SHAPES.length,
                        PROP_SEED, BASE_SEED);
            for (int i = 0; i < ITERATIONS; i++)
                runGuarded(seedForIteration(i));
        }

        logger.info("Zero-copy anticompaction fuzz done: {} iterations took the zero-copy split, {} fell back to " +
                    "the rewrite path. Per shape: {}", eligibleIterations, ineligibleIterations, verdictByShape);

        // Without at least one eligible iteration the oracle above would only ever have exercised the
        // pre-existing rewrite path, i.e. this test would be vacuous with respect to the feature it covers.
        // The two environmental ways that happens without anything being wrong are named here, because "no
        // iteration reached the zero-copy split path" is otherwise indistinguishable from a real regression.
        assertTrue("no iteration reached the zero-copy split path (base seed " + BASE_SEED + ", format "
                   + DatabaseDescriptor.getSelectedSSTableFormat().name() + ", per shape: " + verdictByShape
                   + "); this fuzz is no longer testing the feature it exists for. Two ENVIRONMENTAL causes to rule"
                   + " out before looking for a bug: (1) a disk with too little free space -- the last free-space"
                   + " verdict was [" + lastFreeSpaceVerdict + "], and the precheck honours"
                   + " min_free_space_per_drive and max_space_usable_for_compactions_in_percentage, so a nearly"
                   + " full volume declines every eligible sstable; (2) an sstable version below BIG 'pb' / BTI"
                   + " 'eb', which storage_compatibility_mode pins and which makes the whole feature inert",
                   eligibleIterations > 0);
        assertEquals("some iteration was never classified as zero-copy or fallback",
                     REPLAY_SEED != null ? 1 : ITERATIONS, eligibleIterations + ineligibleIterations);
    }

    /**
     * Iteration {@code i} always gets shape {@code i % SHAPES.length}, but the shape is derived <i>from the
     * seed</i> rather than from {@code i}, so quoting the seed back is enough to replay the whole iteration.
     */
    private static long seedForIteration(int i)
    {
        long base = scramble(BASE_SEED + i) >>> 8;   // non-negative, and still far apart between iterations
        return base * SHAPES.length + (i % SHAPES.length);
    }

    private static Shape shapeForSeed(long seed)
    {
        return SHAPES[(int) Math.floorMod(seed, (long) SHAPES.length)];
    }

    private void runGuarded(long seed) throws Throwable
    {
        Config cfg = new Config(seed);
        try
        {
            runIteration(cfg);
        }
        catch (Throwable t)
        {
            throw new AssertionError("zero-copy anticompaction fuzz iteration FAILED\n" + cfg + '\n'
                                     + replayHint(seed), t);
        }
    }

    private static String replayHint(long seed)
    {
        // a bare -D does not reach the forked test JVM, hence -Dtest.jvm.args
        return "replay this case alone with:\n"
               + "  ant testsome"
               + " -Dtest.name=org.apache.cassandra.db.compaction.ZeroCopyAntiCompactionFuzzTest"
               + " -Dtest.methods=fuzzRangeSets"
               + " -Dtest.jvm.args=\"-D" + PROP_REPLAY_SEED + '=' + seed + '"';
    }

    // ------------------------------------------------------------------------------------------------
    // One iteration
    // ------------------------------------------------------------------------------------------------

    private void runIteration(Config cfg) throws Throwable
    {
        Random rnd = new Random(cfg.seed);

        cfg.shape = shapeForSeed(cfg.seed);
        // Flipped per iteration, so a single run exercises both key-resolution paths. The selected format is global
        // and read at flush time, hence set before createTable below and restored in @After.
        cfg.bti = BTI_AVAILABLE && rnd.nextBoolean();
        TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(cfg.bti ? BtiFormat.NAME : BigFormat.NAME);
        cfg.compressor = COMPRESSORS[rnd.nextInt(COMPRESSORS.length)];
        cfg.chunkKb = CHUNK_KB[rnd.nextInt(CHUNK_KB.length)];
        cfg.columnIndexKb = COLUMN_INDEX_KB[rnd.nextInt(COLUMN_INDEX_KB.length)];
        cfg.columnIndexCacheKb = COLUMN_INDEX_CACHE_KB[rnd.nextInt(COLUMN_INDEX_CACHE_KB.length)];
        cfg.wide = rnd.nextBoolean();
        cfg.partitions = 16 + rnd.nextInt(25);
        cfg.totalBytes = 300_000 + rnd.nextInt(400_000);

        long perPartition = Math.max(200, cfg.totalBytes / cfg.partitions);
        cfg.rowsPerPartition = cfg.wide ? 6 + rnd.nextInt(19) : 1 + rnd.nextInt(2);
        cfg.valueBytes = (int) Math.min(24_000, Math.max(48, perPartition / cfg.rowsPerPartition));

        DatabaseDescriptor.setColumnIndexSizeInKiB(cfg.columnIndexKb);
        DatabaseDescriptor.setColumnIndexCacheSize(cfg.columnIndexCacheKb);
        // flush_compression defaults to `fast`, which silently replaces any compressor that does not advertise
        // FAST_COMPRESSION with CompressionParams.DEFAULT -- LZ4 at 16 KiB. Without this, every non-LZ4
        // iteration below would quietly test the same single configuration.
        DatabaseDescriptor.setFlushCompression(FlushCompression.table);

        createTable(ddl(cfg));
        disableCompaction();
        writeData(cfg, rnd);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TableMetadata metadata = cfs.metadata();
        assertTrue("this test builds its ranges out of Murmur3 long tokens; CQLTester is supposed to force "
                   + "Murmur3Partitioner but the table is on " + cfs.getPartitioner(),
                   cfs.getPartitioner() instanceof Murmur3Partitioner);

        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals("expected exactly one sstable after the flush, got " + live, 1, live.size());
        SSTableReader parent = live.iterator().next();

        cfg.format = parent.descriptor.getFormat().name();
        cfg.version = parent.descriptor.version.version;

        assertTrue("the generator produced an uncompressed sstable, so the whole compressor matrix is void",
                   parent.compression);
        // Not "should be splittable" but "this run cannot test the feature at all": a version below BIG 'pb' /
        // BTI 'eb' cannot carry StatsMetadata.hasUnindexedRegions, so isSupported() is false for every sstable and
        // every iteration would take the fallback while the oracle happily passed.
        assertTrue("sstable version '" + cfg.version + "' cannot carry the StatsMetadata.hasUnindexedRegions marker"
                   + " (BIG needs 'pb', BTI 'eb'), so zero-copy splitting is INERT for every iteration of this run."
                   + " storage_compatibility_mode pins the version written and must be NONE. " + cfg,
                   parent.descriptor.version.hasUnindexedRegionsMarker());
        assertTrue("a compressed " + cfg.format + " sstable must be splittable. " + cfg,
                   ZeroCopySSTableSplitter.isSupported(parent));
        // If flush_compression silently downgraded the table's compression this is where it shows up.
        assertEquals("the table's chunk_length_in_kb did not survive to the sstable; flush_compression has "
                     + "replaced the requested compressor and this iteration would test nothing",
                     cfg.chunkKb * 1024, parent.getCompressionMetadata().chunkLength());
        assertEquals("the table's compressor did not survive to the sstable; flush_compression has replaced it "
                     + "and this iteration would test nothing",
                     cfg.compressor,
                     parent.getCompressionMetadata().parameters.getSstableCompressor().getClass().getSimpleName());

        // ---- the parent, as read before anything touches it: the oracle's left-hand side ----
        Map<String, String> before = new HashMap<>();
        List<DecoratedKey> keysInOrder = new ArrayList<>();
        readPartitions(parent, metadata, before, keysInOrder);
        assertEquals("the generator did not write one partition per pk", cfg.partitions, keysInOrder.size());
        cfg.parentPartitions = keysInOrder.size();

        List<Long> distinctTokens = new ArrayList<>(new TreeSet<>(tokensOf(keysInOrder)));
        assertTrue("only " + distinctTokens.size() + " distinct tokens; the range shapes need at least "
                   + MIN_DISTINCT_TOKENS, distinctTokens.size() >= MIN_DISTINCT_TOKENS);

        // ---- the range set ----
        TimeUUID sessionID = nextTimeUUID();
        Set<Range<Token>> fullRanges = new LinkedHashSet<>();
        Set<Range<Token>> transientRanges = new LinkedHashSet<>();
        buildRanges(cfg.shape, rnd, distinctTokens, fullRanges, transientRanges);
        transientRanges.removeAll(fullRanges);   // RangesAtEndpoint.Builder rejects one range being both
        ensureIntersectsSSTable(parent, distinctTokens, fullRanges, transientRanges);
        cfg.fullRanges = fullRanges.toString();
        cfg.transientRanges = transientRanges.toString();

        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(local);
        for (Range<Token> range : fullRanges)
            builder.add(new Replica(local, range, true));
        for (Range<Token> range : transientRanges)
            builder.add(new Replica(local, range, false));
        RangesAtEndpoint ranges = builder.build();

        // ---- what the range set implies, computed here from Range.contains alone ----
        List<AntiCompactionRunPlanner.Label> expectedLabels = new ArrayList<>(keysInOrder.size());
        for (DecoratedKey key : keysInOrder)
            expectedLabels.add(labelOf(key.getToken(), fullRanges, transientRanges));

        List<Integer> runStarts = runStarts(expectedLabels);
        int expectedRunCount = runStarts.size();
        int fullRuns = countRuns(expectedLabels, runStarts, AntiCompactionRunPlanner.Label.FULL);
        int transientRuns = countRuns(expectedLabels, runStarts, AntiCompactionRunPlanner.Label.TRANSIENT);
        boolean expectedEligible = expectedRunCount >= 2 && fullRuns <= 1 && transientRuns <= 1;
        Set<AntiCompactionRunPlanner.Label> labelsPresent = new TreeSet<>(expectedLabels);

        cfg.labels = expectedLabels.toString();
        cfg.runCount = expectedRunCount;
        cfg.expectedEligible = expectedEligible;
        verdictByShape.put(cfg.shape.name(), (expectedEligible ? "zero-copy" : "fallback")
                                             + " runs=" + expectedRunCount);

        // ---- the planner must agree, on every detail the split depends on ----
        AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.plan(parent, ranges, sessionID);
        assertEquals(cfg + " -- planner disagrees about eligibility (" + plan + ')',
                     expectedEligible, plan.eligible);
        assertEquals(cfg + " -- planner counted the wrong number of runs (" + plan + ')',
                     expectedRunCount, plan.runCount);
        if (expectedEligible)
        {
            List<DecoratedKey> expectedBoundaries = new ArrayList<>();
            for (int b = 1; b < runStarts.size(); b++)
                expectedBoundaries.add(keysInOrder.get(runStarts.get(b)));
            assertEquals(cfg + " -- wrong split boundaries", expectedBoundaries, plan.boundaries);

            List<ZeroCopySSTableSplitter.RepairState> expectedStates = new ArrayList<>();
            for (int start : runStarts)
                expectedStates.add(expectedState(expectedLabels.get(start), sessionID));
            assertEquals(cfg + " -- wrong per-child repair state", expectedStates, plan.perChild);
        }
        else
        {
            assertNotNull(cfg + " -- an ineligible plan must say why", plan.ineligibleReason);
        }

        // ---- the environmental precondition, asked the same way CompactionManager asks it ----
        // Recorded BEFORE the run, while the parent still exists, so that a "the plan was eligible but the metric
        // did not move" failure below already carries the answer instead of looking like a gate regression.
        if (expectedEligible)
        {
            cfg.freeSpace = freeSpaceVerdict(cfs, parent, plan.perChild.size());
            lastFreeSpaceVerdict = cfg.freeSpace;
        }

        // ---- run the anticompaction through the real public entry point ----
        long zcBytesBefore = cfs.metric.bytesZeroCopyAnticompaction.table.getCount();
        try
        {
            ActiveRepairService.instance().registerParentRepairSession(sessionID, local, Lists.newArrayList(cfs),
                                                                       ranges.ranges(), true, UNREPAIRED_SSTABLE,
                                                                       true, PreviewKind.NONE);
            Set<SSTableReader> sstables = new HashSet<>(live);
            try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
                 Refs<SSTableReader> refs = Refs.ref(sstables))
            {
                assertNotNull(cfg + " -- could not mark the sstable compacting", txn);
                CompactionManager.instance.performAnticompaction(cfs, ranges, refs, txn, sessionID, () -> false);
            }
        }
        finally
        {
            ActiveRepairService.instance().removeParentRepairSession(sessionID);
        }
        long zcBytesAfter = cfs.metric.bytesZeroCopyAnticompaction.table.getCount();

        // ---- which path actually ran ----
        if (expectedEligible)
        {
            eligibleIterations++;
            assertTrue(cfg + " -- the plan was eligible but BytesZeroCopyAnticompaction did not move ("
                       + zcBytesBefore + " -> " + zcBytesAfter + "), so the rewrite path ran instead. If the"
                       + " free-space verdict above says FAILED this is ENVIRONMENTAL, not a gate regression:"
                       + " CompactionManager.zeroCopyAntiCompact declines an eligible sstable when"
                       + " Directories.hasDiskSpaceForCompactionsAndStreams says the destination may not have room,"
                       + " which honours min_free_space_per_drive and"
                       + " max_space_usable_for_compactions_in_percentage",
                       zcBytesAfter > zcBytesBefore);
        }
        else
        {
            ineligibleIterations++;
            assertEquals(cfg + " -- the plan was ineligible but BytesZeroCopyAnticompaction moved, so the gate "
                         + "let a zero-copy split through", zcBytesBefore, zcBytesAfter);
        }

        // The split produces exactly one sstable per run; the rewrite produces exactly one per non-empty
        // destination, i.e. one per label that any partition carries. (The whole-sstable-FULL case that
        // mutateFullyContainedSSTables serves is a single label too, so it needs no special case here.)
        int expectedOutputs = expectedEligible ? expectedRunCount : labelsPresent.size();
        Util.assertOnDiskState(cfs, expectedOutputs);

        // An sstable whose whole token span sits inside one range never gets rewritten or split at all:
        // mutateFullyContainedSSTables rewrites its metadata in place and drops it from the transaction.
        boolean fullyContained = fullyContained(parent, fullRanges) || fullyContained(parent, transientRanges);
        if (fullyContained)
        {
            assertEquals(cfg + " -- every partition of a fully contained sstable must carry the same label, "
                         + "otherwise the in-place metadata mutation mislabels some of them",
                         1, labelsPresent.size());
            assertTrue(cfg + " -- a fully contained sstable must be kept and only have its metadata mutated",
                       cfs.getLiveSSTables().contains(parent));
        }
        else
        {
            assertFalse(cfg + " -- the parent was not obsoleted; its data now exists twice",
                        cfs.getLiveSSTables().contains(parent));
        }

        // ---- THE ORACLE ----
        Map<String, String> after = new HashMap<>();
        Map<String, AntiCompactionRunPlanner.Label> expectedByKey = new HashMap<>();
        for (int i = 0; i < keysInOrder.size(); i++)
            expectedByKey.put(hex(keysInOrder.get(i)), expectedLabels.get(i));

        for (SSTableReader output : cfs.getLiveSSTables())
        {
            assertFalse(cfg + " -- " + output.descriptor + " was marked repaired; anticompaction only ever "
                        + "produces pending-repair or unrepaired sstables", output.isRepaired());

            List<DecoratedKey> outputKeys = new ArrayList<>();
            Map<String, String> outputPartitions = new HashMap<>();
            readPartitions(output, metadata, outputPartitions, outputKeys);
            assertFalse(cfg + " -- " + output.descriptor + " is empty", outputKeys.isEmpty());

            for (DecoratedKey key : outputKeys)
            {
                String hex = hex(key);
                AntiCompactionRunPlanner.Label expected = expectedByKey.get(hex);
                assertNotNull(cfg + " -- " + output.descriptor + " holds a partition the parent never had: " + key,
                              expected);

                // (2) the repair state of the sstable a partition landed in must match its token's label
                ZeroCopySSTableSplitter.RepairState want = expectedState(expected, sessionID);
                assertEquals(cfg + " -- " + key + " is labelled " + expected + " but landed in "
                             + output.descriptor + " with pendingRepair=" + output.getPendingRepair(),
                             want.pendingRepair, output.getPendingRepair());
                assertEquals(cfg + " -- " + key + " is labelled " + expected + " but landed in "
                             + output.descriptor + " with isTransient=" + output.isTransient(),
                             want.isTransient, output.isTransient());

                // (1a) exactly once
                String previous = after.put(hex, outputPartitions.get(hex));
                assertNull(cfg + " -- " + key + " appears in more than one output sstable (again in "
                           + output.descriptor + ')', previous);
            }
        }

        // (1b) all of them, content-identical
        assertEquals(cfg + " -- the outputs do not hold exactly the parent's partitions",
                     new TreeSet<>(before.keySet()), new TreeSet<>(after.keySet()));
        for (Map.Entry<String, String> entry : before.entrySet())
            assertEquals(cfg + " -- partition " + entry.getKey() + " changed content",
                         entry.getValue(), after.get(entry.getKey()));

        for (SSTableReader output : cfs.getLiveSSTables())
        {
            assertFalse(cfg + " -- " + output.descriptor + " is marked compacted", output.isMarkedCompacted());
            assertEquals(cfg + " -- leaked reference on " + output.descriptor,
                         1, output.selfRef().globalCount());
        }
        assertEquals(cfg + " -- sstables left marked compacting", 0, cfs.getTracker().getCompacting().size());
    }

    // ------------------------------------------------------------------------------------------------
    // The independent labelling / run oracle
    // ------------------------------------------------------------------------------------------------

    /**
     * Deliberately the naive form -- a linear scan of the raw ranges with {@code Range.contains} --
     * so it shares nothing with the production {@code OrderedRangeContainmentChecker} (normalize plus a
     * forward-only cursor) that both the planner and the rewrite path use. Full wins over transient, which is
     * the precedence {@code antiCompactGroup} routes by.
     */
    private static AntiCompactionRunPlanner.Label labelOf(Token token,
                                                         Collection<Range<Token>> full,
                                                         Collection<Range<Token>> trans)
    {
        for (Range<Token> range : full)
            if (range.contains(token))
                return AntiCompactionRunPlanner.Label.FULL;
        for (Range<Token> range : trans)
            if (range.contains(token))
                return AntiCompactionRunPlanner.Label.TRANSIENT;
        return AntiCompactionRunPlanner.Label.UNREPAIRED;
    }

    /** The index of the first partition of each contiguous run of identical labels. */
    private static List<Integer> runStarts(List<AntiCompactionRunPlanner.Label> labels)
    {
        List<Integer> starts = new ArrayList<>();
        AntiCompactionRunPlanner.Label previous = null;
        for (int i = 0; i < labels.size(); i++)
        {
            if (labels.get(i) != previous)
            {
                starts.add(i);
                previous = labels.get(i);
            }
        }
        return starts;
    }

    private static int countRuns(List<AntiCompactionRunPlanner.Label> labels,
                                 List<Integer> runStarts,
                                 AntiCompactionRunPlanner.Label label)
    {
        int count = 0;
        for (int start : runStarts)
            if (labels.get(start) == label)
                count++;
        return count;
    }

    /** The triples {@code createWriterForAntiCompaction} is called with, spelled out rather than delegated. */
    private static ZeroCopySSTableSplitter.RepairState expectedState(AntiCompactionRunPlanner.Label label,
                                                                    TimeUUID sessionID)
    {
        switch (label)
        {
            case FULL:
                return new ZeroCopySSTableSplitter.RepairState(UNREPAIRED_SSTABLE, sessionID, false);
            case TRANSIENT:
                return new ZeroCopySSTableSplitter.RepairState(UNREPAIRED_SSTABLE, sessionID, true);
            default:
                return new ZeroCopySSTableSplitter.RepairState(UNREPAIRED_SSTABLE, NO_PENDING_REPAIR, false);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Range-set generation
    // ------------------------------------------------------------------------------------------------

    private static void buildRanges(Shape shape,
                                    Random rnd,
                                    List<Long> tokens,
                                    Set<Range<Token>> full,
                                    Set<Range<Token>> trans)
    {
        int n = tokens.size();
        switch (shape)
        {
            case COVER_ALL:
                full.add(range(Long.MIN_VALUE, tokens.get(n - 1)));
                break;
            case PREFIX:
                full.add(range(Long.MIN_VALUE, tokens.get(pick(rnd, 0, n - 3))));
                break;
            case SUFFIX:
                full.add(range(tokens.get(pick(rnd, 0, n - 2)), Long.MAX_VALUE));
                break;
            case MIDDLE:
            {
                int i = pick(rnd, 0, n - 4);
                int j = pick(rnd, i + 1, n - 2);
                full.add(range(tokens.get(i), tokens.get(j)));
                break;
            }
            case SINGLE_PARTITION:
            {
                int i = pick(rnd, 1, n - 2);
                full.add(range(tokens.get(i - 1), tokens.get(i)));
                break;
            }
            case TOUCH_FIRST:
                full.add(range(Long.MIN_VALUE, tokens.get(0)));
                break;
            case TOUCH_LAST:
                full.add(range(tokens.get(n - 2), tokens.get(n - 1)));
                break;
            case GAP_MISS:
                full.add(gapRange(tokens));
                break;
            case BELOW_MISS:
                // strictly below every partition; ensureIntersectsSSTable() adds what validation needs
                full.add(range(Long.MIN_VALUE, tokens.get(0) - 1));
                break;
            case ABOVE_MISS:
                full.add(range(tokens.get(n - 1), Long.MAX_VALUE));
                break;
            case FULL_THEN_TRANSIENT:
            {
                int i = pick(rnd, 0, n - 4);
                int j = pick(rnd, i + 1, n - 3);
                int k = pick(rnd, j + 1, n - 2);
                full.add(range(tokens.get(i), tokens.get(j)));
                trans.add(range(tokens.get(j), tokens.get(k)));
                break;
            }
            case TRANSIENT_MIDDLE:
            {
                int i = pick(rnd, 0, n - 4);
                int j = pick(rnd, i + 1, n - 2);
                trans.add(range(tokens.get(i), tokens.get(j)));
                break;
            }
            case TRANSIENT_INSIDE_FULL:
            {
                int i = pick(rnd, 0, n - 5);
                int j = pick(rnd, i + 3, n - 2);
                full.add(range(tokens.get(i), tokens.get(j)));
                trans.add(range(tokens.get(i + 1), tokens.get(j - 1)));
                break;
            }
            case FULL_INSIDE_TRANSIENT:
            {
                int i = pick(rnd, 0, n - 5);
                int j = pick(rnd, i + 3, n - 2);
                trans.add(range(tokens.get(i), tokens.get(j)));
                full.add(range(tokens.get(i + 1), tokens.get(j - 1)));
                break;
            }
            case INTERLEAVED_FULL:
            {
                int a = pick(rnd, 0, n - 5);
                int b = pick(rnd, a + 1, n - 4);
                int c = pick(rnd, b + 1, n - 3);
                int d = pick(rnd, c + 1, n - 2);
                full.add(range(tokens.get(a), tokens.get(b)));
                full.add(range(tokens.get(c), tokens.get(d)));
                break;
            }
            case VNODE:
                for (int i = 0; i + 1 <= n - 2 && full.size() < 6; i += 2)
                    full.add(range(tokens.get(i), tokens.get(i + 1)));
                break;
            default:
                throw new AssertionError("unhandled shape " + shape);
        }
    }

    /**
     * {@code validateSSTableBoundsForAnticompaction} (CompactionManager) throws outright if no range even
     * intersects the sstable's bounds, which the deliberately-missing shapes would otherwise trip. Add a range
     * that lives strictly inside a gap between two adjacent tokens: it satisfies validation without covering a
     * single partition, so the labelling -- and therefore the oracle -- is untouched.
     */
    private static void ensureIntersectsSSTable(SSTableReader parent,
                                                List<Long> tokens,
                                                Set<Range<Token>> full,
                                                Set<Range<Token>> trans)
    {
        if (intersectsBounds(parent, full, trans))
            return;

        Range<Token> gap = gapRange(tokens);
        if (full.isEmpty())
            trans.add(gap);
        else
            full.add(gap);
        assertTrue("could not make the range set intersect the sstable bounds even with the gap range " + gap,
                   intersectsBounds(parent, full, trans));
    }

    /** The {@code findSSTablesToAnticompact} predicate: is the whole token span inside a single range? */
    private static boolean fullyContained(SSTableReader parent, Set<Range<Token>> ranges)
    {
        if (ranges.isEmpty())
            return false;
        Token first = parent.getFirst().getToken();
        Token last = parent.getLast().getToken();
        for (Range<Token> range : Range.normalize(ranges))
        {
            if (range.contains(first) && range.contains(last))
                return true;
        }
        return false;
    }

    private static boolean intersectsBounds(SSTableReader parent,
                                            Set<Range<Token>> full,
                                            Set<Range<Token>> trans)
    {
        List<Range<Token>> all = new ArrayList<>(full);
        all.addAll(trans);
        Bounds<Token> bounds = new Bounds<>(parent.getFirst().getToken(), parent.getLast().getToken());
        for (Range<Token> range : Range.normalize(all))
        {
            if ((range.contains(bounds.left) && range.contains(bounds.right)) || range.intersects(bounds))
                return true;
        }
        return false;
    }

    /** A range strictly between two adjacent partition tokens, so it can never contain a partition. */
    private static Range<Token> gapRange(List<Long> tokens)
    {
        for (int i = 0; i + 1 < tokens.size(); i++)
        {
            if (tokens.get(i + 1) - tokens.get(i) >= 2)
                return range(tokens.get(i), tokens.get(i + 1) - 1);
        }
        throw new AssertionError("no gap of two or more between any adjacent tokens: " + tokens);
    }

    private static Range<Token> range(long left, long right)
    {
        assertTrue("refusing to build the wraparound/full-ring range (" + left + ", " + right + ']', left < right);
        return new Range<>(new Murmur3Partitioner.LongToken(left), new Murmur3Partitioner.LongToken(right));
    }

    private static int pick(Random rnd, int lo, int hi)
    {
        assertTrue("empty index range [" + lo + ',' + hi + "]; the shape needs more distinct tokens", lo <= hi);
        return lo + rnd.nextInt(hi - lo + 1);
    }

    // ------------------------------------------------------------------------------------------------
    // Schema, data and reading
    // ------------------------------------------------------------------------------------------------

    private static String ddl(Config cfg)
    {
        return "CREATE TABLE %s (pk text, ck int, v blob, s text static, PRIMARY KEY (pk, ck))"
               + " WITH compression = {'class': '" + cfg.compressor
               + "', 'chunk_length_in_kb': " + cfg.chunkKb + '}'
               // far in the future, so no tombstone this test writes could ever become droppable and let the
               // rewrite path legitimately diverge from the copy path
               + " AND gc_grace_seconds = 864000";
    }

    /**
     * One INSERT per {@code (pk, ck)} and one per static row, all at distinct explicit timestamps: nothing is
     * overwritten, nothing is deleted, so neither anticompaction path is permitted to change the content.
     */
    private void writeData(Config cfg, Random rnd) throws Throwable
    {
        long ts = BASE_TS;
        for (int p = 0; p < cfg.partitions; p++)
        {
            String pk = String.format("p%05d", p);
            for (int r = 0; r < cfg.rowsPerPartition; r++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                        pk, r, blob(rnd, cfg.valueBytes, rnd.nextInt(4) == 0), ts++);
            if (rnd.nextBoolean())
                execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP ?", pk, text(rnd, 24), ts++);
        }
    }

    private static ByteBuffer blob(Random rnd, int size, boolean compressible)
    {
        byte[] bytes = new byte[size];
        if (compressible)
        {
            Arrays.fill(bytes, (byte) ('a' + rnd.nextInt(26)));
            for (int i = 0; i < bytes.length; i += 512)
                bytes[i] = (byte) rnd.nextInt();
        }
        else
        {
            rnd.nextBytes(bytes);   // near-incompressible, so the sstable really spans many chunks
        }
        return ByteBuffer.wrap(bytes);
    }

    private static String text(Random rnd, int length)
    {
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = (char) ('a' + rnd.nextInt(26));
        return new String(chars);
    }

    /**
     * Reads every partition of {@code sstable}, appending a fully-detailed rendering of its content to
     * {@code contents} (keyed by the hex of the partition key) and its key to {@code keys}, in token order.
     */
    private static void readPartitions(SSTableReader sstable,
                                       TableMetadata metadata,
                                       Map<String, String> contents,
                                       List<DecoratedKey> keys)
    {
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    // clone the key: the scanner's buffers are not guaranteed to outlive the iterator
                    DecoratedKey key = sstable.getPartitioner()
                                              .decorateKey(ByteBufferUtil.clone(partition.partitionKey().getKey()));
                    StringBuilder sb = new StringBuilder();
                    sb.append("deletion=").append(partition.partitionLevelDeletion());
                    sb.append(" static=").append(partition.staticRow().toString(metadata, true));
                    while (partition.hasNext())
                        sb.append("\n  ").append(partition.next().toString(metadata, true));

                    String previous = contents.put(hex(key), sb.toString());
                    assertNull("the same partition key appears twice inside " + sstable.descriptor + ": " + key,
                               previous);
                    keys.add(key);
                }
            }
        }
    }

    /**
     * {@code Murmur3Partitioner.LongToken.token} is package private, so the primitive comes out of
     * {@link Token#getLongValue()} -- which is exactly that field for a {@code LongToken} and throws
     * {@link UnsupportedOperationException} for any partitioner that is not backed by a long, i.e. it keeps the
     * Murmur3 assumption this generator is built on explicit.
     */
    private static List<Long> tokensOf(List<DecoratedKey> keys)
    {
        List<Long> tokens = new ArrayList<>(keys.size());
        for (DecoratedKey key : keys)
            tokens.add(key.getToken().getLongValue());
        return tokens;
    }

    private static String hex(DecoratedKey key)
    {
        return ByteBufferUtil.bytesToHex(key.getKey());
    }

    /**
     * The exact question {@code CompactionManager.zeroCopyAntiCompact} asks before it takes the zero-copy path, with
     * the same estimate: every component of the parent, plus one boundary chunk per interior boundary (the chunk a
     * boundary falls inside is copied into both of its neighbours) and one {@code Reflink.RANGE_ALIGNMENT} head pad
     * per child. Reproduced here rather than inferred, so a decline is legible as an environmental fact.
     */
    private static String freeSpaceVerdict(ColumnFamilyStore cfs, SSTableReader parent, int children)
    {
        try
        {
            File destination = parent.descriptor.directory;
            long chunkLength = parent.getCompressionMetadata().chunkLength();
            long needed = parent.bytesOnDisk()
                          + (children - 1) * chunkLength
                          + children * Reflink.RANGE_ALIGNMENT;
            boolean hasSpace = cfs.getDirectories()
                                  .hasDiskSpaceForCompactionsAndStreams(
                                      Collections.singletonMap(destination, needed),
                                      CompactionManager.instance.active.estimatedRemainingWriteToDiskBytes());
            return (hasSpace ? "PASSED" : "FAILED") + " for " + needed + " bytes in " + destination
                   + " (usable " + destination.toJavaIOFile().getUsableSpace() + ')';
        }
        catch (Throwable t)
        {
            // The production path declines rather than skips on a stat failure, so this must not hide one.
            return "NOT EVALUABLE: " + t;
        }
    }

    /** splitmix64, so consecutive base seeds give uncorrelated iterations. */
    private static long scramble(long seed)
    {
        long z = seed + 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /** Everything needed to understand -- and replay -- one iteration. Mutated as the iteration progresses. */
    private static final class Config
    {
        final long seed;

        Shape shape;
        boolean bti;
        String format = "?";
        String version = "?";
        String freeSpace = "not evaluated";
        String compressor = "?";
        int chunkKb = -1;
        int columnIndexKb = -1;
        int columnIndexCacheKb = -1;
        boolean wide;
        int partitions = -1;
        int rowsPerPartition = -1;
        int valueBytes = -1;
        long totalBytes = -1;

        int parentPartitions = -1;
        String fullRanges = "?";
        String transientRanges = "?";
        String labels = "?";
        int runCount = -1;
        boolean expectedEligible;

        Config(long seed)
        {
            this.seed = seed;
        }

        @Override
        public String toString()
        {
            return "seed=" + seed
                   + " shape=" + shape
                   + " requestedBti=" + bti
                   + " format=" + format
                   + " version=" + version
                   + " compressor=" + compressor
                   + " chunkKb=" + chunkKb
                   + " columnIndexKb=" + columnIndexKb
                   + " columnIndexCacheKb=" + columnIndexCacheKb
                   + " wide=" + wide
                   + " partitions=" + partitions
                   + " rowsPerPartition=" + rowsPerPartition
                   + " valueBytes=" + valueBytes
                   + " totalBytes=" + totalBytes
                   + " parentPartitions=" + parentPartitions
                   + " runCount=" + runCount
                   + " expectedEligible=" + expectedEligible
                   + "\n  freeSpacePrecheck=" + freeSpace
                   + "\n  fullRanges=" + fullRanges
                   + "\n  transientRanges=" + transientRanges
                   + "\n  labels=" + labels;
        }
    }
}
