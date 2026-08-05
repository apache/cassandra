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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.CRC32;

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
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.CompressionInfoComponent;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Randomised end-to-end test of {@link ZeroCopySSTableSplitter}.
 * <p>
 * The oracle never changes: <b>the concatenation of the children, read in order, must equal the parent,
 * partition for partition and unfiltered for unfiltered</b>. Everything else that is asserted (chunk
 * arithmetic, physical file lengths, rebased index positions, the digest) is a cross-check derived
 * independently from the parent's own {@code CompressionMetadata} and {@code Index.db}, not from the values
 * the splitter reports.
 *
 * <h2>Reproducing a failure</h2>
 * Every iteration derives its own seed and every assertion message carries the whole configuration plus that
 * seed. To replay exactly one failing iteration, and nothing else:
 * <pre>
 *   ant testsome -Dtest.name=org.apache.cassandra.io.sstable.ZeroCopySSTableSplitterFuzzTest \
 *       -Dtest.methods=fuzz \
 *       -Dtest.jvmargs="-Dcassandra.test.zerocopysplitter.replaySeed=&lt;seed from the failure message&gt;"
 * </pre>
 * For a long soak run, raise the iteration count and/or move the base seed:
 * <pre>
 *   -Dtest.jvmargs="-Dcassandra.test.zerocopysplitter.iterations=500
 *                   -Dcassandra.test.zerocopysplitter.seed=12345"
 * </pre>
 * The default of {@value #DEFAULT_ITERATIONS} iterations is deliberately modest so this stays inside a normal
 * unit-test run.
 * <p>
 * The whole class is BIG-only: {@link #readIndex} walks {@code Index.db} directly and the splitter refuses
 * anything but {@link BigFormat}, so every test starts with {@code assumeTrue(BigFormat.isSelected())}.
 */
public class ZeroCopySSTableSplitterFuzzTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(ZeroCopySSTableSplitterFuzzTest.class);

    private static final String PROP_SEED = "cassandra.test.zerocopysplitter.seed";
    private static final String PROP_ITERATIONS = "cassandra.test.zerocopysplitter.iterations";
    private static final String PROP_REPLAY_SEED = "cassandra.test.zerocopysplitter.replaySeed";

    static final int DEFAULT_ITERATIONS = 24;

    // Developer-only replay knobs for this single test class, read directly rather than through
    // CassandraRelevantProperties: that class is an enum of node-level settings, it has no API for reading an
    // unregistered key, and three constants that only one fuzz test ever reads do not belong in it. Hence the
    // per-line suppressions of the blockSystemPropertyUsage rule. If a reviewer would rather see them
    // registered, the entries would be TEST_ZERO_COPY_SPLITTER_SEED / _ITERATIONS / _REPLAY_SEED and these
    // three lines become CassandraRelevantProperties.<X>.getLong(default) with the suppressions removed.
    private static final long BASE_SEED = Long.getLong(PROP_SEED, 20260726_0001L); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    private static final int ITERATIONS = Integer.getInteger(PROP_ITERATIONS, DEFAULT_ITERATIONS); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    /** When set, exactly one iteration runs, with this literal seed. */
    private static final Long REPLAY_SEED = Long.getLong(PROP_REPLAY_SEED); // checkstyle: suppress nearby 'blockSystemPropertyUsage'

    /** Explicit insert timestamps keep the on-disk layout stable across runs of the same seed. */
    private static final long PAST_TS = 1_600_000_000_000_000L;
    /** Strictly greater than any wall-clock timestamp a DELETE will get during this test. */
    private static final long FUTURE_TS = 2_000_000_000_000_000L;

    /** Keep a single iteration's sstable small enough that one flush is one sstable. */
    private static final long MAX_TABLE_BYTES = 1_200_000L;

    private static final String[] COMPRESSORS = { "LZ4Compressor", "SnappyCompressor", "DeflateCompressor",
                                                  "ZstdCompressor", null /* uncompressed: must be refused */ };
    private static final int[] CHUNK_KB = { 4, 8, 16, 32, 64 };
    /** 0 disables the raw-chunk fallback; > 1 makes most chunks store raw, which is the sharpest edge case. */
    private static final double[] MIN_COMPRESS_RATIO = { 0.0, 0.0, 1.0, 2.0, 8.0 };
    private static final int[] COLUMN_INDEX_KB = { 1, 2, 4, 16, 64 };
    private static final int[] COLUMN_INDEX_CACHE_KB = { 0, 2, 99999 };

    /**
     * Everything the generator actually produced across a run, logged once at the end. This is the only test that
     * generates tombstones (partition, row, range and single-cell), static rows, TTLs, collections and more than one
     * clustering column, so a reader of a green run should be able to see that it still does -- see also the pinned
     * first iteration in {@link #fuzz()}.
     */
    private final Set<String> exercised = new TreeSet<>();

    private int savedIndexSize;
    private int savedCacheSize;
    private FlushCompression savedFlushCompression;
    private boolean savedDigestEnabled;

    @Before
    public void saveConfigurationAndHooks()
    {
        savedIndexSize = DatabaseDescriptor.getColumnIndexSizeInKiB();
        savedCacheSize = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
        savedFlushCompression = DatabaseDescriptor.getFlushCompression();
        savedDigestEnabled = DatabaseDescriptor.getZeroCopySplitDigestEnabled();
    }

    /**
     * Every one of these is process-wide, and an iteration that fails part way through skips its own restore, so
     * this is the backstop: without it one failing iteration would silently change what every LATER test in the
     * same JVM exercises -- {@code forceAlignedLayoutForTesting} turning every subsequent split into a padded one,
     * and {@code zero_copy_split_digest_enabled} deciding whether siblings get a Digest.crc32 at all.
     */
    @After
    public void restoreConfigurationAndHooks()
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(savedIndexSize);
        DatabaseDescriptor.setColumnIndexCacheSize(savedCacheSize);
        DatabaseDescriptor.setFlushCompression(savedFlushCompression);
        DatabaseDescriptor.setZeroCopySplitDigestEnabled(savedDigestEnabled);
        ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = false;
        ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
    }

    // ------------------------------------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------------------------------------

    @Test
    public void fuzz() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        // Everything this loop changes process-wide is saved by @Before and restored by @After, including on the
        // iteration that throws.
        if (REPLAY_SEED != null)
        {
            logger.info("Replaying a single ZeroCopySSTableSplitter fuzz iteration, seed {}", REPLAY_SEED);
            runGuarded(REPLAY_SEED, false);
            return;
        }

        logger.info("ZeroCopySSTableSplitter fuzz: {} iterations from base seed {}", ITERATIONS, BASE_SEED);
        for (int i = 0; i < ITERATIONS; i++)
        {
            // The schema knobs are rolled per iteration, so on a short run a fixed seed could in principle leave
            // the widest schema this generator can produce untried -- and that schema is where the interesting
            // shapes live: a static row, two clustering columns (hence range tombstones with a clustering prefix)
            // and both collections. The first iteration is therefore pinned to it, so that those shapes are copied
            // verbatim through a split on EVERY run rather than on most seeds.
            runGuarded(scramble(BASE_SEED + i), i == 0);
        }
        logger.info("ZeroCopySSTableSplitter fuzz exercised: {}", exercised);
    }

    /**
     * The deliberately adversarial generator: every partition is calibrated to be exactly {@code L},
     * {@code L - 1} or {@code L + 1} bytes, so partition boundaries land exactly on, one byte before, and one
     * byte after a compression-chunk boundary. That is precisely where {@code (hi-1)/L} vs {@code hi/L},
     * {@code lo mod L} and the {@code O(j+1) - O(i)} physical length are most likely to be off by one.
     * <p>
     * Splitting at every partition maximises the number of such boundaries exercised.
     */
    @Test
    public void straddlesChunkBoundaries() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        int savedIndexSize = DatabaseDescriptor.getColumnIndexSizeInKiB();
        int savedCacheSize = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
        FlushCompression savedFlushCompression = DatabaseDescriptor.getFlushCompression();
        try
        {
            // partitions are one row each, so no promoted index; keep the grid coarse and predictable
            DatabaseDescriptor.setColumnIndexSizeInKiB(64);
            DatabaseDescriptor.setColumnIndexCacheSize(2);
            // Same reason as in runIteration(): without this the chunk_length_in_kb the scenario is built around
            // is silently replaced by LZ4 at 16 KiB for every non-LZ4 compressor, and the partitions engineered
            // to land exactly on a chunk boundary would land nowhere in particular. The fork relied on fuzz()
            // having already set this process-wide, which made the coverage depend on JUnit method order.
            DatabaseDescriptor.setFlushCompression(FlushCompression.table);

            long seed = REPLAY_SEED != null ? REPLAY_SEED : BASE_SEED;
            int overhead = calibrateOverhead(new Random(seed));
            boolean anyConverged = false;

            for (int chunkKb : new int[]{ 4, 16 })
            {
                for (int delta : new int[]{ -1, 0, 1 })
                {
                    long scenarioSeed = scramble(seed + chunkKb * 1000L + delta);
                    try
                    {
                        anyConverged |= runStraddleScenario(chunkKb, delta, overhead, scenarioSeed);
                    }
                    catch (Throwable t)
                    {
                        // The BASE seed in the hint, not scenarioSeed: this method takes REPLAY_SEED as its base and
                        // re-derives every scenario seed from it (and calibrateOverhead is seeded from it too), so a
                        // hint naming the derived seed would replay a DIFFERENT scenario -- the one whose base
                        // happened to be this one's derivative. scenarioSeed is still reported, since it is what the
                        // failing scenario's own data generator used.
                        throw new AssertionError(String.format("straddle scenario FAILED: chunkKb=%d delta=%d " +
                                                               "overhead=%d baseSeed=%d scenarioSeed=%d%n%s",
                                                               chunkKb, delta, overhead, seed, scenarioSeed,
                                                               replayHint(seed, "straddlesChunkBoundaries")), t);
                    }
                }
            }

            assertTrue("the adversarial generator never converged on an exact partition size; it is no longer " +
                       "producing chunk-straddling partitions and this test has silently stopped testing anything",
                       anyConverged);
        }
        finally
        {
            DatabaseDescriptor.setColumnIndexSizeInKiB(savedIndexSize);
            DatabaseDescriptor.setColumnIndexCacheSize(savedCacheSize);
            DatabaseDescriptor.setFlushCompression(savedFlushCompression);
        }
    }

    /**
     * The implementation refuses an uncompressed parent rather than emitting a child with a misaligned CRC.db.
     * If that ever changes this test is the reminder to extend the fuzz loop to cover it.
     */
    @Test
    public void uncompressedParentIsRefused() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        createTable("CREATE TABLE %s (pk text PRIMARY KEY, v blob) WITH compression = {'enabled': 'false'}");
        disableCompaction();
        for (int i = 0; i < 8; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?) USING TIMESTAMP ?",
                    String.format("p%05d", i), ByteBuffer.wrap(new byte[512]), PAST_TS + i);
        flush();

        SSTableReader parent = onlySSTable(getCurrentColumnFamilyStore(), "uncompressed refusal");
        assertFalse("an uncompressed sstable must not be reported as supported", parent.compression);
        assertFalse("isSupported() must be false for an uncompressed parent",
                    ZeroCopySSTableSplitter.isSupported(parent));

        try
        {
            ZeroCopySSTableSplitter.split(parent, 2, null);
            fail("expected UnsupportedOperationException for an uncompressed parent");
        }
        catch (UnsupportedOperationException e)
        {
            assertTrue("refusal message must start with the public constant, got: " + e.getMessage(),
                       e.getMessage().startsWith(ZeroCopySSTableSplitter.UNCOMPRESSED_UNSUPPORTED_MESSAGE));
        }
    }

    // ------------------------------------------------------------------------------------------------
    // One fuzz iteration
    // ------------------------------------------------------------------------------------------------

    private void runGuarded(long seed, boolean widestSchema) throws Throwable
    {
        Config cfg = new Config(seed);
        cfg.widestSchema = widestSchema;
        try
        {
            runIteration(cfg);
        }
        catch (Throwable t)
        {
            throw new AssertionError("ZeroCopySSTableSplitter fuzz iteration FAILED\n" + cfg + '\n'
                                     + replayHint(seed, "fuzz"), t);
        }
    }

    private static String replayHint(long seed, String method)
    {
        // -D on the ant command line does not reach the forked test JVM; it has to go through test.jvmargs.
        return "replay this case alone with:\n"
               + "  ant testsome"
               + " -Dtest.name=org.apache.cassandra.io.sstable.ZeroCopySSTableSplitterFuzzTest"
               + " -Dtest.methods=" + method
               + " -Dtest.jvmargs=\"-D" + PROP_REPLAY_SEED + '=' + seed + '"';
    }

    private void runIteration(Config cfg) throws Throwable
    {
        Random rnd = new Random(cfg.seed);

        cfg.compressor = COMPRESSORS[rnd.nextInt(COMPRESSORS.length)];
        cfg.chunkKb = CHUNK_KB[rnd.nextInt(CHUNK_KB.length)];
        cfg.minCompressRatio = MIN_COMPRESS_RATIO[rnd.nextInt(MIN_COMPRESS_RATIO.length)];
        cfg.columnIndexKb = COLUMN_INDEX_KB[rnd.nextInt(COLUMN_INDEX_KB.length)];
        cfg.columnIndexCacheKb = COLUMN_INDEX_CACHE_KB[rnd.nextInt(COLUMN_INDEX_CACHE_KB.length)];
        cfg.clusterings = rnd.nextInt(3);                          // 0, 1 or 2 clustering columns
        cfg.reverse0 = cfg.clusterings >= 1 && rnd.nextBoolean();
        cfg.reverse1 = cfg.clusterings >= 2 && rnd.nextBoolean();
        cfg.hasStatic = cfg.clusterings >= 1 && rnd.nextBoolean();
        cfg.hasMap = rnd.nextBoolean();
        cfg.hasSet = rnd.nextBoolean();

        if (cfg.widestSchema)
        {
            // Pinned, not rolled: see fuzz(). Every roll above still happened, so the rest of this iteration --
            // compressor, chunk length, split mode, sizes, tombstones -- is the same stream it would have been.
            if (cfg.compressor == null)
                cfg.compressor = COMPRESSORS[0];
            cfg.clusterings = 2;
            cfg.hasStatic = true;
            cfg.hasMap = true;
            cfg.hasSet = true;
        }

        exercised.add("clusterings=" + cfg.clusterings);
        if (cfg.compressor == null)
            exercised.add("uncompressed");
        if (cfg.hasStatic)
            exercised.add("static");
        if (cfg.hasMap)
            exercised.add("map");
        if (cfg.hasSet)
            exercised.add("set");
        if (cfg.reverse0 || cfg.reverse1)
            exercised.add("reversed");

        DatabaseDescriptor.setColumnIndexSizeInKiB(cfg.columnIndexKb);
        DatabaseDescriptor.setColumnIndexCacheSize(cfg.columnIndexCacheKb);

        int chunkLength = cfg.chunkKb * 1024;
        // big chunks + big partitions would blow the byte budget; scale the partition count to compensate
        cfg.partitions = cfg.chunkKb >= 32 ? 6 + rnd.nextInt(10) : 8 + rnd.nextInt(30);

        // flush_compression defaults to `fast`, which silently replaces any compressor that does not
        // advertise FAST_COMPRESSION with CompressionParams.DEFAULT -- LZ4 at 16 KiB (BigTableWriter.java:127-151).
        // Without this the whole compressor/chunk-length matrix below would be a no-op for every
        // non-LZ4 iteration and the fuzz would only ever exercise one configuration.
        DatabaseDescriptor.setFlushCompression(FlushCompression.table);

        createTable(ddl(cfg));
        disableCompaction();
        writeRandomData(cfg, rnd, chunkLength);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs, cfg.toString());

        if (cfg.compressor == null)
        {
            // the uncompressed variant of the fuzz: assert the refusal, exactly and every time
            assertFalse("uncompressed parent reported as supported", ZeroCopySSTableSplitter.isSupported(parent));
            try
            {
                ZeroCopySSTableSplitter.split(parent, 2, null);
                fail("expected UnsupportedOperationException for an uncompressed parent");
            }
            catch (UnsupportedOperationException e)
            {
                assertTrue("bad refusal message: " + e.getMessage(),
                           e.getMessage().startsWith(ZeroCopySSTableSplitter.UNCOMPRESSED_UNSUPPORTED_MESSAGE));
            }
            return;
        }

        assertTrue("compressed parent reported as unsupported", ZeroCopySSTableSplitter.isSupported(parent));
        ParentIndex index = readIndex(parent);
        assertEquals("the generator did not write one partition per pk", cfg.partitions, index.size());
        cfg.parentPartitions = index.size();
        cfg.parentUncompressedLength = parent.uncompressedLength();
        cfg.parentChunkLength = parent.getCompressionMetadata().chunkLength();
        assertEquals("the table's chunk_length_in_kb did not survive to the sstable",
                     chunkLength, cfg.parentChunkLength);

        // ---- split-point selection ------------------------------------------------------------------
        boolean byKeys = rnd.nextBoolean();
        cfg.splitMode = byKeys ? "boundaries" : "numChildren";
        cfg.useTxn = rnd.nextBoolean();
        // Half the iterations use the ALIGNED layout, in which every child's Data.db starts with up to 64 KiB
        // of the parent's previous chunk so that its extents could be shared with the parent by FICLONERANGE.
        // Forced rather than left to the filesystem: no CI box can share extents, and the layout is the part
        // that has to survive every compressor, chunk length and raw-chunk threshold in this matrix. A padded
        // range that is copied instead of shared produces a byte-identical child, so this covers the layout
        // fully and the ioctl not at all.
        //
        // NOTE: test/conf/cassandra.yaml sets disk_access_mode: mmap_index_only, so a child's Data.db is read
        // through a standard (non-mmap) segmented file here and the padded-CompressionInfo path in
        // MmappedRegions.updateState(CompressionMetadata) is NOT reached from this test. That path is covered
        // directly by MmappedRegionsTest#testMapForCompressionMetadataWithFrontPad and by
        // ZeroCopySSTableSplitterTest#alignedChildrenAreReadableEverywhere.
        cfg.alignedLayout = rnd.nextBoolean();
        ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = cfg.alignedLayout;
        // ...and a quarter of them skip Digest.crc32 entirely, which is a supported configuration and therefore
        // has to hold for every compressor, chunk length and raw-chunk threshold in the matrix, not just for the
        // one case a dedicated test would pick.
        cfg.writeDigest = rnd.nextInt(4) != 0;
        DatabaseDescriptor.setZeroCopySplitDigestEnabled(cfg.writeDigest);
        exercised.add(cfg.splitMode);
        exercised.add(cfg.alignedLayout ? "alignedLayout" : "plainLayout");
        exercised.add(cfg.writeDigest ? "digest" : "noDigest");
        if (cfg.useTxn)
            exercised.add("txn");

        LifecycleTransaction txn = cfg.useTxn ? LifecycleTransaction.offline(OperationType.UNKNOWN) : null;
        ZeroCopySSTableSplitter.Result result = null;
        try
        {
            int[] expectedRunStarts;
            if (byKeys)
            {
                cfg.boundaryIndices = pickBoundaryIndices(rnd, index, chunkLength);
                List<DecoratedKey> boundaries = new ArrayList<>(cfg.boundaryIndices.length);
                for (int idx : cfg.boundaryIndices)
                    boundaries.add(index.keys[idx]);
                // boundary keys are existing keys, so run b+1 starts exactly at that key's record index
                expectedRunStarts = cfg.boundaryIndices;
                result = ZeroCopySSTableSplitter.split(parent, boundaries, txn);
            }
            else
            {
                cfg.numChildren = 1 + rnd.nextInt(Math.min(6, index.size()));
                expectedRunStarts = null;
                result = ZeroCopySSTableSplitter.split(parent, cfg.numChildren, txn);
            }

            cfg.actualChildren = result.children.size();
            verify(parent, index, result, cfg, expectedRunStarts);
        }
        finally
        {
            ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = false;
            // The value this JVM had before the test, NOT a literal true: `true` happens to be the Config default,
            // so restoring it would quietly overwrite a node-level setting (or another test's save/restore) with
            // something that merely looks like the default, and the next test in this JVM would run under a
            // configuration nobody chose.
            DatabaseDescriptor.setZeroCopySplitDigestEnabled(savedDigestEnabled);
            releaseChildren(result);
            if (txn != null)
            {
                // closing an unfinished offline transaction aborts it, which deletes everything trackNew'd on
                // it: that doubles as this iteration's cleanup and proves trackNew really registered the
                // children with the LogTransaction.
                try
                {
                    txn.close();
                }
                catch (Throwable t)
                {
                    logger.warn("failed aborting the split transaction", t);
                }
                LifecycleTransaction.waitForDeletions();
            }
            deleteChildFiles(result);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Verification: the oracle plus independent structural cross-checks
    // ------------------------------------------------------------------------------------------------

    private void verify(SSTableReader parent,
                        ParentIndex index,
                        ZeroCopySSTableSplitter.Result result,
                        Config cfg,
                        int[] expectedRunStarts) throws Exception
    {
        String ctx = cfg.toString();
        List<ZeroCopySSTableSplitter.Child> children = result.children;
        assertFalse(ctx + " -- split produced no children", children.isEmpty());

        // The runs the split is REQUIRED to produce, derived here from the boundary indices alone. Empty runs
        // (a boundary at record 0, or two boundaries resolving to the same record) must yield no child.
        List<int[]> expectedRuns = null;
        if (expectedRunStarts == null)
        {
            assertEquals(ctx + " -- chooseByByteShare must always produce exactly numChildren non-empty runs",
                         cfg.numChildren, children.size());
        }
        else
        {
            expectedRuns = new ArrayList<>();
            int previous = 0;
            for (int start : expectedRunStarts)
            {
                if (start > previous)
                    expectedRuns.add(new int[]{ previous, start });
                previous = start;
            }
            if (index.size() > previous)
                expectedRuns.add(new int[]{ previous, index.size() });
            assertEquals(ctx + " -- wrong number of children for boundaries "
                         + Arrays.toString(expectedRunStarts), expectedRuns.size(), children.size());
        }

        CompressionMetadata parentMeta = parent.getCompressionMetadata();
        int chunkLength = parentMeta.chunkLength();
        int parentChunkCount = (int) ((parentMeta.dataLength + chunkLength - 1) / chunkLength);

        long physicalSum = 0;
        long deadSum = 0;
        long partitionSum = 0;
        int cursor = 0;

        for (int b = 0; b < children.size(); b++)
        {
            ZeroCopySSTableSplitter.Child child = children.get(b);
            String cctx = ctx + " -- child " + b + '/' + children.size() + ' ' + child;

            // an empty child is not representable: IndexSummaryBuilder.build and getPositionsForRanges both assert
            assertTrue(cctx + " -- empty child", child.partitionCount > 0);
            assertTrue(cctx + " -- claims more partitions than remain in the parent",
                       cursor + child.partitionCount <= index.size());
            int from = cursor;
            int to = (int) (cursor + child.partitionCount);
            cursor = to;

            if (expectedRuns != null)
            {
                assertEquals(cctx + " -- child does not start at the requested boundary "
                             + Arrays.toString(expectedRunStarts), expectedRuns.get(b)[0], from);
                assertEquals(cctx + " -- child does not end at the requested boundary "
                             + Arrays.toString(expectedRunStarts), expectedRuns.get(b)[1], to);
            }

            assertEquals(cctx + " -- wrong first key", index.keys[from], child.first);
            assertEquals(cctx + " -- wrong last key", index.keys[to - 1], child.last);

            // ---- chunk arithmetic, recomputed from the parent, not read back from the child ----
            long lo = index.positions[from];
            long hi = to < index.size() ? index.positions[to] : parentMeta.dataLength;
            ZeroCopySSTableSplitter.ChunkRange expected = ZeroCopySSTableSplitter.chunkRange(lo, hi, chunkLength);
            assertEquals(cctx + " -- firstChunk for [" + lo + ',' + hi + ')', expected.firstChunk, child.firstChunk);
            assertEquals(cctx + " -- lastChunk for [" + lo + ',' + hi + ')', expected.lastChunk, child.lastChunk);
            assertEquals(cctx + " -- dataLength", expected.dataLength, child.dataLength);
            assertEquals(cctx + " -- shift", expected.shift, child.shift);
            assertEquals(cctx + " -- deadPrefixBytes", expected.deadPrefixBytes, child.deadPrefixBytes);
            assertEquals(cctx + " -- deadPrefixBytes must equal lo mod L", lo % chunkLength, child.deadPrefixBytes);
            assertTrue(cctx + " -- (C-1)*L < Dp invariant broken",
                       (expected.chunkCount - 1) * (long) chunkLength < child.dataLength);
            assertTrue(cctx + " -- Dp <= C*L invariant broken",
                       child.dataLength <= expected.chunkCount * (long) chunkLength);

            long copyFrom = chunkOffset(parentMeta, expected.firstChunk, parentChunkCount, chunkLength);
            long copyTo = chunkOffset(parentMeta, expected.lastChunk + 1, parentChunkCount, chunkLength);
            assertEquals(cctx + " -- physicalBytes must be exactly O(j+1) - O(i)",
                         copyTo - copyFrom, child.physicalBytes);

            // ---- the child's files on disk ----
            // A child aligned for extent sharing carries a head pad of O(i) mod 64 KiB, and its physical
            // lengths are all measured from there rather than from 0. Zero on a filesystem that cannot share.
            long pad = child.headPadBytes;
            assertTrue(cctx + " -- head pad must be under one alignment unit", pad >= 0 && pad < 64 * 1024);
            assertTrue(cctx + " -- head pad must be O(i) mod alignment, or nothing",
                       pad == 0 || pad == copyFrom % (64 * 1024));
            // If the layout was forced aligned, the pad is not optional: it is exactly O(i) mod A. Without this
            // the forcing could quietly stop working and half the fuzz matrix would test the plain layout twice.
            if (cfg.alignedLayout)
                assertEquals(cctx + " -- forced aligned layout did not pad", copyFrom % (64 * 1024), pad);
            assertEquals(cctx + " -- onDiskLength", pad + child.physicalBytes, child.onDiskLength());
            long onDisk = child.descriptor.fileFor(Components.DATA).length();
            assertEquals(cctx + " -- child Data.db has trailing slack (or is short)", child.onDiskLength(), onDisk);
            assertEquals(cctx + " -- child uncompressedLength", child.dataLength, child.reader.uncompressedLength());

            // CompressionInfoComponent.load reads the child's CompressionInfo.db against the on-disk Data.db
            // length, i.e. exactly the (descriptor, onDisk) pair the 4.1 constructor took.
            CompressionMetadata childMeta = CompressionInfoComponent.load(child.descriptor, null);
            try
            {
                assertEquals(cctx + " -- child CompressionInfo dataLength", child.dataLength, childMeta.dataLength);
                assertEquals(cctx + " -- child chunkLength", chunkLength, childMeta.chunkLength());
                assertEquals(cctx + " -- child maxCompressedLength", parentMeta.maxCompressedLength(),
                             childMeta.maxCompressedLength());
                assertEquals(cctx + " -- child offsets[0] must be the head pad", pad, childMeta.chunkFor(0).offset);
                // the last chunk must end exactly at the physical end of the file
                long lastChunkStart = (long) ((childMeta.dataLength - 1) / chunkLength) * chunkLength;
                CompressionMetadata.Chunk lastChunk = childMeta.chunkFor(lastChunkStart);
                assertEquals(cctx + " -- child last chunk does not end at EOF",
                             onDisk, lastChunk.offset + lastChunk.length + 4);
            }
            finally
            {
                childMeta.close();
            }

            // Digest.crc32 is optional (zero_copy_split_digest_enabled), and the component set is the authority:
            // if it claims the digest the value must be right, and if it does not the file must not exist.
            assertEquals(cctx + " -- the digest component must follow the config",
                         cfg.writeDigest, child.components.contains(Components.DIGEST));
            if (cfg.writeDigest)
            {
                assertEquals(cctx + " -- Digest.crc32 does not match the child Data.db",
                             crc32(child.descriptor.fileFor(Components.DATA)),
                             Long.parseLong(readAll(child.descriptor.fileFor(Components.DIGEST)).trim()));
            }
            else
            {
                assertFalse(cctx + " -- Digest.crc32 exists but was not requested",
                            child.descriptor.fileFor(Components.DIGEST).exists());
            }

            // ---- every index position was rebased by exactly `shift` ----
            // trunk's getPosition returns the position directly, with a negative value standing in for the 4.1
            // null RowIndexEntry; the `position >= 0` check below is the old assertNotNull in the new shape.
            for (int r = from; r < to; r++)
            {
                long position = child.reader.getPosition(index.keys[r], SSTableReader.Operator.EQ, false);
                assertTrue(cctx + " -- child cannot find key " + index.keys[r], position >= 0);
                assertEquals(cctx + " -- rebased position for record " + r,
                             index.positions[r] - child.shift, position);
            }
            assertEquals(cctx + " -- first partition must land at the dead prefix",
                         child.deadPrefixBytes,
                         child.reader.getPosition(child.first, SSTableReader.Operator.EQ, false));
            assertTrue(cctx + " -- the dead prefix must be smaller than one chunk",
                       child.deadPrefixBytes < chunkLength);

            // ---- children must be disjoint and in token order ----
            if (b > 0)
                assertTrue(cctx + " -- children overlap or are out of order",
                           children.get(b - 1).last.compareTo(child.first) < 0);
            assertTrue(cctx + " -- first > last", child.first.compareTo(child.last) <= 0);

            physicalSum += child.physicalBytes;
            deadSum += child.deadPrefixBytes;
            partitionSum += child.partitionCount;
        }

        assertEquals(ctx + " -- children do not cover every parent partition", index.size(), partitionSum);
        assertEquals(ctx + " -- totalPhysicalBytesCopied", physicalSum, result.totalPhysicalBytesCopied);
        assertEquals(ctx + " -- totalDeadPrefixBytes", deadSum, result.totalDeadPrefixBytes);

        // ---- THE ORACLE ----
        assertConcatenatedChildrenEqualParent(parent, children, ctx);
    }

    /** concatenated children == parent, exactly. */
    private static void assertConcatenatedChildrenEqualParent(SSTableReader parent,
                                                              List<ZeroCopySSTableSplitter.Child> children,
                                                              String ctx)
    {
        try (ISSTableScanner parentScanner = parent.getScanner())
        {
            long seen = 0;
            for (int b = 0; b < children.size(); b++)
            {
                ZeroCopySSTableSplitter.Child child = children.get(b);
                long inChild = 0;
                try (ISSTableScanner childScanner = child.reader.getScanner())
                {
                    while (childScanner.hasNext())
                    {
                        assertTrue(ctx + " -- child " + b + " has partitions the parent does not, after " + seen,
                                   parentScanner.hasNext());
                        try (UnfilteredRowIterator expected = parentScanner.next();
                             UnfilteredRowIterator actual = childScanner.next())
                        {
                            assertPartitionEquals(expected, actual, ctx + " -- child " + b + " partition " + seen);
                        }
                        inChild++;
                        seen++;
                    }
                }
                assertEquals(ctx + " -- child " + b + " scanned a different number of partitions than it reported",
                             child.partitionCount, inChild);
            }
            assertFalse(ctx + " -- the children are missing trailing parent partitions (saw " + seen + ')',
                        parentScanner.hasNext());
        }
    }

    private static void assertPartitionEquals(UnfilteredRowIterator expected, UnfilteredRowIterator actual, String ctx)
    {
        assertEquals(ctx + " -- partition key", expected.partitionKey(), actual.partitionKey());
        String key = " (" + expected.partitionKey() + ')';
        assertEquals(ctx + " -- partition level deletion" + key,
                     expected.partitionLevelDeletion(), actual.partitionLevelDeletion());
        assertEquals(ctx + " -- static row" + key, expected.staticRow(), actual.staticRow());
        assertEquals(ctx + " -- columns" + key, expected.columns(), actual.columns());
        assertEquals(ctx + " -- reverse order" + key, expected.isReverseOrder(), actual.isReverseOrder());

        int u = 0;
        while (expected.hasNext())
        {
            assertTrue(ctx + " -- child partition truncated at unfiltered " + u + key, actual.hasNext());
            assertEquals(ctx + " -- unfiltered " + u + key, expected.next(), actual.next());
            u++;
        }
        assertFalse(ctx + " -- child partition has extra unfiltereds past " + u + key, actual.hasNext());
    }

    // ------------------------------------------------------------------------------------------------
    // Adversarial generator: partitions calibrated to straddle chunk boundaries as tightly as possible
    // ------------------------------------------------------------------------------------------------

    /** @return the constant per-partition serialized overhead of the straddle schema, i.e. size - blobLength. */
    private int calibrateOverhead(Random rnd) throws Throwable
    {
        int probeBlob = 4096;
        int size = straddleTableAndMeasure(probeBlob, 3, "{'class': 'LZ4Compressor', 'chunk_length_in_kb': 16}", rnd);
        return size - probeBlob;
    }

    /**
     * @return true if the generator converged on partitions of exactly {@code L + delta} bytes
     */
    private boolean runStraddleScenario(int chunkKb, int delta, int overhead, long seed) throws Throwable
    {
        Random rnd = new Random(seed);
        int chunkLength = chunkKb * 1024;
        long target = chunkLength + delta;
        String compressor = COMPRESSORS[rnd.nextInt(COMPRESSORS.length - 1)];  // never the uncompressed slot
        String compression = String.format("{'class': '%s', 'chunk_length_in_kb': %d}", compressor, chunkKb);

        int partitions = 12;
        int blobLength = (int) target - overhead;
        if (blobLength <= 0)
        {
            logger.warn("straddle target {} is smaller than the per-partition overhead {}", target, overhead);
            return false;
        }

        boolean converged = false;
        for (int attempt = 0; attempt < 3 && blobLength > 0; attempt++)
        {
            int measured = straddleTableAndMeasure(blobLength, partitions, compression, rnd);
            if (measured == target)
            {
                converged = true;
                break;
            }
            blobLength += (int) target - measured;
        }

        if (!converged)
        {
            logger.warn("straddle generator did not converge for chunkKb={} delta={} (overhead={}); the split " +
                        "oracle still runs, but partitions are not exactly chunk-aligned", chunkKb, delta, overhead);
            if (blobLength <= 0)
                return false;
        }

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String ctx = String.format("straddle[chunkKb=%d delta=%d target=%d blob=%d compressor=%s converged=%s seed=%d]",
                                   chunkKb, delta, target, blobLength, compressor, converged, seed);
        SSTableReader parent = onlySSTable(cfs, ctx);
        ParentIndex index = readIndex(parent);
        assertEquals(ctx + " -- wrong partition count", partitions, index.size());

        if (converged)
        {
            for (int i = 0; i < index.size(); i++)
                assertEquals(ctx + " -- partition " + i + " is not at an exact multiple of the target size",
                             i * target, index.positions[i]);
            assertEquals(ctx + " -- parent uncompressedLength", partitions * target, parent.uncompressedLength());
        }

        Config cfg = new Config(seed);
        cfg.compressor = compressor;
        cfg.chunkKb = chunkKb;
        cfg.adversarialNote = ctx;
        cfg.parentPartitions = index.size();
        cfg.parentUncompressedLength = parent.uncompressedLength();
        cfg.parentChunkLength = chunkLength;
        // Alternate the aligned (extent-shareable) layout with the plain one across the six scenarios, so the
        // partitions engineered to land exactly on chunk boundaries are exercised against both.
        cfg.alignedLayout = delta >= 0;
        ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = cfg.alignedLayout;

        try
        {
            // (a) one child per partition: every interior boundary is a chunk boundary +/- delta
            int[] all = new int[partitions - 1];
            for (int i = 0; i < all.length; i++)
                all[i] = i + 1;
            runSplitByBoundaries(parent, index, cfg, all);

            // (b) a plain byte-share split over the same adversarial layout
            cfg.splitMode = "numChildren";
            cfg.numChildren = 3;
            cfg.boundaryIndices = null;
            ZeroCopySSTableSplitter.Result byCount = null;
            try
            {
                byCount = ZeroCopySSTableSplitter.split(parent, 3, null);
                cfg.actualChildren = byCount.children.size();
                verify(parent, index, byCount, cfg, null);
            }
            finally
            {
                releaseChildren(byCount);
                deleteChildFiles(byCount);
            }

            // (c) a random subset of the same boundaries
            int[] subset = pickBoundaryIndices(rnd, index, chunkLength);
            runSplitByBoundaries(parent, index, cfg, subset);
        }
        finally
        {
            ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = false;
        }

        return converged;
    }

    private void runSplitByBoundaries(SSTableReader parent, ParentIndex index, Config cfg, int[] indices)
    throws Exception
    {
        cfg.splitMode = "boundaries";
        cfg.numChildren = -1;
        cfg.boundaryIndices = indices;
        List<DecoratedKey> boundaries = new ArrayList<>(indices.length);
        for (int idx : indices)
            boundaries.add(index.keys[idx]);

        ZeroCopySSTableSplitter.Result result = null;
        try
        {
            result = ZeroCopySSTableSplitter.split(parent, boundaries, null);
            cfg.actualChildren = result.children.size();
            verify(parent, index, result, cfg, indices);
        }
        finally
        {
            releaseChildren(result);
            deleteChildFiles(result);
        }
    }

    /**
     * Creates a fresh single-row-per-partition table, writes {@code partitions} identically sized partitions and
     * flushes.
     *
     * @return the exact uncompressed size of one partition
     */
    private int straddleTableAndMeasure(int blobLength, int partitions, String compression, Random rnd)
    throws Throwable
    {
        createTable("CREATE TABLE %s (pk text PRIMARY KEY, v blob) WITH compression = " + compression);
        disableCompaction();
        for (int i = 0; i < partitions; i++)
        {
            byte[] bytes = new byte[blobLength];
            rnd.nextBytes(bytes);   // incompressible, so chunks are stored raw when min_compress_ratio bites
            execute("INSERT INTO %s (pk, v) VALUES (?, ?) USING TIMESTAMP ?",
                    String.format("p%05d", i), ByteBuffer.wrap(bytes), PAST_TS + i);
        }
        flush();

        // The probe needs one sstable so that consecutive index positions give one partition's exact size.
        // A memtable can flush on its own part way through the loop above (heap pressure from earlier test
        // methods in this JVM is enough to trigger it), which leaves two sstables and used to make this an
        // order-dependent flake. Consolidate instead of asserting and hoping.
        ColumnFamilyStore probeCfs = getCurrentColumnFamilyStore();
        if (probeCfs.getLiveSSTables().size() > 1)
        {
            compact();
            assertEquals("straddle probe could not consolidate to a single sstable",
                         1, probeCfs.getLiveSSTables().size());
        }

        SSTableReader sstable = onlySSTable(probeCfs, "straddle probe");
        ParentIndex index = readIndex(sstable);
        assertEquals("straddle probe wrote the wrong number of partitions", partitions, index.size());

        long size = index.size() > 1 ? index.positions[1] - index.positions[0]
                                     : sstable.uncompressedLength() - index.positions[0];
        for (int i = 1; i < index.size(); i++)
        {
            long end = i + 1 < index.size() ? index.positions[i + 1] : sstable.uncompressedLength();
            assertEquals("straddle probe partitions are not all the same size; the calibration assumption is broken",
                         size, end - index.positions[i]);
        }
        return Math.toIntExact(size);
    }

    // ------------------------------------------------------------------------------------------------
    // Schema and data generation
    // ------------------------------------------------------------------------------------------------

    private static String ddl(Config cfg)
    {
        StringBuilder sb = new StringBuilder("CREATE TABLE %s (pk text");
        if (cfg.clusterings >= 1)
            sb.append(", ck0 int");
        if (cfg.clusterings >= 2)
            sb.append(", ck1 text");
        sb.append(", v blob, t text, n int");
        if (cfg.hasStatic)
            sb.append(", s text static");
        if (cfg.hasMap)
            sb.append(", m map<int, text>");
        if (cfg.hasSet)
            sb.append(", st set<text>");
        sb.append(", PRIMARY KEY (pk");
        if (cfg.clusterings >= 1)
            sb.append(", ck0");
        if (cfg.clusterings >= 2)
            sb.append(", ck1");
        sb.append("))");

        sb.append(" WITH compression = ");
        if (cfg.compressor == null)
        {
            sb.append("{'enabled': 'false'}");
        }
        else
        {
            sb.append("{'class': '").append(cfg.compressor)
              .append("', 'chunk_length_in_kb': ").append(cfg.chunkKb);
            if (cfg.minCompressRatio > 0)
                sb.append(", 'min_compress_ratio': ").append(cfg.minCompressRatio);
            sb.append('}');
        }

        if (cfg.clusterings >= 1)
        {
            sb.append(" AND CLUSTERING ORDER BY (ck0 ").append(cfg.reverse0 ? "DESC" : "ASC");
            if (cfg.clusterings >= 2)
                sb.append(", ck1 ").append(cfg.reverse1 ? "DESC" : "ASC");
            sb.append(')');
        }
        return sb.toString();
    }

    private void writeRandomData(Config cfg, Random rnd, int chunkLength) throws Throwable
    {
        long budget = MAX_TABLE_BYTES;
        long pastTs = PAST_TS;   // monotonic, and always older than the wall-clock timestamp a DELETE gets
        cfg.rowsPerPartition = new int[cfg.partitions];
        cfg.valueBytes = new int[cfg.partitions];

        for (int p = 0; p < cfg.partitions; p++)
        {
            String pk = String.format("p%05d", p);

            // A partition-level tombstone that data written afterwards (at FUTURE_TS) survives.
            boolean partitionTombstone = rnd.nextInt(10) == 0;
            if (partitionTombstone)
                execute("DELETE FROM %s WHERE pk = ?", pk);
            long rowTs = partitionTombstone ? FUTURE_TS + p * 1000L : pastTs;

            // value size class: tiny / small / much bigger than one chunk
            int roll = rnd.nextInt(100);
            int valueSize;
            int rows;
            if (roll < 45)
            {
                valueSize = rnd.nextInt(200);
                rows = cfg.clusterings == 0 ? 1 : 1 + rnd.nextInt(6);
            }
            else if (roll < 80)
            {
                valueSize = 200 + rnd.nextInt(2000);
                rows = cfg.clusterings == 0 ? 1 : 1 + rnd.nextInt(4);
            }
            else
            {
                // deliberately much larger than chunkLength so a single partition spans many chunks
                valueSize = chunkLength + rnd.nextInt(2 * chunkLength);
                rows = cfg.clusterings == 0 ? 1 : 1 + rnd.nextInt(2);
            }
            if ((long) valueSize * rows > budget)
            {
                valueSize = Math.min(valueSize, 256);
                rows = Math.min(rows, 2);
            }
            budget -= (long) valueSize * rows;
            cfg.rowsPerPartition[p] = rows;
            cfg.valueBytes[p] = valueSize;

            boolean compressible = rnd.nextBoolean();
            for (int r = 0; r < rows; r++)
                insertRow(cfg, rnd, pk, r, valueSize, compressible, rowTs + r);

            if (cfg.hasStatic && rnd.nextBoolean())
                execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP ?",
                        pk, text(rnd, 1 + rnd.nextInt(40)), rowTs + rows);

            // only the PAST counter advances; the FUTURE timestamps of a resurrected partition must not leak
            // into the next partition or the deletions below would stop biting.
            pastTs += rows + 2;

            // deletions run at wall-clock timestamps: they always shadow PAST_TS data and never FUTURE_TS data,
            // but either way the tombstones themselves land in the copied blobs.
            if (cfg.clusterings >= 1 && rows > 1)
            {
                if (rnd.nextInt(4) == 0)   // row tombstone
                {
                    if (cfg.clusterings == 1)
                        execute("DELETE FROM %s WHERE pk = ? AND ck0 = ?", pk, rnd.nextInt(rows));
                    else
                        execute("DELETE FROM %s WHERE pk = ? AND ck0 = ? AND ck1 = ?",
                                pk, rnd.nextInt(rows), "c" + rnd.nextInt(rows));
                }
                if (rnd.nextInt(4) == 0)   // range tombstone
                {
                    int a = rnd.nextInt(rows);
                    int b = a + 1 + rnd.nextInt(Math.max(1, rows - a));
                    execute("DELETE FROM %s WHERE pk = ? AND ck0 >= ? AND ck0 < ?", pk, a, b);
                }
                if (rnd.nextInt(4) == 0)   // single cell tombstone
                {
                    if (cfg.clusterings == 1)
                        execute("DELETE t FROM %s WHERE pk = ? AND ck0 = ?", pk, rnd.nextInt(rows));
                    else
                        execute("DELETE t FROM %s WHERE pk = ? AND ck0 = ? AND ck1 = ?",
                                pk, rnd.nextInt(rows), "c" + rnd.nextInt(rows));
                }
            }
            else if (cfg.clusterings == 0 && rnd.nextInt(6) == 0)
            {
                execute("DELETE t FROM %s WHERE pk = ?", pk);
            }
        }
    }

    private void insertRow(Config cfg, Random rnd, String pk, int row, int valueSize, boolean compressible, long ts)
    throws Throwable
    {
        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        columns.add("pk");
        values.add(pk);
        if (cfg.clusterings >= 1)
        {
            columns.add("ck0");
            values.add(row);
        }
        if (cfg.clusterings >= 2)
        {
            columns.add("ck1");
            values.add("c" + row);
        }

        columns.add("v");
        values.add(blob(rnd, valueSize, compressible));

        int textRoll = rnd.nextInt(6);
        if (textRoll != 0)
        {
            columns.add("t");
            // empty and null values both appear
            values.add(textRoll == 1 ? "" : textRoll == 2 ? null : text(rnd, 1 + rnd.nextInt(64)));
        }
        if (rnd.nextInt(3) != 0)
        {
            columns.add("n");
            values.add(rnd.nextInt(4) == 0 ? null : rnd.nextInt());
        }
        if (cfg.hasMap && rnd.nextInt(3) == 0)
        {
            columns.add("m");
            if (rnd.nextInt(5) == 0)
            {
                values.add(null);                       // collection tombstone
            }
            else
            {
                // sorted, so the serialized collection matches the element type's comparator
                Map<Integer, String> map = new TreeMap<>();
                for (int i = 0, n = rnd.nextInt(4); i < n; i++)
                    map.put(rnd.nextInt(100), text(rnd, 1 + rnd.nextInt(16)));
                values.add(map);
            }
        }
        if (cfg.hasSet && rnd.nextInt(3) == 0)
        {
            columns.add("st");
            Set<String> set = new TreeSet<>();
            for (int i = 0, n = rnd.nextInt(4); i < n; i++)
                set.add(text(rnd, 1 + rnd.nextInt(16)));
            values.add(set);
        }
        if (cfg.hasStatic && rnd.nextInt(5) == 0)
        {
            columns.add("s");
            values.add(text(rnd, 1 + rnd.nextInt(32)));
        }

        StringBuilder query = new StringBuilder("INSERT INTO %s (");
        for (int i = 0; i < columns.size(); i++)
            query.append(i == 0 ? "" : ", ").append(columns.get(i));
        query.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++)
            query.append(i == 0 ? "?" : ", ?");
        query.append(") USING TIMESTAMP ?");
        values.add(ts);

        // a long TTL: long enough that nothing can expire mid-test, but it still writes real expiry info
        boolean ttl = rnd.nextInt(4) == 0;
        if (ttl)
        {
            query.append(" AND TTL ?");
            values.add(100_000 + rnd.nextInt(1_000_000));
        }

        execute(query.toString(), values.toArray());
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
            rnd.nextBytes(bytes);
        }
        return ByteBuffer.wrap(bytes);
    }

    private static String text(Random rnd, int length)
    {
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = (char) ('!' + rnd.nextInt(90));
        return new String(chars);
    }

    // ------------------------------------------------------------------------------------------------
    // Split-point selection for the test side
    // ------------------------------------------------------------------------------------------------

    /**
     * Strictly increasing record indices to use as boundary keys. Biased hard towards partitions that start
     * exactly on a chunk boundary, or one byte either side of one. Index 0 is occasionally included on purpose:
     * it produces an empty leading run, which must yield no child at all.
     */
    private static int[] pickBoundaryIndices(Random rnd, ParentIndex index, int chunkLength)
    {
        int n = index.size();
        if (n < 2)
            return new int[0];   // a single partition cannot be cut; the empty list must give exactly one child
        int wanted = 1 + rnd.nextInt(Math.min(5, n - 1));

        List<Integer> aligned = new ArrayList<>();
        for (int i = 1; i < n; i++)
        {
            long mod = index.positions[i] % chunkLength;
            if (mod == 0 || mod == 1 || mod == chunkLength - 1)
                aligned.add(i);
        }

        TreeSet<Integer> chosen = new TreeSet<>();
        boolean preferAligned = !aligned.isEmpty() && rnd.nextInt(3) != 0;
        while (chosen.size() < wanted)
        {
            if (preferAligned && rnd.nextInt(4) != 0)
                chosen.add(aligned.get(rnd.nextInt(aligned.size())));
            else
                chosen.add(1 + rnd.nextInt(Math.max(1, n - 1)));
            if (chosen.size() >= n)
                break;
        }
        if (rnd.nextInt(6) == 0)
            chosen.add(0);   // empty leading run

        int[] out = new int[chosen.size()];
        int i = 0;
        for (int idx : chosen)
            out[i++] = idx;
        return out;
    }

    // ------------------------------------------------------------------------------------------------
    // Plumbing
    // ------------------------------------------------------------------------------------------------

    /**
     * Also the one place to guard the property no scenario in this file mentions: the splitter REFUSES a version
     * that cannot carry {@code StatsMetadata.hasUnindexedRegions} (BIG {@code pb}+), so under a
     * {@code storage_compatibility_mode} that pinned newly written sstables to {@code nb} or {@code oa} the entire
     * fuzz would pass while asserting nothing but that refusal. Every test target sets
     * {@code storage_compatibility_mode: NONE} today, which is precisely why this must fail loudly rather than be
     * assumed.
     */
    private static SSTableReader onlySSTable(ColumnFamilyStore cfs, String ctx)
    {
        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals(ctx + " -- expected exactly one sstable after the flush, got " + live, 1, live.size());
        SSTableReader sstable = live.iterator().next();
        assertTrue(ctx + " -- the fixture wrote version '" + sstable.descriptor.version.version + "', which the" +
                   " splitter refuses outright; the whole fuzz would silently degenerate into a refusal test",
                   sstable.descriptor.version.hasUnindexedRegionsMarker());
        return sstable;
    }

    /** {@code O(k)}, with {@code O(N)} defined as the physical file length, recomputed from the parent. */
    private static long chunkOffset(CompressionMetadata meta, long k, int chunkCount, int chunkLength)
    {
        if (k == chunkCount)
            return meta.compressedFileLength;
        return meta.chunkFor(k * (long) chunkLength).offset;
    }

    private static final class ParentIndex
    {
        final DecoratedKey[] keys;
        final long[] positions;

        ParentIndex(DecoratedKey[] keys, long[] positions)
        {
            this.keys = keys;
            this.positions = positions;
        }

        int size()
        {
            return keys.length;
        }
    }

    /** Independent walk of the parent Index.db; nothing here goes through the splitter. */
    private static ParentIndex readIndex(SSTableReader sstable) throws IOException
    {
        List<DecoratedKey> keys = new ArrayList<>();
        List<Long> positions = new ArrayList<>();
        try (RandomAccessReader in = RandomAccessReader.open(sstable.descriptor.fileFor(Components.PRIMARY_INDEX)))
        {
            long length = in.length();
            while (in.getFilePointer() != length)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                long position = RowIndexEntry.Serializer.readPosition(in);
                int promotedSize = in.readUnsignedVInt32();
                if (promotedSize > 0)
                    in.skipBytesFully(promotedSize);
                keys.add(sstable.getPartitioner().decorateKey(key));
                positions.add(position);
            }
        }
        long[] pos = new long[positions.size()];
        for (int i = 0; i < pos.length; i++)
            pos[i] = positions.get(i);
        return new ParentIndex(keys.toArray(new DecoratedKey[0]), pos);
    }

    private static long crc32(File file) throws IOException
    {
        CRC32 crc = new CRC32();
        byte[] buffer = new byte[64 * 1024];
        try (InputStream in = file.newInputStream())
        {
            int n;
            while ((n = in.read(buffer)) > 0)
                crc.update(buffer, 0, n);
        }
        return crc.getValue();
    }

    private static String readAll(File file) throws IOException
    {
        return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    }

    private static void releaseChildren(ZeroCopySSTableSplitter.Result result)
    {
        if (result == null)
            return;
        for (ZeroCopySSTableSplitter.Child child : result.children)
        {
            try
            {
                child.reader.selfRef().release();
            }
            catch (Throwable t)
            {
                logger.warn("failed releasing child {}", child.descriptor, t);
            }
        }
    }

    private static void deleteChildFiles(ZeroCopySSTableSplitter.Result result)
    {
        if (result == null)
            return;
        for (ZeroCopySSTableSplitter.Child child : result.children)
        {
            for (Component component : child.components)
            {
                try
                {
                    child.descriptor.fileFor(component).deleteIfExists();
                }
                catch (Throwable t)
                {
                    logger.warn("failed deleting {} of {}", component, child.descriptor, t);
                }
            }
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

        String compressor = "?";
        int chunkKb = -1;
        double minCompressRatio = -1;
        int columnIndexKb = -1;
        int columnIndexCacheKb = -1;
        int clusterings = -1;
        boolean reverse0;
        boolean reverse1;
        boolean hasStatic;
        boolean hasMap;
        boolean hasSet;
        int partitions = -1;
        int[] rowsPerPartition;
        int[] valueBytes;

        int parentPartitions = -1;
        long parentUncompressedLength = -1;
        int parentChunkLength = -1;

        String splitMode = "?";
        int numChildren = -1;
        int[] boundaryIndices;
        int actualChildren = -1;
        boolean useTxn;
        boolean alignedLayout;
        boolean writeDigest = true;
        /** The one iteration whose schema is pinned to the widest this generator can produce; see {@code fuzz()}. */
        boolean widestSchema;
        String adversarialNote;

        Config(long seed)
        {
            this.seed = seed;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("seed=").append(seed)
              .append(" compressor=").append(compressor)
              .append(" chunkKb=").append(chunkKb)
              .append(" minCompressRatio=").append(minCompressRatio)
              .append(" columnIndexKb=").append(columnIndexKb)
              .append(" columnIndexCacheKb=").append(columnIndexCacheKb)
              .append(" clusterings=").append(clusterings)
              .append(" reverse=[").append(reverse0).append(',').append(reverse1).append(']')
              .append(" static=").append(hasStatic)
              .append(" map=").append(hasMap)
              .append(" set=").append(hasSet)
              .append(" partitions=").append(partitions)
              .append(" parentPartitions=").append(parentPartitions)
              .append(" parentUncompressedLength=").append(parentUncompressedLength)
              .append(" parentChunkLength=").append(parentChunkLength)
              .append(" splitMode=").append(splitMode)
              .append(" numChildren=").append(numChildren)
              .append(" boundaryIndices=").append(Arrays.toString(boundaryIndices))
              .append(" actualChildren=").append(actualChildren)
              .append(" useTxn=").append(useTxn)
              .append(" alignedLayout=").append(alignedLayout)
              .append(" writeDigest=").append(writeDigest)
              .append(" widestSchema=").append(widestSchema);
            if (adversarialNote != null)
                sb.append(" adversarial=").append(adversarialNote);
            sb.append("\n  rowsPerPartition=").append(Arrays.toString(rowsPerPartition));
            sb.append("\n  valueBytes=").append(Arrays.toString(valueBytes));
            return sb.toString();
        }
    }
}
