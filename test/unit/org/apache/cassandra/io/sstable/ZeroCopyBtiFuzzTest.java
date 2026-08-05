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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config.FlushCompression;
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
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiZeroCopySplit;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Randomised coverage of the BTI zero-copy split and slice, over the parameters that decide where a copied Rows.db
 * entry can go: the row index granularity, the partition width, the compression cell size, and how many disjoint
 * runs a slice has.
 *
 * <p>The hazard this exists for cannot be caught by construction. A trie node is only ever read out of one
 * rebuffered page, so an entry placed such that one of its nodes straddles a page boundary produces a garbage row
 * position or an exception at LOOKUP time -- long after the write that caused it, with nothing wrong on the way out.
 * {@code BtiZeroCopySplit.RowIndexCopier} has three placements for an entry and the interesting one is the third,
 * which only happens when a single partition's trie is larger than a page. This sweep drives
 * {@code column_index_size} down to 0 (index every row) with wide partitions specifically to reach it, and asserts
 * on the counters afterwards rather than trusting that it did.
 *
 * <p>The SHAPE of each iteration's data is randomised too, and for a different reason: {@link #assertSamePartition}
 * compares {@code partitionLevelDeletion()}, {@code staticRow()} and every {@code Unfiltered} with {@code equals}, and
 * each of those comparisons is vacuous unless the data carries the thing being compared. So a case may have one or
 * two clustering columns, a static column, a collection, TTL'd cells, and partition, row, range and cell tombstones.
 *
 * <p>Every iteration is verified the same way: read the target back through its own rebuilt index and compare every
 * partition and row against the parent's, then run {@code nodetool verify}'s extended pass over it.
 */
public class ZeroCopyBtiFuzzTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(ZeroCopyBtiFuzzTest.class);

    /** Same knobs, same names, as {@code ZeroCopySSTableSliceFuzzTest}. */
    private static final String PROP_ITERATIONS = "cassandra.test.zerocopybti.iterations";
    private static final String PROP_SEED = "cassandra.test.zerocopybti.seed";
    private static final String PROP_REPLAY_SEED = "cassandra.test.zerocopybti.replaySeed";

    // Developer-facing replay knobs for this one class, not runtime configuration, so they are deliberately not in
    // CassandraRelevantProperties -- the same reason ZeroCopySSTableSliceFuzzTest suppresses this rule rather than
    // registering its debug switches globally.
    // checkstyle: suppress below 'blockSystemPropertyUsage'
    private static final int ITERATIONS = Integer.getInteger(PROP_ITERATIONS, 16);
    private static final long BASE_SEED = Long.getLong(PROP_SEED, 20260803_0001L);
    /** When set, exactly one iteration runs, with this literal seed. */
    private static final Long REPLAY_SEED = Long.getLong(PROP_REPLAY_SEED);

    /** Older than the wall-clock timestamp of everything else, so a partition tombstone does not shadow its rows. */
    private static final long OLD_TS = 1_600_000_000_000_000L;
    private static final long NEW_TS = 1_900_000_000_000_000L;

    private SSTableFormat<?, ?> savedFormat;
    private int savedIndexSize;
    private FlushCompression savedFlush;
    private boolean savedDigestEnabled;

    private int splitsVerified;
    private int slicesVerified;
    private int multiRunSlices;
    private int narrowFixtures;
    private int wideFixtures;
    private int digestedSplits;
    private int digestlessSplits;
    /** Slices whose Data.db really did carry partitions their index does not describe, AND which then verified. */
    private int slicesWithInteriorGapsVerified;

    @Before
    public void selectBtiFormat()
    {
        savedFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        savedIndexSize = DatabaseDescriptor.getColumnIndexSizeInKiB();
        savedFlush = DatabaseDescriptor.getFlushCompression();
        savedDigestEnabled = DatabaseDescriptor.getZeroCopySplitDigestEnabled();
        TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(BtiFormat.NAME);
        // Otherwise flush_compression: fast silently swaps the table's compressor and chunk length for LZ4 at
        // 16 KiB, and the sweep over cell sizes would only ever test one of them.
        DatabaseDescriptor.setFlushCompression(FlushCompression.table);
    }

    @After
    public void restoreSettings()
    {
        ZeroCopySSTableSplitter.forceAlignedLayoutForTesting = false;
        ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
        DatabaseDescriptor.setColumnIndexSizeInKiB(savedIndexSize);
        DatabaseDescriptor.setFlushCompression(savedFlush);
        DatabaseDescriptor.setZeroCopySplitDigestEnabled(savedDigestEnabled);
        if (savedFormat != null)
            TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(savedFormat);
    }

    @Test
    public void fuzz() throws Throwable
    {
        long congruenceBefore = BtiZeroCopySplit.RowIndexCopier.congruenceAlignments.sum();

        if (REPLAY_SEED != null)
        {
            logger.info("Replaying a single BTI zero-copy fuzz iteration, seed {}", REPLAY_SEED);
            runGuarded(REPLAY_SEED);
            return;
        }

        logger.info("BTI zero-copy fuzz: {} iterations from base seed {}", ITERATIONS, BASE_SEED);
        for (int i = 0; i < ITERATIONS; i++)
            runGuarded(scramble(BASE_SEED + i));

        long congruence = BtiZeroCopySplit.RowIndexCopier.congruenceAlignments.sum() - congruenceBefore;
        logger.info("BTI zero-copy fuzz covered: {} splits ({} with a digest, {} without), {} slices ({} multi-run, " +
                    "{} with an interior gap), {} narrow and {} wide fixtures; Rows.db placements in place {}, " +
                    "page-aligned {}, congruence-aligned {}",
                    splitsVerified, digestedSplits, digestlessSplits, slicesVerified, multiRunSlices,
                    slicesWithInteriorGapsVerified, narrowFixtures, wideFixtures,
                    BtiZeroCopySplit.RowIndexCopier.inPlacePlacements.sum(),
                    BtiZeroCopySplit.RowIndexCopier.pageAlignments.sum(), congruence);

        // Guard the guard: a generator that drifted into only trivial shapes would pass on nothing.
        assertTrue("verified almost no splits: " + splitsVerified, splitsVerified >= ITERATIONS);
        assertTrue("verified almost no slices: " + slicesVerified, slicesVerified >= ITERATIONS);
        assertTrue("no multi-run slice was verified", multiRunSlices > 0);
        assertTrue("no narrow fixture was generated", narrowFixtures > 0);
        assertTrue("no wide fixture was generated", wideFixtures > 0);
        assertTrue("zero_copy_split_digest_enabled was never true", digestedSplits > 0);
        assertTrue("zero_copy_split_digest_enabled was never false", digestlessSplits > 0);
        // Without this, every verified slice could have been a contiguous one, and the SortedTableVerifier changes on
        // this branch -- which only a slice reaches -- would be exercised over nothing but a dead prefix.
        assertTrue("no verified slice had an interior unindexed region", slicesWithInteriorGapsVerified > 0);
        // The one branch nothing else can observe: a partition whose row index trie spans more than one page, which
        // has to be placed at its original offset within a page.
        //
        // CAVEAT, and it is a large one: this assertion is currently satisfied by the KNOWN DEFECT documented on
        // BtiZeroCopySplit.Cursor.rowIndexBlockStart(). A bounded cursor -- which only the SLICE path builds --
        // reports 0 as its first entry's node region start, so `nodeBytes` becomes the parent's whole Rows.db prefix
        // and align() takes its multi-page branch for a partition whose trie is nowhere near a page wide. The genuine
        // multi-page placement therefore has its own test, through the split path where the block starts are right:
        // BtiZeroCopySplitTest.aGenuinelyMultiPageRowIndexTrieIsPlacedAtItsCongruentOffset. When the defect is fixed
        // this assertion is expected to start failing, and the fix -- not the assertion -- is what should decide
        // whether it stays.
        assertTrue("no entry needed congruence alignment, so the multi-page placement is untested",
                   congruence > 0);
    }

    private void runGuarded(long seed) throws Throwable
    {
        Case c = new Case(seed);
        try
        {
            run(c);
        }
        catch (Throwable t)
        {
            logger.error("BTI zero-copy fuzz iteration FAILED\n  {}\nreplay this case alone with:\n" +
                         "  ant testsome -Dtest.name=org.apache.cassandra.io.sstable.ZeroCopyBtiFuzzTest" +
                         " -Dtest.methods=fuzz -D" + PROP_REPLAY_SEED + '=' + seed, c, t);
            throw new AssertionError("BTI zero-copy fuzz iteration FAILED, " + c, t);
        }
    }

    // ------------------------------------------------------------------------------------------------

    /** One randomly generated shape. Printed verbatim when an iteration fails, so it is the whole reproducer. */
    private static final class Case
    {
        final long seed;
        final String compressor;
        final int chunkKb;
        final int columnIndexKb;
        final int partitions;
        final int rowsPerPartition;
        final int valueBytes;
        final int numChildren;
        final int[][] ranges;

        /** Shape of the data, which is what makes the oracle's per-partition comparisons non-vacuous. */
        final int clusterings;
        final boolean staticColumn;
        final boolean collections;
        final boolean ttl;
        final boolean tombstones;
        /** {@code zero_copy_split_digest_enabled} for this iteration's split. */
        final boolean digest;

        Case(long seed)
        {
            this.seed = seed;
            Random random = new Random(seed);
            this.compressor = new String[]{ "LZ4Compressor", "SnappyCompressor", "DeflateCompressor" }
                              [random.nextInt(3)];
            this.chunkKb = new int[]{ 4, 8, 16, 64 }[random.nextInt(4)];
            // 0 indexes every row, which is what makes a single partition's trie big enough to span pages.
            this.columnIndexKb = new int[]{ 0, 0, 1, 2, 16, 64 }[random.nextInt(6)];
            this.partitions = 12 + random.nextInt(40);
            this.rowsPerPartition = random.nextInt(4) == 0 ? 1 : 20 + random.nextInt(200);
            this.valueBytes = 40 + random.nextInt(400);
            this.numChildren = 1 + random.nextInt(Math.min(6, partitions));

            this.clusterings = 1 + random.nextInt(2);
            this.staticColumn = random.nextBoolean();
            this.collections = random.nextBoolean();
            this.ttl = random.nextBoolean();
            this.tombstones = random.nextInt(4) != 0;   // usually on: it is the shape with the most to go wrong
            this.digest = random.nextBoolean();

            int runs = 1 + random.nextInt(3);
            List<int[]> chosen = new ArrayList<>(runs);
            int cursor = 0;
            for (int r = 0; r < runs && cursor < partitions; r++)
            {
                int gap = random.nextInt(Math.max(1, partitions / (runs + 1)));
                int from = Math.min(partitions - 1, cursor + gap);
                int length = 1 + random.nextInt(Math.max(1, (partitions - from) / (runs - r)));
                int to = Math.min(partitions, from + length);
                if (to > from)
                    chosen.add(new int[]{ from, to });
                cursor = to + 1;    // +1 so the next run is separated by at least one partition
            }
            if (chosen.isEmpty())
                chosen.add(new int[]{ 0, partitions });
            this.ranges = chosen.toArray(new int[0][]);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            for (int[] range : ranges)
                sb.append('[').append(range[0]).append(',').append(range[1]).append(')');
            return String.format("Case{seed=%d, compressor=%s, chunkKb=%d, columnIndexKb=%d, partitions=%d, " +
                                 "rowsPerPartition=%d, valueBytes=%d, numChildren=%d, clusterings=%d, static=%b, " +
                                 "collections=%b, ttl=%b, tombstones=%b, digest=%b, ranges=%s}",
                                 seed, compressor, chunkKb, columnIndexKb, partitions, rowsPerPartition,
                                 valueBytes, numChildren, clusterings, staticColumn, collections, ttl, tombstones,
                                 digest, sb);
        }
    }

    private void run(Case c) throws Throwable
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(c.columnIndexKb);
        DatabaseDescriptor.setZeroCopySplitDigestEnabled(c.digest);

        createTable(ddl(c));
        disableCompaction();

        writeShapes(c, new Random(c.seed ^ 0x5DEECE66DL));
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals("expected exactly one sstable", 1, live.size());
        SSTableReader parent = live.iterator().next();
        assertTrue(BtiFormat.is(parent.descriptor.getFormat()));
        // Both paths refuse a version below 'eb', so a fixture pinned below it would turn the whole sweep into a
        // sweep of refusals -- loudly for the split, but verifySlice would simply fail() with the reason.
        assertTrue("the fixture flushed version '" + parent.descriptor.version.version +
                   "', which cannot carry hasUnindexedRegions",
                   parent.descriptor.version.hasUnindexedRegionsMarker());

        if (parent.descriptor.fileFor(BtiFormat.Components.ROW_INDEX).length() > 0)
            wideFixtures++;
        else
            narrowFixtures++;

        List<DecoratedKey> keys = keysInOrder(parent);
        assertEquals(c.partitions, keys.size());

        verifySplit(cfs, parent, keys, c);
        verifySlice(cfs, parent, keys, c);
    }

    /** Split into {@code numChildren} and check that the children together are the parent, partition for partition. */
    private void verifySplit(ColumnFamilyStore cfs, SSTableReader parent, List<DecoratedKey> keys, Case c)
    throws IOException
    {
        ZeroCopySSTableSplitter.Result result =
            ZeroCopySSTableSplitter.split(parent, Math.min(c.numChildren, keys.size()), null);
        try
        {
            List<DecoratedKey> seen = new ArrayList<>();
            DecoratedKey previousLast = null;
            for (ZeroCopySSTableSplitter.Child child : result.children)
            {
                SSTableReader reader = child.reader;
                if (previousLast != null)
                    assertTrue(previousLast + " >= " + reader.getFirst(),
                               previousLast.compareTo(reader.getFirst()) < 0);
                previousLast = reader.getLast();

                // The three descriptions of a child's components have to be the same set, or a restart or a
                // nodetool refresh -- which rediscover from TOC.txt -- sees a different sstable than this does.
                assertEquals("TOC of " + child.descriptor,
                             child.components, TOCComponent.loadTOC(child.descriptor, false));
                assertEquals("files on disk for " + child.descriptor,
                             child.components, child.descriptor.discoverComponents());
                assertEquals("Digest.crc32 of " + child.descriptor + " does not follow the flag",
                             c.digest, child.components.contains(SSTableFormat.Components.DIGEST));
                assertEquals("Digest.crc32 file of " + child.descriptor + " does not follow the flag",
                             c.digest, child.descriptor.fileFor(SSTableFormat.Components.DIGEST).exists());

                for (DecoratedKey key : allKeys(reader))
                {
                    seen.add(key);
                    assertTrue("child cannot find its own key " + key,
                               reader.getPosition(key, SSTableReader.Operator.EQ) >= 0);
                }
                assertContentMatches(parent, reader, allKeys(reader));
                verify(cfs, reader);
            }
            assertEquals("the children do not add up to the parent", keys, seen);
            splitsVerified++;
            if (c.digest)
                digestedSplits++;
            else
                digestlessSplits++;
        }
        finally
        {
            for (ZeroCopySSTableSplitter.Child child : result.children)
                child.reader.selfRef().release();
        }
    }

    /** Slice over the case's ranges, materialise the way the stream would, and check the same things. */
    private void verifySlice(ColumnFamilyStore cfs, SSTableReader parent, List<DecoratedKey> keys, Case c)
    throws IOException
    {
        List<Range<Token>> tokenRanges = new ArrayList<>(c.ranges.length);
        for (int[] range : c.ranges)
        {
            Token left = range[0] == 0 ? parent.getPartitioner().getMinimumToken()
                                       : keys.get(range[0] - 1).getToken();
            tokenRanges.add(new Range<>(left, keys.get(range[1] - 1).getToken()));
        }
        List<PartitionPositionBounds> sections = parent.getPositionsForRanges(tokenRanges);
        if (sections.isEmpty())
            return;

        Plan plan = ZeroCopySSTableSlice.plan(parent, sections, 1.0);
        if (!plan.isEligible())
            fail("a compressed BTI parent should always be sliceable: " + plan);
        if (plan.runs.size() > 1)
            multiRunSlices++;

        List<DecoratedKey> expected = new ArrayList<>();
        for (int[] range : c.ranges)
            for (int i = range[0]; i < range[1]; i++)
                expected.add(keys.get(i));

        Descriptor target = ZeroCopySSTableSlice.newDescriptor(parent);
        ZeroCopySSTableSlice.Slice slice = ZeroCopySSTableSlice.write(parent, plan, target);
        Set<Component> components = new HashSet<>(slice.components);
        components.add(SSTableFormat.Components.DATA);
        SSTableReader reader = null;
        try
        {
            copyRuns(parent, plan, target);
            components.add(SSTableFormat.Components.TOC);
            TOCComponent.updateTOC(target, components);
            reader = SSTableReader.open(cfs, target, components, cfs.metadata);

            assertEquals(expected.size(), slice.partitionCount);
            assertEquals(expected, allKeys(reader));
            for (DecoratedKey key : expected)
                assertTrue("the slice cannot find its own key " + key,
                           reader.getPosition(key, SSTableReader.Operator.EQ) >= 0);
            // A slice with an interior gap MUST be marked, or getScanner() hands back the partitions the copied
            // cells dragged along -- data the receiving node was never sent. Asserted here rather than only counted,
            // because the count below is what proves the verifier was ever run over such an sstable at all.
            assertEquals("hasUnindexedRegions does not follow the plan's interior dead space: " + plan,
                         plan.interiorDeadBytes() > 0, reader.hasUnindexedRegions());
            assertContentMatches(parent, reader, expected);
            verify(cfs, reader);
            slicesVerified++;
            if (plan.interiorDeadBytes() > 0)
                slicesWithInteriorGapsVerified++;
        }
        finally
        {
            if (reader != null)
                reader.selfRef().release();
            ZeroCopySSTableSlice.delete(target, components);
        }
    }

    private static void copyRuns(SSTableReader parent, Plan plan, Descriptor target) throws IOException
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
        assertEquals(plan.physicalBytes, target.fileFor(SSTableFormat.Components.DATA).length());
    }

    // ------------------------------------------------------------------------------------------------
    // The fixture
    // ------------------------------------------------------------------------------------------------

    /** The case's DDL. Generated rather than fixed, so the row shape can vary with the case. */
    private static String ddl(Case c)
    {
        StringBuilder sb = new StringBuilder("CREATE TABLE %s (pk text, ck0 int");
        if (c.clusterings > 1)
            sb.append(", ck1 text");
        sb.append(", val text, t text");
        if (c.staticColumn)
            sb.append(", sv text static");
        if (c.collections)
            sb.append(", m map<int, text>");
        sb.append(", PRIMARY KEY (pk, ck0");
        if (c.clusterings > 1)
            sb.append(", ck1");
        sb.append("))");
        sb.append(" WITH compression = {'class': '").append(c.compressor)
          .append("', 'chunk_length_in_kb': '").append(c.chunkKb).append("'}");
        return sb.toString();
    }

    private void writeShapes(Case c, Random random) throws Throwable
    {
        boolean twoClusterings = c.clusterings > 1;
        String insertPlain = twoClusterings ? "INSERT INTO %s (pk, ck0, ck1, val) VALUES (?, ?, ?, ?) USING TIMESTAMP ?"
                                            : "INSERT INTO %s (pk, ck0, val) VALUES (?, ?, ?) USING TIMESTAMP ?";
        String insertCollection = twoClusterings
                                  ? "INSERT INTO %s (pk, ck0, ck1, val, m) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?"
                                  : "INSERT INTO %s (pk, ck0, val, m) VALUES (?, ?, ?, ?) USING TIMESTAMP ?";
        String insertTtl = insertPlain + " AND TTL 8640000";
        String deleteRow = twoClusterings ? "DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 = ? AND ck1 = ?"
                                          : "DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 = ?";
        String deleteCell = twoClusterings ? "DELETE t FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 = ? AND ck1 = ?"
                                           : "DELETE t FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 = ?";

        for (int p = 0; p < c.partitions; p++)
        {
            String pk = String.format("k%06d", p);

            // Older than every row below, so the rows survive and the deletion stays in the partition header.
            if (c.tombstones && p % 5 == 0)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", OLD_TS, pk);
            if (c.staticColumn && p % 3 != 0)
                execute("INSERT INTO %s (pk, sv) VALUES (?, ?) USING TIMESTAMP ?", pk, "static-" + p, NEW_TS);

            for (int r = 0; r < c.rowsPerPartition; r++)
            {
                String value = randomText(random, c.valueBytes);
                // The second disjunct in each condition is what reaches these shapes in a one-row-per-partition
                // case, where r is always 0 and a modulus on it would never fire.
                if (c.collections && (r % 7 == 3 || (c.rowsPerPartition == 1 && p % 4 == 3)))
                {
                    Map<Integer, String> m = new HashMap<>();
                    m.put(r, "m" + r);
                    m.put(r + 1, "m" + (r + 1));
                    execute(insertCollection, args(twoClusterings, pk, r, value, m, NEW_TS));
                }
                else if (c.ttl && (r % 11 == 5 || (c.rowsPerPartition == 1 && p % 4 == 2)))
                {
                    execute(insertTtl, args(twoClusterings, pk, r, value, null, NEW_TS));
                }
                else
                {
                    execute(insertPlain, args(twoClusterings, pk, r, value, null, NEW_TS));
                }
            }

            if (c.tombstones && p % 4 == 1 && c.rowsPerPartition > 1)
            {
                // A row tombstone over the LAST row, so no partition is ever emptied.
                int last = c.rowsPerPartition - 1;
                execute(deleteRow, twoClusterings ? new Object[]{ NEW_TS + 1, pk, last, "c" + last }
                                                  : new Object[]{ NEW_TS + 1, pk, last });
                if (c.rowsPerPartition > 8)
                    execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck0 > ? AND ck0 < ?",
                            NEW_TS + 1, pk, c.rowsPerPartition / 2, c.rowsPerPartition / 2 + 3);
            }
            if (c.tombstones && p % 6 == 2)
                execute(deleteCell, twoClusterings ? new Object[]{ NEW_TS + 1, pk, 0, "c0" }
                                                   : new Object[]{ NEW_TS + 1, pk, 0 });
        }
    }

    /** Bind values for one row insert, with or without the second clustering column and the collection. */
    private static Object[] args(boolean twoClusterings, String pk, int r, String value, Object collection, long ts)
    {
        List<Object> values = new ArrayList<>(6);
        values.add(pk);
        values.add(r);
        if (twoClusterings)
            values.add("c" + r);
        values.add(value);
        if (collection != null)
            values.add(collection);
        values.add(ts);
        return values.toArray();
    }

    // ------------------------------------------------------------------------------------------------

    private static void assertContentMatches(SSTableReader parent, SSTableReader target, List<DecoratedKey> expected)
    {
        Set<DecoratedKey> wanted = new HashSet<>(expected);
        int compared = 0;
        try (ISSTableScanner parentScanner = parent.getScanner();
             ISSTableScanner targetScanner = target.getScanner())
        {
            while (targetScanner.hasNext())
            {
                assertTrue("more partitions than expected", compared < expected.size());
                try (UnfilteredRowIterator actual = targetScanner.next())
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
        assertEquals("missing partitions", expected.size(), compared);
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
            assertTrue(context + ": target ran out at row " + row, actual.hasNext());
            assertEquals(context + " row " + row, expected.next(), actual.next());
            row++;
        }
        assertFalse(context + ": target has extra rows", actual.hasNext());
    }

    private static void verify(ColumnFamilyStore cfs, SSTableReader sstable)
    {
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), true,
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

    /** Near-incompressible payload, so the sstable really does span many compression cells. */
    private static String randomText(Random random, int length)
    {
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = (char) ('!' + random.nextInt(94));
        return new String(chars);
    }

    private static long scramble(long seed)
    {
        long x = seed * 0x9E3779B97F4A7C15L;
        x = (x ^ (x >>> 30)) * 0xBF58476D1CE4E5B9L;
        x = (x ^ (x >>> 27)) * 0x94D049BB133111EBL;
        return x ^ (x >>> 31);
    }
}
