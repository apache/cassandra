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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
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

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end wiring test for the zero-copy anticompaction path
 * ({@link CompactionManager#zeroCopyAntiCompact}, {@link AntiCompactionRunPlanner},
 * {@link ZeroCopySSTableSplitter}). Every test drives the real
 * {@link CompactionManager#performAnticompaction} against a real registered parent repair session, so the whole
 * chain -- {@code validateSSTableBoundsForAnticompaction}, {@code mutateFullyContainedSSTables},
 * {@code doAntiCompaction}'s per-group {@code txn.split}, the carve-out, the 1-to-N transaction replacement and
 * the accounting guard -- is exercised.
 *
 * <h2>How "which path ran" is observed</h2>
 * {@code cfs.metric.bytesZeroCopyAnticompaction} is marked <em>only</em> after a successful zero-copy commit, and
 * CQLTester makes a fresh table (hence fresh {@code TableMetrics}) per test, so its per-table count is an exact,
 * non-flaky witness: {@code > 0} means the split ran, {@code 0} the rewrite. The output sstable count is a second,
 * independent witness, since the paths differ structurally for the same input: the rewrite produces at most one
 * sstable per repair bucket (full / transient / unrepaired) where the split produces one child per contiguous label
 * <em>run</em>, so {@code U F U} is 2 rewritten but 3 split and {@code U F U T U} is 3 versus 5. Both are asserted
 * in every test, in both directions, so this fails if the gate silently stops <em>or</em> starts engaging.
 *
 * <h2>Correctness assertions, applied identically to every case</h2>
 * {@link #assertOutcome} snapshots every partition of the parent before the run (key -&gt; a full textual rendering
 * including partition deletions, row liveness info, row deletions, every cell value and timestamp) and compares it
 * against the union of the outputs afterwards, failing on a missing key, an extra key, a key in two outputs at
 * once, or any content difference. Repair state is checked <em>per partition key</em> rather than per sstable,
 * since a partition routed into the wrong bucket is the critical failure mode, and {@link Util#assertOnDiskState}
 * proves the parent's files are really gone.
 */
public class ZeroCopyAntiCompactionTest extends CQLTester
{
    /** Enough partitions to give every run several compression chunks of its own. */
    private static final int PARTITIONS = 200;
    private static final int ROWS_PER_PARTITION = 5;
    private static final int VALUE_BYTES = 500;

    /** What the ranges say a partition should become. Computed by the test, independently of the planner. */
    private enum Expect
    { FULL, TRANSIENT, UNREPAIRED }

    private boolean savedZeroCopyEnabled;

    @Before
    public void saveZeroCopyFlag()
    {
        savedZeroCopyEnabled = DatabaseDescriptor.getZeroCopyAnticompactionEnabled();
    }

    @After
    public void restoreZeroCopyFlag()
    {
        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(savedZeroCopyEnabled);
    }

    // ----------------------------------------------------------------------------------------------------
    // The happy path
    // ----------------------------------------------------------------------------------------------------

    /**
     * The single-contiguous-run shape the gate exists for: {@code UNREPAIRED, FULL, UNREPAIRED}. The zero-copy
     * split must run and must produce one child per run (3), each with the right repair state for every key it
     * holds, and together holding exactly the parent's data.
     */
    @Test
    public void singleFullRunIsSplitZeroCopy() throws Throwable
    {
        createCompressedTable("");
        insertPartitions();
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertTrue("the whole point of this test is a compressed BIG sstable",
                   ZeroCopySSTableSplitter.isSupported(parent));

        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        assertEquals(keys.get(0), parent.first);
        assertEquals(keys.get(keys.size() - 1), parent.last);

        // (keys[59].token, keys[139].token] covers keys[60..139] exactly: U(0..59) F(60..139) U(140..199).
        Collection<Range<Token>> full = Collections.singleton(rangeCovering(keys, 60, 140));
        RangesAtEndpoint ranges = rangesAtEndpoint(full, Collections.emptySet());

        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);
        Outputs before = collect(Collections.singleton(parent));
        assertEquals(PARTITIONS, before.partitions);

        TimeUUID sessionID = nextTimeUUID();
        anticompact(cfs, ranges, sessionID);

        assertTrue("the zero-copy split did not run: metric is " + zeroCopyBytes(cfs),
                   zeroCopyBytes(cfs) > 0);
        // 3 runs -> 3 children. The rewrite path would have produced 2 (one pending, one unrepaired).
        assertOutcome(cfs, parent, before, full, Collections.emptySet(), sessionID, 3);
        assertReopenableAndVerifiable(cfs);
    }

    /**
     * Both a full and a transient range, laid out as {@code UNREPAIRED, FULL, UNREPAIRED, TRANSIENT,
     * UNREPAIRED} -- 5 runs, still eligible because FULL and TRANSIENT each occupy exactly one run. Getting
     * {@code isTransient} the wrong way round is a real correctness bug (a non-transient pending child is
     * promoted to repaired at session finalize instead of being dropped), so it is asserted per key.
     */
    @Test
    public void fullAndTransientRunsAreSplitZeroCopy() throws Throwable
    {
        createCompressedTable("");
        insertPartitions();
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertTrue(ZeroCopySSTableSplitter.isSupported(parent));

        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        Collection<Range<Token>> full = Collections.singleton(rangeCovering(keys, 40, 90));
        Collection<Range<Token>> trans = Collections.singleton(rangeCovering(keys, 120, 170));
        RangesAtEndpoint ranges = rangesAtEndpoint(full, trans);

        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);
        Outputs before = collect(Collections.singleton(parent));
        assertEquals(PARTITIONS, before.partitions);

        TimeUUID sessionID = nextTimeUUID();
        anticompact(cfs, ranges, sessionID);

        assertTrue("the zero-copy split did not run", zeroCopyBytes(cfs) > 0);
        // 5 runs -> 5 children. The rewrite path would have produced 3.
        assertOutcome(cfs, parent, before, full, trans, sessionID, 5);
        assertReopenableAndVerifiable(cfs);

        // and, explicitly: exactly one transient output, holding exactly the transient range's partitions
        int transientSSTables = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (sstable.isTransient())
            {
                transientSSTables++;
                assertTrue("a transient child must be pending repair", sstable.isPendingRepair());
                assertEquals(sessionID, sstable.getPendingRepair());
            }
        }
        assertEquals(1, transientSSTables);
    }

    // ----------------------------------------------------------------------------------------------------
    // The gate: everything that must NOT take the zero-copy path
    // ----------------------------------------------------------------------------------------------------

    /**
     * Interleaved full ranges -- FULL in two runs, i.e. {@code U F U F U} -- is what vnodes produce and is
     * exactly what the gate rejects, because the split can only emit contiguous key ranges. The rewrite path
     * must run instead, and the result must still be completely correct.
     */
    @Test
    public void interleavedFullRangesFallBackToTheRewritePath() throws Throwable
    {
        createCompressedTable("");
        insertPartitions();
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertTrue("the sstable itself is eligible; only the range layout must make it ineligible",
                   ZeroCopySSTableSplitter.isSupported(parent));

        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        List<Range<Token>> full = new ArrayList<>();
        full.add(rangeCovering(keys, 20, 40));
        full.add(rangeCovering(keys, 100, 120));
        RangesAtEndpoint ranges = rangesAtEndpoint(full, Collections.emptySet());

        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);
        Outputs before = collect(Collections.singleton(parent));

        TimeUUID sessionID = nextTimeUUID();
        anticompact(cfs, ranges, sessionID);

        assertEquals("the zero-copy split must not run for interleaved ranges", 0, zeroCopyBytes(cfs));
        // The rewrite routes by token, so the two FULL runs land in ONE pending sstable and the three
        // UNREPAIRED runs land in ONE unrepaired sstable. The split would have produced 5.
        assertOutcome(cfs, parent, before, full, Collections.emptySet(), sessionID, 2);
    }

    /** An uncompressed sstable has no compression chunks to copy, so {@code isSupported} is false. */
    @Test
    public void uncompressedSSTableFallsBackToTheRewritePath() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        disableCompaction();
        insertPartitions();
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertFalse("an uncompressed sstable must not be splittable",
                    ZeroCopySSTableSplitter.isSupported(parent));

        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        Collection<Range<Token>> full = Collections.singleton(rangeCovering(keys, 60, 140));
        RangesAtEndpoint ranges = rangesAtEndpoint(full, Collections.emptySet());

        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);
        Outputs before = collect(Collections.singleton(parent));

        TimeUUID sessionID = nextTimeUUID();
        anticompact(cfs, ranges, sessionID);

        assertEquals("an uncompressed sstable must not take the zero-copy path", 0, zeroCopyBytes(cfs));
        assertOutcome(cfs, parent, before, full, Collections.emptySet(), sessionID, 2);
    }

    /**
     * The kill switch. Exactly the eligible layout of {@link #singleFullRunIsSplitZeroCopy}, but with
     * {@code zero_copy_anticompaction_enabled = false}: the split must not run at all (not even the planner's
     * Index.db walk), and the outcome must be the unchanged rewrite result.
     */
    @Test
    public void killSwitchDisablesTheZeroCopyPath() throws Throwable
    {
        createCompressedTable("");
        insertPartitions();
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertTrue("with the flag on this sstable would be split", ZeroCopySSTableSplitter.isSupported(parent));

        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        Collection<Range<Token>> full = Collections.singleton(rangeCovering(keys, 60, 140));
        RangesAtEndpoint ranges = rangesAtEndpoint(full, Collections.emptySet());

        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(false);
        Outputs before = collect(Collections.singleton(parent));

        TimeUUID sessionID = nextTimeUUID();
        anticompact(cfs, ranges, sessionID);

        assertEquals("the kill switch did not stop the zero-copy path", 0, zeroCopyBytes(cfs));
        // 2, not the 3 children the split produces for this same layout.
        assertOutcome(cfs, parent, before, full, Collections.emptySet(), sessionID, 2);
    }

    // ----------------------------------------------------------------------------------------------------
    // The accepted behaviour change
    // ----------------------------------------------------------------------------------------------------

    /**
     * Pins the accepted behaviour change: the zero-copy path copies compression chunks verbatim and therefore
     * RETAINS droppable tombstones and shadowed data the rewriting anticompaction would have purged. Retention,
     * never loss -- nothing can be resurrected -- and deliberately not gated on the droppable-tombstone ratio.
     * <p>
     * The parent carries a partition-level and a row-level tombstone that are genuinely droppable when the
     * anticompaction runs ({@code gc_grace_seconds = 0}, and the run is more than a second after the deletes, so
     * {@code localDeletionTime < gcBefore}), asserted via {@link SSTableReader#getDroppableTombstonesBefore} so this
     * cannot pass vacuously. Both must still be there afterwards. If someone later "fixes" this by purging, this
     * test is the record that the retention was intentional.
     */
    @Test
    public void purgeableTombstonesSurviveTheZeroCopySplit() throws Throwable
    {
        createCompressedTable(" AND gc_grace_seconds = 0");
        insertPartitions();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        // Both deleted partitions sit in the middle of the token order, so the full range below contains them.
        String deletedPartition = keyOf(keys.get(100));
        String partiallyDeleted = keyOf(keys.get(101));
        execute("DELETE FROM %s WHERE pk = ?", deletedPartition);
        execute("DELETE FROM %s WHERE pk = ? AND ck = ?", partiallyDeleted, 2);
        flush();

        SSTableReader parent = onlySSTable(cfs);
        assertTrue(ZeroCopySSTableSplitter.isSupported(parent));

        // gcBefore is computed from the wall clock when the anticompaction starts, and purging requires
        // localDeletionTime < gcBefore, so the deletes must be strictly in the past for them to be droppable.
        Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);

        Collection<Range<Token>> full = Collections.singleton(rangeCovering(keys, 60, 140));
        RangesAtEndpoint ranges = rangesAtEndpoint(full, Collections.emptySet());

        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);
        Outputs before = collect(Collections.singleton(parent));
        assertEquals(PARTITIONS, before.partitions);
        DeletionTime parentTombstone = before.partitionDeletion.get(hexOf(deletedPartition));
        assertFalse("the fully deleted partition should carry a partition level tombstone in the parent",
                    parentTombstone.isLive());
        // Non-vacuity check. Assert the exact predicate CompactionController's purge evaluator applies
        // (localDeletionTime < gcBefore) rather than SSTableReader.getDroppableTombstonesBefore: that reads
        // estimatedTombstoneDropTime, and StreamingTombstoneHistogramBuilder.update rounds every point UP to
        // the next roundSeconds boundary (ceilKey(point, roundSeconds), 60s by default per SSTable.java:81).
        // A tombstone written a second ago therefore lands in a future bucket and the estimate is legitimately
        // 0 here, which says nothing about whether the tombstone is actually droppable.
        assertTrue("the partition tombstone is not droppable yet, so this test would pass vacuously:"
                   + " localDeletionTime=" + parentTombstone.localDeletionTime()
                   + " gcBefore=" + FBUtilities.nowInSeconds(),
                   parentTombstone.localDeletionTime() < FBUtilities.nowInSeconds());

        TimeUUID sessionID = nextTimeUUID();
        anticompact(cfs, ranges, sessionID);

        assertTrue("the zero-copy split did not run", zeroCopyBytes(cfs) > 0);
        // The content comparison inside assertOutcome already proves byte-for-byte retention of both
        // tombstones; the explicit assertions below make the intent unmissable.
        Outputs after = assertOutcome(cfs, parent, before, full, Collections.emptySet(), sessionID, 3);

        DeletionTime survived = after.partitionDeletion.get(hexOf(deletedPartition));
        assertNotNull("the fully deleted partition disappeared entirely", survived);
        assertFalse("DECISION 2: a droppable partition tombstone must SURVIVE the zero-copy split",
                    survived.isLive());
        assertEquals("the surviving partition tombstone must be bit-identical",
                     before.partitionDeletion.get(hexOf(deletedPartition)), survived);
        assertEquals("DECISION 2: the droppable row tombstone must SURVIVE the zero-copy split",
                     before.content.get(hexOf(partiallyDeleted)),
                     after.content.get(hexOf(partiallyDeleted)));
    }

    // ----------------------------------------------------------------------------------------------------
    // Driving the real anticompaction
    // ----------------------------------------------------------------------------------------------------

    /**
     * Runs {@link CompactionManager#performAnticompaction} over every live sstable, exactly as
     * {@code PendingAntiCompaction} does: a registered parent repair session, the sstables marked
     * {@code ANTICOMPACTION} in the tracker, and a {@link Refs} that {@code performAnticompaction} consumes
     * itself (which is what finally unlinks the obsoleted parent's files).
     */
    private void anticompact(ColumnFamilyStore cfs, RangesAtEndpoint ranges, TimeUUID sessionID) throws Exception
    {
        Set<SSTableReader> sstables = ImmutableSet.copyOf(cfs.getLiveSSTables());
        assertFalse("nothing to anticompact", sstables.isEmpty());

        ActiveRepairService.instance.registerParentRepairSession(sessionID,
                                                                InetAddressAndPort.getByName("127.0.0.1"),
                                                                Lists.newArrayList(cfs),
                                                                ranges.ranges(),
                                                                true,
                                                                ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                true,
                                                                PreviewKind.NONE);
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            assertNotNull("could not mark the sstables compacting", txn);
            CompactionManager.instance.performAnticompaction(cfs, ranges, refs, txn, sessionID, () -> false);
        }
        finally
        {
            ActiveRepairService.instance.removeParentRepairSession(sessionID);
        }
    }

    /**
     * Every outcome assertion that must hold no matter which path ran:
     * <ol>
     *   <li>the parent is gone from the live set, is marked compacted, and no orphan Data.db survives
     *       {@code waitForDeletions} ({@link Util#assertOnDiskState});</li>
     *   <li>the live sstable count is exactly {@code expectedSSTables} -- which differs between the two paths
     *       and so doubles as a witness of which one ran;</li>
     *   <li>no partition is lost, duplicated or altered: same key set, same content, same total count;</li>
     *   <li>the repair state of the sstable holding each key matches what the ranges say for that key.</li>
     * </ol>
     *
     * @return the outputs, so a caller can make extra assertions about them
     */
    private Outputs assertOutcome(ColumnFamilyStore cfs,
                                  SSTableReader parent,
                                  Outputs before,
                                  Collection<Range<Token>> full,
                                  Collection<Range<Token>> trans,
                                  TimeUUID sessionID,
                                  int expectedSSTables)
    {
        assertFalse("the parent is still live, so it was not obsoleted", cfs.getLiveSSTables().contains(parent));
        assertTrue("the parent was not marked compacted", parent.isMarkedCompacted());
        // waits for deletions, asserts the live count, and asserts that every *Data.db on disk belongs to a
        // live sstable -- i.e. that the parent's files really are gone
        Util.assertOnDiskState(cfs, expectedSSTables);
        assertEquals("sstables were left marked compacting", 0, cfs.getTracker().getCompacting().size());

        Outputs after = collect(cfs.getLiveSSTables());

        assertEquals("partitions were lost or duplicated", before.partitions, after.partitions);
        assertEquals("the set of partition keys changed", before.content.keySet(), after.content.keySet());
        for (Map.Entry<String, String> entry : before.content.entrySet())
        {
            assertEquals("the content of partition " + entry.getKey() + " changed",
                         entry.getValue(), after.content.get(entry.getKey()));
        }

        for (Map.Entry<String, Token> entry : after.token.entrySet())
        {
            String key = entry.getKey();
            Expect expected = expectedFor(entry.getValue(), full, trans);
            SSTableReader owner = after.owner.get(key);
            String context = "partition " + key + " (expected " + expected + ") in " + owner.descriptor;

            // repairedAt is never set by anticompaction; the promotion happens later, at session finalize.
            assertFalse(context + ": must not be repaired", owner.isRepaired());
            assertEquals(context + ": repairedAt", ActiveRepairService.UNREPAIRED_SSTABLE, owner.getRepairedAt());
            switch (expected)
            {
                case FULL:
                    assertEquals(context + ": pendingRepair", sessionID, owner.getPendingRepair());
                    assertFalse(context + ": isTransient", owner.isTransient());
                    break;
                case TRANSIENT:
                    assertEquals(context + ": pendingRepair", sessionID, owner.getPendingRepair());
                    assertTrue(context + ": isTransient", owner.isTransient());
                    break;
                default:
                    assertNull(context + ": must not be pending repair", owner.getPendingRepair());
                    assertFalse(context + ": isTransient", owner.isTransient());
                    break;
            }
        }
        return after;
    }

    /**
     * Nothing may depend on in-memory state: reopen each output purely from its on-disk components and run the
     * extended {@link Verifier} over it, which walks Data.db linearly from the first index position, validates
     * Digest.crc32, the index, the summary and the bloom filter, and throws on any inconsistency. This is the
     * assertion that catches a child whose rebuilt components disagree with its copied Data.db.
     */
    private void assertReopenableAndVerifiable(ColumnFamilyStore cfs) throws Exception
    {
        for (SSTableReader live : cfs.getLiveSSTables())
        {
            SSTableReader reopened = SSTableReader.open(live.descriptor, live.getComponents(), cfs.metadata);
            try
            {
                assertEquals(live.first, reopened.first);
                assertEquals(live.last, reopened.last);
                assertEquals(live.getPendingRepair(), reopened.getPendingRepair());
                assertEquals(live.isTransient(), reopened.isTransient());
                assertEquals(live.getRepairedAt(), reopened.getRepairedAt());
                try (Verifier verifier = new Verifier(cfs, reopened, true,
                                                      Verifier.options().extendedVerification(true).build()))
                {
                    verifier.verify();
                }
            }
            finally
            {
                reopened.selfRef().release();
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Content snapshots
    // ----------------------------------------------------------------------------------------------------

    /** Everything the assertions need about one set of sstables, keyed by hex partition key. */
    private static final class Outputs
    {
        /** Full textual rendering of the partition: deletions, liveness info, cells, timestamps. */
        final Map<String, String> content = new HashMap<>();
        /** Which sstable held the partition. */
        final Map<String, SSTableReader> owner = new HashMap<>();
        final Map<String, Token> token = new HashMap<>();
        final Map<String, DeletionTime> partitionDeletion = new HashMap<>();
        int partitions;
    }

    /**
     * Scans every sstable and records each partition. A partition appearing in two sstables fails here, which
     * is how duplication is detected: the outputs of an anticompaction are disjoint by construction.
     */
    private static Outputs collect(Collection<SSTableReader> sstables)
    {
        Outputs out = new Outputs();
        for (SSTableReader sstable : sstables)
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        DecoratedKey key = partition.partitionKey();
                        String hex = ByteBufferUtil.bytesToHex(key.getKey());
                        DeletionTime deletion = partition.partitionLevelDeletion();
                        // describe() consumes the iterator, so it must happen inside this block
                        String description = describe(partition, sstable.metadata());
                        SSTableReader previous = out.owner.get(hex);
                        assertNull("partition " + hex + " is in both " + previous + " and " + sstable.descriptor,
                                   out.content.put(hex, description));
                        out.owner.put(hex, sstable);
                        out.token.put(hex, key.getToken());
                        out.partitionDeletion.put(hex, deletion);
                        out.partitions++;
                    }
                }
            }
        }
        assertTrue("nothing was collected", out.partitions > 0);
        return out;
    }

    /**
     * A canonical rendering of one partition. {@code toString(metadata, true)} is the full-detail form: primary key
     * liveness info (timestamp, ttl, local expiration), the row deletion if any, and every cell via
     * {@code AbstractCell.toString()}, which includes the cell timestamp and marks tombstones. So comparing these
     * strings compares rows, cells, timestamps and deletions, not just keys -- and unlike the iterators they can be
     * retained safely after the scanner and the parent's files are gone.
     */
    private static String describe(UnfilteredRowIterator partition, TableMetadata metadata)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("partitionDeletion=").append(partition.partitionLevelDeletion());
        sb.append(" static=").append(partition.staticRow().toString(metadata, true));
        while (partition.hasNext())
            sb.append('\n').append(partition.next().toString(metadata, true));
        return sb.toString();
    }

    // ----------------------------------------------------------------------------------------------------
    // Fixtures
    // ----------------------------------------------------------------------------------------------------

    private void createCompressedTable(String extraOptions) throws Throwable
    {
        // small chunks plus near-incompressible values, so the sstable really spans many compression chunks
        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}" + extraOptions);
        disableCompaction();
    }

    private void insertPartitions() throws Throwable
    {
        for (int p = 0; p < PARTITIONS; p++)
            for (int c = 0; c < ROWS_PER_PARTITION; c++)
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", key(p), c, randomText(VALUE_BYTES));
    }

    private static String key(int p)
    {
        return String.format("k%06d", p);
    }

    /** Near-incompressible payload. */
    private static String randomText(int length)
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = (char) ('!' + random.nextInt(94));
        return new String(chars);
    }

    /**
     * The partition keys in on-disk (token) order, derived by decorating them directly rather than by reading
     * the sstable, so ranges can be chosen before anything is written and so the test is partitioner agnostic.
     * The callers cross-check the result against the parent's {@code first} / {@code last}.
     */
    private static List<DecoratedKey> keysInTokenOrder(ColumnFamilyStore cfs)
    {
        List<DecoratedKey> keys = new ArrayList<>(PARTITIONS);
        for (int p = 0; p < PARTITIONS; p++)
            keys.add(cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(key(p))));
        keys.sort(DecoratedKey::compareTo);
        return keys;
    }

    /**
     * A range covering exactly {@code keys[fromInclusive .. toExclusive - 1]}. Ranges are half-open
     * {@code (left, right]}, so the left bound is the token of the key just before the first one wanted.
     * {@code fromInclusive} must be &gt; 0 and {@code toExclusive} &lt;= {@code keys.size()}, which is what
     * leaves an unrepaired run on each side and keeps {@code mutateFullyContainedSSTables} from claiming the
     * sstable via the metadata-only path.
     */
    private static Range<Token> rangeCovering(List<DecoratedKey> keys, int fromInclusive, int toExclusive)
    {
        assertTrue("leave an unrepaired prefix", fromInclusive > 0);
        assertTrue("leave an unrepaired suffix", toExclusive < keys.size());
        return new Range<>(keys.get(fromInclusive - 1).getToken(), keys.get(toExclusive - 1).getToken());
    }

    private static RangesAtEndpoint rangesAtEndpoint(Collection<Range<Token>> full,
                                                     Collection<Range<Token>> trans)
    {
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(local);
        for (Range<Token> range : full)
            builder.add(Replica.fullReplica(local, range));
        for (Range<Token> range : trans)
            builder.add(Replica.transientReplica(local, range));
        return builder.build();
    }

    /** What the ranges say a token becomes; full wins over transient, as the anticompaction routing does. */
    private static Expect expectedFor(Token token, Collection<Range<Token>> full, Collection<Range<Token>> trans)
    {
        for (Range<Token> range : full)
        {
            if (range.contains(token))
                return Expect.FULL;
        }
        for (Range<Token> range : trans)
        {
            if (range.contains(token))
                return Expect.TRANSIENT;
        }
        return Expect.UNREPAIRED;
    }

    /**
     * The witness of which path ran: marked only on a successful zero-copy commit, and zero for a fresh table.
     */
    private static long zeroCopyBytes(ColumnFamilyStore cfs)
    {
        return cfs.metric.bytesZeroCopyAnticompaction.getCount();
    }

    private static SSTableReader onlySSTable(ColumnFamilyStore cfs)
    {
        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals("expected exactly one sstable", 1, live.size());
        return live.iterator().next();
    }

    private static String keyOf(DecoratedKey key) throws Exception
    {
        return ByteBufferUtil.string(key.getKey());
    }

    private static String hexOf(String partitionKey)
    {
        return ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(partitionKey));
    }
}
