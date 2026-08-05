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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TestDatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
 * {@code cfs.metric.bytesZeroCopyAnticompaction} is marked <em>only</em> after a successful zero-copy commit
 * (CompactionManager.zeroCopySplitOne), and CQLTester makes a fresh table (hence a fresh {@code TableMetrics})
 * per test, so its per-table count is an exact, non-flaky witness: {@code > 0} means the split ran, {@code 0}
 * means the three-writer rewrite ran. The number of output sstables is a second, independent witness, because
 * the two paths produce structurally different results for the same input: the rewrite produces at most one
 * sstable per repair bucket (full / transient / unrepaired), while the split produces one child per contiguous
 * label <em>run</em> -- so a {@code U F U} layout is 2 sstables rewritten but 3 children split, and
 * {@code U F U T U} is 3 rewritten but 5 split. Both are asserted in every test, in both directions, so this
 * file fails if the gate silently stops engaging <em>or</em> silently starts engaging.
 *
 * <h2>Correctness assertions, applied identically to every case</h2>
 * {@link #assertOutcome} snapshots every partition of the parent before the run (key -&gt; a full textual
 * rendering including partition deletions, row liveness info, row deletions, every cell value and every cell
 * timestamp) and compares it against the union of the outputs afterwards, failing on a missing key, an extra
 * key, a key present in two outputs at once, or any content difference. It also checks the repair state
 * <em>per partition key</em> rather than per sstable, since a partition routed into the wrong bucket is the
 * critical failure mode, and finishes with {@link Util#assertOnDiskState} which proves the parent's files are
 * really gone.
 *
 * <h2>Format and configuration preconditions</h2>
 * BIG and BTI are both supported. The cases whose expected output sstable COUNT is stated in terms of the BIG
 * layout are gated on {@link BigFormat#isSelected()} so they do not double as accidental BTI tests; the case that
 * is genuinely format agnostic ({@link #uncompressedSSTableFallsBackToTheRewritePath}) runs under whichever format
 * is selected, and {@link #btiSSTableIsSplitZeroCopy} forces BTI on purpose.
 * {@code zero_copy_anticompaction_enabled} defaults to <b>false</b>, so every test sets it explicitly (in both
 * directions); it, the disk failure policy and the splitter's injection hook are saved and restored around each one.
 *
 * <p><b>Eligibility is asserted, never assumed.</b> {@link ZeroCopySSTableSplitter#isSupported} also requires an
 * sstable version that can carry {@code StatsMetadata.hasUnindexedRegions} (BIG {@code pb}+, BTI {@code eb}+), which
 * a run with {@code storage_compatibility_mode} set does not produce. Every test whose meaning depends on the split
 * running therefore goes through {@link #assertSplittable}, so an inert build fails loudly and says why instead of
 * quietly turning each "the split ran" assertion into an assertion about a fallback.
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
    private Config.DiskFailurePolicy savedDiskFailurePolicy;

    @Before
    public void saveZeroCopyFlag()
    {
        savedZeroCopyEnabled = DatabaseDescriptor.getZeroCopyAnticompactionEnabled();
        savedDiskFailurePolicy = DatabaseDescriptor.getDiskFailurePolicy();
        // zeroCopySplitOne runs its failure through JVMStabilityInspector, and a CorruptSSTableException under
        // stop/stop_paranoid/die would take the transports or the JVM down with the test.
        DatabaseDescriptor.setDiskFailurePolicy(Config.DiskFailurePolicy.ignore);
    }

    /**
     * Restores everything, unconditionally and before any other teardown, because several tests here mutate global
     * state on the way in: two configuration flags and one {@code static volatile} injection hook on a production
     * class. A hook left set leaks into every later test in the same JVM -- {@code ZeroCopySSTableSplitter} is used
     * by streaming as well -- so it is cleared here rather than only in the finally of the test that sets it.
     */
    @After
    public void restoreZeroCopyFlag()
    {
        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(savedZeroCopyEnabled);
        DatabaseDescriptor.setDiskFailurePolicy(savedDiskFailurePolicy);
        ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
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
        Assume.assumeTrue(BigFormat.isSelected());
        createCompressedTable("");
        insertPartitions();
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertSplittable("the whole point of this test is a compressed BIG sstable", parent);

        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        assertEquals(keys.get(0), parent.getFirst());
        assertEquals(keys.get(keys.size() - 1), parent.getLast());

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
        Assume.assumeTrue(BigFormat.isSelected());
        createCompressedTable("");
        insertPartitions();
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertSplittable(parent);

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
        // BIG only: the point of the case is that an *eligible* sstable is rejected purely on its range layout,
        // which is only observable where isSupported() can be true in the first place.
        Assume.assumeTrue(BigFormat.isSelected());
        createCompressedTable("");
        insertPartitions();
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertSplittable("the sstable itself is eligible; only the range layout must make it ineligible", parent);

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

    /**
     * TWO sstables in one anticompaction group, both eligible. {@code groupSSTablesForAntiCompaction} packs
     * groupSize = 2, so this is the DEFAULT shape in production, yet every other test here has a single-sstable
     * group -- which leaves {@code zeroCopyAntiCompact}'s loop, its repeated {@code groupTxn.split()} on the same
     * transaction, and the {@code handledByZeroCopy} accounting untested.
     *
     * <p>The specific hazard: the loop iterates a copy ({@code new ArrayList<>(groupTxn.originals())}) because
     * {@code originals()} is an unmodifiable view over a live set that {@code split()} removes from. With one
     * member a HashSet iterator never rechecks modCount, so dropping that copy is invisible; with two it is a
     * ConcurrentModificationException.
     *
     * <p>The parents are made token-disjoint so every output can be attributed to exactly one of them, which is
     * what proves the carve-out took only its own sstable.
     */
    @Test
    public void groupOfTwoEligibleSSTablesIsFullyZeroCopySplit() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        createCompressedTable("");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        insertPartitionsByTokenRank(cfs, 0, 100);
        flush();
        insertPartitionsByTokenRank(cfs, 100, PARTITIONS);
        flush();

        List<SSTableReader> live = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals("expected two sstables", 2, live.size());
        live.sort(SSTableReader.firstKeyComparator);
        SSTableReader lower = live.get(0);
        SSTableReader upper = live.get(1);
        assertTrue("the parents must be token-disjoint", lower.getLast().compareTo(upper.getFirst()) < 0);

        // The premise: the real grouping code puts both in one group.
        assertEquals("the two sstables must land in ONE anticompaction group", 1,
                     cfs.getCompactionStrategyManager().groupSSTablesForAntiCompaction(live).size());

        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        // One FULL range inside each parent, so each is U F U -> 3 children, 6 outputs in total.
        List<Range<Token>> full = new ArrayList<>();
        full.add(rangeCovering(keys, 20, 40));
        full.add(rangeCovering(keys, 120, 140));
        RangesAtEndpoint ranges = rangesAtEndpoint(full, Collections.emptySet());

        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);
        Outputs before = collect(live);
        assertEquals(PARTITIONS, before.partitions);

        TimeUUID sessionID = nextTimeUUID();
        for (SSTableReader parent : live)
            assertTrue("both parents must be eligible: " + parent,
                       AntiCompactionRunPlanner.plan(parent, ranges, sessionID).eligible);

        anticompact(cfs, ranges, sessionID);

        assertTrue("the zero-copy split must have run", zeroCopyBytes(cfs) > 0);
        Outputs after = assertOutcome(cfs, live, before, full, Collections.emptySet(), sessionID, 6);
        assertEquals(PARTITIONS, after.partitions);

        // No output may mix the two parents: 3 children from each, each child wholly inside one parent's range.
        Map<SSTableReader, Integer> perParent = new HashMap<>();
        for (Map.Entry<String, SSTableReader> entry : after.owner.entrySet())
        {
            int source = after.token.get(entry.getKey()).compareTo(upper.getFirst().getToken()) < 0 ? 0 : 1;
            Integer previous = perParent.putIfAbsent(entry.getValue(), source);
            if (previous != null)
                assertEquals("output " + entry.getValue().descriptor + " mixes both parents",
                             previous.intValue(), source);
        }
        assertEquals("expected 6 outputs, 3 per parent", 6, perParent.size());
        assertEquals("expected 3 outputs from the lower parent", 3L,
                     perParent.values().stream().filter(v -> v == 0).count());

        assertReopenableAndVerifiable(cfs);
    }

    /**
     * The case {@code CompactionManager.zeroCopySplitOne}'s fallback branch exists for: an sstable the planner
     * declares ELIGIBLE, whose split then fails. Every other fallback case here fails in the PLANNER, before the
     * parent is carved into its own transaction, so none of them reaches that code.
     *
     * <p>Injected through {@code failBeforeChildForTesting}, and it has to be a hook rather than a corrupt file:
     * everything the split reads before it writes anything -- Statistics.db, Index.db, the compression metadata --
     * is read by the rewrite path too ({@code antiCompactGroup} calls {@code getApproximateKeyCount}, which loads
     * Statistics.db from disk), so corrupting any of it fails both paths and proves nothing about the fallback.
     * The hook throws before the SECOND child, so the failure lands with one child already written and open --
     * which is also what makes {@code cleanUp} run with a non-empty children list here.
     *
     * <p>What this pins: the fallback runs on the same transaction, the parent is obsoleted exactly once, no child
     * of the aborted split survives, no transaction log survives, and every partition lands in the bucket the
     * ranges say it belongs in. Skipping the sstable instead would leave data unrepaired that the repair session
     * believes is pending.
     */
    @Test
    public void splitFailureFallsBackToTheRewritePathOnTheSameTransaction() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        createCompressedTable("");
        insertPartitions();
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertSplittable("the sstable itself must be splittable, or the split cannot be what fails", parent);

        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        Collection<Range<Token>> full = Collections.singleton(rangeCovering(keys, 60, 140));
        RangesAtEndpoint ranges = rangesAtEndpoint(full, Collections.emptySet());

        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);
        Outputs before = collect(Collections.singleton(parent));
        assertEquals(PARTITIONS, before.partitions);

        TimeUUID sessionID = nextTimeUUID();

        // Non-vacuity: the planner must still say yes, or this degenerates into one of the planner-rejection cases.
        AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.plan(parent, ranges, sessionID);
        assertTrue("the planner must declare this eligible: " + plan, plan.eligible);
        assertEquals("U F U", 3, plan.runCount);

        AtomicInteger childrenStarted = new AtomicInteger();
        ZeroCopySSTableSplitter.failBeforeChildForTesting = alreadyBuilt -> {
            childrenStarted.incrementAndGet();
            if (alreadyBuilt == 1)
                throw new RuntimeException("injected mid-split failure");
        };
        try
        {
            anticompact(cfs, ranges, sessionID);
        }
        finally
        {
            ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
        }

        assertEquals("the injection must fire with exactly one child already built", 2, childrenStarted.get());
        assertEquals("a failed split must not mark the zero-copy metric", 0, zeroCopyBytes(cfs));
        // 2 = the rewrite result (one pending, one unrepaired); the split would have produced 3.
        Outputs after = assertOutcome(cfs, parent, before, full, Collections.emptySet(), sessionID, 2);
        assertEquals(PARTITIONS, after.partitions);

        LifecycleTransaction.waitForDeletions();
        for (File dir : cfs.getDirectories().getCFDirectories())
            for (File f : dir.tryList())
                assertFalse("a lifecycle transaction log survived: " + f, f.name().contains("_txn_"));

        assertReopenableAndVerifiable(cfs);
    }

    /**
     * An uncompressed sstable has no compression chunks to copy, so {@code isSupported} is false. Deliberately
     * <em>not</em> gated on {@link BigFormat#isSelected()}: the expectation ("not splittable, therefore
     * rewritten into two sstables") holds for BTI as well, so this is the one case that covers the fallback for
     * both formats.
     */
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
     * The same happy path as {@link #singleFullRunIsSplitZeroCopy}, on a BTI parent: the format is selectable on
     * trunk and is now supported, so the whole {@link #assertOutcome} oracle -- key-for-key data equality against
     * the parent, one child per run, and the per-partition repair state -- has to hold there too.
     *
     * <p>This is the highest-value test of BTI support, because it is the only one that goes through the real
     * {@code CompactionManager} path: the planner's walk, the split, the lifecycle transaction, the repair state
     * stamped into each child's Statistics.db, and {@code nodetool verify} on the result. Everything BTI-specific
     * in the split -- the rebuilt Partitions.db, the copied Rows.db and its patched positions -- is only correct
     * if the children read back identical to the parent afterwards.
     */
    @Test
    public void btiSSTableIsSplitZeroCopy() throws Throwable
    {
        Assume.assumeTrue("this build does not register the BTI format",
                          DatabaseDescriptor.getSSTableFormats().containsKey(BtiFormat.NAME));

        SSTableFormat<?, ?> savedFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        try
        {
            // the format is chosen globally at write time, so this has to happen before the flush
            TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(BtiFormat.NAME);

            createCompressedTable("");
            insertPartitions();
            flush();

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            SSTableReader parent = onlySSTable(cfs);
            assertTrue("the fixture did not produce a BTI sstable, so this test proves nothing",
                       BtiFormat.is(parent.descriptor.getFormat()));
            assertSplittable("a compressed BTI sstable must be splittable", parent);

            List<DecoratedKey> keys = keysInTokenOrder(cfs);
            Collection<Range<Token>> full = Collections.singleton(rangeCovering(keys, 60, 140));
            RangesAtEndpoint ranges = rangesAtEndpoint(full, Collections.emptySet());

            DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);

            TimeUUID sessionID = nextTimeUUID();

            AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.plan(parent, ranges, sessionID);
            assertTrue("the planner refused a BTI sstable: " + plan.ineligibleReason, plan.eligible);
            assertNull(plan.ineligibleReason);
            assertEquals(3, plan.runCount);

            Outputs before = collect(Collections.singleton(parent));
            assertEquals(PARTITIONS, before.partitions);

            anticompact(cfs, ranges, sessionID);

            assertTrue("the zero-copy split did not run: metric is " + zeroCopyBytes(cfs),
                       zeroCopyBytes(cfs) > 0);
            // 3 runs -> 3 children. The rewrite path would have produced 2 (one pending, one unrepaired).
            assertOutcome(cfs, parent, before, full, Collections.emptySet(), sessionID, 3);
            assertReopenableAndVerifiable(cfs);
        }
        finally
        {
            TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(savedFormat);
        }
    }

    /**
     * The kill switch. Exactly the eligible layout of {@link #singleFullRunIsSplitZeroCopy}, but with
     * {@code zero_copy_anticompaction_enabled = false}: the split must not run at all (not even the planner's
     * Index.db walk), and the outcome must be the unchanged rewrite result.
     */
    @Test
    public void killSwitchDisablesTheZeroCopyPath() throws Throwable
    {
        // BIG only: without a splittable sstable the kill switch would have nothing to switch off.
        Assume.assumeTrue(BigFormat.isSelected());
        createCompressedTable("");
        insertPartitions();
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertSplittable("with the flag on this sstable would be split", parent);

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
     * Pins DECISION 2 ("accept losing tombstone purge, unconditionally"): the zero-copy path copies compression
     * chunks verbatim and therefore RETAINS droppable tombstones and shadowed data that the rewriting
     * anticompaction would have purged. That is retention, never loss -- nothing can be resurrected -- and it
     * is deliberately not gated on the droppable-tombstone ratio.
     * <p>
     * The parent here carries a partition-level tombstone and a row-level tombstone that are genuinely
     * droppable at the moment the anticompaction runs ({@code gc_grace_seconds = 0}, and the run happens more
     * than a second after the deletes, so {@code localDeletionTime < gcBefore}) -- asserted via
     * {@link SSTableReader#getDroppableTombstonesBefore} so this test cannot pass vacuously. Both tombstones
     * must still be there afterwards.
     * <p>
     * If someone later "fixes" this by purging, this test is the record that the retention was intentional.
     */
    @Test
    public void purgeableTombstonesSurviveTheZeroCopySplit() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
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
        assertSplittable(parent);

        Collection<Range<Token>> full = Collections.singleton(rangeCovering(keys, 60, 140));
        RangesAtEndpoint ranges = rangesAtEndpoint(full, Collections.emptySet());

        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);
        Outputs before = collect(Collections.singleton(parent));
        assertEquals(PARTITIONS, before.partitions);
        DeletionTime parentTombstone = before.partitionDeletion.get(hexOf(deletedPartition));
        assertFalse("the fully deleted partition should carry a partition level tombstone in the parent",
                    parentTombstone.isLive());
        // gcBefore is computed from the wall clock when the anticompaction starts and purging requires
        // localDeletionTime < gcBefore (gc_grace_seconds is 0 here, so gcBefore == nowInSeconds), so the second
        // the deletes were written in has to be over before the run. Waited out on the CONDITION rather than for a
        // fixed 1100 ms: a sleep long enough to be safe is also long enough to be dead time in every run, and a
        // sleep that turns out not to be would make this test pass vacuously rather than fail.
        //
        // The condition is the exact predicate CompactionController's purge evaluator applies, deliberately not
        // SSTableReader.getDroppableTombstonesBefore: that reads estimatedTombstoneDropTime, and
        // StreamingTombstoneHistogramBuilder.update rounds every point UP to the next roundSeconds boundary
        // (ceilKey(point, roundSeconds), 60s by default per SSTable.java:81). A tombstone written a second ago
        // therefore lands in a future bucket and the estimate is legitimately 0 here, which says nothing about
        // whether the tombstone is actually droppable.
        awaitDroppable(parentTombstone.localDeletionTime());
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
    // The differential: the SAME input down BOTH paths, compared against each other
    // ----------------------------------------------------------------------------------------------------

    /**
     * The one comparison neither of the other oracles here can make. {@link #assertOutcome} and the fuzz test both
     * compare the outputs against the PARENT, which is structurally blind to any field a split inherits verbatim
     * while a rewrite recomputes it: the parent's value and the child's agree by construction, so the divergence
     * from what the rewrite would have written is invisible. This runs one byte-identical input through
     * {@code zero_copy_anticompaction_enabled = true} and then again with {@code false}, and compares the two sets
     * of outputs to each other -- contents first, then {@link StatsMetadata} field by field.
     *
     * <p><b>Why the shape is {@code F U} and not the usual {@code U F U}.</b> The two paths only produce
     * output-for-output comparable results when the run count equals the number of non-empty repair buckets:
     * {@code F U} is two children split and two sstables rewritten, holding the same two partition sets, so each
     * output has exactly one counterpart -- matched here by the hex of its first partition key.
     *
     * <p><b>What the fixture is built to expose.</b> Uniform data would make most of the inherited fields agree by
     * accident and the pin vacuous, so the divergences are placed deliberately: the global MINIMUM timestamp lives
     * in the unrepaired half and the global MAXIMUM in the full half (so each side inherits a bound that its own
     * partitions do not justify), the only partition-level tombstone and the only extended clustering live in the
     * full half, and {@code gc_grace_seconds} is 10 days so nothing is droppable and the rewrite is not entitled to
     * drop anything the copy retains. The partition tombstone is written at a timestamp BELOW every row it covers,
     * so it shadows nothing: were it above, the rewrite would legitimately drop the shadowed rows and the content
     * comparison would be comparing two different things.
     *
     * <p><b>The contract.</b> Three disjoint sets of field names below, and a reflective completeness check that
     * every public field of {@link StatsMetadata} is in exactly one of them -- so a field added later cannot slip
     * through unclassified. {@code EQUAL_ACROSS_PATHS} is asserted equal output-for-output;
     * {@code INHERITED_BY_THE_SPLIT} is asserted equal to the PARENT's value, which is the actual contract
     * ("verbatim"), and the accepted divergence from the rewrite is then asserted explicitly, one field at a time,
     * so the direction of the error is pinned and not merely tolerated; {@code DERIVED_PER_OUTPUT} is recomputed by
     * both paths from different raw material and is checked in the shape it is meaningful in.
     */
    @Test
    public void bothPathsProduceTheSameDataAndAPinnedStatsMetadataDifference() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        // The SAME session id for both runs, so pendingRepair is comparable field-for-field below; each run
        // registers and removes it around its own anticompaction.
        TimeUUID sessionID = nextTimeUUID();
        Differential split = runDifferential(true, sessionID);
        Differential rewrite = runDifferential(false, sessionID);

        assertTrue("the zero-copy run did not take the zero-copy path", split.zeroCopyBytes > 0);
        assertEquals("the control run took the zero-copy path", 0, rewrite.zeroCopyBytes);

        // ---- (1) the two paths produced the same data, partition for partition ----
        assertEquals("the two runs did not start from the same parent, so nothing below compares anything",
                     rewrite.parentContent, split.parentContent);
        assertEquals("the two paths do not hold the same partition keys",
                     rewrite.content.keySet(), split.content.keySet());
        for (Map.Entry<String, String> entry : rewrite.content.entrySet())
            assertEquals("partition " + entry.getKey() + " differs between the two paths",
                         entry.getValue(), split.content.get(entry.getKey()));

        // ---- (2) the same outputs, matched by first key ----
        assertEquals("the two paths did not produce output sstables over the same key ranges",
                     rewrite.statsByFirstKey.keySet(), split.statsByFirstKey.keySet());
        assertEquals("the F U shape must give both paths exactly two outputs", 2, split.statsByFirstKey.size());

        // ---- (3) every StatsMetadata field is classified ----
        Set<String> classified = new HashSet<>();
        classified.addAll(STATS_EQUAL_ACROSS_PATHS);
        classified.addAll(STATS_INHERITED_BY_THE_SPLIT);
        classified.addAll(STATS_INHERITED_COMPARED_BY_RENDERING);
        classified.addAll(STATS_DERIVED_PER_OUTPUT);
        for (Field field : StatsMetadata.class.getFields())
        {
            if (Modifier.isStatic(field.getModifiers()))
                continue;
            assertTrue("StatsMetadata." + field.getName() + " is new and unclassified: decide whether a zero-copy" +
                       " child must match what the rewrite writes, inherits the parent's value verbatim, or derives" +
                       " its own, and add it to the matching set in this test",
                       classified.contains(field.getName()));
        }

        for (String firstKey : rewrite.statsByFirstKey.keySet())
        {
            StatsMetadata splitStats = split.statsByFirstKey.get(firstKey);
            StatsMetadata rewriteStats = rewrite.statsByFirstKey.get(firstKey);
            String where = "output starting at " + firstKey + ": ";

            for (String name : STATS_EQUAL_ACROSS_PATHS)
                assertEquals(where + "StatsMetadata." + name + " must be identical whichever path ran",
                             statsField(rewriteStats, name), statsField(splitStats, name));

            for (String name : STATS_INHERITED_BY_THE_SPLIT)
                assertEquals(where + "StatsMetadata." + name + " must be inherited from the parent VERBATIM -- the" +
                             " copied rows encode their timestamps and columns against it, so tightening it" +
                             " corrupts them with every CRC still passing",
                             statsField(split.parentStats, name), statsField(splitStats, name));

            for (String name : STATS_INHERITED_COMPARED_BY_RENDERING)
                assertEquals(where + "StatsMetadata." + name + " must be inherited from the parent VERBATIM (compared"
                             + " by rendering; see STATS_INHERITED_COMPARED_BY_RENDERING for why not by equals)",
                             String.valueOf(statsField(split.parentStats, name)),
                             String.valueOf(statsField(splitStats, name)));

            // estimatedPartitionSize: both paths derive it exactly, from the parent's index positions and from the
            // bytes written respectively. Pinned on the partition count and the mean rather than on the histogram,
            // since the two measurements can legitimately land in different buckets at the margin.
            assertEquals(where + "estimatedPartitionSize must count every partition of the output",
                         rewriteStats.estimatedPartitionSize.count(), splitStats.estimatedPartitionSize.count());
            assertTrue(where + "estimatedPartitionSize means disagree by more than a bucket: rewrite="
                       + rewriteStats.estimatedPartitionSize.mean() + " split=" + splitStats.estimatedPartitionSize.mean(),
                       Math.abs(rewriteStats.estimatedPartitionSize.mean() - splitStats.estimatedPartitionSize.mean())
                       <= Math.max(64, rewriteStats.estimatedPartitionSize.mean() / 50));
            assertTrue(where + "estimatedPartitionSize must be per-child, not the parent's",
                       splitStats.estimatedPartitionSize.count() < split.parentStats.estimatedPartitionSize.count());

            // compressionRatio: onDiskLength/dataLength of a CHUNK-ALIGNED child, which carries a dead prefix and
            // whichever bytes of the two boundary chunks it shares. Sane, never the parent's verbatim value.
            assertTrue(where + "compressionRatio is not a ratio: " + splitStats.compressionRatio,
                       splitStats.compressionRatio > 0 && Double.isFinite(splitStats.compressionRatio));

            // tokenSpaceCoverage: DELIBERATELY not inherited. The parent's coverage is its whole token range, and
            // handing it to K children would multiply the table's apparent coverage and mislead the density
            // calculations that drive compaction; recomputing it would need the local ranges, so a split writes
            // MetadataCollector's "unknown" instead. The rewrite goes through SSTableWriter, which computes it.
            assertTrue(where + "a zero-copy child must report tokenSpaceCoverage as unknown, got "
                       + splitStats.tokenSpaceCoverage, Double.isNaN(splitStats.tokenSpaceCoverage));
            // And so does the rewrite: SSTableWriter.setTokenSpaceCoverage has exactly one caller in the tree,
            // ShardTracker, on the UCS sharded-writer path. createWriterForAntiCompaction does not go through it, so
            // MetadataCollector's NaN default survives on BOTH paths. The split's NaN above is therefore the
            // deliberate choice it looks like, but it is not a DIVERGENCE from the rewrite -- asserting one here
            // would be asserting something no anticompaction does.
            assertTrue(where + "neither anticompaction path sets tokenSpaceCoverage, got "
                       + rewriteStats.tokenSpaceCoverage, Double.isNaN(rewriteStats.tokenSpaceCoverage));

            // A split's children stop their dataLength at the last byte of their last partition and their dead
            // prefix sits BEFORE the first indexed position, so neither breaks the invariant the marker exists to
            // flag. Marking one would send every read of it through the index instead of a linear scan.
            assertFalse(where + "a split must not invent an unindexed region", splitStats.hasUnindexedRegions);
            assertFalse(where + "the rewrite path never marks one", rewriteStats.hasUnindexedRegions);
        }

        // ---- (4) the accepted divergences, spelled out and directed ----
        StatsMetadata parent = split.parentStats;
        StatsMetadata splitFull = split.statsByFirstKey.get(split.fullFirstKey);
        StatsMetadata splitUnrepaired = split.statsByFirstKey.get(split.unrepairedFirstKey);
        StatsMetadata rewriteFull = rewrite.statsByFirstKey.get(split.fullFirstKey);
        StatsMetadata rewriteUnrepaired = rewrite.statsByFirstKey.get(split.unrepairedFirstKey);

        // ACCEPTED: absolute per-sstable TOTALS cannot be recomputed without deserialising rows, which is the whole
        // cost this path exists to avoid, so every child carries the PARENT-WIDE figure. Conservative in direction
        // (never smaller than the truth): per-table aggregates over-report by ~K and worthDroppingTombstones
        // under-fires by ~K until the children are compacted normally.
        assertEquals("totalRows must be the parent's in every child", parent.totalRows, splitFull.totalRows);
        assertEquals("totalRows must be the parent's in every child", parent.totalRows, splitUnrepaired.totalRows);
        assertTrue("the fixture no longer splits the rows between the outputs, so the totals pin is vacuous",
                   rewriteFull.totalRows < parent.totalRows && rewriteUnrepaired.totalRows < parent.totalRows);
        assertEquals(parent.totalColumnsSet, splitFull.totalColumnsSet);
        assertTrue("totalColumnsSet is over-reported by design; if the rewrite agrees, the pin is vacuous",
                   rewriteUnrepaired.totalColumnsSet < parent.totalColumnsSet);
        assertEquals("estimatedCellPerPartitionCount must be the parent's, i.e. cover every partition",
                     parent.estimatedCellPerPartitionCount.count(),
                     splitUnrepaired.estimatedCellPerPartitionCount.count());
        assertTrue("estimatedCellPerPartitionCount is over-reported by design",
                   rewriteUnrepaired.estimatedCellPerPartitionCount.count()
                   < parent.estimatedCellPerPartitionCount.count());

        // ACCEPTED: the tombstone histogram and the deletion-time bounds are parent-wide too, so a child holding no
        // tombstone at all still advertises the parent's. That is what keeps a fully expired child from being
        // dropped whole by getFullyExpiredSSTables and puts every child in the parent's TWCS window.
        assertTrue("the fixture must put a tombstone in the full half only", parent.estimatedTombstoneDropTime.size() > 0);
        assertEquals("estimatedTombstoneDropTime must be inherited even where there is no tombstone",
                     parent.estimatedTombstoneDropTime, splitUnrepaired.estimatedTombstoneDropTime);
        assertEquals("the rewrite path must see no tombstone in the unrepaired half; if it does, the pin is vacuous",
                     0, rewriteUnrepaired.estimatedTombstoneDropTime.size());
        // MIN, not max: MetadataCollector feeds Cell.NO_DELETION_TIME (Long.MAX_VALUE) into the tracker for every
        // live row, so maxLocalDeletionTime is that sentinel in any sstable holding live data and cannot be made to
        // diverge without an all-tombstone child. The min is the tombstone's own localDeletionTime in the parent and
        // the sentinel in a half that holds none, which is the same accepted imprecision, observably.
        assertEquals("minLocalDeletionTime must be inherited, not recomputed",
                     parent.minLocalDeletionTime, splitUnrepaired.minLocalDeletionTime);
        assertNotEquals("the rewrite path recomputes minLocalDeletionTime; if it agrees, the pin is vacuous",
                        parent.minLocalDeletionTime, rewriteUnrepaired.minLocalDeletionTime);
        assertEquals("maxLocalDeletionTime must be inherited too, even though this fixture cannot make it diverge",
                     parent.maxLocalDeletionTime, splitUnrepaired.maxLocalDeletionTime);
        assertTrue("hasPartitionLevelDeletions must be inherited (conservative direction)",
                   splitUnrepaired.hasPartitionLevelDeletions);
        assertFalse("the rewrite path must see no partition deletion in the unrepaired half",
                    rewriteUnrepaired.hasPartitionLevelDeletions);

        // MANDATORY, not merely accepted: the copied rows encode timestamps as unsigned vint deltas off
        // minTimestamp/minLocalDeletionTime/minTTL and their columns as a bitmap subset of the header's, so these
        // must be the parent's bit for bit. The fixture puts the global minimum in the unrepaired half and the
        // global maximum in the full half, so each of the two children inherits a bound its own rows do not justify.
        assertEquals(parent.minTimestamp, splitFull.minTimestamp);
        assertNotEquals("the fixture must put the global minimum timestamp outside the full half",
                        parent.minTimestamp, rewriteFull.minTimestamp);
        assertEquals(parent.maxTimestamp, splitUnrepaired.maxTimestamp);
        assertNotEquals("the fixture must put the global maximum timestamp outside the unrepaired half",
                        parent.maxTimestamp, rewriteUnrepaired.maxTimestamp);

        // ACCEPTED: coveredClustering is inherited as a superset of the child's own.
        assertEquals(parent.coveredClustering, splitUnrepaired.coveredClustering);
        assertNotEquals("the fixture must put the widest clustering outside the unrepaired half",
                        parent.coveredClustering, rewriteUnrepaired.coveredClustering);
    }

    /**
     * Fields a zero-copy child must carry identically to what the rewrite would have written. Anything here that
     * starts diverging is a bug, not a trade.
     */
    private static final Set<String> STATS_EQUAL_ACROSS_PATHS =
        ImmutableSet.of("repairedAt", "pendingRepair", "isTransient",   // the whole point of an anticompaction
                        "sstableLevel",                                 // single input, so both keep the parent's
                        "hasLegacyCounterShards",
                        "minTTL", "maxTTL",
                        "firstKey", "lastKey");                         // outrank Summary.db once version.hasKeyRange()

    /**
     * Fields a zero-copy child inherits from the parent VERBATIM. For the three encoding bases
     * ({@code minTimestamp}, {@code minLocalDeletionTime}, {@code minTTL} -- the last of which is in the set above
     * because this fixture writes no TTL) that is mandatory: the copied rows are deltas off them. For the rest it is
     * the accepted imprecision, always in the conservative direction. {@code commitLogIntervals} and
     * {@code originatingHostId} are here as an ATOMIC PAIR: stamping the child with the local host id while
     * inheriting a foreign parent's intervals would make CommitLogReplayer read foreign segment ids against the
     * local commitlog and discard acked-but-unflushed mutations.
     */
    private static final Set<String> STATS_INHERITED_BY_THE_SPLIT =
        ImmutableSet.of("minTimestamp", "maxTimestamp", "minLocalDeletionTime", "maxLocalDeletionTime",
                        "estimatedCellPerPartitionCount", "estimatedTombstoneDropTime",
                        "totalColumnsSet", "totalRows",
                        "coveredClustering", "hasPartitionLevelDeletions",
                        "originatingHostId",
                        // Inherited, deliberately NOT written as a literal false: a split adds no unindexed region
                        // of its own but cannot remove one the parent already had (a sliced sstable received by
                        // partial zero-copy streaming), and clearing the marker hands the child to the linear
                        // scanner. Asserted false for both paths below, this fixture's parent being a flush.
                        "hasUnindexedRegions",
                        "encodingStats");   // a view of minTimestamp/minLocalDeletionTime/minTTL, so it follows them

    /** Fields both paths derive for themselves, from different raw material; compared in the shape that means something. */
    private static final Set<String> STATS_DERIVED_PER_OUTPUT =
        ImmutableSet.of("estimatedPartitionSize", "compressionRatio", "tokenSpaceCoverage");

    /**
     * Inherited verbatim like {@link #STATS_INHERITED_BY_THE_SPLIT}, but compared by rendering rather than by
     * {@code equals}, because their {@code equals} is not round-trip safe and would fail on values that are in fact
     * identical.
     * <p>
     * {@code commitLogIntervals} is an {@code IntervalSet<CommitLogPosition>}, and {@code CommitLogPosition.equals}
     * tests {@code getClass() != o.getClass()}. A freshly flushed sstable's interval END is a
     * {@link org.apache.cassandra.db.memtable.Memtable.LastCommitLogPosition} -- a SUBCLASS -- while the same value
     * read back from Statistics.db deserialises as a plain {@code CommitLogPosition}. So parent-in-memory and
     * child-on-disk compare unequal with byte-identical contents and indistinguishable {@code toString}s. That is a
     * pre-existing asymmetry in {@code CommitLogPosition}, not something this feature introduced, and it means
     * {@code StatsMetadata.equals} is unreliable for any freshly flushed sstable compared against its own
     * deserialised form. Compared here on the rendering, which is exactly as strong for "was it inherited verbatim".
     */
    private static final Set<String> STATS_INHERITED_COMPARED_BY_RENDERING = ImmutableSet.of("commitLogIntervals");

    private static Object statsField(StatsMetadata stats, String name)
    {
        try
        {
            return StatsMetadata.class.getField(name).get(stats);
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError("StatsMetadata." + name + " is named in this test but not a public field", e);
        }
    }

    /** One run of the differential fixture: everything retained as immutable values, so it outlives its readers. */
    private static final class Differential
    {
        StatsMetadata parentStats;
        /** hex partition key -&gt; timestamp-stable rendering, of the parent and of the outputs. */
        final Map<String, String> parentContent = new HashMap<>();
        final Map<String, String> content = new HashMap<>();
        /** hex of the output's FIRST partition key -&gt; its stats; the pairing between the two runs. */
        final Map<String, StatsMetadata> statsByFirstKey = new HashMap<>();
        String fullFirstKey;
        String unrepairedFirstKey;
        long zeroCopyBytes;
    }

    /** The differential fixture. Byte-identical between the two runs: fixed seed, fixed timestamps, one session id. */
    private Differential runDifferential(boolean zeroCopyEnabled, TimeUUID sessionID) throws Throwable
    {
        // gc_grace_seconds far in the future: nothing this writes can become droppable, so the rewrite path is not
        // entitled to drop anything the verbatim copy retains and the content comparison is between like and like.
        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}" +
                    " AND gc_grace_seconds = 864000");
        disableCompaction();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        Random rnd = new Random(DIFFERENTIAL_SEED);
        for (int p = 0; p < PARTITIONS; p++)
            for (int c = 0; c < ROWS_PER_PARTITION; c++)
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?) USING TIMESTAMP ?",
                        key(p), c, fixedText(rnd, VALUE_BYTES), DIFFERENTIAL_TS);

        // The three deliberate asymmetries. Every (pk, ck) below is fresh, and the partition tombstone is written
        // BELOW the timestamp of every row it covers, so nothing is shadowed and neither path may drop anything.
        String fullSideRow = keyOf(keys.get(10));
        String fullSideTombstone = keyOf(keys.get(20));
        String unrepairedSideRow = keyOf(keys.get(150));
        execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?) USING TIMESTAMP ?",
                fullSideRow, 99, "widest-clustering-and-newest-timestamp", DIFFERENTIAL_TS + 10_000_000L);
        execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?) USING TIMESTAMP ?",
                unrepairedSideRow, 50, "oldest-timestamp", DIFFERENTIAL_TS - 10_000_000L);
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", DIFFERENTIAL_TS - 1, fullSideTombstone);
        flush();

        SSTableReader parent = onlySSTable(cfs);
        assertSplittable("the differential is meaningless if the split cannot run", parent);

        // F U: FULL over the token prefix through keys[99], unrepaired above it. Two runs, hence two children, and
        // two non-empty rewrite buckets holding the same two partition sets -- the only shape in which the outputs
        // of the two paths can be matched one for one.
        Collection<Range<Token>> full =
            Collections.singleton(new Range<>(cfs.getPartitioner().getMinimumToken(), keys.get(99).getToken()));
        RangesAtEndpoint ranges = rangesAtEndpoint(full, Collections.emptySet());

        Differential out = new Differential();
        out.parentStats = parent.getSSTableMetadata();
        readStable(parent, out.parentContent);

        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(zeroCopyEnabled);
        AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.plan(parent, ranges, sessionID);
        assertTrue("the F U shape must be eligible or there is no differential: " + plan, plan.eligible);
        assertEquals("F U", 2, plan.runCount);

        anticompact(cfs, ranges, sessionID);
        LifecycleTransaction.waitForDeletions();

        out.zeroCopyBytes = zeroCopyBytes(cfs);
        assertEquals("the F U shape must give exactly two outputs on either path", 2, cfs.getLiveSSTables().size());
        for (SSTableReader output : cfs.getLiveSSTables())
        {
            Map<String, String> partitions = new HashMap<>();
            readStable(output, partitions);
            String firstKey = ByteBufferUtil.bytesToHex(output.getFirst().getKey());
            assertNull("two outputs claim the same first key", out.statsByFirstKey.put(firstKey,
                                                                                      output.getSSTableMetadata()));
            for (Map.Entry<String, String> entry : partitions.entrySet())
                assertNull("partition " + entry.getKey() + " is in two outputs at once",
                           out.content.put(entry.getKey(), entry.getValue()));
            if (output.isPendingRepair())
                out.fullFirstKey = firstKey;
            else
                out.unrepairedFirstKey = firstKey;
        }
        assertNotNull("no pending-repair output; the routing changed and the differential means nothing",
                      out.fullFirstKey);
        assertNotNull("no unrepaired output", out.unrepairedFirstKey);
        return out;
    }

    /**
     * A rendering that is stable between two runs of the same fixture, unlike {@link #describe}: it drops the
     * {@code localDeletionTime} of the partition tombstone, which is the wall clock second the DELETE was executed
     * in and therefore differs between the two runs. Everything else -- {@code markedForDeleteAt}, the static row,
     * every row, every cell and every cell timestamp -- is explicit and compared.
     */
    private static void readStable(SSTableReader sstable, Map<String, String> into)
    {
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    StringBuilder sb = new StringBuilder();
                    sb.append("markedForDeleteAt=").append(partition.partitionLevelDeletion().markedForDeleteAt());
                    sb.append(" static=").append(partition.staticRow().toString(sstable.metadata(), true));
                    while (partition.hasNext())
                        sb.append('\n').append(partition.next().toString(sstable.metadata(), true));
                    into.put(ByteBufferUtil.bytesToHex(partition.partitionKey().getKey()), sb.toString());
                }
            }
        }
    }

    private static final long DIFFERENTIAL_SEED = 0x5EED_1234L;
    private static final long DIFFERENTIAL_TS = 1_600_000_000_000_000L;

    /** Near-incompressible but deterministic, so the two runs build byte-identical sstables. */
    private static String fixedText(Random rnd, int length)
    {
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = (char) ('!' + rnd.nextInt(94));
        return new String(chars);
    }

    // ----------------------------------------------------------------------------------------------------
    // Cancellation: the repair session going away, which no CompactionInfo.Holder can see
    // ----------------------------------------------------------------------------------------------------

    /**
     * A session cancelled DURING the planning walk. The walk is the first thing in an anticompaction that takes real
     * time and it is deliberately not registered with {@code ActiveCompactions} -- it writes nothing, so it must not
     * be credited bytes or reserve disk against every other compaction and stream -- which is exactly why it has to
     * answer the session's own predicate instead.
     * <p>
     * The predicate returns false once (for {@code zeroCopyAntiCompact}'s between-sstables gate) and true from then
     * on, so the flip lands inside the walk, at its first check point. The plan comes back
     * {@link AntiCompactionRunPlanner.Plan#interrupted}, the sstable and the rest of the group stay in the group
     * transaction, and {@code antiCompactGroup} -- whose iterator ORs the same predicate into
     * {@code isStopRequested()} -- raises the interruption the coordinator is owed. What must NOT happen is a
     * fallback rewrite that commits children for a session that has already failed and then reports success.
     */
    @Test
    public void cancellationDuringPlanningLeavesTheGroupToTheRewritePathAndInterrupts() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        createCompressedTable("");
        insertPartitions();
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertSplittable(parent);

        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        Collection<Range<Token>> full = Collections.singleton(rangeCovering(keys, 60, 140));
        RangesAtEndpoint ranges = rangesAtEndpoint(full, Collections.emptySet());
        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);

        TimeUUID sessionID = nextTimeUUID();
        // Non-vacuity: uncancelled, this sstable really would be split.
        assertTrue(AntiCompactionRunPlanner.plan(parent, ranges, sessionID).eligible);

        AtomicInteger asked = new AtomicInteger();
        assertThatThrownBy(() -> anticompact(cfs, ranges, sessionID, () -> asked.incrementAndGet() > 1))
            .isInstanceOf(CompactionInterruptedException.class);

        assertTrue("the planning walk must have consulted the predicate", asked.get() > 1);
        assertEquals("nothing may be credited to the zero-copy metric for a cancelled session",
                     0, zeroCopyBytes(cfs));
        assertTrue("the parent must still be live and unmodified", cfs.getLiveSSTables().contains(parent));
        assertFalse(parent.isMarkedCompacted());
        assertNoPendingRepairPublished(cfs, sessionID);
        // waits for deletions and proves no child of the abandoned attempt survived
        Util.assertOnDiskState(cfs, 1);
        assertNoTransactionLogs(cfs);
    }

    /**
     * A session cancelled DURING the copy, i.e. after the plan was accepted and while chunks are moving. The split
     * has to abort rather than finish: without this a 400 GiB split of a session that failed in its first minute
     * would run to completion and then publish children stamped with that dead session's {@code pendingRepair},
     * handing them to a {@code PendingRepairManager} for a session that will never finalize them.
     * <p>
     * Deliberately NOT answered with a fallback rewrite, which is why this asserts the exception escapes: the
     * operator (or the coordinator) asked for the work to STOP, not to be done a more expensive way. The flip is
     * driven off the splitter's own injection hook so it lands inside the copy deterministically, one child in.
     */
    @Test
    public void cancellationDuringTheCopyAbortsAndPublishesNothing() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        Fixture fixture = eligibleFixture();
        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);
        TimeUUID sessionID = nextTimeUUID();
        assertTrue(AntiCompactionRunPlanner.plan(fixture.parent, fixture.ranges, sessionID).eligible);

        AtomicInteger childrenStarted = new AtomicInteger();
        AtomicBoolean cancelled = new AtomicBoolean();
        ZeroCopySSTableSplitter.failBeforeChildForTesting = alreadyBuilt -> {
            childrenStarted.incrementAndGet();
            if (alreadyBuilt == 1)
                cancelled.set(true);   // the session dies with one child already written and open
        };

        assertThatThrownBy(() -> anticompact(fixture.cfs, fixture.ranges, sessionID, cancelled::get))
            .isInstanceOf(CompactionInterruptedException.class);

        assertTrue("the injection never fired, so nothing was cancelled mid-copy", childrenStarted.get() >= 2);
        assertEquals("an aborted split must not mark the zero-copy metric", 0, zeroCopyBytes(fixture.cfs));
        assertTrue("the parent must still be live", fixture.cfs.getLiveSSTables().contains(fixture.parent));
        assertNoPendingRepairPublished(fixture.cfs, sessionID);
        Util.assertOnDiskState(fixture.cfs, 1);
        assertNoTransactionLogs(fixture.cfs);
    }

    /**
     * The window the split's own periodic check cannot cover: between its last chunk and the {@code tracker.apply}
     * that publishes the children. {@code zeroCopySplitOne} re-asks the predicate there, and this pins that it does.
     * <p>
     * The flip is timed exactly, without a sleep or a guessed call count, by keying it on the split's own
     * {@link CompactionInfo.Holder}: the holder is registered with {@code active} around the copy ONLY, so
     * "registered at least once, and no longer registered" is true at precisely one consultation -- the re-check
     * after {@code split()} returned and {@code finishCompaction} ran. During planning the holder was never there
     * (that walk registers nothing on purpose) and during the copy it is, so neither of those is affected.
     */
    @Test
    public void cancellationBetweenTheCopyAndThePublishDiscardsTheChildren() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        Fixture fixture = eligibleFixture();
        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);
        TimeUUID sessionID = nextTimeUUID();
        assertTrue(AntiCompactionRunPlanner.plan(fixture.parent, fixture.ranges, sessionID).eligible);

        AtomicBoolean sawTheCopyRegistered = new AtomicBoolean();
        AtomicInteger flips = new AtomicInteger();
        BooleanSupplier cancelAfterTheCopy = () -> {
            // getCompactionsForSSTable returns null, not an empty collection, when nothing matches
            Collection<CompactionInfo> running = CompactionManager.instance.active
                                                .getCompactionsForSSTable(fixture.parent,
                                                                          OperationType.ANTICOMPACTION);
            if (running != null && !running.isEmpty())
            {
                sawTheCopyRegistered.set(true);
                return false;
            }
            if (!sawTheCopyRegistered.get())
                return false;
            flips.incrementAndGet();
            return true;
        };

        assertThatThrownBy(() -> anticompact(fixture.cfs, fixture.ranges, sessionID, cancelAfterTheCopy))
            .isInstanceOf(CompactionInterruptedException.class);

        assertTrue("the copy was never registered with ActiveCompactions, so the flip cannot have been timed to it",
                   sawTheCopyRegistered.get());
        assertEquals("the cancellation should have been answered exactly once, at the pre-publish re-check",
                     1, flips.get());
        assertEquals(0, zeroCopyBytes(fixture.cfs));
        assertTrue("the parent must still be live", fixture.cfs.getLiveSSTables().contains(fixture.parent));
        assertNoPendingRepairPublished(fixture.cfs, sessionID);
        Util.assertOnDiskState(fixture.cfs, 1);
        assertNoTransactionLogs(fixture.cfs);
    }

    // ----------------------------------------------------------------------------------------------------
    // The fallback classifier: which failures may be answered by rewriting, and which may not
    // ----------------------------------------------------------------------------------------------------

    /**
     * A failure that says the NODE is in trouble must fail the anticompaction, not be answered with a slower second
     * attempt. This is a deliberate behaviour change and the reason it needs pinning: the previous shape ran the
     * throwable through {@code JVMStabilityInspector.inspectThrowable} -- which does not merely classify, it
     * EXECUTES the disk failure policy -- and then fell back and reported success. So under the default {@code stop}
     * policy an {@code FSError} would take this node's transports down and {@code doAntiCompaction} would still log
     * "Anticompaction completed successfully", because the rewrite reads far less of the sstable than the planning
     * walk does and very possibly succeeds.
     * <p>
     * The whole cause chain is walked, so the wrapped case is here too; {@code Error} is in the list because
     * Cassandra runs with {@code -ea} and an {@code AssertionError} from a broken invariant is not a local refusal
     * either. The contrasting half -- an ordinary retryable refusal that MUST still fall back -- is
     * {@link #plannerFailureThatIsALocalRefusalStillFallsBackToTheRewritePath} and
     * {@link #splitFailureFallsBackToTheRewritePathOnTheSameTransaction}.
     * <p>
     * Injected through the cancellation predicate, which is the only lever into the walk that does not mean
     * corrupting a file the live reader may have mapped. The walk really does raise
     * {@code CorruptSSTableException} of its own accord: {@code walkBigIndex} wraps every {@code IOException} in one.
     */
    @Test
    public void plannerFailureThatIsNotALocalRefusalFailsTheAnticompaction() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        for (Throwable injected : nodeLevelFailures())
        {
            Fixture fixture = eligibleFixture();
            DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);
            TimeUUID sessionID = nextTimeUUID();
            assertTrue(AntiCompactionRunPlanner.plan(fixture.parent, fixture.ranges, sessionID).eligible);

            // False for the between-sstables gate, thrown ONCE from inside the walk, false ever after -- so a
            // regression that fell back would produce a clean rewrite and fail this as "no exception thrown",
            // rather than throwing again out of antiCompactGroup's own stop check and looking like a pass.
            AtomicInteger asked = new AtomicInteger();
            assertThatThrownBy(() -> anticompact(fixture.cfs, fixture.ranges, sessionID, () -> {
                if (asked.incrementAndGet() == 2)
                    throw sneak(injected);
                return false;
            })).describedAs("a %s out of the planning walk must fail the anticompaction, not fall back",
                            injected.getClass().getSimpleName())
               .isSameAs(injected);

            assertEquals(injected + ": nothing may be credited to the zero-copy metric",
                         0, zeroCopyBytes(fixture.cfs));
            assertTrue(injected + ": the parent must still be live", fixture.cfs.getLiveSSTables()
                                                                               .contains(fixture.parent));
            assertNoPendingRepairPublished(fixture.cfs, sessionID);
            Util.assertOnDiskState(fixture.cfs, 1);
            assertNoTransactionLogs(fixture.cfs);
        }
    }

    /** The same classifier, on the other side of the plan: a failure while the children are being written. */
    @Test
    public void splitFailureThatIsNotALocalRefusalFailsTheAnticompaction() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        for (Throwable injected : nodeLevelFailures())
        {
            Fixture fixture = eligibleFixture();
            DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);
            TimeUUID sessionID = nextTimeUUID();
            assertTrue(AntiCompactionRunPlanner.plan(fixture.parent, fixture.ranges, sessionID).eligible);

            ZeroCopySSTableSplitter.failBeforeChildForTesting = alreadyBuilt -> {
                if (alreadyBuilt == 1)
                    throw sneak(injected);
            };
            try
            {
                assertThatThrownBy(() -> anticompact(fixture.cfs, fixture.ranges, sessionID, () -> false))
                    .describedAs("a %s out of the copy must fail the anticompaction, not fall back",
                                 injected.getClass().getSimpleName())
                    .isSameAs(injected);
            }
            finally
            {
                ZeroCopySSTableSplitter.failBeforeChildForTesting = null;
            }

            assertEquals(injected + ": a failed split must not mark the zero-copy metric",
                         0, zeroCopyBytes(fixture.cfs));
            assertTrue(injected + ": the parent must still be live",
                       fixture.cfs.getLiveSSTables().contains(fixture.parent));
            assertNoPendingRepairPublished(fixture.cfs, sessionID);
            Util.assertOnDiskState(fixture.cfs, 1);
            assertNoTransactionLogs(fixture.cfs);
        }
    }

    /**
     * The other half of the classifier, and the one that keeps repairs working: an ordinary, local, retryable
     * refusal out of the PLANNER is still answered by rewriting the sstable. Skipping it instead would leave data
     * unrepaired that the repair session believes is pending, and failing the whole anticompaction over it would
     * turn a recoverable hiccup into a failed repair.
     */
    @Test
    public void plannerFailureThatIsALocalRefusalStillFallsBackToTheRewritePath() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        Fixture fixture = eligibleFixture();
        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);
        Outputs before = collect(Collections.singleton(fixture.parent));
        assertEquals(PARTITIONS, before.partitions);

        TimeUUID sessionID = nextTimeUUID();
        assertTrue(AntiCompactionRunPlanner.plan(fixture.parent, fixture.ranges, sessionID).eligible);

        // Thrown once, from inside the walk; false afterwards, so the fallback rewrite -- which is handed the same
        // predicate as its stop check -- is not tripped by the injection meant for the planner.
        AtomicInteger asked = new AtomicInteger();
        anticompact(fixture.cfs, fixture.ranges, sessionID, () -> {
            if (asked.incrementAndGet() == 2)
                throw new IllegalStateException("injected local refusal");
            return false;
        });
        assertTrue("the injection never fired, so nothing fell back", asked.get() > 2);

        assertEquals("a failed plan must not mark the zero-copy metric", 0, zeroCopyBytes(fixture.cfs));
        // 2 = the rewrite result (one pending, one unrepaired); the split would have produced 3.
        assertOutcome(fixture.cfs, fixture.parent, before, fixture.full, Collections.emptySet(), sessionID, 2);
    }

    /**
     * The classifier's input set: every category {@code mayFallBackToRewrite} refuses to answer with a rewrite, plus
     * one wrapped, because being wrapped is not a reprieve. {@code FSError} extends {@code IOError} and so is
     * already an {@code Error}; it is listed separately anyway, since that is an implementation detail of the JDK
     * hierarchy and not something this contract should rest on.
     */
    private static List<Throwable> nodeLevelFailures()
    {
        return Arrays.asList(new CorruptSSTableException(new IOException("injected bad sector"), "Data.db"),
                             new FSWriteError(new IOException("injected ENOSPC"), "Data.db"),
                             new AssertionError("injected broken invariant"),
                             new RuntimeException("wrapper",
                                                 new CorruptSSTableException(new IOException("injected"),
                                                                            "Index.db")));
    }

    /** Rethrows any throwable from a lambda whose functional interface declares none. */
    private static RuntimeException sneak(Throwable t)
    {
        return ZeroCopyAntiCompactionTest.sneakyThrow(t);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> RuntimeException sneakyThrow(Throwable t) throws T
    {
        throw (T) t;
    }

    /** The eligible {@code U F U} fixture every failure-injection case above starts from. */
    private static final class Fixture
    {
        ColumnFamilyStore cfs;
        SSTableReader parent;
        Collection<Range<Token>> full;
        RangesAtEndpoint ranges;
    }

    private Fixture eligibleFixture() throws Throwable
    {
        createCompressedTable("");
        insertPartitions();
        flush();

        Fixture fixture = new Fixture();
        fixture.cfs = getCurrentColumnFamilyStore();
        fixture.parent = onlySSTable(fixture.cfs);
        assertSplittable(fixture.parent);
        List<DecoratedKey> keys = keysInTokenOrder(fixture.cfs);
        fixture.full = Collections.singleton(rangeCovering(keys, 60, 140));
        fixture.ranges = rangesAtEndpoint(fixture.full, Collections.emptySet());
        return fixture;
    }

    /**
     * Nothing may be visible under {@code sessionID}: a child committed for a session that has failed or been
     * cancelled is handed to a {@code PendingRepairManager} for a session that will never finish it, and its data
     * is neither promoted to repaired nor available as unrepaired.
     */
    private static void assertNoPendingRepairPublished(ColumnFamilyStore cfs, TimeUUID sessionID)
    {
        for (SSTableReader live : cfs.getLiveSSTables())
            assertNotEquals("child of a cancelled session was published: " + live.descriptor,
                            sessionID, live.getPendingRepair());
    }

    private static void assertNoTransactionLogs(ColumnFamilyStore cfs)
    {
        LifecycleTransaction.waitForDeletions();
        for (File dir : cfs.getDirectories().getCFDirectories())
            for (File f : dir.tryList())
                assertFalse("a lifecycle transaction log survived: " + f, f.name().contains("_txn_"));
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
        anticompact(cfs, ranges, sessionID, () -> false);
    }

    /**
     * @param isCancelled the repair session's own cancellation predicate, which is what
     *                    {@code PendingAntiCompaction} passes and the only signal that reaches the planning walk
     */
    private void anticompact(ColumnFamilyStore cfs,
                             RangesAtEndpoint ranges,
                             TimeUUID sessionID,
                             BooleanSupplier isCancelled) throws Exception
    {
        Set<SSTableReader> sstables = ImmutableSet.copyOf(cfs.getLiveSSTables());
        assertFalse("nothing to anticompact", sstables.isEmpty());

        ActiveRepairService.instance().registerParentRepairSession(sessionID,
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
            CompactionManager.instance.performAnticompaction(cfs, ranges, refs, txn, sessionID, isCancelled);
        }
        finally
        {
            ActiveRepairService.instance().removeParentRepairSession(sessionID);
        }
    }

    /**
     * The precondition every "the split ran" and "the planner said yes" assertion in this file rests on, with a
     * message that names the likeliest cause. {@link ZeroCopySSTableSplitter#isSupported} also demands a version
     * that can carry {@code StatsMetadata.hasUnindexedRegions} (BIG {@code pb}+, BTI {@code eb}+), so on a build
     * whose {@code storage_compatibility_mode} pins newly written sstables to {@code nb} or {@code oa} the whole
     * feature is INERT -- and every eligibility assertion here would quietly become an assertion about a refusal,
     * passing for entirely the wrong reason.
     */
    private static void assertSplittable(SSTableReader parent)
    {
        assertSplittable("fixture no longer produces a splittable sstable", parent);
    }

    private static void assertSplittable(String why, SSTableReader parent)
    {
        assertTrue("this run writes '" + parent.descriptor.version.version + "' sstables, which cannot carry the" +
                   " StatsMetadata.hasUnindexedRegions marker (BIG needs 'pb', BTI 'eb'), so zero-copy splitting is" +
                   " INERT here and every eligibility assertion in this class would silently become an assertion" +
                   " about a fallback. storage_compatibility_mode pins the version written; it must be NONE.",
                   parent.descriptor.version.hasUnindexedRegionsMarker());
        assertTrue(why + ": " + parent.descriptor, ZeroCopySSTableSplitter.isSupported(parent));
    }

    /**
     * Waits until the wall clock has left the second {@code localDeletionTime} was recorded in, which is what makes
     * a {@code gc_grace_seconds = 0} tombstone droppable ({@code localDeletionTime < gcBefore}). Bounded and
     * condition-driven, so it costs only as long as it has to and cannot silently be too short.
     */
    private static void awaitDroppable(long localDeletionTime)
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (FBUtilities.nowInSeconds() <= localDeletionTime)
        {
            assertTrue("the clock never passed localDeletionTime=" + localDeletionTime,
                       System.nanoTime() < deadline);
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);
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
        return assertOutcome(cfs, Collections.singleton(parent), before, full, trans, sessionID, expectedSSTables);
    }

    private Outputs assertOutcome(ColumnFamilyStore cfs,
                                  Collection<SSTableReader> parents,
                                  Outputs before,
                                  Collection<Range<Token>> full,
                                  Collection<Range<Token>> trans,
                                  TimeUUID sessionID,
                                  int expectedSSTables)
    {
        for (SSTableReader parent : parents)
        {
            assertFalse("a parent is still live, so it was not obsoleted", cfs.getLiveSSTables().contains(parent));
            assertTrue("a parent was not marked compacted", parent.isMarkedCompacted());
        }
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
     * extended {@link IVerifier} over it, which walks Data.db linearly from the first index position, validates
     * Digest.crc32, the index, the summary and the bloom filter, and throws on any inconsistency. This is the
     * assertion that catches a child whose rebuilt components disagree with its copied Data.db.
     */
    private void assertReopenableAndVerifiable(ColumnFamilyStore cfs) throws Exception
    {
        for (SSTableReader live : cfs.getLiveSSTables())
        {
            SSTableReader reopened = SSTableReader.open(cfs, live.descriptor, live.getComponents(), cfs.metadata);
            try
            {
                assertEquals(live.getFirst(), reopened.getFirst());
                assertEquals(live.getLast(), reopened.getLast());
                assertEquals(live.getPendingRepair(), reopened.getPendingRepair());
                assertEquals(live.isTransient(), reopened.isTransient());
                assertEquals(live.getRepairedAt(), reopened.getRepairedAt());
                // isOffline = true: the reopened reader is not in the tracker, so it must not go through the
                // compaction rate limiter and must not have its repair status mutated on a failure.
                try (IVerifier verifier = reopened.getVerifier(cfs, new OutputHandler.LogOutput(), true,
                                                               IVerifier.options().extendedVerification(true)
                                                                                  .build()))
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
     * A canonical rendering of one partition. {@code toString(metadata, true)} is the full-detail form: it
     * prints the primary key liveness info (timestamp, ttl, local expiration), the row deletion when there is
     * one, and every cell via {@code AbstractCell.toString()}, which includes the cell timestamp and marks
     * tombstones. Comparing these strings therefore compares rows, cells, timestamps and deletions, not just
     * keys. Strings are used deliberately: they can be retained safely after the scanner (and the parent's
     * files) are gone.
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

    /**
     * Insert only the partitions whose rank in TOKEN order falls in {@code [fromRank, toRank)}, so that two
     * flushes can produce two token-disjoint sstables. Insertion order alone will not do it: the keys hash all
     * over the ring, so flushing the first half of {@code key(0..199)} gives an sstable spanning nearly the whole
     * range, and no output could then be attributed to one parent.
     */
    private void insertPartitionsByTokenRank(ColumnFamilyStore cfs, int fromRank, int toRank) throws Throwable
    {
        List<Integer> byToken = new ArrayList<>();
        for (int p = 0; p < PARTITIONS; p++)
            byToken.add(p);
        byToken.sort((a, b) -> cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(key(a)))
                                 .compareTo(cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(key(b)))));
        for (int rank = fromRank; rank < toRank; rank++)
        {
            int p = byToken.get(rank);
            for (int c = 0; c < ROWS_PER_PARTITION; c++)
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", key(p), c, randomText(VALUE_BYTES));
        }
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
        return cfs.metric.bytesZeroCopyAnticompaction.table.getCount();
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
