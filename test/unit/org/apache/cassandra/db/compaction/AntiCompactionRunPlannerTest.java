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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TestDatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.RepairState;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The run-planning half of zero-copy anticompaction: {@link AntiCompactionRunPlanner} decides whether one
 * sstable's FULL / TRANSIENT / UNREPAIRED partitions form few enough contiguous runs that the splitter can
 * reproduce the anticompaction, and if so where to cut and what repair state each piece gets.
 *
 * <p>Two layers are tested. The pure run-length encoding is driven through
 * {@link AntiCompactionRunPlanner#planFromLabels} with no sstable at all, so every shape -- including the
 * pathological vnode interleavings -- is cheap to express. Then a handful of real compressed sstables are run
 * through {@link AntiCompactionRunPlanner#plan} with ranges derived from their own index keys, which is the only
 * way to prove the Index.db walk labels and cuts in the same places.
 *
 * <p>The load-bearing assertion throughout is the exact identity of the boundary keys. A boundary is the FIRST
 * key of the NEW run (the splitter starts a run at the first record whose key is {@code >=} the boundary), so an
 * off-by-one there silently hands one partition to the wrong repair state -- data that should stay unrepaired
 * gets marked pending-repair for a session that never validated it, or vice versa. That is the worst bug this
 * feature can have and it is invisible in a "the children add up to the parent" test, so every eligible case
 * below pins the boundary keys down exactly.
 *
 * <p>The pure run-encoding half is format agnostic and always runs. The half that needs a real sstable is written
 * against BIG, because the boundary-key assertions are stated in terms of an Index.db walk, so those tests assume
 * {@link BigFormat#isSelected()}; BTI is supported too and is covered by {@link #planAcceptsBtiFormatSSTable()},
 * which forces the format on purpose.
 *
 * <p><b>Eligibility is a precondition, not an outcome.</b> {@link ZeroCopySSTableSplitter#isSupported} now also
 * requires an sstable version that can carry {@code StatsMetadata.hasUnindexedRegions} (BIG {@code pb}+, BTI
 * {@code eb}+). A run whose {@code storage_compatibility_mode} pins newly written BIG sstables to {@code nb} or
 * {@code oa} makes the whole feature inert, and every "the planner says eligible" assertion here would quietly
 * become an assertion about a refusal. {@link #compressedSSTable} therefore fails loudly, and says why, rather
 * than letting that happen.
 */
public class AntiCompactionRunPlannerTest extends CQLTester
{
    // Shorthand so a label sequence reads like the on-disk shape it describes.
    private static final AntiCompactionRunPlanner.Label F = AntiCompactionRunPlanner.Label.FULL;
    private static final AntiCompactionRunPlanner.Label T = AntiCompactionRunPlanner.Label.TRANSIENT;
    private static final AntiCompactionRunPlanner.Label U = AntiCompactionRunPlanner.Label.UNREPAIRED;

    // ----------------------------------------------------------------------------------------------------
    // Pure run encoding: no sstable, no files
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void noPartitionsIsIneligible()
    {
        List<AntiCompactionRunPlanner.Label> noLabels = Collections.emptyList();
        List<DecoratedKey> noKeys = Collections.emptyList();

        AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.planFromLabels(noLabels, noKeys, nextTimeUUID());

        assertFalse(plan.eligible);
        assertEquals(0, plan.runCount);
        assertEquals("sstable has no partitions", plan.ineligibleReason);
        assertTrue(plan.boundaries.isEmpty());
        assertTrue(plan.perChild.isEmpty());
    }

    /**
     * A single run means there is nothing to cut. All three flavours are ineligible, and each says which label
     * it was -- "entire sstable is FULL" and "entire sstable is UNREPAIRED" are operationally very different
     * situations for an operator reading the log, so they must
     * not collapse into one generic message.
     */
    @Test
    public void singleRunIsIneligibleWithItsOwnReason()
    {
        TimeUUID session = nextTimeUUID();
        AntiCompactionRunPlanner.Plan allUnrepaired = planOf(session, U, U, U, U);
        AntiCompactionRunPlanner.Plan allFull = planOf(session, F, F, F, F);
        AntiCompactionRunPlanner.Plan allTransient = planOf(session, T, T, T, T);

        for (AntiCompactionRunPlanner.Plan plan : Arrays.asList(allUnrepaired, allFull, allTransient))
        {
            assertFalse(plan.toString(), plan.eligible);
            assertEquals(1, plan.runCount);
            assertTrue(plan.boundaries.isEmpty());
            assertTrue(plan.perChild.isEmpty());
        }

        assertTrue(allUnrepaired.ineligibleReason,
                   allUnrepaired.ineligibleReason.contains("entire sstable is UNREPAIRED"));
        assertTrue(allFull.ineligibleReason, allFull.ineligibleReason.contains("entire sstable is FULL"));
        assertTrue(allTransient.ineligibleReason,
                   allTransient.ineligibleReason.contains("entire sstable is TRANSIENT"));

        HashSet<String> distinct = new HashSet<>(Arrays.asList(allUnrepaired.ineligibleReason,
                                                              allFull.ineligibleReason,
                                                              allTransient.ineligibleReason));
        assertEquals("the three single-run reasons must be distinguishable", 3, distinct.size());
    }

    /** A one-partition sstable is one run, whatever that partition is labelled. */
    @Test
    public void singlePartitionIsIneligible()
    {
        TimeUUID session = nextTimeUUID();
        for (AntiCompactionRunPlanner.Label label : Arrays.asList(U, F, T))
        {
            AntiCompactionRunPlanner.Plan plan = planOf(session, label);
            assertFalse(plan.eligible);
            assertEquals(1, plan.runCount);
            assertTrue(plan.ineligibleReason.contains("entire sstable is " + label));
        }
    }

    @Test
    public void unrepairedThenFullIsEligible()
    {
        TimeUUID session = nextTimeUUID();
        List<DecoratedKey> keys = ascendingKeys(6);

        AntiCompactionRunPlanner.Plan plan =
            AntiCompactionRunPlanner.planFromLabels(Arrays.asList(U, U, U, F, F, F), keys, session);

        assertTrue(plan.ineligibleReason, plan.eligible);
        assertNull(plan.ineligibleReason);
        assertEquals(2, plan.runCount);
        // the cut is the first FULL key, not the last UNREPAIRED one
        assertEquals(Arrays.asList(keys.get(3)), plan.boundaries);
        assertNotEquals(keys.get(2), plan.boundaries.get(0));
        assertEquals(Arrays.asList(unrepaired(), pendingFull(session)), plan.perChild);
    }

    @Test
    public void fullThenUnrepairedIsEligible()
    {
        TimeUUID session = nextTimeUUID();
        List<DecoratedKey> keys = ascendingKeys(5);

        AntiCompactionRunPlanner.Plan plan =
            AntiCompactionRunPlanner.planFromLabels(Arrays.asList(F, F, U, U, U), keys, session);

        assertTrue(plan.ineligibleReason, plan.eligible);
        assertEquals(2, plan.runCount);
        assertEquals(Arrays.asList(keys.get(2)), plan.boundaries);
        assertEquals(Arrays.asList(pendingFull(session), unrepaired()), plan.perChild);
    }

    /** The common straddle: an sstable whose middle is owned and whose ends are not. */
    @Test
    public void unrepairedFullUnrepairedIsEligible()
    {
        TimeUUID session = nextTimeUUID();
        List<DecoratedKey> keys = ascendingKeys(9);

        AntiCompactionRunPlanner.Plan plan =
            AntiCompactionRunPlanner.planFromLabels(Arrays.asList(U, U, F, F, F, F, U, U, U), keys, session);

        assertTrue(plan.ineligibleReason, plan.eligible);
        assertEquals(3, plan.runCount);
        assertEquals(Arrays.asList(keys.get(2), keys.get(6)), plan.boundaries);
        assertEquals(Arrays.asList(unrepaired(), pendingFull(session), unrepaired()), plan.perChild);
    }

    /**
     * Two partitions with a transition between them: the tightest possible off-by-one. The boundary must be the
     * SECOND key. If it were the first, the splitter would put partition 0 in the FULL child and the whole
     * sstable would be marked pending-repair.
     */
    @Test
    public void twoPartitionTransitionCutsAtTheSecondKey()
    {
        TimeUUID session = nextTimeUUID();
        List<DecoratedKey> keys = ascendingKeys(2);

        AntiCompactionRunPlanner.Plan plan =
            AntiCompactionRunPlanner.planFromLabels(Arrays.asList(U, F), keys, session);

        assertTrue(plan.ineligibleReason, plan.eligible);
        assertEquals(2, plan.runCount);
        assertEquals(1, plan.boundaries.size());
        assertEquals("the boundary must be the first key of the new run", keys.get(1), plan.boundaries.get(0));
        assertNotEquals("the boundary must not be the last key of the old run", keys.get(0), plan.boundaries.get(0));
        assertEquals(Arrays.asList(unrepaired(), pendingFull(session)), plan.perChild);

        // and the mirror image, so the assertion above cannot pass by accident on a constant
        AntiCompactionRunPlanner.Plan mirrored =
            AntiCompactionRunPlanner.planFromLabels(Arrays.asList(F, U), keys, session);
        assertTrue(mirrored.eligible);
        assertEquals(Arrays.asList(keys.get(1)), mirrored.boundaries);
        assertEquals(Arrays.asList(pendingFull(session), unrepaired()), mirrored.perChild);
    }

    @Test
    public void fullInTwoRunsIsIneligible()
    {
        TimeUUID session = nextTimeUUID();

        AntiCompactionRunPlanner.Plan plan = planOf(session, F, F, U, U, F, F);

        assertFalse(plan.eligible);
        assertEquals(3, plan.runCount);
        assertTrue(plan.ineligibleReason, plan.ineligibleReason.contains("FULL appears in 2 runs"));
        assertTrue(plan.boundaries.isEmpty());
        assertTrue(plan.perChild.isEmpty());
    }

    /** What vnodes actually produce, and the whole reason for the gate. */
    @Test
    public void alternatingRunsAreIneligible()
    {
        TimeUUID session = nextTimeUUID();

        AntiCompactionRunPlanner.Plan plan = planOf(session, F, U, F, U, F);

        assertFalse(plan.eligible);
        assertEquals(5, plan.runCount);
        assertTrue(plan.ineligibleReason, plan.ineligibleReason.contains("FULL appears in 3 runs"));
    }

    /**
     * Past {@code MAX_RETAINED_RUNS} the planner stops retaining boundary keys so an alternating layout cannot
     * pin one key per partition on the heap -- but the run counters must stay exact, since they are what the
     * INFO/DEBUG log reports.
     */
    @Test
    public void longAlternatingRunsStillCountExactlyWithoutRetainingKeys()
    {
        TimeUUID session = nextTimeUUID();
        List<AntiCompactionRunPlanner.Label> labels = new ArrayList<>();
        for (int i = 0; i < 40; i++)
            labels.add(i % 2 == 0 ? F : U);
        List<DecoratedKey> keys = ascendingKeys(labels.size());

        AntiCompactionRunPlanner.RunEncoding runs = AntiCompactionRunPlanner.encodeRuns(labels, keys);
        assertEquals(40, runs.runCount);
        assertEquals(20, runs.fullRuns);
        assertEquals(20, runs.unrepairedRuns);
        assertEquals(0, runs.transientRuns);
        assertTrue("run detail must be dropped past the retention cap", runs.runLabels.isEmpty());
        assertTrue("run detail must be dropped past the retention cap", runs.runFirstKeys.isEmpty());

        AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.planFromLabels(labels, keys, session);
        assertFalse(plan.eligible);
        assertEquals(40, plan.runCount);
        assertTrue(plan.ineligibleReason, plan.ineligibleReason.contains("FULL appears in 20 runs"));
    }

    /** Run detail is retained for shapes that can still turn out eligible. */
    @Test
    public void runEncodingRetainsDetailForShortShapes()
    {
        List<DecoratedKey> keys = ascendingKeys(7);

        AntiCompactionRunPlanner.RunEncoding runs =
            AntiCompactionRunPlanner.encodeRuns(Arrays.asList(U, U, F, F, T, U, U), keys);

        assertEquals(4, runs.runCount);
        assertEquals(1, runs.fullRuns);
        assertEquals(1, runs.transientRuns);
        assertEquals(2, runs.unrepairedRuns);
        assertEquals(Arrays.asList(U, F, T, U), runs.runLabels);
        assertEquals(Arrays.asList(keys.get(0), keys.get(2), keys.get(4), keys.get(5)), runs.runFirstKeys);
    }

    @Test
    public void fullThenTransientThenUnrepairedIsEligible()
    {
        TimeUUID session = nextTimeUUID();
        List<DecoratedKey> keys = ascendingKeys(7);

        AntiCompactionRunPlanner.Plan plan =
            AntiCompactionRunPlanner.planFromLabels(Arrays.asList(F, F, T, T, U, U, U), keys, session);

        assertTrue(plan.ineligibleReason, plan.eligible);
        assertEquals(3, plan.runCount);
        assertEquals(Arrays.asList(keys.get(2), keys.get(4)), plan.boundaries);
        assertEquals(Arrays.asList(pendingFull(session), pendingTransient(session), unrepaired()), plan.perChild);
    }

    /** The widest eligible shape: FULL once, TRANSIENT once, UNREPAIRED leading, trailing and in between. */
    @Test
    public void fiveRunShapeWithOneFullAndOneTransientIsEligible()
    {
        TimeUUID session = nextTimeUUID();
        List<DecoratedKey> keys = ascendingKeys(10);

        AntiCompactionRunPlanner.Plan plan =
            AntiCompactionRunPlanner.planFromLabels(Arrays.asList(U, U, F, F, U, U, T, T, U, U), keys, session);

        assertTrue(plan.ineligibleReason, plan.eligible);
        assertEquals(5, plan.runCount);
        assertEquals(Arrays.asList(keys.get(2), keys.get(4), keys.get(6), keys.get(8)), plan.boundaries);
        assertEquals(Arrays.asList(unrepaired(), pendingFull(session), unrepaired(),
                                   pendingTransient(session), unrepaired()),
                     plan.perChild);
    }

    @Test
    public void transientInTwoRunsIsIneligible()
    {
        TimeUUID session = nextTimeUUID();

        AntiCompactionRunPlanner.Plan plan = planOf(session, T, T, U, U, T, T);

        assertFalse(plan.eligible);
        assertEquals(3, plan.runCount);
        assertTrue(plan.ineligibleReason, plan.ineligibleReason.contains("TRANSIENT appears in 2 runs"));
        assertTrue(plan.boundaries.isEmpty());
        assertTrue(plan.perChild.isEmpty());
    }

    /** The three triples must be exactly what {@code createWriterForAntiCompaction} is handed today. */
    @Test
    public void repairStatePerLabelMatchesTheRewritePath()
    {
        TimeUUID session = nextTimeUUID();

        RepairState full = AntiCompactionRunPlanner.stateFor(F, session);
        assertEquals(UNREPAIRED_SSTABLE, full.repairedAt);
        assertEquals(session, full.pendingRepair);
        assertFalse(full.isTransient);

        RepairState trans = AntiCompactionRunPlanner.stateFor(T, session);
        assertEquals(UNREPAIRED_SSTABLE, trans.repairedAt);
        assertEquals(session, trans.pendingRepair);
        assertTrue(trans.isTransient);

        RepairState unrepaired = AntiCompactionRunPlanner.stateFor(U, session);
        assertEquals(UNREPAIRED_SSTABLE, unrepaired.repairedAt);
        assertEquals(NO_PENDING_REPAIR, unrepaired.pendingRepair);
        assertFalse(unrepaired.isTransient);

        assertNotEquals(full, trans);
        assertNotEquals(full, unrepaired);
    }

    /**
     * Overlapping full and transient ranges are permitted, and full must win -- the same precedence
     * {@code antiCompactGroup} applies when it routes a partition to one of its three writers. Getting this
     * backwards would mark data transient on a full replica, and transient pending-repair data is DELETED rather
     * than promoted when the session finalizes.
     */
    @Test
    public void fullWinsOverTransientForOverlappingRanges()
    {
        // tokens 1000, 2000, ... 6000
        List<DecoratedKey> keys = ascendingKeys(6);
        Range<Token> full = new Range<>(token(2500), token(4500));    // tokens 3000, 4000
        Range<Token> trans = new Range<>(token(1500), token(5500));   // tokens 2000..5000, overlapping full
        RangesAtEndpoint ranges = rangesAtEndpoint(Collections.singletonList(full),
                                                  Collections.singletonList(trans));

        assertEquals(Arrays.asList(U, T, F, F, T, U), AntiCompactionRunPlanner.labels(keys, ranges));

        // ...and that shape is TRANSIENT-in-two-runs, so it is ineligible
        AntiCompactionRunPlanner.Plan plan =
            AntiCompactionRunPlanner.planFromLabels(AntiCompactionRunPlanner.labels(keys, ranges),
                                                    keys, nextTimeUUID());
        assertFalse(plan.eligible);
        assertEquals(5, plan.runCount);
        assertTrue(plan.ineligibleReason, plan.ineligibleReason.contains("TRANSIENT appears in 2 runs"));
    }

    /** No full ranges and no transient ranges at all: every partition is UNREPAIRED, and nothing blows up. */
    @Test
    public void emptyRangesLabelEverythingUnrepaired()
    {
        List<DecoratedKey> keys = ascendingKeys(4);
        RangesAtEndpoint empty = rangesAtEndpoint(Collections.emptyList(), Collections.emptyList());

        assertEquals(Arrays.asList(U, U, U, U), AntiCompactionRunPlanner.labels(keys, empty));
    }

    // ----------------------------------------------------------------------------------------------------
    // End to end: a real compressed sstable, ranges derived from its own index keys
    // ----------------------------------------------------------------------------------------------------

    @Test
    public void planOnRealSSTableCutsAtTheFirstOwnedKey() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        SSTableReader parent = compressedSSTable(40);
        List<DecoratedKey> keys = indexKeys(parent);
        assertEquals(40, keys.size());
        TimeUUID session = nextTimeUUID();

        // (token[19], token[39]] owns exactly keys 20..39
        RangesAtEndpoint ranges = fullOnly(new Range<>(keys.get(19).getToken(), keys.get(39).getToken()));

        AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.plan(parent, ranges, session);

        assertTrue(plan.ineligibleReason, plan.eligible);
        assertNull(plan.ineligibleReason);
        assertEquals(2, plan.runCount);
        assertEquals(Arrays.asList(keys.get(20)), plan.boundaries);
        assertNotEquals(keys.get(19), plan.boundaries.get(0));
        assertEquals(Arrays.asList(unrepaired(), pendingFull(session)), plan.perChild);
    }

    @Test
    public void planOnRealSSTableFindsTheThreeRunStraddle() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        SSTableReader parent = compressedSSTable(40);
        List<DecoratedKey> keys = indexKeys(parent);
        TimeUUID session = nextTimeUUID();

        // (token[9], token[24]] owns exactly keys 10..24
        RangesAtEndpoint ranges = fullOnly(new Range<>(keys.get(9).getToken(), keys.get(24).getToken()));

        AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.plan(parent, ranges, session);

        assertTrue(plan.ineligibleReason, plan.eligible);
        assertEquals(3, plan.runCount);
        assertEquals(Arrays.asList(keys.get(10), keys.get(25)), plan.boundaries);
        assertEquals(Arrays.asList(unrepaired(), pendingFull(session), unrepaired()), plan.perChild);
    }

    @Test
    public void planOnRealSSTableHandlesAFullAndATransientRun() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        SSTableReader parent = compressedSSTable(40);
        List<DecoratedKey> keys = indexKeys(parent);
        TimeUUID session = nextTimeUUID();

        Range<Token> full = new Range<>(keys.get(9).getToken(), keys.get(19).getToken());    // keys 10..19
        Range<Token> trans = new Range<>(keys.get(19).getToken(), keys.get(29).getToken());  // keys 20..29
        RangesAtEndpoint ranges = rangesAtEndpoint(Collections.singletonList(full),
                                                   Collections.singletonList(trans));

        AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.plan(parent, ranges, session);

        assertTrue(plan.ineligibleReason, plan.eligible);
        assertEquals(4, plan.runCount);
        assertEquals(Arrays.asList(keys.get(10), keys.get(20), keys.get(30)), plan.boundaries);
        assertEquals(Arrays.asList(unrepaired(), pendingFull(session), pendingTransient(session), unrepaired()),
                     plan.perChild);
    }

    @Test
    public void planOnRealSSTableRejectsInterleavedFullRanges() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        SSTableReader parent = compressedSSTable(40);
        List<DecoratedKey> keys = indexKeys(parent);

        // two owned islands with unowned partitions between them: U F U F U
        Range<Token> firstIsland = new Range<>(keys.get(4).getToken(), keys.get(9).getToken());   // keys 5..9
        Range<Token> secondIsland = new Range<>(keys.get(19).getToken(), keys.get(24).getToken()); // keys 20..24
        RangesAtEndpoint ranges = rangesAtEndpoint(Arrays.asList(firstIsland, secondIsland),
                                                   Collections.emptyList());

        AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.plan(parent, ranges, nextTimeUUID());

        assertFalse(plan.eligible);
        assertEquals(5, plan.runCount);
        assertTrue(plan.ineligibleReason, plan.ineligibleReason.contains("FULL appears in 2 runs"));
        assertTrue(plan.boundaries.isEmpty());
        assertTrue(plan.perChild.isEmpty());
    }

    /**
     * The Index.db walk has to be interruptible: it is the first thing in an anticompaction that takes real time --
     * a couple of percent of a multi-GiB sstable, and for a narrow-partition BTI parent a decompressing read of the
     * whole data file -- and nothing else in it answers a repair session that has gone away.
     * <p>
     * The contract is REPORTING, not throwing, and that is the whole point of the {@code BooleanSupplier} overload:
     * {@link AntiCompactionRunPlanner} answers a question, so a cancelled walk comes back as
     * {@link AntiCompactionRunPlanner.Plan#interrupted} -- "this is not a verdict about the sstable" -- and
     * {@code CompactionManager.zeroCopyAntiCompact} stops planning the rest of the group rather than reading it as
     * "rewrite this one". Note what is NOT here any more: the walk is deliberately not registered with
     * {@code ActiveCompactions} (it writes nothing, so it must not be credited bytes or reserve disk), so
     * {@code nodetool stop} and TRUNCATE do not reach it; a repair-session cancellation, which is what this
     * predicate carries, does.
     */
    @Test
    public void cancellationDuringTheIndexWalkIsReportedRatherThanThrown() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        SSTableReader parent = compressedSSTable(40);
        List<DecoratedKey> keys = indexKeys(parent);
        RangesAtEndpoint ranges = rangesAtEndpoint(Collections.singletonList(
            new Range<>(keys.get(9).getToken(), keys.get(19).getToken())), Collections.emptyList());

        // Non-vacuity: the same call with no cancellation plans normally, so what follows is the predicate firing
        // and not the shape being ineligible.
        AntiCompactionRunPlanner.Plan uncancelled = AntiCompactionRunPlanner.plan(parent, ranges, nextTimeUUID());
        assertTrue(uncancelled.toString(), uncancelled.eligible);
        assertFalse(uncancelled.interrupted);

        AtomicInteger checks = new AtomicInteger();
        AntiCompactionRunPlanner.Plan plan =
            AntiCompactionRunPlanner.plan(parent, ranges, nextTimeUUID(), () -> {
                checks.incrementAndGet();
                return true;
            });

        assertTrue("the cancellation predicate must be consulted during the walk", checks.get() >= 1);
        assertTrue(plan.toString(), plan.interrupted);
        // Not a verdict: interrupted and eligible must never both hold, and nothing partial may be reported.
        assertFalse("an interrupted plan must never also be eligible", plan.eligible);
        assertEquals("a walk that stopped part way through counted nothing", 0, plan.runCount);
        assertTrue(plan.boundaries.isEmpty());
        assertTrue(plan.perChild.isEmpty());
        assertNotNull(plan.ineligibleReason);
        assertTrue(plan.ineligibleReason, plan.ineligibleReason.contains("cancelled"));
        assertTrue(plan.toString(), plan.toString().contains("interrupted"));
    }

    /**
     * The cadence, pinned because it is the whole cost/latency trade of the check: one consultation per 1024 index
     * records, keyed on the record ordinal. Cheaper and the cancellation goes unnoticed for up to 1024 partitions
     * of walking; more expensive and a predicate that touches shared repair state is called per partition.
     * <p>
     * 1030 partitions gives exactly two check points (ordinals 0 and 1024), so both the count and the fact that a
     * flip at the SECOND one still abandons the walk are observable. The second half is what a real cancellation
     * looks like -- the session is alive when planning starts and dies during it -- which the {@code () -> true}
     * case above cannot show, since it fires on the very first record.
     */
    @Test
    public void theIndexWalkChecksCancellationEveryThousandRecords() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        SSTableReader parent = compressedSSTable(1030);
        List<DecoratedKey> keys = indexKeys(parent);
        assertEquals(1030, keys.size());
        RangesAtEndpoint ranges = fullOnly(new Range<>(keys.get(9).getToken(), keys.get(499).getToken()));

        AtomicInteger checks = new AtomicInteger();
        AntiCompactionRunPlanner.Plan completed =
            AntiCompactionRunPlanner.plan(parent, ranges, nextTimeUUID(), () -> {
                checks.incrementAndGet();
                return false;
            });
        assertTrue(completed.toString(), completed.eligible);
        assertEquals("one check per 1024 records, at ordinals 0 and 1024", 2, checks.get());

        // ...and a flip at the second check point abandons a walk that had already labelled 1024 partitions.
        AtomicInteger secondRun = new AtomicInteger();
        AntiCompactionRunPlanner.Plan interrupted =
            AntiCompactionRunPlanner.plan(parent, ranges, nextTimeUUID(),
                                          () -> secondRun.incrementAndGet() >= 2);
        assertTrue(interrupted.toString(), interrupted.interrupted);
        assertFalse(interrupted.eligible);
        assertEquals("nothing partial may be reported", 0, interrupted.runCount);
    }

    /**
     * The planner catches exactly one thing out of its walk callback -- its own private control-flow marker -- so a
     * predicate that throws is NOT converted into a verdict. Worth pinning: the obvious way to write that catch is
     * around the whole walk and broad enough to swallow a {@link CompactionInterruptedException} or a
     * {@code CorruptSSTableException}, and a swallowed one of those becomes "rewrite this sstable" -- which is how
     * {@code CompactionManager} ends up reporting a successful anticompaction on a node with a bad sector. See
     * {@code ZeroCopyAntiCompactionTest.plannerFailureThatIsNotALocalRefusalFailsTheAnticompaction} for the other half.
     */
    @Test
    public void aThrowingCancellationPredicateIsNotSwallowed() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        SSTableReader parent = compressedSSTable(40);
        List<DecoratedKey> keys = indexKeys(parent);
        RangesAtEndpoint ranges = fullOnly(new Range<>(keys.get(9).getToken(), keys.get(19).getToken()));

        assertTrue(AntiCompactionRunPlanner.plan(parent, ranges, nextTimeUUID()).eligible);

        assertThatThrownBy(() -> AntiCompactionRunPlanner.plan(parent, ranges, nextTimeUUID(), () -> {
            throw new CompactionInterruptedException("stopped during the index walk");
        })).isInstanceOf(CompactionInterruptedException.class);

        assertThatThrownBy(() -> AntiCompactionRunPlanner.plan(parent, ranges, nextTimeUUID(), () -> {
            throw new CorruptSSTableException(new IOException("bad sector"),
                                              parent.descriptor.fileFor(BigFormat.Components.PRIMARY_INDEX));
        })).isInstanceOf(CorruptSSTableException.class);
    }

    @Test
    public void planOnRealSSTableRejectsAFullyOwnedSSTable() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        SSTableReader parent = compressedSSTable(20);
        List<DecoratedKey> keys = indexKeys(parent);

        RangesAtEndpoint ranges = fullOnly(new Range<>(parent.getPartitioner().getMinimumToken(),
                                                      keys.get(keys.size() - 1).getToken()));

        AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.plan(parent, ranges, nextTimeUUID());

        assertFalse(plan.eligible);
        assertEquals(1, plan.runCount);
        assertTrue(plan.ineligibleReason, plan.ineligibleReason.contains("entire sstable is FULL"));
    }

    /**
     * An uncompressed sstable must be REPORTED ineligible, not throw: {@code plan} is called on every sstable of
     * every anticompaction group, and a throw there would fail repairs on any uncompressed table.
     */
    @Test
    public void planReportsUncompressedSSTableAsIneligible() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        disableCompaction();
        for (int p = 0; p < 10; p++)
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", key(p), 0, "v");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader parent = cfs.getLiveSSTables().iterator().next();
        assertFalse(parent.compression);
        assertFalse(ZeroCopySSTableSplitter.isSupported(parent));

        List<DecoratedKey> keys = indexKeys(parent);
        RangesAtEndpoint ranges = fullOnly(new Range<>(keys.get(4).getToken(), keys.get(9).getToken()));

        AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.plan(parent, ranges, nextTimeUUID());

        assertFalse(plan.eligible);
        assertEquals(0, plan.runCount);
        assertTrue(plan.ineligibleReason,
                   plan.ineligibleReason.contains("not a compressed BIG- or BTI-format sstable"));
        // the reason names the thing that actually disqualified it
        assertTrue(plan.ineligibleReason, plan.ineligibleReason.contains("compressed=false"));
        assertTrue(plan.boundaries.isEmpty());
        assertTrue(plan.perChild.isEmpty());
    }

    /**
     * BTI is supported now, so what this pins is that the planner ENGAGES on it rather than reporting it
     * ineligible on format grounds. It is worth keeping as a planner test rather than folding into
     * {@code ZeroCopySSTableSplitterBtiTest}: {@code plan} is called for every sstable of every anticompaction
     * group, so a regression that made it refuse BTI again would silently send every BTI incremental repair back
     * to the rewriting path with nothing in the log that named the format.
     */
    @Test
    public void planAcceptsBtiFormatSSTable() throws Throwable
    {
        Assume.assumeTrue(DatabaseDescriptor.getSSTableFormats().containsKey(BtiFormat.NAME));

        SSTableFormat<?, ?> selected = DatabaseDescriptor.getSelectedSSTableFormat();
        SSTableReader parent;
        try
        {
            // the selected format is global and is read at flush time, so switching it here is what makes the
            // flush below produce a BTI sstable even on a BIG-selected run
            TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(BtiFormat.NAME);
            createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                        "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
            disableCompaction();
            for (int p = 0; p < 10; p++)
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", key(p), 0, "value");
            flush();

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            assertEquals(1, cfs.getLiveSSTables().size());
            parent = cfs.getLiveSSTables().iterator().next();
        }
        finally
        {
            TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(selected);
        }

        assertTrue(parent.descriptor.getFormat().name(), BtiFormat.is(parent.descriptor.getFormat()));
        assertTrue(parent.compression);
        assertSplittable(parent);

        List<DecoratedKey> keys = indexKeys(parent);
        RangesAtEndpoint ranges = fullOnly(new Range<>(keys.get(4).getToken(), keys.get(9).getToken()));

        AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.plan(parent, ranges, nextTimeUUID());

        assertTrue(String.valueOf(plan.ineligibleReason), plan.eligible);
        assertNull(plan.ineligibleReason);
        assertTrue(plan.toString(), plan.runCount >= 2);
        assertEquals(plan.boundaries.size() + 1, plan.perChild.size());
    }

    /**
     * A child is assembled from a fixed component list and its TOC written from that same list, so any component the
     * parent carries and the splitter cannot write is silently absent from every child. For storage-attached index
     * components that is not benign -- the child is live and readable and its rows match no index predicate, because
     * {@code SSTableContextManager.update} drops an sstable with no per-sstable completion marker out of the index
     * view without reporting it invalid -- so the planner declines instead, and does so BEFORE the Index.db walk,
     * which is what {@code runCount == 0} pins.
     * <p>
     * A custom component stands in for a real index here so the test needs no SAI: the gate tests the same
     * {@code parent.getComponents() \ WRITTEN_COMPONENTS} difference either way. The authoritative gate for SAI is
     * {@code SecondaryIndexManager.hasSSTableAttachedIndexes()} in {@code CompactionManager.zeroCopyAntiCompact};
     * this one is the format-agnostic backstop, and covers any component type added later.
     */
    @Test
    public void planRefusesAnSSTableCarryingComponentsAChildWouldNotGet() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
        disableCompaction();
        for (int p = 0; p < 10; p++)
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", key(p), 0, "value");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader parent = cfs.getLiveSSTables().iterator().next();
        assertSplittable(parent);

        List<DecoratedKey> keys = indexKeys(parent);
        RangesAtEndpoint ranges = fullOnly(new Range<>(keys.get(4).getToken(), keys.get(9).getToken()));

        // Eligible first, so the refusal below is provably the component and not the shape.
        assertTrue(AntiCompactionRunPlanner.plan(parent, ranges, nextTimeUUID()).eligible);

        Component extra = PLANNER_TEST_EXTRA_TYPE.createComponent("PlannerTestExtra.db");
        parent.descriptor.fileFor(extra).createFileIfNotExists();
        parent.registerComponents(Collections.singleton(extra), cfs.getTracker());

        AntiCompactionRunPlanner.Plan plan = AntiCompactionRunPlanner.plan(parent, ranges, nextTimeUUID());
        assertFalse(plan.toString(), plan.eligible);
        assertNotNull(plan.ineligibleReason);
        assertTrue(plan.ineligibleReason, plan.ineligibleReason.contains("PlannerTestExtra.db"));
        assertEquals("the Index.db walk should have been skipped entirely", 0, plan.runCount);

        // The splitter itself refuses too, for a caller that never asked the planner.
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.split(parent, 2, null))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("PlannerTestExtra.db");
    }

    // ----------------------------------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------------------------------

    /**
     * Stands in for a storage-attached index component: present on the parent, outside the splitter's written list.
     * Static because {@code Component.Type}'s constructor registers itself globally, so it must be created once.
     */
    private static final Component.Type PLANNER_TEST_EXTRA_TYPE =
        Component.Type.create("PlannerTestExtra", ".*-PlannerTestExtra\\.db", true, null);

    private static AntiCompactionRunPlanner.Plan planOf(TimeUUID session, AntiCompactionRunPlanner.Label... labels)
    {
        return AntiCompactionRunPlanner.planFromLabels(Arrays.asList(labels), ascendingKeys(labels.length), session);
    }

    /** {@code count} distinct keys with strictly ascending tokens 1000, 2000, ... */
    private static List<DecoratedKey> ascendingKeys(int count)
    {
        List<DecoratedKey> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
            keys.add(new BufferDecoratedKey(token(1000L * (i + 1)), ByteBufferUtil.bytes(String.format("k%06d", i))));
        return keys;
    }

    private static Token token(long value)
    {
        return new Murmur3Partitioner.LongToken(value);
    }

    private static RepairState unrepaired()
    {
        return new RepairState(UNREPAIRED_SSTABLE, NO_PENDING_REPAIR, false);
    }

    private static RepairState pendingFull(TimeUUID session)
    {
        return new RepairState(UNREPAIRED_SSTABLE, session, false);
    }

    private static RepairState pendingTransient(TimeUUID session)
    {
        return new RepairState(UNREPAIRED_SSTABLE, session, true);
    }

    private static RangesAtEndpoint fullOnly(Range<Token> range)
    {
        return rangesAtEndpoint(Collections.singletonList(range), Collections.emptyList());
    }

    private static RangesAtEndpoint rangesAtEndpoint(List<Range<Token>> full, List<Range<Token>> trans)
    {
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(local);
        for (Range<Token> range : full)
            builder.add(Replica.fullReplica(local, range));
        for (Range<Token> range : trans)
            builder.add(Replica.transientReplica(local, range));
        return builder.build();
    }

    /**
     * One compressed sstable holding exactly {@code partitions} partitions. Compression is what makes the
     * splitter applicable at all, so a SchemaLoader-style uncompressed table could never reach the eligible
     * path.
     */
    private SSTableReader compressedSSTable(int partitions) throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
        disableCompaction();
        for (int p = 0; p < partitions; p++)
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", key(p), 0, "value");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader parent = cfs.getLiveSSTables().iterator().next();
        assertTrue(parent.compression);
        assertSplittable(parent);
        return parent;
    }

    /**
     * The precondition every eligibility assertion in this file rests on, asserted with a message that names the
     * likely cause. Without it a run that writes pre-{@code pb} sstables turns each of those assertions into an
     * assertion about a refusal, which passes for entirely the wrong reason.
     */
    private static void assertSplittable(SSTableReader parent)
    {
        assertTrue("this run writes '" + parent.descriptor.version.version + "' sstables, which cannot carry the" +
                   " StatsMetadata.hasUnindexedRegions marker (BIG needs 'pb', BTI 'eb'), so zero-copy splitting is" +
                   " INERT here and every eligibility assertion in this class would silently become an assertion" +
                   " about a refusal. storage_compatibility_mode pins the version being written; it must be NONE.",
                   parent.descriptor.version.hasUnindexedRegionsMarker());
        assertTrue("fixture no longer produces a splittable sstable: " + parent.descriptor,
                   ZeroCopySSTableSplitter.isSupported(parent));
    }

    /**
     * The sstable's partition keys in on-disk (token) order -- the exact sequence the planner walks. For a BIG
     * sstable {@code keyIterator()} is the same sequential Index.db read the planner does. {@code retainable()}
     * because the keys are accumulated across advances while the reader recycles the buffer they were read from,
     * which is exactly why the planner's own walk retains its run boundaries the same way.
     */
    private static List<DecoratedKey> indexKeys(SSTableReader sstable) throws IOException
    {
        List<DecoratedKey> keys = new ArrayList<>();
        try (KeyIterator it = sstable.keyIterator())
        {
            while (it.hasNext())
                keys.add(it.next().retainable());
        }
        for (int i = 1; i < keys.size(); i++)
            assertTrue("index keys must be strictly ascending", keys.get(i - 1).compareTo(keys.get(i)) < 0);
        return keys;
    }

    private static String key(int p)
    {
        return String.format("k%06d", p);
    }
}
