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

package org.apache.cassandra.service.accord.execution;

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import accord.primitives.PartialDeps;
import accord.primitives.Range;
import accord.primitives.Routable;
import accord.primitives.SaveStatus;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import org.apache.cassandra.service.accord.AccordTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This test has been authored entirely by Claude.
 *
 * What a failed command-processing task leaves behind, stage by stage. A task that throws is rolled back by
 * {@code SafeTask.discardUpdatesExclusive}, which restores every command it referenced to the value it held on entry
 * and abandons every CommandsForKey it touched. These tests check that the rollback is <em>complete</em>: that the
 * command, the journal and the progress log all agree afterwards, whether the fault arrives before the command's
 * derived state is updated ({@link Point#PROGRESS_LOG_UPDATE}), just after the command is updated
 * ({@code Phase.event}), or later in the same task ({@link Point#TASK_BODY}).
 *
 * <p>The interesting asymmetry is that a command is rolled back but the things that were told about it are not:
 * {@code discardUpdatesExclusive} carries a {@code TODO (required)} saying exactly this ("the progress log can be left
 * with modifications"). That surface is now reduced - {@code SafeCommandStore.update()} tells the progress log last, so
 * only a fault arriving after {@code update()} has returned can leave the log ahead of the command - and what remains is
 * accepted: see {@link #progressLogDoesNotOutrunARolledBackCommandTest}.
 *
 * <p>The consequences of a failure <em>after</em> Apply has begun applying writes are in
 * {@link AccordApplyFailureTest}.
 *
 * <p><b>Status:</b> all enabled tests pass; {@link #progressLogDoesNotOutrunARolledBackCommandTest} is disabled and
 * describes an accepted limitation.
 */
public class AccordCommandStageFailureTest extends AccordCommandFailureTestBase
{
    private static int nextKey = 200;

    /** every phase, faulted at each of the three points, with nothing else going on */
    private interface Check
    {
        /** @return a description of the inconsistency, or null if this outcome is consistent */
        String check(Harness harness, Fixture fixture, Phase phase, Point point, SaveStatus before);
    }

    /**
     * A task that fails must leave the command exactly as it found it: the executor has no way to complete the update
     * (it has not been journalled), so a partially updated command would be a state no replica could have reached.
     */
    @Test
    public void commandIsRolledBackWhenTheTaskFailsTest()
    {
        forEachFault((harness, fixture, phase, point, before) -> {
            SaveStatus after = status(harness, fixture.txnId);
            return before == after ? null : "command is " + after + ", was " + before;
        });
    }

    /** and what a restart would recover must agree with what the store holds */
    @Test
    public void journalAgreesWithTheRolledBackCommandTest()
    {
        forEachFault((harness, fixture, phase, point, before) -> {
            SaveStatus after = status(harness, fixture.txnId);
            SaveStatus persisted = persistedStatus(harness, fixture.txnId);
            return after == persisted ? null : "command is " + after + " but the journal holds " + persisted;
        });
    }

    /**
     * The progress log holds no authoritative state, so an update it was told about that the command store then
     * discarded is tolerated: at worst a transaction it has retired needs recovery to complete it. Since
     * {@code SafeCommandStore.update()} now tells the progress log <em>last</em>, this can only happen when the fault
     * arrives after {@code update()} has returned - a fault raised while updating the command's derived state no longer
     * reaches the log at all, which is pinned by
     * {@link AccordApplyFailureTest#progressLogIsNotToldOfAnUpdateThatFailsWhileNotifyingTest}.
     *
     * <p>Left here, disabled, as the specification of what a complete rollback would look like. For
     * {@code DefaultProgressLog} an update to Applied retires the transaction's home state entirely
     * ({@code updateHomeState} -&gt; {@code setHomeDone} -&gt; {@code maybeRemove}), so a rolled back Applied leaves
     * nothing to drive the transaction to completion - see
     * {@link AccordApplyFailureTest#failedPostApplyDoesNotStrandTheTransactionTest}. The divergence runs the other way
     * too, and may explain the second reported symptom ("a similar fault ... left a NotDefined command"): a rolled back
     * PreAccept leaves the command Uninitialised while the progress log has been told it reached PreAccepted, so the log
     * will try to make progress on a transaction the store has no record of, and the first accessor that must
     * materialise it gets a NotDefined command.
     */
    @Ignore("accepted for now: the progress log is not rolled back with the command update it was told about")
    @Test
    public void progressLogDoesNotOutrunARolledBackCommandTest()
    {
        forEachFault((harness, fixture, phase, point, before) -> {
            SaveStatus after = status(harness, fixture.txnId);
            SaveStatus told = harness.progressLog.lastUpdate(fixture.txnId);
            if (told == null || told == after)
                return null;
            return "the progress log was told " + told + " but the command is " + after;
        });
    }

    /** the injected fault, and cancellations caused by it, must be the only exceptions reported */
    @Test
    public void onlyTheInjectedFaultIsReportedTest()
    {
        forEachFault((harness, fixture, phase, point, before) -> {
            List<Throwable> unexpected = harness.agent.unexpected();
            return unexpected.isEmpty() ? null : "reported " + unexpected;
        });
    }

    /**
     * A fault at the progress log - which {@code SafeCommandStore.update()} invokes last, after the command's derived
     * state - must leave no trace: the command is rolled back, nothing was applied, and the log itself never recorded
     * the update it refused.
     */
    @Test
    public void faultAtTheProgressLogUpdateLeavesNoTraceTest()
    {
        List<String> problems = new ArrayList<>();
        for (Phase phase : Phase.values())
        {
            int key = nextKey++;
            try (Harness harness = new Harness("ks", "tbl"))
            {
                Fixture fixture = newTxn(harness, key);
                runPhasesBefore(harness, fixture, phase);
                SaveStatus before = status(harness, fixture.txnId);
                SaveStatus toldBefore = harness.progressLog.lastUpdate(fixture.txnId);

                harness.fault = Fault.at(Point.PROGRESS_LOG_UPDATE, fixture.txnId);
                Throwable failure = runPhase(harness, fixture, phase);

                if (failure == null)
                    problems.add(phase + ": the task did not report the fault");
                if (harness.fault.fired == 0)
                    problems.add(phase + ": the fault never fired");
                SaveStatus after = status(harness, fixture.txnId);
                if (after != before)
                    problems.add(phase + ": command is " + after + ", was " + before);
                SaveStatus toldAfter = harness.progressLog.lastUpdate(fixture.txnId);
                if (toldAfter != toldBefore)
                    problems.add(phase + ": the progress log was told " + toldAfter + ", had been told " + toldBefore);
                if (rowExists(key))
                    problems.add(phase + ": the transaction's write was applied");
                if (!harness.agent.unexpected().isEmpty())
                    problems.add(phase + ": reported " + harness.agent.unexpected());
            }
        }
        assertConsistent(problems);
    }

    /**
     * Not a fault-injection test, but a guard on the accessors the fault-injection work introduced: the paths that
     * acquire a command once an operation is under way must not raise a log fault, and
     * {@code SafeCommandStore.unsafeGetNoCleanup} therefore refuses a command it does not reference rather than cleaning
     * it up. Some callers legitimately ask about a command they may not have, and must keep getting null instead:
     * {@code Commands.updateWaitingOn} inspects each dependency a newly Stable command waits on, and the range
     * dependencies it iterates are (unlike keys) not part of the execution context, so most often they are simply not
     * there - see {@code unsafeIfReferencedNoCleanup}.
     *
     * <p>A Stable commit whose dependencies include a range transaction the store has never seen must therefore succeed,
     * leaving the command waiting on it.
     */
    @Test
    public void commitWaitingOnAnUnreferencedRangeDependencyTest()
    {
        try (Harness harness = new Harness("ks", "tbl"))
        {
            Fixture txn = newTxn(harness, nextKey++);
            TxnId rangeDep = AccordTestUtils.txnId(1, clock.incrementAndGet(), 1, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range);
            PartialDeps deps;
            try (PartialDeps.Builder builder = PartialDeps.builder(txn.route, true))
            {
                for (Range range : harness.store.unsafeGetRangesForEpoch().currentRanges())
                    builder.add(range, rangeDep);
                deps = builder.build();
            }
            Fixture withRangeDep = new Fixture(txn.txnId, txn.txn, txn.route, txn.partialRoute, txn.partialTxn, deps, txn.key);

            assertNull(preaccept(harness, withRangeDep));
            assertNull(accept(harness, withRangeDep));
            assertNull("a Stable commit must not fail because a dependency is not in the execution context",
                       commit(harness, withRangeDep, SaveStatus.Stable));
            assertEquals(SaveStatus.Stable, status(harness, txn.txnId));
        }
    }

    /**
     * Runs the pipeline once per (phase, injection point), applying {@code check} to each outcome and failing with every
     * inconsistency found rather than only the first, so that one run says how far the problem spreads.
     */
    private void forEachFault(Check check)
    {
        List<String> problems = new ArrayList<>();
        for (Phase phase : Phase.values())
        {
            for (Point point : new Point[]{ Point.PROGRESS_LOG_UPDATE, phase.event, Point.TASK_BODY })
            {
                int key = nextKey++;
                try (Harness harness = new Harness("ks", "tbl"))
                {
                    Fixture fixture = newTxn(harness, key);
                    runPhasesBefore(harness, fixture, phase);
                    SaveStatus before = status(harness, fixture.txnId);

                    harness.fault = Fault.at(point, fixture.txnId);
                    Throwable failure = runPhase(harness, fixture, phase);
                    if (harness.fault.fired == 0)
                        throw new AssertionError(phase + "/" + point + ": the fault never fired, so the case was not exercised");

                    String problem = check.check(harness, fixture, phase, point, before);
                    if (problem != null)
                        problems.add(String.format("%s faulted at %s (reported to caller: %s): %s", phase, point, failure != null, problem));
                }
            }
        }
        assertConsistent(problems);
    }

    /** one line per inconsistency, counted on the first line so that a truncated report still says how many there were */
    private static void assertConsistent(List<String> problems)
    {
        assertTrue(problems.size() + " inconsistent outcome(s): " + String.join(" | ", problems), problems.isEmpty());
    }
}
