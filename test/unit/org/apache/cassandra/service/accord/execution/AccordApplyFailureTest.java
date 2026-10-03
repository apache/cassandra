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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Ignore;
import org.junit.Test;

import accord.primitives.SaveStatus;

import static accord.primitives.SaveStatus.Applied;
import static accord.primitives.SaveStatus.Applying;
import static accord.primitives.SaveStatus.PreApplied;
import static accord.primitives.SaveStatus.ReadyToExecute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This test has been authored entirely by Claude.
 *
 * Failures once Apply has begun. Apply is the one phase whose work escapes the task that started it: it updates the
 * command to Applying, then begins {@code Commands.applyChain}, whose head applies the transaction's mutations and whose
 * tail is a {@code PostApply} continuation that records the command as Applied and notifies whatever was waiting for it.
 * A fault anywhere in that sequence therefore has three parties to keep consistent: the command, the mutations, and the
 * follow-up task.
 *
 * <p>The tests below cover, in order of increasing blast radius:
 * <ul>
 * <li>{@link #mutationContinuationIsCancelledWithItsSubmitterTest} - the mechanism: a continuation submitted through
 *     {@code AccordExecutor} (which is how {@code TxnWrite} submits its mutation) must be cancelled when its submitter
 *     fails, as one submitted through the command store is.</li>
 * <li>{@link #failedApplyDoesNotApplyItsWritesTest} / {@link #failedApplyDoesNotRunPostApplyTest} - the direct
 *     consequence: a failed Apply task must not write to the table, nor run PostApply against the command it just rolled
 *     back.</li>
 * <li>{@link #failedPostApplyRollsBackCleanlyTest} and
 *     {@link #logFaultWhileNotifyingWaitingCommandRollsBackCleanlyTest} - a fault in PostApply, where a transaction's
 *     dependents are notified, must not let a dependent record itself applied over a CommandsForKey that has been rolled
 *     back (which trips CommandsForKey's own linearizability check), and must not reach the progress log with an update
 *     that is discarded.</li>
 * <li>{@link #failedPostApplyDoesNotStrandTheTransactionTest} and
 *     {@link #logFaultWhileNotifyingWaitingCommandDoesNotStrandEitherTransactionTest} - disabled: once the mutations have
 *     been applied there is no recovery story for a failure before the command records it, so the transaction is left at
 *     Applying and any dependent waiting on it is left waiting. Accepted for now.</li>
 * </ul>
 *
 * <p>All of these are variations on the fault reported from production: a {@code logFault} thrown while updating
 * CommandsForKey, after it had notified the commands waiting on the one being applied.
 */
public class AccordApplyFailureTest extends AccordCommandFailureTestBase
{
    private static int nextKey = 300;

    /**
     * {@code TxnWrite.Update.write} submits the mutation with
     * {@code AccordExecutor.continuationChain(Runnable)}. {@code AsyncExecutor.executeContinuation} defaults to
     * degrading to {@code execute}, whose task is an ordinary consequence that
     * {@code Task.cancelSafeTasksAndContinuations} - which cancels a failed task's {@code SafeTask} and
     * <em>continuation</em> consequences - deliberately leaves alone. {@code AccordExecutor} therefore implements
     * {@code executeContinuation} itself, as {@code ExclusiveExecutor} (reached through
     * {@code CommandStore.continuationChain}) does.
     *
     * <p>Everything else in this class follows from this: without it the mutation runs after the task that authorised it
     * has been rolled back, and its completion submits PostApply.
     */
    @Test
    public void mutationContinuationIsCancelledWithItsSubmitterTest()
    {
        try (Harness harness = new Harness("ks", "tbl"))
        {
            Fixture fixture = newTxn(harness, nextKey++);
            AtomicBoolean viaExecutor = new AtomicBoolean();
            AtomicBoolean viaCommandStore = new AtomicBoolean();
            AtomicReference<Throwable> executorFailure = new AtomicReference<>();
            AtomicReference<Throwable> commandStoreFailure = new AtomicReference<>();

            Throwable failure = runTask(harness, fixture.context(), safeStore -> {
                // as TxnWrite.Update.write submits its mutation
                harness.store.executor().continuationChain(() -> viaExecutor.set(true))
                             .begin((success, fail) -> executorFailure.set(fail));
                // and as Commands' own continuations are submitted
                harness.store.continuationChain(() -> viaCommandStore.set(true))
                             .begin((success, fail) -> commandStoreFailure.set(fail));
                throw new InjectedFault("submitter fails after delegating");
            });

            assertTrue("the submitter should have failed", failure != null);
            assertFalse("a continuation submitted through the command store is cancelled with its submitter",
                        viaCommandStore.get());
            assertTrue("expected the command store's continuation to be told it was cancelled, was told "
                       + commandStoreFailure.get(), isCancellation(commandStoreFailure.get()));
            assertFalse("a continuation submitted through the executor must also be cancelled with its submitter:"
                        + " TxnWrite submits the transaction's mutations this way, so a mutation authorised by a command"
                        + " update that was then rolled back would be applied anyway. It ran, and was told "
                        + executorFailure.get(), viaExecutor.get());
            assertTrue("expected the executor's continuation to be told it was cancelled, was told "
                       + executorFailure.get(), isCancellation(executorFailure.get()));
        }
    }

    /**
     * A fault later in the Apply task (in production: a {@code logFault} from a subsequent key's cleanup, or any bug)
     * rolls the command back from Applying to ReadyToExecute, so nothing records that the transaction executed - so the
     * mutation the Applying update authorised must not be applied either.
     */
    @Test
    public void failedApplyDoesNotApplyItsWritesTest()
    {
        int key = nextKey++;
        try (Harness harness = new Harness("ks", "tbl"))
        {
            Fixture fixture = failApply(harness, key, Point.TASK_BODY);

            assertEquals("the command should have been rolled back", ReadyToExecute, status(harness, fixture.txnId));
            assertFalse("the transaction's write must not be applied when the command update that authorised it was"
                        + " rolled back: the store would have no record that the transaction executed, but its writes"
                        + " would be visible", rowExists(key));
        }
    }

    /**
     * Before {@code AccordExecutor.executeContinuation} existed this failed with
     * {@code IllegalArgumentException: Unable to cast Command{...:ReadyToExecute} to Command$Executed} from
     * {@code SafeCommand.applied} - PostApply running against the command the failed task had just rolled back, which is
     * the reported symptom ("a failure in Apply seems to still call PostApply ... encountering the PreAccepted command"),
     * reached here from the same task rather than through a notification.
     */
    @Test
    public void failedApplyDoesNotRunPostApplyTest()
    {
        int key = nextKey++;
        try (Harness harness = new Harness("ks", "tbl"))
        {
            Fixture fixture = failApply(harness, key, Point.TASK_BODY);

            List<Throwable> unexpected = harness.agent.unexpected();
            assertTrue("a failed Apply must not leave PostApply to run: the command is " + status(harness, fixture.txnId)
                       + " and " + unexpected, unexpected.isEmpty());
            assertFalse("and must not report the command applied", harness.sawEvent("Applied", fixture.txnId));
        }
    }

    /**
     * A fault in PostApply must roll back cleanly, even though PostApply is where the transaction's dependents are
     * notified and therefore where a second transaction may be executed. Before
     * {@code AccordExecutor.executeContinuation} existed, the dependent's mutation task survived the failure and its own
     * PostApply recorded it Applied over the rolled back CommandsForKey - a write applied out of order as far as the key
     * is concerned, which {@code CommandsForKey.checkIntegrity} catches with an {@code IllegalStateException}.
     *
     * <p>Two writes on one key, the second dependent on the first and already PreApplied (its Apply arrived while it was
     * still waiting); applying the first notifies the second, and the fault is injected once the first has recorded
     * itself Applied.
     */
    @Test
    public void failedPostApplyRollsBackCleanlyTest()
    {
        int key = nextKey++;
        try (Harness harness = new Harness("ks", "tbl"))
        {
            Pair pair = dependentPair(harness, key);
            harness.fault = Fault.at(Point.ON_APPLIED, pair.first.txnId);
            computeWrites(harness, pair.first);
            apply(harness, pair.first);
            drain(harness);

            String state = state(harness, pair);
            assertTrue("a failed PostApply must not let a dependent transaction record itself applied over the rolled"
                       + " back CommandsForKey. " + state, harness.agent.unexpected().isEmpty());
            assertEquals("the first must be rolled back to Applying. " + state, Applying, status(harness, pair.first.txnId));
            assertEquals("and the second to PreApplied. " + state, PreApplied, status(harness, pair.second.txnId));
            assertFalse("neither may be recorded applied by CommandsForKey. " + state,
                        commandsForKeySummary(harness, pair.first.routingKey()).contains("APPLIED"));
        }
    }

    /**
     * DISABLED: accepted limitation. Once the mutations have been applied, a failure before the command records that has
     * no recovery story - the transaction is left at Applying, with
     * <ul>
     * <li>the mutations applied (the row is visible),</li>
     * <li>the Apply message's caller told the apply succeeded - the fault is in a later task, so the reply is already
     *     sent, and the coordinator counts this replica as applied,</li>
     * <li>and nothing that will retry PostApply.</li>
     * </ul>
     * Left here as the specification of what completing the transaction would have to achieve.
     */
    @Ignore("accepted for now: a failure after the writes have been applied has no recovery story")
    @Test
    public void failedPostApplyDoesNotStrandTheTransactionTest()
    {
        int key = nextKey++;
        try (Harness harness = new Harness("ks", "tbl"))
        {
            Fixture fixture = failApply(harness, key, Point.ON_APPLIED);

            assertTrue("the mutation should have been applied before PostApply failed", rowExists(key));
            SaveStatus status = status(harness, fixture.txnId);
            assertEquals("a transaction whose writes are applied, and whose caller has been told so, must not be left"
                         + " short of Applied: nothing remains that could complete it", Applied, status);
        }
    }

    /**
     * The production shape of {@link #failedPostApplyRollsBackCleanlyTest}: rather than a synthetic fault after
     * {@code applied()}, the fault is a {@link accord.local.LogFaultException} thrown by the CommandsForKey update
     * itself, after it has notified the transactions that were waiting on the one being applied - which is what a
     * {@code Cleanup.logFault} does when the waiting command's record falls in a part of the log that has been marked
     * faulty.
     */
    @Test
    public void logFaultWhileNotifyingWaitingCommandRollsBackCleanlyTest()
    {
        int key = nextKey++;
        try (Harness harness = new Harness("ks", "tbl"))
        {
            Pair pair = logFaultWhileNotifying(harness, key);

            String state = state(harness, pair);
            assertTrue("the sink should have notified the waiting transaction before throwing",
                       harness.sawEvent("notWaiting", pair.second.txnId));
            assertTrue("no invariant may be tripped by the work the notification set in motion. " + state,
                       harness.agent.unexpected().isEmpty());
            assertEquals("the first must be rolled back to Applying. " + state, Applying, status(harness, pair.first.txnId));
            assertEquals("and the second to PreApplied. " + state, PreApplied, status(harness, pair.second.txnId));
            assertFalse("neither may be recorded applied by CommandsForKey. " + state,
                        commandsForKeySummary(harness, pair.first.routingKey()).contains("APPLIED"));
        }
    }

    /**
     * {@code SafeCommandStore.update()} invokes the progress log last, so a fault raised while updating the command's
     * derived state - here from the CommandsForKey notification - must not reach the log at all: it must not be left
     * believing the command reached the status that was discarded. (For {@code DefaultProgressLog} being told Applied
     * retires the transaction, which is what would otherwise remove the last thing that could recover it.)
     */
    @Test
    public void progressLogIsNotToldOfAnUpdateThatFailsWhileNotifyingTest()
    {
        int key = nextKey++;
        try (Harness harness = new Harness("ks", "tbl"))
        {
            Pair pair = logFaultWhileNotifying(harness, key);

            assertEquals("the progress log must not have been told about the update the fault discarded. " + state(harness, pair),
                         status(harness, pair.first.txnId), harness.progressLog.lastUpdate(pair.first.txnId));
        }
    }

    /**
     * DISABLED: accepted limitation, and the same one as
     * {@link #failedPostApplyDoesNotStrandTheTransactionTest}, seen from the dependent's side. The notification that
     * would have executed the second transaction is discarded with the update that was making it, so the second is left
     * waiting on a transaction that will never be recorded applied.
     */
    @Ignore("accepted for now: a failure after the writes have been applied has no recovery story")
    @Test
    public void logFaultWhileNotifyingWaitingCommandDoesNotStrandEitherTransactionTest()
    {
        int key = nextKey++;
        try (Harness harness = new Harness("ks", "tbl"))
        {
            Pair pair = logFaultWhileNotifying(harness, key);

            String state = state(harness, pair);
            assertEquals("the first applied its writes, so it must end Applied. " + state,
                         Applied, status(harness, pair.first.txnId));
            assertEquals("and the second must be executed by the notification, not left waiting. " + state,
                         Applied, status(harness, pair.second.txnId));
        }
    }

    /** the transaction reaches Applying, applies its writes, and PostApply finishes it, when nothing fails */
    @Test
    public void appliesWhenNothingFailsTest()
    {
        int key = nextKey++;
        try (Harness harness = new Harness("ks", "tbl"))
        {
            Fixture fixture = newTxn(harness, key);
            runPhasesBefore(harness, fixture, Phase.APPLY);
            computeWrites(harness, fixture);
            assertNull(apply(harness, fixture));
            drain(harness);

            assertEquals(Applied, status(harness, fixture.txnId));
            assertEquals(Applied, persistedStatus(harness, fixture.txnId));
            assertTrue(rowExists(key));
            assertTrue(harness.agent.unexpected().isEmpty());
            assertEquals(Applied, harness.progressLog.lastUpdate(fixture.txnId));
        }
    }

    /**
     * The cascade the failure tests interrupt, when nothing fails: applying the first notifies the second through
     * CommandsForKey, which executes it, applies its mutation and records it Applied - all as consequences of the first's
     * PostApply task.
     */
    @Test
    public void dependentPairAppliesWhenNothingFailsTest()
    {
        int key = nextKey++;
        try (Harness harness = new Harness("ks", "tbl"))
        {
            Pair pair = dependentPair(harness, key);
            computeWrites(harness, pair.first);
            assertNull(apply(harness, pair.first));
            drain(harness);

            assertEquals(Applied, status(harness, pair.first.txnId));
            assertEquals("the second should have been notified and applied", Applied, status(harness, pair.second.txnId));
            assertTrue(rowExists(key));
            assertTrue(harness.agent.unexpected().isEmpty());
        }
    }

    // ---- scenarios

    /** the dependent pair, with a log fault thrown by the CommandsForKey update once it has notified the second */
    private Pair logFaultWhileNotifying(Harness harness, int key)
    {
        Pair pair = dependentPair(harness, key);
        installThrowingNotifySink(harness, pair.first);
        harness.fault = Fault.at(Point.CFK_NOTIFY, pair.second.txnId).asLogFault();

        computeWrites(harness, pair.first);
        apply(harness, pair.first);
        drain(harness);
        assertEquals("the fault should have fired exactly once", 1, harness.fault.fired);
        return pair;
    }

    private String state(Harness harness, Pair pair)
    {
        return "first=" + status(harness, pair.first.txnId) + " second=" + status(harness, pair.second.txnId)
               + " commandsForKey=[" + commandsForKeySummary(harness, pair.first.routingKey()) + ']'
               + " progressLog: first=" + harness.progressLog.lastUpdate(pair.first.txnId)
               + " second=" + harness.progressLog.lastUpdate(pair.second.txnId)
               + " reported " + harness.agent.unexpected();
    }

    /** preaccept -> accept -> stable -> Apply, with {@code point} faulted for this transaction only */
    private Fixture failApply(Harness harness, int key, Point point)
    {
        Fixture fixture = newTxn(harness, key);
        runPhasesBefore(harness, fixture, Phase.APPLY);
        assertEquals(ReadyToExecute, status(harness, fixture.txnId));

        harness.fault = Fault.at(point, fixture.txnId);
        computeWrites(harness, fixture);
        apply(harness, fixture);
        drain(harness);
        assertEquals("the fault should have fired exactly once", 1, harness.fault.fired);
        return fixture;
    }

    private static class Pair
    {
        final Fixture first;
        final Fixture second;

        Pair(Fixture first, Fixture second)
        {
            this.first = first;
            this.second = second;
        }
    }

    /**
     * Two writes on one key, the second depending on the first. Both are Stable; the second's Apply arrives first, so it
     * is PreApplied and waiting on the first, and applying the first is what will execute it.
     */
    private Pair dependentPair(Harness harness, int key)
    {
        Fixture first = newTxn(harness, key);
        runPhasesBefore(harness, first, Phase.APPLY);

        Fixture second = newTxn(harness, key, first.txnId);
        runPhasesBefore(harness, second, Phase.APPLY);
        computeWrites(harness, second);
        assertNull(apply(harness, second));

        assertEquals(ReadyToExecute, status(harness, first.txnId));
        assertEquals("the second must be waiting on the first", PreApplied, status(harness, second.txnId));
        assertFalse("nothing should have been applied yet", rowExists(key));
        return new Pair(first, second);
    }
}
