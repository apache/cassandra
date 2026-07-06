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

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.AsyncExecutor;
import accord.api.ExclusiveAsyncExecutor;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.Coordinations;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultLocalListeners.NotifySink;
import accord.impl.DefaultRemoteListeners;
import accord.impl.TestAgent;
import accord.impl.basic.InMemoryJournal;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.ExecutionContext;
import accord.local.LoadKeys;
import accord.local.LoadKeysFor;
import accord.local.Node.Id;
import accord.local.NodeCommandStoreService;
import accord.local.SafeCommandStore;
import accord.local.TimeService;
import accord.local.durability.DurabilityService;
import accord.primitives.Ballot;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.TopologyManager;
import accord.utils.DefaultRandom;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.concurrent.Condition;

import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This test has been authored entirely by Claude.
 *
 * A non-{@link SafeTask} consequence is ordinarily left alone when its submitter fails while running, because plain work
 * is not necessarily part of what the submitter was doing and may have nowhere to report a cancellation - see
 * {@link Task#cancelSafeTasksAndContinuations}. A <em>continuation</em> opts in: it exists only to complete what its
 * submitter started, so it is cancelled with it, and its {@code RunOrFail} is failed so that whatever waits on the chain
 * is told.
 *
 * <p>These tests pin the three behaviours the opt-in depends on: a continuation of a failed task is cancelled and never
 * runs; an ordinary submission in the same position still runs; and a continuation submitted with no running task to
 * attach to degrades to an ordinary submission rather than failing - the path taken by {@code TxnWrite.applyDirect} and
 * {@code TxnRead.readDirect} when reached from the message thread (via {@code overrideWithSynchronousApply}) rather than
 * from within a command store task.
 */
public class AccordExecutorContinuationCancellationTest
{
    private static final int TIMEOUT_SECONDS = 30;
    private static final TableId TABLE_ID = TableId.fromUUID(new java.util.UUID(0, 1));

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /** a continuation of a task that fails while running is cancelled: it never runs, and its chain is failed */
    @Test
    public void continuationOfFailedParentIsCancelledTest() throws InterruptedException
    {
        Outcome outcome = nested(true, true);
        assertFalse("the continuation should not have run", outcome.ran);
        assertTrue("expected a CancellationException, found " + outcome.failure,
                   outcome.failure instanceof CancellationException);
    }

    /** an ordinary (non-continuation) submission in the same position is unaffected by its submitter's failure */
    @Test
    public void plainConsequenceOfFailedParentStillRunsTest() throws InterruptedException
    {
        Outcome outcome = nested(true, false);
        assertTrue("the plain consequence should have run", outcome.ran);
        assertNull("the plain consequence should not have been failed", outcome.failure);
    }

    /** and a continuation of a submitter that succeeds runs normally */
    @Test
    public void continuationOfSucceedingParentRunsTest() throws InterruptedException
    {
        Outcome outcome = nested(false, true);
        assertTrue("the continuation should have run", outcome.ran);
        assertNull("the continuation should not have been failed", outcome.failure);
    }

    /**
     * With no running task to attach to there is no submitter that could cancel us, so a continuation must degrade to
     * an ordinary submission.
     */
    @Test
    public void continuationWithNoParentIsSubmittedTest() throws InterruptedException
    {
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Condition done = Condition.newOneTimeCondition();

        withStore(store -> {
            // submitted from the test thread, i.e. not from within any task on this executor
            store.continuationChain(() -> ran.set(true))
                 .begin((success, fail) -> { failure.set(fail); done.signal(); });
            await(done, "the continuation was never notified");
        });

        assertTrue("the continuation should have run", ran.get());
        assertNull("the continuation should not have been failed", failure.get());
    }

    /**
     * A {@link PlainChain} reports the failure of its own body through its chain rather than by throwing, so it needs to
     * record that failure itself for the executor's benefit - otherwise it completes as a success and any continuation it
     * had already delegated work to survives a submitter that failed.
     */
    @Test
    public void continuationOfFailedChainBodyIsCancelledTest() throws InterruptedException
    {
        AtomicBoolean innerRan = new AtomicBoolean();
        AtomicReference<Throwable> innerFailure = new AtomicReference<>();
        AtomicReference<Throwable> outerFailure = new AtomicReference<>();
        Condition innerDone = Condition.newOneTimeCondition();
        Condition outerDone = Condition.newOneTimeCondition();

        withStore(store -> {
            store.chain(() -> {
                // delegated from within the outer chain's run, so attached to it as a consequence
                store.continuationChain(() -> innerRan.set(true))
                     .begin((success, fail) -> { innerFailure.set(fail); innerDone.signal(); });
                throw new RuntimeException("outer chain body fails");
            }).begin((success, fail) -> { outerFailure.set(fail); outerDone.signal(); });

            await(outerDone, "the outer chain was never notified");
            await(innerDone, "the inner continuation was never notified");
        });

        // the outer chain's own failure is reported through its chain, exactly once
        assertTrue("expected the outer chain to be failed, found " + outerFailure.get(),
                   outerFailure.get() instanceof RuntimeException);
        assertFalse("the continuation should not have run", innerRan.get());
        assertTrue("expected a CancellationException, found " + innerFailure.get(),
                   innerFailure.get() instanceof CancellationException);
    }

    /**
     * Drain must not refuse a continuation. A continuation continues work that has already begun and cannot be
     * abandoned - that is what distinguishes it from an ordinary submission, and why it is admitted where another task
     * would be refused - so stopping the store between its submitter completing and the continuation running must not
     * turn it into a {@code RejectedExecutionException}. Refusing it leaves the work its submitter started with nobody
     * to finish it: for {@code Commands.applyChain}, the writes have been applied and the continuation is what records
     * that they were.
     *
     * <p>{@code ExclusiveExecutor.reject} rejects every non-{@code SafeTask} outright, and a continuation submitted
     * through {@code continuationChain}/{@code executeContinuation} is a {@code PlainChain}, so it is refused today.
     */
    @Test
    public void continuationIsNotRefusedAtDrainTest() throws InterruptedException
    {
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Condition parentDone = Condition.newOneTimeCondition();
        Condition continuationDone = Condition.newOneTimeCondition();

        withStore(store -> {
            TxnId txnId = TxnId.fromValues(1, 1, 0, new Id(1));
            RoutingKey key = new TokenKey(TABLE_ID, DatabaseDescriptor.getPartitioner().getToken(Int32Type.instance.decompose(0)));
            ExecutionContext parentContext = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(key), LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "parent");
            store.execute(parentContext, (Consumer<? super SafeCommandStore>) safeStore -> {
                store.continuationChain(() -> ran.set(true))
                     .begin((success, fail) -> { failure.set(fail); continuationDone.signal(); });
                // we hold the run slot, so the store is stopped between our completion and the continuation's run
                store.exclusiveExecutor().stop();
            }, (success, fail) -> parentDone.signal());

            await(parentDone, "the parent was never notified");
            await(continuationDone, "the continuation was never notified");
        });

        assertTrue("a continuation must be admitted at drain: it completes work that has already begun. It was told "
                   + failure.get(), ran.get());
        assertNull("and must not be failed, but was told " + failure.get(), failure.get());
    }

    private static class Outcome
    {
        final boolean ran;
        final Throwable failure;

        Outcome(boolean ran, Throwable failure)
        {
            this.ran = ran;
            this.failure = failure;
        }
    }

    /**
     * Submit a nested chain from within a task on the same command store, so that it is attached to that task as a
     * consequence, then optionally fail the submitter while it is still running.
     */
    private Outcome nested(boolean parentFails, boolean asContinuation) throws InterruptedException
    {
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Throwable> nestedFailure = new AtomicReference<>();
        Condition parentDone = Condition.newOneTimeCondition();
        Condition nestedDone = Condition.newOneTimeCondition();

        withStore(store -> {
            TxnId txnId = TxnId.fromValues(1, 1, 0, new Id(1));
            RoutingKey key = new TokenKey(TABLE_ID, DatabaseDescriptor.getPartitioner().getToken(Int32Type.instance.decompose(0)));
            ExecutionContext parentContext = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(key), LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "parent");
            store.execute(parentContext, (Consumer<? super SafeCommandStore>) safeStore -> {
                Runnable run = () -> ran.set(true);
                // submitted from within the parent's run, on the parent's own executor, so it is attached to the parent
                (asContinuation ? store.continuationChain(run) : store.chain(run))
                    .begin((success, fail) -> { nestedFailure.set(fail); nestedDone.signal(); });
                if (parentFails)
                    throw new RuntimeException("parent fails");
            }, (success, fail) -> parentDone.signal());

            await(parentDone, "the parent was never notified");
            await(nestedDone, "the nested task was never notified");
        });

        return new Outcome(ran.get(), nestedFailure.get());
    }

    private void withStore(Consumer<AccordCommandStore> test) throws InterruptedException
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, new TestAgent());
        executor.cacheUnsafe().types().forEach(type -> type.unsafeSetLoadFunction((ignoreStore, ignoreKey) -> null));
        AccordCommandStore store = commandStore(TABLE_ID, partitioner, executor);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        try
        {
            test.accept(store);
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static void await(Condition condition, String message)
    {
        try
        {
            if (!condition.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                throw new AssertionError(message);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * A command store with an in-memory journal and no persistence, so that we need no schema, cluster metadata or
     * commit log. We deliberately do not use {@code AccordAgent}, as reporting an exception there initialises
     * {@code AccordSystemMetrics}, which requires a started {@code AccordService}.
     */
    private static AccordCommandStore commandStore(TableId tableId, IPartitioner partitioner, AccordExecutor executor)
    {
        AtomicLong clock = new AtomicLong();
        LongSupplier now = clock::incrementAndGet;
        Id nodeId = new Id(1);
        NodeCommandStoreService node = new NodeCommandStoreService()
        {
            private final ToLongFunction<TimeUnit> elapsed = TimeService.elapsedWrapperFromNonMonotonicSource(TimeUnit.MICROSECONDS, this::now);
            private long stamp = 0;

            @Override public AsyncExecutor someExecutor() { return null; }
            @Override public ExclusiveAsyncExecutor someExclusiveExecutor() { return null; }
            @Override public accord.api.Timeouts timeouts() { return null; }
            @Override public DurableBefore durableBefore() { return DurableBefore.EMPTY; }
            @Override public DurabilityService durability() { return null; }
            @Override public Id id() { return nodeId; }
            @Override public long epoch() { return 1; }
            @Override public long now() { return now.getAsLong(); }
            @Override public long uniqueNow(long atLeast) { return now.getAsLong(); }
            @Override public long elapsed(TimeUnit units) { return elapsed.applyAsLong(units); }
            @Override public TopologyManager topology() { throw new UnsupportedOperationException(); }
            @Override public Coordinations coordinations() { return new Coordinations(); }
            @Override public long currentStamp() { return stamp; }
            @Override public void updateStamp() { ++stamp; }
            @Override public boolean isReplaying() { return false; }
            @Override public void reportLocalExecution(TxnId txnId, Route<?> route, Ballot ballot, Timestamp applyAt, Writes writes, Result result) {}
        };

        return new AccordCommandStore(0, node, new TestAgent(), null,
                                      cs -> new ProgressLog.NoOpProgressLog(),
                                      cs -> new DefaultLocalListeners(null, new DefaultRemoteListeners.NoOpRemoteListeners(), new NotifySink.NoOpNotifySink()),
                                      new RangesForEpoch(1, Ranges.of(TokenRange.fullRange(tableId, partitioner))),
                                      new InMemoryJournal(nodeId, new DefaultRandom(1)),
                                      executor);
    }
}
