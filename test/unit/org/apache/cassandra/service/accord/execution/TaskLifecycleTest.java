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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.RoutingKey;
import accord.local.ExecutionContext;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SafeCommandStore;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.Cancellable;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.execution.AccordExecutor.Mode;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITH_LOCK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test has been authored entirely by Claude.
 *
 * Lifecycle tests for {@link Task} and its interaction with {@link AccordExecutor}, covering the invariants the
 * executor's callers rely on:
 * <ul>
 *   <li>every task is notified exactly once, and every {@code chain()} eventually completes (success, failure or
 *       cancellation) - a task that is dropped silently hangs its caller forever;</li>
 *   <li>a task is completed exactly once, so {@code tasks}/{@link Tranches} accounting returns to zero and
 *       {@link AccordExecutor#hasTasks()} becomes false (otherwise {@code waitForQuiescence} and
 *       {@code afterSubmittedAndConsequences} never fire again);</li>
 *   <li>a task always releases its cache references, even when it fails;</li>
 *   <li>no failure is reported to the {@link accord.api.Agent} on any of these paths - an agent exception here means
 *       an internal invariant was broken, not that the user's operation failed.</li>
 * </ul>
 *
 * Each test creates a private executor + command store so that failures cannot leak between tests.
 */
public class TaskLifecycleTest
{
    private static final long TIMEOUT_SECONDS = 30;
    private static final AtomicLong clock = new AtomicLong(0);

    private final List<AccordExecutor> executors = new CopyOnWriteArrayList<>();
    private final List<AccordCommandStore> stores = new CopyOnWriteArrayList<>();

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
        AccordService.unsafeSetNoop();
    }

    @After
    public void after()
    {
        stores.forEach(AccordCommandStore::shutdown);
        stores.clear();
        executors.forEach(AccordExecutor::shutdown);
        executors.clear();
    }

    /**
     * An executor plus one command store, and a record of everything reported to the agent.
     */
    private class Env
    {
        final List<Throwable> agentExceptions = new CopyOnWriteArrayList<>();
        final AccordExecutor executor;
        final AccordCommandStore store;

        Env(Mode mode)
        {
            AccordAgent agent = new AccordAgent()
            {
                @Override
                public void onException(Throwable t)
                {
                    agentExceptions.add(t);
                }

                @Override
                public void onException(Throwable t, String context)
                {
                    agentExceptions.add(t);
                }
            };
            agent.setup(Id.NONE);
            this.executor = new AccordExecutorSyncSubmit(0, mode, "TaskLifecycleTest", agent);
            executors.add(executor);
            this.store = newStore(executor);
            stores.add(store);
        }

        void assertQuiescent()
        {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (executor.hasTasks() && System.nanoTime() < deadline)
                Thread.yield();
            assertThat(executor.hasTasks()).describedAs("executor still has registered tasks").isFalse();
            assertThat(executor.unsafeRunningCount()).describedAs("tasks still assigned to a runner").isZero();
        }

        void assertNoAgentExceptions()
        {
            assertThat(agentExceptions).describedAs("internal failures reported to the agent").isEmpty();
        }
    }

    private static AccordCommandStore newStore(AccordExecutor executor)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata("ks", "tbl");
        TokenRange range = TokenRange.fullRange(metadata.id, Murmur3Partitioner.instance);
        Node.Id node = new Id(1);
        Topology topology = new Topology(1, Shard.create(range, new SortedArrayList<>(new Id[]{ node }), Sets.newHashSet(node)));
        AccordCommandStore store = AccordTestUtils.createAccordCommandStore(node, clock::incrementAndGet, topology, executor);
        // NOTE: capacity must be set via the executor, so that its derived maxWorkingCapacityInBytes is refreshed
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(1 << 20);
            executor.setWorkingSetSize(1 << 20);
        });
        return store;
    }

    private TxnId nextTxnId()
    {
        return AccordTestUtils.txnId(1, clock.incrementAndGet(), 1);
    }

    private static Consumer<? super SafeCommandStore> noop()
    {
        return ignore -> {};
    }

    private static void await(CountDownLatch latch, String what) throws InterruptedException
    {
        assertThat(latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).describedAs(what).isTrue();
    }

    /**
     * A task that fails must cancel *every* consequence it accumulated, and must still be unregistered.
     */
    @Test
    public void failedTaskCancelsAllConsequences() throws Throwable
    {
        Env env = new Env(RUN_WITH_LOCK);
        int count = 4;
        List<AtomicReference<Throwable>> children = new ArrayList<>();
        CountDownLatch childrenDone = CountDownLatch.newCountDownLatch(count);
        for (int i = 0 ; i < count ; ++i)
            children.add(new AtomicReference<>());

        AtomicReference<Throwable> parentFailure = new AtomicReference<>();
        CountDownLatch parentDone = CountDownLatch.newCountDownLatch(1);
        RuntimeException failure = new RuntimeException("deliberate failure");

        env.store.execute(ExecutionContext.unsequenced(nextTxnId(), "parent"), (Consumer<? super SafeCommandStore>) safe -> {
            for (int i = 0 ; i < count ; ++i)
            {
                AtomicReference<Throwable> child = children.get(i);
                env.store.execute(ExecutionContext.unsequenced(nextTxnId(), "child" + i), noop(),
                                  (r, f) -> { child.set(f == null ? new AssertionError("child should not have run") : f); childrenDone.decrement(); });
            }
            throw failure;
        }, (r, f) -> { parentFailure.set(f); parentDone.decrement(); });

        await(parentDone, "parent was notified");
        assertThat(parentFailure.get()).isSameAs(failure);
        await(childrenDone, "every consequence was notified");
        for (int i = 0 ; i < count ; ++i)
            assertThat(children.get(i).get()).describedAs("child" + i).isInstanceOf(CancellationException.class);

        env.assertNoAgentExceptions();
        env.assertQuiescent();
    }

    /**
     * The happy path counterpart: consequences of a successful task all run.
     */
    @Test
    public void successfulTaskSubmitsAllConsequences() throws Throwable
    {
        Env env = new Env(RUN_WITH_LOCK);
        int count = 4;
        AtomicInteger ran = new AtomicInteger();
        CountDownLatch childrenDone = CountDownLatch.newCountDownLatch(count);
        CountDownLatch parentDone = CountDownLatch.newCountDownLatch(1);

        env.store.execute(ExecutionContext.unsequenced(nextTxnId(), "parent"), (Consumer<? super SafeCommandStore>) safe -> {
            for (int i = 0 ; i < count ; ++i)
                env.store.execute(ExecutionContext.unsequenced(nextTxnId(), "child" + i),
                                  (Consumer<? super SafeCommandStore>) s -> ran.incrementAndGet(),
                                  (r, f) -> { assertThat(f).isNull(); childrenDone.decrement(); });
        }, (r, f) -> { assertThat(f).isNull(); parentDone.decrement(); });

        await(parentDone, "parent was notified");
        await(childrenDone, "every consequence was notified");
        assertThat(ran.get()).isEqualTo(count);
        env.assertNoAgentExceptions();
        env.assertQuiescent();
    }

    /**
     * Cancelling one consequence must not disturb its siblings. Here the cancellation is submitted from within the
     * parent, so it is applied after the parent has completed (i.e. once the consequence has been submitted).
     */
    @Test
    public void cancellingOneConsequenceDoesNotAffectSiblings() throws Throwable
    {
        Env env = new Env(RUN_WITH_LOCK);
        AtomicReference<Throwable> cancelled = new AtomicReference<>();
        CountDownLatch cancelledDone = CountDownLatch.newCountDownLatch(1);
        CountDownLatch siblingDone = CountDownLatch.newCountDownLatch(1);
        CountDownLatch parentDone = CountDownLatch.newCountDownLatch(1);

        env.store.execute(ExecutionContext.unsequenced(nextTxnId(), "parent"), (Consumer<? super SafeCommandStore>) safe -> {
            Cancellable cancel = env.store.execute(ExecutionContext.unsequenced(nextTxnId(), "cancelled"), noop(),
                                                   (r, f) -> { cancelled.set(f); cancelledDone.decrement(); });
            env.store.execute(ExecutionContext.unsequenced(nextTxnId(), "sibling"), noop(),
                              (r, f) -> { assertThat(f).isNull(); siblingDone.decrement(); });
            cancel.cancel();
        }, (r, f) -> { assertThat(f).isNull(); parentDone.decrement(); });

        await(parentDone, "parent was notified");
        await(siblingDone, "sibling of the cancelled consequence ran");
        await(cancelledDone, "cancelled consequence was notified");
        assertThat(cancelled.get()).isInstanceOf(CancellationException.class);
        env.assertNoAgentExceptions();
        env.assertQuiescent();
    }

    /**
     * As above, but the cancellation arrives from a foreign thread while the parent is still running, so the
     * consequence is terminated before it has ever been submitted.
     */
    @Test
    public void externallyCancellingOneConsequenceDoesNotAffectSiblings() throws Throwable
    {
        Env env = new Env(RUN_WITHOUT_LOCK); // the parent blocks, so it must not hold the executor lock
        AtomicReference<Cancellable> toCancel = new AtomicReference<>();
        AtomicReference<Throwable> cancelled = new AtomicReference<>();
        CountDownLatch cancelledDone = CountDownLatch.newCountDownLatch(1);
        CountDownLatch siblingDone = CountDownLatch.newCountDownLatch(1);
        CountDownLatch parentDone = CountDownLatch.newCountDownLatch(1);
        CountDownLatch consequencesAdded = CountDownLatch.newCountDownLatch(1);
        CountDownLatch releaseParent = CountDownLatch.newCountDownLatch(1);

        env.store.execute(ExecutionContext.unsequenced(nextTxnId(), "parent"), (Consumer<? super SafeCommandStore>) safe -> {
            toCancel.set(env.store.execute(ExecutionContext.unsequenced(nextTxnId(), "cancelled"), noop(),
                                           (r, f) -> { cancelled.set(f); cancelledDone.decrement(); }));
            env.store.execute(ExecutionContext.unsequenced(nextTxnId(), "sibling"), noop(),
                              (r, f) -> { assertThat(f).isNull(); siblingDone.decrement(); });
            consequencesAdded.decrement();
            releaseParent.awaitUninterruptibly();
        }, (r, f) -> { assertThat(f).isNull(); parentDone.decrement(); });

        await(consequencesAdded, "consequences were registered");
        toCancel.get().cancel();
        releaseParent.decrement();

        await(parentDone, "parent was notified");
        await(siblingDone, "sibling of the cancelled consequence ran");
        await(cancelledDone, "cancelled consequence was notified");
        assertThat(cancelled.get()).isInstanceOf(CancellationException.class);
        env.assertNoAgentExceptions();
        env.assertQuiescent();
    }

    /**
     * A task whose prepare fails must be failed and completed, and the {@link ExclusiveExecutor} it was dispatched
     * from must go on to dispatch the next task.
     */
    @Test
    public void prepareFailureCompletesTaskAndDispatchesNext() throws Throwable
    {
        Env env = new Env(RUN_WITH_LOCK);
        ExclusiveExecutor exclusive = env.executor.newExclusiveExecutor(0);

        CountDownLatch failed = CountDownLatch.newCountDownLatch(1);
        CountDownLatch ran = CountDownLatch.newCountDownLatch(1);
        RuntimeException failure = new RuntimeException("deliberate prepare failure");
        TestTask first = new TestTask(env.executor, exclusive, failure, failed);
        TestTask second = new TestTask(env.executor, exclusive, null, ran);

        env.executor.executeDirectlyWithLock(() -> {
            first.submitExclusiveNoExcept();
            second.submitExclusiveNoExcept();
        });

        await(failed, "the task that failed to prepare was notified");
        assertThat(first.failure).isSameAs(failure);
        await(ran, "the next queued task was dispatched");
        env.assertNoAgentExceptions();
        env.assertQuiescent();
    }

    /**
     * A chain submitted to a command store hosted by a *different* executor must be submitted to that executor
     * independently, not attached as a consequence of the running task (whose executor's lock we hold).
     */
    @Test
    public void consequenceOnAnotherExecutorIsSubmittedIndependently() throws Throwable
    {
        Env a = new Env(RUN_WITHOUT_LOCK);
        Env b = new Env(RUN_WITHOUT_LOCK);

        CountDownLatch childDone = CountDownLatch.newCountDownLatch(1);
        CountDownLatch parentDone = CountDownLatch.newCountDownLatch(1);

        a.store.execute(ExecutionContext.unsequenced(nextTxnId(), "parentOnA"), (Consumer<? super SafeCommandStore>) safe -> {
            b.store.execute(ExecutionContext.unsequenced(nextTxnId(), "childOnB"), noop(),
                            (r, f) -> { assertThat(f).isNull(); childDone.decrement(); });
        }, (r, f) -> { assertThat(f).isNull(); parentDone.decrement(); });

        await(parentDone, "parent was notified");
        await(childDone, "child on the other executor ran");
        a.assertNoAgentExceptions();
        b.assertNoAgentExceptions();
        a.assertQuiescent();
        b.assertQuiescent();
    }

    /**
     * An incremental task with more keys than a single batch can hold must run several batches and then complete once.
     */
    @Test
    public void incrementalTaskRunsMultipleBatches() throws Throwable
    {
        Env env = new Env(RUN_WITH_LOCK);
        AtomicInteger batches = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = CountDownLatch.newCountDownLatch(1);

        env.store.execute(ExecutionContext.unsequencedIncrementalWrite(keys(0, 200), "incremental"),
                          (Consumer<? super SafeCommandStore>) safe -> batches.incrementAndGet(),
                          (r, f) -> { failure.set(f); done.decrement(); });

        await(done, "incremental task completed");
        assertThat(failure.get()).isNull();
        assertThat(batches.get()).describedAs("expected several batches").isGreaterThan(1);
        env.assertNoAgentExceptions();
        env.assertQuiescent();
    }

    /**
     * An incremental task can be failed while it is parked between batches - this is what a late failure of one of
     * its (optional) key loads does, see {@link AccordExecutor#onLoadedExclusive}. It must release its cache
     * references, exactly as it would if it were failed before it first ran.
     */
    @Test
    public void incrementalTaskFailedBetweenBatchesReleasesResources() throws Throwable
    {
        Env env = new Env(RUN_WITHOUT_LOCK);
        AtomicInteger batches = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = CountDownLatch.newCountDownLatch(1);
        RuntimeException loadFailure = new RuntimeException("simulated load failure");

        Cancellable submitted =
            env.store.execute(ExecutionContext.unsequencedIncrementalWrite(keys(1000, 1200), "incremental"),
                              (Consumer<? super SafeCommandStore>) safe -> {
                                  batches.incrementAndGet();
                                  try { Thread.sleep(5); } catch (InterruptedException e) { throw new RuntimeException(e); }
                              },
                              (r, f) -> { failure.set(f); done.decrement(); });

        SafeTask<?> task = (SafeTask<?>) submitted;
        AtomicReference<Task.State> failedIn = new AtomicReference<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (failedIn.get() == null && failure.get() == null && System.nanoTime() < deadline)
        {
            env.executor.executeDirectlyWithLock(() -> {
                Task.State state = task.state();
                if (batches.get() > 0 && task.isState(Task.State.WAITING))
                {
                    failedIn.set(state);
                    task.tryFailAndCompleteUnexecutedExclusive(loadFailure, Task.State.FAILED);
                }
            });
        }

        assertThat(failedIn.get()).describedAs("did not observe the task parked between batches").isNotNull();
        await(done, "task was notified of the failure");
        assertThat(failure.get()).isSameAs(loadFailure);
        int stillHeld = task.refs == null ? 0 : task.refs.size();
        assertThat(stillHeld).describedAs("cache references were not released").isZero();
        env.assertNoAgentExceptions();
        env.assertQuiescent();
    }

    /**
     * The executor throttles loading while the cache is over its working-set budget, but must always be able to make
     * *some* progress: if the only work that can run is loading, it must run regardless of the budget.
     */
    @Test
    public void progressesWithZeroCacheCapacity() throws Throwable
    {
        Env env = new Env(RUN_WITH_LOCK);
        env.executor.executeDirectlyWithLock(() -> {
            env.executor.setCapacity(0);
            env.executor.setWorkingSetSize(0);
        });

        for (int i = 0 ; i < 2 ; ++i)
        {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch done = CountDownLatch.newCountDownLatch(1);
            env.store.execute(ExecutionContext.unsequencedIncrementalWrite(keys(2000 + i * 200, 2200 + i * 200), "zeroCapacity" + i),
                              noop(), (r, f) -> { failure.set(f); done.decrement(); });
            await(done, "task completed with a zero capacity cache");
            assertThat(failure.get()).isNull();
        }

        env.assertNoAgentExceptions();
        env.assertQuiescent();
    }

    private static RoutingKeys keys(int from, int to)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata("ks", "tbl");
        List<RoutingKey> keys = new ArrayList<>(to - from);
        for (int i = from ; i < to ; ++i)
            keys.add(AccordTestUtils.key(metadata, i).toUnseekable());
        return RoutingKeys.of(keys);
    }

    /**
     * A minimal {@link Plain} task: optionally fails during prepare, otherwise records that it ran.
     */
    private static class TestTask extends Plain
    {
        final ExclusiveExecutor exclusiveExecutor;
        final RuntimeException failPrepareWith;
        final CountDownLatch notified;
        volatile Throwable failure;

        TestTask(AccordExecutor executor, ExclusiveExecutor exclusiveExecutor, RuntimeException failPrepareWith, CountDownLatch notified)
        {
            super(executor, ExclusiveGroup.OTHER);
            this.exclusiveExecutor = exclusiveExecutor;
            this.failPrepareWith = failPrepareWith;
            this.notified = notified;
        }

        @Override
        ExclusiveExecutor exclusiveExecutor()
        {
            return exclusiveExecutor;
        }

        @Override
        void prepareExclusiveMayThrow()
        {
            if (failPrepareWith != null)
                throw failPrepareWith;
        }

        @Override
        boolean runMayThrow()
        {
            if (failPrepareWith != null)
                throw new AssertionError("should not have run");
            notified.decrement();
            return true;
        }

        @Override
        void reportFailureMayThrow(Throwable fail)
        {
            failure = fail;
            notified.decrement();
        }

        @Override
        public String description()
        {
            return "TestTask[failPrepare=" + (failPrepareWith != null) + ']';
        }

        @Override
        String briefDescription()
        {
            return description();
        }
    }
}
