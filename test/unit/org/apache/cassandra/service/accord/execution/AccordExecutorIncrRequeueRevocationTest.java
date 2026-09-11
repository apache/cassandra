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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.AsyncExecutor;
import accord.api.ExclusiveAsyncExecutor;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Scheduler;
import accord.coordinate.Coordinations;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultLocalListeners.NotifySink;
import accord.impl.DefaultRemoteListeners;
import accord.impl.TestAgent;
import accord.impl.basic.InMemoryJournal;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.ExecutionContext;
import accord.local.ExecutionContext.ExecutionKind;
import accord.local.ExecutionContext.ExecutionSequence;
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
import accord.utils.Invariants;

import org.apache.cassandra.config.AccordConfig;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.concurrent.Condition;

import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.junit.Assert.fail;

/**
 * This test has been authored entirely by Claude.
 *
 * An {@code INCR} task with batches left re-enqueues itself on its command store's {@link ExclusiveExecutor} from
 * {@code completeExclusiveMayThrow}, i.e. from <em>within</em> {@link ExclusiveExecutor#completeTask}: at that point its
 * slot has been vacated but the next occupant has not been polled yet, so the task is both {@code ExclusiveExecutor.task}
 * and a member of the multi queue. If it is then revoked before completion returns, {@code unqueue} must treat it as the
 * ordinary waiter it now is.
 *
 * <p>Such a revocation is exactly what the consequences submitted after the parent completes can cause: a nested
 * unsequenced task granted a slot on a key the INCR task still holds (and holds a txnId lock across, so it may not run
 * alongside a grant) revokes the INCR task's permission to run, which must take it out of the run queue.
 *
 * <p>Before the fix that took the wrong branch - {@code removeCurrentTask}, which requires the store's {@code selfTask}
 * to be waiting in the runnable queue, though completion has already cleaned it out - so the revocation failed the task
 * with an {@code IllegalStateException} (and, with invariants compiled out, would have dropped the task it polled into
 * the vacated slot, double-enqueued {@code selfTask}, and left the INCR task queued in two places at once).
 */
public class AccordExecutorIncrRequeueRevocationTest
{
    private static final int KEYS = 4;
    private static final int THREADS = 4;
    private static final int TASKS = 400;
    private static final int TIMEOUT_SECONDS = 60;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        // one key per batch, so that an INCR task over several keys must come back for more: it is the requeue between
        // batches that this test is about. Read once, when AccordExecutor initialises, so it must be set before that -
        // the require below both checks that and forces the initialisation to happen here, where we can explain it
        AccordConfig config = DatabaseDescriptor.getAccord();
        config.queue_nonsync_min_batch_size = 1;
        config.queue_nonsync_max_batch_size = 1;
        Invariants.require(AccordExecutor.NONSYNC_MAX_BATCH_SIZE == 1,
                           "expected a batch size of one, found %d: AccordExecutor was initialised before this test set it",
                           AccordExecutor.NONSYNC_MAX_BATCH_SIZE);
    }

    @Test
    public void asyncSubmitTest() throws InterruptedException
    {
        run(id -> new AccordExecutorAsyncSubmit(id, RUN_WITHOUT_LOCK, THREADS, i -> "Loop" + i, new RecordingAgent()));
    }

    @Test
    public void signalLoopTest() throws InterruptedException
    {
        run(id -> new AccordExecutorSignalLoop(id, RUN_WITHOUT_LOCK, THREADS, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, new RecordingAgent()));
    }

    /** records what is reported to it: on these paths any report is a broken internal invariant, not a failed operation */
    static class RecordingAgent extends TestAgent
    {
        static final List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        @Override
        public void onException(Throwable t)
        {
            // print only the first: with hundreds of tasks a broken invariant is reported for every one of them
            if (exceptions.isEmpty())
                t.printStackTrace(System.out);
            exceptions.add(t);
        }

        @Override
        public void onException(Throwable t, String context)
        {
            onException(t);
        }
    }

    private void run(IntFunction<AccordExecutor> executorFactory) throws InterruptedException
    {
        RecordingAgent.exceptions.clear();
        RUN_COUNT.set(0);
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 1));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = new RoutingKey[KEYS];
        for (int i = 0 ; i < KEYS ; ++i)
            keys[i] = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(i)));

        AccordExecutor executor = executorFactory.apply(0);
        // synthetic loads: an uninitialised value, so we need no schema, cluster metadata or commit log
        executor.cacheUnsafe().types().forEach(type -> type.unsafeSetLoadFunction((ignoreStore, ignoreKey) -> null));
        AccordCommandStore store = commandStore(tableId, partitioner, executor);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicInteger outstanding = new AtomicInteger();
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        Condition done = Condition.newOneTimeCondition();
        List<Thread> submitters = new ArrayList<>();
        // a charge of our own, held until every submitter has finished, so that the count cannot reach zero while there
        // are submissions still to come
        outstanding.incrementAndGet();
        try
        {
            for (int t = 0 ; t < THREADS ; ++t)
            {
                Thread thread = new Thread(() -> {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();
                    for (int i = 0 ; i < TASKS / THREADS ; ++i)
                    {
                        // exactly two keys, one per batch: after the first batch the INCR task has one ready key left, so
                        // it is wait-ready and re-enqueues itself, and one arrival is enough to take that key away again
                        int first = rnd.nextInt(KEYS), second = (first + 1 + rnd.nextInt(KEYS - 1)) % KEYS;
                        RoutingKeys declaredKeys = RoutingKeys.of(keys[Math.min(first, second)], keys[Math.max(first, second)]);

                        // an INCR task with no txnId holds no lock between its runs, so it is not upgraded to a fifo
                        // claim and keeps an ordinary prioritised position on the keys it has yet to process. A later
                        // arrival that sorts ahead of it therefore displaces it as head of the entry - which is the
                        // revocation this test needs. Consequences inherit their parent's position, so the sort is
                        // decided by execution kind: we take the last, and give the nested task below the first.
                        ExecutionContext context = kind(AccordExecutionTestUtils.idempotent(ExecutionContext.contextFor(null, null, declaredKeys,
                                                                                   LoadKeys.INCR, LoadKeysFor.READ_WRITE, "incr")),
                                                       ExecutionSequence.BY_PRIORITY, ExecutionKind.OTHER);
                        AtomicInteger runs = new AtomicInteger();
                        submit(store, context, outstanding, failures, done, safeStore -> {
                            RUN_COUNT.incrementAndGet();
                            if (runs.incrementAndGet() > 1)
                                return;

                            // A consequence on our own command store, and not ATOMIC, so it is submitted once we have
                            // completed this batch - by which point we have re-enqueued ourselves for the next one. It
                            // declares every key we declared and sorts ahead of us, so it takes the head of the entry we
                            // were waiting for and revokes the permission to run we have just been given.
                            ExecutionContext nested = kind(ExecutionContext.contextFor(null, null, declaredKeys,
                                                                                       LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "nested"),
                                                           ExecutionSequence.UNSEQUENCED, ExecutionKind.PREACCEPT);
                            submit(store, nested, outstanding, failures, done, ignore -> {});
                        });
                    }
                }, "submit" + t);
                submitters.add(thread);
                thread.start();
            }
            for (Thread thread : submitters)
                thread.join();
            if (outstanding.decrementAndGet() == 0)
                done.signal();

            if (!done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                fail(outstanding.get() + " tasks did not complete within " + TIMEOUT_SECONDS + "s: " + failures);
            if (!failures.isEmpty())
                fail(failures.size() + " tasks failed, e.g. " + failures.get(0));
            if (!RecordingAgent.exceptions.isEmpty())
                fail(RecordingAgent.exceptions.size() + " exceptions were reported to the agent, e.g. " + RecordingAgent.exceptions.get(0));
            // every INCR task must have run twice, i.e. every one of them really did requeue itself between batches
            int expected = 2 * THREADS * (TASKS / THREADS);
            if (RUN_COUNT.get() != expected)
                fail("expected " + expected + " runs of " + (THREADS * (TASKS / THREADS)) + " incremental tasks, found " + RUN_COUNT.get());
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static void submit(AccordCommandStore store, ExecutionContext context, AtomicInteger outstanding,
                               List<Throwable> failures, Condition done, Consumer<? super SafeCommandStore> body)
    {
        outstanding.incrementAndGet();
        store.execute(context, body, (success, fail) -> {
            if (fail != null)
                failures.add(fail);
            if (outstanding.decrementAndGet() == 0)
                done.signal();
        });
    }

    /** the number of tasks that ran, so that a test which submits nothing cannot pass */
    private static final AtomicInteger RUN_COUNT = new AtomicInteger();

    /** the only way to control a task's sequencing and kind, as {@link ExecutionContext#contextFor} does not */
    private static ExecutionContext kind(ExecutionContext wrap, ExecutionSequence sequence, ExecutionKind kind)
    {
        return new ExecutionContext.Wrapped()
        {
            @Override public ExecutionSequence executionSequence() { return sequence; }
            @Override public ExecutionKind executionKind() { return kind; }
            @Override public ExecutionContext wrapped() { return wrap; }
        };
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
            @Override public Scheduler scheduler() { return null; }
            @Override public long currentStamp() { return stamp; }
            @Override public void updateStamp() { ++stamp; }
            @Override public boolean isReplaying() { return false; }
            @Override public void reportLocalExecution(TxnId txnId, Route<?> route, Ballot ballot, Timestamp applyAt, Writes writes, Result result) {}
        };

        return new AccordCommandStore(0, node, new RecordingAgent(), null,
                                      cs -> new ProgressLog.NoOpProgressLog(),
                                      cs -> new DefaultLocalListeners(null, new DefaultRemoteListeners.NoOpRemoteListeners(), new NotifySink.NoOpNotifySink()),
                                      new RangesForEpoch(1, Ranges.of(TokenRange.fullRange(tableId, partitioner))),
                                      new InMemoryJournal(nodeId, new DefaultRandom(1)),
                                      executor);
    }
}
