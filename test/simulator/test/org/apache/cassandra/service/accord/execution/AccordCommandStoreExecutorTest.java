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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import org.junit.Test;

import accord.api.AsyncExecutor;
import accord.api.ExclusiveAsyncExecutor;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Scheduler;
import accord.coordinate.Coordinations;
import accord.impl.DefaultLocalListeners;
import accord.impl.TestAgent;
import accord.impl.DefaultLocalListeners.NotifySink;
import accord.impl.DefaultRemoteListeners;
import accord.impl.basic.InMemoryJournal;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.TimeService;
import accord.local.durability.DurabilityService;
import accord.local.ExecutionContext;
import accord.local.LoadKeys;
import accord.local.LoadKeysFor;
import accord.local.Node.Id;
import accord.local.NodeCommandStoreService;
import accord.local.SafeCommandStore;
import accord.local.SafeState;
import accord.primitives.Ballot;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.TopologyManager;
import accord.utils.DefaultRandom;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableSupplier;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.simulator.test.SimulationTestBase;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;

/**
 * Simulator driven test of {@link SafeTask} execution on a real {@link AccordCommandStore}, i.e. of the parts of the
 * executor {@link AccordExecutorTest} cannot reach: cache references, the per-key/per-txnId queues, and the ordering
 * guarantees they provide.
 *
 * <p>The command store is real, but everything below it is synthetic: an in-memory journal, and cache load functions
 * that return {@code null} (an uninitialised value) rather than reading from {@code system_accord}. So no schema, no
 * cluster metadata, no commit log - only the executor and task machinery is under test.
 *
 * <h2>Scope (phase 1)</h2>
 * A single executor and command store; {@code SYNC} key-domain contexts; no nesting, no cancellation, no failures.
 * Verifies:
 * <ul>
 *   <li><b>liveness</b>: every submission is notified exactly once, and successfully;</li>
 *   <li><b>declared access</b>: while running, a task holds a reference for every key and txnId it declared;</li>
 *   <li><b>mutual exclusion</b>: two tasks that declare the same key or txnId never execute concurrently - the
 *       central guarantee of the cache-entry queues;</li>
 *   <li><b>no leaks</b>: once everything has been notified, no cache entry is still referenced and no task retains
 *       its references.</li>
 * </ul>
 *
 * <h2>Notes</h2>
 * <ul>
 *   <li>only the {@code SIGNAL} and {@code ASYNC} submission models can be simulated; {@code SYNC}/{@code SEMI_SYNC}
 *       (the production default) need the simulator to intercept {@link java.util.concurrent.locks.ReentrantLock},
 *       see the TODO in {@link AccordExecutorTest};</li>
 *   <li>only one test method may be run per JVM (pre-existing limitation of {@link AccordExecutorTest}); ant forks
 *       per test method.</li>
 * </ul>
 */
public class AccordCommandStoreExecutorTest extends SimulationTestBase
{
    private static final int KEYS = 8;
    private static final int TXN_IDS = 8;
    private static final int SUBMIT_THREADS = 8;
    private static final int OUTER_LOOP = 5;
    private static final int INNER_LOOP = 10;
    private static final int MAX_KEYS_PER_TASK = 3;
    private static final int MAX_TXN_IDS_PER_TASK = 2;
    private static final int MAX_TASKS = SUBMIT_THREADS * OUTER_LOOP * INNER_LOOP;

    @Test
    public void signalLoopTest()
    {
        test(() -> new AccordExecutorSignalLoop(1, RUN_WITHOUT_LOCK, 4, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, new RecordingAgent()));
    }

    @Test
    public void asyncSubmitTest()
    {
        test(() -> new AccordExecutorAsyncSubmit(1, RUN_WITHOUT_LOCK, 4, i -> "Loop" + i, new RecordingAgent()));
    }

    private void test(SerializableSupplier<AccordExecutor> executorSupplier)
    {
        simulate(arr(() -> {
                     try
                     {
                         DatabaseDescriptor.daemonInitialization();
                         AccordExecutor executor = executorSupplier.get();
                         Env env = new Env(executor);

                         for (float loadDelayChance : new float[]{ 0f, 0.1f })
                         {
                             for (float sleepChance : new float[]{ 0f, 0.1f })
                             {
                                 System.out.printf("loadDelayChance %.2f, sleepChance %.2f%n", loadDelayChance, sleepChance);
                                 env.loadDelayChance = loadDelayChance;
                                 env.round(sleepChance);
                             }
                         }
                     }
                     catch (Throwable t)
                     {
                         throw new RuntimeException(t);
                     }
                 }),
                 () -> {}, 1L);
    }

    /**
     * Records everything reported to the agent: on these paths any report indicates a broken internal invariant,
     * not a failed operation. We do not use {@link AccordAgent}, as reporting an exception there touches
     * {@code AccordSystemMetrics}, which requires a started {@code AccordService}.
     */
    public static class RecordingAgent extends TestAgent
    {
        static final List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        @Override
        public void onException(Throwable t)
        {
            exceptions.add(t);
            System.out.println("### agent.onException: " + t);
            t.printStackTrace(System.out);
        }

        @Override
        public void onException(Throwable t, String context)
        {
            onException(t);
        }
    }

    static class Env
    {
        final AccordExecutor executor;
        final AccordCommandStore store;
        final RoutingKey[] keys = new RoutingKey[KEYS];
        final TxnId[] txnIds = new TxnId[TXN_IDS];

        /** key/txnId ordinal -> id of the task currently executing with it, or -1 */
        final AtomicIntegerArray keyOwner = new AtomicIntegerArray(KEYS);
        final AtomicIntegerArray txnIdOwner = new AtomicIntegerArray(TXN_IDS);

        /** reset for every round: task ids are round-local */
        AtomicInteger nextTaskId = new AtomicInteger();
        /** task id -> number of times its callback has been invoked; must be exactly one for every submission */
        AtomicIntegerArray notifications = new AtomicIntegerArray(MAX_TASKS);
        final List<Throwable> failures = new CopyOnWriteArrayList<>();
        SafeTask<?>[] tasks = new SafeTask<?>[MAX_TASKS];

        volatile float loadDelayChance;

        Env(AccordExecutor executor)
        {
            this.executor = executor;
            TableId tableId = TableId.fromUUID(new java.util.UUID(0, 1));
            IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
            this.store = newCommandStore(tableId, partitioner, executor);
            for (int i = 0 ; i < KEYS ; ++i)
                keys[i] = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(i)));
            for (int i = 0 ; i < TXN_IDS ; ++i)
                txnIds[i] = TxnId.fromValues(1, 1 + i, 0, new Id(1));
            for (int i = 0 ; i < KEYS ; ++i)
                keyOwner.set(i, -1);
            for (int i = 0 ; i < TXN_IDS ; ++i)
                txnIdOwner.set(i, -1);

            // synthetic loads: an uninitialised value, optionally after some simulated latency
            executor.cacheUnsafe().types().forEach(type -> type.unsafeSetLoadFunction((ignoreStore, ignoreKey) -> {
                maybePark(loadDelayChance);
                return null;
            }));
        }

        void round(float sleepChance) throws ExecutionException, InterruptedException
        {
            nextTaskId = new AtomicInteger();
            notifications = new AtomicIntegerArray(MAX_TASKS);
            tasks = new SafeTask<?>[MAX_TASKS];
            ExecutorPlus submit = executorFactory().pooled("submit", SUBMIT_THREADS);
            try
            {
                List<Future<?>> submitting = new ArrayList<>();
                for (int i = 0 ; i < SUBMIT_THREADS ; ++i)
                {
                    int id = i;
                    submitting.add(submit.submit(() -> {
                        for (int outer = 0 ; outer < OUTER_LOOP ; ++outer)
                        {
                            CountDownLatch inner = CountDownLatch.newCountDownLatch(INNER_LOOP);
                            for (int j = 0 ; j < INNER_LOOP ; ++j)
                                submitOne(sleepChance, inner);
                            inner.awaitUninterruptibly();
                            System.out.println("Loop " + id + '.' + outer);
                        }
                    }));
                }
                for (Future<?> f : submitting)
                    f.get();
            }
            finally
            {
                submit.shutdown();
            }

            verifyRoundComplete();
        }

        private void submitOne(float sleepChance, CountDownLatch done)
        {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            int taskId = nextTaskId.getAndIncrement();
            int[] keyOrdinals = distinct(rnd, KEYS, 1 + rnd.nextInt(MAX_KEYS_PER_TASK));
            int[] txnIdOrdinals = distinct(rnd, TXN_IDS, rnd.nextInt(1 + MAX_TXN_IDS_PER_TASK));

            RoutingKeys declaredKeys = keys(keyOrdinals);
            TxnId primary = txnIdOrdinals.length > 0 ? txnIds[txnIdOrdinals[0]] : null;
            TxnId additional = txnIdOrdinals.length > 1 ? txnIds[txnIdOrdinals[1]] : null;
            ExecutionContext context = ExecutionContext.contextFor(primary, additional, declaredKeys, LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "task" + taskId);

            Cancellable submitted =
                store.execute(context, (java.util.function.Consumer<? super SafeCommandStore>) safeStore -> body(taskId, keyOrdinals, txnIdOrdinals, safeStore, sleepChance),
                              (success, fail) -> {
                                  notifications.incrementAndGet(taskId);
                                  if (fail != null)
                                      failures.add(fail);
                                  done.decrement();
                              });
            tasks[taskId] = (SafeTask<?>) submitted;
        }

        private void body(int taskId, int[] keyOrdinals, int[] txnIdOrdinals, SafeCommandStore safeStore, float sleepChance)
        {
            SafeTask<?> task = ((SaferCommandStore) safeStore).task;

            // declared access: we must hold a reference for everything we declared, and be able to look it up
            for (int k : keyOrdinals)
            {
                SafeState<?> ref = task.refs.get(keys[k]);
                Invariants.require(ref != null, "task %d declared key %d but holds no reference for it", taskId, k);
                Invariants.require(SaferState.global(ref).references() > 0, "task %d holds an unreferenced entry for key %d", taskId, k);
                safeStore.ifLoadedAndInitialised(keys[k]);
            }
            for (int t : txnIdOrdinals)
            {
                SafeState<?> ref = task.refs.get(txnIds[t]);
                Invariants.require(ref != null, "task %d declared txnId %d but holds no reference for it", taskId, t);
                Invariants.require(SaferState.global(ref).references() > 0, "task %d holds an unreferenced entry for txnId %d", taskId, t);
                safeStore.ifInitialised(txnIds[t]);
            }

            // mutual exclusion: nobody else may be executing with any key or txnId we declared
            int keysTaken = 0, txnIdsTaken = 0;
            try
            {
                while (keysTaken < keyOrdinals.length)
                {
                    int k = keyOrdinals[keysTaken];
                    Invariants.require(keyOwner.compareAndSet(k, -1, taskId),
                                       "task %d ran concurrently with task %d, which also declared key %d", taskId, keyOwner.get(k), k);
                    ++keysTaken;
                }
                while (txnIdsTaken < txnIdOrdinals.length)
                {
                    int t = txnIdOrdinals[txnIdsTaken];
                    Invariants.require(txnIdOwner.compareAndSet(t, -1, taskId),
                                       "task %d ran concurrently with task %d, which also declared txnId %d", taskId, txnIdOwner.get(t), t);
                    ++txnIdsTaken;
                }

                maybePark(sleepChance);
            }
            finally
            {
                while (keysTaken > 0)
                    keyOwner.set(keyOrdinals[--keysTaken], -1);
                while (txnIdsTaken > 0)
                    txnIdOwner.set(txnIdOrdinals[--txnIdsTaken], -1);
            }
        }

        private void verifyRoundComplete()
        {
            Invariants.require(failures.isEmpty(), "%s", failures);
            Invariants.require(RecordingAgent.exceptions.isEmpty(), "%s", RecordingAgent.exceptions);

            int lastTaskId = nextTaskId.get();
            Invariants.require(lastTaskId == MAX_TASKS, "submitted %d tasks, expected %d", lastTaskId, MAX_TASKS);
            for (int taskId = 0 ; taskId < lastTaskId ; ++taskId)
            {
                Invariants.require(notifications.get(taskId) == 1, "task %d was notified %d times", taskId, notifications.get(taskId));
                Invariants.require(tasks[taskId].refs == null, "task %d did not release its references", taskId);
            }
            for (int i = 0 ; i < KEYS ; ++i)
                Invariants.require(keyOwner.get(i) == -1, "key %d is still owned by task %d", i, keyOwner.get(i));
            for (int i = 0 ; i < TXN_IDS ; ++i)
                Invariants.require(txnIdOwner.get(i) == -1, "txnId %d is still owned by task %d", i, txnIdOwner.get(i));

            try (AccordCommandStore.ExclusiveCaches caches = store.lockCaches())
            {
                for (AccordCacheEntry<?, ?, ?> entry : caches.commands())
                    Invariants.require(entry.references() == 0, "%s is still referenced", entry);
                for (AccordCacheEntry<?, ?, ?> entry : caches.commandsForKeys())
                    Invariants.require(entry.references() == 0, "%s is still referenced", entry);
            }
        }

        private RoutingKeys keys(int[] ordinals)
        {
            RoutingKey[] result = new RoutingKey[ordinals.length];
            for (int i = 0 ; i < ordinals.length ; ++i)
                result[i] = keys[ordinals[i]];
            return RoutingKeys.of(result);
        }
    }

    private static void maybePark(float chance)
    {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        if (chance > 0 && rnd.nextFloat() < chance)
            LockSupport.parkNanos(rnd.nextInt(10000, 100000));
    }

    /** {@code count} distinct ordinals in [0..limit), in ascending order (so a task never declares a key twice) */
    private static int[] distinct(ThreadLocalRandom rnd, int limit, int count)
    {
        int mask = 0;
        for (int i = 0 ; i < count ; ++i)
            mask |= 1 << rnd.nextInt(limit);
        int[] result = new int[Integer.bitCount(mask)];
        for (int i = 0, ordinal = 0 ; mask != 0 ; ++ordinal, mask >>>= 1)
        {
            if ((mask & 1) != 0)
                result[i++] = ordinal;
        }
        return result;
    }

    private static AccordCommandStore newCommandStore(TableId tableId, IPartitioner partitioner, AccordExecutor executor)
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

        Range range = TokenRange.fullRange(tableId, partitioner);
        RangesForEpoch rangesForEpoch = new RangesForEpoch(1, Ranges.of(range));
        AccordCommandStore store = new AccordCommandStore(0, node, new RecordingAgent(), null,
                                                          cs -> new ProgressLog.NoOpProgressLog(),
                                                          cs -> new DefaultLocalListeners(null, new DefaultRemoteListeners.NoOpRemoteListeners(), new NotifySink.NoOpNotifySink()),
                                                          rangesForEpoch,
                                                          new InMemoryJournal(nodeId, new DefaultRandom(1)),
                                                          executor);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });
        return store;
    }
}
