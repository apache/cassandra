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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test has been authored entirely by Claude.
 *
 * An {@code INCR} task that declares a txnId locks that txnId with {@link AccordCacheEntry.LockMode#HOLD_QUEUE} for the
 * whole of its execution, i.e. across all of its batches. That deadlocks against an unsequenced task that has been
 * granted one of the INCR task's keys and then has to wait for that txnId:
 *
 * <ol>
 *   <li>an INCR task {@code H} declares txnId {@code t} and key {@code k}, and becomes head of both entries;</li>
 *   <li>a SYNC task {@code T} declares {@code t} and {@code k} too. On {@code k},
 *       {@link AccordCacheEntry#addUnsequenced} <i>grants</i> {@code T} an unsequenced slot - it does so whenever
 *       {@code H} has not started yet, or whenever a grant is already outstanding. On {@code t}, {@code T} is queued
 *       behind {@code H}, so it cannot run;</li>
 *   <li>{@code H} can no longer run with {@code k}, because {@code AccordCacheEntry.isRunnable} requires
 *       {@code unsequenced == 0} for a head that holds locks between runs. So {@code H} never completes and never
 *       releases {@code t}, {@code T} never runs and never releases its grant on {@code k}, and both tasks - plus
 *       everything queued behind them - are stuck.</li>
 * </ol>
 *
 * Neither task alone deadlocks: with no txnId an INCR task does not hold locks between runs
 * ({@code holdsLocksBetweenRuns()} is false), and with no shared txnId the unsequenced task never blocks on {@code H}.
 * ASYNC tasks are also unaffected, as they too never hold locks between runs.
 */
public class AccordExecutorIncrDeadlockTest
{
    private static final int KEYS = 8;
    private static final int TXN_IDS = 8;
    private static final int THREADS = 4;
    private static final int TASKS = 200;
    private static final int MAX_SYNC_KEYS = 3, MAX_BATCHED_KEYS = 6, MAX_TXN_IDS = 2;
    private static final int TIMEOUT_SECONDS = 30;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void incrementalTaskHoldingTxnIdDoesNotDeadlockTest() throws InterruptedException
    {
        run(LoadKeys.INCR, true);
    }

    /** the same workload with no txnId on the INCR tasks, which is expected to pass */
    @Test
    public void incrementalTaskWithoutTxnIdTest() throws InterruptedException
    {
        run(LoadKeys.INCR, false);
    }

    /** the same workload with ASYNC in place of INCR, which is expected to pass */
    @Test
    public void asyncTaskHoldingTxnIdTest() throws InterruptedException
    {
        run(LoadKeys.ASYNC, true);
    }

    private void run(LoadKeys batched, boolean batchedDeclaresTxnIds) throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 1));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = new RoutingKey[KEYS];
        for (int i = 0 ; i < KEYS ; ++i)
            keys[i] = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(i)));
        TxnId[] txnIds = new TxnId[TXN_IDS];
        for (int i = 0 ; i < TXN_IDS ; ++i)
            txnIds[i] = TxnId.fromValues(1, 1 + i, 0, new Id(1));

        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 4, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, new TestAgent());
        // synthetic loads: an uninitialised value, so we need no schema, cluster metadata or commit log
        executor.cacheUnsafe().types().forEach(type -> type.unsafeSetLoadFunction((ignoreStore, ignoreKey) -> null));
        AccordCommandStore store = commandStore(tableId, partitioner, executor);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        // Seeded with the whole workload before any submitter starts. Incrementing per submission instead lets
        // outstanding transiently reach 0 whenever the executor drains everything submitted so far - very likely on the
        // first task, while the submitters are still building keys - and `done` is a one-time condition, so the await
        // below would return true immediately with the remaining ~199 tasks deadlocked exactly as the class javadoc
        // describes. That is a silent false pass of the only test for that deadlock.
        assertEquals("outstanding is seeded with TASKS, so the submitters must submit exactly that many",
                     0, TASKS % THREADS);
        AtomicInteger outstanding = new AtomicInteger(TASKS);
        // "completed" is not "ran": a task refused at setup completes too, so the counts below are what stops this
        // workload from passing without ever taking the claims it exists to interleave
        AtomicInteger ran = new AtomicInteger();
        List<Throwable> failures = new java.util.concurrent.CopyOnWriteArrayList<>();
        Condition done = Condition.newOneTimeCondition();
        List<Thread> submitters = new ArrayList<>();
        try
        {
            for (int t = 0 ; t < THREADS ; ++t)
            {
                Thread thread = new Thread(() -> {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();
                    for (int i = 0 ; i < TASKS / THREADS ; ++i)
                    {
                        boolean isBatched = rnd.nextBoolean();
                        LoadKeys loadKeys = isBatched ? batched : LoadKeys.SYNC;
                        int[] keyOrdinals = distinct(rnd, KEYS, 1 + rnd.nextInt(isBatched ? MAX_BATCHED_KEYS : MAX_SYNC_KEYS));
                        RoutingKey[] declared = new RoutingKey[keyOrdinals.length];
                        for (int k = 0 ; k < keyOrdinals.length ; ++k)
                            declared[k] = keys[keyOrdinals[k]];
                        int[] txnIdOrdinals = isBatched && !batchedDeclaresTxnIds ? new int[0] : distinct(rnd, TXN_IDS, rnd.nextInt(1 + MAX_TXN_IDS));
                        TxnId primary = txnIdOrdinals.length > 0 ? txnIds[txnIdOrdinals[0]] : null;
                        TxnId additional = txnIdOrdinals.length > 1 ? txnIds[txnIdOrdinals[1]] : null;
                        // idempotent(): an INCR task that does not declare it is refused outright at setup, and a refused
                        // task *completes* - so without this every INCR submission here failed before it took a single
                        // claim, and the only test for the INCR/txnId deadlock passed while exercising none of it
                        ExecutionContext context = AccordExecutionTestUtils.idempotent(
                            ExecutionContext.contextFor(primary, additional, RoutingKeys.of(declared), loadKeys, LoadKeysFor.READ_WRITE, "task"));
                        store.execute(context, (Consumer<? super SafeCommandStore>) safeStore -> ran.incrementAndGet(),
                                      (success, fail) -> {
                                          if (fail != null)
                                              failures.add(fail);
                                          if (outstanding.decrementAndGet() == 0)
                                              done.signal();
                                      });
                    }
                }, "submit" + t);
                submitters.add(thread);
                thread.start();
            }
            for (Thread thread : submitters)
                thread.join();

            if (!done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                fail(outstanding.get() + " of " + TASKS + " tasks did not complete within " + TIMEOUT_SECONDS + "s");
            assertEquals("every task must have completed", 0, outstanding.get());
            assertTrue("no task may fail: a failure here means the workload was refused or errored rather than run, and "
                       + "a refused task completes, so the count above would still pass. First: "
                       + (failures.isEmpty() ? "none" : failures.get(0) + " (" + failures.size() + " of " + TASKS + ')'),
                       failures.isEmpty());
            assertTrue("every task must have run its body at least once, or nothing took a claim and the interleaving "
                       + "this test exists for never happened: ran " + ran.get() + " of " + TASKS, ran.get() >= TASKS);
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /** {@code count} distinct ordinals in [0..limit), in ascending order */
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

        return new AccordCommandStore(0, node, new TestAgent(), null,
                                      cs -> new ProgressLog.NoOpProgressLog(),
                                      cs -> new DefaultLocalListeners(null, new DefaultRemoteListeners.NoOpRemoteListeners(), new NotifySink.NoOpNotifySink()),
                                      new RangesForEpoch(1, Ranges.of(TokenRange.fullRange(tableId, partitioner))),
                                      new InMemoryJournal(nodeId, new DefaultRandom(1)),
                                      executor);
    }
}
