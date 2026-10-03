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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import accord.primitives.Unseekables;
import org.apache.cassandra.config.DurationSpec;
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
import accord.local.FindKeys;
import accord.local.LoadKeys;
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
 * This test was authored by Claude (Anthropic).
 *
 * A non-sync INCR task queued BY_PRIORITY over two keys (the shape of "Update Unmanaged CommandsForKey" for a sync
 * point) must lead both keys at once to start. ATOMIC consequences (the shape of "Update CommandsForKey") take fifo
 * positions at setup, and a fifo region runs ahead of the priority region, so a continuous stream of them on one of
 * those keys keeps the victim from ever leading it. {@link AccordExecutor#promoteAgedWaitersExclusive} bounds this by
 * giving a task that has waited long enough a fifo position of its own.
 */
public class AccordExecutorFifoStarvationTest
{
    private static final long UPGRADE_AGE_MS = 300;
    private static final long STORM_MS = 4000;
    private static final int STORM_CHAINS = 4, RESTART = 4;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        // must precede AccordExecutor's static initialiser
        DatabaseDescriptor.getAccord().queue_cache_fifo_upgrade_age = new DurationSpec.IntMillisecondsBound(UPGRADE_AGE_MS);
    }

    @Test
    public void withoutPromotionTheVictimStarves() throws Throwable
    {
        long waited = run(false);
        System.out.println("without promotion: victim ran after " + (waited < 0 ? "never (storm " + STORM_MS + "ms)" : waited + "ms"));
        assertTrue("expected the victim to starve for the whole storm, but it ran after " + waited + "ms", waited < 0);
    }

    @Test
    public void promotionBoundsTheWait() throws Throwable
    {
        long waited = run(true);
        System.out.println("with promotion: victim ran after " + waited + "ms");
        assertTrue("victim never ran during a " + STORM_MS + "ms storm despite promotion", waited >= 0);
        assertTrue("victim waited " + waited + "ms, expected about the upgrade age (" + UPGRADE_AGE_MS + "ms)", waited < 10 * UPGRADE_AGE_MS);
    }

    /**
     * NOTE: this needs both the cache-entry fifo promotion and a fix for executor-queue (TaskQueueMulti) starvation: the
     * storm's consequences inherit an old position, so a runnable SYNC task in the same group is never dispatched. With
     * promotion alone ~2/3 of tasks complete (all INCR victims; the remainder queue behind a WAITING_TO_RUN SYNC task).
     *
     * Many INCR victims (with and without txnIds, over random subsets of keys including the hot one) and SYNC tasks
     * on the same keys, under the storm, with promotion: every task must complete, and promotion must actually occur.
     * Run with paranoia enabled, SafeTask's own checks validate every entry's status as each task becomes runnable.
     */
    @Test
    public void mixedWorkloadWithoutPromotion() throws Throwable
    {
        long[] result = runMixed(false);
        System.out.println("mixed without promotion: " + result[0] + " of " + result[2] + " tasks completed");
    }

    @Test
    public void mixedWorkloadAllCompleteWithPromotion() throws Throwable
    {
        long[] result = runMixed(true);
        assertEquals(result[2] - result[0] + " of " + result[2] + " tasks did not complete under the storm", result[2], result[0]);
        System.out.println("mixed with promotion: " + result[0] + " of " + result[2] + " tasks completed, " + result[1] + " promoted while waiting");
        assertTrue("promotion was never exercised", result[1] > 0);
    }

    private long[] runMixed(boolean promote) throws Throwable
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 1));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = new RoutingKey[6];
        for (int i = 0 ; i < keys.length ; ++i)
            keys[i] = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(i)));
        RoutingKey hot = keys[0];

        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, new TestAgent());
        executor.cacheUnsafe().types().forEach(type -> type.unsafeSetLoadFunction((ignoreStore, ignoreKey) -> null));
        AccordCommandStore store = commandStore(tableId, partitioner, executor);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong nextHlc = new AtomicLong(1_000_000);
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        List<Thread> threads = new ArrayList<>();
        AtomicLong promoted = new AtomicLong();
        try
        {
            for (int t = 0 ; t < STORM_CHAINS ; ++t)
            {
                int phase = t;
                Thread thread = new Thread(() -> {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(phase * 2L));
                    startChain(store, hot, nextHlc, stop, failures, phase);
                }, "storm" + t);
                threads.add(thread);
                thread.start();
            }
            Thread housekeeping = new Thread(() -> {
                while (!stop.get())
                {
                    executor.executeDirectlyWithLock(() -> {
                        List<SafeTask<?>> before = new ArrayList<>();
                        for (int i = 0 ; i < executor.waiting.size() ; ++i)
                        {
                            SafeTask<?> task = executor.waiting.getSingle(i);
                            if (task.isIncremental() && !task.isCacheQueuedFifo()) before.add(task);
                        }
                        if (promote) executor.promoteAgedWaitersExclusive();
                        for (SafeTask<?> task : before)
                            if (task.isCacheQueuedFifo()) promoted.incrementAndGet();
                    });
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
                }
            }, "housekeeping");
            threads.add(housekeeping);
            housekeeping.start();

            int tasks = 120;
            AtomicInteger outstanding = new AtomicInteger(tasks);
            java.util.concurrent.ThreadLocalRandom rnd = java.util.concurrent.ThreadLocalRandom.current();
            for (int i = 0 ; i < tasks ; ++i)
            {
                int kind = rnd.nextInt(3); // 0: INCR with txnId, 1: INCR without, 2: SYNC
                int mask = 1 | rnd.nextInt(1 << keys.length); // always includes the hot key
                List<RoutingKey> declared = new ArrayList<>();
                for (int k = 0 ; k < keys.length ; ++k)
                    if ((mask & (1 << k)) != 0) declared.add(keys[k]);
                TxnId txnId = kind == 1 ? null : TxnId.fromValues(1, 1 + i, 0, new Id(2));
                ExecutionContext context = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(declared.toArray(new RoutingKey[0])),
                                                                       kind == 2 ? LoadKeys.SYNC : LoadKeys.INCR, FindKeys.DECLARED, "mixed" + kind);
                if (kind != 2) context = AccordExecutionTestUtils.idempotent(context);
                store.execute(context, (Consumer<? super SafeCommandStore>) safeStore -> {},
                              (success, fail) -> { if (fail != null) failures.add(fail); outstanding.decrementAndGet(); });
                if (rnd.nextInt(8) == 0) Thread.sleep(5);
            }

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (outstanding.get() > 0 && System.nanoTime() < deadline)
                Thread.sleep(10);

            assertTrue("failures: " + failures, failures.isEmpty());
            if (outstanding.get() > 0)
            {
                String[] report = new String[1];
                executor.executeDirectlyWithLock(() -> {}); // let any in-flight housekeeping finish
                report[0] = CacheWedgeReport.describe(store, TimeUnit.SECONDS.toNanos(5), 4);
                System.out.println("### stuck (promote=" + promote + "): " + outstanding.get() + " of " + tasks + report[0]);
            }
            return new long[] { tasks - outstanding.get(), promoted.get(), tasks };
        }
        finally
        {
            stop.set(true);
            for (Thread thread : threads)
                thread.join(10_000);
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /** @return ms from submission until the victim ran, or -1 if it had not run when the storm ended */
    private long run(boolean promote) throws Throwable
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 1));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey hot = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(0)));
        RoutingKey cold = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(1)));

        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, new TestAgent());
        executor.cacheUnsafe().types().forEach(type -> type.unsafeSetLoadFunction((ignoreStore, ignoreKey) -> null));
        AccordCommandStore store = commandStore(tableId, partitioner, executor);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong nextHlc = new AtomicLong(1000);
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        List<Thread> threads = new ArrayList<>();
        try
        {
            // the storm: chains of ATOMIC INCR consequences on the hot key. Each link takes a fifo position at setup and,
            // while running, spawns the next link (which inherits its fifoAt), so a chain always has a successor queued
            // in the hot key's fifo region before the current link completes. Every RESTART links a chain is re-rooted
            // from a fresh parent (so a fresh fifoAt), as real consequence chains end and new ones begin; several chains
            // with staggered phases keep the region continuously occupied.
            for (int t = 0 ; t < STORM_CHAINS ; ++t)
            {
                int phase = t;
                Thread thread = new Thread(() -> {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(phase * 2L));
                    startChain(store, hot, nextHlc, stop, failures, phase);
                }, "storm" + t);
                threads.add(thread);
                thread.start();
            }

            if (promote)
            {
                // what AccordCommandStores' 1s housekeeping does in production, at a test-friendly cadence
                Thread housekeeping = new Thread(() -> {
                    while (!stop.get())
                    {
                        executor.executeDirectlyWithLock(executor::promoteAgedWaitersExclusive);
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                    }
                }, "housekeeping");
                threads.add(housekeeping);
                housekeeping.start();
            }

            Thread.sleep(200); // let the storm build its fifo region
            long submittedAt = System.nanoTime();
            AtomicLong ranAt = new AtomicLong(-1);
            TxnId victimTxnId = TxnId.fromValues(1, 1, 0, new Id(1));
            ExecutionContext victim = AccordExecutionTestUtils.idempotent(
                ExecutionContext.contextFor(victimTxnId, null, RoutingKeys.of(hot, cold), LoadKeys.INCR, FindKeys.DECLARED, "victim"));
            store.execute(victim, (Consumer<? super SafeCommandStore>) safeStore -> ranAt.compareAndSet(-1, System.nanoTime()),
                          (success, fail) -> { if (fail != null) failures.add(fail); });

            long deadline = submittedAt + TimeUnit.MILLISECONDS.toNanos(STORM_MS);
            while (ranAt.get() < 0 && System.nanoTime() < deadline)
                Thread.sleep(10);

            assertTrue("failures: " + failures, failures.isEmpty());
            return ranAt.get() < 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(ranAt.get() - submittedAt);
        }
        finally
        {
            stop.set(true);
            for (Thread thread : threads)
                thread.join(10_000);
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static void startChain(AccordCommandStore store, RoutingKey hot, AtomicLong nextHlc, AtomicBoolean stop, List<Throwable> failures, int links)
    {
        if (stop.get()) return;
        TxnId txnId = TxnId.fromValues(1, nextHlc.incrementAndGet(), 0, new Id(1));
        ExecutionContext root = ExecutionContext.contextFor(txnId, null, RoutingKeys.EMPTY, LoadKeys.SYNC, FindKeys.DECLARED, "storm root");
        store.execute(root, (Consumer<? super SafeCommandStore>) safeStore -> link(store, txnId, hot, nextHlc, stop, failures, links),
                      (success, fail) -> { if (fail != null) failures.add(fail); });
    }

    private static void link(AccordCommandStore store, TxnId txnId, RoutingKey hot, AtomicLong nextHlc, AtomicBoolean stop, List<Throwable> failures, int links)
    {
        if (stop.get()) return;
        store.execute(atomic(txnId, hot), (Consumer<? super SafeCommandStore>) safeStore -> {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            // NB: re-rooting from within a task makes the new root a consequence, which inherits this link's position;
            // so every storm task keeps the very first root's (ever older) position. That also starves same-group work
            // in the executor queue (TaskQueueMulti always prefers the older head), which mixedWorkload* exposes.
            if ((links + 1) % RESTART == 0) startChain(store, hot, nextHlc, stop, failures, links + 1);
            else link(store, txnId, hot, nextHlc, stop, failures, links + 1);
        }, (success, fail) -> { if (fail != null && !(fail instanceof java.util.concurrent.CancellationException)) failures.add(fail); });
    }

    private static ExecutionContext atomic(TxnId txnId, RoutingKey key)
    {
        return new ExecutionContext()
        {
            @Override public TxnId primaryTxnId() { return txnId; }
            @Override public Unseekables<?> keys() { return RoutingKeys.of(key); }
            @Override public LoadKeys loadKeys() { return LoadKeys.INCR; }
            @Override public ExecutionSequence executionSequence() { return ExecutionSequence.ATOMIC; }
            @Override public boolean isIdempotent() { return true; }
            @Override public String reason() { return "storm child"; }
            @Override public String toString() { return describe(); }
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

        return new AccordCommandStore(0, node, new TestAgent(), null,
                                      cs -> new ProgressLog.NoOpProgressLog(),
                                      cs -> new DefaultLocalListeners(null, new DefaultRemoteListeners.NoOpRemoteListeners(), new NotifySink.NoOpNotifySink()),
                                      new RangesForEpoch(1, Ranges.of(TokenRange.fullRange(tableId, partitioner))),
                                      new InMemoryJournal(nodeId, new DefaultRandom(1)),
                                      executor);
    }
}
