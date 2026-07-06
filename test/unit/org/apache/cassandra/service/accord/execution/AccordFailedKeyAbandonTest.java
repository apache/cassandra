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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.RoutingKey;
import accord.local.ExecutionContext;
import accord.local.ExecutionContext.ExecutionSequence;
import accord.local.LoadKeys;
import accord.local.LoadKeysFor;
import accord.local.Node.Id;
import accord.local.SafeCommandStore;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordFailedKeyTestHarness;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.concurrent.Condition;

import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test has been authored entirely by Claude.
 *
 * Abandoning a fan-out that has already run a round, which is what {@code ExclusiveExecutorTask.prepareTask}'s catch does
 * when a started task's prepare throws - an unrecoverable program bug, but one whose damage {@code SafeTask
 * .poisonUnreachedExclusive} must still limit: the keys the update never reached are marked and keep their claim, so
 * nothing may read a state the update did not reach.
 *
 * <p>It needs its own class for the batch sizes. Everywhere else {@code min == max}, and then a fan-out over loaded keys
 * never parks with a loaded key in hand: parking needs {@code readyCount < min(remaining, MIN_BATCH)}, and while every
 * remaining key is ready {@code readyCount == remaining}. With {@code MIN_BATCH > MAX_BATCH} a round takes one key and
 * leaves the rest ready but short of the threshold, so the fan-out parks holding exactly the shape under test: a
 * <em>loaded</em>, led, unreached key. {@code AccordExecutor} reads the sizes once into static finals, so a JVM can only
 * have one setting (ant gives each class its own).
 */
public class AccordFailedKeyAbandonTest
{
    private static final int TIMEOUT_SECONDS = 30;
    /** a round takes one key, but two must be ready to start one, so a round always leaves the fan-out parked */
    private static final int MIN_BATCH = 2;
    private static final int MAX_BATCH = 1;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.getAccord().queue_nonsync_min_batch_size = MIN_BATCH;
        DatabaseDescriptor.getAccord().queue_nonsync_max_batch_size = MAX_BATCH;
    }

    @Before
    public void requireBatchSizesTookEffect()
    {
        // the config must win the race with AccordExecutor's first class-load, or the fan-out never parks with a loaded
        // key in hand and this class quietly tests nothing that AccordFailedKeyTest does not already cover
        Invariants.require(AccordExecutor.NONSYNC_MIN_BATCH_SIZE == MIN_BATCH && AccordExecutor.NONSYNC_MAX_BATCH_SIZE == MAX_BATCH,
                           "expected batch sizes %d/%d, found %d/%d: the config was read before setUp set it",
                           MIN_BATCH, MAX_BATCH, AccordExecutor.NONSYNC_MIN_BATCH_SIZE, AccordExecutor.NONSYNC_MAX_BATCH_SIZE);
    }

    /**
     * A started, ATOMIC fan-out abandoned by a failed prepare, holding one key it has applied, one it has not (loaded and
     * led) and one whose load is still outstanding. Each must be treated differently, and the middle one is the case no
     * other test can reach:
     * <ul>
     *   <li>the key it applied is released and unmarked - a batch whose body ran to completion must not be poisoned, or a
     *       successfully applied key stalls the store for ever;</li>
     *   <li>the loaded key it never reached is <em>marked and retained</em> (reference and claim), so no later operation
     *       may read it as though the update had arrived;</li>
     *   <li>the key whose load had not landed is marked and retained too - it is a key the update did not reach like any
     *       other - and the load, when it lands, must find the entry marked, keep it referenced and report nothing: the
     *       task that was waiting for it has departed, and {@code AccordExecutor.onLoadedExclusive} must skip it rather
     *       than notify a completed task.</li>
     * </ul>
     * Plus the two properties every abandonment shares: the caller is told the failure, and no internal error is reported
     * - a failure left recorded as RUN_INCOMPLETE is refused by {@code completeState}, which masks the real one.
     */
    @Test
    public void abandonedFanOutMarksEveryKeyItDidNotReach() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 30));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] loaded = { key(tableId, partitioner, 0), key(tableId, partitioner, 1) };
        RoutingKey slow = key(tableId, partitioner, 2);
        TxnId txnId = TxnId.fromValues(1, 1, 0, new Id(1));
        RuntimeException injected = new RuntimeException("injected prepare failure");

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        // one key whose load never lands, so the fan-out cannot reach the MIN_BATCH threshold again after its first round
        Condition releaseLoad = Condition.newOneTimeCondition();
        executor.cacheUnsafe().types().forEach(type -> setLoadFunction(type, slow, releaseLoad));
        AccordCommandStore store = AccordFailedKeyTestHarness.commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        Set<RoutingKey> processed = ConcurrentHashMap.newKeySet();
        Set<RoutingKey> marked = ConcurrentHashMap.newKeySet();
        Set<RoutingKey> claimed = ConcurrentHashMap.newKeySet();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> abandoned = new AtomicReference<>(false);
        AtomicReference<Throwable> sameTxn = new AtomicReference<>();
        Condition done = Condition.newOneTimeCondition();
        Condition sameTxnDone = Condition.newOneTimeCondition();
        AtomicReference<Boolean> slowSettled = new AtomicReference<>(false);
        boolean settled = false, sameTxnRan = false, landed = false;
        try
        {
            Object submitted =
            store.execute(fanOut(txnId, loaded[0], loaded[1], slow),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              Unseekables<?> batch = safeStore.context().keys();
                              for (int i = 0 ; i < batch.size() ; ++i)
                                  processed.add((RoutingKey) batch.get(i));
                          },
                          (success, fail) -> { failure.set(fail); done.signal(); });
            SafeTask<?> task = (SafeTask<?>) submitted;

            // as prepareTask's catch does for a task whose prepare threw: fail and complete it where it is parked
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (!abandoned.get() && failure.get() == null && System.nanoTime() < deadline)
            {
                executor.executeDirectlyWithLock(() -> {
                    if (abandoned.get() || !task.hasIncrementalStarted() || !task.isState(Task.State.WAITING))
                        return;
                    abandoned.set(true);
                    task.failAndCompleteExclusive(injected, Task.State.FAILED);
                });
            }

            if (abandoned.get() && done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
            {
                // poll: the report reaches the callback before the task releases anything
                settled = await(() -> {
                    executor.executeDirectlyWithLock(() -> {
                        marked.clear();
                        claimed.clear();
                        for (RoutingKey key : new RoutingKey[]{ loaded[0], loaded[1], slow })
                        {
                            AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(key);
                            if (entry == null)
                                continue;
                            if (entry.isInconsistent())
                                marked.add(key);
                            if (!entry.hasNoTasks() || entry.references() > 0)
                                claimed.add(key);
                        }
                    });
                    return marked.equals(unreached(loaded, slow, processed)) && claimed.equals(marked);
                });

                // the txnId is marked too, so a later task on that command must be told promptly rather than queue
                // behind a HOLD_QUEUE lock nothing will release
                store.execute(ExecutionContext.contextFor(txnId, null, RoutingKeys.of(key(tableId, partitioner, 100)),
                                                          LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "same txn"),
                              (Consumer<? super SafeCommandStore>) ignore -> {},
                              (success, fail) -> { sameTxn.set(fail); sameTxnDone.signal(); });
                sameTxnRan = sameTxnDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

                // and the load the abandoned fan-out was still waiting for lands: it must find the entry marked, keep it
                // referenced, and report nothing - the fan-out that was waiting for it is gone
                releaseLoad.signal();
                landed = await(() -> {
                    executor.executeDirectlyWithLock(() -> {
                        AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(slow);
                        slowSettled.set(entry != null && !entry.isLoading() && entry.isInconsistent() && entry.references() > 0);
                    });
                    return slowSettled.get();
                });
            }
        }
        finally
        {
            releaseLoad.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertTrue("the fan-out was never observed parked between rounds, so this test proves nothing", abandoned.get());
        assertEquals("with a batch of one and a threshold of two, exactly one key can have been applied before the "
                     + "fan-out parked: " + processed, 1, processed.size());
        assertTrue("the caller must be told the failure, and was told " + failure.get(), failure.get() == injected);
        assertTrue("exactly the keys the update did not reach may be marked, and exactly the marked keys may keep a"
                   + " claim or a reference: applied " + processed + ", marked " + marked + ", claimed " + claimed
                   + ", expected marked " + unreached(loaded, slow, processed), settled);
        assertTrue("the load the abandoned fan-out was waiting for must land on an entry that is still marked and still "
                   + "referenced, so the outstanding update cannot be lost", landed);
        assertTrue("a later task declaring the same txnId must be told, promptly, that an update is outstanding: it may "
                   + "not hang behind a HOLD_QUEUE lock the abandoned fan-out will never release (L2). Ran=" + sameTxnRan
                   + ", told " + sameTxn.get(),
                   sameTxnRan && sameTxn.get() instanceof InconsistentEntryException);
        assertTrue("no internal error may be reported - a failure recorded as RUN_INCOMPLETE is refused by completeState,"
                   + " which masks the real failure. The only report expected is the rejection of the later task we "
                   + "deliberately submitted against the now-marked txnId: " + agent.exceptions,
                   agent.exceptions.stream().allMatch(t -> t instanceof InconsistentEntryException));
    }

    /** every key the fan-out had not applied when it was abandoned, loaded or not */
    private static Set<RoutingKey> unreached(RoutingKey[] loaded, RoutingKey slow, Set<RoutingKey> processed)
    {
        Set<RoutingKey> unreached = ConcurrentHashMap.newKeySet();
        for (RoutingKey key : loaded)
        {
            if (!processed.contains(key))
                unreached.add(key);
        }
        unreached.add(slow);
        return unreached;
    }

    /** as the real INCR fan-outs declare themselves, and ATOMIC so that its failure must be witnessed */
    private static ExecutionContext fanOut(TxnId txnId, RoutingKey... keys)
    {
        ExecutionContext wrapped = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(keys), LoadKeys.INCR,
                                                               LoadKeysFor.READ_WRITE, "fanout");
        return new ExecutionContext.Wrapped()
        {
            @Override public ExecutionSequence executionSequence() { return ExecutionSequence.ATOMIC; }
            @Override public ExecutionContext wrapped() { return wrapped; }
            @Override public boolean isIdempotent() { return true; }
        };
    }

    /** a load that does not land until {@code release}, by which time the fan-out waiting for it has been abandoned */
    private static void setLoadFunction(AccordCache.Type type, RoutingKey slowAndFailing, Condition release)
    {
        type.unsafeSetLoadFunction((java.util.function.BiFunction<AccordCommandStore, Object, Object>) (ignoreStore, k) -> {
            if (slowAndFailing.equals(k))
            {
                release.awaitUninterruptibly();
            }
            return k instanceof RoutingKey ? new accord.local.cfk.CommandsForKey((RoutingKey) k) : null;
        });
        type.unsafeSetSaveFunction((accord.utils.QuadFunction<AccordCommandStore, Object, Object, Object, Runnable>) (ignoreStore, k, v, identity) -> () -> {});
    }

    private static RoutingKey key(TableId tableId, IPartitioner partitioner, int k)
    {
        return new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(k)));
    }

    private static boolean await(BooleanSupplier until) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline)
        {
            if (until.getAsBoolean())
                return true;
            Thread.sleep(1);
        }
        return false;
    }
}
