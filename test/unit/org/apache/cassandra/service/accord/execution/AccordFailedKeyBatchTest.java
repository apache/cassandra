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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This test has been authored entirely by Claude.
 *
 * A failed round with <em>more than one key in its batch</em>. Everything else in {@link AccordFailedKeyTest} runs with
 * a batch of one, so its rounds mark, retain and convert exactly one claim; the loop that does so, and the interaction
 * between several retained claims held at once by a task that is still running, are only exercised here.
 *
 * <p>It needs its own class because {@code AccordExecutor} reads the batch sizes once into static finals, so a JVM can
 * only have one setting - which is also why this class must not share a JVM with the others (ant gives each its own).
 */
public class AccordFailedKeyBatchTest
{
    private static final int TIMEOUT_SECONDS = 30;
    /** the batch every round takes: 3 of the 4 keys, so one round fails with three keys and one applies the remainder */
    private static final int BATCH = 3;
    private static final int KEYS = 4;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.getAccord().queue_nonsync_min_batch_size = BATCH;
        DatabaseDescriptor.getAccord().queue_nonsync_max_batch_size = BATCH;
    }

    @Before
    public void requireBatchSizeTookEffect()
    {
        // the config must win the race with AccordExecutor's first class-load, or every case here quietly runs with a
        // batch of one and tests what AccordFailedKeyTest already covers
        Invariants.require(AccordExecutor.NONSYNC_MAX_BATCH_SIZE == BATCH,
                           "expected a batch of %d, found %d: the config was read before setUp set it",
                           BATCH, AccordExecutor.NONSYNC_MAX_BATCH_SIZE);
    }

    static class InjectedBodyFailure extends RuntimeException
    {
        InjectedBodyFailure(Object keys)
        {
            super("injected body failure while applying " + keys);
        }
    }

    private static boolean isInjected(Throwable t)
    {
        for (Throwable cur = t ; cur != null ; cur = cur.getCause())
        {
            if (cur instanceof InjectedBodyFailure)
                return true;
        }
        return false;
    }

    /**
     * A round of three keys throws: all three are marked and retained - each with its reference and its claim - and the
     * fan-out carries on and applies the fourth.
     *
     * <p>What only a multi-key batch can catch: the retention loop must handle every key of the batch, and a task that
     * is still running must be able to hold several retained claims at once (each is a HOLD_QUEUE lock plus the fifo
     * head of its own entry, so the per-entry invariant has to hold for all of them simultaneously). A loop that stopped
     * at the first key, or a conversion that assumed at most one retained claim per task, passes every single-key test.
     */
    @Test
    public void bodyFailureMarksEveryKeyInItsBatch() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 1));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = new RoutingKey[KEYS];
        for (int i = 0 ; i < KEYS ; ++i)
            keys[i] = key(tableId, partitioner, i);

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(AccordFailedKeyTestHarness::setInMemoryFunctions);
        AccordCommandStore store = AccordFailedKeyTestHarness.commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicInteger rounds = new AtomicInteger();
        List<Integer> batchSizes = new CopyOnWriteArrayList<>();
        Set<RoutingKey> failedIn = ConcurrentHashMap.newKeySet();
        Set<RoutingKey> applied = ConcurrentHashMap.newKeySet();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Condition done = Condition.newOneTimeCondition();
        Set<RoutingKey> inconsistent = ConcurrentHashMap.newKeySet();
        Set<RoutingKey> claimed = ConcurrentHashMap.newKeySet();
        Map<RoutingKey, Integer> references = new ConcurrentHashMap<>();
        AtomicReference<Object> stalledOn = new AtomicReference<>();
        try
        {
            store.execute(fanOut(TxnId.fromValues(1, 1, 0, new Id(1)), keys),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              Unseekables<?> batch = safeStore.context().keys();
                              if (batch.isEmpty())
                                  return;
                              batchSizes.add(batch.size());
                              if (rounds.incrementAndGet() == 1)
                              {
                                  for (int i = 0 ; i < batch.size() ; ++i)
                                      failedIn.add((RoutingKey) batch.get(i));
                                  throw new InjectedBodyFailure(batch);
                              }
                              for (int i = 0 ; i < batch.size() ; ++i)
                                  applied.add((RoutingKey) batch.get(i));
                          },
                          (success, fail) -> { failure.set(fail); done.signal(); });

            // the callback fires at the failure, in round 1, so poll for the end state: both rounds run, the failed
            // batch marked, and the round that succeeded released
            assertTrue("the caller was never told", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            await(() -> {
                executor.executeDirectlyWithLock(() -> {
                    inconsistent.clear();
                    claimed.clear();
                    references.clear();
                    for (RoutingKey k : keys)
                    {
                        AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(k);
                        if (entry == null)
                            continue;
                        if (entry.isInconsistent())
                            inconsistent.add(k);
                        if (!entry.hasNoTasks())
                            claimed.add(k);
                        references.put(k, entry.references());
                    }
                    stalledOn.set(AccordExecutionTestUtils.anyInconsistentIntersecting(store, null));
                });
                if (rounds.get() != 2 || inconsistent.size() != BATCH)
                    return false;
                for (RoutingKey k : applied)
                {
                    if (!Integer.valueOf(0).equals(references.get(k)))
                        return false;
                }
                return true;
            });
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertEquals("the first round must have taken a batch of " + BATCH + ", or this test is the single-key case "
                     + "again: batches " + batchSizes, Integer.valueOf(BATCH), batchSizes.get(0));
        assertEquals("both rounds must have run: a failed round does not end the fan-out. Batches " + batchSizes, 2, rounds.get());
        assertEquals("the failed round must have held every key but one: " + failedIn, BATCH, failedIn.size());
        assertEquals("the second round must have applied the remainder: " + applied, KEYS - BATCH, applied.size());
        assertTrue("the caller must be told the body failure, and was told " + failure.get(), isInjected(failure.get()));

        assertEquals("every key of the failed batch must be marked, not just the first: " + inconsistent + " of "
                     + failedIn, failedIn, inconsistent);
        assertEquals("and every one of them retained - claim as well as reference - at the same time: claimed "
                     + claimed + ", references " + references, failedIn, claimed);
        for (RoutingKey k : failedIn)
            assertTrue("a marked key must keep its reference: " + references, references.get(k) > 0);
        for (RoutingKey k : applied)
        {
            assertTrue("an applied key must not be marked: " + inconsistent, !inconsistent.contains(k));
            assertEquals("an applied key must be released: " + references, Integer.valueOf(0), references.get(k));
        }
        assertNotNull("a durability report must be refused while the update is outstanding", stalledOn.get());
        assertTrue("no internal error may be reported: " + agent.exceptions,
                   agent.exceptions.stream().allMatch(AccordFailedKeyBatchTest::isInjected));
    }

    /**
     * The same shape, but the fan-out holds no fifo claim - no txnId and not ATOMIC, the {@code Set Durable} shape - so
     * nothing is retained: a multi-key batch must release <em>every</em> key it
     * locked. One key left locked is enough to fail the next task to reach {@code lockExclusive} for it, and with a
     * batch of one that cannot be told apart from releasing none.
     */
    @Test
    public void nonAtomicBatchReleasesEveryKey() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 2));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = new RoutingKey[KEYS];
        for (int i = 0 ; i < KEYS ; ++i)
            keys[i] = key(tableId, partitioner, i);

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(AccordFailedKeyTestHarness::setInMemoryFunctions);
        AccordCommandStore store = AccordFailedKeyTestHarness.commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicInteger rounds = new AtomicInteger();
        Set<RoutingKey> failedIn = ConcurrentHashMap.newKeySet();
        Condition done = Condition.newOneTimeCondition();
        Set<RoutingKey> claimed = ConcurrentHashMap.newKeySet();
        Set<RoutingKey> inconsistent = ConcurrentHashMap.newKeySet();
        Map<RoutingKey, Throwable> after = new ConcurrentHashMap<>();
        try
        {
            store.execute(nonAtomicFanOut(null, keys),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              Unseekables<?> batch = safeStore.context().keys();
                              if (batch.isEmpty())
                                  return;
                              if (rounds.incrementAndGet() == 1)
                              {
                                  for (int i = 0 ; i < batch.size() ; ++i)
                                      failedIn.add((RoutingKey) batch.get(i));
                                  throw new InjectedBodyFailure(batch);
                              }
                          },
                          (success, fail) -> done.signal());

            assertTrue("the caller was never told", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            await(() -> {
                executor.executeDirectlyWithLock(() -> {
                    claimed.clear();
                    inconsistent.clear();
                    for (RoutingKey k : keys)
                    {
                        AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(k);
                        if (entry == null)
                            continue;
                        if (!entry.hasNoTasks())
                            claimed.add(k);
                        if (entry.isInconsistent())
                            inconsistent.add(k);
                    }
                });
                return rounds.get() == 2 && claimed.isEmpty();
            });

            // every key of the failed batch must now be usable: a lock left behind on any one of them fails the next
            // task to prepare for it
            for (RoutingKey k : failedIn)
            {
                Condition afterDone = Condition.newOneTimeCondition();
                store.execute(ExecutionContext.contextFor(TxnId.fromValues(1, 2 + after.size(), 0, new Id(1)), null,
                                                          RoutingKeys.of(k), LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "after"),
                              (Consumer<? super SafeCommandStore>) ignore -> {},
                              (success, fail) -> { if (fail != null) after.put(k, fail); afterDone.signal(); });
                assertTrue("the later operation on " + k + " was never notified", afterDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertEquals("both rounds must have run", 2, rounds.get());
        assertEquals("an update that need not be witnessed must mark nothing: " + inconsistent,
                     java.util.Collections.emptySet(), inconsistent);
        assertEquals("and must leave no claim on any key of the failed batch: " + claimed,
                     java.util.Collections.emptySet(), claimed);
        assertEquals("every key of the failed batch must still be usable, but these were refused: " + after,
                     java.util.Collections.emptyMap(), after);
        assertNull("no internal error may be reported: " + agent.exceptions,
                   agent.exceptions.stream().filter(t -> !isInjected(t)).findFirst().orElse(null));
    }

    /** as the real ATOMIC fan-outs declare themselves: the isolation they promise is what makes retention necessary */
    private static ExecutionContext fanOut(TxnId txnId, RoutingKey... keys)
    {
        ExecutionContext wrapped = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(keys), LoadKeys.INCR,
                                                              LoadKeysFor.READ_WRITE, "fanout");
        return new ExecutionContext.Wrapped()
        {
            @Override public ExecutionContext wrapped() { return wrapped; }
            @Override public ExecutionSequence executionSequence() { return ExecutionSequence.ATOMIC; }
            @Override public boolean isIdempotent() { return true; }
        };
    }

    /**
     * The same fan-out without a fifo claim to keep - BY_PRIORITY and, crucially, no txnId - which is what decides
     * whether a failed round retains anything ({@code NonSyncState.postRunExclusive} gates on {@code isAtomic()}, which a
     * txnId-declaring INCR task acquires when {@code prepareExclusiveMayThrow} upgrades it to a fifo claim on its first
     * run).
     */
    private static ExecutionContext nonAtomicFanOut(@javax.annotation.Nullable TxnId txnId, RoutingKey... keys)
    {
        ExecutionContext wrapped = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(keys), LoadKeys.INCR,
                                                              LoadKeysFor.READ_WRITE, "fanout");
        return new ExecutionContext.Wrapped()
        {
            @Override public ExecutionContext wrapped() { return wrapped; }
            @Override public boolean isIdempotent() { return true; }
        };
    }

    private static RoutingKey key(TableId tableId, IPartitioner partitioner, int k)
    {
        return new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(k)));
    }

    /** poll {@code until} for up to {@link #TIMEOUT_SECONDS}; the conditions here are set by the executor's loop thread */
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
