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

import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.RoutingKey;
import accord.impl.TestAgent;
import accord.local.ExecutionContext;
import accord.local.LoadKeys;
import accord.local.LoadKeysFor;
import accord.local.Node.Id;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordFailedKeyTestHarness;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.concurrent.Condition;

import static accord.local.ExecutionContext.ExecutionSequence.ATOMIC;
import static org.apache.cassandra.service.accord.execution.AccordExecutionTestUtils.anyInconsistentIntersecting;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This test has been authored entirely by Claude.
 *
 * The failure semantics of an incremental ({@code INCR}) fan-out, as agreed: a key whose load fails does <em>not</em>
 * abandon the fan-out. The task keeps going with its remaining batches, the failing key is marked FAILED so that no
 * future operation uses it, and durability bounds that overlap a FAILED key stall.
 *
 * <p>These are the properties that the current code does not have. Today {@code SafeTask.onFailedToLoadExclusive}
 * fails the <em>task</em>: below {@code WAITING_TO_RUN} through {@code tryFailAndCompleteUnexecutedExclusive}, and with
 * a run in flight through {@code failWhileRunningExclusive} / {@code RUNNING_WHILE_FAILED}, which discards the round's
 * work and completes the task. Every key the fan-out had not yet reached is then dropped silently, with the only
 * report going to the agent - and, because the durability bound is computed from what is <em>modified in the cache</em>
 * rather than from what is outstanding, replay will not re-derive them either.
 *
 * <p>The harness deliberately has no schema, cluster metadata or commit log: an in-memory journal and cache load
 * functions supplied per test. {@code TestAgent} is used rather than {@code AccordAgent}, as reporting an exception
 * through the latter initialises {@code AccordSystemMetrics}, which needs a started {@code AccordService}.
 */
public class AccordFailedKeyTest
{
    private static final int TIMEOUT_SECONDS = 30;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        // one key per batch, so a three key fan-out takes three rounds and the failing key is its own round
        DatabaseDescriptor.getAccord().queue_nonsync_min_batch_size = 1;
        DatabaseDescriptor.getAccord().queue_nonsync_max_batch_size = 1;
    }

    static class InjectedLoadFailure extends RuntimeException
    {
        InjectedLoadFailure(Object key)
        {
            super("injected load failure for " + key);
        }
    }

    static class InjectedBodyFailure extends RuntimeException
    {
        InjectedBodyFailure(Object key)
        {
            super("injected body failure while applying " + key);
        }
    }

    private static boolean isInjected(Throwable t)
    {
        return isInjected(t, InjectedLoadFailure.class);
    }

    private static boolean isInjected(Throwable t, Class<? extends Throwable> type)
    {
        for (Throwable cur = t ; cur != null ; cur = cur.getCause())
        {
            if (type.isInstance(cur))
                return true;
        }
        return false;
    }

    /**
     * An INCR fan-out over three keys, one of which cannot be loaded, must still process the other two, and must
     * terminate.
     *
     * <p>Two distinct failures are asserted, because they have different causes:
     * <ul>
     *   <li><b>the remaining keys are processed.</b> Today the first failed load fails the whole task, so whichever
     *       keys the fan-out had not yet reached are never processed and nobody is told which they were.</li>
     *   <li><b>the task terminates.</b> This is the hazard introduced by <em>fixing</em> the first: a dropped key must
     *       be accounted for in the round loop. {@code NonSyncState.prepareExclusive} advances {@code processed} by the
     *       size of each batch and only sets {@code INCREMENTAL_FINISHING} when {@code processed == keys}, and
     *       {@code isWaitReady} needs {@code readyCount() >= min(keys - processed, MIN_BATCH)}. A key that failed to
     *       load is never in {@code blocking}/{@code notBlocking}, so it is never in a batch and never counted: drop it
     *       without adjusting the accounting and the task parks in {@code WAITING_ON_KEY} for ever, holding its
     *       {@code HOLD_QUEUE} lock on its TxnId, and every later task on that command queues behind it. That is L2
     *       ("a started INCR task releases HOLD_QUEUE in finitely many rounds") failing.</li>
     * </ul>
     *
     * <p>The report must also name the failure rather than reporting plain success, or the submitter of a
     * {@code Commands.setDurability} fan-out believes every key learned of the durability.
     */
    @Test
    public void incrementalFanOutContinuesPastFailedKey() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 1));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = { key(tableId, partitioner, 0), key(tableId, partitioner, 1), key(tableId, partitioner, 2) };
        RoutingKey failing = keys[1];

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        AtomicInteger loadAttempts = new AtomicInteger();
        // the failure must land on a fan-out that has committed to execute, so hold it until a round has run
        Condition hasRun = Condition.newOneTimeCondition();
        executor.cacheUnsafe().types().forEach(type -> setLoadFunction(type, failing, loadAttempts, hasRun));
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        Set<RoutingKey> processed = ConcurrentHashMap.newKeySet();
        List<Integer> batchSizes = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<String> stillClaimed = new AtomicReference<>();
        Set<RoutingKey> poisonedKeys = ConcurrentHashMap.newKeySet();
        AtomicReference<Boolean> blocking = new AtomicReference<>();
        Condition done = Condition.newOneTimeCondition();
        boolean completed = false, quiescent = false;
        try
        {
            ExecutionContext context = fanOut(TxnId.fromValues(1, 1, 0, new Id(1)), LoadKeys.INCR, true, true, keys);
            store.execute(context, (Consumer<? super SafeCommandStore>) safeStore -> {
                Unseekables<?> batch = safeStore.context().keys();
                batchSizes.add(batch.size());
                for (int i = 0 ; i < batch.size() ; ++i)
                    processed.add((RoutingKey) batch.get(i));
                if (!batch.isEmpty())
                    hasRun.signal();
            }, (success, fail) -> {
                failure.set(fail);
                done.signal();
            });

            completed = done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (completed)
            {
                // R6, as restated for this design: a completed task keeps its claim on the key it could not reach -
                // that is what blocks everything queued behind it, rather than letting it read a state the update did
                // not reach - and on nothing else, or unrelated work waits on a task that will never run.
                // Poll: the report reaches the callback before the task releases anything.
                await(() -> {
                    executor.executeDirectlyWithLock(() -> {
                        String claimed = null;
                        poisonedKeys.clear();
                        for (RoutingKey key : keys)
                        {
                            AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(key);
                            if (entry == null)
                                continue;
                            if (entry.isInconsistent())
                                poisonedKeys.add(key);
                            else if (!entry.hasNoTasks())
                                claimed = entry.toString();
                        }
                        stillClaimed.set(claimed);
                        AccordCacheEntry<?, ?, ?> failedEntry = store.cachesUnsafe().commandsForKeys().getUnsafe(failing);
                        blocking.set(failedEntry == null ? null : (!failedEntry.hasNoTasks() && failedEntry.references() > 0));
                    });
                    return stillClaimed.get() == null && poisonedKeys.contains(failing);
                });
                // ...and the executor's accounting must return to empty: a fan-out that dropped a key is still
                // registered and completed exactly once, or waitForQuiescence/afterSubmittedAndConsequences never fire
                // again. The reference it deliberately retains on the failing key is not a task.
                quiescent = await(() -> !executor.hasTasks() && executor.unsafeRunningCount() == 0);
            }
        }
        finally
        {
            // shut down before asserting, so that a failure here cannot leave this executor running into the next test
            hasRun.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertTrue("the fan-out never completed: a dropped key must still be accounted for in the round loop, or "
                   + "processed can never reach keys and the task parks in WAITING_ON_KEY holding its TxnId lock "
                   + "for ever (L2). Batches: " + batchSizes + ", processed: " + processed,
                   completed);
        assertTrue("the fan-out must process every key it can: a load failure on " + failing + " must not abandon "
                   + "the keys the fan-out had not yet reached. Processed " + processed + " in batches " + batchSizes,
                   processed.contains(keys[0]) && processed.contains(keys[2]));
        assertTrue("the failing key must not be processed", !processed.contains(failing));
        assertTrue("the report must name the failure - a caller told 'success' believes every key learned of the "
                   + "update - but was " + failure.get(),
                   failure.get() != null && isInjected(failure.get()));
        assertTrue("an entry the fan-out did reach is still claimed by it: " + stillClaimed.get(),
                   stillClaimed.get() == null);
        // and the key it could not reach is deliberately still claimed and referenced: the claim blocks the work queued
        // behind it, and the reference stops the entry - and the record that the update is outstanding - being evicted
        assertEquals("the key the update could not reach must still be claimed and referenced by the failed fan-out",
                     Boolean.TRUE, blocking.get());
        assertTrue("an idempotent update must not be escalated: replay re-applies it", !agent.saw(InconsistentEntryException.class));
        // blast radius: only the key the update did not reach is poisoned. The keys processed in the final round are
        // still held in refs when the task completes (reporting a key failure skips NonSyncState.postRunExclusive), so
        // "poison what we still hold" must exclude a batch whose body ran to completion, or a successfully applied key
        // is poisoned and the store stalls for it.
        assertEquals("only the key the update could not reach may be poisoned, but poisoned " + poisonedKeys,
                     java.util.Collections.singleton(failing), poisonedKeys);
        assertTrue("the executor still has registered tasks or an assigned runner after the fan-out completed", quiescent);
    }

    /**
     * A key an update failed to reach is inconsistent, and a later operation that intersects it is rejected outright:
     * the update is still outstanding, so nothing may act on the key's state, and nothing may advance a bound over it.
     *
     * <p>Two things are asserted, and the second is what makes the first durable enough to rely on. The rejection
     * itself is a property of the entry's INCONSISTENT bit, so it only means anything for as long as the entry is
     * there - and the bit is not durable. What keeps it there is that the failed task <em>retains its reference</em>:
     * an entry with a live reference cannot be evicted, so the bit cannot be silently dropped and the key cannot be
     * quietly re-loaded as though nothing had happened. The load-attempt count is how we tell the difference: a second
     * attempt would mean the entry had gone and been rebuilt.
     */
    @Test
    public void operationIntersectingFailedKeyIsRejected() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 2));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey ok = key(tableId, partitioner, 0);
        RoutingKey failing = key(tableId, partitioner, 1);

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        AtomicInteger loadAttempts = new AtomicInteger();
        // the failure must land on a fan-out that has committed to execute, so hold it until a round has run
        Condition hasRun = Condition.newOneTimeCondition();
        executor.cacheUnsafe().types().forEach(type -> setLoadFunction(type, failing, loadAttempts, hasRun));
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicReference<Throwable> second = new AtomicReference<>();
        AtomicReference<Integer> references = new AtomicReference<>(0);
        AtomicReference<Boolean> inconsistent = new AtomicReference<>(false);
        try
        {
            AtomicReference<Throwable> first = new AtomicReference<>();
            Condition firstDone = Condition.newOneTimeCondition();
            store.execute(fanOut(TxnId.fromValues(1, 1, 0, new Id(1)), LoadKeys.INCR, true, true, ok, failing),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              if (!safeStore.context().keys().isEmpty())
                                  hasRun.signal();
                          },
                          (success, fail) -> { first.set(fail); firstDone.signal(); });
            assertTrue("the fan-out never completed", firstDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals("the failing key must have been attempted exactly once", 1, loadAttempts.get());

            // poll: the failure reaches the callback before the task retains and releases anything
            await(() -> {
                executor.executeDirectlyWithLock(() -> {
                    AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(failing);
                    inconsistent.set(entry != null && entry.isInconsistent());
                    references.set(entry == null ? 0 : entry.references());
                });
                return inconsistent.get() && references.get() > 0;
            });

            // ... and a later, unrelated operation that touches the same key
            Condition secondDone = Condition.newOneTimeCondition();
            store.execute(ExecutionContext.contextFor(TxnId.fromValues(1, 2, 0, new Id(1)), null, RoutingKeys.of(failing),
                                                      LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "after"),
                          (Consumer<? super SafeCommandStore>) ignore -> {},
                          (success, fail) -> { second.set(fail); secondDone.signal(); });
            assertTrue("the later operation was never notified", secondDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
        finally
        {
            hasRun.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertTrue("the key the update could not reach must be inconsistent", inconsistent.get());
        assertTrue("the failed task must retain its reference, or the entry - and the record that an update is "
                   + "outstanding - can be evicted", references.get() > 0);
        assertTrue("an operation intersecting an inconsistent key must be rejected, and was told " + second.get(),
                   second.get() != null);
        assertEquals("the rejection must be outright, not by re-attempting the load: the update that did not reach "
                     + "the key is still outstanding", 1, loadAttempts.get());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void evictUnsafe(AccordExecutor executor, AccordCacheEntry<?, ?, ?> entry)
    {
        executor.cacheUnsafe().tryEvict((AccordCacheEntry) entry);
    }

    /** raw, because the cache types are heterogeneous: a key loads to an empty CommandsForKey, a TxnId to null */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void setLoadFunction(AccordCache.Type type, RoutingKey failing, AtomicInteger loadAttempts)
    {
        setLoadFunction(type, failing, loadAttempts, null);
    }

    /**
     * As above, but the failing load waits for {@code hasRun} first, so that the failure lands on a fan-out that has
     * <em>committed to execute</em> - which is the only case that drops the key and carries on. Without the fence the
     * outcome is a race between the load and the first round: if the failure wins, the fan-out has applied nothing and is
     * failed outright, as {@link #syncFailureMarksNothingEither} and
     * {@link #fanOutRefusedBeforeItStartsFailsLikeAnyOtherTask} require of a task that published nothing.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void setLoadFunction(AccordCache.Type type, RoutingKey failing, AtomicInteger loadAttempts, @javax.annotation.Nullable Condition hasRun)
    {
        type.unsafeSetLoadFunction((java.util.function.BiFunction<AccordCommandStore, Object, Object>) (ignoreStore, k) -> {
            if (failing.equals(k))
            {
                if (hasRun != null && !hasRun.awaitUninterruptibly(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    throw new AssertionError("the fan-out never ran a round before its failing key was loaded");
                loadAttempts.incrementAndGet();
                throw new InjectedLoadFailure(k);
            }
            return k instanceof RoutingKey ? new CommandsForKey((RoutingKey) k) : null;
        });
        type.unsafeSetSaveFunction((accord.utils.QuadFunction<AccordCommandStore, Object, Object, Object, Runnable>) (ignoreStore, k, v, identity) -> () -> {});
    }

    /**
     * An update that need not be witnessed does not poison. Nothing later depends on it having been applied, so the
     * entry's existing state stays usable, later operations on the key run normally, and no durability bound stalls -
     * the only consequence is that the caller is told the update did not complete.
     *
     * <p>The failure is held until a round has run, so this is the fan-out that drops a key <em>and carries on</em> while
     * holding no fifo claim: its position on the dropped key went with the failed load (only a fifo claim survives
     * {@code drainWaitingToLoad}), so the key is in {@code refs}, abandoned and positionless, for the rest of the
     * fan-out's life - which {@code waitToRunExclusive}'s ref scan must tolerate. It is asserted through the agent,
     * because that scan runs re-entrantly inside {@code onLoadedExclusive}'s notification, whose per-task catch swallows
     * the failure: the fan-out is then left in no queue holding its other claims, and only the agent knows.
     */
    @Test
    public void updateThatNeedNotBeWitnessedDoesNotPoison() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 3));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey ok = key(tableId, partitioner, 0);
        RoutingKey failing = key(tableId, partitioner, 1);

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        AtomicInteger loadAttempts = new AtomicInteger();
        Condition hasRun = Condition.newOneTimeCondition();
        executor.cacheUnsafe().types().forEach(type -> setLoadFunction(type, failing, loadAttempts, hasRun));
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicReference<Throwable> first = new AtomicReference<>();
        AtomicReference<Boolean> poisoned = new AtomicReference<>(true);
        AtomicReference<Throwable> second = new AtomicReference<>();
        Condition firstDone = Condition.newOneTimeCondition();
        Condition secondDone = Condition.newOneTimeCondition();
        try
        {
            // no txnId and not ATOMIC, so no fifo claim is ever taken: this is the "Set Durable" shape, the update
            // whose effects need not be witnessed
            store.execute(fanOut(null, LoadKeys.INCR, false, true, ok, failing),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              if (!safeStore.context().keys().isEmpty())
                                  hasRun.signal();
                          },
                          (success, fail) -> { first.set(fail); firstDone.signal(); });
            assertTrue("the fan-out never completed", firstDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // the failure is still reported to the caller ...
            assertTrue("the caller must still be told the update did not complete, but was told " + first.get(),
                       first.get() != null && isInjected(first.get()));

            // ... but nothing is poisoned, and the load may be re-attempted by whoever needs the key next
            await(() -> {
                executor.executeDirectlyWithLock(() -> {
                    AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(failing);
                    poisoned.set(entry != null && entry.isInconsistent());
                });
                return !poisoned.get();
            });
            executor.executeDirectlyWithLock(() ->
                assertNull("no durability report may be stalled by an update that need not be witnessed",
                           anyInconsistentIntersecting(store, null)));

            store.execute(ExecutionContext.contextFor(TxnId.fromValues(1, 2, 0, new Id(1)), null, RoutingKeys.of(ok),
                                                      LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "after"),
                          (Consumer<? super SafeCommandStore>) ignore -> {},
                          (success, fail) -> { second.set(fail); secondDone.signal(); });
            assertTrue("the later operation was never notified", secondDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
        finally
        {
            hasRun.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertTrue("the entry must not be poisoned by an update that need not be witnessed", !poisoned.get());
        assertNull("an operation on an unaffected key must run normally", second.get());
        assertTrue("no internal error may be reported: a dropped key stays in refs, abandoned and positionless, and every "
                   + "invariant over refs must tolerate it: " + agent.exceptions,
                   agent.exceptions.stream().allMatch(AccordFailedKeyTest::isInjected));
    }

    /**
     * An incremental fan-out may only be issued if it is idempotent, and that is now a precondition rather than
     * something to cope with after the fact: a failed round is retried, and replay re-issues the whole operation, so
     * an update that cannot be re-executed cannot be recovered by either. The submission is refused outright, where it
     * can still be attributed to its caller, rather than becoming an unrecoverable failure later on.
     */
    @Test
    public void nonIdempotentIncrementalUpdateIsRefused() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 4));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey a = key(tableId, partitioner, 0);
        RoutingKey b = key(tableId, partitioner, 1);

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(AccordFailedKeyTestHarness::setInMemoryFunctions);
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        Condition done = Condition.newOneTimeCondition();
        AtomicInteger rounds = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try
        {
            store.execute(fanOut(TxnId.fromValues(1, 1, 0, new Id(1)), LoadKeys.INCR, true, false, a, b),
                          (Consumer<? super SafeCommandStore>) ignore -> rounds.incrementAndGet(),
                          (success, fail) -> { failure.set(fail); done.signal(); });
            assertTrue("the caller was never told", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertNotNull("a non-idempotent incremental fan-out must be refused, not run", failure.get());
        assertEquals("it must not have run at all", 0, rounds.get());
    }

    /**
     * A round whose body throws loses that round's keys and nothing else: the fan-out drops them - marked and retained,
     * so no later operation acts on a state the update never reached and no durability report is made - and carries on
     * with its remaining batches, exactly as it does for a key whose load failed. It does not abandon the update: a
     * fan-out that gave up after its first round succeeded would leave every key it had not reached silently
     * un-updated, which is the hole the marking exists to close.
     *
     * <p>Three properties, and the middle one is what this test exists for:
     * <ul>
     *   <li>the round after the failure still runs and applies its key;</li>
     *   <li>only the failed round's key is marked, and it is <em>retained</em> - reference and claim - so its entry,
     *       and the INCONSISTENT bit on it, cannot be evicted, and anything queued on it blocks;</li>
     *   <li>the keys that were applied are neither marked nor retained: their state is correct and current, and marking
     *       them would refuse every later operation on them and stall their ranges for nothing.</li>
     * </ul>
     *
     * <p>The caller is told the failure at the moment it happens, which takes the callback, so the successful finish of
     * the last round cannot report success over the top of it.
     */
    @Test
    public void bodyFailureMarksItsRoundAndContinues() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 5));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = { key(tableId, partitioner, 0), key(tableId, partitioner, 1), key(tableId, partitioner, 2) };

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(AccordFailedKeyTestHarness::setInMemoryFunctions);
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        List<RoutingKey> applied = new CopyOnWriteArrayList<>();
        List<RoutingKey> offered = new CopyOnWriteArrayList<>();
        AtomicReference<RoutingKey> failedIn = new AtomicReference<>();
        AtomicInteger rounds = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Condition done = Condition.newOneTimeCondition();
        Set<RoutingKey> inconsistent = ConcurrentHashMap.newKeySet();
        Map<RoutingKey, Integer> references = new ConcurrentHashMap<>();
        AtomicReference<Object> stalledOn = new AtomicReference<>();
        try
        {
            store.execute(fanOut(TxnId.fromValues(1, 1, 0, new Id(1)), LoadKeys.INCR, true, true, keys),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              Unseekables<?> batch = safeStore.context().keys();
                              if (batch.isEmpty())
                                  return;
                              RoutingKey k = (RoutingKey) batch.get(0);
                              offered.add(k);
                              if (rounds.incrementAndGet() == 2)
                              {
                                  failedIn.set(k);
                                  throw new InjectedBodyFailure(k);
                              }
                              applied.add(k);
                          },
                          (success, fail) -> { failure.set(fail); done.signal(); });

            // The callback fires at the failure, in round 2, so it is not a fence for the rounds after it: the fan-out
            // carries on, and only when it has finished are the keys it applied released. So poll for the end state -
            // every round run, one key marked, and every applied key released - rather than for the report.
            assertTrue("the caller was never told", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            await(() -> {
                executor.executeDirectlyWithLock(() -> {
                    inconsistent.clear();
                    references.clear();
                    for (RoutingKey k : keys)
                    {
                        AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(k);
                        if (entry == null)
                            continue;
                        if (entry.isInconsistent())
                            inconsistent.add(k);
                        references.put(k, entry.references());
                    }
                    stalledOn.set(anyInconsistentIntersecting(store, null));
                });
                if (rounds.get() != keys.length || inconsistent.size() != 1)
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

        assertNotNull("the second round never ran, so this test proves nothing", failedIn.get());
        assertEquals("the fan-out must run every round: one that failed, and one either side of it. Rounds " + rounds.get()
                     + ", applied " + applied, 3, rounds.get());
        assertEquals("the rounds either side of the failure must both have applied their key: " + applied,
                     2, applied.size());
        assertTrue("the round after the failure must have applied its key: the fan-out carries on past a failed round, "
                   + "as it does past a key whose load failed. Applied " + applied,
                   !applied.contains(failedIn.get()));
        // the accounting half of carrying on: a failed round's keys are counted as processed, not put back, so the
        // fan-out neither offers them again (which would re-run a body over a key it has abandoned and marked) nor
        // waits for them for ever (they are gone from refs, so nothing can make them ready again)
        assertEquals("no key may be offered to the body twice: " + offered,
                     new java.util.HashSet<>(offered).size(), offered.size());
        assertEquals("every key must be offered exactly once: " + offered, keys.length, offered.size());
        assertTrue("the caller must be told the body failure, and was told " + failure.get(),
                   failure.get() != null && isInjected(failure.get(), InjectedBodyFailure.class));

        assertEquals("only the failed round's key may be marked inconsistent, but " + inconsistent + " were",
                     java.util.Collections.singleton(failedIn.get()), inconsistent);
        assertTrue("the failed round's key must be retained, or its entry can be evicted and the record of the "
                   + "outstanding update lost: references " + references,
                   references.get(failedIn.get()) != null && references.get(failedIn.get()) > 0);
        for (RoutingKey k : applied)
            assertEquals("an applied key must be released, not retained: " + k, Integer.valueOf(0), references.get(k));
        assertNotNull("a durability report must be stalled while an update is outstanding", stalledOn.get());
    }

    /**
     * A fan-out whose <em>first</em> round fails must mark and retain that round's key, and must still go on to apply
     * the rest.
     *
     * <p>Two distinct regressions, which is why the first round is worth its own test:
     * <ul>
     *   <li>{@code completeExclusiveMayThrow}'s guard used to admit a failed non-sync task only when
     *       {@code hasIncrementalStarted()}, and INCREMENTAL_STARTED is set at the <em>end</em> of a round - so a
     *       first-round failure skipped {@code postRunExclusive} altogether: nothing was marked, the round's keys were
     *       released as though nothing had happened, and the durability bound was left free to advance over an update
     *       that only replay could re-derive, and that replay would then skip. The guard is {@code is(PREPARED)}.</li>
     *   <li>the fan-out must not stop. A failed round used to be terminal, so a single failure at the front dropped
     *       every remaining key silently - no mark, no report naming them, nothing for a later operation to trip
     *       over.</li>
     * </ul>
     */
    @Test
    public void firstRoundFailureMarksItsRound() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 9));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = { key(tableId, partitioner, 0), key(tableId, partitioner, 1) };

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(AccordFailedKeyTestHarness::setInMemoryFunctions);
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicInteger rounds = new AtomicInteger();
        List<RoutingKey> applied = new CopyOnWriteArrayList<>();
        AtomicReference<RoutingKey> failedIn = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Condition done = Condition.newOneTimeCondition();
        Set<RoutingKey> inconsistent = ConcurrentHashMap.newKeySet();
        Map<RoutingKey, Integer> references = new ConcurrentHashMap<>();
        AtomicReference<Object> stalledOn = new AtomicReference<>();
        try
        {
            store.execute(fanOut(TxnId.fromValues(1, 1, 0, new Id(1)), LoadKeys.INCR, true, true, keys),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              Unseekables<?> batch = safeStore.context().keys();
                              if (batch.isEmpty())
                                  return;
                              RoutingKey k = (RoutingKey) batch.get(0);
                              if (rounds.incrementAndGet() == 1)
                              {
                                  failedIn.set(k);
                                  throw new InjectedBodyFailure(k);
                              }
                              applied.add(k);
                          },
                          (success, fail) -> { failure.set(fail); done.signal(); });

            // the callback fires at the failure, in round 1, so poll for the end state rather than treating it as a fence
            assertTrue("the caller was never told", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            await(() -> {
                executor.executeDirectlyWithLock(() -> {
                    inconsistent.clear();
                    references.clear();
                    for (RoutingKey k : keys)
                    {
                        AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(k);
                        if (entry == null)
                            continue;
                        if (entry.isInconsistent())
                            inconsistent.add(k);
                        references.put(k, entry.references());
                    }
                    stalledOn.set(anyInconsistentIntersecting(store, null));
                });
                return rounds.get() == keys.length && inconsistent.size() == 1
                       && applied.size() == 1 && Integer.valueOf(0).equals(references.get(applied.get(0)));
            });
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertNotNull("the body never ran, so this test proves nothing", failedIn.get());
        assertEquals("the fan-out must carry on past a first-round failure and apply its remaining key: rounds "
                     + rounds.get() + ", applied " + applied, 2, rounds.get());
        assertEquals("the round after the failure must have applied its key: " + applied, 1, applied.size());
        assertTrue("and it must be the other key, not the one that failed: " + applied,
                   !applied.contains(failedIn.get()));
        assertTrue("the caller must be told the body failure, and was told " + failure.get(),
                   failure.get() != null && isInjected(failure.get(), InjectedBodyFailure.class));
        assertEquals("the first round's key must be marked inconsistent, but " + inconsistent + " were",
                     java.util.Collections.singleton(failedIn.get()), inconsistent);
        assertTrue("the first round's key must be retained, or its entry can be evicted and the record of the "
                   + "outstanding update lost: references " + references,
                   references.get(failedIn.get()) != null && references.get(failedIn.get()) > 0);
        assertEquals("the key applied after the failure must be released, not retained: " + references,
                     Integer.valueOf(0), references.get(applied.get(0)));
        assertNotNull("a durability report must be refused while the update is outstanding", stalledOn.get());
    }

    /**
     * A task already queued on a key when an INCR round fails for it is <em>not served</em> - it would read a state the
     * update never reached - and <em>keeps its claim</em>, which is the order a retry has to preserve and, for an ATOMIC
     * task, the isolation it was promised. What it does not keep is the accounting: its caller is told now
     * ({@code InconsistentEntryException}) rather than waiting on something that will not happen, and it leaves its
     * tranche, so durability reports and quiescence are not stalled behind it.
     *
     * <p>Getting the not-served half to work at all took the retained claim: a round locks its batch with RELEASE_QUEUE,
     * so what a failed round holds is the entry's <em>lock</em> and no position, and the task behind it was told
     * NEWLY_RUNNABLE when we took that lock (we were expected to release it in the same completion). It was therefore
     * scheduled, reached {@code SaferCommandsForKey.preExecute} and died on {@code Invariants.require(!isLocked())} in
     * {@code AccordCacheEntry.lockExclusive} - neither served nor blocked, but failed by an internal invariant.
     *
     * <p>So this asserts four things: the blocked task's body never runs, it is told the outstanding update rather than
     * an internal error, it still holds its claim on the entry, and the store still runs work on an unrelated key (the
     * block is per key, not a stalled executor). That its tranche is released is
     * {@link #blockedTaskDoesNotStallTheTrancheBarrier}.
     */
    @Test
    public void oneTaskQueuedBehindFailedRoundBlocks() throws InterruptedException
    {
        // one waiter: the entry's claims are the lock holder plus a single task, an AccordCacheEntryMiniQueue
        taskQueuedBehindFailedRoundBlocks(10, 1);
    }

    @Test
    public void twoTasksQueuedBehindFailedRoundBlock() throws InterruptedException
    {
        // two waiters force a full AccordCacheEntryQueue, so the retained claim must go back into the fifo region
        taskQueuedBehindFailedRoundBlocks(11, 2);
    }

    private void taskQueuedBehindFailedRoundBlocks(int tableIdSuffix, int waiters) throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, tableIdSuffix));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = { key(tableId, partitioner, 0), key(tableId, partitioner, 1) };
        RoutingKey unrelated = key(tableId, partitioner, 2);

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(AccordFailedKeyTestHarness::setInMemoryFunctions);
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicInteger rounds = new AtomicInteger();
        // the key of the round that is about to fail, published by the body so that the blocked task queues on that key
        AtomicReference<RoutingKey> failingIn = new AtomicReference<>();
        Condition queuedBehind = Condition.newOneTimeCondition();
        AtomicReference<Throwable> fanOutFailure = new AtomicReference<>();
        Condition fanOutDone = Condition.newOneTimeCondition();
        AtomicReference<Object> blockedReport = new AtomicReference<>();
        Condition blockedDone = Condition.newOneTimeCondition();
        AtomicReference<Throwable> unrelatedFailure = new AtomicReference<>();
        Condition unrelatedDone = Condition.newOneTimeCondition();
        AtomicReference<Boolean> stillClaimed = new AtomicReference<>();
        boolean blockedWasNotified;
        boolean unrelatedRan = false;
        try
        {
            store.execute(fanOut(TxnId.fromValues(1, 1, 0, new Id(1)), LoadKeys.INCR, true, true, keys),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              Unseekables<?> batch = safeStore.context().keys();
                              if (batch.isEmpty())
                                  return;
                              RoutingKey k = (RoutingKey) batch.get(0);
                              if (rounds.incrementAndGet() != 2)
                                  return;
                              // hold the round while the test queues a task on the key it is about to fail for
                              failingIn.set(k);
                              queuedBehind.awaitUninterruptibly();
                              throw new InjectedBodyFailure(k);
                          },
                          (success, fail) -> { fanOutFailure.set(fail); fanOutDone.signal(); });

            // wait for the second round to have taken its lock and told us which key it holds
            assertTrue("the second round never ran", await(() -> failingIn.get() != null));
            RoutingKey blocked = failingIn.get();
            for (int i = 0 ; i < waiters ; ++i)
            {
                store.execute(ExecutionContext.contextFor(TxnId.fromValues(1, 2 + i, 0, new Id(1)), null, RoutingKeys.of(blocked),
                                                          LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "behind"),
                              (Consumer<? super SafeCommandStore>) ignore -> blockedReport.set("ran"),
                              (success, fail) -> { blockedReport.compareAndSet(null, fail == null ? "success" : fail); blockedDone.signal(); });
                // "ran" would overwrite the failure, so a body that runs is visible in the assertion below
            }

            // each has taken its own reference on the entry once it is set up and queued behind the running round
            assertTrue("the tasks behind the round never queued on " + blocked,
                       await(() -> {
                           AtomicInteger references = new AtomicInteger();
                           executor.executeDirectlyWithLock(() -> {
                               AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(blocked);
                               references.set(entry == null ? 0 : entry.references());
                           });
                           return references.get() >= 1 + waiters;
                       }));

            queuedBehind.signal();
            assertTrue("the fan-out never completed", fanOutDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            // positive fence: the marking has happened and the dead fan-out still holds the entry
            assertTrue("the failed round never marked and retained " + blocked,
                       await(() -> {
                           executor.executeDirectlyWithLock(() -> {
                               AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(blocked);
                               stillClaimed.set(entry != null && entry.isInconsistent() && entry.references() > 0
                                                && !entry.hasNoTasks());
                           });
                           return Boolean.TRUE.equals(stillClaimed.get());
                       }));

            // second positive fence: work on an unrelated key still runs, so the executor has polled past the blocked
            // task rather than merely not got to it yet
            store.execute(ExecutionContext.contextFor(TxnId.fromValues(1, 2 + waiters, 0, new Id(1)), null, RoutingKeys.of(unrelated),
                                                      LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "unrelated"),
                          (Consumer<? super SafeCommandStore>) ignore -> {},
                          (success, fail) -> { unrelatedFailure.set(fail); unrelatedDone.signal(); });
            unrelatedRan = unrelatedDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            blockedWasNotified = blockedDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        finally
        {
            queuedBehind.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertTrue("the caller must be told the body failure, and was told " + fanOutFailure.get(),
                   fanOutFailure.get() != null && isInjected(fanOutFailure.get(), InjectedBodyFailure.class));
        assertTrue("an unrelated key must still be served: the block is per key, and was " + unrelatedFailure.get(),
                   unrelatedRan && unrelatedFailure.get() == null);
        assertTrue("a task queued on the key before the round failed for it must be told the outstanding update, and "
                   + "not be served: it was told " + blockedReport.get(),
                   blockedReport.get() instanceof InconsistentEntryException);
        assertTrue("its caller must be told, or it waits on something that cannot happen", blockedWasNotified);
        assertTrue("and nothing may reach the agent as an internal error: " + agent.exceptions,
                   agent.exceptions.stream().allMatch(t -> isInjected(t, InjectedBodyFailure.class)));
        assertTrue("the dead fan-out must still hold the entry it marked", Boolean.TRUE.equals(stillClaimed.get()));
    }

    /**
     * More than one round of the same fan-out may fail, and each must be handled on its own: two marked, retained keys
     * held simultaneously by a task that is <em>still running</em>, and the round between them applied and released.
     *
     * <p>This is only reachable because a failed round no longer ends the fan-out, and it is the case that exercises
     * repetition: a second {@code retainClaimExclusive} while the first retained claim is still held, a second entry
     * marked, and the batch accounting surviving two failures ({@code processed} must count both, or
     * {@code processed + failed == keys} is never reached and the task parks for ever holding its TxnId - L2).
     */
    @Test
    public void twoFailedRoundsBothMarkAndTheRestStillApplies() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 13));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = { key(tableId, partitioner, 0), key(tableId, partitioner, 1), key(tableId, partitioner, 2) };

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(AccordFailedKeyTestHarness::setInMemoryFunctions);
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicInteger rounds = new AtomicInteger();
        List<RoutingKey> applied = new CopyOnWriteArrayList<>();
        Set<RoutingKey> failedIn = ConcurrentHashMap.newKeySet();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Condition done = Condition.newOneTimeCondition();
        Set<RoutingKey> inconsistent = ConcurrentHashMap.newKeySet();
        Set<RoutingKey> claimed = ConcurrentHashMap.newKeySet();
        Map<RoutingKey, Integer> references = new ConcurrentHashMap<>();
        AtomicReference<Object> stalledOn = new AtomicReference<>();
        try
        {
            store.execute(fanOut(TxnId.fromValues(1, 1, 0, new Id(1)), LoadKeys.INCR, true, true, keys),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              Unseekables<?> batch = safeStore.context().keys();
                              if (batch.isEmpty())
                                  return;
                              RoutingKey k = (RoutingKey) batch.get(0);
                              // fail the first and the last round, so a success sits between two failures
                              if (rounds.incrementAndGet() != 2)
                              {
                                  failedIn.add(k);
                                  throw new InjectedBodyFailure(k);
                              }
                              applied.add(k);
                          },
                          (success, fail) -> { failure.set(fail); done.signal(); });

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
                    stalledOn.set(anyInconsistentIntersecting(store, null));
                });
                return rounds.get() == keys.length && inconsistent.size() == 2
                       && applied.size() == 1 && Integer.valueOf(0).equals(references.get(applied.get(0)));
            });
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertEquals("every round must have run: two failures must not stop the fan-out. Rounds " + rounds.get()
                     + ", applied " + applied + ", failed in " + failedIn, 3, rounds.get());
        assertEquals("the round between the failures must have applied its key: " + applied, 1, applied.size());
        assertTrue("the caller must be told a body failure, and was told " + failure.get(),
                   failure.get() != null && isInjected(failure.get(), InjectedBodyFailure.class));
        assertEquals("both failed rounds' keys must be marked: " + inconsistent + ", failed in " + failedIn,
                     failedIn, inconsistent);
        assertEquals("and both must be retained, reference and claim, at the same time: claimed " + claimed
                     + ", references " + references, failedIn, claimed);
        for (RoutingKey k : failedIn)
            assertTrue("a marked key must keep its reference: " + references, references.get(k) > 0);
        assertEquals("the applied key must be released: " + references, Integer.valueOf(0), references.get(applied.get(0)));
        assertNotNull("a durability report must be refused while any update is outstanding", stalledOn.get());
    }

    /**
     * A body failure in an update that need not be witnessed marks nothing and retains nothing - the mirror of
     * {@link #updateThatNeedNotBeWitnessedDoesNotPoison}, which covers the load-failure path only.
     *
     * <p>The half that matters is the release. {@code postRunExclusive} takes one of two branches for a failed round,
     * and the {@code mustBeWitnessed()} branch keeps the entry's <em>lock</em> (converted to a retained claim). If the
     * other branch did not give that lock up, the next task to reach {@code prepareExclusive} for the key would fail
     * {@code require(!isLocked())} inside {@code lockExclusive} - an internal error, on a key nothing is wrong with. So
     * this asserts a later operation on the failed round's own key runs normally.
     */
    @Test
    public void bodyFailureThatNeedNotBeWitnessedReleasesEverything() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 14));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = { key(tableId, partitioner, 0), key(tableId, partitioner, 1) };

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(AccordFailedKeyTestHarness::setInMemoryFunctions);
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicInteger rounds = new AtomicInteger();
        AtomicReference<RoutingKey> failedIn = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Condition done = Condition.newOneTimeCondition();
        Set<RoutingKey> inconsistent = ConcurrentHashMap.newKeySet();
        Map<RoutingKey, Integer> references = new ConcurrentHashMap<>();
        Set<RoutingKey> claimed = ConcurrentHashMap.newKeySet();
        AtomicReference<Object> stalledOn = new AtomicReference<>();
        AtomicReference<Throwable> after = new AtomicReference<>();
        Condition afterDone = Condition.newOneTimeCondition();
        try
        {
            store.execute(fanOut(null, LoadKeys.INCR, false, true, keys),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              Unseekables<?> batch = safeStore.context().keys();
                              if (batch.isEmpty())
                                  return;
                              RoutingKey k = (RoutingKey) batch.get(0);
                              if (rounds.incrementAndGet() == 1)
                              {
                                  failedIn.set(k);
                                  throw new InjectedBodyFailure(k);
                              }
                          },
                          (success, fail) -> { failure.set(fail); done.signal(); });

            assertTrue("the caller was never told", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            // the end state: every round run, and everything released
            assertTrue("the fan-out never released its keys",
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
                               stalledOn.set(anyInconsistentIntersecting(store, null));
                           });
                           return rounds.get() == keys.length && claimed.isEmpty()
                                  && Integer.valueOf(0).equals(references.get(failedIn.get()));
                       }));

            // and the key the round failed for is usable: nothing holds its lock, and nothing refuses it
            store.execute(ExecutionContext.contextFor(TxnId.fromValues(1, 2, 0, new Id(1)), null, RoutingKeys.of(failedIn.get()),
                                                      LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "after"),
                          (Consumer<? super SafeCommandStore>) ignore -> {},
                          (success, fail) -> { after.set(fail); afterDone.signal(); });
            assertTrue("the later operation was never notified", afterDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertNotNull("the body never ran, so this test proves nothing", failedIn.get());
        assertTrue("the caller must still be told the failure, and was told " + failure.get(),
                   failure.get() != null && isInjected(failure.get(), InjectedBodyFailure.class));
        assertEquals("an update that need not be witnessed must mark nothing: " + inconsistent,
                     java.util.Collections.emptySet(), inconsistent);
        assertEquals("and retain nothing: claimed " + claimed + ", references " + references,
                     java.util.Collections.emptySet(), claimed);
        assertEquals("the failed round's key must be released: " + references, Integer.valueOf(0), references.get(failedIn.get()));
        assertNull("no durability report may be refused", stalledOn.get());
        assertNull("a later operation on the failed round's key must run: if the round kept its lock, it fails "
                   + "require(!isLocked()) in lockExclusive instead. It was told " + after.get(), after.get());
    }

    /**
     * Drain does not abandon a fan-out that has already begun: it finishes.
     *
     * <p>A fan-out between rounds is unfinished work, not new work - {@code waitToRunExclusive} re-queues it on the
     * {@code ExclusiveExecutor} to continue - so {@code ExclusiveExecutor.reject} admits it once it has published a
     * round, for the same reason it admits a continuation. Refusing it would leave the update partially applied with
     * nobody to complete it, and would mark keys that never took part in a failed execution.
     *
     * <p>That also removes a durability stall from the shutdown path (the audit's (B)): because the fan-out completes,
     * there is nothing to mark, and {@code AccordCommandStore.shutdownAsync}'s {@code ensureDurable} - whose barrier
     * waits for this task, since an INCR task keeps its tranche across rounds - can report the bound legitimately
     * instead of being refused.
     */
    @Test
    public void drainDoesNotAbandonAStartedFanOut() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 6));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = { key(tableId, partitioner, 0), key(tableId, partitioner, 1), key(tableId, partitioner, 2) };

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(AccordFailedKeyTestHarness::setInMemoryFunctions);
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        List<RoutingKey> applied = new CopyOnWriteArrayList<>();
        AtomicInteger rounds = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Condition done = Condition.newOneTimeCondition();
        Set<RoutingKey> poisoned = ConcurrentHashMap.newKeySet();
        AtomicReference<Object> stalledOn = new AtomicReference<>();
        AtomicReference<Boolean> taskDone = new AtomicReference<>(false);
        try
        {
            Object submitted =
            store.execute(fanOut(TxnId.fromValues(1, 1, 0, new Id(1)), LoadKeys.INCR, true, true, keys),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              Unseekables<?> batch = safeStore.context().keys();
                              if (batch.isEmpty())
                                  return;
                              applied.add((RoutingKey) batch.get(0));
                              rounds.incrementAndGet();
                              // we run on the executor thread, holding it, so this stops the store between our rounds
                              if (rounds.get() == 1)
                                  store.exclusiveExecutor().stop();
                          },
                          (success, fail) -> { failure.set(fail); done.signal(); });
            SafeTask<?> task = (SafeTask<?>) submitted;

            assertTrue("the caller was never told", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue("the fan-out never terminated",
                       await(() -> {
                           executor.executeDirectlyWithLock(() -> taskDone.set(task.state().isDone()));
                           return taskDone.get();
                       }));
            await(() -> {
                executor.executeDirectlyWithLock(() -> {
                    poisoned.clear();
                    for (RoutingKey k : keys)
                    {
                        AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(k);
                        if (entry != null && entry.isInconsistent())
                            poisoned.add(k);
                    }
                    stalledOn.set(anyInconsistentIntersecting(store, null));
                });
                return rounds.get() == keys.length;
            });
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertEquals("every key must have been applied: a fan-out already under way is not refused at drain, so it "
                     + "finishes. Applied " + applied, keys.length, applied.size());
        assertNull("and the caller must be told it succeeded, not that it was rejected: " + failure.get(), failure.get());
        assertEquals("nothing may be marked: every key received the update, so there is nothing outstanding to record: "
                     + poisoned, java.util.Collections.emptySet(), poisoned);
        assertNull("and so no durability report is refused - which is the point: at shutdown the bound may advance, "
                   + "rather than being stalled by a fan-out we abandoned ourselves", stalledOn.get());
        assertTrue("no internal error may be reported: " + agent.exceptions, agent.exceptions.isEmpty());
    }

    /**
     * A fan-out that declares <em>no txnId</em> still marks and still blocks, provided it is ATOMIC: the fifo upgrade in
     * {@code SafeTask.prepareExclusiveMayThrow} is {@code holdsLocksBetweenRuns() || isAtomic()}, so declaring ATOMIC is
     * enough on its own, and the claim a failed round retains does not depend on holding a command lock across rounds.
     *
     * <p>That is why sequencing, rather than a flag of its own, is what decides whether a failed update is witnessed: the
     * only fan-out that cannot block is one with neither a txnId nor ATOMIC - it never takes a fifo stamp, and a fresh
     * one would sort <em>behind</em> everything that queued after it - and that shape marks nothing at all
     * ({@link #updateThatNeedNotBeWitnessedDoesNotPoison}, {@link #bodyFailureThatNeedNotBeWitnessedReleasesEverything}).
     * The production path in that shape is {@code Commands.setDurability}'s "Set Durable" fan-out.
     */
    @Test
    public void fanOutWithNoTxnIdStillMarksAndBlocks() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 12));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = { key(tableId, partitioner, 0), key(tableId, partitioner, 1) };

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(AccordFailedKeyTestHarness::setInMemoryFunctions);
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicInteger rounds = new AtomicInteger();
        List<RoutingKey> applied = new CopyOnWriteArrayList<>();
        AtomicReference<RoutingKey> failedIn = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Condition done = Condition.newOneTimeCondition();
        Set<RoutingKey> inconsistent = ConcurrentHashMap.newKeySet();
        Map<RoutingKey, Integer> references = new ConcurrentHashMap<>();
        AtomicReference<Boolean> unclaimed = new AtomicReference<>();
        AtomicReference<Object> stalledOn = new AtomicReference<>();
        AtomicReference<Throwable> after = new AtomicReference<>();
        Condition afterDone = Condition.newOneTimeCondition();
        try
        {
            store.execute(fanOut(null, LoadKeys.INCR, true, true, keys),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              Unseekables<?> batch = safeStore.context().keys();
                              if (batch.isEmpty())
                                  return;
                              RoutingKey k = (RoutingKey) batch.get(0);
                              if (rounds.incrementAndGet() == 1)
                              {
                                  failedIn.set(k);
                                  throw new InjectedBodyFailure(k);
                              }
                              applied.add(k);
                          },
                          (success, fail) -> { failure.set(fail); done.signal(); });
            assertTrue("the caller was never told", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            await(() -> {
                executor.executeDirectlyWithLock(() -> {
                    inconsistent.clear();
                    references.clear();
                    for (RoutingKey k : keys)
                    {
                        AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(k);
                        if (entry == null)
                            continue;
                        if (entry.isInconsistent())
                            inconsistent.add(k);
                        references.put(k, entry.references());
                    }
                    AccordCacheEntry<?, ?, ?> failed = failedIn.get() == null ? null : store.cachesUnsafe().commandsForKeys().getUnsafe(failedIn.get());
                    unclaimed.set(failed == null ? null : failed.hasNoTasks());
                    stalledOn.set(anyInconsistentIntersecting(store, null));
                });
                return rounds.get() == keys.length && inconsistent.size() == 1
                       && applied.size() == 1 && Integer.valueOf(0).equals(references.get(applied.get(0)));
            });

            store.execute(ExecutionContext.contextFor(TxnId.fromValues(1, 2, 0, new Id(1)), null, RoutingKeys.of(failedIn.get()),
                                                      LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "after"),
                          (Consumer<? super SafeCommandStore>) ignore -> {},
                          (success, fail) -> { after.set(fail); afterDone.signal(); });
            assertTrue("the later operation was never notified", afterDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertNotNull("the body never ran, so this test proves nothing", failedIn.get());
        assertTrue("the caller must be told the body failure, and was told " + failure.get(),
                   failure.get() != null && isInjected(failure.get(), InjectedBodyFailure.class));
        assertEquals("a fan-out with no txnId must still carry on past a failed round: rounds " + rounds.get()
                     + ", applied " + applied, 2, rounds.get());
        assertEquals("the failed round's key must still be marked", java.util.Collections.singleton(failedIn.get()), inconsistent);
        assertTrue("the reference must still be retained, or the entry - and the bit - can be evicted: " + references,
                   references.get(failedIn.get()) != null && references.get(failedIn.get()) > 0);
        assertNotNull("a durability report must still be refused", stalledOn.get());
        // and, unlike a fan-out with neither a txnId nor ATOMIC, it does hold a fifo claim to keep: ATOMIC alone upgrades
        // it, so the failed round retains the claim that blocks whatever queues behind it
        assertEquals("an ATOMIC fan-out must retain its claim even with no txnId, or nothing blocks on the key the update "
                     + "did not reach", Boolean.FALSE, unclaimed.get());
        assertTrue("an operation arriving after the marking is still refused, and was told " + after.get(),
                   after.get() instanceof InconsistentEntryException);
    }

    /**
     * The one case a rejection can still reach a fan-out - refused <em>before</em> its first round, i.e. the store was
     * already stopping when it arrived - and it is an ordinary task failure. It published nothing, so there is no partial
     * application to record: everything is released, the caller is told, and <em>nothing is marked</em>.
     *
     * <p>That is deliberate, and it is why marking belongs to partial application only. Marking the batch it happened to
     * have claimed would plug one arbitrary instance of a general hole rather than the hole: any task that would have
     * propagated an applied command's derived state and fails before publishing leaves no record - a SYNC task running
     * the same update inline included, see {@link #syncFailureMarksNothingEither} - and {@code ensureDurable} is exposed
     * to all of them equally. The fix is R2's obligation bound, not a mark here.
     *
     * <p>What {@code RunState.RUN_REJECTED} is still for is the other half: a first-round <em>body</em> failure must
     * carry on ({@link #firstRoundFailureMarksItsRound}) and a refusal must not, or it would offer each remaining batch
     * in turn and have each refused.
     */
    @Test
    public void fanOutRefusedBeforeItStartsFailsLikeAnyOtherTask() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 15));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = { key(tableId, partitioner, 0), key(tableId, partitioner, 1), key(tableId, partitioner, 2) };

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(AccordFailedKeyTestHarness::setInMemoryFunctions);
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicInteger rounds = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Condition stopped = Condition.newOneTimeCondition();
        Condition done = Condition.newOneTimeCondition();
        Set<RoutingKey> marked = ConcurrentHashMap.newKeySet();
        Set<RoutingKey> claimed = ConcurrentHashMap.newKeySet();
        Map<RoutingKey, Integer> references = new ConcurrentHashMap<>();
        AtomicReference<Object> stalledOn = new AtomicReference<>();
        AtomicReference<Boolean> taskDone = new AtomicReference<>(false);
        try
        {
            // stop the store from inside an unrelated task, so that the fan-out below is refused before it ever runs
            store.execute(ExecutionContext.contextFor(TxnId.fromValues(1, 1, 0, new Id(1)), null, RoutingKeys.of(keys[0]),
                                                      LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "stopper"),
                          (Consumer<? super SafeCommandStore>) ignore -> store.exclusiveExecutor().stop(),
                          (success, fail) -> stopped.signal());
            assertTrue("the store was never stopped", stopped.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            Object submitted =
            store.execute(fanOut(TxnId.fromValues(1, 2, 0, new Id(1)), LoadKeys.INCR, true, true, keys),
                          (Consumer<? super SafeCommandStore>) ignore -> rounds.incrementAndGet(),
                          (success, fail) -> { failure.set(fail); done.signal(); });
            SafeTask<?> task = (SafeTask<?>) submitted;

            assertTrue("the caller was never told", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue("the fan-out never terminated",
                       await(() -> {
                           executor.executeDirectlyWithLock(() -> taskDone.set(task.state().isDone()));
                           return taskDone.get();
                       }));
            executor.executeDirectlyWithLock(() -> {
                for (RoutingKey k : keys)
                {
                    AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(k);
                    if (entry == null)
                        continue;
                    if (entry.isInconsistent())
                        marked.add(k);
                    if (!entry.hasNoTasks())
                        claimed.add(k);
                    references.put(k, entry.references());
                }
                stalledOn.set(anyInconsistentIntersecting(store, null));
            });
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertEquals("the body must never have run", 0, rounds.get());
        assertTrue("the caller must be told the rejection, and was told " + failure.get(),
                   failure.get() instanceof java.util.concurrent.RejectedExecutionException);
        assertEquals("nothing may be marked: nothing was applied, so there is no partial application to record and no "
                     + "reason to treat this differently from any other failing task: " + marked,
                     java.util.Collections.emptySet(), marked);
        assertEquals("and nothing retained - every claim released, as an ordinary failure releases them: " + claimed
                     + ", references " + references, java.util.Collections.emptySet(), claimed);
        assertNull("so no durability report is refused. The bound advancing over an update that never began is the "
                   + "general hole (R2), which a mark here would only paper over for one shape of task", stalledOn.get());
        assertTrue("no internal error may be reported: " + agent.exceptions,
                   agent.exceptions.stream().allMatch(t -> t instanceof java.util.concurrent.RejectedExecutionException));
    }

    /**
     * The general hole, in executable form: a <em>SYNC</em> task that fails marks nothing, retains nothing, and stalls no
     * durability report - even though its body may have been about to propagate an applied command's derived state to
     * exactly the same keys (the inline half of {@code SafeCommandStore.updateManagedCommandsForKey} runs in whatever
     * task is executing, which is frequently a SYNC apply).
     *
     * <p>This is not a bug being pinned, it is the boundary of what marking is for. Marking exists for <em>partial</em>
     * application - some keys of an update reached, others not, so the ones that missed it must not be read or counted
     * durable - and every path that marks is therefore non-sync ({@code SafeTask.onFailedToLoadExclusive},
     * {@code NonSyncState.postRunExclusive}). A task that publishes nothing leaves no partial state, and is left to the
     * ordinary failure path, which is why {@link #fanOutRefusedBeforeItStartsFailsLikeAnyOtherTask} marks nothing either.
     *
     * <p>What remains uncovered by <em>both</em> is that the command may already be counted LOCALLY_APPLIED while its
     * derived state was never propagated, so {@code ensureDurable} may report a bound over it and
     * {@code AbstractReplayer.minReplay} will then skip it. That is R2 - {@code bound = min(LOCALLY_APPLIED, oldest
     * outstanding obligation)} - and it is a follow-up, not something a mark in one arm can fix.
     */
    @Test
    public void syncFailureMarksNothingEither() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 16));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey ok = key(tableId, partitioner, 0);
        RoutingKey failing = key(tableId, partitioner, 1);

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        AtomicInteger loadAttempts = new AtomicInteger();
        executor.cacheUnsafe().types().forEach(type -> setLoadFunction(type, failing, loadAttempts));
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicInteger rounds = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Condition done = Condition.newOneTimeCondition();
        Set<RoutingKey> marked = ConcurrentHashMap.newKeySet();
        AtomicReference<Object> stalledOn = new AtomicReference<>();
        AtomicReference<Throwable> after = new AtomicReference<>();
        Condition afterDone = Condition.newOneTimeCondition();
        try
        {
            // a SYNC task over both keys: its keys are required, so the failing load fails the task outright
            store.execute(ExecutionContext.contextFor(TxnId.fromValues(1, 1, 0, new Id(1)), null, RoutingKeys.of(ok, failing),
                                                      LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "sync"),
                          (Consumer<? super SafeCommandStore>) ignore -> rounds.incrementAndGet(),
                          (success, fail) -> { failure.set(fail); done.signal(); });
            assertTrue("the caller was never told", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // fence on a later task completing, so this reads state after the failed task released
            store.execute(ExecutionContext.contextFor(TxnId.fromValues(1, 2, 0, new Id(1)), null, RoutingKeys.of(ok),
                                                      LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "after"),
                          (Consumer<? super SafeCommandStore>) ignore -> {},
                          (success, fail) -> { after.set(fail); afterDone.signal(); });
            assertTrue("the later operation was never notified", afterDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            executor.executeDirectlyWithLock(() -> {
                for (RoutingKey k : new RoutingKey[]{ ok, failing })
                {
                    AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(k);
                    if (entry != null && entry.isInconsistent())
                        marked.add(k);
                }
                stalledOn.set(anyInconsistentIntersecting(store, null));
            });
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertEquals("the body must never have run: a SYNC task's keys are required", 0, rounds.get());
        assertTrue("the caller must be told the load failure, and was told " + failure.get(),
                   failure.get() != null && isInjected(failure.get()));
        assertEquals("a SYNC task marks nothing, however much its body was about to propagate: " + marked,
                     java.util.Collections.emptySet(), marked);
        assertNull("and stalls no durability report - the same exposure as any other task that publishes nothing",
                   stalledOn.get());
        assertNull("an operation on the other key must run normally", after.get());
    }

    /**
     * A task blocked behind an update that failed must not hold up the tranche barrier - which is what
     * {@code afterSubmittedAndConsequences}, and so every durability report on this executor, waits on.
     *
     * <p>Retaining the claim is what preserves the order a retry has to re-run in, but a retained claim also means the
     * task behind it never completes, and a task that never completes never releases its tranche: {@code tranches.complete}
     * is reached only from {@code completedTaskExclusive}, gated on {@code compareTo(EXECUTED) >= 0}, and
     * {@code Tranches.complete} advances strictly in order - so <em>one</em> blocked task stalls every later tranche on
     * the executor, not merely the ranges it touches. Before the fix both barriers below stayed silent, including one
     * registered after every runnable task had drained, and {@code waitForQuiescence} could never return either.
     *
     * <p>So a blocked task is counted out of the accounting and its caller is told, while the fan-out keeps the claim on
     * the key it marked: the caller of the blocked task is told {@code InconsistentEntryException} and the task itself is
     * failed and released like any other task that will not run - only a continuation or a fan-out mid-flight keeps a
     * claim it can no longer be served on ({@code AccordCacheEntryQueue.requireNotFailed} permits nothing else).
     */
    @Test
    public void blockedTaskDoesNotStallTheTrancheBarrier() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 17));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = { key(tableId, partitioner, 0), key(tableId, partitioner, 1) };
        RoutingKey unrelated = key(tableId, partitioner, 2);

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(AccordFailedKeyTestHarness::setInMemoryFunctions);
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicInteger rounds = new AtomicInteger();
        AtomicReference<RoutingKey> failingIn = new AtomicReference<>();
        Condition queuedBehind = Condition.newOneTimeCondition();
        Condition fanOutDone = Condition.newOneTimeCondition();
        Condition blockedDone = Condition.newOneTimeCondition();
        Condition unrelatedDone = Condition.newOneTimeCondition();
        Condition barrierBefore = Condition.newOneTimeCondition();
        Condition barrierAfter = Condition.newOneTimeCondition();
        AtomicReference<Boolean> stillClaimed = new AtomicReference<>();
        boolean firedBefore = false, firedAfter = false;
        try
        {
            store.execute(fanOut(TxnId.fromValues(1, 1, 0, new Id(1)), LoadKeys.INCR, true, true, keys),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              Unseekables<?> batch = safeStore.context().keys();
                              if (batch.isEmpty())
                                  return;
                              RoutingKey k = (RoutingKey) batch.get(0);
                              if (rounds.incrementAndGet() != 2)
                                  return;
                              failingIn.set(k);
                              queuedBehind.awaitUninterruptibly();
                              throw new InjectedBodyFailure(k);
                          },
                          (success, fail) -> fanOutDone.signal());

            assertTrue("the second round never ran", await(() -> failingIn.get() != null));
            RoutingKey blocked = failingIn.get();
            store.execute(ExecutionContext.contextFor(TxnId.fromValues(1, 2, 0, new Id(1)), null, RoutingKeys.of(blocked),
                                                      LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "behind"),
                          (Consumer<? super SafeCommandStore>) ignore -> {},
                          (success, fail) -> blockedDone.signal());
            assertTrue("the task behind the round never queued on " + blocked,
                       await(() -> {
                           AtomicInteger references = new AtomicInteger();
                           executor.executeDirectlyWithLock(() -> {
                               AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(blocked);
                               references.set(entry == null ? 0 : entry.references());
                           });
                           return references.get() >= 2;
                       }));

            // a barrier registered while the blocked task is outstanding ...
            executor.afterSubmittedAndConsequences(barrierBefore::signal);
            queuedBehind.signal();
            assertTrue("the fan-out never completed", fanOutDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue("the blocked task's caller was never told", blockedDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // ... and one registered after all runnable work has drained
            store.execute(ExecutionContext.contextFor(TxnId.fromValues(1, 3, 0, new Id(1)), null, RoutingKeys.of(unrelated),
                                                      LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "unrelated"),
                          (Consumer<? super SafeCommandStore>) ignore -> {},
                          (success, fail) -> unrelatedDone.signal());
            assertTrue("the unrelated task never ran", unrelatedDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            executor.afterSubmittedAndConsequences(barrierAfter::signal);

            firedBefore = barrierBefore.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            firedAfter = barrierAfter.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            executor.executeDirectlyWithLock(() -> {
                AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(blocked);
                stillClaimed.set(entry != null && entry.isInconsistent() && !entry.hasNoTasks() && entry.references() > 0);
            });
        }
        finally
        {
            queuedBehind.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertTrue("a barrier registered while a blocked task was outstanding never fired: the blocked task is holding "
                   + "its tranche, so every later durability report on this executor is stalled behind it", firedBefore);
        assertTrue("nor did one registered after every runnable task had drained", firedAfter);
        assertTrue("the failed fan-out must still hold its claim - that is the order a retry has to preserve - and the "
                   + "entry must still be marked and referenced", Boolean.TRUE.equals(stillClaimed.get()));
        assertTrue("nothing may reach the agent as an internal error: " + agent.exceptions,
                   agent.exceptions.stream().allMatch(t -> isInjected(t, InjectedBodyFailure.class)));
    }

    /**
     * An incremental fan-out journals each round's command diff as it goes, rather than accumulating the whole diff for a
     * final round that may never come.
     *
     * <p>Why it matters: a round's mutations become visible when its batch is released, and the notifications derived from
     * them are submitted as consequences of that round, so a fan-out that journalled only at the end could lose a command
     * mutation whose derived commands-for-key state had already been saved - the divergence would be unrecoverable,
     * because commands are journal-only ({@code CommandAdapter.save} returns null).
     *
     * <p>Two assertions, and they pin the two halves against each other:
     * <ul>
     *   <li>a record has been offered to the journal <em>before the fan-out finishes</em> (read from inside round two,
     *       which the body holds open): that is journalling as we go;</li>
     *   <li>exactly one record is offered over the whole fan-out, though there are three rounds: that is the re-base -
     *       {@code SaferCommand.onJournalled} makes each round diff against what the log already has, so a round that
     *       changes no command offers nothing.</li>
     * </ul>
     */
    @Test
    public void roundsJournalTheirOwnCommandUpdates() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 19));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = { key(tableId, partitioner, 0), key(tableId, partitioner, 1), key(tableId, partitioner, 2) };
        TxnId txnId = TxnId.fromValues(1, 1, 0, new Id(1));

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(AccordFailedKeyTestHarness::setInMemoryFunctions);
        AtomicInteger journalled = new AtomicInteger();
        AccordCommandStore store = AccordFailedKeyTestHarness.commandStore(tableId, partitioner, executor, agent,
                                                                          recordingJournal(txnId, journalled));
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicInteger rounds = new AtomicInteger();
        AtomicInteger emptyRounds = new AtomicInteger();
        AtomicInteger journalledBeforeRoundTwo = new AtomicInteger(-1);
        Condition inRoundTwo = Condition.newOneTimeCondition();
        Condition finishRoundTwo = Condition.newOneTimeCondition();
        Condition done = Condition.newOneTimeCondition();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try
        {
            store.execute(fanOut(txnId, LoadKeys.INCR, true, true, keys),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              if (safeStore.context().keys().isEmpty())
                              {
                                  emptyRounds.incrementAndGet();
                                  return;
                              }
                              if (rounds.incrementAndGet() == 2)
                              {
                                  // round one has completed, so whatever it journalled has been offered by now
                                  journalledBeforeRoundTwo.set(journalled.get());
                                  inRoundTwo.signal();
                                  finishRoundTwo.awaitUninterruptibly(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                              }
                          },
                          (success, fail) -> { failure.set(fail); done.signal(); });

            assertTrue("the second round never ran", inRoundTwo.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            finishRoundTwo.signal();
            assertTrue("the fan-out never completed", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
        finally
        {
            finishRoundTwo.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertNull("the fan-out must have succeeded, and was told " + failure.get(), failure.get());
        assertEquals("every key must have been processed, one per round", keys.length, rounds.get());
        assertTrue("the command must be journalled as the fan-out goes, not only when it finishes: nothing had been "
                   + "offered to the journal by the second round", journalledBeforeRoundTwo.get() >= 1);
        assertEquals("and only what changed: the command changes once, so one round offers a record and the others - "
                     + "diffing against what the log already has - offer nothing", 1, journalled.get());
    }

    /** an in-memory journal that counts what is offered for {@code txnId}, including the offers it turns into no-ops */
    private static accord.api.Journal recordingJournal(TxnId txnId, AtomicInteger counter)
    {
        return new accord.impl.basic.InMemoryJournal(new Id(1), new accord.utils.DefaultRandom(1))
        {
            @Override
            public void saveCommand(int commandStoreId, accord.api.Journal.CommandUpdate update, Runnable onFlush)
            {
                if (txnId.equals(update.txnId))
                    counter.incrementAndGet();
                super.saveCommand(commandStoreId, update, onFlush);
            }
        };
    }

    /**
     * The report to the caller waits for <em>every</em> round's append, not just the last one's.
     *
     * <p>Now that a fan-out journals per round, the final round is often the round with nothing left to write - the command
     * changed once, early - so gating the report on "this round's flush" would tell the caller "done" while an earlier
     * round's record was still in flight, and a caller may take externally visible action on the strength of it. The
     * counter in {@code SafeTask.onAppendPersisted} is what closes that: the last append to land reports.
     *
     * <p>The journal here holds every flush callback until the test releases it, so "still in flight" is a state we can
     * observe: the fan-out runs all its rounds and terminates, and the caller must still not have been told.
     */
    @Test
    public void callbackWaitsForEveryRoundsAppend() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 20));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = { key(tableId, partitioner, 0), key(tableId, partitioner, 1), key(tableId, partitioner, 2) };
        TxnId txnId = TxnId.fromValues(1, 1, 0, new Id(1));

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(AccordFailedKeyTestHarness::setInMemoryFunctions);
        List<Runnable> heldFlushes = new CopyOnWriteArrayList<>();
        AccordCommandStore store = AccordFailedKeyTestHarness.commandStore(tableId, partitioner, executor, agent,
                                                                          journalHoldingFlushes(heldFlushes));
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicInteger rounds = new AtomicInteger();
        AtomicInteger reports = new AtomicInteger();
        Condition done = Condition.newOneTimeCondition();
        AtomicReference<Boolean> taskDone = new AtomicReference<>(false);
        boolean reportedWhileInFlight = true, reportedAfterFlush = false;
        try
        {
            Object submitted =
            store.execute(fanOut(txnId, LoadKeys.INCR, true, true, keys),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              if (!safeStore.context().keys().isEmpty())
                                  rounds.incrementAndGet();
                          },
                          (success, fail) -> { reports.incrementAndGet(); done.signal(); });
            SafeTask<?> task = (SafeTask<?>) submitted;

            // every round runs and the task terminates, while its record is still in the journal's hands
            assertTrue("the fan-out never terminated",
                       await(() -> {
                           executor.executeDirectlyWithLock(() -> taskDone.set(task.state().isDone()));
                           return taskDone.get() && rounds.get() == keys.length;
                       }));
            reportedWhileInFlight = done.await(1, TimeUnit.SECONDS);

            assertTrue("the journal was never asked to write anything, so this test proves nothing", !heldFlushes.isEmpty());
            heldFlushes.forEach(Runnable::run);
            reportedAfterFlush = done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        finally
        {
            heldFlushes.forEach(Runnable::run);
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertTrue("the caller must not be told while an append is still in flight - the final round had nothing of its own "
                   + "to write, so only the counter can hold the report back", !reportedWhileInFlight);
        assertTrue("and must be told once every append is durable", reportedAfterFlush);
        assertEquals("exactly once", 1, reports.get());
        assertTrue("no internal error may be reported: " + agent.exceptions, agent.exceptions.isEmpty());
    }

    /** an in-memory journal that hands every flush callback to {@code held} instead of running it */
    private static accord.api.Journal journalHoldingFlushes(List<Runnable> held)
    {
        return new accord.impl.basic.InMemoryJournal(new Id(1), new accord.utils.DefaultRandom(1))
        {
            @Override
            public void saveCommand(int commandStoreId, accord.api.Journal.CommandUpdate update, Runnable onFlush)
            {
                super.saveCommand(commandStoreId, update, null);
                if (onFlush != null)
                    held.add(onFlush);
            }
        };
    }

    /**
     * The last key a fan-out was waiting on fails, so it has nothing new to run - and it must still <em>complete</em>,
     * releasing its tranche and the {@code HOLD_QUEUE} lock on its txnId (L2: a started INCR task releases it in finitely
     * many rounds).
     *
     * <p>Nothing special is done for this case, and that is the point. {@code isLoaded}/{@code isWaitReady} measure what
     * is left as {@code keys - (processed + failed)}, so with nothing left they are trivially satisfied: the fan-out is
     * re-queued, takes an <em>empty</em> batch, and {@code NonSyncState.prepareExclusive} then sets INCREMENTAL_FINISHING
     * because {@code processed + failed == keys}, so it finishes through the ordinary path. Taking it out of the
     * executor's accounting here as well ({@code deregisterBlockedExclusive}, which is only sound for a task that never
     * completes) would double-count it: {@code completedTaskExclusive} unregisters it again, and
     * {@code Tranches.complete} then either trips its own count assertion or fires a later barrier early.
     *
     * <p>The three assertions are the accounting (a barrier registered afterwards fires), the lock (a later task that
     * declares the same txnId runs), and the agent (a double-unregister is reported there, from the loop thread).
     */
    @Test
    public void lastOutstandingKeyFailsAndTheFanOutStillCompletes() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 18));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey ok = key(tableId, partitioner, 0);
        RoutingKey failing = key(tableId, partitioner, 1);
        RoutingKey unrelated = key(tableId, partitioner, 2);
        TxnId txnId = TxnId.fromValues(1, 1, 0, new Id(1));

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        AtomicInteger loadAttempts = new AtomicInteger();
        // the failing key is the *last* one outstanding, and the test decides when it fails, so that the failure lands
        // after a barrier has been registered - i.e. while the fan-out is the only task counted in its own tranche
        Condition releaseFailure = Condition.newOneTimeCondition();
        executor.cacheUnsafe().types().forEach(type -> setLoadFunction(type, failing, loadAttempts, releaseFailure));
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicInteger rounds = new AtomicInteger();
        AtomicInteger emptyRounds = new AtomicInteger();
        Condition hasRun = Condition.newOneTimeCondition();
        Condition finishRound = Condition.newOneTimeCondition();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Condition reported = Condition.newOneTimeCondition();
        Condition barrierBefore = Condition.newOneTimeCondition();
        Condition barrierAfter = Condition.newOneTimeCondition();
        Condition sameTxnDone = Condition.newOneTimeCondition();
        AtomicReference<Throwable> sameTxn = new AtomicReference<>();
        AtomicReference<Boolean> taskDone = new AtomicReference<>(false);
        boolean firedBefore = false, firedAfter = false, sameTxnRan = false;
        try
        {
            Object submitted =
            store.execute(fanOut(txnId, LoadKeys.INCR, true, true, ok, failing),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              if (safeStore.context().keys().isEmpty())
                              {
                                  emptyRounds.incrementAndGet();
                                  return;
                              }
                              rounds.incrementAndGet();
                              hasRun.signal();
                              // hold the round open, so the failure below lands while it is in flight: that is the state
                              // in which neither wait arm of onFailingKeyExclusive applies
                              finishRound.awaitUninterruptibly(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                          },
                          (success, fail) -> { failure.set(fail); reported.signal(); });
            SafeTask<?> task = (SafeTask<?>) submitted;

            assertTrue("the fan-out never ran its reachable key", hasRun.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            // close the tranche while the fan-out is still outstanding in it, so its count is exactly one: an extra
            // unregister then advances the tranche early, and the real completion finds it no longer tracked
            executor.afterSubmittedAndConsequences(barrierBefore::signal);
            releaseFailure.signal();

            assertTrue("the caller was never told", reported.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            finishRound.signal();
            // the report is made at the drop, while the fan-out is still live, so poll for termination
            assertTrue("the fan-out never terminated: with no key left it must take an empty final batch and finish, or it "
                       + "parks for ever holding its txnId",
                       await(() -> {
                           executor.executeDirectlyWithLock(() -> taskDone.set(task.state().isDone()));
                           return taskDone.get();
                       }));

            // the accounting: the barrier that was waiting on it, and one registered afterwards, must both fire
            firedBefore = barrierBefore.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            executor.afterSubmittedAndConsequences(barrierAfter::signal);
            firedAfter = barrierAfter.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // the lock: a later task declaring the same txnId can only run once the fan-out has released HOLD_QUEUE
            store.execute(ExecutionContext.contextFor(txnId, null, RoutingKeys.of(unrelated),
                                                      LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "same txn"),
                          (Consumer<? super SafeCommandStore>) ignore -> {},
                          (success, fail) -> { sameTxn.set(fail); sameTxnDone.signal(); });
            sameTxnRan = sameTxnDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        finally
        {
            releaseFailure.signal();
            finishRound.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertEquals("exactly one key was reachable, so exactly one non-empty round may have run", 1, rounds.get());
        // and the round that had nothing left to offer was completed in prepare, so the body never saw an empty key set
        assertEquals("the body must not be run with an empty key set: a round that locks no keys is completed in prepare",
                     0, emptyRounds.get());
        assertTrue("the caller must be told the load failure, and was told " + failure.get(),
                   failure.get() != null && isInjected(failure.get()));
        assertTrue("the barrier the fan-out was outstanding in never fired", firedBefore);
        assertTrue("a barrier registered after the fan-out finished never fired: it is still counted outstanding, so every "
                   + "later durability report on this executor is stalled behind it", firedAfter);
        assertTrue("a later task declaring the same txnId never ran: the fan-out did not release its HOLD_QUEUE lock (L2), "
                   + "and was told " + sameTxn.get(), sameTxnRan && sameTxn.get() == null);
        assertTrue("no internal error may be reported - a task counted out of the accounting and then completed is "
                   + "unregistered twice, which Tranches reports from the loop thread: " + agent.exceptions,
                   agent.exceptions.stream().allMatch(AccordFailedKeyTest::isInjected));
    }

    /** as {@link #setLoadFunction}, plus a key whose load blocks until {@code release} is signalled */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void setLoadFunction(AccordCache.Type type, RoutingKey failing, RoutingKey blocked, Condition release)
    {
        type.unsafeSetLoadFunction((java.util.function.BiFunction<AccordCommandStore, Object, Object>) (ignoreStore, k) -> {
            if (failing.equals(k))
                throw new InjectedLoadFailure(k);
            if (blocked.equals(k))
                release.awaitUninterruptibly();
            return k instanceof RoutingKey ? new CommandsForKey((RoutingKey) k) : null;
        });
        type.unsafeSetSaveFunction((accord.utils.QuadFunction<AccordCommandStore, Object, Object, Object, Runnable>) (ignoreStore, k, v, identity) -> () -> {});
    }

    /**
     * A started fan-out failed from outside a run must still complete cleanly: tell its caller, give up its txnId's
     * HOLD_QUEUE lock, release every key it reached, mark and retain the one it did not - and report no internal error.
     *
     * <p>The route is {@code ExclusiveExecutor.ExclusiveExecutorTask.prepareTask}'s catch, which sets FAILED and
     * completes a task whose {@code prepareExclusiveMayThrow} threw; nothing else reaches it, as
     * {@code tryFailAndCompleteUnexecutedExclusive} excludes a started task. So this drives the same seam directly,
     * {@code failAndCompleteExclusive} on a fan-out parked between rounds. What makes it worth asserting is the run
     * state it is parked with: RUN_INCOMPLETE, which {@code completeState} refuses, so a completion that does not record
     * the failure first reports {@code UnhandledEnum: Invalid RunState} to the agent and <em>masks the real failure</em>.
     *
     * <p>The park is made deterministic by a key whose load never lands: with one key per batch the fan-out processes
     * every reachable key, then parks in WAITING_ON_KEY with nothing ready. So the key it did not reach is exactly that
     * one, and it must be marked and retained like any other unreached key. The ATOMIC fan-out that leaves a
     * <em>loaded</em> key unreached needs a batch threshold this class cannot have and lives in
     * {@code AccordFailedKeyAbandonTest}; the fan-out that is not witnessed at all - neither txnId nor ATOMIC, so nothing
     * is marked - is {@code TaskLifecycleTest.startedIncrementalTaskFailedOutsideARoundReleasesResources}.
     */
    @Test
    public void startedFanOutFailedOutsideARoundMarksTheKeyItDidNotReach() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 21));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = { key(tableId, partitioner, 0), key(tableId, partitioner, 1), key(tableId, partitioner, 2) };
        RoutingKey slow = keys[1];
        TxnId txnId = TxnId.fromValues(1, 1, 0, new Id(1));
        RuntimeException injected = new RuntimeException("injected prepare failure");

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        // the fan-out cannot start another round while this key is outstanding, so it parks - and stays parked
        Condition releaseLoad = Condition.newOneTimeCondition();
        executor.cacheUnsafe().types().forEach(type -> setLoadFunction(type, slow, releaseLoad));
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        Set<RoutingKey> processed = ConcurrentHashMap.newKeySet();
        Set<RoutingKey> poisoned = ConcurrentHashMap.newKeySet();
        Set<RoutingKey> stillClaimed = ConcurrentHashMap.newKeySet();
        // the only key the fan-out cannot reach, so the only one it may mark and retain
        Set<RoutingKey> unreached = java.util.Collections.singleton(slow);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> failedWhileParked = new AtomicReference<>(false);
        AtomicReference<Boolean> commandMarked = new AtomicReference<>(false);
        AtomicReference<Integer> heldRefs = new AtomicReference<>();
        AtomicReference<Throwable> sameTxn = new AtomicReference<>();
        Condition done = Condition.newOneTimeCondition();
        Condition sameTxnDone = Condition.newOneTimeCondition();
        boolean released = false, sameTxnRan = false;
        try
        {
            Object submitted =
            store.execute(fanOut(txnId, LoadKeys.INCR, true, true, keys),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              Unseekables<?> batch = safeStore.context().keys();
                              for (int i = 0 ; i < batch.size() ; ++i)
                                  processed.add((RoutingKey) batch.get(i));
                          },
                          (success, fail) -> { failure.set(fail); done.signal(); });
            SafeTask<?> task = (SafeTask<?>) submitted;

            // as prepareTask's catch does: fail and complete a task that has run a round and is parked between rounds
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (!failedWhileParked.get() && failure.get() == null && System.nanoTime() < deadline)
            {
                executor.executeDirectlyWithLock(() -> {
                    if (failedWhileParked.get() || !task.hasIncrementalStarted() || !task.isState(Task.State.WAITING))
                        return;
                    heldRefs.set(task.refs == null ? 0 : task.refs.size());
                    failedWhileParked.set(true);
                    task.failAndCompleteExclusive(injected, Task.State.FAILED);
                });
            }

            if (failedWhileParked.get() && done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
            {
                // poll: the report reaches the callback before the task releases anything
                released = await(() -> {
                    executor.executeDirectlyWithLock(() -> {
                        poisoned.clear();
                        stillClaimed.clear();
                        for (RoutingKey key : keys)
                        {
                            AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(key);
                            if (entry == null)
                                continue;
                            if (entry.isInconsistent())
                                poisoned.add(key);
                            if (!entry.hasNoTasks() || entry.references() > 0)
                                stillClaimed.add(key);
                        }
                        AccordCacheEntry<?, ?, ?> command = store.cachesUnsafe().commands().getUnsafe(txnId);
                        commandMarked.set(command != null && command.isInconsistent());
                    });
                    return poisoned.equals(unreached) && stillClaimed.equals(unreached) && commandMarked.get();
                });

                // the txnId is marked too, so a later task on that command must be told promptly rather than queue
                // behind a HOLD_QUEUE lock nothing will release
                store.execute(ExecutionContext.contextFor(txnId, null, RoutingKeys.of(key(tableId, partitioner, 100)),
                                                          LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "same txn"),
                              (Consumer<? super SafeCommandStore>) ignore -> {},
                              (success, fail) -> { sameTxn.set(fail); sameTxnDone.signal(); });
                sameTxnRan = sameTxnDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        }
        finally
        {
            releaseLoad.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertTrue("the fan-out was never observed parked between rounds, so this test proves nothing", failedWhileParked.get());
        assertTrue("the fan-out must have run at least one round and still held references when it was failed: processed "
                   + processed + ", refs " + heldRefs.get(),
                   !processed.isEmpty() && heldRefs.get() != null && heldRefs.get() > 0);
        assertTrue("the caller must be told the failure, and was told " + failure.get(), failure.get() == injected);
        assertTrue("exactly the key the fan-out did not reach may be marked, and exactly that key may keep a claim or a "
                   + "reference - everything it did reach must be released. Marked " + poisoned + ", still claimed "
                   + stillClaimed + ", expected " + unreached, released);
        assertEquals("the txnId must be marked too: the update it was applying is outstanding, so the command must be "
                     + "treated as unavailable rather than read as though every key had witnessed it",
                     Boolean.TRUE, commandMarked.get());
        assertTrue("a later task declaring the same txnId must be told, promptly, that an update is outstanding: it may "
                   + "not hang behind a HOLD_QUEUE lock the abandoned fan-out will never release (L2). Ran=" + sameTxnRan
                   + ", told " + sameTxn.get(),
                   sameTxnRan && isInjected(sameTxn.get(), InconsistentEntryException.class));
        assertTrue("no internal error may be reported - a failure recorded as RUN_INCOMPLETE is refused by completeState,"
                   + " which masks the real failure. The only report expected is the rejection of the later task we "
                   + "deliberately submitted against the now-marked txnId: " + agent.exceptions,
                   agent.exceptions.stream().allMatch(t -> t instanceof InconsistentEntryException));
    }

    /**
     * A load that fails <em>after</em> the task that wanted the key has gone must not blow up.
     *
     * <p>The shape: a fan-out whose body throws in a later round completes and releases the key that is still loading -
     * it was never marked inconsistent, because nothing failed for it - so when the load finally fails, the entry is
     * unreferenced and no task is waiting on it. That is the path {@code AccordCache.failedToLoad} takes with
     * {@code references() == 0}, and it must evict quietly rather than report an internal error.
     */
    @Test
    public void loadFailsAfterTheTaskHasGone() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 8));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey first = key(tableId, partitioner, 0);
        RoutingKey slow = key(tableId, partitioner, 1);
        RoutingKey second = key(tableId, partitioner, 2);

        AccordFailedKeyTestHarness.RecordingAgent agent = new AccordFailedKeyTestHarness.RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        Condition releaseLoad = Condition.newOneTimeCondition();
        executor.cacheUnsafe().types().forEach(type -> setLoadFunction(type, slow, releaseLoad));
        AccordCommandStore store = commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicInteger rounds = new AtomicInteger();
        Condition done = Condition.newOneTimeCondition();
        Condition afterDone = Condition.newOneTimeCondition();
        AtomicReference<Throwable> after = new AtomicReference<>();
        try
        {
            store.execute(fanOut(TxnId.fromValues(1, 1, 0, new Id(1)), LoadKeys.INCR, true, true, first, slow, second),
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              Unseekables<?> batch = safeStore.context().keys();
                              if (batch.isEmpty())
                                  return;
                              if (rounds.incrementAndGet() == 2)
                                  throw new InjectedBodyFailure(batch.get(0));
                          },
                          (success, fail) -> done.signal());
            assertTrue("the fan-out never completed", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // now let the load of the key nobody is waiting for fail
            releaseLoad.signal();

            // and require the store still works: a later operation on an unrelated key must run
            store.execute(ExecutionContext.contextFor(TxnId.fromValues(1, 2, 0, new Id(1)), null, RoutingKeys.of(first),
                                                      LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "after"),
                          (Consumer<? super SafeCommandStore>) ignore -> {},
                          (success, fail) -> { after.set(fail); afterDone.signal(); });
            assertTrue("the later operation was never notified", afterDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
        finally
        {
            releaseLoad.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertNull("an operation on an applied key must still run", after.get());
        for (Throwable t : agent.exceptions)
        {
            assertTrue("a load that fails once nobody holds the entry must not be reported as an internal error, but "
                       + "the agent was told " + t,
                       isInjected(t) || isInjected(t, InjectedBodyFailure.class)
                       || t instanceof InconsistentEntryException);
        }
    }

    /** as {@link #setLoadFunction(AccordCache.Type, RoutingKey, AtomicInteger)}, but the key blocks and then fails */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void setLoadFunction(AccordCache.Type type, RoutingKey slowAndFailing, Condition release)
    {
        type.unsafeSetLoadFunction((java.util.function.BiFunction<AccordCommandStore, Object, Object>) (ignoreStore, k) -> {
            if (slowAndFailing.equals(k))
            {
                release.awaitUninterruptibly();
                throw new InjectedLoadFailure(k);
            }
            return k instanceof RoutingKey ? new CommandsForKey((RoutingKey) k) : null;
        });
        type.unsafeSetSaveFunction((accord.utils.QuadFunction<AccordCommandStore, Object, Object, Object, Runnable>) (ignoreStore, k, v, identity) -> () -> {});
    }

    private static RoutingKey never(RoutingKey[] keys, RoutingKey applied, RoutingKey failedIn)
    {
        for (RoutingKey k : keys)
        {
            if (!k.equals(applied) && !k.equals(failedIn))
                return k;
        }
        throw new AssertionError("every key was either applied or failed in");
    }

    /**
     * As the real INCR fan-outs declare themselves: re-applied by replay, and - if its effects must be witnessed - able
     * to block on a key it failed to reach. A fan-out can only block on a key it holds a fifo claim over, and it holds
     * one exactly when it is ATOMIC or declares a txnId (which upgrades it on its first run), so "must be witnessed" is
     * expressed by the sequencing rather than by a flag of its own: an update that holds no fifo claim - the
     * {@code unsequencedIdempotentIncrementalWrite} "Set Durable" shape, no txnId and not ATOMIC - marks nothing.
     */
    private static ExecutionContext fanOut(@javax.annotation.Nullable TxnId txnId, LoadKeys loadKeys, boolean mustBeWitnessed, boolean isIdempotent, RoutingKey... keys)
    {
        ExecutionContext wrapped = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(keys), loadKeys,
                                                               LoadKeysFor.READ_WRITE, "fanout");
        return new ExecutionContext.Wrapped()
        {
            @Override public ExecutionSequence executionSequence() { return mustBeWitnessed ? ATOMIC : ExecutionSequence.BY_PRIORITY; }
            @Override public ExecutionContext wrapped() { return wrapped; }
            @Override public boolean isIdempotent() { return isIdempotent; }
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

    private static AccordCommandStore commandStore(TableId tableId, IPartitioner partitioner, AccordExecutor executor, TestAgent agent)
    {
        return org.apache.cassandra.service.accord.AccordFailedKeyTestHarness.commandStore(tableId, partitioner, executor, agent);
    }
}
