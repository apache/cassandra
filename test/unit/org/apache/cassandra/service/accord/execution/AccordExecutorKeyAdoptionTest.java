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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import org.junit.After;
import org.junit.Before;
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
import accord.local.cfk.CommandsForKey;
import accord.local.durability.DurabilityService;
import accord.primitives.Ballot;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.topology.TopologyManager;
import accord.utils.DefaultRandom;
import accord.utils.Invariants;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.ControllableRangeIndex;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.concurrent.Condition;

import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This test has been authored entirely by Claude.
 *
 * The <em>range-domain</em> half of a task's lifecycle, and in particular key <em>adoption</em>: a task that declares
 * {@code Ranges} rather than {@code RoutingKeys} runs a {@code SafeTask.RangeTxnAndKeyScanner}, which registers a
 * {@code KeyWatcher} on the commands-for-key cache for the duration of the scan and, on every {@code onUpdate} for a key
 * inside its ranges, may <em>adopt</em> a reference to that key mid-flight.
 *
 * <p>Adoption is the one place a task's reference set grows after setup, so it is the one place the at-once acquisition
 * premise of the cycle-freedom argument can be broken: a task must hold exactly one queue position for every entry it
 * holds a reference for. Both failure directions are real regressions, and both are pinned by
 * {@link #assertQueuesConsistent}: adopting without taking a position lets the task run with a key it does not lead;
 * adopting and taking the position twice makes it wait for itself and leave a position behind when it completes.
 *
 * <p>The watcher is live from {@code startInternal} until {@code finish}, so adoption is reachable in the states named by
 * {@link Arrival}, one case each. Everything the arrangement needs is under the test's control: which keys the scan
 * discovers ({@link ControllableRangeIndex#discover}), when the scan finishes, which key loads finish when, and whether a
 * prior task holds a key or the txnId. Both production sources of {@code onUpdate} are covered, as they leave the entry
 * in materially different shapes - see {@link Source}.
 */
public class AccordExecutorKeyAdoptionTest
{
    private static final int TIMEOUT_SECONDS = 30;
    /**
     * How long to wait for an <em>arrangement</em> - a state to be reached, an adoption to be observed - as opposed to
     * for real work to finish. Short deliberately: an arrangement that has not happened within a second or two is one
     * the executor is not going to make, and every case waiting out the full timeout exceeds the suite's own.
     */
    private static final int ARRANGE_TIMEOUT_SECONDS = 5;
    /**
     * Key ordinals, in token order. A {@code TokenRange} is {@code (start, end]}, so {@link #LO} and {@link #HI} are
     * outside the range the task declares: the in-range keys are strictly interior, and there is always an out-of-range
     * key for the filter to refuse.
     */
    private static final int LO = 0, IN_FIRST = 1, IN_LAST = 4, HI = 5, KEY_COUNT = 6;
    /** the minimum batch a non-sync task runs with; 2 so that leading one key is not enough (so LOADING_OPTIONAL exists) */
    private static final int MIN_BATCH = 2;

    /**
     * The state the adopting task is in when the notification arrives, limited to those arrangeable deterministically in
     * a real executor; see {@link #everyArrivalStateAdoptsExactlyOnceTest} for those that are not, and why.
     */
    enum Arrival
    {
        /** the scan is still running: the common case, and the only one where the task has no keys at all yet */
        SCANNING_RANGES,
        /** post-scan, a required (sync) load outstanding */
        LOADING_REQUIRED,
        /** post-scan, a batched task short of its minimum batch. The adoption can tip it over - the double-place case */
        LOADING_OPTIONAL,
        /** post-scan, queued behind another task on the primary txnId; INCR only */
        WAITING_ON_TXN,
        /** leads everything and is in the run queue, but the runner is busy with another task */
        WAITING_TO_RUN
    }

    /** how {@code Listener::onUpdate} is provoked */
    enum Source
    {
        /** {@code AccordCache.loaded} - fired with the entry's waiters drained and not yet re-added */
        LOAD_COMPLETED,
        /** {@code AccordCache.Type.Instance.release} - fired with the entry still locked by the releasing task */
        MODIFIED_RELEASE
    }

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        // LOADING_OPTIONAL must be reachable: with a minimum of 1 a task is never short of its batch, so never parks
        DatabaseDescriptor.getAccord().queue_nonsync_min_batch_size = MIN_BATCH;
        DatabaseDescriptor.getAccord().queue_nonsync_max_batch_size = MIN_BATCH;
    }

    @Before
    public void requireArrangeablePreconditions()
    {
        // AccordExecutor reads the batch sizes once into static finals, so setUpClass must win the race with its first
        // class-load, or LOADING_OPTIONAL is unreachable and every case naming it quietly tests something else
        Invariants.require(AccordExecutor.NONSYNC_MIN_BATCH_SIZE == MIN_BATCH,
                           "expected a minimum batch of %d, found %d: the config was read before setUpClass set it",
                           MIN_BATCH, AccordExecutor.NONSYNC_MIN_BATCH_SIZE);
    }

    @After
    public void clearRangeIndex()
    {
        AccordCommandStore.unsafeRangeIndexFactory = null;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // the enumeration
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Every reachable arrival state x every notification source it admits x sync/batched, run as one table so that a
     * failure names the case. Some combinations are excluded by construction:
     *
     * <ul>
     *   <li>{@code LOADING_REQUIRED} is a sync-only state (a batched task's keys are optional) and
     *       {@code LOADING_OPTIONAL} a batched-only one.</li>
     *   <li>{@link Source#MODIFIED_RELEASE} needs a task to <em>run</em> and release, and an {@code ExclusiveExecutor}
     *       serves one task per command store at a time - which is the lever {@code WAITING_TO_RUN} uses to hold itself
     *       open. So the release source is covered only in the states that do not need the run slot held.</li>
     *   <li>{@code WAITING_ON_TXN} is INCR-only, and {@code WAITING_ON_KEY} is absent. To wait, a task must queue behind
     *       a claim in the fifo region or the sorted prefix, and a top-level task is always unsequenced, so on
     *       commands-for-key entries two top-level tasks on one store never block each other - they do not need to, as
     *       the store's run slot serialises them. The exception is a txnId claim by an INCR task, which
     *       {@code isUnsequenced(entry)} routes to the sorted prefix; that is what {@code WAITING_ON_TXN} uses here.
     *       Blocking on a <em>key</em> needs a fifo claim, which nothing can hold while the store's runner is occupied,
     *       so that state belongs in a harness that sets it directly (see {@code AccordCacheEntrySafeTaskCycleTest}).</li>
     * </ul>
     */
    @Test
    public void everyArrivalStateAdoptsExactlyOnceTest() throws InterruptedException
    {
        List<String> failures = new ArrayList<>();
        int cases = 0;
        for (Arrival arrival : Arrival.values())
        {
            for (Source source : Source.values())
            {
                for (LoadKeys loadKeys : new LoadKeys[]{ LoadKeys.SYNC, LoadKeys.ASYNC, LoadKeys.INCR })
                {
                    if (arrival == Arrival.LOADING_REQUIRED && loadKeys != LoadKeys.SYNC)
                        continue; // a batched task's keys are optional, so it never waits on a required key load
                    if (arrival == Arrival.LOADING_OPTIONAL && loadKeys == LoadKeys.SYNC)
                        continue; // and a sync task has no optional keys, so it never parks there
                    if (arrival == Arrival.WAITING_ON_TXN && loadKeys != LoadKeys.INCR)
                        continue; // only an INCR task takes a sorted txnId claim, and only sorted claims order
                    if (source == Source.MODIFIED_RELEASE && holdsRunSlot(arrival))
                        continue; // the run slot is held, so nothing can run and release

                    ++cases;
                    String desc = arrival + " via " + source + " (" + loadKeys + ')';
                    try
                    {
                        runCase(arrival, source, loadKeys);
                    }
                    catch (Throwable t)
                    {
                        java.io.StringWriter w = new java.io.StringWriter();
                        t.printStackTrace(new java.io.PrintWriter(w));
                        failures.add(desc + "\n      " + (failures.isEmpty() ? w.toString() : t.toString()));
                    }
                }
            }
        }

        if (!failures.isEmpty())
            org.junit.Assert.fail(failures.size() + " of " + cases + " adoption cases failed:\n  " + String.join("\n  ", failures));
        System.out.println(getClass().getSimpleName() + ": " + cases + " adoption cases checked");
    }

    /**
     * The filter, not the state machine: an adoption must be refused for a key outside the declared ranges, and for one
     * the loader does not consider relevant - so that the enumeration above cannot pass by adopting everything.
     */
    @Test
    public void adoptsOnlyRelevantKeysInRangeTest() throws InterruptedException
    {
        try (Fixture f = new Fixture())
        {
            f.blockScan();
            f.irrelevant(IN_FIRST);
            f.submitRangeTask(LoadKeys.SYNC);

            assertTrue("the scan never started", f.scanStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // HI is outside the declared range; IN_FIRST is inside it but the loader rejects it. Neither may be adopted.
            f.loadIntoCache(HI);
            f.loadIntoCache(IN_FIRST);
            f.assertQueuesConsistent();

            f.releaseScan.signal();
            f.assertRanCleanly();

            assertFalse("a key outside the declared ranges was adopted", f.holdsRef(HI));
            assertFalse("an irrelevant key was adopted", f.holdsRef(IN_FIRST));
            f.assertNoResidualPositions();
        }
    }

    /**
     * The double-place, stated on its own so that it fails by name. A batched task parked in {@code LOADING_OPTIONAL}
     * whose adoption is what completes its minimum batch: {@code addLoadedOptionalKey} then drives it through
     * {@code waitOnTxnsExclusive} into {@code waitOnKeysExclusive}, which claims the just-adopted reference, so the state
     * test in {@code reference} must not claim it again.
     */
    @Test
    public void adoptionThatCompletesTheBatchPlacesOnceTest() throws InterruptedException
    {
        runCase(Arrival.LOADING_OPTIONAL, Source.LOAD_COMPLETED, LoadKeys.ASYNC);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // one case
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Whether {@code arrival} is a state the task can only be <em>held</em> in by denying it the command store's run
     * slot. An {@code ExclusiveExecutor} serves one task per store at a time, so a task parked inside its body keeps
     * every other task on that store out of the runner, making these states holdable rather than transient.
     */
    /**
     * Adoption refuses a key an update failed to reach: {@code RangeTxnAndKeyScanner.reference} consults the
     * INCONSISTENT bit and fails its own task rather than taking a reference on it.
     *
     * <p>This is the one path that reaches the bit without declaring the key: a range task does not know its keys when
     * it is submitted, so it cannot be refused at {@code setupExclusive} the way a key-domain task is - it discovers
     * them through the watcher, mid-flight, and must refuse them there instead. Without that check the task would run
     * over a state an update never reached, and would do so with no report to anyone.
     *
     * <p>The arrangement is the reachable one: the entry is marked while it is still <em>loading</em>, with the scan
     * live, so {@code Listener::onUpdate} fires from {@code loaded()} and reaches the watcher. Marking it before its
     * loader was set up would simply refuse the loader, and there would be no notification at all.
     */
    @Test
    public void adoptionRefusesAKeyAnUpdateFailedToReachTest() throws InterruptedException
    {
        try (Fixture f = new Fixture())
        {
            f.blockLoadOf(IN_FIRST);
            f.startBlockedLoadOf(IN_FIRST);
            f.awaitLoading(IN_FIRST);

            // the scan must still be running when the notification arrives, or the watcher has been deregistered
            f.blockScan();
            f.submitRangeTask(LoadKeys.INCR);
            assertTrue("the scan never started", f.scanStarted.await(ARRANGE_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            f.awaitState(Arrival.SCANNING_RANGES);

            f.markInconsistent(IN_FIRST);
            f.releaseLoad.signal();
            // the notification is delivered inside loaded(), under the executor lock, so an entry observed LOADED from
            // another thread (which must take that lock) has already notified: this is the fence for "the watcher has
            // seen it", and without it releasing the scan below could race the notification
            f.awaitLoaded(IN_FIRST);

            // let the scan finish, so the task completes either way: if the bit were not consulted it would adopt the
            // key and run with it, and this test must fail with that, not with a timeout
            f.releaseScan.signal();
            // NB a timeout here is itself a failure of the property: with the bit check removed the task adopts the key
            // and then does not complete, so "never completed" means the refusal did not happen either
            assertTrue("the range task never completed: it must be failed by the key it may not adopt, and instead took "
                       + "it (adopted=" + f.holdsRef(IN_FIRST) + ')',
                       f.done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            assertTrue("a range task that meets a key an update failed to reach must be failed with "
                       + "InconsistentEntryException, and was told " + f.failure.get(),
                       f.failure.get() instanceof InconsistentEntryException);
            assertTrue("it must not have adopted the key it refused", !f.holdsRef(IN_FIRST));
            assertTrue("and must not have run with it: " + IN_FIRST, !f.ranWith(IN_FIRST));
            f.assertNoResidualPositions();
            f.assertNoInternalFailures();
        }
    }

    private static boolean holdsRunSlot(Arrival arrival)
    {
        switch (arrival)
        {
            case WAITING_ON_TXN:
            case WAITING_TO_RUN:
                return true;
            default:
                return false;
        }
    }

    private void runCase(Arrival arrival, Source source, LoadKeys loadKeys) throws InterruptedException
    {
        try (Fixture f = new Fixture())
        {
            // key 1 is what the scan discovers, key 2 is what gets adopted, key 3 is a second scanned key where one is
            // needed; all three are strictly inside the declared range (LO, IN_LAST]. HI is outside it, and is what the
            // run-slot holder takes, so that it cannot itself be discovered or adopted.
            int scanned = 1, adopted = 2, secondScanned = 3;

            f.discover(scanned);
            if (arrival == Arrival.LOADING_OPTIONAL)
                f.discover(secondScanned); // so that a minimum batch of 2 is not met by one key alone

            // the load timings that produce the state we want at the moment of the notification
            if (arrival == Arrival.LOADING_REQUIRED || arrival == Arrival.LOADING_OPTIONAL)
                f.blockLoadOf(scanned);              // the scanned key never finishes loading until we let it
            if (arrival == Arrival.LOADING_OPTIONAL)
                f.loadIntoCache(secondScanned);      // ... but the other scanned key is loaded: loaded=1, keys=2

            // for a holdable state the task must have everything it needs and be denied only the runner, so its key is
            // loaded up front, and everything that has to *run* must do so before the run slot is taken
            Blocker blocker = null;
            if (holdsRunSlot(arrival))
            {
                f.loadIntoCache(scanned);
                // the run slot first: setup proceeds without it, so anything submitted afterwards reaches
                // WAITING_TO_RUN and parks there holding whatever positions it took
                blocker = f.parkTaskHoldingRunSlot(HI);
                // then the competing claim, before the task under test, so that compare() puts it ahead
                if (arrival == Arrival.WAITING_ON_TXN)
                {
                    f.submitTxnIdCompetitor(f.txnId, HI);
                    f.awaitCompetitorWaitingToRun();
                }
            }

            if (arrival == Arrival.SCANNING_RANGES)
                f.blockScan();

            f.submitRangeTask(loadKeys);

            // wait until the task really is in the state this case is about
            if (arrival == Arrival.SCANNING_RANGES)
                assertTrue("the scan never started", f.scanStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            else
                f.awaitState(arrival);

            // ... and provoke the adoption
            f.provoke(source, adopted);
            f.awaitAdopted(adopted);

            // the observers saw the adoption itself, which is the only moment it is visible: a task releases its
            // references as it completes, so polling afterwards would find nothing
            Adoption adoption = f.adoption(adopted);
            assertEquals("the adoption did not happen in " + arrival + " (" + adoption + ')',
                         Task.State.valueOf(arrival.name()), adoption.before);
            // At-once acquisition: a task at or past WAITING_ON_TXN holds exactly one position for every key it holds a
            // reference for; before that it holds none. waitOnTxnsExclusive claims the keys alongside the txnIds, and
            // never gives them back.
            int expected = adoption.after.compareTo(Task.State.WAITING_ON_TXN) >= 0 ? 1 : 0;
            assertEquals("the adopted key ended up with " + adoption.positions + " positions in " + adoption.after
                         + " (" + adoption + "), not " + expected, expected, adoption.positions);

            // then let everything go, in the order that keeps the arrangement meaningful
            if (arrival == Arrival.SCANNING_RANGES)
                f.releaseScan.signal();
            f.releaseLoad.signal();
            if (blocker != null)
                blocker.release();

            f.assertRanCleanly();
            assertTrue("the task never ran with the key it adopted", f.ranWith(adopted));
            f.assertNoResidualPositions();
            f.assertKeyStillUsable(adopted);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // fixture
    // -----------------------------------------------------------------------------------------------------------------

    /** a task parked mid-run, holding the command store's run slot, until {@link #release()} */
    private static class Blocker
    {
        final Condition running = Condition.newOneTimeCondition();
        final Condition release = Condition.newOneTimeCondition();
        final Condition done = Condition.newOneTimeCondition();

        void release()
        {
            release.signal();
        }
    }

    /** a task parked in {@code WAITING_TO_RUN}, holding the queue positions the task under test must wait behind */
    private static class Waiter
    {
        final Condition done = Condition.newOneTimeCondition();
        volatile SafeTask<?> task;
    }

    private static class Fixture implements AutoCloseable
    {
        final TableId tableId = TableId.fromUUID(new java.util.UUID(0, 1));
        final IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        /** keys in <em>token</em> order, so that {@code key(i)..key(j)} is a meaningful range */
        private final TokenKey[] keys;
        final TxnId txnId = TxnId.fromValues(1, 1, 0, new Id(1));
        private final AtomicLong nextTxnId = new AtomicLong(100);

        final Condition scanStarted = Condition.newOneTimeCondition();
        final Condition releaseScan = Condition.newOneTimeCondition();
        final Condition releaseLoad = Condition.newOneTimeCondition();
        final Condition done = Condition.newOneTimeCondition();
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        private final AccordExecutor executor;
        private final AccordCommandStore store;
        private final ControllableRangeIndex rangeIndex;
        private volatile boolean blockScan;
        private volatile TokenKey blockLoadKey;
        /** the task under test, published from its own body as well as from submit: it may run before submit returns */
        private volatile SafeTask<?> task;
        /** the keys the task under test was running with, accumulated across batches */
        private final java.util.Set<RoutingKey> ranWith = java.util.concurrent.ConcurrentHashMap.newKeySet();
        /** what the observers saw at each adoption: the state it happened in, and how many positions resulted */
        private final java.util.Map<RoutingKey, Adoption> adoptions = new java.util.concurrent.ConcurrentHashMap<>();
        /** whether the task under test held a reference for the notifying key when the watcher was entered */
        private final java.util.Map<RoutingKey, Boolean> heldBefore = new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.Map<RoutingKey, Task.State> stateBefore = new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.List<Throwable> observerFailures = new java.util.concurrent.CopyOnWriteArrayList<>();
        /**
         * Every helper task's completion. The residual-position check is only meaningful once all of them have finished
         * and released, as an entry still referenced by a loader task that has not run yet is not evidence of anything.
         */
        private final java.util.List<Condition> awaiting = new java.util.concurrent.CopyOnWriteArrayList<>();
        private Waiter competitor;
        /**
         * Anything reported to the agent. On these paths a report means an internal invariant broke - including the
         * at-once assertion inside {@code reference}, which throws on the loop thread and would otherwise surface only
         * as the secondary "never adopted", since the observers never get to run.
         */
        private final java.util.List<Throwable> agentFailures = new java.util.concurrent.CopyOnWriteArrayList<>();

        Fixture()
        {
            TokenKey[] all = new TokenKey[KEY_COUNT];
            for (int i = 0 ; i < all.length ; ++i)
                all[i] = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(i)));
            java.util.Arrays.sort(all);
            keys = all;

            // Four loop threads: they serve both tasks and IO, so a load the test is holding open occupies one and the
            // rest of the lifecycle needs the others. With one thread a held load wedges the executor.
            executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 4, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, recordingAgent());
            AtomicReference<ControllableRangeIndex> installed = new AtomicReference<>();
            AccordCommandStore.unsafeRangeIndexFactory = cs -> {
                ControllableRangeIndex index = new ControllableRangeIndex(cs, primaryTxnId -> {
                    scanStarted.signal();
                    if (blockScan)
                        releaseScan.awaitUninterruptibly(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    return ControllableRangeIndex.Outcome.NOTHING;
                });
                installed.set(index);
                return index;
            };
            // without a load function the cache tries to load for real, which with no schema behind it is an NPE rather
            // than a miss; a key loads to a non-null CommandsForKey so that the entry is worth caching
            executor.cacheUnsafe().types().forEach(this::setLoadFunction);
            store = commandStore(tableId, partitioner, executor, recordingAgent());
            rangeIndex = installed.get();
            assertNotNull("the range index factory was not consulted", rangeIndex);
            registerObservers();
            executor.executeDirectlyWithLock(() -> {
                executor.setCapacity(8 << 20);
                executor.setWorkingSetSize(4 << 20);
            });
        }

        /**
         * Two observers, which between them turn an adoption into an event with a before and an after; polling cannot see
         * it, as the task releases its references as it completes.
         *
         * <p>{@code notifyListeners} runs the <em>instance</em> listeners and then the <em>type</em> listeners, and the
         * scanner's {@code KeyWatcher} is an instance listener registered when the scan starts. So an instance listener
         * registered here - before the task under test exists - runs before the watcher, and a type listener after it.
         */
        private void registerObservers()
        {
            executor.executeDirectlyWithLock(() -> {
                store.cachesUnsafe().commandsForKeys().register(new AccordCache.Listener<RoutingKey, CommandsForKey>()
                {
                    @Override
                    public void onUpdate(AccordCacheEntry<RoutingKey, CommandsForKey, ?> entry)
                    {
                        SafeTask<?> t = task;
                        if (t == null || t.refs == null)
                            return;
                        heldBefore.put(entry.key(), t.refs.containsKey(entry.key()));
                        stateBefore.put(entry.key(), t.state());
                    }
                });
                store.cachesUnsafe().commandsForKeys().parent().register(new AccordCache.Listener<RoutingKey, CommandsForKey>()
                {
                    @Override
                    public void onUpdate(AccordCacheEntry<RoutingKey, CommandsForKey, ?> entry)
                    {
                        try { observeAfter(entry); }
                        catch (Throwable t) { observerFailures.add(t); }
                    }
                });
            });
        }

        private void observeAfter(AccordCacheEntry<RoutingKey, CommandsForKey, ?> entry)
        {
            SafeTask<?> t = task;
            if (t == null || t.refs == null || !t.refs.containsKey(entry.key()))
                return;
            if (Boolean.TRUE.equals(heldBefore.get(entry.key())))
                return; // already held it, so this notification adopted nothing

            int positions = 0;
            for (SafeTask<?> queued : entry.unsafeQueuedTasks())
            {
                if (queued == t)
                    ++positions;
            }
            Adoption prev = adoptions.put(entry.key(), new Adoption(stateBefore.get(entry.key()), t.state(), positions));
            Invariants.require(prev == null, "%s was adopted twice", entry.key());
        }

        private TestAgent recordingAgent()
        {
            return new TestAgent()
            {
                @Override public void onException(Throwable t) { agentFailures.add(t); }
                @Override public void onException(Throwable t, String context) { agentFailures.add(t); }
            };
        }

        /** raw, because the cache types are heterogeneous */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private void setLoadFunction(AccordCache.Type type)
        {
            type.unsafeSetLoadFunction((java.util.function.BiFunction<AccordCommandStore, Object, Object>) (ignoreStore, k) -> {
                TokenKey held = blockLoadKey;
                if (held != null && held.equals(k))
                    releaseLoad.awaitUninterruptibly(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                return k instanceof RoutingKey ? new CommandsForKey((RoutingKey) k) : null;
            });
        }

        TokenKey key(int ordinal)
        {
            return keys[ordinal];
        }

        /** the range the task under test declares: {@code (key(LO), key(IN_LAST)]}, so {@link #LO} is outside it */
        Ranges range()
        {
            return Ranges.of(TokenRange.create(key(LO), key(IN_LAST)));
        }

        void discover(int ordinal)
        {
            rangeIndex.discover(key(ordinal));
        }

        /** make the loader reject {@code ordinal}, so an in-range notification for it must not be adopted */
        void irrelevant(int ordinal)
        {
            TokenKey reject = key(ordinal);
            rangeIndex.relevance(k -> !reject.equals(k));
        }

        void blockScan()
        {
            blockScan = true;
        }

        void blockLoadOf(int ordinal)
        {
            blockLoadKey = key(ordinal);
        }

        void submitRangeTask(LoadKeys loadKeys)
        {
            ExecutionContext context = AccordExecutionTestUtils.idempotent(
                ExecutionContext.contextFor(txnId, null, range(), loadKeys, LoadKeysFor.RECOVERY, "adopting"));
            Object submitted = store.execute(context, (Consumer<? super SafeCommandStore>) safeStore -> {
                SafeTask<?> self = ((SaferCommandStore) safeStore).task;
                task = self;
                Unseekables<?> active = self.isSync() ? self.executionContext().keys() : self.nonSync().keys();
                if (active != null && active.domain() == accord.primitives.Routable.Domain.Key)
                {
                    for (RoutingKey k : (accord.primitives.AbstractUnseekableKeys) active)
                        ranWith.add(k);
                }
                else
                {   // a sync range task runs with everything it referenced
                    self.refs.forEach((k, v) -> { if (k instanceof RoutingKey) ranWith.add((RoutingKey) k); });
                }
            }, (success, fail) -> { failure.set(fail); done.signal(); });
            task = (SafeTask<?>) submitted;
        }

        /**
         * A task parked mid-run: it holds the command store's single run slot until {@link Blocker#release()}, which is
         * what makes {@code WAITING_TO_RUN} and the waiting states holdable rather than transient. Its key is outside the
         * range the task under test declares, so it neither discovers nor adopts it.
         */
        Blocker parkTaskHoldingRunSlot(int ordinal)
        {
            Blocker blocker = new Blocker();
            ExecutionContext context = ExecutionContext.contextFor(otherTxnId(), null, RoutingKeys.of(key(ordinal)),
                                                                   LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "runSlotHolder");
            store.execute(context, (Consumer<? super SafeCommandStore>) ignore -> {
                blocker.running.signal();
                blocker.release.awaitUninterruptibly(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }, (success, fail) -> blocker.done.signal());
            awaiting.add(blocker.done);
            assertTrue("the run-slot holder never started", blocker.running.awaitUninterruptibly(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            return blocker;
        }

        /**
         * An INCR task on {@code txnId}, submitted before the task under test so that {@code compare} orders it first.
         * INCR because {@code isUnsequenced(entry)} routes an incremental task's <em>txnId</em> claim to the sorted
         * prefix rather than to the bag, and only ordered claims make anyone wait.
         */
        void submitTxnIdCompetitor(TxnId txnId, int keyOrdinal)
        {
            competitor = new Waiter();
            ExecutionContext context = AccordExecutionTestUtils.idempotent(
                ExecutionContext.contextFor(txnId, null, RoutingKeys.of(key(keyOrdinal)),
                                            LoadKeys.INCR, LoadKeysFor.READ_WRITE, "competitor"));
            competitor.task = (SafeTask<?>) store.execute(context, (Consumer<? super SafeCommandStore>) ignore -> {},
                                                          (success, fail) -> competitor.done.signal());
            awaiting.add(competitor.done);
        }

        /**
         * Wait until the competitor has parked in {@code WAITING_TO_RUN}: only then has it taken - and, with the run slot
         * held, kept - the txnId position the task under test must queue behind. One that had already <em>run</em> would
         * not do, as it locks with {@code RELEASE_QUEUE} and gives its position up entirely.
         */
        void awaitCompetitorWaitingToRun() throws InterruptedException
        {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(ARRANGE_TIMEOUT_SECONDS);
            Task.State last = null;
            while (System.nanoTime() < deadline)
            {
                AtomicReference<Task.State> state = new AtomicReference<>();
                executor.executeDirectlyWithLock(() -> state.set(competitor.task.state()));
                last = state.get();
                if (last == Task.State.WAITING_TO_RUN)
                    return;
                Thread.sleep(1);
            }
            throw new AssertionError("the competitor never parked in WAITING_TO_RUN (last saw " + last + ") - the run slot "
                                     + "is not being held, so it cannot keep the position under test");
        }

        /** a txnId nothing else in the case uses */
        TxnId otherTxnId()
        {
            return TxnId.fromValues(1, nextTxnId.incrementAndGet(), 0, new Id(1));
        }

        /** provoke a {@code Listener::onUpdate} for {@code ordinal}, by the requested route */
        void provoke(Source source, int ordinal) throws InterruptedException
        {
            switch (source)
            {
                case LOAD_COMPLETED:
                    // a task that loads the key: its load completing runs AccordCache.loaded, which notifies
                    loadIntoCache(ordinal);
                    break;
                case MODIFIED_RELEASE:
                    // a task that modifies the key and releases it: Instance.release notifies while still locked
                    modifyAndRelease(ordinal);
                    break;
            }
        }

        /**
         * Submit a task over {@code ordinal} and wait until the <em>entry</em> is loaded, not until the task completes:
         * the load is an IO task on a loop thread, so it finishes - and notifies - whether or not the run slot is
         * available, and waiting for the task would deadlock every case that holds the run slot.
         */
        void loadIntoCache(int ordinal) throws InterruptedException
        {
            Condition ready = Condition.newOneTimeCondition();
            awaiting.add(ready);
            ExecutionContext context = ExecutionContext.contextFor(otherTxnId(), null, RoutingKeys.of(key(ordinal)),
                                                                   LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "load" + ordinal);
            store.execute(context, (Consumer<? super SafeCommandStore>) ignore -> {}, (success, fail) -> ready.signal());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (System.nanoTime() < deadline)
            {
                AtomicReference<Boolean> loaded = new AtomicReference<>(false);
                executor.executeDirectlyWithLock(() -> {
                    AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(key(ordinal));
                    loaded.set(entry != null && entry.isLoaded());
                });
                if (loaded.get())
                    return;
                Thread.sleep(1);
            }
            throw new AssertionError("the entry for key " + ordinal + " never finished loading");
        }

        /**
         * Modify {@code ordinal} through a task and let it complete: {@code Instance.release} notifies <em>before</em> it
         * removes the task's position, so the watcher sees the entry still locked by the releasing task - a materially
         * different shape from a load completion, which notifies with the waiters drained and not yet re-added.
         */
        void modifyAndRelease(int ordinal) throws InterruptedException
        {
            Condition ready = Condition.newOneTimeCondition();
            ExecutionContext context = ExecutionContext.contextFor(otherTxnId(), null, RoutingKeys.of(key(ordinal)),
                                                                   LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "modify" + ordinal);
            store.execute(context, (Consumer<? super SafeCommandStore>) safeStore -> {
                SaferCommandsForKey safeCfk = (SaferCommandsForKey) ((SaferCommandStore) safeStore).task.refs.get(key(ordinal));
                safeCfk.set(new CommandsForKey(key(ordinal), nextTxnId.incrementAndGet()));
            }, (success, fail) -> ready.signal());
            awaiting.add(ready);
            assertTrue("modifying key " + ordinal + " never completed", ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }

        /** poll until the task under test is in {@code arrival} */
        void awaitState(Arrival arrival) throws InterruptedException
        {
            Task.State expected = Task.State.valueOf(arrival.name());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(ARRANGE_TIMEOUT_SECONDS);
            Task.State last = null;
            while (System.nanoTime() < deadline)
            {
                SafeTask<?> t = task;
                if (t != null)
                {
                    AtomicReference<Task.State> state = new AtomicReference<>();
                    executor.executeDirectlyWithLock(() -> state.set(t.state()));
                    last = state.get();
                    if (last == expected)
                        return;
                }
                Thread.sleep(1);
            }
            throw new AssertionError("the task never reached " + expected + " (last saw " + last + ')');
        }

        /**
         * Start a task that loads {@code ordinal} and return without waiting: the load blocks in the load function
         * ({@link #blockLoadOf}) until {@link #releaseLoad} is signalled, which is what leaves the entry LOADING while
         * the test arranges the rest.
         */
        void startBlockedLoadOf(int ordinal)
        {
            Condition ready = Condition.newOneTimeCondition();
            awaiting.add(ready);
            ExecutionContext context = ExecutionContext.contextFor(otherTxnId(), null, RoutingKeys.of(key(ordinal)),
                                                                  LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "blockedLoad" + ordinal);
            store.execute(context, (Consumer<? super SafeCommandStore>) ignore -> {}, (success, fail) -> ready.signal());
        }

        /** poll until {@code ordinal}'s entry exists and is loading, so that marking it cannot race with its creation */
        void awaitLoading(int ordinal) throws InterruptedException
        {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(ARRANGE_TIMEOUT_SECONDS);
            while (System.nanoTime() < deadline)
            {
                AtomicReference<Boolean> loading = new AtomicReference<>(false);
                executor.executeDirectlyWithLock(() -> {
                    AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(key(ordinal));
                    loading.set(entry != null && entry.isLoading());
                });
                if (loading.get())
                    return;
                Thread.sleep(1);
            }
            throw new AssertionError("key " + ordinal + " never started loading");
        }

        /** poll until {@code ordinal}'s entry is loaded; see the fence this provides in the adoption-refusal case */
        void awaitLoaded(int ordinal) throws InterruptedException
        {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (System.nanoTime() < deadline)
            {
                AtomicReference<Boolean> loaded = new AtomicReference<>(false);
                executor.executeDirectlyWithLock(() -> {
                    AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(key(ordinal));
                    loaded.set(entry != null && entry.isLoaded());
                });
                if (loaded.get())
                    return;
                Thread.sleep(1);
            }
            throw new AssertionError("key " + ordinal + " never finished loading");
        }

        /** mark {@code ordinal} as holding state an update failed to reach, as a failed fan-out's key would be */
        void markInconsistent(int ordinal)
        {
            executor.executeDirectlyWithLock(() -> {
                AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(key(ordinal));
                assertNotNull("key " + ordinal + " is not resident, so it cannot be marked", entry);
                AccordExecutionTestUtils.setInconsistent(entry);
            });
        }

        /** poll until the task under test has adopted {@code ordinal} */
        void awaitAdopted(int ordinal) throws InterruptedException
        {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(ARRANGE_TIMEOUT_SECONDS);
            while (System.nanoTime() < deadline)
            {
                if (adoptions.containsKey(key(ordinal)))
                    return;
                Thread.sleep(1);
            }
            throw new AssertionError("key " + ordinal + " was never adopted - the notification did not reach the watcher, "
                                     + "so this case proves nothing"
                                     + (agentFailures.isEmpty() ? "" : "; the agent was told: " + agentFailures.get(0)));
        }

        /** what the observers recorded for {@code ordinal}, or null if it was never adopted */
        Adoption adoption(int ordinal)
        {
            return adoptions.get(key(ordinal));
        }

        /** whether the task under test ever adopted {@code ordinal} */
        boolean holdsRef(int ordinal)
        {
            return adoptions.containsKey(key(ordinal));
        }

        /**
         * At-once acquisition: for every commands-for-key reference the task holds it must hold exactly one position if
         * it has reached {@code WAITING_ON_KEY}, and none before that.
         */
        void assertQueuesConsistent()
        {
            SafeTask<?> t = task;
            if (t == null)
                return;
            executor.executeDirectlyWithLock(() -> {
                if (t.refs == null)
                    return;
                boolean placed = t.compareTo(Task.State.WAITING_ON_KEY) >= 0;
                t.refs.forEach((k, v) -> {
                    if (!(v instanceof SaferCommandsForKey))
                        return;
                    AccordCacheEntry<?, ?, ?> entry = ((SaferCommandsForKey) v).global();
                    int found = 0;
                    for (SafeTask<?> queued : entry.unsafeQueuedTasks())
                    {
                        if (queued == t)
                            ++found;
                    }
                    Invariants.require(found <= 1, "%s holds %d positions on %s - it waits for itself",
                                       t.currentState(), found, k);
                    if (placed && entry.isLoaded() && !entry.isLockedBy(t))
                        Invariants.require(found == 1, "%s holds a reference for %s but no position there", t.currentState(), k);
                });
            });
        }

        boolean ranWith(int ordinal)
        {
            return ranWith.contains(key(ordinal));
        }

        /**
         * Nothing may be left holding a position, or anything queued behind it waits for ever. Polled rather than
         * asserted once: a task's callback is invoked from inside {@code finish()}, before its references are released,
         * so having been notified of every submission does not mean the executor has finished with them.
         */
        void assertNoResidualPositions() throws InterruptedException
        {
            for (Condition condition : awaiting)
                assertTrue("a helper task never completed", condition.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            AtomicReference<String> residual = new AtomicReference<>();
            while (true)
            {
                residual.set(null);
                executor.executeDirectlyWithLock(() -> {
                    for (AccordCacheEntry<RoutingKey, CommandsForKey, SaferCommandsForKey> entry : store.cachesUnsafe().commandsForKeys())
                    {
                        if (entry.references() != 0)
                            residual.compareAndSet(null, entry + " is still referenced");
                        else if (!entry.unsafeQueuedTasks().isEmpty())
                            residual.compareAndSet(null, entry.key() + " still has queued tasks: " + entry.unsafeQueuedTasks());
                    }
                });
                if (residual.get() == null)
                    return;
                if (System.nanoTime() >= deadline)
                    throw new AssertionError("after every task completed, " + residual.get());
                Thread.sleep(1);
            }
        }

        /** the corpse detector: a later task on the adopted key must still run */
        void assertKeyStillUsable(int ordinal) throws InterruptedException
        {
            Condition after = Condition.newOneTimeCondition();
            AtomicReference<Throwable> afterFailure = new AtomicReference<>();
            ExecutionContext context = ExecutionContext.contextFor(TxnId.fromValues(1, nextTxnId.incrementAndGet(), 0, new Id(1)),
                                                                   null, RoutingKeys.of(key(ordinal)),
                                                                   LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "after");
            store.execute(context, (Consumer<? super SafeCommandStore>) ignore -> {},
                          (success, fail) -> { afterFailure.set(fail); after.signal(); });
            assertTrue("a task on the adopted key never ran afterwards - a position was left behind",
                       after.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertNull("the task after the adoption failed", afterFailure.get());
        }

        void assertRanCleanly() throws InterruptedException
        {
            assertTrue("the adopting task never completed", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertNull("the adopting task failed", failure.get());
            assertNoInternalFailures();
        }

        /** an agent report or an objection from an observer means an invariant broke */
        void assertNoInternalFailures()
        {
            if (!agentFailures.isEmpty())
                throw new AssertionError("an internal invariant was reported to the agent", agentFailures.get(0));
            if (!observerFailures.isEmpty())
                throw new AssertionError("the adoption observer objected", observerFailures.get(0));
        }

        @Override
        public void close() throws InterruptedException
        {
            releaseScan.signal();
            releaseLoad.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /** what the observers saw around one adoption */
    private static class Adoption
    {
        /** the state the task was in when the watcher was entered, and the state it left in */
        final Task.State before, after;
        /** how many positions it held on the adopted entry once the watcher returned; anything but 1 is a bug */
        final int positions;

        Adoption(Task.State before, Task.State after, int positions)
        {
            this.before = before;
            this.after = after;
            this.positions = positions;
        }

        public String toString()
        {
            return before + "->" + after + " positions=" + positions;
        }
    }

    /**
     * An in-memory journal and no persistence, so no schema, cluster metadata or commit log is needed, and
     * {@link TestAgent} rather than {@code AccordAgent}, whose exception reporting initialises
     * {@code AccordSystemMetrics} and requires a started {@code AccordService}.
     */
    private static AccordCommandStore commandStore(TableId tableId, IPartitioner partitioner, AccordExecutor executor, TestAgent agent)
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

        return new AccordCommandStore(0, node, agent, null,
                                      cs -> new ProgressLog.NoOpProgressLog(),
                                      cs -> new DefaultLocalListeners(null, new DefaultRemoteListeners.NoOpRemoteListeners(), new NotifySink.NoOpNotifySink()),
                                      new RangesForEpoch(1, Ranges.of(TokenRange.fullRange(tableId, partitioner))),
                                      new InMemoryJournal(nodeId, new DefaultRandom(1)),
                                      executor);
    }
}
