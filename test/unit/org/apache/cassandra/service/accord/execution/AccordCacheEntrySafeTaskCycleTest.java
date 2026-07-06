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

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import accord.api.RoutingKey;
import accord.local.ExecutionContext;
import accord.local.ExecutionContext.ExecutionKind;
import accord.local.Node.Id;
import accord.local.SafeCommandStore;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers.BufferList;
import accord.utils.Invariants;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status;
import org.apache.cassandra.service.accord.execution.SafeTask.NonSyncState;
import org.apache.cassandra.service.accord.execution.Task.State;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test has been authored entirely by Claude.
 *
 * The cycle enumeration of {@link AccordCacheEntryCycleTest}, driven through <em>real</em> {@link SafeTask}s, so that
 * none of the placement, reposition or notification logic is reimplemented by the harness (and so a regression in it
 * cannot be invisible here). A real {@link SafeTask} is built against a mocked {@link AccordCommandStore} whose
 * executor's queue fields are real {@link TaskQueue}s, real {@link SaferCommand}/{@link SaferCommandsForKey} populate
 * {@code refs}, and the lifecycle runs through the production methods:
 *
 * <ul>
 *   <li>placement: {@code addFifo} at setup for a fifo claim, else the real {@code ensureCacheQueued} - which is how
 *       {@code completeSetupOfLoaded} and {@code waitOnKeysExclusive} split the work;</li>
 *   <li>the first run: the real {@code prepareExclusiveMayThrow}, which repositions, takes the txnId with
 *       {@code HOLD_QUEUE} through the real {@code SaferCommand.preExecute}, and sets the started bit.</li>
 * </ul>
 */
public class AccordCacheEntrySafeTaskCycleTest
{
    /** how many adoptions the enumeration under way actually performed; a zero would make those axes vacuous */
    private int adoptions;
    /**
     * How many scenarios reached {@code firstRun} with the task still waiting on a txnId, so production would not have
     * let it prepare. Reported as its own skip reason rather than folded into the passing count: an early return there
     * is a scenario that ran less than its name says.
     */
    private static int blockedAtFirstRun;
    /** and which states they happened in, so the coverage is asserted rather than hoped for */
    private final java.util.Set<State> adoptedIn = java.util.EnumSet.noneOf(State.class);

    /** which region of a queue a task's kind puts it in */
    enum Kind { FIFO, SORTED, BAG }

    enum Event { H_JOINS_KEY, H_FIRST_RUN, W_JOINS_TXN, W_JOINS_KEY, KEY_LOAD_COMPLETES, W_ADOPTS_KEY, W_PREPARES }

    private static final Event[] CORE = { Event.H_JOINS_KEY, Event.H_FIRST_RUN, Event.W_JOINS_TXN, Event.W_JOINS_KEY,
                                          Event.W_PREPARES };

    /**
     * Thrown when a scenario asks for a state production cannot reach, so it is counted rather than failed. The rule
     * that matters here: keys are claimed by {@code waitOnKeysExclusive}, which runs only once every txnId is led, so a
     * task blocked on a txnId holds no key positions - unless it is ATOMIC, which takes its fifo positions at setup.
     */
    private static class Unreachable extends RuntimeException
    {
        Unreachable(String why) { super(why); }
    }

    private static int nextId = 0;
    private static final TableId TABLE_ID = TableId.fromUUID(new java.util.UUID(0, 1));

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /** the inline mock maker retains every mock in a global registry that degrades superlinearly */
    @After
    public void clearInlineMocks()
    {
        org.mockito.Mockito.framework().clearInlineMocks();
    }

    @Test
    public void everyDirectCycleIsBroken()
    {
        assertEveryScenario(new Event[0], false);
    }

    @Test
    public void everyDirectCycleIsBrokenAcrossALoad()
    {
        assertEveryScenario(new Event[]{ Event.KEY_LOAD_COMPLETES }, true);
    }

    /**
     * The same enumeration with W <em>adopting</em> a key it never declared, at every point in its life: what a range
     * operation's {@code RangeTxnAndKeyScanner.KeyWatcher} does when the cache tells it about a key inside its ranges.
     *
     * <p>This is the only way a task's reference set grows after setup, so the only way its claims can acquire a second
     * timestamp - and the single-timestamp property is what the no-cycle argument rests on. The adopted entry is one H
     * already holds, so the adoption really does create a new wait edge between the pair.
     *
     * <p>It also reaches the two states {@code AccordExecutorKeyAdoptionTest} cannot arrange against a real executor,
     * {@code WAITING_ON_KEY} and {@code WAITING_TO_RUN}, because here the state is set rather than arranged.
     */
    @Test
    public void everyDirectCycleIsBrokenAcrossAnAdoption()
    {
        assertEveryScenario(new Event[]{ Event.W_ADOPTS_KEY }, false);
    }

    /** and with a load completing as well, so the adoption can land either side of the drain and re-add */
    @Test
    public void everyDirectCycleIsBrokenAcrossAnAdoptionAndALoad()
    {
        assertEveryScenario(new Event[]{ Event.KEY_LOAD_COMPLETES, Event.W_ADOPTS_KEY }, true);
    }

    /**
     * The guard the enumeration above can only report as unreachable: a fifo claim must never be taken from a cache
     * listener, because it would be a second acquisition instant - it precedes every sorted and bagged claim, so it would
     * place the task ahead of tasks it may already be queued behind elsewhere. Ordering the fifo region by {@code fifoAt}
     * narrows this, since a stamp taken once gives the same rank on every entry, but the claim would still cross regions.
     * Two non-local coincidences make it unreachable in
     * production (an ATOMIC range task inherits its parent's scan; the INCR upgrade happens after {@code finish}
     * unregisters the watcher), so the guard itself is worth pinning.
     */
    @Test
    public void aFifoTaskMayNotAdoptFromAListener()
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> txn = world.commandEntry();
        AccordCacheEntry<?, ?, ?> key = world.keyEntry(false);
        AccordCacheEntry<?, ?, ?> adoptable = world.keyEntry(false);

        SafeTask<?> atomic = world.task("ATOMIC", Kind.FIFO, txn, key);
        world.place(atomic, txn);
        world.place(atomic, key);

        try
        {
            atomic.addCachedKeyExclusive(adoptable, new SaferCommandsForKey((AccordCacheEntry) adoptable));
            fail("a fifo task adopted a key from a cache listener: the claim has no ordering guarantee against the "
                 + "claims it already holds");
        }
        catch (IllegalStateException expected)
        {
            // and it must have changed nothing on the way out
            org.junit.Assert.assertFalse("the refused adoption still took a reference", atomic.refs.containsKey(adoptable.key()));
            org.junit.Assert.assertTrue("the refused adoption still took a position", adoptable.unsafeQueuedTasks().isEmpty());
        }
    }

    private void assertEveryScenario(Event[] extras, boolean keyStartsLoading)
    {
        blockedAtFirstRun = 0;
        List<String> failures = new ArrayList<>();
        int scenarios = 0, unreachable = 0;
        adoptions = 0;
        adoptedIn.clear();
        for (Kind hKind : Kind.values())
        {
            for (Kind wKind : Kind.values())
            {
                for (List<Event> core : permutations(CORE))
                {

                    // splice each extra in at every position, independently, so two extras give (n+1)*(n+2) orderings
                    for (List<Event> order : splice(core, extras))
                    {
                        for (boolean unrelatedClaim : new boolean[]{ false, true })
                        for (boolean wSync : new boolean[]{ false, true })
                        for (boolean wSortsFirst : new boolean[]{ false, true })
                        {
                            ++scenarios;
                            // mockito-inline retains every mock until cleared, and nothing lives across a scenario
                            if ((scenarios % 2000) == 0)
                                Mockito.framework().clearInlineMocks();
                            String desc = "H=" + hKind + " W=" + wKind + " order=" + order + " unrelatedClaim=" + unrelatedClaim
                                          + " wSync=" + wSync + " wSortsFirst=" + wSortsFirst;
                            try
                            {
                                runScenario(hKind, wKind, order, unrelatedClaim, keyStartsLoading, wSync, wSortsFirst);
                            }
                            catch (Unreachable skip)
                            {
                                ++unreachable;
                            }
                            catch (Throwable t)
                            {
                                // the first failure carries its stack, to say which production path objected
                                if (failures.isEmpty())
                                {
                                    java.io.StringWriter w = new java.io.StringWriter();
                                    t.printStackTrace(new java.io.PrintWriter(w));
                                    failures.add(desc + "\n      " + w);
                                }
                                else
                                {
                                    failures.add(desc + "\n      " + t);
                                }
                            }
                        }
                    }
                }
            }
        }

        if (!failures.isEmpty())
            fail(failures.size() + " of " + scenarios + " scenarios failed (" + unreachable + " unreachable):\n  "
                 + String.join("\n  ", failures));

        System.out.println(getClass().getSimpleName() + ": " + (scenarios - unreachable) + " scenarios checked, "
                           + unreachable + " unreachable, " + adoptions + " adoptions in " + adoptedIn
                           + ", " + blockedAtFirstRun + " runs skipped because the task was still waiting on a txnId");
        // anti-vacuity: a scenario classified Unreachable is counted, not passed, so if a reachability filter ever
        // became universal every scenario would degrade to a no-op and the suite would stay green with a large
        // unreachable count nobody reads. 5% is comfortably below the 8-25% these axes reach.
        if (scenarios - unreachable <= scenarios / 20)
            fail("only " + (scenarios - unreachable) + " of " + scenarios + " scenarios were reachable: the"
                 + " reachability filters are rejecting nearly everything, so this enumeration checks nearly nothing");
        // an axis that never performed the event it is named for proves nothing, and both states are required as they
        // take different paths through adoptKeyExclusive: WAITING_ON_KEY places and stays, WAITING_TO_RUN places and,
        // for a sync task, is demoted back to WAITING_ON_KEY by incrementWaitingKeys
        for (Event extra : extras)
        {
            if (extra != Event.W_ADOPTS_KEY)
                continue;
            Invariants.require(adoptions > 0, "no adoption was performed");
            Invariants.require(adoptedIn.contains(State.WAITING_ON_KEY), "no adoption happened in WAITING_ON_KEY: %s", adoptedIn);
            Invariants.require(adoptedIn.contains(State.WAITING_TO_RUN), "no adoption happened in WAITING_TO_RUN: %s", adoptedIn);
        }
    }

    /**
     * Rings of three and four tasks: the shapes the two-task enumeration cannot reach.
     *
     * <p>For two tasks a cycle needs the pair ordered one way on one entry and the other way on another, which a sorted
     * region cannot do - {@code compare} is a function of the pair, so it agrees on every entry they share. The only
     * two-task inversion comes from a fifo region, whose order is arrival rather than {@code compare}, and there the
     * reposition at first prepare repairs it. A ring of three or more needs no pair to disagree, as the inversion is
     * transitive, so it is reachable where the two-task shape is not.
     *
     * <p>Each task declares the txnId and two of the keys, so the key sharing itself forms a ring. Enumerated: every
     * assignment of kinds to tasks (3^n), the order the tasks are created in - which decides sorted-region order, since
     * these tasks share a position and fall through to {@code createdAt} - the order they claim their entries in, and
     * where the holder's first run falls among the claims.
     */
    @Test
    public void everyRingIsBroken()
    {
        assertEveryRing(3);
    }

    @Test
    public void everyFourTaskRingIsBroken()
    {
        assertEveryRing(4);
    }

    private void assertEveryRing(int n)
    {
        List<String> failures = new ArrayList<>();
        int scenarios = 0, unreachable = 0;

        List<int[]> creationOrders = n == 3 ? permutationsOf(n) : rotationsOf(n);
        List<int[]> claimOrders = n == 3 ? permutationsOf(n) : rotationsOf(n);

        for (Kind[] kinds : kindAssignments(n))
        {
          for (boolean[] incr : incrementalAssignments(n))
          {
            for (int[] creation : creationOrders)
            {
                for (int[] claims : claimOrders)
                {
                    for (int runAt = 0 ; runAt <= n ; ++runAt)
                    {
                        ++scenarios;
                        // mockito-inline retains every mock until cleared, and clearing only in @After exhausts the
                        // heap part-way through the 4-task enumeration; nothing lives across a scenario
                        if ((scenarios % 2000) == 0)
                            Mockito.framework().clearInlineMocks();
                        String desc = "n=" + n + " kinds=" + Arrays.toString(kinds) + " incr=" + Arrays.toString(incr)
                                      + " creation=" + Arrays.toString(creation)
                                      + " claims=" + Arrays.toString(claims) + " runAt=" + runAt;
                        try
                        {
                            runRing(n, kinds, incr, creation, claims, runAt);
                        }
                        catch (Unreachable skip)
                        {
                            ++unreachable;
                        }
                        catch (Throwable t)
                        {
                            if (failures.isEmpty())
                            {
                                java.io.StringWriter w = new java.io.StringWriter();
                                t.printStackTrace(new java.io.PrintWriter(w));
                                failures.add(desc + "\n      " + w);
                            }
                            else
                            {
                                failures.add(desc + "\n      " + t);
                            }
                        }
                    }
                }
            }
          }
        }

        if (!failures.isEmpty())
            fail(failures.size() + " of " + scenarios + " ring scenarios failed (" + unreachable + " unreachable):\n  "
                 + String.join("\n  ", failures));
        System.out.println(getClass().getSimpleName() + ": " + n + "-task rings, " + (scenarios - unreachable)
                           + " checked, " + unreachable + " unreachable");
        if (scenarios - unreachable <= scenarios / 20)
            fail("only " + (scenarios - unreachable) + " of " + scenarios + " " + n + "-task ring scenarios were"
                 + " reachable: the reachability filters are rejecting nearly everything");
    }

    private void runRing(int n, Kind[] kinds, boolean[] incr, int[] creation, int[] claims, int runAt)
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> txn = world.commandEntry();
        List<AccordCacheEntry<?, ?, ?>> keys = new ArrayList<>();
        for (int i = 0 ; i < n ; ++i)
            keys.add(world.keyEntry(false));

        // task i shares key i with its predecessor and key i+1 with its successor, so the sharing is a ring
        SafeTask<?>[] tasks = new SafeTask<?>[n];
        for (int index : creation)
        {
            tasks[index] = world.task("t" + index, kinds[index], false, incr[index], txn,
                                      keys.get(index), keys.get((index + 1) % n));
        }

        // the holder is whoever leads the txnId, which is whoever was created first
        SafeTask<?> holder = tasks[creation[0]];

        int claimed = 0;
        for (int step = 0 ; step <= n ; ++step)
        {
            if (step == runAt)
                world.firstRun(holder);

            if (claimed < n)
            {
                int index = claims[claimed++];
                SafeTask<?> task = tasks[index];
                world.place(task, txn);
                world.place(task, keys.get(index));
                world.place(task, keys.get((index + 1) % n));
            }

            world.assertNoDuplicatePositions();
            world.assertNoCycle();
            world.assertStartedLeads();
        }
    }

    /**
     * Every assignment of incremental-ness. Only an INCR task that declares a txnId is upgraded to a fifo claim when it
     * starts, so a ring containing non-incremental members has fewer ways out, and one whose blocking positions are all
     * held by non-incremental tasks may have none.
     */
    private static List<boolean[]> incrementalAssignments(int n)
    {
        List<boolean[]> result = new ArrayList<>();
        for (int mask = 0 ; mask < (1 << n) ; ++mask)
        {
            boolean[] incr = new boolean[n];
            for (int i = 0 ; i < n ; ++i)
                incr[i] = (mask & (1 << i)) != 0;
            result.add(incr);
        }
        return result;
    }

    private static List<Kind[]> kindAssignments(int n)
    {
        List<Kind[]> result = new ArrayList<>();
        int total = 1;
        for (int i = 0 ; i < n ; ++i)
            total *= Kind.values().length;
        for (int mask = 0 ; mask < total ; ++mask)
        {
            Kind[] kinds = new Kind[n];
            int rest = mask;
            for (int i = 0 ; i < n ; ++i)
            {
                kinds[i] = Kind.values()[rest % Kind.values().length];
                rest /= Kind.values().length;
            }
            result.add(kinds);
        }
        return result;
    }

    private static List<int[]> permutationsOf(int n)
    {
        List<int[]> result = new ArrayList<>();
        int[] items = new int[n];
        for (int i = 0 ; i < n ; ++i)
            items[i] = i;
        permuteInts(items, 0, result);
        return result;
    }

    private static void permuteInts(int[] items, int from, List<int[]> result)
    {
        if (from == items.length)
        {
            result.add(items.clone());
            return;
        }
        for (int i = from ; i < items.length ; ++i)
        {
            int tmp = items[from]; items[from] = items[i]; items[i] = tmp;
            permuteInts(items, from + 1, result);
            tmp = items[from]; items[from] = items[i]; items[i] = tmp;
        }
    }

    /** rotations rather than every permutation, to keep the four-task space affordable; a ring's natural symmetry */
    private static List<int[]> rotationsOf(int n)
    {
        List<int[]> result = new ArrayList<>();
        for (int r = 0 ; r < n ; ++r)
        {
            int[] order = new int[n];
            for (int i = 0 ; i < n ; ++i)
                order[i] = (i + r) % n;
            result.add(order);
        }
        return result;
    }

    private void runScenario(Kind hKind, Kind wKind, List<Event> order, boolean unrelatedClaim, boolean keyStartsLoading)
    {
        runScenario(hKind, wKind, order, unrelatedClaim, keyStartsLoading, false);
    }

    private void runScenario(Kind hKind, Kind wKind, List<Event> order, boolean unrelatedClaim, boolean keyStartsLoading,
                             boolean wSync)
    {
        runScenario(hKind, wKind, order, unrelatedClaim, keyStartsLoading, wSync, false);
    }

    /**
     * @param wSortsFirst give W the earlier createdAt, so it sorts ahead of H in a sorted region. Without this the cycle
     *                    cannot form for a sorted or bagged W at all: {@code compare} would always place the older H
     *                    first, and W would queue harmlessly behind it.
     */
    private void runScenario(Kind hKind, Kind wKind, List<Event> order, boolean unrelatedClaim, boolean keyStartsLoading,
                             boolean wSync, boolean wSortsFirst)
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> txn = world.commandEntry();
        AccordCacheEntry<?, ?, ?> key = world.keyEntry(keyStartsLoading);

        // a second key, so that a sync W's prepare has a key left to lose while it is locking the first
        AccordCacheEntry<?, ?, ?> otherKey = world.keyEntry(false);
        // and a third that only H declares: what W adopts, so the adoption creates a *new* wait edge between the pair
        AccordCacheEntry<?, ?, ?> adoptable = world.keyEntry(false);
        SafeTask<?> h, w;
        if (wSortsFirst)
        {
            w = wSync ? world.task("W", wKind, true, txn, key, otherKey) : world.task("W", wKind, txn, key);
            h = world.task("H", hKind, txn, key, adoptable);
        }
        else
        {
            h = world.task("H", hKind, txn, key, adoptable);
            w = wSync ? world.task("W", wKind, true, txn, key, otherKey) : world.task("W", wKind, txn, key);
        }

        if (unrelatedClaim)
        {
            // an unrelated bag claim that has been on the key all along, so the cycle forms into a non-empty queue
            SafeTask<?> other = world.task("other", Kind.BAG, key);
            world.place(other, key);
        }

        world.place(h, txn);
        // An ATOMIC task claims every entry it declared in one uninterrupted pass, and its notifications can only take
        // positions and move tasks between executor queues - never run another task's prepare. So an ATOMIC claim cannot
        // straddle somebody else's first run, and modelling it as two events would manufacture unreachable cycles.
        boolean hClaimsAtSetup = hKind == Kind.FIFO;
        boolean wClaimsAtSetup = wSync || wKind == Kind.FIFO;
        if (hClaimsAtSetup)
        {
            world.place(h, key);
            world.place(h, adoptable);
        }

        try
        {
            for (Event event : order)
            {
                switch (event)
                {
                    case H_JOINS_KEY:
                        if (!hClaimsAtSetup)
                        {
                            world.place(h, key);
                            world.place(h, adoptable);
                        }
                        break;
                    case H_FIRST_RUN: world.firstRun(h); break;
                    case W_JOINS_TXN:
                        if (!wClaimsAtSetup)
                            world.place(w, txn);
                        break;
                    case W_JOINS_KEY:
                        if (wClaimsAtSetup)
                        {
                            world.place(w, txn);
                            world.place(w, key);
                            // only a sync W declares the second key; an ATOMIC W declares the txnId and the one key
                            if (wSync)
                                world.place(w, otherKey);
                        }
                        else
                        {
                            world.place(w, key);
                        }
                        break;
                    case KEY_LOAD_COMPLETES: world.completeLoad(key); break;
                    case W_ADOPTS_KEY: world.adoptKey(w, adoptable); break;
                    case W_PREPARES:
                        if (wSync)
                            world.prepareSync(w, txn, key, otherKey);
                        break;

                }
                world.assertNoDuplicatePositions();
                world.assertNoCycle();
                world.assertStartedLeads();
            }
        }
        finally
        {
            // accumulate even when a scenario is skipped part-way, so the coverage gate counts what happened
            adoptions += world.adoptions;
            adoptedIn.addAll(world.adoptedIn);
        }
    }

    private static class World
    {
        final List<AccordCacheEntry<?, ?, ?>> entries = new ArrayList<>();
        final Map<AccordCacheEntry<?, ?, ?>, SafeTask<?>> heldBy = new IdentityHashMap<>();
        final Map<SafeTask<?>, Kind> kinds = new IdentityHashMap<>();
        /** the txnId each task declares, which it must claim before any of its keys unless it is ATOMIC */
        final Map<SafeTask<?>, AccordCacheEntry<?, ?, ?>> declaredTxn = new IdentityHashMap<>();
        /** everything each task declared, so we can tell when it has claimed all of it and is ready to run */
        final Map<SafeTask<?>, List<AccordCacheEntry<?, ?, ?>>> declared = new IdentityHashMap<>();
        final AccordCommandStore store;
        final AtomicLong uniqueCreatedAt = new AtomicLong();
        /** how many adoptions this scenario performed, and the state each happened in */
        int adoptions;
        final java.util.Set<State> adoptedIn = java.util.EnumSet.noneOf(State.class);

        World()
        {
            AccordExecutor executor = mock(AccordExecutor.class);
            // the queues are final fields initialised inline, so a mock leaves them null; they must be real, as
            // Task.unqueue reads expected.kind and the tasks really do move between them
            set(executor, "runnable", new TaskQueueRunnable<Task>());
            set(executor, "loading", new TaskQueueStandalone<SafeTask<?>>(Task.ExecutorQueue.LOADING));
            set(executor, "waiting", new TaskQueueStandalone<SafeTask<?>>(Task.ExecutorQueue.WAITING));
            // final and initialised inline, so a mock leaves it null; prepareExclusiveMayThrow draws fifoAt from it when
            // it upgrades an incremental task to fifo. Shared with the tasks' createdAt, as production shares it.
            set(executor, "uniqueCreatedAt", uniqueCreatedAt);

            store = mock(AccordCommandStore.class);
            when(store.executor()).thenReturn(executor);
            // a real one: it must maintain the task's queue bits for the state machine
            when(store.exclusiveExecutor()).thenReturn(new ExclusiveExecutor(executor));
            when(store.id()).thenReturn(0);
            // SafeTask.toString reads commandStore.node().id()
            accord.local.NodeCommandStoreService node = mock(accord.local.NodeCommandStoreService.class);
            when(node.id()).thenReturn(new Id(1));
            when(store.node()).thenReturn(node);
        }

        AccordCacheEntry<?, ?, ?> commandEntry()
        {
            AccordCacheEntry<?, ?, ?> entry = new AccordCacheEntry<>(TxnId.fromValues(1, ++nextId, 0, new Id(1)), null);
            entry.unsafeSetStatus(Status.LOADED);
            entries.add(entry);
            return entry;
        }

        AccordCacheEntry<?, ?, ?> keyEntry(boolean loading)
        {
            RoutingKey routingKey = new TokenKey(TABLE_ID, DatabaseDescriptor.getPartitioner().getToken(Int32Type.instance.decompose(++nextId)));
            AccordCacheEntry<?, ?, ?> entry = new SaferCommandsForKey.CommandsForKeyCacheEntry(routingKey, null);
            entry.unsafeSetStatus(loading ? Status.LOADING : Status.LOADED);
            entries.add(entry);
            return entry;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        SafeTask<?> task(String name, Kind kind, AccordCacheEntry<?, ?, ?>... refs)
        {
            return task(name, kind, false, refs);
        }

        /**
         * @param sync a SYNC task, which must lead every one of its keys to run and counts its waits in
         *             {@code waitingForState} - a different notification path and prepare branch from a non-sync task
         */
        SafeTask<?> task(String name, Kind kind, boolean sync, AccordCacheEntry<?, ?, ?>... refs)
        {
            return task(name, kind, sync, true, refs);
        }

        /**
         * @param incremental an INCR task, which holds its txnId across runs and - where it declares one - is the only
         *                    kind upgraded to fifo on starting; a plain non-sync task can therefore block a chain while
         *                    being immovable
         */
        SafeTask<?> task(String name, Kind kind, boolean sync, boolean incremental, AccordCacheEntry<?, ?, ?>... refs)
        {
            ExecutionContext context = mock(ExecutionContext.class);
            TxnId primary = refs[0].isCommandsForKey() ? null : (TxnId) refs[0].key();
            when(context.primaryTxnId()).thenReturn(primary);
            when(context.executionKind()).thenReturn(ExecutionKind.OTHER);
            when(context.describe()).thenReturn(name);

            Function<? super SafeCommandStore, Object> function = ignore -> null;
            SafeTask<Object> task = new SafeTask<>(store, context, function, uniqueCreatedAt);
            if (!sync)
            {
                set(task, "optional", new NonSyncState<>(task, context));
                task.setNonSyncExclusive();
                if (incremental)
                {
                    // an INCR task that declares a txnId holds it locked across its runs, so it must impose an order on
                    // the keys it also holds across them: production rejects UNSEQUENCED outright (see
                    // requireSequencedIfHoldsLocksBetweenRuns), and isUnsequenced(entry) asserts the combination away, so
                    // there is nothing here to enumerate
                    if (kind == Kind.BAG && primary != null)
                        throw new Unreachable("an UNSEQUENCED INCR task may not declare a txnId");
                    // an INCR task holds its txnId across its runs
                    task.setIncrementalExclusive();
                }
            }

            int keys = 0;
            for (AccordCacheEntry<?, ?, ?> entry : refs)
            {
                if (entry.isCommandsForKey())
                    ++keys;
            }
            // keys is the count declared at setup, but the refs are added as the task joins each entry:
            // waitToRunExclusive requires every ref to already hold a position
            set(task, "keys", keys);
            task.setCacheQueuedExclusive();

            if (kind == Kind.FIFO)
            {
                task.setSequencedExclusive(ExecutionContext.ExecutionSequence.ATOMIC);
                // the fifo region is ordered by fifoAt, stamped at setup for an ATOMIC task; an incremental task is
                // stamped later, by prepareExclusiveMayThrow, so it sorts behind everything already fifo
                task.setCacheQueuedFifoExclusive();
            }
            else if (kind == Kind.SORTED)
            {
                task.setSequencedExclusive(ExecutionContext.ExecutionSequence.BY_PRIORITY);
            }
            // BAG is the default: unsequenced

            // WAITING_ON_KEY means enqueued on the waiting queue: the state bits and the queue bits must agree, or the
            // first transition trips Task.unqueue's check
            task.unsafeSetStateExclusive(State.WAITING_ON_KEY);
            store.executor().waiting.enqueue(task);
            kinds.put(task, kind);
            declared.put(task, new ArrayList<>(Arrays.asList(refs)));
            if (refs.length > 0 && !refs[0].isCommandsForKey())
                declaredTxn.put(task, refs[0]);
            return task;
        }

        /**
         * How a claim is really placed: {@code completeSetupOfLoaded} takes the fifo position at setup, and everything
         * else is placed by {@code waitOnKeysExclusive} through {@code ensureCacheQueued}.
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        void addRef(SafeTask<?> task, AccordCacheEntry<?, ?, ?> entry)
        {
            if (task.refs.containsKey(entry.key()))
                return;
            // real ones: SaferState.global switches on exact class, and preExecute locks through them
            ((Map) task.refs).put(entry.key(), entry.isCommandsForKey() ? new SaferCommandsForKey((AccordCacheEntry) entry)
                                                                       : new SaferCommand((AccordCacheEntry) entry));
        }

        /**
         * Stamp the {@code fifoAt} the fifo region is ordered by, if this task does not have one yet.
         * <p>
         * Production stamps at the instant a task becomes a fifo claim: at setup for an ATOMIC task, whose claim pass
         * immediately follows, and at first prepare for an incremental one (which {@code prepareExclusiveMayThrow} does
         * itself). This harness creates its tasks up front and then interleaves their claims, so stamping at construction
         * would order an ATOMIC task ahead of one that upgraded on start before the ATOMIC task's pass began.
         */
        void ensureFifoAt(SafeTask<?> task)
        {
            if (task.fifoAt == 0)
                task.fifoAt = uniqueCreatedAt.incrementAndGet();
        }

        void place(SafeTask<?> task, AccordCacheEntry<?, ?, ?> entry)
        {
            if (entry.isCommandsForKey() && !task.isCacheQueuedFifo())
            {
                // waitOnTxnsExclusive claims the keys alongside the txnIds, so a task blocked on a txnId does hold its
                // keys; it must still have claimed the txnId first
                AccordCacheEntry<?, ?, ?> txn = declaredTxn.get(task);
                if (txn != null && !task.refs.containsKey(txn.key()))
                    throw new Unreachable("keys are claimed after txnIds unless the task is ATOMIC");
            }

            addRef(task, entry);
            RunnableStatus status;
            if (entry.isLoading())
            {
                // a fifo claim still takes its position, everyone else is bagged until the drain
                if (task.isCacheQueuedFifo()) { ensureFifoAt(task); entry.addFifo(task); }
                else entry.addWaitingToLoad(task);
                // setup counts a load as a wait, which stops a sync task preparing against an entry not yet loaded. A
                // txnId is counted in the high bits of waitingFor, and counts for a batched task too; a key is counted in
                // the low bits, and only a sync task counts it.
                if (!entry.isCommandsForKey())
                    set(task, "waitingFor", task.waitingFor + SafeTask.WAITING_FOR_TXN_INCR);
                else if (task.isSync())
                    set(task, "waitingFor", task.waitingFor + 1);
                return;
            }
            // completeSetupOfLoaded takes an ATOMIC task's fifo position, and counts the key as loaded, but does not do
            // the batch bookkeeping; queueOnKeysExclusive supplies that afterwards, and for a fifo claim its
            // ensureCacheQueued is a *query* (statusOfPresent) precisely because the position is already held. Both
            // halves must run here, or the task reaches its first prepare leading every key and knowing none of them.
            if (task.isCacheQueuedFifo() && !entry.contains(task))
            {
                ensureFifoAt(task);
                entry.addFifo(task);
                if (!task.isSync())
                    task.nonSync().addLoaded();
                status = task.ensureCacheQueued(entry);
            }
            else
            {
                status = task.ensureCacheQueued(entry);
            }
            recordHead(task, entry, status);
            settle(task, entry, status);
            maybeReadyToRun(task);
        }

        /**
         * Once a sync task leads everything it declared it belongs in the run queue, which
         * {@code incrementWaitingKeys} asserts. Production places every ref before settling; this harness places them
         * one at a time, so it settles here.
         */
        void maybeReadyToRun(SafeTask<?> task)
        {
            if (!task.isSync() || task.waitingFor != 0 || task.is(State.WAITING_TO_RUN))
                return;
            for (AccordCacheEntry<?, ?, ?> entry : declared.getOrDefault(task, Collections.emptyList()))
            {
                if (!task.refs.containsKey(entry.key()))
                    return; // still claiming
            }
            task.unqueue(store.executor().waiting);
            task.waitToRunExclusive();
        }

        /**
         * The state machine around a placement, as waitOnTxnsExclusive and continueWaitingOnKeysExclusive drive it: a
         * txnId we do not lead is a wait we must count, and counting it moves us to WAITING_ON_TXN. Mirrors the
         * transitions only; the placement itself is production code.
         */
        void settle(SafeTask<?> task, AccordCacheEntry<?, ?, ?> entry, RunnableStatus status)
        {
            if (status != RunnableStatus.NOT_RUNNABLE)
                return;

            // for a sync task a key it does not lead is counted exactly as a txnId is; a non-sync task tracks keys
            // through its blocking/notBlocking sets instead
            if (entry.isCommandsForKey())
            {
                if (task.isSync())
                {
                    if (task.is(State.WAITING_TO_RUN))
                    {
                        task.unqueue(store.exclusiveExecutor());
                        task.unsafeSetStateExclusive(State.WAITING_ON_KEY);
                        store.executor().waiting.enqueue(task);
                    }
                    set(task, "waitingFor", task.waitingFor + 1);
                }
                return;
            }

            if (task.is(State.WAITING_TO_RUN))
            {
                task.unqueue(store.exclusiveExecutor());
                task.unsafeSetStateExclusive(State.WAITING_ON_TXN);
                store.executor().waiting.enqueue(task);
            }
            else if (!task.is(State.WAITING_ON_TXN))
            {
                task.unsafeSetStateExclusive(State.WAITING_ON_TXN);
            }
            set(task, "waitingFor", task.waitingFor + SafeTask.WAITING_FOR_TXN_INCR);
        }

        /**
         * The batch bookkeeping addQueuedOptionalKey does with the status: which keys this task currently leads. Calling
         * it directly would also drive state transitions through the executor queues.
         */
        void recordHead(SafeTask<?> task, AccordCacheEntry<?, ?, ?> entry, RunnableStatus status)
        {
            if (!entry.isCommandsForKey() || status == null || task.isSync())
                return;
            switch (status)
            {
                case NEWLY_RUNNABLE:
                case STILL_RUNNABLE:
                    task.nonSync().onNewHead(entry);
                    break;
                case NEWLY_BLOCKING_RUNNABLE:
                case STILL_RUNNABLE_NEWLY_BLOCKING:
                    task.nonSync().onNewBlockingHead(entry);
                    break;
                default:
                    break;
            }
        }

        /**
         * A sync task's prepare: the real thing, which locks every commandsForKey ref it holds with RELEASE_QUEUE and then
         * does its txnIds. Each of those locks removes it from a queue and promotes whoever was behind, which re-enters
         * the queues while the loop still has keys to lock.
         */
        void prepareSync(SafeTask<?> task, AccordCacheEntry<?, ?, ?>... declared)
        {
            // a task prepares only once it has claimed every ref it declared: setup runs to completion before
            // waitOnTxns/waitOnKeys, and prepare after those
            for (AccordCacheEntry<?, ?, ?> entry : declared)
            {
                if (!task.refs.containsKey(entry.key()))
                    throw new Unreachable("a task prepares only after claiming every entry it declared");
            }
            if (task.waitingFor != 0)
                return; // does not lead everything yet, so production would leave it waiting

            if (!task.is(State.WAITING_TO_RUN))
            {
                task.unqueue(store.executor().waiting);
                task.waitToRunExclusive();
            }
            task.prepareExclusiveMayThrow();
            releaseLocks(task);
        }

        /** what the cache's release does at the end of a run: give back the lock, and the position with it */
        void releaseLocks(SafeTask<?> task)
        {
            for (AccordCacheEntry<?, ?, ?> entry : new ArrayList<>(entries))
            {
                if (!entry.isLockedBy(task))
                    continue;
                entry.remove(task, true, null);
                task.refs.remove(entry.key());
                heldBy.remove(entry, task);
            }
        }

        /** the real first prepare: reposition, take the txnId with HOLD_QUEUE, become started */
        void firstRun(SafeTask<?> task)
        {
            // a task never prepares holding only part of what it declared
            for (AccordCacheEntry<?, ?, ?> entry : declared.getOrDefault(task, Collections.emptyList()))
            {
                if (!task.refs.containsKey(entry.key()))
                    throw new Unreachable("a task prepares only after claiming every entry it declared");
            }
            if (task.waitingFor != 0)
            {
                // still blocked on a txnId, so production would not let it prepare. Not Unreachable: the rest of the
                // scenario is still exercised, but counted so that "the run never happened" is visible rather than
                // silently folded into the passing count.
                ++blockedAtFirstRun;
                return;
            }

            if (!task.is(State.WAITING_TO_RUN))
            {
                task.unqueue(store.executor().waiting);
                task.waitToRunExclusive();
            }
            task.prepareExclusiveMayThrow();
            for (AccordCacheEntry<?, ?, ?> entry : entries)
            {
                if (entry.isLockedBy(task) && entry.isLockedHoldingQueue())
                    heldBy.put(entry, task);
            }
        }

        /**
         * What {@code RangeTxnAndKeyScanner.KeyWatcher} does: adopt a reference to a key the task never declared, and
         * place it if the task's state requires it. The real {@code SafeTask.adoptCachedKeyExclusive} does all of it;
         * this method is only the reachability filter {@code reference}'s relevance checks and the watcher's lifetime
         * impose, so a scenario production cannot reach is counted rather than manufactured into a failure.
         */
        void adoptKey(SafeTask<?> task, AccordCacheEntry<?, ?, ?> entry)
        {
            if (task.isCacheQueuedFifo())
                throw new Unreachable("a fifo task never has a live KeyWatcher: ATOMIC over ranges implies an inherited "
                                      + "range scan, and the INCR upgrade happens after finish() unregisters it");
            if (task.refs.containsKey(entry.key()))
                throw new Unreachable("reference() returns early for a key we already hold");
            if (entry.isLoading())
                throw new Unreachable("reference() returns early for a WAITING_TO_LOAD or LOADING entry");
            if (task.compareTo(State.PREPARING) >= 0)
                throw new Unreachable("the watcher is unregistered by finish(), at the top of prepareExclusiveMayThrow");
            // the same rule place() encodes: a task that has not claimed its txnId has not reached WAITING_ON_KEY and
            // cannot place a key, adopted or declared (this harness parks every task in WAITING_ON_KEY from creation)
            AccordCacheEntry<?, ?, ?> txn = declaredTxn.get(task);
            if (txn != null && !task.refs.containsKey(txn.key()))
                throw new Unreachable("keys - adopted or declared - are claimed after txnIds");
            // ...and a sync task in WAITING_ON_KEY has at least one outstanding wait by construction
            if (task.isSync() && task.is(State.WAITING_ON_KEY) && task.waitingFor == 0)
                throw new Unreachable("a sync task with no outstanding wait is WAITING_TO_RUN, not WAITING_ON_KEY");

            declared.get(task).add(entry);
            adoptedIn.add(task.state());
            task.addCachedKeyExclusive(entry, new SaferCommandsForKey((AccordCacheEntry) entry));
            ++adoptions;
        }

        /** AccordExecutor.onLoadedExclusive: drain, mark loaded, re-add in compareForNotify order */
        void completeLoad(AccordCacheEntry<?, ?, ?> entry)
        {
            if (!entry.isLoading())
                return;

            List<SafeTask<?>> drained = new ArrayList<>();
            try (BufferList<SafeTask<?>> tasks = entry.drainWaitingToLoad())
            {
                entry.unsafeSetStatus(Status.LOADED);
                tasks.sort(AccordCacheEntryQueue::compareForNotify);
                drained.addAll(tasks);
            }
            for (SafeTask<?> task : drained)
            {
                // onLoadOneExclusive: the load we were waiting for has arrived, decremented in the half of waitingFor
                // that setup counted it in
                if (!entry.isCommandsForKey())
                {
                    if (task.waitingForTxnCount() > 0)
                        set(task, "waitingFor", task.waitingFor - SafeTask.WAITING_FOR_TXN_INCR);
                }
                else if (task.isSync() && task.waitingForKeyCount() > 0)
                {
                    set(task, "waitingFor", task.waitingFor - 1);
                }
                // ...and re-place the claim only in the states production re-places it in: a task below WAITING_ON_TXN
                // is not re-placed, and notifying one is forbidden by onChangeKeyRunnableStatus
                if (task.compareTo(State.WAITING_ON_TXN) >= 0)
                {
                    // as place(): the status the re-placed claim reports is both batch bookkeeping and, for a sync task,
                    // a wait to count. Production converts the load wait we just dropped into a queue wait in
                    // queueOnKeysExclusive, which counts every key it does not lead; recordHead alone is a no-op for a
                    // sync task, so without settle the wait is simply lost and the next promotion decrements from zero.
                    RunnableStatus status = task.ensureCacheQueued(entry);
                    recordHead(task, entry, status);
                    settle(task, entry, status);
                }
                maybeReadyToRun(task);
            }
        }

        /*
         * There is no "the lock holder leads" invariant to assert on a Command entry, though it looks like one: a
         * HOLD_QUEUE holder keeps a fifo position, but a task that took the lock with RELEASE_QUEUE gave its position up
         * entirely and is not a queue member at all.
         */

        /**
         * A task that has begun running and holds a txnId locked across its runs holds something here may be waiting for,
         * so nothing that has not begun may be ahead of it. Two parts, both consequences of the upgrade at first prepare:
         * <ol>
         *   <li>such a started task is a fifo claim on every entry it holds - it is upgraded on its first prepare and
         *       never reverts, so it can never be found in the sorted region or the bag;</li>
         *   <li>and it therefore precedes every sorted or bagged claim. The only un-started task that can be ahead of it
         *       is another fifo claim, whose order within the region is by fifoAt, which nothing reorders.</li>
         * </ol>
         * A started task that holds no lock between runs is excluded: it is deliberately left in its declared region, as
         * nothing waits for it across runs, so neither part applies to it.
         * <p>
         * Neither part is conditional on anything being blocked, so both are checked on every entry after every step.
         */
        void assertStartedLeads()
        {
            for (AccordCacheEntry<?, ?, ?> entry : entries)
            {
                if (entry.isLoading())
                    continue;

                List<SafeTask<?>> queued = entry.unsafeQueuedTasks();
                int runnable = entry.unsafeRunnablePrefix(), fifoSize = entry.unsafeFifoSize();

                // (1) started implies fifo, for a task that holds a lock between runs: the fifo claims lead the list
                for (int i = fifoSize ; i < queued.size() ; ++i)
                {
                    if (hasStartedHoldingLocks(queued.get(i)))
                        throw new AssertionError("T4: started " + queued.get(i) + " holds a non-fifo position on "
                                                 + entry.key() + ": " + queued);
                }

                boolean prefixHasNotStarted = false;
                for (int i = 0 ; i < runnable ; ++i)
                    prefixHasNotStarted |= !hasStartedHoldingLocks(queued.get(i));
                if (!prefixHasNotStarted)
                    continue;

                // (2) and precedes anything un-started, unless both are fifo claims
                for (int i = runnable ; i < queued.size() ; ++i)
                {
                    if (hasStartedHoldingLocks(queued.get(i)) && i >= fifoSize)
                        throw new AssertionError("T4: started " + queued.get(i) + " waits for one that has not, on "
                                                 + entry.key() + ": " + queued);
                }
            }
        }

        /** T4 covers only tasks the upgrade at first prepare applies to, which is those holding a txnId across runs */
        private static boolean hasStartedHoldingLocks(SafeTask<?> task)
        {
            return task.hasIncrementalStarted() && task.holdsLocksBetweenRuns();
        }

        /** a task queued twice waits for itself, and validate() cannot see it */
        void assertNoDuplicatePositions()
        {
            for (AccordCacheEntry<?, ?, ?> entry : entries)
            {
                List<SafeTask<?>> queued = entry.unsafeQueuedTasks();
                for (int i = 0 ; i < queued.size() ; ++i)
                {
                    for (int j = i + 1 ; j < queued.size() ; ++j)
                    {
                        if (queued.get(i) == queued.get(j))
                            throw new AssertionError("S2: " + queued.get(i) + " holds two positions on " + entry.key() + ": " + queued);
                    }
                }
            }
        }

        void assertNoCycle()
        {
            Map<SafeTask<?>, List<SafeTask<?>>> waitsFor = new IdentityHashMap<>();
            for (AccordCacheEntry<?, ?, ?> entry : entries)
            {
                if (entry.isLoading())
                    continue; // everyone here waits for the load, which completes on its own

                List<SafeTask<?>> queued = entry.unsafeQueuedTasks();
                int runnable = entry.unsafeRunnablePrefix();
                SafeTask<?> holder = heldBy.get(entry);
                for (int i = 0 ; i < queued.size() ; ++i)
                {
                    SafeTask<?> waiter = queued.get(i);
                    List<SafeTask<?>> targets = waitsFor.computeIfAbsent(waiter, ignore -> new ArrayList<>());
                    if (holder != null && holder != waiter)
                        targets.add(holder);
                    if (i >= runnable)
                    {
                        for (int j = 0 ; j < runnable ; ++j)
                            targets.add(queued.get(j));
                    }
                }
            }

            List<SafeTask<?>> cycle = findCycle(waitsFor);
            if (cycle != null)
                throw new AssertionError("wait cycle: " + cycle);
        }
    }

    private static List<SafeTask<?>> findCycle(Map<SafeTask<?>, List<SafeTask<?>>> waitsFor)
    {
        Map<SafeTask<?>, Integer> state = new IdentityHashMap<>();
        Deque<SafeTask<?>> path = new ArrayDeque<>();
        for (SafeTask<?> from : waitsFor.keySet())
        {
            List<SafeTask<?>> cycle = findCycle(from, waitsFor, state, path);
            if (cycle != null)
                return cycle;
        }
        return null;
    }

    private static List<SafeTask<?>> findCycle(SafeTask<?> at, Map<SafeTask<?>, List<SafeTask<?>>> waitsFor,
                                               Map<SafeTask<?>, Integer> state, Deque<SafeTask<?>> path)
    {
        Integer seen = state.get(at);
        if (seen != null && seen == 2)
            return null;
        if (seen != null && seen == 1)
        {
            List<SafeTask<?>> cycle = new ArrayList<>(path);
            cycle.add(at);
            return cycle;
        }

        state.put(at, 1);
        path.addLast(at);
        for (SafeTask<?> next : waitsFor.getOrDefault(at, Collections.emptyList()))
        {
            List<SafeTask<?>> cycle = findCycle(next, waitsFor, state, path);
            if (cycle != null)
                return cycle;
        }
        path.removeLast();
        state.put(at, 2);
        return null;
    }

    private static void set(Object target, String name, Object value)
    {
        try
        {
            for (Class<?> k = target.getClass() ; k != null ; k = k.getSuperclass())
            {
                try
                {
                    Field f = k.getDeclaredField(name);
                    f.setAccessible(true);
                    f.set(target, value);
                    return;
                }
                catch (NoSuchFieldException ignore) { }
            }
            throw new NoSuchFieldException(name);
        }
        catch (Throwable t)
        {
            throw new RuntimeException("could not set " + name, t);
        }
    }

    /**
     * {@code core} with each of {@code extras} inserted at every position, independently of the others, so that a second
     * extra can land on either side of the first: one extra gives {@code n+1} orderings, two give {@code (n+1)(n+2)}.
     */
    private static List<List<Event>> splice(List<Event> core, Event[] extras)
    {
        List<List<Event>> result = new ArrayList<>();
        result.add(new ArrayList<>(core));
        for (Event extra : extras)
        {
            List<List<Event>> next = new ArrayList<>();
            for (List<Event> soFar : result)
            {
                for (int at = 0 ; at <= soFar.size() ; ++at)
                {
                    List<Event> with = new ArrayList<>(soFar);
                    with.add(at, extra);
                    next.add(with);
                }
            }
            result = next;
        }
        return result;
    }

    private static List<List<Event>> permutations(Event[] events)
    {
        List<List<Event>> result = new ArrayList<>();
        permute(new ArrayList<>(Arrays.asList(events)), 0, result);
        return result;
    }

    private static void permute(List<Event> events, int from, List<List<Event>> result)
    {
        if (from == events.size())
        {
            result.add(new ArrayList<>(events));
            return;
        }
        for (int i = from ; i < events.size() ; ++i)
        {
            Collections.swap(events, from, i);
            permute(events, from + 1, result);
            Collections.swap(events, from, i);
        }
    }
}
