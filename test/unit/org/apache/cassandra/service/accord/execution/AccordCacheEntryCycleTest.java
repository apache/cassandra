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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.RoutingKey;
import accord.local.ExecutionContext;
import accord.local.ExecutionContext.ExecutionKind;
import accord.local.Node.Id;
import accord.local.SafeState;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers.BufferList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test has been authored entirely by Claude.
 *
 * Exhaustive test of deadlock freedom in the cache-entry queues, over real {@link AccordCacheEntry}s and real
 * {@link AccordCacheEntryQueue}s.
 *
 * <p>A task waits for another for exactly two reasons, and a deadlock is a cycle in the union of the two:
 * <ul>
 *   <li><b>a lock edge</b>: the entry is held across runs with {@code HOLD_QUEUE}, so everything holding a position on
 *       it waits for the holder. {@code RELEASE_QUEUE} and {@code UNQUEUED} are given back within the run that took
 *       them, so produce no edge;</li>
 *   <li><b>a position edge</b>: the task is not in the entry's runnable prefix (the fifo head, else the sorted head,
 *       else the whole bag), so it waits for whoever is.</li>
 * </ul>
 *
 * <p>The smallest deadlock is two tasks and two entries: {@code H} holds a txnId across its runs and {@code W} queues
 * behind it there, while on a key they share {@code W} sits ahead of {@code H}. Neither can give up what the other needs.
 * This "direct cycle" is prevented by <b>upgrade-on-start</b>: when {@code H} begins running it becomes a fifo claim on
 * every entry it holds, and fifo precedes sorted precedes bag, so {@code W} can only be ahead of it if {@code W} is
 * itself a fifo claim. An ATOMIC {@code W} claims every entry it declared in one uninterrupted pass, so it cannot get a
 * key ahead of {@code H} and the txnId behind it - which is why {@link #isLegalOrder} rejects interleavings that split
 * that pass.
 *
 * <p>The bugs this catches are ordering bugs, so rather than assert one interleaving it enumerates them: for each region
 * {@code H} and {@code W} may occupy on the shared key, for each order in which the four events can occur, and with and
 * without an unrelated task already on the key. After each run it asserts the two properties above on the real queues.
 */
public class AccordCacheEntryCycleTest
{
    /**
     * {@code AccordCacheEntry.remove} reads {@code AccordExecutor.CACHE_QUEUES_ENABLED}, which is assigned from config in
     * the executor's static initialiser, so without this the first removal fails with an {@code ExceptionInInitializerError}
     * rather than anything to do with the queues.
     */
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /** which region of a key's queue a task occupies; the queue orders fifo before sorted before bag */
    enum Region { FIFO, SORTED, BAG }

    /**
     * The same enumeration, but the shared key starts out loading and completes at every point in the sequence. While an
     * entry loads nothing is runnable and every non-fifo claim is bagged regardless of its kind, so the regions the
     * tasks end up in are not decided until the drain - by {@code compareForNotify}, which knows nothing about locks.
     * A cycle that the loaded-entry enumeration prevents may therefore survive across a load.
     */
    @Test
    public void everyDirectCycleIsBrokenAcrossALoad()
    {
        assertEveryScenario(Event.KEY_LOAD_COMPLETES, true);
    }

    /**
     * The same enumeration, but H completes a round of work at every point in the sequence. That takes a key it leads
     * with {@code RELEASE_QUEUE}, giving up the position and promoting whoever was behind it, which re-enters the queues
     * from a place none of the other events reach.
     */
    @Test
    public void everyDirectCycleIsBrokenAfterAProcessedRound()
    {
        assertEveryScenario(Event.H_PROCESSES_OTHER_KEY, false);
    }

    /**
     * Runs {@link #everyDirectCycleIsBroken}'s enumeration with one extra lifecycle event spliced in at every position,
     * including omitted, so that the extra step is tried before, between and after each of the four events that build the
     * cycle.
     */
    private void assertEveryScenario(Event extra, boolean keyStartsLoading)
    {
        List<String> failures = new ArrayList<>();
        int scenarios = 0, rejected = 0;
        for (Region holderRegion : Region.values())
        {
            for (Region waiterRegion : Region.values())
            {
                for (List<Event> core : permutations(CORE_EVENTS))
                {
                    if (!isLegalOrder(core, holderRegion, waiterRegion))
                    {
                        ++rejected;
                        continue;
                    }

                    for (int at = 0 ; at <= core.size() ; ++at)
                    {
                        List<Event> order = new ArrayList<>(core);
                        order.add(at, extra);

                        for (boolean preBlocked : new boolean[]{ false, true })
                        {
                            ++scenarios;
                            String desc = "H=" + holderRegion + " W=" + waiterRegion + " order=" + order
                                          + " preBlocked=" + preBlocked + " loading=" + keyStartsLoading;
                            try
                            {
                                runScenario(holderRegion, waiterRegion, order, preBlocked, keyStartsLoading);
                            }
                            catch (Throwable t)
                            {
                                failures.add(desc + "\n      " + t);
                            }
                        }
                    }
                }
            }
        }

        if (!failures.isEmpty())
            fail(failures.size() + " of " + scenarios + " scenarios failed:\n  " + String.join("\n  ", failures));
        System.out.println(getClass().getSimpleName() + ": " + scenarios + " scenarios checked, " + rejected
                           + " orderings rejected as unreachable");
        // anti-vacuity: isLegalOrder rejects most interleavings, and if it ever rejected all of them this enumeration
        // would pass having run nothing. The same principle as the coverage probes in the specification.
        if (scenarios * 8 <= rejected)
            fail("only " + scenarios + " scenarios ran against " + rejected + " rejected orderings: isLegalOrder is"
                 + " rejecting nearly everything, so this enumeration checks nearly nothing");
    }

    /**
     * The claim-ordering rules production enforces, which decide whether an interleaving is reachable at all:
     * <ul>
     *   <li>H must hold the txnId before anyone can queue behind it, and {@code lockExclusive} requires the locker to
     *       lead - so H may lock before W joins the txnId, but not after. For an ATOMIC W the txnId is claimed together
     *       with the key, so it is {@code W_JOINS_KEY} that must come after the lock;</li>
     *   <li>a task that is not ATOMIC claims its txnIds strictly before its keys: {@code waitOnKeysExclusive} runs only
     *       once {@code waitOnTxnsExclusive} has left it leading every txnId it declared;</li>
     *   <li><b>and it claims every entry it declared before it runs at all.</b> {@code waitToRunExclusive} follows the last
     *       key placement and {@code prepareExclusive} follows that, so H cannot begin a run and then pick up a key -
     *       which is the only remaining way for a started H to arrive at a key an ATOMIC W already leads.</li>
     * </ul>
     * An ATOMIC task's claims are excluded from all of this because it takes them in one uninterrupted pass at setup
     * (see {@code runScenario}), so nothing can be interleaved with them.
     */
    private static boolean isLegalOrder(List<Event> order, Region holderRegion, Region waiterRegion)
    {
        boolean wClaimsAtSetup = waiterRegion == Region.FIFO;
        int wJoinsTxn = order.indexOf(wClaimsAtSetup ? Event.W_JOINS_KEY : Event.W_JOINS_TXN);
        if (order.indexOf(Event.H_LOCKS_TXN) > wJoinsTxn)
            return false;
        if (!wClaimsAtSetup && order.indexOf(Event.W_JOINS_TXN) > order.indexOf(Event.W_JOINS_KEY))
            return false;
        return holderRegion == Region.FIFO || order.indexOf(Event.H_JOINS_KEY) < order.indexOf(Event.H_LOCKS_TXN);
    }

    private static final Event[] CORE_EVENTS = { Event.H_JOINS_KEY, Event.H_LOCKS_TXN, Event.W_JOINS_TXN, Event.W_JOINS_KEY };

    /**
     * The four things that have to happen for the cycle to exist, plus the lifecycle steps interleaved with them.
     * <ul>
     *   <li>{@link #KEY_LOAD_COMPLETES} - the shared key was loading, and now drains and re-adds everyone. Claims made
     *       while it loaded were bagged, so this is where their final regions are decided, by {@code compareForNotify}
     *       rather than by any of the cycle avoidance.</li>
     *   <li>{@link #H_PROCESSES_OTHER_KEY} - H finishes a round against a key it leads with {@code RELEASE_QUEUE},
     *       promoting whoever is next. That promotion re-enters the queues from a place nothing else here reaches.
     *       Note the txnId is not unlocked between rounds: HOLD_QUEUE is held to completion.</li>
     * </ul>
     */
    enum Event { H_JOINS_KEY, H_LOCKS_TXN, W_JOINS_TXN, W_JOINS_KEY, KEY_LOAD_COMPLETES, H_PROCESSES_OTHER_KEY }

    private static int nextId = 0;
    private static long nextPosition = 1;
    /** the stamp the fifo region is ordered by, taken when a task becomes a fifo claim */
    private static long nextFifoAt = 1;

    @Test
    public void everyDirectCycleIsBroken()
    {
        List<String> failures = new ArrayList<>();
        int scenarios = 0, rejected = 0;
        for (Region holderRegion : Region.values())
        {
            for (Region waiterRegion : Region.values())
            {
                for (List<Event> order : permutations(CORE_EVENTS))
                {
                    if (!isLegalOrder(order, holderRegion, waiterRegion))
                    {
                        ++rejected;
                        continue;
                    }

                    for (boolean preBlocked : new boolean[]{ false, true })
                    {
                        ++scenarios;
                        String desc = "H=" + holderRegion + " W=" + waiterRegion + " order=" + order + " preBlocked=" + preBlocked;
                        try
                        {
                            runScenario(holderRegion, waiterRegion, order, preBlocked);
                        }
                        catch (Throwable t)
                        {
                            failures.add(desc + "\n      " + t);
                        }
                    }
                }
            }
        }

        if (!failures.isEmpty())
            fail(failures.size() + " of " + scenarios + " cycle scenarios failed:\n  " + String.join("\n  ", failures));
        System.out.println(getClass().getSimpleName() + ": direct cycles, " + scenarios + " checked, " + rejected
                           + " orderings rejected as unreachable");
        if (scenarios * 8 <= rejected)
            fail("only " + scenarios + " scenarios ran against " + rejected + " rejected orderings: isLegalOrder is"
                 + " rejecting nearly everything, so this enumeration checks nearly nothing");
    }

    /**
     * A three-task stall whose shape is not a ring of pairwise sharing: the middle task <em>leads</em> the entry it
     * shares with the third, so the third is blocked without ever being the first blocked member anywhere.
     *
     * <pre>
     *   A (BY_PRIORITY, sorted)  waits for B on k3   - B's fifo claim is ahead of A's sorted position
     *   B (ATOMIC, fifo)         waits for C on txn  - C holds the lock across its runs
     *   C (UNSEQUENCED, started) waits for A on k1   - A's sorted position is ahead of C's bag position
     * </pre>
     *
     * With upgrade-on-start, C takes a fifo position on k1 the moment it begins running, so A's later sorted claim
     * lands behind it and the edge C -> A never exists.
     */
    @Test
    public void threeTaskCycleThroughANonLeadingWaiter()
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> txn = world.commandEntry();
        AccordCacheEntry<?, ?, ?> k1 = world.keyEntry();
        AccordCacheEntry<?, ?, ?> k3 = world.keyEntry();
        AccordCacheEntry<?, ?, ?> k4 = world.keyEntry();
        AccordCacheEntry<?, ?, ?> k5 = world.keyEntry();

        // C holds the txnId across its runs
        Task c = world.task("C", 10, Region.BAG);
        Task b = world.task("B", 20, Region.FIFO);
        Task a = world.task("A", 30, Region.SORTED);

        world.join(c, txn);
        world.join(c, k1);
        world.join(c, k3);
        world.join(c, k4);
        world.join(c, k5);
        // C must take the lock before B joins the txnId: B is ATOMIC, so its fifo claim would lead the txnId, and
        // lockExclusive requires the locker to lead. So this shape can only be built with C starting first.
        world.beginRun(c, txn);

        world.join(b, txn);   // queues behind C's lock, and is flagged for it
        world.join(b, k3);    // a fifo claim, so it leads k3
        world.join(b, k4);    // ...and a second key it shares only with C, as in the production shape

        world.join(a, k1);    // sorted, ahead of C's bag position - this is the edge that must be broken
        world.join(a, k3);    // sorted, behind B's fifo claim

        List<SafeTask<?>> queue = k1.unsafeQueuedTasks();
        int runnable = k1.unsafeRunnablePrefix();
        boolean canRun = false;
        for (int i = 0 ; i < runnable && i < queue.size() ; ++i)
            canRun |= queue.get(i) == c.safeTask;

        // C leads k1 because it took a fifo position there before A ever arrived
        if (!canRun)
            fail("C is not runnable on k1, so the cycle stands: k1=" + queue + " runnablePrefix=" + runnable + ")");
    }


    /** the joins that can be interleaved once C holds its lock */
    private enum Late { B_JOIN_TXN, B_JOIN_K3, A_JOIN_K1, A_JOIN_K3, C_PROCESSES_K4 }

    /**
     * The topology above, enumerated. Varied here:
     * <ul>
     *   <li>whether A and B are incremental - only an INCR task is upgraded to a fifo claim when it starts, so a
     *       non-incremental blocker can never move;</li>
     *   <li>every interleaving of the four joins that happen after C takes its lock;</li>
     *   <li>whether C claims k1 before or after k3.</li>
     * </ul>
     * C must lock before B joins the txnId, because B is ATOMIC and its fifo claim would lead it - lockExclusive requires
     * the locker to lead - so that one ordering is fixed rather than enumerated.
     */
    @Test
    public void everyNonLeadingWaiterCycleIsBroken()
    {
        List<String> failures = new ArrayList<>();
        int scenarios = 0;
        for (boolean aIncr : new boolean[]{ true, false })
        {
            for (boolean bIncr : new boolean[]{ true, false })
            {
                for (boolean cK1First : new boolean[]{ true, false })
                {
                    for (List<Late> order : latePermutations())
                    {
                        ++scenarios;
                        String desc = "aIncr=" + aIncr + " bIncr=" + bIncr + " cK1First=" + cK1First + " order=" + order;
                        try
                        {
                            runNonLeadingWaiter(aIncr, bIncr, cK1First, order);
                        }
                        catch (Unreachable skip)
                        {
                            --scenarios;
                        }
                        catch (Throwable t)
                        {
                            failures.add(desc + "\n      " + t);
                        }
                    }
                }
            }
        }
        System.out.println(getClass().getSimpleName() + ": non-leading-waiter cycles, " + scenarios + " checked");
        if (!failures.isEmpty())
            fail(failures.size() + " of " + scenarios + " failed:\n  " + String.join("\n  ", failures));
        if (scenarios == 0)
            fail("every non-leading-waiter scenario was Unreachable, so this enumeration checked nothing");
    }

    private static class Unreachable extends RuntimeException
    {
        Unreachable(String why) { super(why, null, false, false); }
    }

    private static List<List<Late>> latePermutations()
    {
        List<List<Late>> result = new ArrayList<>();
        permute(new ArrayList<>(Arrays.asList(Late.values())), new ArrayList<>(), result);
        return result;
    }

    private static void permute(List<Late> remaining, List<Late> prefix, List<List<Late>> out)
    {
        if (remaining.isEmpty()) { out.add(new ArrayList<>(prefix)); return; }
        for (int i = 0 ; i < remaining.size() ; ++i)
        {
            Late next = remaining.remove(i);
            prefix.add(next);
            permute(remaining, prefix, out);
            prefix.remove(prefix.size() - 1);
            remaining.add(i, next);
        }
    }

    private void runNonLeadingWaiter(boolean aIncr, boolean bIncr, boolean cK1First, List<Late> order)
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> txn = world.commandEntry();
        AccordCacheEntry<?, ?, ?> k1 = world.keyEntry();
        AccordCacheEntry<?, ?, ?> k3 = world.keyEntry();
        AccordCacheEntry<?, ?, ?> k4 = world.keyEntry();

        Task c = world.task("C", 10, Region.BAG, true);
        Task b = world.task("B", 20, Region.FIFO, bIncr);
        Task a = world.task("A", 30, Region.SORTED, aIncr);

        world.join(c, txn);
        if (cK1First) { world.join(c, k1); world.join(c, k3); }
        else          { world.join(c, k3); world.join(c, k1); }
        world.join(c, k4);
        world.beginRun(c, txn);

        for (Late late : order)
        {
            switch (late)
            {
                case B_JOIN_TXN: world.join(b, txn); break;
                case B_JOIN_K3:  world.join(b, k3); break;
                case A_JOIN_K1:  world.join(a, k1); break;
                case A_JOIN_K3:  world.join(a, k3); break;
                // C completes a round against a key it leads, giving up and re-taking positions mid-cycle
                case C_PROCESSES_K4: world.processRound(c, k4); break;
            }
        }
        world.join(b, k4);

        // C holds the lock everyone else is waiting behind, so it must be able to run: its upgrade to a fifo claim on k1
        // must put it ahead of A there, whichever way round the joins happened
        List<SafeTask<?>> queue = k1.unsafeQueuedTasks();
        int runnable = k1.unsafeRunnablePrefix();
        boolean canRun = false;
        for (int i = 0 ; i < runnable && i < queue.size() ; ++i)
            canRun |= queue.get(i) == c.safeTask;
        if (!canRun)
            throw new AssertionError("C cannot run on k1: queue=" + queue + " runnable=" + runnable);
    }

    private void runScenario(Region holderRegion, Region waiterRegion, List<Event> order, boolean preBlocked)
    {
        runScenario(holderRegion, waiterRegion, order, preBlocked, false);
    }

    private void runScenario(Region holderRegion, Region waiterRegion, List<Event> order, boolean preBlocked,
                             boolean keyStartsLoading)
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> txn = world.commandEntry();
        AccordCacheEntry<?, ?, ?> key = world.keyEntry(keyStartsLoading);
        // a second key, which H leads and can process a round against; only used by H_PROCESSES_OTHER_KEY
        AccordCacheEntry<?, ?, ?> otherKey = world.keyEntry();

        // H is the incremental task that will hold the txnId across its runs, so must end up ahead of W
        Task h = world.task("H", nextPosition++, holderRegion);
        Task w = world.task("W", nextPosition++, waiterRegion);

        // an unrelated task sitting on the key all along, so the cycle forms into a non-empty queue
        if (preBlocked)
        {
            Task other = world.task("other", nextPosition++, Region.BAG);
            world.join(other, key);
        }

        // An ATOMIC task claims every entry it declared in one uninterrupted pass, so its claims cannot straddle
        // another task's first run; modelling them as separate events manufactures cycles production cannot reach.
        boolean hClaimsAtSetup = holderRegion == Region.FIFO;
        boolean wClaimsAtSetup = waiterRegion == Region.FIFO;

        // H must be on the txnId, and be its head, before it can take the lock
        world.join(h, txn);
        if (hClaimsAtSetup)
            world.join(h, key);

        for (Event event : order)
        {
            switch (event)
            {
                case H_JOINS_KEY:
                    if (!hClaimsAtSetup)
                        world.join(h, key);
                    break;
                case H_LOCKS_TXN:
                    world.beginRun(h, txn);
                    break;
                case W_JOINS_TXN:
                    if (!wClaimsAtSetup)
                        world.join(w, txn);
                    break;
                case W_JOINS_KEY:
                    if (wClaimsAtSetup)
                        world.join(w, txn);
                    world.join(w, key);
                    break;
                case KEY_LOAD_COMPLETES:
                    if (key.isLoading())
                        world.completeLoad(key);
                    break;
                case H_PROCESSES_OTHER_KEY:
                    if (!otherKey.contains(h.safeTask))
                        world.join(h, otherKey);
                    world.processRound(h, otherKey);
                    break;
            }
        }

        world.assertNoDuplicatePositions();
        world.assertNoCycle();
        world.assertStartedTasksLead(key);
    }

    /**
     * The generalisation of {@link #everyDirectCycleIsBroken}: generate a workload and require that no cycle of any
     * length ever forms, reaching the <em>indirect</em> cycles the two-task enumeration cannot.
     *
     * <p>Varied per seed: 2-4 tasks, 0-2 txnIds, 1-3 keys, each key initially loaded or loading, and each task's fixed
     * kind. With no txnId nothing is held across runs, so no cycle is possible at all; those seeds are a control on the
     * claim that position edges alone can never cycle, {@code compare} being a total order.
     *
     * <p>A fifo claim's positions are all taken in one operation, because production takes them in one uninterrupted pass
     * (O4/O5) and never takes another afterwards: {@code adoptCachedKeyExclusive} requires {@code !isCacheQueuedFifo()}.
     * A claim taken outside that pass carries a stamp issued before it, so it can be placed ahead of a {@code HOLD_QUEUE}
     * holder stamped in between, which is a real cycle - of a state production cannot build.
     *
     * <p>Invariants are checked after every operation, so a failure names the operation that introduced it, and the log
     * replays deterministically from the seed.
     */
    @Test
    public void randomisedNoCycleOfAnyLength()
    {
//        long seed = new SecureRandom().nextLong();
        long seed = 20261085;
        for (int iteration = 0 ; iteration < 2000 ; ++iteration, ++seed)
        {
            List<String> log = new ArrayList<>();
            try
            {
                runRandom(new Random(seed), log);
            }
            catch (Throwable t)
            {
                throw new AssertionError("seed " + seed + " failed after:\n  " + String.join("\n  ", log), t);
            }
        }
    }

    private void runRandom(Random rnd, List<String> log)
    {
        World world = new World();

        int taskCount = 2 + rnd.nextInt(3);      // 2..4
        int txnCount = rnd.nextInt(3);           // 0..2
        int keyCount = 1 + rnd.nextInt(3);       // 1..3

        List<AccordCacheEntry<?, ?, ?>> txns = new ArrayList<>();
        for (int i = 0 ; i < txnCount ; ++i)
            txns.add(world.commandEntry());
        List<AccordCacheEntry<?, ?, ?>> keys = new ArrayList<>();
        for (int i = 0 ; i < keyCount ; ++i)
            keys.add(world.keyEntry(rnd.nextBoolean()));

        List<Task> tasks = new ArrayList<>();
        for (int i = 0 ; i < taskCount ; ++i)
            tasks.add(world.task("t" + i, nextPosition++, Region.values()[rnd.nextInt(Region.values().length)]));
        log.add(taskCount + " tasks, " + txnCount + " txns, " + keyCount + " keys");

        for (int op = 0 ; op < 16 ; ++op)
        {
            switch (rnd.nextInt(4))
            {
                case 0:
                {
                    // claim an entry we do not already hold a position on
                    Task task = tasks.get(rnd.nextInt(tasks.size()));
                    if (task.safeTask.isCacheQueuedFifo())
                    {
                        // O4/O5: a fifo claim takes every position it will ever hold in one uninterrupted pass - at setup
                        // for an ATOMIC task, and by moveToFifo at upgrade-on-start for an incremental one - and
                        // adoptCachedKeyExclusive's require(!isCacheQueuedFifo()) bars a later one. A claim taken after the
                        // pass is placed by a stamp issued before it, so it can land ahead of a HOLD_QUEUE holder stamped
                        // in between - which really is a cycle (and which addFifo reports), but is not a state production
                        // can reach, so building one here would test nothing.
                        if (task.safeTask.fifoAt != 0)
                            continue;

                        // so take a random, non-empty subset of the entries, all of it here with nothing interleaved
                        List<AccordCacheEntry<?, ?, ?>> claims = new ArrayList<>();
                        for (AccordCacheEntry<?, ?, ?> candidate : txns)
                        {
                            if (rnd.nextBoolean())
                                claims.add(candidate);
                        }
                        for (AccordCacheEntry<?, ?, ?> candidate : keys)
                        {
                            if (rnd.nextBoolean())
                                claims.add(candidate);
                        }
                        if (claims.isEmpty())
                            continue;
                        for (AccordCacheEntry<?, ?, ?> candidate : claims)
                        {
                            log.add("claim " + task.safeTask + " -> " + candidate.key() + " as " + world.regionOn(task, candidate));
                            world.join(task, candidate);
                        }
                        break;
                    }
                    AccordCacheEntry<?, ?, ?> entry = !txns.isEmpty() && rnd.nextBoolean()
                                                      ? txns.get(rnd.nextInt(txns.size()))
                                                      : keys.get(rnd.nextInt(keys.size()));
                    if (entry.contains(task.safeTask) || world.heldBy.get(entry) == task)
                        continue;
                    log.add("join " + task.safeTask + " -> " + entry.key() + " as " + world.regionOn(task, entry));
                    world.join(task, entry);
                    break;
                }
                case 1:
                {
                    // begin a run, holding a txnId across it; only the head of a loaded, unlocked entry may do so
                    if (txns.isEmpty())
                        continue;
                    AccordCacheEntry<?, ?, ?> txn = txns.get(rnd.nextInt(txns.size()));
                    if (txn.isLocked() || txn.isLoading())
                        continue;
                    List<SafeTask<?>> queued = txn.unsafeQueuedTasks();
                    if (queued.isEmpty())
                        continue;
                    Task head = world.bySafeTask.get(queued.get(0));
                    // an unsequenced claim holds no position to keep, so it cannot hold the queue across runs
                    if (head == null || head.kind == Region.BAG)
                        continue;
                    // a task prepares only once it leads every txnId it declared; without this two started tasks could
                    // each hold a txnId the other is queued behind, and nothing orders two started tasks against each other
                    if (!world.leadsEveryTxn(head, txns))
                        continue;
                    log.add("beginRun " + head.safeTask + " holds " + txn.key());
                    world.beginRun(head, txn);
                    break;
                }
                case 2:
                {
                    // a load completes: drain everyone who queued meanwhile, then re-add them
                    List<AccordCacheEntry<?, ?, ?>> loading = new ArrayList<>();
                    for (AccordCacheEntry<?, ?, ?> key : keys)
                    {
                        if (key.isLoading())
                            loading.add(key);
                    }
                    if (loading.isEmpty())
                        continue;
                    AccordCacheEntry<?, ?, ?> entry = loading.get(rnd.nextInt(loading.size()));
                    log.add("completeLoad " + entry.key());
                    world.completeLoad(entry);
                    break;
                }
                default:
                {
                    // a run finishes: give back the lock, and the position with it
                    List<AccordCacheEntry<?, ?, ?>> held = new ArrayList<>(world.heldBy.keySet());
                    if (held.isEmpty())
                        continue;
                    AccordCacheEntry<?, ?, ?> entry = held.get(rnd.nextInt(held.size()));
                    Task holder = world.heldBy.remove(entry);
                    log.add("release " + holder.safeTask + " from " + entry.key());
                    entry.remove(holder.safeTask, true, null);
                    world.positions.getOrDefault(holder, Collections.emptyList()).remove(entry);
                    break;
                }
            }

            // only the cycle check here: assertStartedTasksLead is sufficient for cycle freedom but not necessary, so
            // over arbitrary workloads it reports arrangements that are harmless or self-repairing
            world.assertNoDuplicatePositions();
            world.assertNoCycle();
        }
    }

    /** the world of entries and tasks for one scenario */
    private static class World
    {
        final List<AccordCacheEntry<?, ?, ?>> entries = new ArrayList<>();
        /** which entries hold a position for each task */
        final Map<Task, List<AccordCacheEntry<?, ?, ?>>> positions = new IdentityHashMap<>();
        /** who holds each entry across runs; the queue records this too, but we keep it for the wait graph */
        final Map<AccordCacheEntry<?, ?, ?>, Task> heldBy = new IdentityHashMap<>();
        final Map<SafeTask<?>, Task> bySafeTask = new IdentityHashMap<>();

        AccordCacheEntry<?, ?, ?> commandEntry()
        {
            AccordCacheEntry<?, ?, ?> entry = new AccordCacheEntry<>(TxnId.fromValues(1, ++nextId, 0, new Id(1)), null);
            entry.unsafeSetStatus(Status.LOADED);
            entries.add(entry);
            return entry;
        }

        AccordCacheEntry<?, ?, ?> keyEntry()
        {
            return keyEntry(false);
        }

        AccordCacheEntry<?, ?, ?> keyEntry(boolean loading)
        {
            RoutingKey routingKey = mock(RoutingKey.class);
            when(routingKey.toString()).thenReturn("key" + nextId++);
            AccordCacheEntry<?, ?, ?> entry = new SaferCommandsForKey.CommandsForKeyCacheEntry(routingKey, null);
            entry.unsafeSetStatus(loading ? Status.LOADING : Status.LOADED);
            entries.add(entry);
            return entry;
        }

        Task task(String name, long position)
        {
            return newTask(this, name, position);
        }

        /**
         * A task's kind is fixed: it is fifo on every entry it claims, or ordered on every one, or unsequenced on every
         * one. Mixing them per entry would manufacture orderings production cannot produce.
         */
        Task task(String name, long position, Region kind)
        {
            Task task = newTask(this, name, position);
            task.kind = kind;
            return task;
        }

        /**
         * Stamp the {@code fifoAt} the fifo region is ordered by, if this task does not have one yet.
         * <p>
         * Production stamps at the instant a task becomes a fifo claim: at setup for an ATOMIC task, whose claim pass
         * immediately follows, and at first prepare for an incremental one. This harness creates its tasks up front and
         * then interleaves their claims, so stamping at construction would date an ATOMIC task's claim pass from before
         * events that in production precede its setup - and would order it ahead of a task that started first.
         */
        void ensureFifoAt(Task task)
        {
            if (task.safeTask.fifoAt == 0)
                task.safeTask.fifoAt = nextFifoAt++;
        }

        Task task(String name, long position, Region kind, boolean incremental)
        {
            Task task = task(name, position, kind);
            task.incremental = incremental;
            return task;
        }

        /** SafeTask dispatches through {@code SaferState.global}, which switches on exact class, so these must be real */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        void addRef(Task task, AccordCacheEntry<?, ?, ?> entry)
        {
            if (task.safeTask.refs.containsKey(entry.key()))
                return;
            SafeState<?> ref = entry.isCommandsForKey()
                               ? new SaferCommandsForKey((AccordCacheEntry) entry)
                               : new SaferCommand((AccordCacheEntry) entry);
            ((Map) task.safeTask.refs).put(entry.key(), ref);
        }

        /** the region this task's kind puts it in on this entry */
        Region regionOn(Task task, AccordCacheEntry<?, ?, ?> entry)
        {
            if (task.kind == Region.FIFO) return Region.FIFO;
            return task.kind == Region.BAG && entry.isCommandsForKey() ? Region.BAG : Region.SORTED;
        }

        void join(Task task, AccordCacheEntry<?, ?, ?> entry)
        {
            join(task, entry, regionOn(task, entry));
        }

        void join(Task task, AccordCacheEntry<?, ?, ?> entry, Region region)
        {
            positions.computeIfAbsent(task, ignore -> new ArrayList<>()).add(entry);
            addRef(task, entry);

            // nothing is runnable while an entry loads, so every waiter is bagged, and a fifo claim still takes its
            // position; the drain re-places everyone when the load completes
            if (entry.isLoading())
            {
                if (region == Region.FIFO) { ensureFifoAt(task); entry.addFifo(task.safeTask); }
                else entry.addWaitingToLoad(task.safeTask);
                return;
            }

            ensureCacheQueued(task, entry, region);
        }

        /** mirrors SafeTask.ensureCacheQueued */
        void ensureCacheQueued(Task task, AccordCacheEntry<?, ?, ?> entry, Region region)
        {
            if (region == Region.FIFO)
            {
                if (!entry.contains(task.safeTask))
                {
                    ensureFifoAt(task);
                    entry.addFifo(task.safeTask);
                }
                return;
            }
            if (region == Region.BAG) entry.addUnsequenced(task.safeTask);
            else entry.addPrioritised(task.safeTask);
        }

        /**
         * The load-completion path, as {@code AccordExecutor.onLoadedExclusive} drives it: drain everyone who queued
         * while the entry loaded, mark it loaded, then re-add them in {@code compareForNotify} order. The order this
         * produces is decided by that sort rather than by anything the started tasks did.
         */
        void completeLoad(AccordCacheEntry<?, ?, ?> entry)
        {
            List<Task> drained = new ArrayList<>();
            try (BufferList<SafeTask<?>> tasks = entry.drainWaitingToLoad())
            {
                entry.unsafeSetStatus(Status.LOADED);
                tasks.sort(AccordCacheEntryQueue::compareForNotify);
                for (SafeTask<?> safeTask : tasks)
                    drained.add(bySafeTask.get(safeTask));
            }
            for (Task task : drained)
                ensureCacheQueued(task, entry, regionOn(task, entry));
        }

        /**
         * A task's first run, as {@code prepareExclusiveMayThrow} sequences it: an incremental task that is not already a
         * fifo claim is upgraded to one on every entry it holds, <em>then</em> it takes the txnId lock, <em>then</em> it
         * records that the run has begun. The order matters: the upgrade happens while the task still counts as not
         * started, and the txnId lock is taken once it is a fifo claim there, which is what lets {@code lockExclusive}
         * require the locker to lead.
         */
        void beginRun(Task task, AccordCacheEntry<?, ?, ?> txn)
        {
            if (task.incremental && task.kind != Region.FIFO)
            {
                // the task is a fifo claim from now on, on every entry, and takes the stamp the region is ordered by -
                // late, so it sorts behind every claim already stamped
                task.kind = Region.FIFO;
                ensureFifoAt(task);
                // moveToFifo on *every* ref, loading or not: a fifo claim holds a position on a loading entry too, and
                // the drain preserves it, so skipping them would leave the task in the bag until the load completed
                for (AccordCacheEntry<?, ?, ?> entry : new ArrayList<>(positions.getOrDefault(task, Collections.emptyList())))
                    entry.moveToFifo(task.safeTask);
            }
            lockHoldingQueue(task, txn);
            task.setStarted();
        }

        /**
         * One round of an incremental task's work against a key it leads: take it with {@code RELEASE_QUEUE}, giving up
         * the queue position so the next claim can be promoted, then give the lock back.
         */
        void processRound(Task task, AccordCacheEntry<?, ?, ?> entry)
        {
            if (entry.isLoading() || entry.isLocked())
                return;
            List<SafeTask<?>> queued = entry.unsafeQueuedTasks();
            int runnable = entry.unsafeRunnablePrefix();
            boolean leads = false;
            for (int i = 0 ; i < runnable ; ++i)
                leads |= queued.get(i) == task.safeTask;
            if (!leads)
                return; // not ours to process yet

            entry.lockExclusive(task.safeTask, AccordCacheEntry.LockMode.RELEASE_QUEUE);
            entry.remove(task.safeTask, true, null);
            positions.getOrDefault(task, Collections.emptyList()).remove(entry);
        }

        /** whether {@code task} leads every txnId it holds a position on, which is what lets it begin a run */
        boolean leadsEveryTxn(Task task, List<AccordCacheEntry<?, ?, ?>> txns)
        {
            for (AccordCacheEntry<?, ?, ?> txn : txns)
            {
                if (!txn.contains(task.safeTask))
                    continue;
                List<SafeTask<?>> queued = txn.unsafeQueuedTasks();
                int runnable = txn.unsafeRunnablePrefix();
                boolean leads = false;
                for (int i = 0 ; i < runnable ; ++i)
                    leads |= queued.get(i) == task.safeTask;
                if (!leads)
                    return false;
                // and nothing else may be holding it across its own runs
                Task holder = heldBy.get(txn);
                if (holder != null && holder != task)
                    return false;
            }
            return true;
        }

        void lockHoldingQueue(Task task, AccordCacheEntry<?, ?, ?> entry)
        {
            entry.lockExclusive(task.safeTask, AccordCacheEntry.LockMode.HOLD_QUEUE);
            heldBy.put(entry, task);
        }

        /**
         * On one entry: a task that has begun running holds locks something here may be waiting for, so nothing that has
         * <em>not</em> begun may be ahead of it. A started task is a fifo claim on every entry it holds, and fifo
         * precedes sorted precedes bag, so it can only sit behind an un-started task if that task is also a fifo claim.
         *
         * <p>"Ahead" means the wait relation, not the slot order: everything outside the runnable prefix waits for the
         * prefix, and within the bag nobody waits for anybody.
         *
         * @return a description of the violation, or null. {@code tolerateFifoGap} skips fifo-behind-fifo, which cannot
         *         arise where the claim orders are legal (see {@link #isLegalOrder}).
         */
        String startedTasksLeadViolation(AccordCacheEntry<?, ?, ?> entry, boolean tolerateFifoGap)
        {
            List<SafeTask<?>> queued = entry.unsafeQueuedTasks();
            int runnable = entry.unsafeRunnablePrefix();
            int fifoSize = entry.unsafeFifoSize();

            // upgrade-on-start: a started task is a fifo claim wherever it is queued
            for (int i = 0 ; i < queued.size() ; ++i)
            {
                if (queued.get(i).hasIncrementalStarted() && i >= fifoSize)
                    return "started " + queued.get(i) + " holds a non-fifo position, in " + queued;
            }

            boolean prefixHasNotStarted = false;
            for (int i = 0 ; i < runnable ; ++i)
                prefixHasNotStarted |= !queued.get(i).hasIncrementalStarted();

            if (!prefixHasNotStarted)
                return null;

            for (int i = runnable ; i < queued.size() ; ++i)
            {
                if (!queued.get(i).hasIncrementalStarted())
                    continue;

                // both in the fifo: arrival order decides, and nothing reorders it
                if (tolerateFifoGap && i < fifoSize)
                    continue;

                return "started " + queued.get(i) + " waits for a task that has not started, in " + queued;
            }
            return null;
        }

        void assertStartedTasksLead(AccordCacheEntry<?, ?, ?> entry)
        {
            assertStartedTasksLead(entry, false);
        }

        void assertStartedTasksLead(AccordCacheEntry<?, ?, ?> entry, boolean tolerateFifoGap)
        {
            String violation = startedTasksLeadViolation(entry, tolerateFifoGap);
            if (violation != null)
                throw new AssertionError(violation);
        }

        /**
         * Every task that has been added and not removed occupies exactly one position. {@code validate()} checks only
         * which slots are occupied, never by whom, and a task queued twice waits for itself.
         */
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
                            throw new AssertionError(queued.get(i) + " holds two positions on " + entry.key() + ": " + queued);
                    }
                }
            }
        }

        /** An edge means the source cannot run until the target does; a cycle in those edges is a deadlock. */
        void assertNoCycle()
        {
            Map<SafeTask<?>, List<SafeTask<?>>> waitsFor = new IdentityHashMap<>();
            for (AccordCacheEntry<?, ?, ?> entry : entries)
            {
                // nothing on a loading entry waits for anything here: they all wait for the load, which completes on its
                // own, so a loading entry cannot be part of a cycle
                if (entry.isLoading())
                    continue;

                List<SafeTask<?>> queued = entry.unsafeQueuedTasks();
                int runnable = entry.unsafeRunnablePrefix();
                Task holder = heldBy.get(entry);

                for (int i = 0 ; i < queued.size() ; ++i)
                {
                    SafeTask<?> waiter = queued.get(i);
                    List<SafeTask<?>> targets = waitsFor.computeIfAbsent(waiter, ignore -> new ArrayList<>());

                    // a lock edge: nothing here runs until the holder gives the entry back
                    if (holder != null && holder.safeTask != waiter)
                        targets.add(holder.safeTask);

                    // a position edge: outside the runnable prefix, we wait for it
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

    /** depth first search for a back edge, returning the cycle if there is one */
    private static List<SafeTask<?>> findCycle(Map<SafeTask<?>, List<SafeTask<?>>> waitsFor)
    {
        Map<SafeTask<?>, Integer> state = new IdentityHashMap<>(); // 1 = on the stack, 2 = done
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

    /** a task, with the mutable bits the queues read */
    private static class Task
    {
        SafeTask<?> safeTask;
        final boolean[] started = new boolean[1];
        World world;
        Region kind = Region.SORTED;
        /**
         * Only an INCR task is upgraded to a fifo claim when it begins running: a non-incremental task keeps whatever
         * region its kind puts it in for life, so it can block a chain while being immovable.
         */
        boolean incremental = true;

        void setStarted()
        {
            started[0] = true;
        }
    }

    private static Task newTask(World world, String name, long position)
    {
        Task task = new Task();
        task.world = world;

        SafeTask<?> safeTask = mock(SafeTask.class);
        ExecutionContext context = mock(ExecutionContext.class);
        when(context.executionKind()).thenReturn(ExecutionKind.OTHER);
        when(safeTask.executionContext()).thenReturn(context);
        when(safeTask.toString()).thenReturn(name + '@' + position);
        when(safeTask.isNonSync()).thenReturn(true);
        when(safeTask.is(org.apache.cassandra.service.accord.execution.Task.State.WAITING_TO_RUN)).thenReturn(true);
        when(safeTask.hasIncrementalStarted()).thenAnswer(ignore -> task.started[0]);
        // these all model INCR tasks. isIncremental is set at construction, before the task is ever queued
        when(safeTask.isIncremental()).thenAnswer(ignore -> task.incremental);
        // a task's kind is fixed until it starts, when an incremental one is upgraded to a fifo claim (see beginRun)
        when(safeTask.isCacheQueuedFifo()).thenAnswer(ignore -> task.kind == Region.FIFO);
        // an UNSEQUENCED task is bagged on every entry it claims; BAG is that kind here
        when(safeTask.isUnsequenced()).thenAnswer(ignore -> task.kind == Region.BAG);
        safeTask.refs = new org.agrona.collections.Object2ObjectHashMap<>();
        safeTask.position = position;

        task.safeTask = safeTask;
        world.bySafeTask.put(safeTask, task);
        return task;
    }

    /** all orderings of the events */
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
