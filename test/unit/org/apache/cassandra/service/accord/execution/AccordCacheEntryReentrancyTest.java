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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import accord.api.RoutingKey;
import accord.local.ExecutionContext;
import accord.local.ExecutionContext.ExecutionKind;
import accord.local.Node.Id;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers.BufferList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test has been authored entirely by Claude.
 *
 * Reentrancy of the cache-entry queues, driven adversarially: the queues notify tasks while they are mutating themselves,
 * and those notifications come back in. This suite injects, from inside a notification, every mutation a future callback
 * might plausibly perform, and then checks the outcome against L3.
 *
 * <p><b>L3 is what the outcome is measured against.</b> {@code spec/accord-execution/INVARIANTS.md} L3 says a
 * notification handler must not mutate the queue of the entry that is notifying it - several mutators publish their
 * state only after notifying, so a reentrant insertion or lock would overwrite a live claim or lose a lock holder.
 * Nothing enforces that at runtime: it holds because no production handler does it. The handlers reached from a queue
 * notification are {@code SafeTask.onChangeKeyRunnableStatus} / {@code onChangeTxnRunnableStatus} and, through them,
 * {@code NonSyncState.onKeyReady} / {@code onKeyNotReady} and {@code waitOnKeysExclusive}; of those only
 * {@code waitOnKeysExclusive} takes queue positions, and it takes them on the task's <em>keys</em>, never on the txnId
 * entry that notified it. So the two halves of the enumeration are:
 *
 * <ul>
 *   <li>{@link Where#SAME_ENTRY}: the injected mutation targets the notifying entry. Every such injection that touches a
 *       queue is <em>forbidden</em> by L3 and no production handler performs it. It is enumerated and counted so that a
 *       shape change which stops producing it is noticed, but it is deliberately not performed: with no guard to reject
 *       it, the behaviour it would exercise is undefined, and this suite would be asserting either undefined behaviour
 *       or the existence of a guard. See {@code isForbiddenByL3}, which is the one place to change if a runtime guard is
 *       added back.</li>
 *   <li>{@link Where#OTHER_ENTRY}: the injected mutation targets a <em>different</em> loaded entry that every task also
 *       holds a position on. L3 is per entry and permits exactly this, and it is the production shape - a txnId
 *       promotion queues on keys from inside a notification via {@code waitOnKeysExclusive}, and a deferred CFK release
 *       removes positions from inside one. This is where the adversarial coverage lives, and it is what the
 *       "from inside a notification" counters below are gates on.</li>
 * </ul>
 *
 * <p>Enumerated exhaustively: {shape} x {driver} x {injection site} x {injected action} x {where} x {target}. A
 * configuration that cannot arise is counted as skipped rather than passed, so the reachable total is visible, and the
 * reachable total itself is asserted to be a substantial fraction of the enumeration.
 *
 * <p>{@link Action#DRAIN_AND_READD} re-enters least obviously: a load completing drains every waiter and the caller
 * re-adds them, so the re-add of one waiter runs while another is drained but not yet back in the queue. It always acts
 * on a third, still-loading entry, so it is permitted by L3 whichever way it is injected.
 *
 * <p>Not injectable here: releasing a lock, as {@code releaseExclusive} goes through the cache {@code Instance}, which
 * these entries do not have. That is covered incidentally by {@code AccordCacheEntrySafeTaskCycleTest}.
 */
public class AccordCacheEntryReentrancyTest
{
    private enum Region { FIFO, SORTED, BAG }

    /**
     * Where the injected action fires from. There is only one outward edge: the notification every mutation issues to the
     * tasks whose runnability it changed. Kept as an enum for the identifier it contributes to a scenario id.
     */
    private enum Site { STATUS }

    /** what a reentrant callback might plausibly do to a queue */
    private enum Action
    {
        NONE,
        REMOVE_OTHER,
        REMOVE_SELF,
        ADD_FIFO,
        ADD_SORTED,
        ADD_BAG,
        LOCK_HOLD,
        LOCK_RELEASE_MODE,
        LOCK_UNQUEUED,
        UPGRADE_TO_FIFO,
        DRAIN_AND_READD,
        RELEASE_LOCK
    }

    /** the queue we inject into */
    private enum Shape { TWO_FIFO, FIFO_SORTED, SORTED_BAG, TWO_FIFO_BAG, HELD_FIFO, HELD_RELEASE_MODE, LOADING_WAITERS }

    /** the mutation whose notification carries the injection */
    private enum Driver { ADD_NEW, REMOVE_HEAD, TAKE_LOCK, DRAIN }

    /**
     * Which entry the injected action mutates: the one that is notifying us (which L3 forbids) or a different one that
     * the same tasks are queued on (which L3 permits).
     */
    private enum Where { SAME_ENTRY, OTHER_ENTRY }

    /** whether the action mutates the queue of the entry it is directed at, which is what L3 restricts */
    private static boolean mutatesTheTargetQueue(Action action)
    {
        switch (action)
        {
            case NONE:            return false;
            // acts on the third, still-loading entry, never on the entry it is injected into
            case DRAIN_AND_READD: return false;
            default:              return true;
        }
    }

    /**
     * A SAME_ENTRY mutation is forbidden by L3, and there is no runtime guard that rejects one: L3 holds because no
     * production notification handler mutates the notifying entry's queue (survey in the class javadoc), not because the
     * entry polices it. So such an injection is out of contract - the behaviour it would exercise is undefined, and
     * asserting anything about it would either assert undefined behaviour or (as this suite used to) assert that a guard
     * exists. It is enumerated, counted, and not performed. If a guard is ever added back, delete this and require the
     * report instead: the enumeration already reaches every forbidden combination, which {@link #forbiddenSkipped}
     * proves.
     */
    private static boolean isForbiddenByL3(Where where, Action action)
    {
        return where == Where.SAME_ENTRY && mutatesTheTargetQueue(action);
    }

    /** the stamp the fifo region is ordered by, taken when a task becomes a fifo claim */
    private static long nextFifoAt = 0;

    /**
     * Proof that the paths we care about are exercised, not merely enumerated. The four "from inside a notification"
     * counters are gates, and they count only injections that <em>completed</em>.
     */
    private static int drainsFromInsideNotify, injectionsFired, releasesFromInsideNotify, upgradesFromInsideNotify;
    private static int opportunisticLocksFromInsideNotify;
    /** how many injections L3 forbids, and which are therefore enumerated but not performed (see isForbiddenByL3) */
    private static int forbiddenSkipped;

    private static class Unreachable extends RuntimeException
    {
        Unreachable(String why) { super(why, null, false, false); }
    }

    @BeforeClass
    public static void beforeClass()
    {
        // remove(task, ownsLock, null) derives its RemoveMode from AccordExecutor.CACHE_QUEUES_ENABLED, whose static
        // initialiser reads DatabaseDescriptor.getAccord(); without this the release paths die in class initialisation
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void clearMocks()
    {
        // mockito-inline keeps every mock in a global registry that degrades superlinearly; without this the enumeration
        // does not finish
        Mockito.framework().clearInlineMocks();
    }

    @Test
    public void everyInjection()
    {
        drainsFromInsideNotify = injectionsFired = releasesFromInsideNotify = upgradesFromInsideNotify = 0;
        opportunisticLocksFromInsideNotify = forbiddenSkipped = 0;
        int scenarios = 0, checked = 0, skipped = 0;
        List<String> failures = new ArrayList<>();
        for (Shape shape : Shape.values())
        {
            for (Driver driver : Driver.values())
            {
                for (Site site : Site.values())
                {
                    for (Action action : Action.values())
                    {
                        for (Where where : Where.values())
                        {
                            for (int target = 0; target < 3; ++target)
                            {
                                String id = shape + "/" + driver + "/" + site + "/" + action + "/" + where + "/t" + target;
                                ++scenarios;
                                try
                                {
                                    new World(shape, driver, site, action, where, target).run();
                                    ++checked;
                                }
                                catch (Unreachable u)
                                {
                                    ++skipped;
                                }
                                catch (Throwable t)
                                {
                                    StringBuilder at = new StringBuilder();
                                    for (StackTraceElement e : t.getStackTrace())
                                    {
                                        if (e.getClassName().contains("accord"))
                                        {
                                            at.append(e.getMethodName()).append(':').append(e.getLineNumber()).append(' ');
                                            if (at.length() > 90) break;
                                        }
                                    }
                                    failures.add(id + ": " + t + " @ " + at);
                                }
                            }
                        }
                    }
                }
            }
        }

        System.out.println("reentrancy injection: " + checked + " checked, " + skipped + " unreachable, of "
                           + scenarios + " enumerated");
        System.out.println("  injections fired=" + injectionsFired
                           + " forbidden by L3 (enumerated, not performed)=" + forbiddenSkipped
                           + " drains from inside a notification=" + drainsFromInsideNotify
                           + " releases from inside a notification=" + releasesFromInsideNotify
                           + " upgrades from inside a notification=" + upgradesFromInsideNotify
                           + " opportunistic locks from inside a notification=" + opportunisticLocksFromInsideNotify);

        // anti-vacuity: an enumeration that enumerated nothing must fail, and so must one whose reachability filters
        // have quietly become universal. A quarter is well below the ~40% these axes reach today.
        if (checked <= scenarios / 4)
            failures.add("only " + checked + " of " + scenarios + " scenarios were reachable: the filters are too broad");
        if (injectionsFired == 0) failures.add("no injection ever fired");
        if (drainsFromInsideNotify == 0) failures.add("no drain ever ran from inside a notification");
        if (releasesFromInsideNotify == 0) failures.add("no lock was ever released from inside a notification");
        if (upgradesFromInsideNotify == 0) failures.add("no task was ever upgraded to fifo from inside a notification");
        if (opportunisticLocksFromInsideNotify == 0) failures.add("no opportunistic (UNQUEUED) lock was ever taken");
        // L3 itself: the forbidden half must still be *enumerated*, so that a shape change which stops producing it is
        // noticed. It is deliberately not performed - see isForbiddenByL3.
        if (forbiddenSkipped == 0)
            failures.add("no SAME_ENTRY mutation was enumerated: the L3-forbidden half of the matrix has stopped firing");

        if (!failures.isEmpty())
        {
            StringBuilder sb = new StringBuilder(failures.size() + " of " + (checked + failures.size())
                                                 + " injections failed:\n");
            for (String f : failures) sb.append("  ").append(f).append('\n');
            throw new AssertionError(sb.toString());
        }
    }

    private static class T
    {
        SafeTask<?> safeTask;
        Region kind;
        boolean started;
        /**
         * Every entry this task holds a <em>ref</em> for, which is not the same as the entries it holds a position in: a
         * RELEASE_QUEUE lock removes the position and keeps the ref, and the entry then decides membership from the
         * queued-state hint or a scan. Entries are not deleted here on removal, or a removed task could no longer be
         * checked against the entry it had left, making a class of stale-hint bug unreachable.
         */
        final List<AccordCacheEntry<?, ?, ?>> positions = new ArrayList<>();
        String name;

        @Override
        public String toString() { return name; }
    }

    private class World
    {
        final Shape shape;
        final Driver driver;
        final Site site;
        final Action action;
        final Where where;
        final int target;

        final Map<SafeTask<?>, T> byTask = new IdentityHashMap<>();
        final List<T> queued = new ArrayList<>();
        AccordCacheEntry<?, ?, ?> entry;   // the entry the driver mutates, and therefore the one that notifies
        // a loaded sibling holding the same members in the same regions. Mutating this one from inside entry's
        // notification is what L3 permits, so it is the OTHER_ENTRY injection target.
        AccordCacheEntry<?, ?, ?> sibling;
        AccordCacheEntry<?, ?, ?> other;   // a second entry every task also holds, so propagation re-enters
        // a third entry, still loading, that every member is bagged on. A loading entry passes a null owner and so never
        // notifies, which means a drain can only ever be reached from inside *another* entry's notification - exactly the
        // production shape, where a load completes while we are notifying about something else.
        AccordCacheEntry<?, ?, ?> loadingEntry;
        boolean injected;
        boolean armed;
        boolean lockInFlight;
        int nextId;

        World(Shape shape, Driver driver, Site site, Action action, Where where, int target)
        {
            this.shape = shape;
            this.driver = driver;
            this.site = site;
            this.action = action;
            this.where = where;
            this.target = target;
        }

        /** the entry the injected action is directed at */
        private AccordCacheEntry<?, ?, ?> injectInto()
        {
            return where == Where.SAME_ENTRY ? entry : sibling;
        }

        void run()
        {
            build();
            armed = true; // construction notifies too; the injection belongs to the driver, not the setup
            drive();
            check();
        }

        // ---------------- construction ----------------

        private AccordCacheEntry<?, ?, ?> commandEntry()
        {
            AccordCacheEntry<?, ?, ?> e = new AccordCacheEntry<>(TxnId.fromValues(1, ++nextId, 0, new Id(1)), null);
            e.unsafeSetStatus(Status.LOADED);
            return e;
        }

        private AccordCacheEntry<?, ?, ?> keyEntry(boolean loading)
        {
            RoutingKey k = mock(RoutingKey.class);
            when(k.toString()).thenReturn("key" + (++nextId));
            AccordCacheEntry<?, ?, ?> e = new SaferCommandsForKey.CommandsForKeyCacheEntry(k, null);
            e.unsafeSetStatus(loading ? Status.LOADING : Status.LOADED);
            return e;
        }

        private T task(String name, Region kind, long position)
        {
            T t = new T();
            t.name = name;
            t.kind = kind;

            SafeTask<?> safeTask = mock(SafeTask.class);
            ExecutionContext context = mock(ExecutionContext.class);
            when(context.executionKind()).thenReturn(ExecutionKind.OTHER);
            when(safeTask.executionContext()).thenReturn(context);
            when(safeTask.toString()).thenReturn(name);
            when(safeTask.isNonSync()).thenReturn(true);
            when(safeTask.is(Task.State.WAITING_TO_RUN)).thenReturn(true);
            when(safeTask.isIncremental()).thenReturn(true);
            when(safeTask.hasIncrementalStarted()).thenAnswer(i -> t.started);
            when(safeTask.isCacheQueuedFifo()).thenAnswer(i -> t.kind == Region.FIFO);
            when(safeTask.isUnsequenced()).thenAnswer(i -> t.kind == Region.BAG);

            // production callbacks do real work here; we only inject, so the baseline is a no-op
            doAnswer(i -> {
                maybeInject(Site.STATUS, t);
                return null;
            }).when(safeTask).onChangeRunnableStatus(any(), any());

            safeTask.refs = new org.agrona.collections.Object2ObjectHashMap<>();
            safeTask.position = position;
            // Every task here is incremental, and a command entry orders every incremental task as a fifo claim (see the
            // second-entry setup below), so every task is stamped at setup. The fifo region is ordered by this.
            safeTask.fifoAt = ++nextFifoAt;

            t.safeTask = safeTask;
            byTask.put(safeTask, t);
            return t;
        }

        /** real refs, so the queued-state hint is exercised rather than left UNKNOWN on every path */
        private void addRef(T t, AccordCacheEntry<?, ?, ?> e)
        {
            if (t.safeTask.refs.containsKey(e.key()))
                return;
            ((Map) t.safeTask.refs).put(e.key(), e.isCommandsForKey() ? new SaferCommandsForKey((AccordCacheEntry) e)
                                                                     : new SaferCommand((AccordCacheEntry) e));
        }

        private void place(AccordCacheEntry<?, ?, ?> e, T t)
        {
            addRef(t, e);
            // before the add, not after: production populates refs first, so a task flagged by its own insertion's
            // notification still propagates back here and is counted
            t.positions.add(e);
            if (e == entry) queued.add(t);
            switch (t.kind)
            {
                case FIFO:   e.addFifo(t.safeTask); break;
                case SORTED: e.addPrioritised(t.safeTask); break;
                case BAG:    e.addUnsequenced(t.safeTask); break;
            }
        }

        private void build()
        {
            boolean loading = shape == Shape.LOADING_WAITERS;
            // a commandsForKey entry, so a BAG task is genuinely unsequenced here (isUnsequenced requires it)
            entry = keyEntry(loading);
            other = commandEntry();

            List<T> members = new ArrayList<>();
            switch (shape)
            {
                case TWO_FIFO:
                    members.add(task("f0", Region.FIFO, 10));
                    members.add(task("f1", Region.FIFO, 20));
                    break;
                case FIFO_SORTED:
                    members.add(task("f0", Region.FIFO, 10));
                    members.add(task("s1", Region.SORTED, 20));
                    break;
                case SORTED_BAG:
                    members.add(task("s0", Region.SORTED, 10));
                    members.add(task("b1", Region.BAG, 20));
                    break;
                case TWO_FIFO_BAG:
                    members.add(task("f0", Region.FIFO, 10));
                    members.add(task("f1", Region.FIFO, 20));
                    members.add(task("b2", Region.BAG, 30));
                    break;
                case HELD_FIFO:
                case HELD_RELEASE_MODE:
                    members.add(task("h0", Region.FIFO, 10));
                    members.add(task("f1", Region.FIFO, 20));
                    break;
                case LOADING_WAITERS:
                    members.add(task("w0", Region.BAG, 10));
                    members.add(task("w1", Region.BAG, 20));
                    break;
            }

            if (loading)
            {
                for (T t : members)
                {
                    t.positions.add(entry);
                    queued.add(t);
                    addRef(t, entry);
                    entry.addWaitingToLoad(t.safeTask);
                }
            }
            else
            {
                for (T t : members)
                    place(entry, t);
            }

            // a loaded sibling holding the same members in the same regions. L3 is per entry, so a notification handler
            // may mutate this one - which is what production does (waitOnKeysExclusive from a txnId promotion, the
            // deferred CFK release) - and it is the OTHER_ENTRY injection target.
            sibling = keyEntry(false);
            for (T t : members)
                place(sibling, t);

            // every member also holds a position on a second entry, so any cascade that fans out over a task's refs can
            // re-enter the entry under test - the indirect-cycle case
            for (T t : members)
            {
                Region was = t.kind;
                t.kind = Region.FIFO; // a command entry orders every incremental task
                t.positions.add(other);
                addRef(t, other);
                other.addFifo(t.safeTask);
                t.kind = was;
            }

            // every member is also waiting on a loading entry, so DRAIN_AND_READD has something to drain from inside a
            // notification of the entry under test, and the re-adds can propagate back into it.
            //
            // How they join it is completeSetupOfLoading's split, not a uniform addWaitingToLoad: a fifo claim takes its
            // position, everyone else is bagged until the drain. That is what makes the drain/re-add pair consistent, as
            // drainWaitingToLoad reports fifo claims without removing them.
            loadingEntry = keyEntry(true);
            for (T t : members)
            {
                t.positions.add(loadingEntry);
                addRef(t, loadingEntry);
                if (t.kind == Region.FIFO) loadingEntry.addFifo(t.safeTask);
                else loadingEntry.addWaitingToLoad(t.safeTask);
            }

            if (shape == Shape.HELD_FIFO)
            {
                // the head holds the queue across runs, keeping its fifo position as well as the lock. The sibling is
                // locked by the same task in the same mode, which is what a run holding both entries looks like, and is
                // what makes RELEASE_LOCK reachable as an OTHER_ENTRY injection.
                entry.lockExclusive(members.get(0).safeTask, AccordCacheEntry.LockMode.HOLD_QUEUE);
                sibling.lockExclusive(members.get(0).safeTask, AccordCacheEntry.LockMode.HOLD_QUEUE);
                members.get(0).started = true;
            }
            else if (shape == Shape.HELD_RELEASE_MODE)
            {
                // the commoner case: the lock is given back at the end of the run, so taking it gives up the position
                T head = members.get(0);
                entry.lockExclusive(head.safeTask, AccordCacheEntry.LockMode.RELEASE_QUEUE);
                sibling.lockExclusive(head.safeTask, AccordCacheEntry.LockMode.RELEASE_QUEUE);
                // it keeps its ref and loses its position
                queued.remove(head);
                head.started = true;
            }
        }

        // ---------------- the mutation that carries the injection ----------------

        private void drive()
        {
            switch (driver)
            {
                case ADD_NEW:
                {
                    if (entry.isLoading()) throw new Unreachable("addFifo requires loaded");
                    // The fifo region is ordered by fifoAt, not position, so an arrival with the largest stamp (which is
                    // what task() hands out) is appended at the tail - and addFifo's tail path only notifies when
                    // totalSize() == 2, so for every shape with more members than that the driver injected nothing at
                    // all. Stamping strictly below every current member makes it take the head and demote the incumbent.
                    long stamp = lowestStamp(entry) - 1;
                    if (stamp <= 0) throw new Unreachable("no stamp below the existing members");
                    if (entry.isLockedHoldingQueue())
                    {
                        // O8: a HOLD_QUEUE holder is pinned at the fifo head, so an arrival can neither displace it nor
                        // change anybody's status - addFifo would report the displacement instead. There is no fifo
                        // arrival that notifies on this shape.
                        throw new Unreachable("a HOLD_QUEUE holder cannot be displaced from the fifo head (O8)");
                    }
                    T t = task("new", Region.FIFO, 5);
                    t.safeTask.fifoAt = stamp;
                    place(entry, t);
                    break;
                }
                case REMOVE_HEAD:
                {
                    if (queued.isEmpty()) throw new Unreachable("nothing to remove");
                    if (entry.isLoading()) throw new Unreachable("removal from a loading entry is the drain's job");
                    T head = runnableHead();
                    // A HOLD_QUEUE holder leads the entry and holds its lock, and gives its position up only in the
                    // release that also gives up the lock - which is what remove's expect(ownsLock) states. Passing
                    // false here was the harness claiming a removal production cannot perform, and is where this suite's
                    // pre-existing expect(ownsLock) failures came from.
                    boolean ownsLock = entry.isLockedBy(head.safeTask);
                    entry.remove(head.safeTask, ownsLock, AccordCacheEntryQueue.RemoveMode.IF_PRESENT);
                    queued.remove(head);
                    break;
                }
                case TAKE_LOCK:
                {
                    if (entry.isLoading()) throw new Unreachable("cannot lock while loading");
                    if (entry.isLocked()) throw new Unreachable("already locked");
                    T head = runnableHead();
                    lockInFlight = true;
                    try { entry.lockExclusive(head.safeTask, AccordCacheEntry.LockMode.RELEASE_QUEUE); }
                    finally { lockInFlight = false; }
                    queued.remove(head);
                    break;
                }
                case DRAIN:
                {
                    if (!entry.isLoading()) throw new Unreachable("only a loading entry drains");
                    drainAndReadd();
                    break;
                }
            }
        }

        /** exactly what onLoadOneExclusive does: complete the load, drain every waiter, re-add each one */
        private void drainAndReadd()
        {
            // the drain requires the entry to still be loading, and requires no lock; the status flips after
            if (!entry.isLoading() || entry.isLocked()) throw new Unreachable("drain needs a loading, unlocked entry");
            List<T> list = new ArrayList<>();
            try (BufferList<SafeTask<?>> drained = entry.drainWaitingToLoad())
            {
                for (SafeTask<?> s : drained)
                    list.add(byTask.get(s));
            }
            queued.clear();
            entry.unsafeSetStatus(Status.LOADED);
            for (T t : list)
            {
                place(entry, t);
            }
        }

        /** the same drain-and-re-add, applied to the third entry, so it can run from inside a notification */
        private void drainAndReaddLoading()
        {
            if (!loadingEntry.isLoading() || loadingEntry.isLocked()) throw new Unreachable("drain needs a loading, unlocked entry");
            List<T> list = new ArrayList<>();
            try (BufferList<SafeTask<?>> drained = loadingEntry.drainWaitingToLoad())
            {
                for (SafeTask<?> s : drained)
                    list.add(byTask.get(s));
            }
            loadingEntry.unsafeSetStatus(Status.LOADED);
            for (T t : list)
            {
                switch (t.kind)
                {
                    // a fifo claim is *reported* by the drain but not removed by it, so it must not be re-added:
                    // ensureCacheQueued's fifo branch only queries status(), as the position is still held
                    case FIFO:   loadingEntry.statusOfPresent(t.safeTask); break;
                    case SORTED: loadingEntry.addPrioritised(t.safeTask); break;
                    case BAG:    loadingEntry.addUnsequenced(t.safeTask); break;
                }
            }
        }

        // ---------------- the injected action ----------------

        private void maybeInject(Site at, T from)
        {
            if (!armed || injected || at != site) return;
            // fire on the first notification of the chosen kind; the target index selects who we act upon
            injected = true;
            // L3 forbids mutating the queue of the entry notifying us. Nothing in production does it and nothing rejects
            // it, so performing it here would exercise undefined behaviour: count it and stop (see isForbiddenByL3)
            if (isForbiddenByL3(where, action))
            {
                ++forbiddenSkipped;
                return;
            }
            try
            {
                ++injectionsFired;
                inject(injectInto(), from);
            }
            catch (Unreachable ignore)
            {
                // an action that cannot apply in this state is not a failure of the queue
            }
        }

        /** the smallest fifoAt among the entry's current members, or Long.MAX_VALUE if it has none */
        private long lowestStamp(AccordCacheEntry<?, ?, ?> e)
        {
            long lowest = Long.MAX_VALUE;
            for (SafeTask<?> s : e.unsafeQueuedTasks())
                lowest = Math.min(lowest, s.fifoAt);
            return lowest;
        }

        /** live membership, which is what every reachability filter below must consult (see targetOther) */
        private boolean isMember(AccordCacheEntry<?, ?, ?> e, T t)
        {
            return e.unsafeQueuedTasks().contains(t.safeTask);
        }

        /** the driver mutates the entry under test, so its head is read from there */
        private T runnableHead()
        {
            return runnableHead(entry);
        }

        /** only a task that currently leads may take the lock; anything else lockExclusive rejects outright */
        private T runnableHead(AccordCacheEntry<?, ?, ?> e)
        {
            List<SafeTask<?>> members = e.unsafeQueuedTasks();
            if (members.isEmpty() || e.unsafeRunnablePrefix() < 1) throw new Unreachable("nothing leads the entry");
            T t = byTask.get(members.get(0));
            if (t == null) throw new Unreachable("head is not a tracked member");
            return t;
        }

        /**
         * Chosen from the entry's <em>live</em> membership, not the harness's bookkeeping: the latter is updated after each
         * mutation returns, so during a notification it still lists a task part-way through being removed.
         */
        private T targetOther(AccordCacheEntry<?, ?, ?> e, T from)
        {
            List<T> candidates = new ArrayList<>();
            for (SafeTask<?> s : e.unsafeQueuedTasks())
            {
                T t = byTask.get(s);
                if (t != null && t != from) candidates.add(t);
            }
            if (candidates.isEmpty()) throw new Unreachable("no other task is currently a member");
            return candidates.get(target % candidates.size());
        }

        private void inject(AccordCacheEntry<?, ?, ?> into, T from)
        {
            if (where == Where.OTHER_ENTRY && !mutatesTheTargetQueue(action))
                throw new Unreachable("this action does not touch the entry it is directed at, so OTHER_ENTRY repeats SAME_ENTRY");

            switch (action)
            {
                case NONE:
                    break;
                case REMOVE_OTHER:
                {
                    T t = targetOther(into, from);
                    // a member that is also the lock holder can only be removed by whoever owns the lock (see the
                    // REMOVE_HEAD driver); anything else is a removal production cannot perform
                    into.remove(t.safeTask, into.isLockedBy(t.safeTask), AccordCacheEntryQueue.RemoveMode.IF_PRESENT);
                    if (into == entry) queued.remove(t);
                    break;
                }
                case REMOVE_SELF:
                {
                    if (!isMember(into, from)) throw new Unreachable("not a member");
                    into.remove(from.safeTask, into.isLockedBy(from.safeTask), AccordCacheEntryQueue.RemoveMode.IF_PRESENT);
                    if (into == entry) queued.remove(from);
                    break;
                }
                case ADD_FIFO:
                case ADD_SORTED:
                case ADD_BAG:
                {
                    if (into.isLoading()) throw new Unreachable("cannot add to a loading entry");
                    Region r = action == Action.ADD_FIFO ? Region.FIFO
                                                         : action == Action.ADD_SORTED ? Region.SORTED : Region.BAG;
                    place(into, task("inj", r, 15));
                    break;
                }
                case LOCK_HOLD:
                case LOCK_RELEASE_MODE:
                {
                    if (into.isLoading() || into.isLocked()) throw new Unreachable("cannot lock now");
                    // a lock is taken in preExecute, never from a notification, so injecting one into an acquisition in
                    // flight is not a state production can reach
                    if (lockInFlight) throw new Unreachable("a lock acquisition is already in flight");
                    T t = runnableHead(into);
                    // HOLD_QUEUE means keeping an ordered position across runs, so there has to be one to keep: an
                    // unsequenced claim has no position of its own and lockExclusive rejects it outright
                    if (action == Action.LOCK_HOLD && t.safeTask.isUnsequenced())
                        throw new Unreachable("an unsequenced claim cannot hold the queue");
                    // the bare representation re-derives the region from isCacheQueuedFifo, so HOLD_QUEUE requires it
                    if (action == Action.LOCK_HOLD && t.kind != Region.FIFO && into.unsafeQueuedTasks().size() == 1)
                        throw new Unreachable("O7 stamps and moves a task to fifo before it takes HOLD_QUEUE");
                    AccordCacheEntry.LockMode mode = action == Action.LOCK_HOLD ? AccordCacheEntry.LockMode.HOLD_QUEUE
                                                                                : AccordCacheEntry.LockMode.RELEASE_QUEUE;
                    into.lockExclusive(t.safeTask, mode);
                    if (mode == AccordCacheEntry.LockMode.RELEASE_QUEUE && into == entry)
                        queued.remove(t);
                    break;
                }
                case UPGRADE_TO_FIFO:
                {
                    // upgrade-on-start from inside a notification. Only the target entry is moved: the point here is the
                    // reentrant mutation, not the upgrade's cross-entry consistency, which the cycle suites cover.
                    if (into.isLoading()) throw new Unreachable("no fifo region while loading");
                    T t = targetOther(into, from);
                    if (t.kind == Region.FIFO) throw new Unreachable("already a fifo claim, so there is nothing to upgrade");
                    t.kind = Region.FIFO;
                    t.started = true;
                    into.moveToFifo(t.safeTask);
                    ++upgradesFromInsideNotify;
                    break;
                }
                case LOCK_UNQUEUED:
                {
                    // Optimistic referencing, as tryLockCaches does: the lock is taken and given back with *no* queue
                    // accounting. The locker holds no position - tryLockCaches only takes this mode on the
                    // refs.putIfAbsent(..) == null branch, i.e. for an entry it did not already reference, and
                    // lockExclusive now requires exactly that (a position recorded here would be orphaned for ever, R2).
                    // So the property under test is that nobody else's position moves.
                    if (into.isLoading()) throw new Unreachable("cannot lock while loading");
                    if (into.isLocked()) throw new Unreachable("already locked");
                    if (lockInFlight) throw new Unreachable("a lock acquisition is already in flight");
                    T t = task("opt", Region.FIFO, 25);
                    List<SafeTask<?>> before = into.unsafeQueuedTasks();
                    into.lockExclusive(t.safeTask, AccordCacheEntry.LockMode.UNQUEUED);
                    into.remove(t.safeTask, true, null);
                    ++opportunisticLocksFromInsideNotify;
                    // every position must survive the round trip: an opportunistic lock is not a queue operation
                    if (!before.equals(into.unsafeQueuedTasks()))
                        throw new AssertionError("an UNQUEUED lock changed the membership of " + into.key() + ": "
                                                 + names(before) + " -> " + names(into.unsafeQueuedTasks()));
                    if (into.isLocked())
                        throw new AssertionError("an UNQUEUED lock was not given back on " + into.key());
                    break;
                }
                case DRAIN_AND_READD:
                {
                    // always the third, still-loading entry, so L3 permits it either way
                    if (loadingEntry.isLoading() && !loadingEntry.isLocked())
                        ++drainsFromInsideNotify;
                    drainAndReaddLoading();
                    break;
                }
                case RELEASE_LOCK:
                {
                    // what AccordCache.Instance.release does to the queue: unlock, and give up the position too if it
                    // was held across runs
                    if (!into.isLocked()) throw new Unreachable("nothing holds the lock");
                    if (lockInFlight) throw new Unreachable("a lock acquisition is in flight");
                    T holder = byTask.get(into.lockedBy());
                    if (holder == null) throw new Unreachable("lock holder is not tracked");
                    into.remove(holder.safeTask, true, null);
                    ++releasesFromInsideNotify;
                    if (into == entry) queued.remove(holder);
                    break;
                }
            }
        }

        // ---------------- invariants ----------------

        private void check()
        {
            // both the notifying entry and the sibling an OTHER_ENTRY injection mutates
            check(entry);
            check(sibling);
        }

        private void check(AccordCacheEntry<?, ?, ?> e)
        {
            List<SafeTask<?>> members = e.unsafeQueuedTasks();

            // R2: a task appears at most once, or it waits for itself
            for (int i = 0; i < members.size(); ++i)
                for (int j = i + 1; j < members.size(); ++j)
                    if (members.get(i) == members.get(j))
                        throw new AssertionError("R2: " + byTask.get(members.get(i)) + " queued twice on " + e.key()
                                                 + ": " + names(members));

            // the runnable prefix is a prefix
            int runnable = e.unsafeRunnablePrefix();
            if (runnable < 0 || runnable > members.size())
                throw new AssertionError("Q4: runnable prefix " + runnable + " of " + members.size() + " on " + e.key());
        }

        private String names(List<SafeTask<?>> tasks)
        {
            StringBuilder sb = new StringBuilder("[");
            for (SafeTask<?> s : tasks)
            {
                if (sb.length() > 1) sb.append(", ");
                T t = byTask.get(s);
                sb.append(t == null ? "?" : t.name);
            }
            return sb.append(']').toString();
        }
    }
}
