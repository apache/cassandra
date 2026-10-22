/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils.concurrent;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/// Tests of the [OpOrder] group life cycle: barriers must report completion exactly when all operations started
/// before them have finished, no unlinking may be lost when groups finish concurrently or out of order, and
/// unlinked groups must not keep any later groups reachable.
public class OpOrderTest
{
    private static final Field PREV = field("prev");
    private static final Field NEXT = field("next");

    // Can be increased for longer runs.
    private static final long STRESS_MILLIS = 5000;
    private static final int RACE_ITERATIONS = 200_000;
    // Time limit for each race test, so that it does not take too long on machines with few cores.
    private static final long RACE_MILLIS = 2000;
    // Number of spins before waiting threads start yielding, letting the other threads run if there are not enough cores.
    private static final int SPINS_BEFORE_YIELD = 1000;

    private static Field field(String name)
    {
        try
        {
            Field f = OpOrder.Group.class.getDeclaredField(name);
            f.setAccessible(true);
            return f;
        }
        catch (NoSuchFieldException e)
        {
            throw new AssertionError(e);
        }
    }

    private static Object get(Field f, OpOrder.Group group)
    {
        try
        {
            return f.get(group);
        }
        catch (IllegalAccessException e)
        {
            throw new AssertionError(e);
        }
    }

    /// Checks the structure of a group given whether it should be finished (unlinked): unlinked groups must have no
    /// links, and groups that have expired but are not unlinked must still be linked to their successor.
    private static void checkLinks(OpOrder.Group group, boolean expired, boolean finished, String context)
    {
        assertEquals(context, finished, group.isFinished());
        if (finished)
        {
            assertNull(context + ": unlinked group retains its next link", get(NEXT, group));
            assertNull(context + ": unlinked group retains its prev link", get(PREV, group));
            assertTrue(context, group.isOldestLiveGroup());
        }
        else if (expired)
            assertNotNull(context + ": expired group lost its next link before being unlinked", get(NEXT, group));
        else
            assertNull(context + ": current group has a next link", get(NEXT, group));
    }

    @Test
    public void testSimpleBarrier()
    {
        OpOrder order = new OpOrder();
        OpOrder.Group op = order.start();
        OpOrder.Barrier barrier = order.newBarrier();
        barrier.issue();
        assertTrue(barrier.isAfter(op));
        OpOrder.Group later = order.start();
        assertFalse(barrier.isAfter(later));
        assertFalse(barrier.allPriorOpsAreFinished());
        later.close();
        assertFalse(barrier.allPriorOpsAreFinished());
        op.close();
        assertTrue(barrier.allPriorOpsAreFinished());
        barrier.await();
        checkLinks(op, true, true, "first group");
        assertTrue(later.isOldestLiveGroup());
        assertNull(get(PREV, later));
    }

    /// Explores all sequences (up to the given sizes) of starting operations, issuing barriers and closing operations
    /// in any order, and checks the state of every group and barrier against a simple model after every step.
    @Test
    public void testAllSequentialOrders()
    {
        int[] count = new int[1];
        explore(new ArrayList<>(), 4, 3, count);
        // Sanity check that the exploration covers a meaningful number of orders.
        assertTrue("explored " + count[0], count[0] > 10000);
    }

    private static final int START = -1;
    private static final int ISSUE = -2;

    private void explore(List<Integer> actions, int maxOps, int maxBarriers, int[] count)
    {
        // Replay the sequence on a fresh OpOrder, checking the state after every step.
        OpOrder order = new OpOrder();
        List<OpOrder.Group> groups = new ArrayList<>();     // group i is the one created by the i-th barrier
        List<OpOrder.Barrier> barriers = new ArrayList<>();
        List<Integer> opGroup = new ArrayList<>();
        List<OpOrder.Group> opHandle = new ArrayList<>();
        List<Boolean> closed = new ArrayList<>();
        groups.add(order.getCurrent());
        for (int i = 0; i < actions.size(); ++i)
        {
            int action = actions.get(i);
            if (action == START)
            {
                OpOrder.Group g = order.start();
                assertTrue(g == groups.get(barriers.size()));
                opGroup.add(barriers.size());
                opHandle.add(g);
                closed.add(false);
            }
            else if (action == ISSUE)
            {
                OpOrder.Barrier b = order.newBarrier();
                b.issue();
                assertTrue(b.getSyncPoint() == groups.get(barriers.size()));
                barriers.add(b);
                groups.add(order.getCurrent());
            }
            else
            {
                opHandle.get(action).close();
                closed.set(action, true);
            }
            checkState(actions.subList(0, i + 1), groups, barriers, opGroup, closed);
        }
        ++count[0];

        if (opGroup.size() < maxOps)
            explore(append(actions, START), maxOps, maxBarriers, count);
        if (barriers.size() < maxBarriers)
            explore(append(actions, ISSUE), maxOps, maxBarriers, count);
        for (int op = 0; op < opGroup.size(); ++op)
            if (!closed.get(op))
                explore(append(actions, op), maxOps, maxBarriers, count);
    }

    private static List<Integer> append(List<Integer> list, int value)
    {
        List<Integer> result = new ArrayList<>(list);
        result.add(value);
        return result;
    }

    private static void checkState(List<Integer> actions,
                                   List<OpOrder.Group> groups,
                                   List<OpOrder.Barrier> barriers,
                                   List<Integer> opGroup,
                                   List<Boolean> closed)
    {
        String context = "after " + actions;
        boolean allBeforeFinished = true;
        for (int g = 0; g < groups.size(); ++g)
        {
            boolean expired = g < barriers.size();
            boolean opsDone = true;
            for (int op = 0; op < opGroup.size(); ++op)
                if (opGroup.get(op) == g && !closed.get(op))
                    opsDone = false;
            OpOrder.Group group = groups.get(g);
            assertEquals(context + " group " + g, allBeforeFinished, group.isOldestLiveGroup());
            allBeforeFinished &= expired && opsDone;
            checkLinks(group, expired, allBeforeFinished, context + " group " + g);
            if (expired)
            {
                assertEquals(context + " barrier " + g, allBeforeFinished, barriers.get(g).allPriorOpsAreFinished());
                if (allBeforeFinished)
                    barriers.get(g).await(); // must not block
            }
        }
    }

    /// An unlinked group that is still referenced (e.g. by a barrier that is kept around, or because it has been
    /// promoted to an old generation) must not keep groups created after it alive.
    @Test
    public void testUnlinkedGroupsDoNotRetainLaterGroups()
    {
        OpOrder order = new OpOrder();
        OpOrder.Barrier retained = order.newBarrier();
        retained.issue();
        retained.await();

        List<WeakReference<OpOrder.Group>> later = new ArrayList<>();
        for (int i = 0; i < 1000; ++i)
        {
            OpOrder.Group op = order.start();
            OpOrder.Barrier barrier = order.newBarrier();
            barrier.issue();
            later.add(new WeakReference<>(op));
            op.close();
            assertTrue(barrier.allPriorOpsAreFinished());
        }
        assertNull(get(NEXT, retained.getSyncPoint()));

        // All the later groups except the current one should be collectable.
        await().atMost(5, TimeUnit.SECONDS)
               .pollInterval(10, TimeUnit.MILLISECONDS)
               .untilAsserted(() -> {
                   System.gc();
                   int alive = countAlive(later);
                   assertTrue("unlinked groups still reachable: " + alive, alive <= 1);
               });
        assertTrue(retained.allPriorOpsAreFinished());
    }

    private static int countAlive(List<WeakReference<OpOrder.Group>> refs)
    {
        int alive = 0;
        for (WeakReference<OpOrder.Group> ref : refs)
            if (ref.get() != null)
                ++alive;
        return alive;
    }

    /// Two adjacent groups finish at the same time. Whichever ordering their last operations take, both groups must
    /// be unlinked by the time both close() calls return.
    @Test
    public void testAdjacentGroupsFinishingConcurrently() throws Throwable
    {
        runRace((order, setup) -> {
            OpOrder.Group a = order.start();
            OpOrder.Barrier barrierA = order.newBarrier();
            barrierA.issue();
            OpOrder.Group b = order.start();
            OpOrder.Barrier barrierB = order.newBarrier();
            barrierB.issue();
            setup.set(new Object[]{ barrierA, barrierB });
            return new Runnable[]{ a::close, b::close };
        }, (order, setup) -> {
            Object[] s = (Object[]) setup;
            OpOrder.Barrier barrierA = (OpOrder.Barrier) s[0];
            OpOrder.Barrier barrierB = (OpOrder.Barrier) s[1];
            assertTrue(barrierA.allPriorOpsAreFinished());
            assertTrue(barrierB.allPriorOpsAreFinished());
            checkLinks(barrierA.getSyncPoint(), true, true, "first group");
            checkLinks(barrierB.getSyncPoint(), true, true, "second group");
            assertNull(get(PREV, order.getCurrent()));
        });
    }

    /// Three groups whose operations finish at the same time, in any order.
    @Test
    public void testThreeGroupsFinishingConcurrently() throws Throwable
    {
        runRace((order, setup) -> {
            OpOrder.Group[] ops = new OpOrder.Group[3];
            OpOrder.Barrier[] barriers = new OpOrder.Barrier[3];
            for (int i = 0; i < 3; ++i)
            {
                ops[i] = order.start();
                barriers[i] = order.newBarrier();
                barriers[i].issue();
            }
            setup.set(barriers);
            return new Runnable[]{ ops[0]::close, ops[1]::close, ops[2]::close };
        }, (order, setup) -> {
            for (OpOrder.Barrier barrier : (OpOrder.Barrier[]) setup)
            {
                assertTrue(barrier.allPriorOpsAreFinished());
                checkLinks(barrier.getSyncPoint(), true, true, "group");
            }
            assertNull(get(PREV, order.getCurrent()));
        });
    }

    /// A barrier expiring a group races with the group's last operation closing, while an earlier group finishes.
    @Test
    public void testExpireRacingWithClose() throws Throwable
    {
        runRace((order, setup) -> {
            OpOrder.Group first = order.start();
            OpOrder.Barrier barrierA = order.newBarrier();
            barrierA.issue();
            OpOrder.Group second = order.start();
            OpOrder.Barrier barrierB = order.newBarrier();
            setup.set(new Object[]{ barrierA, barrierB });
            return new Runnable[]{ first::close, second::close, barrierB::issue };
        }, (order, setup) -> {
            Object[] s = (Object[]) setup;
            OpOrder.Barrier barrierA = (OpOrder.Barrier) s[0];
            OpOrder.Barrier barrierB = (OpOrder.Barrier) s[1];
            assertTrue(barrierA.allPriorOpsAreFinished());
            assertTrue(barrierB.allPriorOpsAreFinished());
            checkLinks(barrierA.getSyncPoint(), true, true, "first group");
            checkLinks(barrierB.getSyncPoint(), true, true, "second group");
            assertNull(get(PREV, order.getCurrent()));
        });
    }

    interface RaceSetup
    {
        Runnable[] setup(OpOrder order, AtomicReference<Object> state);
    }

    interface RaceCheck
    {
        void check(OpOrder order, Object state);
    }

    private static void spinWait(int spins)
    {
        if (spins < SPINS_BEFORE_YIELD)
            Thread.onSpinWait();
        else
            Thread.yield();
    }

    /// Repeatedly sets up a new OpOrder, and runs the returned actions concurrently, each on its own thread, released
    /// at the same moment. Checks the result after all of them have completed. Stops after [#RACE_ITERATIONS] rounds or
    /// [#RACE_MILLIS] milliseconds, whichever comes first.
    private static void runRace(RaceSetup setup, RaceCheck check) throws Throwable
    {
        AtomicReference<Object> state = new AtomicReference<>();
        AtomicReference<Runnable[]> actions = new AtomicReference<>();
        AtomicInteger round = new AtomicInteger();
        AtomicInteger done = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        OpOrder[] order = new OpOrder[1];

        order[0] = new OpOrder();
        Runnable[] first = setup.setup(order[0], state);
        int threadCount = first.length;
        actions.set(first);
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < threadCount; ++t)
        {
            int index = t;
            Thread thread = new Thread(() -> {
                int seen = 0;
                while (true)
                {
                    int r;
                    for (int spins = 0; (r = round.get()) == seen; ++spins)
                        spinWait(spins);
                    if (r < 0)
                        return;
                    seen = r;
                    try
                    {
                        actions.get()[index].run();
                    }
                    catch (Throwable e)
                    {
                        failure.compareAndSet(null, e);
                    }
                    done.incrementAndGet();
                }
            }, "race-" + t);
            thread.setDaemon(true);
            thread.start();
            threads.add(thread);
        }

        try
        {
            long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RACE_MILLIS);
            for (int i = 1; i <= RACE_ITERATIONS && failure.get() == null && System.nanoTime() < end; ++i)
            {
                done.set(0);
                round.set(i); // release all threads together
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                for (int spins = 0; done.get() < threadCount; ++spins)
                {
                    spinWait(spins);
                    if (System.nanoTime() > deadline)
                        fail("race actions did not complete in round " + i);
                }
                if (failure.get() != null)
                    break;
                check.check(order[0], state.get());

                order[0] = new OpOrder();
                actions.set(setup.setup(order[0], state));
            }
        }
        finally
        {
            round.set(-1);
            for (Thread thread : threads)
                thread.join(10000);
        }
        if (failure.get() != null)
            throw failure.get();
    }

    /// Operations of random duration run concurrently with threads issuing barriers and waiting for them, and with a
    /// thread polling the state of recent barriers. Checks that no barrier completes before all operations it waits
    /// for have finished, that barriers complete in order, that no barrier is left waiting forever, and that all
    /// groups are fully unlinked at the end.
    @Test
    public void testConcurrentOperationsAndBarriers() throws Throwable
    {
        OpOrder order = new OpOrder();
        Map<OpOrder.Group, AtomicInteger> running = new ConcurrentHashMap<>();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<OpOrder.Group> syncPoints = Collections.synchronizedList(new ArrayList<>());
        List<OpOrder.Barrier> recent = new ArrayList<>(); // guarded by itself
        AtomicInteger barriersCompleted = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();

        for (int t = 0; t < 8; ++t)
        {
            threads.add(new Thread(() -> {
                Random rand = new Random(ThreadLocalRandom.current().nextLong());
                while (!stop.get())
                {
                    OpOrder.Group group = order.start();
                    AtomicInteger count = running.computeIfAbsent(group, g -> new AtomicInteger());
                    count.incrementAndGet();
                    int r = rand.nextInt(100);
                    if (r < 5)
                        LockSupport.parkNanos(rand.nextInt(1_000_000)); // occasional long operation
                    else if (r < 50)
                        for (int i = rand.nextInt(1000); i > 0; --i)
                            Thread.onSpinWait();
                    count.decrementAndGet();
                    group.close();
                }
            }, "op-" + t));
        }

        for (int t = 0; t < 3; ++t)
        {
            boolean useAwait = t != 0;
            threads.add(new Thread(() -> {
                Random rand = new Random(ThreadLocalRandom.current().nextLong());
                try
                {
                    while (!stop.get())
                    {
                        OpOrder.Barrier barrier = order.newBarrier();
                        barrier.issue();
                        OpOrder.Group syncPoint = barrier.getSyncPoint();
                        syncPoints.add(syncPoint);
                        synchronized (recent)
                        {
                            recent.add(barrier);
                            if (recent.size() > 64)
                                recent.remove(0);
                        }

                        if (useAwait)
                            barrier.await();
                        else
                            while (!barrier.allPriorOpsAreFinished())
                                LockSupport.parkNanos(rand.nextInt(10_000));

                        // Everything started before the barrier must be complete.
                        for (Map.Entry<OpOrder.Group, AtomicInteger> e : running.entrySet())
                        {
                            if (e.getKey().compareTo(syncPoint) > 0)
                                continue;
                            if (e.getValue().get() != 0)
                                throw new AssertionError("Barrier completed while " + e.getValue().get() +
                                                         " operations started before it are still running");
                            // No new operation can start in this group, so it does not need to be checked again.
                            running.remove(e.getKey());
                        }
                        barriersCompleted.incrementAndGet();
                        if (rand.nextInt(4) == 0)
                            LockSupport.parkNanos(rand.nextInt(100_000));
                    }
                }
                catch (Throwable e)
                {
                    failure.compareAndSet(null, e);
                }
            }, "barrier-" + t));
        }

        threads.add(new Thread(() -> {
            try
            {
                while (!stop.get())
                {
                    // Barriers must complete in order: once a later one is seen complete, all earlier ones must be.
                    List<OpOrder.Barrier> snapshot;
                    synchronized (recent)
                    {
                        snapshot = new ArrayList<>(recent);
                    }
                    snapshot.sort((x, y) -> y.getSyncPoint().compareTo(x.getSyncPoint())); // latest first
                    boolean laterFinished = false;
                    for (OpOrder.Barrier barrier : snapshot)
                    {
                        boolean finished = barrier.allPriorOpsAreFinished();
                        if (laterFinished && !finished)
                            throw new AssertionError("Barrier completed before an earlier one");
                        laterFinished |= finished;
                    }
                    Thread.onSpinWait();
                }
            }
            catch (Throwable e)
            {
                failure.compareAndSet(null, e);
            }
        }, "monitor"));

        for (Thread thread : threads)
        {
            thread.setDaemon(true);
            thread.start();
        }
        // Run until the time is up, or a thread fails.
        long deadline = System.currentTimeMillis() + STRESS_MILLIS;
        await().atMost(STRESS_MILLIS + 10_000, TimeUnit.MILLISECONDS)
               .pollInterval(100, TimeUnit.MILLISECONDS)
               .until(() -> failure.get() != null || System.currentTimeMillis() >= deadline);
        stop.set(true);
        for (Thread thread : threads)
        {
            thread.join(30000);
            if (thread.isAlive())
            {
                StringBuilder stack = new StringBuilder();
                for (StackTraceElement e : thread.getStackTrace())
                    stack.append("\n  at ").append(e);
                fail("Thread " + thread.getName() + " did not complete; a barrier probably never completed:" + stack);
            }
        }
        if (failure.get() != null)
            throw failure.get();
        assertTrue(barriersCompleted.get() > 100);

        // Once everything has finished, all groups but the current one must be unlinked.
        order.awaitNewBarrier();
        OpOrder.Group current = order.getCurrent();
        assertNull(get(PREV, current));
        for (OpOrder.Group group : running.keySet())
            if (group != current)
                checkLinks(group, true, true, "group after test");
        for (OpOrder.Group group : syncPoints)
            checkLinks(group, true, true, "barrier sync point after test");
    }
}
