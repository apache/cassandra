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

import java.util.concurrent.CancellationException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup;
import org.apache.cassandra.service.accord.execution.Task.ExecutorQueue;
import org.apache.cassandra.service.accord.execution.Task.GlobalGroup;
import org.apache.cassandra.service.accord.execution.Task.GroupKind;

import static org.apache.cassandra.service.accord.execution.PositionClock.BOOST_FADE;
import static org.apache.cassandra.service.accord.execution.PositionClock.BOOST_LIMIT;
import static org.apache.cassandra.service.accord.execution.PositionClock.boost;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test was authored by Claude (Anthropic).
 *
 * Drives the real {@link TaskQueueMulti} group selection deterministically, modelling one queue: poll one task,
 * "run" it (reset active, as ExclusiveExecutor.completeTask does), and replace it with new work, with simulated time
 * advancing by {@link #DISPATCH_NANOS} per dispatch. Every task's position is assigned by the real
 * {@link PositionClock}, exactly as {@link AccordExecutor#registerExclusive} does, from a realistic (epoch micros)
 * HLC and the simulated creation time.
 *
 * Without bounding the priority of old HLCs, each of these scenarios starves its victim indefinitely: every new
 * arrival in another group (or another command store, or in a chain of consequences) carries a position older than
 * the victim's. With {@link PositionClock}, a task may only be overtaken by work registered within
 * {@link PositionClock#BOOST_LIMIT} after it, and only by as much as {@link PositionClock#boost} permits for its age.
 */
public class TaskQueueMultiStarvationTest
{
    static final long DISPATCH_NANOS = 10_000; // each dispatch takes 10us of simulated time
    static final long DISPATCH_MICROS = DISPATCH_NANOS / 1000;
    static final long HLC_EPOCH = 1_790_000_000_000_000L;
    static final int BACKLOG = 100;
    static final int SLACK = 2;
    static final int MAX_ROUNDS = 1_000_000;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    static final class T extends Task
    {
        final String name;
        T(ExclusiveGroup group, String name) { super(group); this.name = name; unsafeSetStateExclusive(State.WAITING_TO_RUN); }
        T(GlobalGroup group, String name) { super(group); this.name = name; unsafeSetStateExclusive(State.WAITING_TO_RUN); }
        @Override void submitExclusiveMayThrow() {}
        @Override boolean runMayThrow() { return true; }
        @Override void completeExclusiveMayThrow() {}
        @Override void tryCancelExclusive(CancellationException cancelled) {}
        @Override void reportFailureMayThrow(Throwable fail) {}
        @Override AccordExecutor executor() { return null; }
        @Override void unqueueIfQueued() {}
        @Override boolean isNewWork() { return true; }
        @Override String briefDescription() { return name; }
        @Override public String description() { return name; }
        @Override public void cancel() {}
    }

    static final class Q extends TaskQueueMulti<T>
    {
        Q(GroupKind kind) { super(ExecutorQueue.RUNNABLE, kind, kind == GroupKind.GLOBAL ? AccordExecutor.GLOBAL_QUEUE_LIMITS : AccordExecutor.EXCLUSIVE_QUEUE_LIMITS); }

        T pollAndRun()
        {
            T next = pollMulti();
            if (next != null) next.unsetQueue(kind);
            active = 0; // as ExclusiveExecutor.completeTask
            return next;
        }
    }

    static final class Sim
    {
        final PositionClock clock = new PositionClock();
        final Q q;
        final long startNanos = 123_456_789_000L; // an arbitrary nanoTime origin, unrelated to the HLC epoch
        long nanos = startNanos;

        Sim() { this(GroupKind.EXCLUSIVE); }
        Sim(GroupKind kind) { q = new Q(kind); }

        long hlcNow() { return HLC_EPOCH + (nanos - startNanos) / 1000; }

        T fresh(ExclusiveGroup group, String name) { return newWork(new T(group, name), hlcNow()); }
        T aged(ExclusiveGroup group, long age, String name) { return newWork(new T(group, name), hlcNow() - age); }
        T aged(GlobalGroup group, long age, String name) { return newWork(new T(group, name), hlcNow() - age); }

        T newWork(T task, long hlc)
        {
            task.position = clock.assignNew(hlc, nanos);
            return enqueue(task);
        }

        T consequence(ExclusiveGroup group, T parent, String name)
        {
            T task = new T(group, name);
            task.position = clock.assignInherited(parent.position, nanos);
            assertTrue(task.position >= parent.position);
            return enqueue(task);
        }

        T enqueue(T task)
        {
            q.enqueueMulti(task, true);
            return task;
        }

        T pollAndRun()
        {
            nanos += DISPATCH_NANOS;
            return q.pollAndRun();
        }
    }

    /** the number of dispatches before {@code victim} runs, replacing each other task run with {@code replace} */
    static long dispatchesUntil(Sim sim, T victim, java.util.function.Consumer<T> replace)
    {
        for (int round = 0 ; round < MAX_ROUNDS ; ++round)
        {
            T ran = sim.pollAndRun();
            if (ran == victim)
                return round;
            replace.accept(ran);
        }
        return -1;
    }

    static long overtakers(long age)
    {
        return boost(age) / DISPATCH_MICROS;
    }

    @Test
    public void control_newerArrivalsDoNotStarve()
    {
        // victim is fresh; the backlog ahead of it is older (but young), and every new arrival is newer
        Sim sim = new Sim();
        T victim = sim.fresh(ExclusiveGroup.RANGE, "victim");
        for (int i = 0 ; i < BACKLOG ; ++i)
            sim.aged(ExclusiveGroup.DECIDE, 1000 - i, "d");

        long waited = dispatchesUntil(sim, victim, ran -> sim.fresh(ExclusiveGroup.DECIDE, "d"));
        System.out.println("control: victim ran after " + waited + " dispatches");
        assertEquals(BACKLOG, waited);
    }

    /**
     * Every new DECIDE arrival is new work older than the victim by {@code age} (e.g. recovery messages carrying an
     * old ballot, or catch-up work); the victim's group has no further arrivals. The victim may be overtaken only by
     * arrivals within {@code boost(age)} after it - i.e. HLC priority is honoured, but only by a bounded amount.
     */
    @Test
    public void olderNewWorkInAnotherGroupIsBounded()
    {
        long[] ages = BOOST_FADE >= 0 ? new long[] { BOOST_LIMIT / 2, BOOST_LIMIT, BOOST_LIMIT + BOOST_FADE / 2, BOOST_LIMIT + BOOST_FADE, 10_000_000 }
                                      : new long[] { BOOST_LIMIT / 2, BOOST_LIMIT, 10_000_000 };
        for (long age : ages)
        {
            Sim sim = new Sim();
            T victim = sim.fresh(ExclusiveGroup.RANGE, "victim");
            for (int i = 0 ; i < BACKLOG ; ++i)
                sim.aged(ExclusiveGroup.DECIDE, age, "d");

            long waited = dispatchesUntil(sim, victim, ran -> sim.aged(ExclusiveGroup.DECIDE, age, "d"));
            System.out.println("older new work (age " + age + "us, boost " + boost(age) + "us): victim ran after " + waited + " dispatches");
            assertTrue("victim starved", waited >= 0);
            assertTrue("victim waited " + waited + " for age " + age, waited <= BACKLOG + overtakers(age) + SLACK);
            assertTrue("victim waited only " + waited + " for age " + age + ": HLC priority not honoured", waited >= overtakers(age) - SLACK);
        }
    }

    /**
     * Every new DECIDE arrival is a consequence of the last DECIDE task to run, so inherits its position: an
     * unbroken chain of consequences of old work. The chain keeps its place while young, but cannot hold its
     * position for longer than {@code BOOST_LIMIT}.
     */
    @Test
    public void consequenceChainIsBounded()
    {
        Sim sim = new Sim();
        T victim = sim.fresh(ExclusiveGroup.RANGE, "victim");
        for (int i = 0 ; i < BACKLOG ; ++i)
            sim.aged(ExclusiveGroup.DECIDE, BOOST_LIMIT / 2, "d");

        long waited = dispatchesUntil(sim, victim, ran -> sim.consequence(ExclusiveGroup.DECIDE, ran, "d"));
        System.out.println("consequence chain: victim ran after " + waited + " dispatches");
        assertTrue("victim starved", waited >= 0);
        assertTrue("victim waited " + waited, waited <= BACKLOG + BOOST_LIMIT / DISPATCH_MICROS + SLACK);
        // while young, the chain keeps its place ahead of the victim
        assertTrue("victim waited only " + waited, waited >= BOOST_LIMIT / 2 / DISPATCH_MICROS);
    }

    /**
     * As {@link #olderNewWorkInAnotherGroupIsBounded}, but the victim's group has a standing backlog of its own
     */
    @Test
    public void victimGroupWithItsOwnBacklogIsServiced()
    {
        Sim sim = new Sim();
        int victims = 64;
        for (int i = 0 ; i < victims ; ++i)
            sim.fresh(ExclusiveGroup.RANGE, "victim" + i);
        for (int i = 0 ; i < BACKLOG ; ++i)
            sim.aged(ExclusiveGroup.DECIDE, BOOST_LIMIT / 2, "d");

        int rangeRan = 0, round = 0, firstRan = -1;
        for ( ; round < MAX_ROUNDS && rangeRan < victims ; ++round)
        {
            T ran = sim.pollAndRun();
            if (ran.name.startsWith("victim")) { if (rangeRan++ == 0) firstRan = round; }
            else sim.aged(ExclusiveGroup.DECIDE, BOOST_LIMIT / 2, "d");
        }
        System.out.println("standing backlog: RANGE dispatched " + rangeRan + " of " + victims + " in " + round + " dispatches (first after " + firstRan + ')');
        assertEquals(victims, rangeRan);
        assertTrue(firstRan <= BACKLOG + overtakers(BOOST_LIMIT / 2) + SLACK);
        // once the old work has lost its priority, the flow arm may still interleave DECIDE work (RANGE is then the
        // over-serviced group), but the RANGE backlog must drain promptly
        assertTrue(round <= firstRan + 4 * victims);
    }

    /**
     * At the executor level, command stores queue in the COMMAND_STORE group by the position of their head task.
     * Several stores that always have old work to do must not starve a store with only new work.
     */
    @Test
    public void crossStoreStarvationIsBounded()
    {
        Sim sim = new Sim(GroupKind.GLOBAL);
        T victim = sim.newWork(new T(GlobalGroup.COMMAND_STORE, "victim"), sim.hlcNow());
        int stores = 8;
        long age = BOOST_LIMIT / 2;
        for (int i = 0 ; i < stores ; ++i)
            sim.aged(GlobalGroup.COMMAND_STORE, age, "store" + i);

        // each time an old store runs a task, it requeues with the position of its next (equally old) task
        long waited = dispatchesUntil(sim, victim, ran -> sim.aged(GlobalGroup.COMMAND_STORE, age, ran.name));
        System.out.println("cross-store: victim store ran after " + waited + " dispatches");
        assertTrue("victim store starved", waited >= 0);
        assertTrue("victim store waited " + waited, waited <= stores + overtakers(age) + SLACK);
        assertTrue("victim store waited only " + waited, waited >= overtakers(age) - SLACK);
    }

    /**
     * A flood of very stale new work (e.g. catch-up) should not delay new work by the full {@code BOOST_LIMIT}:
     * beyond {@code BOOST_LIMIT + BOOST_FADE} it takes no priority, and is queued FIFO with new work.
     */
    @Test
    public void staleFloodDoesNotDominate()
    {
        long stale = 10_000_000;
        Sim sim = new Sim();
        sim.fresh(ExclusiveGroup.DECIDE, "anchor"); // something fresh has been seen
        for (int i = 0 ; i < BACKLOG ; ++i)
            sim.aged(ExclusiveGroup.DECIDE, stale, "stale");
        T victim = sim.fresh(ExclusiveGroup.DECIDE, "victim");

        long waited = dispatchesUntil(sim, victim, ran -> sim.aged(ExclusiveGroup.DECIDE, stale, "stale"));
        System.out.println("stale flood (boost " + boost(stale) + "us): fresh work ran after " + waited + " dispatches");
        if (BOOST_FADE >= 0)
            assertEquals(0, boost(stale));
        assertTrue("fresh work waited " + waited, waited <= 1 + BACKLOG + overtakers(stale) + SLACK);
    }
}
