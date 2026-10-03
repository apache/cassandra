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

import java.util.function.Consumer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup;
import org.apache.cassandra.service.accord.execution.Task.ExecutorQueue;
import org.apache.cassandra.service.accord.execution.Task.GlobalGroup;
import org.apache.cassandra.service.accord.execution.Task.GroupKind;

import static org.apache.cassandra.service.accord.execution.AccordExecutor.PRIORITY_BLEND_SHIFT;
import static org.apache.cassandra.service.accord.execution.TaskPositions.AGE_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test was authored by Claude (Anthropic).
 *
 * Drives the real {@link TaskQueueMulti} group selection deterministically, modelling one queue: poll one task,
 * "run" it (reset active, as ExclusiveExecutor.completeTask does), and replace it with new work. Positions are
 * assigned by the real {@link TaskPositions}, exactly as {@link AccordExecutor#registerExclusive} does.
 *
 * Previously, each scenario starved its victim indefinitely, as every new arrival in another group (or another
 * command store) carried a position older than the victim's, and the flow imbalance never engaged. Now a fixed share
 * of dispatches is chosen by flow with ties broken round-robin, so every group with work is serviced within a
 * bounded number of dispatches, and old recovery work is queued separately as OLD.
 */
public class TaskQueueMultiStarvationTest
{
    static final long HLC = 1_790_000_000_000_000L;
    static final int BACKLOG = 100;
    static final int MAX_ROUNDS = 1_000_000;
    static int priorityPeriod() { return 1 << PRIORITY_BLEND_SHIFT; }

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    static final class T extends TaskPositionsTest.T
    {
        final String name;
        T(ExclusiveGroup group, long position, String name) { super(group, position); this.name = name; unsafeSetStateExclusive(State.WAITING_TO_RUN); }
        T(GlobalGroup group, long position, String name) { super(group); this.position = position; this.name = name; unsafeSetStateExclusive(State.WAITING_TO_RUN); }
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
        final TaskPositions positions = new TaskPositions();
        final Q q;
        long hlc = HLC; // the HLC of new work: advances by 10us per dispatch

        Sim() { this(GroupKind.EXCLUSIVE); }
        Sim(GroupKind kind) { q = new Q(kind); }

        T submit(ExclusiveGroup group, long age, String name)
        {
            T task = new T(group, hlc - age, name);
            positions.assignNew(task);
            task.setTranche(0);
            return enqueue(task);
        }

        T consequence(ExclusiveGroup group, T parent, String name)
        {
            T task = new T(group, 0, name);
            task.inherit(parent);
            positions.assignInherited(task);
            return enqueue(task);
        }

        T enqueue(T task)
        {
            q.enqueueMulti(task, true);
            return task;
        }

        T pollAndRun()
        {
            hlc += 10;
            return q.pollAndRun();
        }
    }

    /** the number of dispatches before {@code victim} runs, replacing each other task run with {@code replace} */
    static long dispatchesUntil(Sim sim, T victim, Consumer<T> replace)
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

    // a group that is not being serviced has the minimum flow counter, so is serviced within a round-robin cycle of
    // flow dispatches; we allow some slack for the counters of a previously over-serviced group to decay
    static long flowBound(int groups)
    {
        return (long) priorityPeriod() * groups + 1000;
    }

    @Test
    public void control_newerArrivalsDoNotStarve()
    {
        // victim is fresh; the backlog ahead of it in another group is older (but young), and every new arrival is newer
        Sim sim = new Sim();
        T victim = sim.submit(ExclusiveGroup.RANGE, 0, "victim");
        for (int i = 0 ; i < BACKLOG ; ++i)
            sim.submit(ExclusiveGroup.DECIDE, 1000 - i, "d");

        long waited = dispatchesUntil(sim, victim, ran -> sim.submit(ExclusiveGroup.DECIDE, 0, "d"));
        System.out.println("control: victim ran after " + waited + " dispatches");
        assertTrue(waited >= 0 && waited <= BACKLOG);
    }

    /**
     * Every new DECIDE arrival is a consequence of the last to run, so inherits its (old) position, while the victim's
     * group has no further arrivals. Position never favours the victim, but flow services it.
     */
    @Test
    public void olderArrivalsInAnotherGroupAreBounded()
    {
        Sim sim = new Sim();
        T victim = sim.submit(ExclusiveGroup.RANGE, 0, "victim");
        for (int i = 0 ; i < BACKLOG ; ++i)
            sim.submit(ExclusiveGroup.DECIDE, AGE_LIMIT / 2, "d");

        long waited = dispatchesUntil(sim, victim, ran -> sim.consequence(ExclusiveGroup.DECIDE, ran, "d"));
        System.out.println("older arrivals: victim ran after " + waited + " dispatches");
        assertTrue("victim starved", waited >= 0);
        assertTrue("victim waited " + waited, waited <= flowBound(2));
    }

    @Test
    public void victimGroupWithItsOwnBacklogIsServiced()
    {
        Sim sim = new Sim();
        int victims = 64;
        for (int i = 0 ; i < victims ; ++i)
            sim.submit(ExclusiveGroup.RANGE, 0, "victim" + i);
        for (int i = 0 ; i < BACKLOG ; ++i)
            sim.submit(ExclusiveGroup.DECIDE, AGE_LIMIT / 2, "d");

        int rangeRan = 0, round = 0, firstRan = -1;
        for ( ; round < MAX_ROUNDS && rangeRan < victims ; ++round)
        {
            T ran = sim.pollAndRun();
            if (ran.name.startsWith("victim")) { if (rangeRan++ == 0) firstRan = round; }
            else sim.consequence(ExclusiveGroup.DECIDE, ran, "d");
        }
        System.out.println("standing backlog: RANGE dispatched " + rangeRan + " of " + victims + " in " + round + " dispatches (first after " + firstRan + ')');
        assertEquals(victims, rangeRan);
        assertTrue(firstRan <= flowBound(2));
        // RANGE receives at least its round-robin share of flow dispatches
        assertTrue(round <= firstRan + 2L * priorityPeriod() * victims + 1000);
    }

    /**
     * A storm of old recovery/progress work (e.g. CheckStatus for old transactions) is queued as OLD, retaining its
     * (old) HLC priority, so it wins every dispatch chosen by priority; but it is balanced by flow against new work.
     */
    @Test
    public void oldRecoveryStormDoesNotStarveNewWork()
    {
        Sim sim = new Sim();
        sim.submit(ExclusiveGroup.DECIDE, 0, "anchor");
        for (int i = 0 ; i < BACKLOG ; ++i)
            sim.submit(ExclusiveGroup.RECOVER, 10 * AGE_LIMIT, "old");
        for (int i = 0 ; i < BACKLOG ; ++i)
            sim.submit(ExclusiveGroup.PREACCEPT, 0, "new");

        int rounds = 100_000, oldRan = 0, newRan = 0;
        for (int round = 0 ; round < rounds ; ++round)
        {
            T ran = sim.pollAndRun();
            if (ran.name.equals("old")) { ++oldRan; assertTrue(ran.isOld()); sim.submit(ExclusiveGroup.RECOVER, 10 * AGE_LIMIT, "old"); }
            else if (ran.name.equals("new")) { ++newRan; sim.submit(ExclusiveGroup.PREACCEPT, 0, "new"); }
        }
        double newShare = newRan / (double) rounds;
        System.out.printf("old recovery storm: OLD received %.1f%%, new work %.1f%% of dispatches%n", 100.0 * oldRan / rounds, 100 * newShare);
        // new work receives (at least) its round-robin share of flow dispatches
        assertTrue(newShare >= 1.0 / priorityPeriod() / 2 - 0.01);
    }

    /**
     * Live work with an old HLC is queued FIFO, so does not overtake newer live work registered before it
     */
    @Test
    public void oldLiveWorkIsQueuedFifo()
    {
        Sim sim = new Sim();
        sim.submit(ExclusiveGroup.DECIDE, 0, "anchor");
        for (int i = 0 ; i < BACKLOG ; ++i)
            sim.submit(ExclusiveGroup.DECIDE, 10 * AGE_LIMIT, "stale");
        T victim = sim.submit(ExclusiveGroup.DECIDE, 0, "victim");

        long waited = dispatchesUntil(sim, victim, ran -> sim.submit(ExclusiveGroup.DECIDE, 10 * AGE_LIMIT, "stale"));
        System.out.println("stale live work: fresh work ran after " + waited + " dispatches");
        assertTrue(waited >= 0 && waited <= 1 + BACKLOG);
    }

    /**
     * Within OLD, work is processed in approximately age (HLC) order
     */
    @Test
    public void oldWorkIsProcessedInAgeOrder()
    {
        Sim sim = new Sim();
        sim.submit(ExclusiveGroup.DECIDE, 0, "anchor");
        sim.pollAndRun();
        for (int i = 1 ; i <= 10 ; ++i)
            sim.submit(ExclusiveGroup.RECOVER, i * AGE_LIMIT * 2, "old" + i);
        for (int i = 10 ; i >= 1 ; --i)
            assertEquals("old" + i, sim.pollAndRun().name);
    }

    /**
     * At the executor level, a command store is queued in COMMAND_STORE, or in OLD if its next task is OLD.
     * Several stores with OLD work must not starve a store with only new work, though OLD work retains its priority.
     */
    @Test
    public void crossStoreStarvationIsBounded()
    {
        Sim sim = new Sim(GroupKind.GLOBAL);
        T victim = sim.enqueue(new T(GlobalGroup.COMMAND_STORE, sim.hlc, "victim"));
        int stores = 8;
        long oldPosition = HLC - 10 * AGE_LIMIT;
        for (int i = 0 ; i < stores ; ++i)
            sim.enqueue(new T(GlobalGroup.OLD, oldPosition++, "store" + i));

        long position = oldPosition;
        long[] next = { position };
        long waited = dispatchesUntil(sim, victim, ran -> sim.enqueue(new T(GlobalGroup.OLD, next[0]++, ran.name)));
        System.out.println("cross-store: victim store ran after " + waited + " dispatches");
        assertTrue("victim store starved", waited >= 0);
        assertTrue("victim store waited " + waited, waited <= flowBound(2));
    }
}
