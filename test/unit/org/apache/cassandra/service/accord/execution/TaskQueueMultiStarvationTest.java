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
import org.apache.cassandra.service.accord.execution.Task.GroupKind;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test was authored by Claude (Anthropic).
 *
 * Drives the real {@link TaskQueueMulti} group selection (the per-store ExclusiveExecutor's policy) deterministically,
 * modelling one ExclusiveExecutor: poll one task, "run" it (reset active, as ExclusiveExecutor.completeTask does), and
 * replace it with new work. It prints how long a single low-traffic task waits, under the default
 * BLENDED_PRIORITY_PHASE_FAIR balancing, when another group's arrivals carry positions older than it.
 */
public class TaskQueueMultiStarvationTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    static final class T extends Task
    {
        final String name;
        T(ExclusiveGroup group, long position, String name) { super(group); this.position = position; this.name = name; unsafeSetStateExclusive(State.WAITING_TO_RUN); }
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
        Q() { super(ExecutorQueue.RUNNABLE, GroupKind.EXCLUSIVE, AccordExecutor.EXCLUSIVE_QUEUE_LIMITS); }

        T pollAndRun()
        {
            T next = pollMulti();
            if (next != null) next.unsetQueue(kind);
            active = 0; // as ExclusiveExecutor.completeTask
            return next;
        }
    }

    /**
     * One RANGE task at position {@code victimPosition}; a DECIDE backlog that is refilled one-for-one, with each new
     * task at {@code positionOf(i)}. Returns the number of dispatches before the RANGE task runs, or -1.
     */
    static long dispatchesUntilVictimRuns(long victimPosition, java.util.function.LongUnaryOperator positionOf, int backlog, int maxRounds)
    {
        Q q = new Q();
        T victim = new T(ExclusiveGroup.RANGE, victimPosition, "victim");
        q.enqueueMulti(victim, true);
        long next = 0;
        for (int i = 0 ; i < backlog ; ++i)
            q.enqueueMulti(new T(ExclusiveGroup.DECIDE, positionOf.applyAsLong(next++), "d"), true);

        for (int round = 0 ; round < maxRounds ; ++round)
        {
            T ran = q.pollAndRun();
            if (ran == victim)
                return round;
            q.enqueueMulti(new T(ExclusiveGroup.DECIDE, positionOf.applyAsLong(next++), "d"), true);
        }
        return -1;
    }

    @Test
    public void control_newerArrivalsDoNotStarve()
    {
        // victim at 1000; the backlog ahead of it is older (0..99) but every new arrival is newer (>1000)
        long waited = dispatchesUntilVictimRuns(1000, i -> i < 100 ? i : 2000 + i, 100, 1_000_000);
        System.out.println("control: victim ran after " + waited + " dispatches");
        assertEquals(100, waited);
    }

    @Test
    public void olderArrivalsInAnotherGroupStarveIndefinitely()
    {
        // every new DECIDE arrival is older than the victim, e.g. a consequence inheriting an old parent position, or a
        // RECOVER-class message keeping its (old) ballot's hlc under ORIG_HLC_FIFO; the victim's group has no further
        // arrivals. Under a fair policy the victim should still run within a bounded number of dispatches.
        long waited = dispatchesUntilVictimRuns(1_000_000_000L, i -> i, 100, 1_000_000);
        System.out.println("older arrivals: victim ran after " + waited + " dispatches (-1 = never, of 1M)");
        assertTrue("victim starved for 1M dispatches", waited >= 0);
    }

    @Test
    public void victimGroupWithItsOwnBacklogStillStarves()
    {
        // same, but the victim's group has a standing backlog of its own (64 tasks, all newer than the stream), so
        // arrivals there exceed dispatches: the flow arm should see an under-serviced group
        Q q = new Q();
        for (int i = 0 ; i < 64 ; ++i)
            q.enqueueMulti(new T(ExclusiveGroup.RANGE, 1_000_000_000L + i, "victim" + i), true);
        long next = 0;
        for (int i = 0 ; i < 100 ; ++i)
            q.enqueueMulti(new T(ExclusiveGroup.DECIDE, next++, "d"), true);

        int rangeRan = 0, rounds = 1_000_000;
        for (int round = 0 ; round < rounds ; ++round)
        {
            T ran = q.pollAndRun();
            if (ran.name.startsWith("victim")) ++rangeRan;
            else q.enqueueMulti(new T(ExclusiveGroup.DECIDE, next++, "d"), true);
        }
        System.out.println("standing backlog: RANGE dispatched " + rangeRan + " of 64 in " + rounds + " dispatches");
        assertTrue("RANGE group never serviced", rangeRan > 0);
    }
}
