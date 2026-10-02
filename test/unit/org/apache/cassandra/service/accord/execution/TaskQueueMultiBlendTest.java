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
import org.apache.cassandra.service.accord.execution.Task.ExecutorQueue;
import org.apache.cassandra.service.accord.execution.Task.GlobalGroup;
import org.apache.cassandra.service.accord.execution.Task.GroupKind;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test was authored by Claude (Anthropic).
 *
 * The BLENDED_PRIORITY_PHASE_FAIR model blends choosing by flow (the least fairly serviced queue) with choosing by
 * position (the oldest work). Flow must never take all dispatches, else a queue that appears over-serviced is
 * starved however old its work: at least {@code 1 - 1/2^FLOW_MAX_SHARE_SHIFT} of dispatches are chosen by position.
 */
public class TaskQueueMultiBlendTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    static final class T extends Task
    {
        T(GlobalGroup group, long position) { super(group); this.position = position; unsafeSetStateExclusive(State.WAITING_TO_RUN); }
        @Override void submitExclusiveMayThrow() {}
        @Override boolean runMayThrow() { return true; }
        @Override void completeExclusiveMayThrow() {}
        @Override void tryCancelExclusive(CancellationException cancelled) {}
        @Override void reportFailureMayThrow(Throwable fail) {}
        @Override AccordExecutor executor() { return null; }
        @Override void unqueueIfQueued() {}
        @Override boolean isNewWork() { return true; }
        @Override String briefDescription() { return "t"; }
        @Override public String description() { return "t"; }
        @Override public void cancel() {}
    }

    static final class Q extends TaskQueueMulti<T>
    {
        Q() { super(ExecutorQueue.RUNNABLE, GroupKind.GLOBAL, AccordExecutor.GLOBAL_QUEUE_LIMITS); }

        T pollAndRun()
        {
            T next = pollMulti();
            if (next != null) next.unsetQueue(kind);
            active = 0;
            return next;
        }
    }

    @Test
    public void flowWeightIsCapped()
    {
        int max = AccordExecutor.FLOW_MAX_WEIGHT;
        assertEquals(AccordExecutor.BLEND_TOTAL >>> AccordExecutor.FLOW_MAX_SHARE_SHIFT, max);
        assertTrue(max <= AccordExecutor.BLEND_TOTAL / 2);
        int prev = 0;
        for (int imbalance = 0 ; imbalance <= 127 ; ++imbalance)
        {
            int weight = TaskQueueMulti.flowWeight(imbalance);
            assertTrue(weight >= prev);
            assertTrue(weight <= max);
            if (imbalance <= AccordExecutor.FLOW_ONSET) assertEquals(0, weight);
            if (imbalance >= AccordExecutor.FLOW_ONSET + (1 << AccordExecutor.FLOW_WIDTH_SHIFT)) assertEquals(max, weight);
            prev = weight;
        }
    }

    /**
     * The COMMAND_STORE lane holds the oldest work, but its dispatches exceed its recorded arrivals (as when
     * ExclusiveExecutor requeues itself without an arrival), so it appears over-serviced relative to the OTHER lane,
     * which holds only newer work. The imbalance is sustained, so flow is at its maximum weight throughout; the
     * COMMAND_STORE lane must nonetheless receive at least the position-chosen share of dispatches.
     */
    @Test
    public void overServicedQueueWithOldestWorkIsNotStarved()
    {
        Q q = new Q();
        long oldPosition = 1, newPosition = 1_000_000_000_000L;
        q.enqueueMulti(new T(GlobalGroup.COMMAND_STORE, oldPosition++), false);
        for (int i = 0 ; i < 16 ; ++i)
            q.enqueueMulti(new T(GlobalGroup.OTHER, newPosition++), true);

        int rounds = 100_000, storeRan = 0, warmup = 1000;
        for (int round = 0 ; round < rounds ; ++round)
        {
            T ran = q.pollAndRun();
            if (group(ran) == GlobalGroup.COMMAND_STORE)
            {
                if (round >= warmup) ++storeRan;
                q.enqueueMulti(new T(GlobalGroup.COMMAND_STORE, oldPosition++), false);
            }
            else
            {
                q.enqueueMulti(new T(GlobalGroup.OTHER, newPosition++), true);
            }
        }

        double share = storeRan / (double) (rounds - warmup);
        double minShare = 1.0 - (AccordExecutor.FLOW_MAX_WEIGHT / (double) AccordExecutor.BLEND_TOTAL);
        System.out.printf("COMMAND_STORE (oldest work, over-serviced) received %.1f%% of dispatches (minimum %.1f%%)%n", 100 * share, 100 * minShare);
        assertTrue(share >= minShare - 0.01);
    }

    private static GlobalGroup group(T task)
    {
        return GlobalGroup.values()[(task.info >>> Task.GroupKind.GLOBAL.shift) & Task.GROUP_MASK];
    }
}
