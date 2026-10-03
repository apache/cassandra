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

import static org.apache.cassandra.service.accord.execution.AccordExecutor.PRIORITY_BLEND_SHIFT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test was authored by Claude (Anthropic).
 *
 * The BLENDED_PRIORITY_PHASE_FAIR model chooses a fixed share ({@code 1/2^FLOW_SHARE_SHIFT}) of dispatches by flow
 * (the least fairly serviced queue, ties broken round-robin) and the remainder by position (the oldest work).
 */
public class TaskQueueMultiBlendTest
{
    static double priorityBlend() { return 1.0 / (1 << PRIORITY_BLEND_SHIFT); }

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    static final class T extends Task
    {
        T(GlobalGroup group, long position) { super(group); this.position = position; unsafeSetStateExclusive(State.WAITING_TO_RUN); }
        T(ExclusiveGroup group, long position) { super(group); this.position = position; unsafeSetStateExclusive(State.WAITING_TO_RUN); }
        @Override void submitExclusiveMayThrow() {}
        @Override boolean runMayThrow() { return true; }
        @Override void completeExclusiveMayThrow() {}
        @Override void tryCancelExclusive(CancellationException cancelled) {}
        @Override void reportFailureMayThrow(Throwable fail, boolean isExclusive) {}
        @Override AccordExecutor executor() { return null; }
        @Override void unqueueIfQueued() {}
        @Override boolean isNewWork() { return true; }
        @Override String briefDescription() { return "t"; }
        @Override public String description() { return "t"; }
        @Override public void cancel() {}
    }

    static final class Q extends TaskQueueMulti<T>
    {
        Q(GroupKind kind) { super(ExecutorQueue.RUNNABLE, kind, kind == GroupKind.GLOBAL ? AccordExecutor.GLOBAL_QUEUE_LIMITS : AccordExecutor.EXCLUSIVE_QUEUE_LIMITS); }

        T pollAndRun()
        {
            T next = pollMulti();
            if (next != null) next.unsetQueue(kind);
            active = 0;
            return next;
        }
    }

    /**
     * The COMMAND_STORE lane holds the oldest work, but its dispatches exceed its recorded arrivals, so it appears
     * over-serviced: it receives every dispatch chosen by position, and none chosen by flow.
     */
    @Test
    public void overServicedQueueWithOldestWorkReceivesPositionShare()
    {
        Q q = new Q(GroupKind.GLOBAL);
        long oldPosition = 1, newPosition = 1_000_000_000_000L;
        q.enqueueMulti(new T(GlobalGroup.COMMAND_STORE, oldPosition++), false);
        for (int i = 0 ; i < 16 ; ++i)
            q.enqueueMulti(new T(GlobalGroup.OTHER, newPosition++), true);

        int rounds = 100_000, storeRan = 0, warmup = 1000;
        for (int round = 0 ; round < rounds ; ++round)
        {
            T ran = q.pollAndRun();
            if (ran.is(GlobalGroup.COMMAND_STORE))
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
        System.out.printf("COMMAND_STORE (oldest work, over-serviced) received %.1f%% of dispatches (expected %.1f%%)%n", 100 * share, 100 * (1 - priorityBlend()));
        assertEquals(1 - priorityBlend(), share, 0.01);
    }

    /**
     * Groups that are equally fairly serviced share flow dispatches in turn, regardless of group order;
     * the group with the oldest work additionally receives every dispatch chosen by position.
     */
    @Test
    public void flowTiesAreBrokenRoundRobin()
    {
        Q q = new Q(GroupKind.EXCLUSIVE);
        ExclusiveGroup[] groups = { ExclusiveGroup.OUTCOME, ExclusiveGroup.STABLE, ExclusiveGroup.PREACCEPT, ExclusiveGroup.RANGE };
        long[] positions = { 1_000_000, 2_000_000_000L, 3_000_000_000L, 4_000_000_000L };
        // the oldest work belongs to the last group, so that index order and position order disagree
        ExclusiveGroup oldest = ExclusiveGroup.RANGE;
        for (int g = 0 ; g < groups.length ; ++g)
            for (int i = 0 ; i < 4 ; ++i)
                q.enqueueMulti(new T(groups[g], groups[g] == oldest ? 1 + i : positions[g] + i), true);

        int rounds = 100_000;
        int[] ran = new int[ExclusiveGroup.values().length];
        long[] next = positions.clone();
        long nextOldest = 100;
        for (int round = 0 ; round < rounds ; ++round)
        {
            T task = q.pollAndRun();
            int g = task.exclusiveGroupOrdinal();
            ++ran[g];
            q.enqueueMulti(new T(ExclusiveGroup.values()[g], g == oldest.ordinal() ? nextOldest++ : 10 + next[indexOf(groups, g)]++), true);
        }

        double perGroupFlowShare = priorityBlend() / groups.length;
        for (ExclusiveGroup group : groups)
        {
            double share = ran[group.ordinal()] / (double) rounds;
            System.out.printf("%s received %.1f%% of dispatches%n", group, 100 * share);
            double expect = group == oldest ? (1 - priorityBlend()) + perGroupFlowShare : perGroupFlowShare;
            assertEquals(group.toString(), expect, share, 0.01);
        }
    }

    private static int indexOf(ExclusiveGroup[] groups, int ordinal)
    {
        for (int i = 0 ; i < groups.length ; ++i)
            if (groups[i].ordinal() == ordinal)
                return i;
        throw new AssertionError();
    }

    /**
     * Flow and position each receive a fixed share of dispatches whenever they disagree
     */
    @Test
    public void priorityBlendIsFixed()
    {
        assertTrue(AccordExecutor.PRIORITY_BLEND_SHIFT >= 1);
        assertEquals((1 << AccordExecutor.PRIORITY_BLEND_SHIFT) - 1, AccordExecutor.PRIORITY_BLEND_MASK);
    }
}
