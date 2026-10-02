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
import org.apache.cassandra.service.accord.execution.Task.GlobalGroup;

import static org.apache.cassandra.service.accord.execution.TaskPositions.AGE_INHERITED;
import static org.apache.cassandra.service.accord.execution.TaskPositions.AGE_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test was authored by Claude (Anthropic).
 */
public class TaskPositionsTest
{
    static final long HLC = 1_790_000_000_000_000L;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    static class T extends Task
    {
        final boolean onCommandStore;
        T(ExclusiveGroup group, long position) { super(group); this.position = position; this.onCommandStore = true; }
        T(GlobalGroup group) { super(group); this.onCommandStore = false; }
        @Override boolean runsOnCommandStore() { return onCommandStore; }
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

    static T assignNew(TaskPositions positions, T task)
    {
        positions.assignNew(task);
        task.setTranche(0);
        return task;
    }

    static T assignInherited(TaskPositions positions, T task, T parent)
    {
        task.inherit(parent);
        positions.assignInherited(task);
        return task;
    }

    static TaskPositions anchored()
    {
        TaskPositions positions = new TaskPositions();
        assignNew(positions, new T(ExclusiveGroup.DECIDE, HLC));
        return positions;
    }

    @Test
    public void fifoPositionsIncrease()
    {
        TaskPositions positions = new TaskPositions();
        assertEquals(1, assignNew(positions, new T(ExclusiveGroup.OTHER, 0)).position);
        assertEquals(2, assignNew(positions, new T(ExclusiveGroup.OTHER, 0)).position);
    }

    @Test
    public void newerHlcIsKeptAndAdvancesPositions()
    {
        TaskPositions positions = anchored();
        assertEquals(HLC + 1, positions.nextPosition());
        assertEquals(HLC + 1, assignNew(positions, new T(ExclusiveGroup.OTHER, 0)).position);
    }

    @Test
    public void youngWorkKeepsHlc()
    {
        TaskPositions positions = anchored();
        for (ExclusiveGroup group : new ExclusiveGroup[] { ExclusiveGroup.DECIDE, ExclusiveGroup.RECOVER, ExclusiveGroup.RANGE })
        {
            T task = assignNew(positions, new T(group, HLC - AGE_LIMIT + 1));
            assertEquals(HLC - AGE_LIMIT + 1, task.position);
            assertTrue(task.is(group));
        }
    }

    @Test
    public void oldLiveWorkIsQueuedFifo()
    {
        TaskPositions positions = anchored();
        for (ExclusiveGroup group : new ExclusiveGroup[] { ExclusiveGroup.OUTCOME, ExclusiveGroup.STABLE, ExclusiveGroup.DECIDE, ExclusiveGroup.PREACCEPT, ExclusiveGroup.RANGE })
        {
            long next = positions.nextPosition();
            T task = assignNew(positions, new T(group, HLC - AGE_LIMIT - 10));
            assertEquals(next, task.position);
            assertTrue(task.is(group));
            assertFalse(task.isOld());
        }
    }

    @Test
    public void oldRecoveryWorkIsQueuedAsOldRetainingHlc()
    {
        TaskPositions positions = anchored();
        T task = assignNew(positions, new T(ExclusiveGroup.RECOVER, HLC - 10 * AGE_LIMIT));
        assertEquals(HLC - 10 * AGE_LIMIT, task.position);
        assertTrue(task.isOld());
    }

    @Test
    public void consequencesOfOldWorkAreOld()
    {
        TaskPositions positions = anchored();
        T parent = assignNew(positions, new T(ExclusiveGroup.RECOVER, HLC - 10 * AGE_LIMIT));

        // on a command store: queued as OLD, with the parent's position
        T onStore = assignInherited(positions, new T(ExclusiveGroup.DECIDE, 0), parent);
        assertTrue(onStore.isOld());
        assertEquals(parent.position, onStore.position);

        // at the executor level (e.g. a load): retains its group, but is queued FIFO
        long next = positions.nextPosition();
        T load = assignInherited(positions, new T(GlobalGroup.LOAD), parent);
        assertTrue(load.isOld());
        assertTrue(load.is(GlobalGroup.LOAD));
        assertEquals(next, load.position);

        // and so on, transitively
        T grandchild = assignInherited(positions, new T(ExclusiveGroup.OTHER, 0), onStore);
        assertTrue(grandchild.isOld());
        assertEquals(parent.position, grandchild.position);
    }

    @Test
    public void consequencesOfLiveWork()
    {
        TaskPositions positions = anchored();
        T parent = new T(ExclusiveGroup.DECIDE, HLC - AGE_LIMIT / 2);
        assignNew(positions, parent);
        // the parent has aged a lot since it was registered
        assignNew(positions, new T(ExclusiveGroup.DECIDE, HLC + 10 * AGE_LIMIT));

        long next = positions.nextPosition();
        T child = assignInherited(positions, new T(ExclusiveGroup.DECIDE, 0), parent);
        assertFalse(child.isOld());
        assertEquals(AGE_INHERITED ? next : parent.position, child.position);

        T presetup = new T(ExclusiveGroup.DECIDE, 0);
        presetup.setHasPreSetupExclusive();
        assertEquals(parent.position, assignInherited(positions, presetup, parent).position);
    }
}
