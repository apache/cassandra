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

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.AccordConfig;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup;

/**
 * Finalises the position (priority) and classification of each task on registration.
 *
 * <p>A task with an HLC (see {@link Task}'s constructor) is prioritised by it, so long as it is no older than
 * {@link #AGE_LIMIT} relative to the newest position we have assigned. Once older than this:
 * <ul>
 *   <li>recovery and progress work ({@link ExclusiveGroup#RECOVER}) retains its HLC, but is moved to
 *       {@link ExclusiveGroup#OLD}, so that old work is scheduled approximately in age order, but is queued separately
 *       from (and so balanced by flow against) newer work, both within a command store and at the executor level;</li>
 *   <li>all other (live) work is assigned a FIFO position, and is never moved to OLD.</li>
 * </ul>
 * Since all positions outside of OLD are therefore within {@link #AGE_LIMIT} of the position clock when assigned,
 * within any other group a task may only be overtaken by work registered within {@link #AGE_LIMIT} of it.
 *
 * <p>The age of OLD work is halved for each time its transaction has previously been serviced by recovery or progress
 * work, so that a transaction we keep revisiting (which in a healthy system should be rare) does not keep taking
 * priority over other old work; this effect is reset every 64 visits, so that such a transaction is not deprioritised
 * forever (see {@link AccordCacheEntry#recordRecoveryVisit}).
 *
 * <p>Consequences inherit their parent's position and classification (see {@link Task#inherit}): work submitted on
 * behalf of OLD work is itself OLD, and is queued as OLD if it runs on a command store; if instead it runs at the
 * executor level (e.g. loads) it must retain its own group, so is assigned a FIFO position. All other consequences
 * retain their parent's position exactly, unless {@link #AGE_INHERITED}, in which case they are subject to the same
 * rule as live work. NOTE: without {@link #AGE_INHERITED}, a long-lived chain of (live) consequences may retain an
 * old position indefinitely, and so may delay other work in its group (or other command stores, at the executor level).
 */
final class TaskPositions
{
    static final long AGE_LIMIT; // micros
    static final boolean AGE_INHERITED;

    static
    {
        AccordConfig config = DatabaseDescriptor.getAccord();
        AGE_LIMIT = config.queue_priority_age_to_fifo.to(TimeUnit.MICROSECONDS);
        AGE_INHERITED = config.queue_priority_age_inherited;
    }

    // the next FIFO position; greater than every position we have assigned or seen
    long next = 1;

    long nextPosition()
    {
        return next;
    }

    void assignNew(Task task)
    {
        long position = task.position;
        if (position == 0)
        {
            task.position = next++;
        }
        else if (position >= next)
        {
            next = position + 1;
        }
        else if (next - position > AGE_LIMIT)
        {
            if (task.is(ExclusiveGroup.RECOVER))
            {
                task.override(ExclusiveGroup.OLD);
                // halve our effective age for each previous recovery/progress visit to this transaction, so that
                // transactions we have repeatedly serviced do not keep taking priority over other old work
                // (halvings is in 0..63, so is a valid shift)
                int halvings = task.ageHalvings();
                if (halvings > 0)
                    task.position = next - ((next - position) >>> halvings);
            }
            else
            {
                task.position = next++;
            }
        }
    }

    /**
     * The task has already inherited its parent's position and classification
     */
    void assignInherited(Task task)
    {
        if (task.isOld())
        {
            if (!task.runsOnCommandStore())
                task.position = next++;
        }
        else if (AGE_INHERITED && !task.hasPreSetup() && next - task.position > AGE_LIMIT)
        {
            task.position = next++;
        }
    }
}
