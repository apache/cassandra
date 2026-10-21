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

import static org.apache.cassandra.service.accord.execution.Task.ExecutorQueue.RUNNABLE;

final class TaskQueueRunnable<T extends Task> extends TaskQueueMulti<T>
{
    final TaskQueue<T> assigned;

    TaskQueueRunnable()
    {
        super(RUNNABLE, Task.GroupKind.GLOBAL, AccordExecutor.GLOBAL_QUEUE_LIMITS);
        this.assigned = new TaskQueue<>(RUNNABLE);
    }

    T poll()
    {
        T next = pollMulti();
        if (next == null)
            return null;

        assigned.enqueueSingle(next);
        return next;
    }

    void enqueue(T enqueue, boolean incrementArrivals)
    {
        enqueueMulti(enqueue, incrementArrivals);
    }

    void unqueue(T unqueue)
    {
        if (assigned.isQueuedSingle(unqueue))
        {
            int group = group(unqueue);
            if (group >= 0)
                decrementActive(group);

            unqueue.unsetQueue(kind);
            assigned.unqueueSingle(unqueue);
        }
        else
        {
            super.unqueueMulti(unqueue);
        }
    }

    int waitingOrAssignedCount()
    {
        return waitingCount + assigned.size();
    }

    boolean hasAssignedOrWaiting()
    {
        return waitingCount > 0 || !assigned.isEmptySingle();
    }

    boolean hasAssigned()
    {
        return !assigned.isEmptySingle();
    }

    boolean isAssigned(T task)
    {
        return assigned.isQueuedSingle(task);
    }

    void cleanup(T task)
    {
        if (assigned.tryUnqueueSingle(task))
        {
            int group = group(task);
            if (group >= 0)
                decrementActive(group);
            task.unsetQueue(kind);
        }
    }
}
