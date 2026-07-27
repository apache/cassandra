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

import accord.utils.IntrusivePriorityHeap;
import accord.utils.TinyEnumSet;

// has xSingle methods to distinguish from within MultiTaskQueue whether we're invoking the multi or single variation
class TaskQueue<T extends Task> extends IntrusivePriorityHeap<T>
{
    final int states;

    public TaskQueue(int states)
    {
        this.states = states;
    }

    void unqueue(T task)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int compare(T o1, T o2)
    {
        return Long.compare(o1.position, o2.position);
    }

    @Override
    protected final void ensureHeapified()
    {
        super.ensureHeapified();
    }

    protected final boolean requeueSingle(T requeue)
    {
        int oldIndex = updateNode(requeue);
        if (oldIndex < 0)
            return false;

        int newIndex = heapIndex(requeue);
        return Math.min(oldIndex, newIndex) == 0 && heapifiedSize() > 0;
    }

    final T peekSingle()
    {
        ensureHeapified();
        return peekNode();
    }

    final T pollSingle()
    {
        ensureHeapified();
        return pollNode();
    }

    final boolean isEmptySingle()
    {
        return isEmptyInternal();
    }

    final int enqueueSingle(T enqueue)
    {
        return insertNode(enqueue);
    }

    final boolean unqueueSingle(T unqueue)
    {
        int heapIndex = heapIndex(unqueue);
        removeNode(unqueue);
        return heapIndex == 0;
    }

    final boolean tryUnqueueSingle(T remove)
    {
        return removeNodeIfContains(remove);
    }

    final T getSingle(int index)
    {
        return super.getNode(index);
    }

    final boolean isQueuedSingle(T test)
    {
        return containsNode(test);
    }

    @Override
    public String toString()
    {
        return TinyEnumSet.toString(states, Task.State::forOrdinal);
    }
}
