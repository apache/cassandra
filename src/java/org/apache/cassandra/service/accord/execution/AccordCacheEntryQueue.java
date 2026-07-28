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

import java.util.Arrays;

import accord.utils.Invariants;
import accord.utils.SortedArrays;
import accord.utils.TriConsumer;

import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.NEWLY_BLOCKING_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.NEWLY_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.NOT_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.STILL_RUNNABLE;

class AccordCacheEntryQueue
{
    static final AccordCacheEntryQueue EMPTY = new AccordCacheEntryQueue(0);

    private static final int DEFAULT_CAPACITY = 4;
    static final int LOCKED_INDEX = 0;
    static final int PRIORITY_START_INDEX = LOCKED_INDEX + 1;
    /**
     * [priorityHead..priorityTail) stores a priority-sorted list of tasks
     * (fifoTail...fifoHead] stores a fifo queue that runs ahead of any priority tasks
     * (fifoTail-unsequencedCount...fifoTail] stores unsequenced tasks that are waiting for a queued incremental task.
     * This only happens for TxnId cache entries, since they may lockAndHoldQueue. Once the lock is released,
     * any pending unsequenced tasks are notified and immediately made (irrevocably) runnable for this entry.
     */
    SafeTask<?>[] tasks;
    // TODO (expected): use bytes/shorts for indexes to keep size down, and have an expanded version of the Queue
    //  with better algorithmic complexity (e.g. Hash -> IntrusivePriorityHeap)
    int priorityHead, priorityTail, fifoHead, fifoTail;
    int unsequencedSize;

    public AccordCacheEntryQueue()
    {
        this(DEFAULT_CAPACITY);
    }

    public AccordCacheEntryQueue(int capacity)
    {
        tasks = new SafeTask[capacity];
        priorityHead = priorityTail = PRIORITY_START_INDEX;
        fifoHead = fifoTail = capacity - 1;
    }

    AccordCacheEntryQueue(AccordCacheEntryQueue copy)
    {
        tasks = copy.tasks.clone();
        priorityHead = copy.priorityHead;
        priorityTail = copy.priorityTail;
        fifoHead = copy.fifoHead;
        fifoTail = copy.fifoTail;
    }

    // returns true if no fifo tasks already queue (i.e. so we become head)
    boolean addFifo(SafeTask<?> task)
    {
        ensureCapacity();
        boolean isHead = fifoHead == fifoTail;
        if (unsequencedSize > 0) // simply displace the unsequence task, as they're an unordered list
            tasks[fifoTail - unsequencedSize] = tasks[fifoTail];
        tasks[fifoTail--] = task;
        validate();
        return isHead;
    }

    boolean addUnsequenced(SafeTask<?> task)
    {
        Invariants.require(task.isUnsequenced());
        ensureCapacity();
        tasks[fifoTail - unsequencedSize++] = task;
        return unsequencedSize == 1 && sequencedSize() == 1;
    }

    boolean isLocked(SafeTask<?> task)
    {
        return tasks[LOCKED_INDEX] == task;
    }

    SafeTask<?> lockedBy()
    {
        return tasks[LOCKED_INDEX];
    }

    boolean removeIfHead(SafeTask<?> task)
    {
        return removeIfFifoHead(task) || removeIfPriorityHead(task);
    }

    boolean removeIfFifoHead(SafeTask<?> task)
    {
        if (!hasFifo())
            return false;

        if (task != tasks[fifoHead])
            return false;
        tasks[fifoHead--] = null;
        return true;
    }

    boolean removeIfPriorityHead(SafeTask<?> task)
    {
        if (!hasPriority())
            return false;

        if (task != tasks[priorityHead])
            return false;
        tasks[priorityHead++] = null;
        return true;
    }

    void lock(SafeTask<?> task)
    {
        tasks[LOCKED_INDEX] = task;
    }

    void unlock(SafeTask<?> task)
    {
        Invariants.require(tasks[LOCKED_INDEX] == task);
        tasks[LOCKED_INDEX] = null;
    }

    // should always return false, as should never be invoked on an empty queue, and returns true only if we're head of the queue
    boolean addPrioritised(SafeTask<?> task)
    {
        ensureCapacity();
        int insertPos = Arrays.binarySearch(tasks, priorityHead, priorityTail, task, AccordCacheEntryQueue::compare);
        if (insertPos < 0)
            insertPos = -1 - insertPos;

        if (priorityHead == PRIORITY_START_INDEX || insertPos > (priorityTail + priorityHead) / 2)
        {
            System.arraycopy(tasks, insertPos, tasks, insertPos + 1, priorityTail - insertPos);
            tasks[insertPos] = task;
            priorityTail++;
        }
        else
        {
            System.arraycopy(tasks, priorityHead, tasks, priorityHead - 1, insertPos - priorityHead);
            tasks[insertPos - 1] = task;
            priorityHead--;
        }

        validate();
        return fifoHead == fifoTail && tasks[priorityHead] == task;
    }

    // should always return false, as should never be invoked on an empty queue, and returns true only if we're head of the queue
    void addWaitingToLoad(SafeTask<?> task)
    {
        ensureCapacity();
        tasks[priorityTail++] = task;
    }

    private boolean hasTailRoom()
    {
        if (priorityTail + unsequencedSize <= fifoTail)
            return true;
        Invariants.require(priorityTail + unsequencedSize == 1 + fifoTail);
        return false;
    }

    private void ensureCapacity()
    {
        if (!hasTailRoom())
        {
            if (fifoHead == fifoTail && unsequencedSize == 0 && fifoTail < tasks.length - 1)
                fifoHead = fifoTail = tasks.length - 1;
            else if (priorityHead == priorityTail && priorityHead > PRIORITY_START_INDEX)
                priorityHead = priorityTail = PRIORITY_START_INDEX;
            else if (totalSize() >= (tasks.length - 1) / 2) compact(new SafeTask[tasks.length * 2]);
            else compact(tasks);
            Invariants.require(hasTailRoom());
        }
    }

    private void compact(SafeTask<?>[] into)
    {
        if (priorityHead == priorityTail) priorityHead = priorityTail = PRIORITY_START_INDEX;
        else
        {
            int priorityLength = priorityTail - priorityHead;
            System.arraycopy(tasks, priorityHead, into, PRIORITY_START_INDEX, priorityLength);
            int newTail = PRIORITY_START_INDEX + priorityLength;
            Invariants.require(newTail <= priorityTail);
            if (into == tasks)
                Arrays.fill(into, newTail, priorityTail, null);
            priorityHead = PRIORITY_START_INDEX;
            priorityTail = newTail;
        }

        if (fifoHead == fifoTail && unsequencedSize == 0) fifoHead = fifoTail = into.length - 1;
        else
        {
            int fifoLength = fifoHead - fifoTail;
            int copyLength = fifoLength + unsequencedSize;
            int copyFrom = (fifoTail - unsequencedSize) + 1;
            int copyTo = into.length - copyLength;
            Invariants.require(copyTo >= copyFrom);
            System.arraycopy(tasks, copyFrom, into, copyTo, copyLength);
            if (into == tasks)
                Arrays.fill(into, copyFrom, copyTo, null);
            fifoHead = into.length - 1;
            fifoTail = fifoHead - fifoLength;
        }

        if (tasks != into)
        {
            into[LOCKED_INDEX] = tasks[LOCKED_INDEX];
            tasks = into;
        }
        validate();
    }

    private void validate()
    {
        for (int i = PRIORITY_START_INDEX; i < priorityHead; ++i)
            Invariants.require(tasks[i] == null);
        for (int i = priorityHead; i < priorityTail; ++i)
            Invariants.require(tasks[i] != null);
        for (int i = priorityTail; i <= fifoTail - unsequencedSize; ++i)
            Invariants.require(tasks[i] == null);
        for (int i = (fifoTail - unsequencedSize) + 1; i <= fifoHead; ++i)
            Invariants.require(tasks[i] != null);
        for (int i = fifoHead + 1; i < tasks.length; ++i)
            Invariants.require(tasks[i] == null);
    }

    SafeTask<?> peek()
    {
        if (hasFifo()) return tasks[fifoHead];
        if (hasPriority()) return tasks[priorityHead];
        return null;
    }

    SafeTask<?> peekFifo()
    {
        return hasFifo() ? tasks[fifoHead] : null;
    }

    // second task
    SafeTask<?> peekBehind()
    {
        int fifoSize = fifoSize();
        if (fifoSize > 1)
            return tasks[fifoHead - 1];
        int priorityIndex = priorityHead + (1 - fifoSize);
        if (priorityIndex < priorityTail)
            return tasks[priorityIndex];
        return null;
    }

    boolean hasFifo()
    {
        return fifoHead != fifoTail;
    }

    boolean hasPriority()
    {
        return priorityHead != priorityTail;
    }

    boolean hasUnsequenced()
    {
        return unsequencedSize > 0;
    }

    int sequencedSize()
    {
        return prioritySize() + fifoSize();
    }

    int unsequencedSize()
    {
        return unsequencedSize;
    }

    int totalSize()
    {
        return sequencedSize() + unsequencedSize;
    }

    int prioritySize()
    {
        return priorityTail - priorityHead;
    }

    int fifoSize()
    {
        return fifoHead - fifoTail;
    }

    // true iff was head
    boolean removeFifoOrPriority(SafeTask<?> task, boolean permitMissing)
    {
        int fifoIndex = fifoIndexOf(task);
        if (fifoIndex >= 0)
        {
            if (fifoIndex == fifoHead)
            {
                tasks[fifoHead--] = null;
                validate();
                return true;
            }
            else
            {
                if (remove(fifoIndex, fifoTail + 1, fifoHead + 1)) ++fifoTail;
                else --fifoHead;
                validate();
                return false;
            }
        }

        int priorityIndex = priorityIndexOf(task);
        Invariants.require(priorityIndex >= 0 || permitMissing);
        if (priorityIndex >= 0)
        {
            if (priorityIndex == priorityHead)
            {
                tasks[priorityHead++] = null;
                return !hasFifo();
            }

            if (remove(priorityIndex, priorityHead, priorityTail)) ++priorityHead;
            else --priorityTail;
            return false;
        }

        return false;
    }

    boolean removeUnsequenced(SafeTask<?> task)
    {
        int unsequencedIndex = unsequencedIndexOf(task);
        if (unsequencedIndex < 0)
            return false;

        --unsequencedSize;
        tasks[unsequencedIndex] = tasks[fifoTail - unsequencedSize];
        tasks[fifoTail - unsequencedSize] = null;
        return true;
    }

    // return true IFF was head
    private boolean removePriority(SafeTask<?> task, boolean permitAbsent)
    {
        int i = priorityIndexOf(task);
        if (i < 0)
        {
            Invariants.require(permitAbsent);
            return false;
        }

        boolean wasHead = i == priorityHead;
        if (remove(i, priorityHead, priorityTail)) ++priorityHead;
        else --priorityTail;
        return wasHead;
    }

    // return true if we move the start forwards, false if we moved the end back
    private boolean remove(int i, int start, int end)
    {
        if (i < (start + end) / 2)
        {
            System.arraycopy(tasks, start, tasks, start + 1, i - start);
            tasks[start] = null;
            return true;
        }
        else
        {
            System.arraycopy(tasks, i + 1, tasks, i, end - (i + 1));
            tasks[end - 1] = null;
            return false;
        }
    }

    boolean contains(SafeTask<?> task)
    {
        return indexOf(task) >= 0;
    }

    private int indexOf(SafeTask<?> task)
    {
        if (tasks[priorityHead] == task)
            return priorityHead;

        if (tasks[fifoHead] == task)
            return fifoHead;

        int i = priorityIndexOf(task);
        if (i >= 0)
            return i;

        return fifoIndexOf(task);
    }

    private int priorityIndexOf(SafeTask<?> task)
    {
        if (priorityTail - priorityHead > 16)
        {
            if (tasks[priorityHead] == task)
                return priorityHead;

            return Arrays.binarySearch(tasks, priorityHead + 1, priorityTail, task, AccordCacheEntryQueue::compare);
        }

        for (int i = priorityHead; i < priorityTail; ++i)
        {
            if (tasks[i] == task)
                return i;
        }
        return -1;
    }

    private int fifoIndexOf(SafeTask<?> task)
    {
        for (int i = fifoHead; i > fifoTail; --i)
        {
            if (tasks[i] == task)
                return i;
        }
        return -1;
    }

    private int unsequencedIndexOf(SafeTask<?> task)
    {
        for (int i = (fifoTail - unsequencedSize) + 1; i <= fifoTail; ++i)
        {
            if (tasks[i] == task)
                return i;
        }
        return -1;
    }

    <P1, P2> int drainUnsequenced(TriConsumer<SafeTask<?>, P1, P2> forEach, P1 p1, P2 p2)
    {
        for (int i = (fifoTail - unsequencedSize) + 1; i <= fifoTail; ++i)
        {
            SafeTask<?> task = tasks[i];
            tasks[i] = null;
            // should not be reentrant
            forEach.accept(task, p1, p2);
        }
        int count = unsequencedSize;
        unsequencedSize = 0;
        return count;
    }

    AccordCacheEntry.RunnableStatus ensureHeadFifo(SafeTask<?> task)
    {
        if (hasFifo())
        {
            Invariants.require(tasks[fifoHead] == task);
            return NOT_RUNNABLE;
        }

        if (tasks[priorityHead] == task)
        {
            tasks[priorityHead++] = null;
            addFifo(task);
            return STILL_RUNNABLE;
        }
        else
        {
            boolean wasPriorityHead = removePriority(task, false);
            boolean isFifoHead = addFifo(task);
            if (!isFifoHead)
                return NOT_RUNNABLE;
            if (wasPriorityHead)
                return STILL_RUNNABLE;
            if (hasPriority() || hasUnsequenced())
                return NEWLY_BLOCKING_RUNNABLE;
            return NEWLY_RUNNABLE;
        }
    }

    static int compare(SafeTask<?> a, SafeTask<?> b)
    {
        Invariants.require(a != null && b != null);
        int c = Long.compare(a.position, b.position);
        if (c == 0)
            c = a.executionContext().executionKind().compareTo(b.executionContext().executionKind());
        if (c == 0)
            c = Long.compare(a.createdAt, b.createdAt);
        return c;
    }

    public boolean hasQueued()
    {
        return hasFifo() || hasPriority();
    }
}
