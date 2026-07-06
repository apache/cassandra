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

import accord.utils.ArrayBuffers.BufferList;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus;

import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.NEWLY_BLOCKING_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.NEWLY_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.NOT_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.STILL_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.STILL_RUNNABLE_NEWLY_BLOCKING;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntryQueue.RemoveMode.IF_PRESENT;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntryQueue.RemoveMode.REQUIRE_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.Task.State.LOADING_OPTIONAL;
import static org.apache.cassandra.service.accord.execution.Task.State.LOADING_REQUIRED;
import static org.apache.cassandra.service.accord.execution.Task.State.PREPARED;
import static org.apache.cassandra.service.accord.execution.Task.State.PREPARING;
import static org.apache.cassandra.service.accord.execution.Task.State.REGISTERED;
import static org.apache.cassandra.service.accord.execution.Task.State.SCANNING_RANGES;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_ON_KEY;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_ON_TXN;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_TO_RUN;

class AccordCacheEntryQueue
{
    private static final int DEFAULT_CAPACITY = 4;
    static final int LOCKED_INDEX = 0;
    static final int PRIORITY_START_INDEX = LOCKED_INDEX + 1;

    /**
     * [priorityHead..priorityTail) is the sorted prefix, ordered by {@link #compare}
     * [priorityTail..priorityTail+unsequencedSize) is a bag: mutually unordered, but all sort after the priority region
     * (fifoTail...fifoHead] is a fifo region, ordered by {@link SafeTask#fifoAt}, that runs ahead of both
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

    void onInconsistent(AccordCacheEntry<?, ?, ?> owner)
    {
        // When notified, a task may only remove itself, so we can repair our iteration instead of taking a defensive copy
        {
            int fifoHead = this.fifoHead;
            for (int i = fifoHead - 1 ; i > fifoTail ;)
            {
                SafeTask<?> task = tasks[i];
                task.onInconsistentKeyExclusive(owner);
                if (tasks[i] == task) --i;
                else if (fifoHead != this.fifoHead)
                {
                    --i;
                    fifoHead = this.fifoHead;
                }
            }
        }

        int priorityHead = this.priorityHead;
        for (int i = priorityHead; i < priorityTail + unsequencedSize ;)
        {
            SafeTask<?> task = tasks[i];
            task.onInconsistentKeyExclusive(owner);
            if (tasks[i] == task) ++i;
            else if (priorityHead != this.priorityHead)
            {
                ++i;
                priorityHead = this.priorityHead;
            }
        }
    }

    private void onChangeRunnableStatus(int start, int end, AccordCacheEntry<?, ?, ?> owner, RunnableStatus status)
    {
        if (start + 1 >= end)
        {
            if (start < end)
                tasks[start].onChangeRunnableStatus(owner, status);
            return;
        }

        // require that a reentrant notification has not reordered the range we are iterating
        if (Invariants.testParanoia(Invariants.Paranoia.LINEAR, Invariants.Paranoia.LINEAR, Invariants.ParanoiaCostFactor.LOW))
        {
            try (BufferList<SafeTask<?>> check = new BufferList<>())
            {
                for (int i = start; i < end ; ++i)
                    check.add(tasks[i]);
                onChangeRunnableStatusInternal(start, end, owner, status);
                for (int i = start; i < end ; ++i)
                    Invariants.require(check.get(i - start) == tasks[i]);
            }
        }
        else
        {
            onChangeRunnableStatusInternal(start, end, owner, status);
        }
    }

    private void onChangeRunnableStatusInternal(int start, int end, AccordCacheEntry<?, ?, ?> owner, RunnableStatus status)
    {
        for (int i = start; i < end ; ++i)
            tasks[i].onChangeRunnableStatus(owner, status);
    }

    private void onChangeUnsequencedHeadStatus(AccordCacheEntry<?, ?, ?> owner, RunnableStatus status)
    {
        onChangeRunnableStatus(priorityTail, priorityTail + unsequencedSize, owner, status);
    }

    boolean placeInPriorityRegion(SafeTask<?> task)
    {
        return hasPriority() && compare(task, tasks[priorityTail - 1]) < 0;
    }

    /** Returns how many tasks were moved from unsequenced region */
    int extendPriorityRegion(SafeTask<?> task)
    {
        int count = 0;
        for (int i = priorityTail; i < priorityTail + unsequencedSize; ++i)
        {
            if (compare(tasks[i], task) >= 0)
                continue;

            SafeTask<?> tmp = tasks[priorityTail + count];
            tasks[priorityTail + count] = tasks[i];
            tasks[i] = tmp;
            ++count;
        }
        if (count > 0)
        {
            Arrays.sort(tasks, priorityTail, priorityTail + count, AccordCacheEntryQueue::compare);
            priorityTail += count;
            unsequencedSize -= count;
            validate(null);
        }
        return count;
    }

    /**
     * We are not runnable, and notify the running task if we are the first to queue behind them.
     * Batched tasks may then trigger earlier, and will prioritise us over keys that are not blocked
     */
    private RunnableStatus notRunnableMaybeNewlyBlocked(AccordCacheEntry<?, ?, ?> owner)
    {
        if (owner != null && totalSize() == 2 && owner.isLoaded())
        {
            SafeTask<?> head = peekFifoOrPriority();
            if (head != null)
                head.onChangeRunnableStatus(owner, STILL_RUNNABLE_NEWLY_BLOCKING);
        }
        return NOT_RUNNABLE;
    }

    int runnablePrefix()
    {
        if (hasFifo()) return 1;
        if (hasPriority()) return 1;
        return unsequencedSize;
    }

    boolean isLocked(SafeTask<?> task)
    {
        return tasks[LOCKED_INDEX] == task;
    }

    SafeTask<?> lockedBy()
    {
        return tasks[LOCKED_INDEX];
    }

    void removePriorityHeadNoNotify(SafeTask<?> task)
    {
        Invariants.require(hasPriority() && task == tasks[priorityHead]);
        tasks[priorityHead++] = null;
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

    RunnableStatus addFifo(AccordCacheEntry<?, ?, ?> owner, SafeTask<?> task)
    {
        ensureTailCapacity();
        Invariants.require(task.fifoAt > 0);

        int position = fifoTail;
        while (position < fifoHead)
        {
            SafeTask<?> next = tasks[position + 1];
            if (compareFifo(next, task) < 0)
                break;
            ++position;
        }

        boolean wasEmpty = fifoHead == fifoTail;
        if (position == fifoHead && !wasEmpty && tasks[fifoHead] == lockedBy())
        {
            // this should be unreachable and implies a bug with queue acquisition / fifoAt issuance;
            // it could mean a cycle will form, but we report the error and try to continue
            Invariants.expect(false, "%s (fifoAt=%d, createdAt=%d) would displace lock holder %s (fifoAt=%d, createdAt=%d) from the fifo head of %s: %s",
                              task, task.fifoAt, task.createdAt,
                              tasks[fifoHead], tasks[fifoHead].fifoAt, tasks[fifoHead].createdAt,
                              owner == null ? "?" : owner.key(), describeRegions());
            --position;
        }

        System.arraycopy(tasks, fifoTail + 1, tasks, fifoTail, position - fifoTail);
        tasks[position] = task;
        --fifoTail;

        validateMembership(owner);

        if (position != fifoHead)
        {
            validate(owner);
            return owner == null ? NOT_RUNNABLE : notRunnableMaybeNewlyBlocked(owner);
        }

        if (!wasEmpty)
        {
            if (owner != null && owner.isLoaded())
                tasks[fifoHead - 1].onChangeRunnableStatus(owner, NOT_RUNNABLE);

            validate(owner);
            return NEWLY_BLOCKING_RUNNABLE;
        }

        if (!hasPriority() && !hasUnsequenced())
        {
            validate(owner);
            return NEWLY_RUNNABLE;
        }

        if (owner != null && owner.isLoaded())
        {
            if (hasPriority()) tasks[priorityHead].onChangeRunnableStatus(owner, NOT_RUNNABLE);
            else onChangeUnsequencedHeadStatus(owner, NOT_RUNNABLE);
        }

        validate(owner);
        return NEWLY_BLOCKING_RUNNABLE;
    }

    RunnableStatus addPrioritised(AccordCacheEntry<?, ?, ?> owner, SafeTask<?> task)
    {
        ensureTailCapacity();
        int insertPos;
        if (ensureSorted(owner, task)) insertPos = priorityTail;
        else
        {
            insertPos = Arrays.binarySearch(tasks, priorityHead, priorityTail, task, AccordCacheEntryQueue::compare);
            if (insertPos < 0)
                insertPos = -1 - insertPos;
        }

        boolean isNotHead = insertPos != priorityHead;
        if (priorityHead == PRIORITY_START_INDEX || insertPos > (priorityTail + priorityHead) / 2)
        {
            if (unsequencedSize > 0)
                tasks[priorityTail + unsequencedSize] = tasks[priorityTail];
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

        validate(owner);

        if (hasFifo() || isNotHead)
            return notRunnableMaybeNewlyBlocked(owner);

        if (prioritySize() == 1 && !hasUnsequenced())
            return NEWLY_RUNNABLE;

        if (owner != null && owner.isLoaded())
        {
            if (prioritySize() > 1) tasks[priorityHead + 1].onChangeRunnableStatus(owner, NOT_RUNNABLE);
            else onChangeUnsequencedHeadStatus(owner, NOT_RUNNABLE);
        }

        validate(owner);
        return NEWLY_BLOCKING_RUNNABLE;
    }

    RunnableStatus addUnsequenced(AccordCacheEntry<?, ?, ?> owner, SafeTask<?> task)
    {
        if (placeInPriorityRegion(task))
            return addPrioritised(owner, task);

        ensureTailCapacity();
        tasks[priorityTail + unsequencedSize++] = task;
        validate(owner);

        return hasFifo() || hasPriority() ? notRunnableMaybeNewlyBlocked(owner) : NEWLY_RUNNABLE;
    }

    private boolean ensureSorted(AccordCacheEntry<?, ?, ?> owner, SafeTask<?> task)
    {
        if (unsequencedSize == 0 || placeInPriorityRegion(task))
            return false;

        boolean notify = owner != null && !hasPriority() && !hasFifo();
        Invariants.require(owner == null || owner.isLoaded());
        int prevTail = priorityTail;
        for (int i = priorityTail, end = priorityTail + unsequencedSize; i < end ; ++i)
        {
            if (compare(tasks[i], task) < 0)
            {
                if (i != priorityTail)
                {
                    SafeTask<?> tmp = tasks[i];
                    tasks[i] = tasks[priorityTail];
                    tasks[priorityTail] = tmp;
                }
                priorityTail++;
            }
        }

        if (prevTail != priorityTail)
        {
            unsequencedSize -= (priorityTail - prevTail);
            Arrays.sort(tasks, prevTail, priorityTail, AccordCacheEntryQueue::compare);
            if (notify)
                onChangeRunnableStatus(priorityHead + 1, priorityTail + unsequencedSize, owner, NOT_RUNNABLE);
        }
        return true;
    }

    void addWaitingToLoad(SafeTask<?> task)
    {
        addUnsequenced(null, task);
    }

    private boolean hasTailRoom()
    {
        if (priorityTail + unsequencedSize <= fifoTail)
            return true;
        Invariants.require(priorityTail + unsequencedSize == 1 + fifoTail);
        return false;
    }

    private void ensureTailCapacity()
    {
        if (hasTailRoom())
            return;

        if (fifoHead == fifoTail && fifoTail < tasks.length - 1)
            fifoHead = fifoTail = tasks.length - 1;
        else if (priorityHead == priorityTail && unsequencedSize == 0 && priorityHead > PRIORITY_START_INDEX)
            priorityHead = priorityTail = PRIORITY_START_INDEX; // only if the bag is empty: it lives immediately above
        else if (totalSize() >= (tasks.length - 1) / 2)
            compact(new SafeTask[tasks.length * 2]);
        else
            compact(tasks);

        Invariants.require(hasTailRoom());
    }

    private void compact(SafeTask<?>[] into)
    {
        int queuedLength = (priorityTail + unsequencedSize) - priorityHead;
        if (queuedLength == 0) priorityHead = priorityTail = PRIORITY_START_INDEX;
        else if (priorityHead != PRIORITY_START_INDEX || into != tasks)
        {
            int sortedLength = priorityTail - priorityHead;
            System.arraycopy(tasks, priorityHead, into, PRIORITY_START_INDEX, queuedLength);
            int newEnd = PRIORITY_START_INDEX + queuedLength;
            Invariants.require(newEnd <= priorityTail + unsequencedSize);
            if (into == tasks)
                Arrays.fill(into, newEnd, priorityTail + unsequencedSize, null);
            priorityHead = PRIORITY_START_INDEX;
            priorityTail = PRIORITY_START_INDEX + sortedLength;
        }

        if (fifoHead == fifoTail) fifoHead = fifoTail = into.length - 1;
        else
        {
            int fifoLength = fifoHead - fifoTail;
            int copyFrom = fifoTail + 1;
            int copyTo = into.length - fifoLength;
            if (copyFrom != copyTo || into != tasks)
            {
                Invariants.require(copyTo >= copyFrom);
                System.arraycopy(tasks, copyFrom, into, copyTo, fifoLength);
                if (into == tasks)
                    Arrays.fill(into, copyFrom, copyTo, null);
                fifoHead = into.length - 1;
                fifoTail = fifoHead - fifoLength;
            }
        }

        if (tasks != into)
        {
            into[LOCKED_INDEX] = tasks[LOCKED_INDEX];
            tasks = into;
        }
        validate(null);
    }

    private void validate(AccordCacheEntry<?, ?, ?> owner)
    {
        if (!Invariants.testParanoia(Invariants.Paranoia.LINEAR, Invariants.Paranoia.NONE, Invariants.ParanoiaCostFactor.LOW))
            return;

        for (int i = PRIORITY_START_INDEX; i < priorityHead; ++i)
            Invariants.require(tasks[i] == null);
        for (int i = priorityHead; i < priorityTail + unsequencedSize; ++i)
            Invariants.require(tasks[i] != null);
        for (int i = priorityTail + unsequencedSize; i <= fifoTail; ++i)
            Invariants.require(tasks[i] == null);
        for (int i = fifoTail + 1; i <= fifoHead; ++i)
            Invariants.require(tasks[i] != null);

        for (int i = fifoHead + 1; i < tasks.length; ++i)
            Invariants.require(tasks[i] == null);

        for (int i = priorityHead + 1; i < priorityTail; ++i) // Q1
            Invariants.require(compare(tasks[i - 1], tasks[i]) <= 0);
        for (int i = priorityTail; i < priorityTail + unsequencedSize; ++i) // Q2
            Invariants.require(prioritySize() == 0 || compare(tasks[priorityTail - 1], tasks[i]) <= 0);

        validateMembership(owner);

        for (int i = priorityHead; i < priorityTail + unsequencedSize; ++i)
            requireNotFailed(owner, tasks[i], i < priorityTail ? "ordered" : "unsequenced", i);
        for (int i = fifoTail + 1; i <= fifoHead; ++i)
            requireNotFailed(owner, tasks[i], "fifo", i);
        if (tasks[LOCKED_INDEX] != null)
            requireNotFailed(owner, tasks[LOCKED_INDEX], "lock", LOCKED_INDEX);

        if (owner != null && owner.isLiveQueue(this))
        {
            Invariants.require(owner.isLocked() == (tasks[LOCKED_INDEX] != null),
                               "%s: status says locked=%s but LOCKED_INDEX is %s",
                               owner.key(), owner.isLocked(), tasks[LOCKED_INDEX]);

            if (tasks[LOCKED_INDEX] != null && owner.isLockedHoldingQueue())
                Invariants.require(hasFifo() && tasks[fifoHead] == tasks[LOCKED_INDEX],
                                   "%s: HOLD_QUEUE holder %s is not the fifo head (hasFifo=%s)",
                                   owner.key(), tasks[LOCKED_INDEX], hasFifo());
        }

        if (hasFifo())
        {
            for (int i = fifoTail + 1; i < fifoHead; ++i) // Q1
                requireWaitingOnCaches(owner, tasks[i], "fifo", i);
        }
        if (hasPriority())
        {
            for (int i = priorityHead + (hasFifo() ? 0 : 1); i < priorityTail; ++i)
                requireWaitingOnCaches(owner, tasks[i], "ordered", i);
        }
        if (hasUnsequenced() && (hasPriority() || hasFifo()))
        {
            for (int i = priorityTail; i < priorityTail + unsequencedSize; ++i)
                requireWaitingOnCaches(owner, tasks[i], "unsequenced", i);
        }
    }

    private void validateMembership(AccordCacheEntry<?, ?, ?> owner)
    {
        if (!Invariants.testParanoia(Invariants.Paranoia.SUPERLINEAR, Invariants.Paranoia.NONE, Invariants.ParanoiaCostFactor.LOW))
            return;

        for (int i = priorityHead; i <= fifoHead; ++i)
        {
            if (tasks[i] == null)
                continue;

            for (int j = i + 1; j <= fifoHead; ++j)
            {
                if (tasks[i] != tasks[j])
                    continue;

                throw Invariants.illegalState(String.format("%s (%s) holds two positions on %s, at %d (%s) and %d (%s): %s",
                                                            tasks[i], tasks[i].currentState(),
                                                            owner == null ? "?" : String.valueOf(owner.key()),
                                                            i, regionOf(i), j, regionOf(j), describeRegions()));
            }
        }
    }

    private String regionOf(int index)
    {
        if (index == LOCKED_INDEX) return "lock";
        if (index < priorityTail) return "ordered";
        if (index < priorityTail + unsequencedSize) return "unsequenced";
        return "fifo";
    }

    private String describeRegions()
    {
        StringBuilder out = new StringBuilder("fifo=[");
        for (int i = fifoHead; i > fifoTail; --i)
            out.append(i == fifoHead ? "" : ", ").append(tasks[i]);
        out.append("] ordered=[");
        for (int i = priorityHead; i < priorityTail; ++i)
            out.append(i == priorityHead ? "" : ", ").append(tasks[i]);
        out.append("] unsequenced=[");
        for (int i = priorityTail, end = priorityTail + unsequencedSize; i < end; ++i)
            out.append(i == priorityTail ? "" : ", ").append(tasks[i]);
        return out.append("] lock=").append(tasks[LOCKED_INDEX]).toString();
    }

    private void requireNotFailed(AccordCacheEntry<?, ?, ?> owner, SafeTask<?> task, String region, int index)
    {
        if (!task.isFailed())
            return;

        // if we're inconsistent we permit a failed in-progress task to be queued against us
        // but NOT any tasks that can be failed
        if (owner != null && owner.isInconsistent() && (task.isContinuation() || task.hasIncrementalStarted()))
            return;

        throw Invariants.illegalState(String.format("%s is %s but still holds the %s position at %d of %s, waits=%d/%d",
                                                    task, task.currentState(), region, index,
                                                    owner == null ? "?" : String.valueOf(owner.key()),
                                                    task.waitingForKeyCount(), task.waitingForTxnCount()));
    }

    private void requireWaitingOnCaches(AccordCacheEntry<?, ?, ?> owner, SafeTask<?> task, String region, int index)
    {
        if (isWaitingOnCaches(owner, task))
            return;

        throw Invariants.illegalState(String.format("%s is %s in the %s region at %d of %s, holding=%s, nonSync=%s, waits=%d/%d",
                                                    task, task.currentState(), region, index,
                                                    owner == null ? "?" : String.valueOf(owner.key()),
                                                    isLocked(task), task.isNonSync(),
                                                    task.waitingForKeyCount(), task.waitingForTxnCount()));
    }

    private static boolean isWaitingOnCaches(AccordCacheEntry<?, ?, ?> owner, SafeTask<?> task)
    {
        return (task.isNonSync() && (owner == null || owner.isCommandsForKey()) && task.is(PREPARED))
               || task.is(PREPARING) || task.is(WAITING_TO_RUN) || task.is(WAITING_ON_TXN) || task.is(WAITING_ON_KEY)
               || task.is(LOADING_OPTIONAL) || task.is(LOADING_REQUIRED) || task.is(SCANNING_RANGES) || task.is(REGISTERED);
    }

    SafeTask<?> peekAny()
    {
        Invariants.require(totalSize() == 1);
        if (hasFifo()) return tasks[fifoHead];
        if (hasPriority()) return tasks[priorityHead];
        return tasks[priorityTail];
    }

    SafeTask<?> peekFifoOrPriority()
    {
        if (hasFifo()) return tasks[fifoHead];
        if (hasPriority()) return tasks[priorityHead];
        return null;
    }

    SafeTask<?> peekFifo()
    {
        return hasFifo() ? tasks[fifoHead] : null;
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

    private RunnableStatus ifHead(SafeTask<?> task, SafeTask<?> head)
    {
        if (head != task)
            return NOT_RUNNABLE;
        if (totalSize() == 1)
            return NEWLY_RUNNABLE;
        return NEWLY_BLOCKING_RUNNABLE;
    }

    RunnableStatus statusIfPresent(SafeTask<?> task)
    {
        if (hasFifo())
            return ifHead(task, tasks[fifoHead]);

        if (hasPriority())
            return ifHead(task, tasks[priorityHead]);

        return NEWLY_RUNNABLE;
    }

    enum RemoveMode { IF_PRESENT, REQUIRE_PRESENT, REQUIRE_RUNNABLE }

    private void onNewlyRunnablePrefix(AccordCacheEntry<?, ?, ?> owner)
    {
        if (hasFifo()) tasks[fifoHead].onChangeRunnableStatus(owner, totalSize() > 1 ? NEWLY_BLOCKING_RUNNABLE : NEWLY_RUNNABLE);
        else if (hasPriority()) tasks[priorityHead].onChangeRunnableStatus(owner, prioritySize() + unsequencedSize > 1 ? NEWLY_BLOCKING_RUNNABLE : NEWLY_RUNNABLE);
        else if (unsequencedSize > 0) onChangeUnsequencedHeadStatus(owner, NEWLY_RUNNABLE);
    }

    boolean contains(SafeTask<?> task)
    {
        return fifoIndexOf(task) >= 0 || priorityIndexOf(task) >= 0 || unsequencedIndexOf(task) >= 0;
    }

    /**
     * remove the task, and notify any newly runnable prefix
     */
    void remove(AccordCacheEntry<?, ?, ?> owner, SafeTask<?> task, RemoveMode mode)
    {
        if (hasFifo())
        {
            int fifoIndex = fifoIndexOf(task);
            Invariants.require(mode != REQUIRE_RUNNABLE || fifoIndex == fifoHead);
            if (fifoIndex >= 0)
            {
                if (fifoIndex == fifoHead)
                {
                    tasks[fifoHead--] = null;
                    if (owner != null && owner.isLoaded())
                        onNewlyRunnablePrefix(owner);
                }
                else
                {
                    if (remove(fifoIndex, fifoTail + 1, fifoHead + 1)) ++fifoTail;
                    else --fifoHead;
                }
                validate(owner);
                return;
            }
        }

        removePriorityOrUnsequenced(owner, task, mode);
    }

    void removePriorityOrUnsequenced(AccordCacheEntry<?, ?, ?> owner, SafeTask<?> task, RemoveMode mode)
    {
        int priorityIndex = priorityIndexOf(task);
        int unsequencedIndex = priorityIndex >= 0 ? -1 : unsequencedIndexOf(task);
        switch (mode)
        {
            default: throw UnhandledEnum.unknown(mode);
            case IF_PRESENT:
                break;
            case REQUIRE_PRESENT:
                Invariants.require(priorityIndex >= 0 || unsequencedIndex >= 0);
                break;
            case REQUIRE_RUNNABLE:
                Invariants.require(!hasFifo() && (priorityIndex == priorityHead || (!hasPriority() && unsequencedIndex >= 0)));
                break;
        }

        if (priorityIndex >= 0)
        {
            if (priorityIndex == priorityHead)
            {
                tasks[priorityHead++] = null;
                if (!hasFifo() && owner != null && owner.isLoaded())
                    onNewlyRunnablePrefix(owner);
            }
            else if (remove(priorityIndex, priorityHead, priorityTail))
            {
                priorityHead++;
            }
            else
            {
                --priorityTail;
                if (unsequencedSize > 0)
                {
                    // unsequenced begins at priorityTail, so close the gap the sorted region has left behind it
                    int prevUnsequencedTail = priorityTail + unsequencedSize;
                    tasks[priorityTail] = tasks[prevUnsequencedTail];
                    tasks[prevUnsequencedTail] = null;
                }
            }
        }
        else if (unsequencedIndex >= 0)
        {
            --unsequencedSize;
            int unsequencedTail = priorityTail + unsequencedSize;
            if (unsequencedTail != unsequencedIndex)
                tasks[unsequencedIndex] = tasks[unsequencedTail];
            tasks[unsequencedTail] = null;
        }

        validate(owner);
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
        for (int i = priorityTail; i < priorityTail + unsequencedSize; ++i)
        {
            if (tasks[i] == task)
                return i;
        }
        return -1;
    }

    RunnableStatus moveToFifo(AccordCacheEntry<?, ?, ?> owner, SafeTask<?> task)
    {
        boolean wasRunnable = false;
        if (hasFifo())
        {
            // TODO (expected): we shouldn't be in fifo region, so maybe fail invariant check in this case?
            if (tasks[fifoHead] == task)
                return STILL_RUNNABLE;
        }
        else if (hasPriority())
        {
            if (tasks[priorityHead] == task)
            {
                // already runnable, so perform a quick position swap
                removePriorityHeadNoNotify(task);
                addFifo(null, task);
                return STILL_RUNNABLE;
            }
        }
        else wasRunnable = true;

        // no notification on removal, both because we're transiently changing state and because cannot change any runnable status
        // addFifo notifies prior head (if any) that they're no longer runnable
        remove(null, task, IF_PRESENT);
        RunnableStatus status = addFifo(owner, task);
        return wasRunnable ? translateWasRunnable(status) : status;
    }

    static RunnableStatus translateWasRunnable(RunnableStatus status)
    {
        switch (status)
        {
            default: throw UnhandledEnum.unknown(status);
            case STILL_RUNNABLE_NEWLY_BLOCKING:
            case STILL_RUNNABLE:
            case NOT_RUNNABLE:
                throw UnhandledEnum.invalid(status);
            case NEWLY_BLOCKING_RUNNABLE:
                return STILL_RUNNABLE_NEWLY_BLOCKING;
            case NEWLY_RUNNABLE:
                return STILL_RUNNABLE;
        }
    }

    static int compareForNotify(SafeTask<?> a, SafeTask<?> b)
    {
        boolean isCacheQueuedFifo = a.isCacheQueuedFifo();
        if (isCacheQueuedFifo != b.isCacheQueuedFifo())
            return isCacheQueuedFifo ? -1 : 1;
        if (isCacheQueuedFifo)
            return compareFifo(a, b);
        return compare(a, b);
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

    static int compareFifo(SafeTask<?> a, SafeTask<?> b)
    {
        int c = Long.compare(a.fifoAt, b.fifoAt);
        if (c == 0)
            c = Long.compare(a.createdAt, b.createdAt);
        return c;
    }
}
