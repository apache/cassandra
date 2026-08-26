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
     * <p>
     * Q1 the sorted prefix is sorted by {@link #compare};
     * Q2 every bag member sorts after every member of the sorted prefix;
     * Q3 bag members are mutually unordered, so none waits for another;
     * Q4 the runnable prefix is the fifo head if any, else the first sorted member if any, else the whole bag;
     * Q5 the fifo region is ordered by {@code fifoAt}, stamped when a task first becomes a fifo claim, except that a
     *    HOLD_QUEUE holder is kept at the head (see {@link #addFifo}); that exception is expected to be unreachable.
     * <p>
     * Q2 lets an arrival that sorts past the prefix be bagged in O(1), and is maintained by {@link #extendPriorityRegion}.
     * The order imposed on any two tasks is a function of the pair alone, so it is the same on every entry they share;
     * this is what makes the scheme deadlock free. Note that Q5 relies on this too: {@code fifoAt} is inherited by an
     * ATOMIC task from its parent, so two tasks can tie, and {@link #addFifo} then breaks the tie by {@code createdAt} -
     * which is a function of the pair, and orders a child after its parent because it is created by the parent's run.
     * <p>
     * Formal model: {@code spec/accord-execution} (Q1-Q5 are cited there as stated here).
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

    /**
     * Whether an arriving {@code task}'s priority places it inside the sorted prefix rather than the bag.
     */
    boolean placeInPriorityRegion(SafeTask<?> task)
    {
        return hasPriority() && compare(task, tasks[priorityTail - 1]) < 0;
    }

    /**
     * Q2: extend the sorted prefix over every bag member that sorts before {@code task}. They all sort after the
     * existing prefix, so we need only sort them amongst themselves and advance the boundary.
     *
     * @return how many were taken; they are then the last {@code count} members of the sorted prefix
     */
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
     * We are not runnable, and if we are the first to queue behind whoever is, tell them: a batched task uses this to
     * run before it has a full batch ({@code NONSYNC_BLOCKED_LIMIT}) and to prefer the keys others are waiting on.
     * Only the no-follower to one-follower transition is reported, so this is O(1) per arrival.
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

    /** Q4 */
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
        // validate (and so validateMembership) already runs before the notification below
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
        // O(tasks.length), and it runs on every mutation, so it needs a compute budget and not merely paranoia
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

        // NOTE: the fifo region is ordered by when each member's enclosing run began - its own first run, or its
        // submitter's for an ATOMIC consequence. This temporal order is consistent on every entry a pair shares, so it
        // cannot cycle, but it is unrelated to compare() and so cannot be validated locally.
        for (int i = fifoHead + 1; i < tasks.length; ++i)
            Invariants.require(tasks[i] == null);

        for (int i = priorityHead + 1; i < priorityTail; ++i) // Q1
            Invariants.require(compare(tasks[i - 1], tasks[i]) <= 0);
        for (int i = priorityTail; i < priorityTail + unsequencedSize; ++i) // Q2
            Invariants.require(prioritySize() == 0 || compare(tasks[priorityTail - 1], tasks[i]) <= 0);

        validateMembership(owner);

        // A failed or cancelled task will never run, so anyone behind it would wait forever; unlike isWaitingOnCaches
        // this applies to the head, and to a non-sync task on a commands-for-key entry
        for (int i = priorityHead; i < priorityTail + unsequencedSize; ++i)
            requireNotFailed(owner, tasks[i], i < priorityTail ? "ordered" : "unsequenced", i);
        for (int i = fifoTail + 1; i <= fifoHead; ++i)
            requireNotFailed(owner, tasks[i], "fifo", i);
        if (tasks[LOCKED_INDEX] != null)
            requireNotFailed(owner, tasks[LOCKED_INDEX], "lock", LOCKED_INDEX);

        // The lock is recorded both in the entry's status bits and in our LOCKED_INDEX slot; readers trust one or the
        // other, so a drift between them invents (or hides) a wait edge. Only meaningful while we are still the entry's
        // queue: a reentrant lockExclusive may have unwrapped and detached us, leaving our lock slot stale.
        if (owner != null && owner.isLiveQueue(this))
        {
            Invariants.require(owner.isLocked() == (tasks[LOCKED_INDEX] != null),
                               "%s: status says locked=%s but LOCKED_INDEX is %s",
                               owner.key(), owner.isLocked(), tasks[LOCKED_INDEX]);
            // HOLD_QUEUE keeps a fifo position across runs and it must be the head, as everything queued behind the
            // holder waits for it. RELEASE_QUEUE and UNQUEUED hold no position, so cannot be checked positionally.
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

    /**
     * Every task that has been added and not removed occupies <em>exactly one</em> slot. No other check catches a
     * duplicate, as they test only occupancy (which slots are null), but a duplicate is fatal: the task waits for
     * itself, and {@code remove} leaves the second copy behind for ever, blocking everything queued behind it.
     * <p>
     * The add methods do not test membership, so this is checked at the mutation that could create a duplicate rather
     * than at the unrelated arrival that would eventually notice. O(n^2), but n is the number of claims on one entry,
     * and it is budgeted separately so the linear checks in {@link #validate} can run at a lower budget.
     */
    private void validateMembership(AccordCacheEntry<?, ?, ?> owner)
    {
        if (!Invariants.testParanoia(Invariants.Paranoia.SUPERLINEAR, Invariants.Paranoia.NONE, Invariants.ParanoiaCostFactor.LOW))
            return;

        // the lock slot is not a queue position, and a HOLD_QUEUE holder occupies it *and* the fifo head, so exclude it
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

    /** the three regions and the lock slot, in wait order */
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

    /**
     * Take a fifo position for a task that has just become fifo, at its first run once it owns all of its locks. The
     * fifo region leads the entry, so everything already in it has run and keeps its precedence: we join at the back,
     * which is where the pair order puts us, and only lead if the region was empty.
     */
    RunnableStatus moveToFifo(AccordCacheEntry<?, ?, ?> owner, SafeTask<?> task)
    {
        boolean wasRunnable = false;
        if (hasFifo())
        {
            for (int i = fifoHead ; i > fifoTail ; --i)
            {
                // we may already have taken our position, in which case nobody's status changes
                if (tasks[i] == task)
                {
                    if (i != fifoHead)
                        return NOT_RUNNABLE;
                    return STILL_RUNNABLE;
                }
            }
            // the fifo region led the entry, so we did not
        }
        else if (hasPriority())
        {
            if (tasks[priorityHead] == task)
            {
                // we already led the entry and still do, so nobody's status changes; notifying here would demote an
                // unsequenced task that has not been runnable since the priority region was created, double counting its wait
                removePriorityHeadNoNotify(task);
                addFifo(null, task);
                return STILL_RUNNABLE;
            }
        }
        else wasRunnable = true;

        // no notification on removal: we are about to take the head ourselves, so any promotion would be transient.
        // addFifo notifies whoever led the entry before us that they have lost the prefix.
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
