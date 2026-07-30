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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import accord.local.SafeState;
import accord.utils.ArrayBuffers.BufferList;
import accord.utils.IntrusiveLinkedList;
import accord.utils.IntrusiveLinkedListNode;
import accord.utils.Invariants;
import accord.utils.TriFunction;
import accord.utils.UnhandledEnum;
import accord.utils.async.Cancellable;

import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.execution.AccordCache.Adapter;
import org.apache.cassandra.service.accord.execution.AccordCache.Adapter.Shrink;
import org.apache.cassandra.service.accord.execution.AccordCacheEntryQueue.RemoveMode;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode.HOLD_QUEUE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode.UNLOCKED;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode.UNQUEUED;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.NEWLY_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.NOT_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.STILL_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status.EVICTED;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status.FAILED_TO_LOAD;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status.FAILED_TO_SAVE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status.LOADED;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status.LOADING;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status.MODIFIED;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status.SAVING;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status.WAITING_TO_LOAD;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status.WAITING_TO_SAVE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntryQueue.PRIORITY_START_INDEX;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntryQueue.RemoveMode.IF_PRESENT;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntryQueue.RemoveMode.REQUIRE_PRESENT;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntryQueue.RemoveMode.REQUIRE_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.CACHE_QUEUES_ENABLED;

/**
 * Global (per CommandStore) state of a cached entity (Command or CommandsForKey).
 */
public class AccordCacheEntry<K, V, S extends SafeState<V> & SaferState<K, V, S>> extends IntrusiveLinkedListNode
{
    public enum Status
    {
        UNINITIALIZED,

        UNUSED1, // spacing to permit easier bit masks

        WAITING_TO_LOAD(UNINITIALIZED),
        LOADING(WAITING_TO_LOAD),

        /**
         * Consumers should never see this state
         */
        FAILED_TO_LOAD(LOADING),

        UNUSED2, // spacing to permit easier bit masks
        UNUSED3, // spacing to permit easier bit masks
        UNUSED4, // spacing to permit easier bit masks

        LOADED(true, false, UNINITIALIZED, LOADING),
        MODIFIED(true, false, LOADED),

        UNUSED5, // spacing to permit easier bit masks
        UNUSED6, // spacing to permit easier bit masks

        WAITING_TO_SAVE(true, true, MODIFIED),
        SAVING(true, true, MODIFIED, WAITING_TO_SAVE),

        /**
         * Attempted to save but failed. Shouldn't normally happen unless we have a bug in serialization,
         * or commit log has been stopped.
         */
        FAILED_TO_SAVE(true, true, SAVING),

        UNUSED7, // spacing to permit easier bit masks

        EVICTED(WAITING_TO_LOAD, LOADING, LOADED, FAILED_TO_LOAD),
        ;

        static final Status[] VALUES = values();
        static
        {
            MODIFIED.permittedFrom |= 1 << MODIFIED.ordinal();
            MODIFIED.permittedFrom |= 1 << SAVING.ordinal();
            MODIFIED.permittedFrom |= 1 << FAILED_TO_SAVE.ordinal();
            LOADED.permittedFrom |= 1 << SAVING.ordinal();
            LOADED.permittedFrom |= 1 << MODIFIED.ordinal();
            LOADED.permittedFrom |= 1 << WAITING_TO_SAVE.ordinal(); // if nothing to do when saving then we return to LOADED directly
            for (Status status : VALUES)
            {
                if (status.name().startsWith("UNUSED")) continue;
                Invariants.require((status.ordinal() & IS_LOADED) != 0 == status.loaded);
                Invariants.require(((status.ordinal() & IS_LOADED) != 0 && (status.ordinal() & IS_NESTED) != 0) == status.nested);
                Invariants.require(((status.ordinal() & IS_LOADING_OR_WAITING_MASK) == IS_LOADING_OR_WAITING) == (status == LOADING || status == WAITING_TO_LOAD));
                Invariants.require(((status.ordinal() & IS_SAVING_OR_WAITING_MASK) == IS_SAVING_OR_WAITING) == (status == SAVING || status == WAITING_TO_SAVE));
            }
        }

        final boolean loaded;
        final boolean nested;
        int permittedFrom;

        Status(Status ... statuses)
        {
            this(false, false, statuses);
        }

        Status(boolean loaded, boolean nested, Status ... statuses)
        {
            this.loaded = loaded;
            this.nested = nested;
            for (Status status : statuses)
                permittedFrom |= 1 << status.ordinal();
        }
    }

    static final int STATUS_MASK          = 0x0000001F;
    static final int NO_EVICT             = 0x00000020;
    static final int SHRUNK               = 0x00000040;
    static final int LOCKED_MASK          = 0x00000180;
    static final int LOCKED_SHIFT         = Integer.numberOfTrailingZeros(LOCKED_MASK);
    static final int LOCKED_HOLDING_QUEUE = HOLD_QUEUE.ordinal() << LOCKED_SHIFT;
    static final int IS_NOT_EVICTED = 0xF;
    static final int IS_LOADED = 0x8;
    static final int IS_NESTED = 0x4;
    static final int IS_LOADING_OR_WAITING_MASK = 0x6;
    static final int IS_LOADING_OR_WAITING = 0x2;
    static final int IS_SAVING_OR_WAITING_MASK = 0xE;
    static final int IS_SAVING_OR_WAITING = 0xC;
    static final int GENERATION_SHIFT = 9;
    static final int GENERATION_MASK = 0x7fff;
    static final int AGE_SHIFT = 24;
    static final int AGE_MASK = 0xff;

    static final long EMPTY_SIZE = ObjectSizes.measure(new AccordCacheEntry<>(null, null));

    private final K key;
    final AccordCache.Type<K, V, S>.Instance owner;

    private Object state;
    /**
     * Either a single SafeTask or a Queue object. The meaning of a single task is defined by various flags.
     * If locked, then the task is not logically part of the queue unless LOCKED_HOLDING_QUEUE.
     * If unlocked, or LOCKED_HOLDING_QUEUE, the task represents a single-item queue.
     * If the task forms a single-item queue, whether it is FIFO or prioritised is determined by the task's isCacheQueuedFifo flag.
     */
    private Object queue;
    private int status;
    int sizeOnHeap;
    private volatile int references;
    private static final AtomicIntegerFieldUpdater<AccordCacheEntry> referencesUpdater = AtomicIntegerFieldUpdater.newUpdater(AccordCacheEntry.class, "references");

    AccordCacheEntry(K key, AccordCache.Type<K, V, S>.Instance owner)
    {
        this.key = key;
        this.owner = owner;
    }

    // TODO (expected): don't unwrap when only one entry, since this may cause us to flap when locking unsequenced tasks
    private void maybeUnwrap(AccordCacheEntryQueue q)
    {
        // q.remove notifies the new head, and a notification can reentrantly lock this queue which could
        // replace this queue if it were empty; to avoid errors we make sure we are replacing the correct queue

        int size = q.totalSize();
        switch (size)
        {
            case 0:
                if (q == queue)
                    queue = q.lockedBy();
                break;

            case 1:
                SafeTask<?> head = q.peekAny();
                if ((!isLocked() || q.lockedBy() == head) && q == queue)
                    queue = head;
        }
    }

    private boolean maybeUnwrap(AccordCacheEntryQueue q, SafeTask<?> lockedBy)
    {
        if (q.totalSize() == 0)
        {
            queue = lockedBy;
            return true;
        }
        return false;
    }

    private AccordCacheEntryMiniQueue miniQueue()
    {
        Invariants.require(isLocked(), "a mini queue must not outlive the lock it was created for");
        return (AccordCacheEntryMiniQueue) queue;
    }

    // assumes already queued with priority
    final RunnableStatus moveToFifo(SafeTask<?> task)
    {
        if (queue == task)
            return STILL_RUNNABLE;

        if (queue instanceof AccordCacheEntryMiniQueue)
        {
            AccordCacheEntryMiniQueue q = miniQueue();
            if (task == q.lockedBy)
                return STILL_RUNNABLE;

            Invariants.require(task == q.next);
            return isLockedHoldingQueue() ? NOT_RUNNABLE : STILL_RUNNABLE;
        }

        Invariants.require(queue instanceof AccordCacheEntryQueue,
                           "%s cannot move to fifo on %s: queue is %s", task, key, queue == null ? "null" : queue.getClass().getSimpleName() + " " + queue);
        AccordCacheEntryQueue q = (AccordCacheEntryQueue) queue;
        return q.moveToFifo(this, task);
    }

    // drains ONLY those queued with addWaitingToLoad; addFifo are included in the result but are not removed from the collection
    public final BufferList<SafeTask<?>> drainWaitingToLoad()
    {
        Invariants.require(isLoading());
        Invariants.require(!isLocked());
        BufferList<SafeTask<?>> list = new BufferList<>();
        if (queue != null)
        {
            if (queue instanceof AccordCacheEntryQueue)
            {
                AccordCacheEntryQueue q = (AccordCacheEntryQueue) queue;
                for (int i = q.priorityHead ; i < q.priorityTail + q.unsequencedSize ; ++i)
                {
                    list.add(q.tasks[i]);
                    q.tasks[i] = null;
                }
                q.priorityHead = q.priorityTail = PRIORITY_START_INDEX;
                q.unsequencedSize = 0;
                for (int i = q.fifoHead ; i > q.fifoTail ; --i)
                    list.add(q.tasks[i]);

                maybeUnwrap(q);
            }
            else
            {
                // a mini queue exists only while locked, and this requires !isLocked(), so it cannot be one here
                Invariants.require(!(queue instanceof AccordCacheEntryMiniQueue), "a locked entry has no waiters to drain");
                SafeTask<?> task = (SafeTask<?>) queue;
                list.add(task);
                Invariants.require(!isLocked());
                if (!task.isCacheQueuedFifo())
                    queue = null;
            }
        }
        return list;
    }

    final boolean contains(SafeTask<?> task)
    {
        if (queue instanceof AccordCacheEntryQueue)
            return ((AccordCacheEntryQueue) queue).contains(task);
        if (queue instanceof AccordCacheEntryMiniQueue)
            return miniQueue().contains(this, task);
        return queue == task;
    }

    final void remove(SafeTask<?> task, boolean ownsLock, @Nullable RemoveMode removeMode)
    {
        if (queue instanceof AccordCacheEntryQueue)
        {
            AccordCacheEntryQueue q = (AccordCacheEntryQueue) queue;
            boolean isLocked = isLocked() && q.isLocked(task);
            Invariants.require(isLocked || !ownsLock);
            boolean remove = !isLocked;
            if (isLocked)
            {
                Invariants.expect(ownsLock);
                // if locked, we've already released unsequenced/priority/fifo positions unless isLockedHoldingQueue
                remove = isLockedHoldingQueue();
                status &= ~LOCKED_MASK;
                q.unlock(task);
            }

            if (remove)
                q.remove(this, task, removeMode(removeMode, task));

            if (remove || isLocked)
                maybeUnwrap(q);
        }
        else if (queue instanceof AccordCacheEntryMiniQueue)
        {
            AccordCacheEntryMiniQueue q = miniQueue();
            boolean isLocked = isLocked() && q.lockedBy == task;
            Invariants.require(isLocked || !ownsLock);
            if (isLocked)
            {
                Invariants.expect(ownsLock);
                boolean wasLockedHoldingQueue = isLockedHoldingQueue();
                queue = q.next;
                status &= ~LOCKED_MASK;
                if (wasLockedHoldingQueue)
                    q.next.onChangeRunnableStatus(this, NEWLY_RUNNABLE);
            }
            else
            {
                if (q.next == task) queue = q.lockedBy;
                else Invariants.require(removeMode(removeMode, task) == IF_PRESENT);
            }
        }
        else
        {
            boolean isLocked = isLocked();
            Invariants.require(isLocked || !ownsLock);
            if (queue != task)
            {
                Invariants.require(!ownsLock);
                Invariants.require(removeMode(removeMode, task) == IF_PRESENT);
                return;
            }

            if (isLocked)
                status &= ~LOCKED_MASK;
            queue = null;
        }
    }

    private RemoveMode removeMode(RemoveMode param, SafeTask<?> task)
    {
        return param != null ? param : CACHE_QUEUES_ENABLED &&  task.isCacheQueued() && !is(FAILED_TO_LOAD) ? REQUIRE_PRESENT : IF_PRESENT;
    }

    final boolean isCommandsForKey()
    {
        return getClass() == SaferCommandsForKey.CommandsForKeyCacheEntry.class;
    }

    final RunnableStatus statusIfPresent(SafeTask<?> task)
    {
        if (queue == task)
        {
            return NEWLY_RUNNABLE;
        }

        if (queue instanceof AccordCacheEntryMiniQueue)
            return miniQueue().head(this) == task ? NEWLY_RUNNABLE : NOT_RUNNABLE;

        Invariants.require(queue instanceof AccordCacheEntryQueue);
        AccordCacheEntryQueue q = (AccordCacheEntryQueue) queue;
        Invariants.paranoid(q.contains(task));

        return q.statusIfPresent(task);
    }

    private AccordCacheEntryQueue ensureQueue()
    {
        if (queue instanceof AccordCacheEntryQueue)
            return (AccordCacheEntryQueue) queue;

        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        if (queue instanceof AccordCacheEntryMiniQueue)
        {
            AccordCacheEntryMiniQueue prevq = miniQueue();
            if (isLockedHoldingQueue())
                q.addFifo(null, prevq.lockedBy);
            q.lock(prevq.lockedBy);
            addToQueue(q, prevq.next);
        }
        else
        {
            SafeTask<?> head = (SafeTask<?>) this.queue;
            if (isLocked())
            {
                if (isLockedHoldingQueue())
                    q.addFifo(null, head);
                q.lock(head);
            }
            else addToQueue(q, head);
        }

        this.queue = q;
        return q;
    }

    private void addToQueue(AccordCacheEntryQueue q, SafeTask<?> item)
    {
        if (item.isCacheQueuedFifo()) q.addFifo(null, item);
        else if (item.isUnsequenced(this)) q.addUnsequenced(null, item);
        else q.addPrioritised(null, item);
    }

    final void addWaitingToLoad(SafeTask<?> task)
    {
        Invariants.require(isLoading());
        if (queue == null) queue = task;
        else ensureQueue().addWaitingToLoad(task);
    }

    final int waitingCount()
    {
        Invariants.require(isLoading());
        // totalSize(), not sequencedSize(): while loading, waiters are unsequenced rather than ordered
        if (queue == null)
            return 0;
        if (queue instanceof AccordCacheEntryQueue)
            return ((AccordCacheEntryQueue) queue).totalSize();
        if (queue instanceof AccordCacheEntryMiniQueue)
            return isLockedHoldingQueue() ? 2 : 1;
        return isLocked() == isLockedHoldingQueue() ? 1 : 0;
    }

    public enum RunnableStatus
    {
        NOT_RUNNABLE,
        STILL_RUNNABLE,
        STILL_RUNNABLE_NEWLY_BLOCKING,
        NEWLY_RUNNABLE,
        NEWLY_BLOCKING_RUNNABLE,
    }

    // RunnableStatus IF LOADED
    private RunnableStatus add(SafeTask<?> task, TriFunction<AccordCacheEntryQueue, AccordCacheEntry<?, ?, ?>, SafeTask<?>, RunnableStatus> add)
    {
        Object prev = this.queue;
        if (prev == null)
        {
            queue = task;
            return NEWLY_RUNNABLE;
        }

        if (isLocked() && prev instanceof SafeTask<?>)
        {
            queue = new AccordCacheEntryMiniQueue((SafeTask<?>) prev, task);
            return isLockedHoldingQueue() ? NOT_RUNNABLE : NEWLY_RUNNABLE;
        }

        // nothing is runnable while we load, so a claim that arrives meanwhile - a fifo task queues on a loading entry -
        // must not change anyone's status; the drain on load completion re-adds everyone
        boolean isLoaded = isLoaded();
        return add.apply(ensureQueue(), isLoaded ? this : null, task);
    }

    final RunnableStatus addPrioritised(SafeTask<?> task)
    {
        Invariants.require(!isLoading());
        return add(task, AccordCacheEntryQueue::addPrioritised);
    }

    final RunnableStatus addUnsequenced(SafeTask<?> task)
    {
        Invariants.require(!isLoading());
        return add(task, AccordCacheEntryQueue::addUnsequenced);
    }

    final RunnableStatus addFifo(SafeTask<?> task)
    {
        return add(task, AccordCacheEntryQueue::addFifo);
    }

    public enum LockMode
    {
        /**
         * Invalid as a parameter to lock methods, but represents the unlocked state
         */
        UNLOCKED,

        /**
         * If we're sequenced, remove ourselves from the relevant queue so that the next task can queue itself up.
         */
        RELEASE_QUEUE,

        /**
         * Hold onto our queue position (which we expect to be the head position, as we're queued to execute).
         * This is used exclusively for INCR tasks that may hold onto their TxnId for multiple rounds of execution,
         * and prevents later tasks from being scheduled when they will be unable to obtain the lock.
         */
        HOLD_QUEUE,

        /**
         * Skip all queue accounting (sequenced or unsequenced). This mode is used by optimistic referencing via
         * tryLockCaches.
         */
        UNQUEUED
    }

    /**
     * On lock we remove ourselves from the priority/fifo queues and notify the new head
     */
    public final V lockExclusive(SafeTask<?> task, LockMode lockMode)
    {
        Invariants.require(!isLocked());
        if (queue == task)
        {
            if (lockMode == HOLD_QUEUE)
            {
                Invariants.require(task.isCacheQueuedFifo());
                Invariants.require(!task.isUnsequenced());
            }
            else Invariants.require(lockMode != UNLOCKED);
        }
        else if (queue == null)
        {
            Invariants.require(lockMode == UNQUEUED || !CACHE_QUEUES_ENABLED, "Must already be queued");
            queue = task;
        }
        else
        {
            AccordCacheEntryQueue q = ensureQueue();
            switch (lockMode)
            {
                default: throw UnhandledEnum.unknown(lockMode);
                case UNLOCKED: throw UnhandledEnum.invalid(UNLOCKED);
                case HOLD_QUEUE:
                    if (q.hasFifo()) Invariants.require(q.peekFifo() == task);
                    else
                    {
                        Invariants.require(!task.isUnsequenced(this));
                        q.removePriorityHeadNoNotify(task);
                        q.addFifo(null, task); // pass null owner so we don't notify of changes, since we're not logically changing the queue
                    }
                    q.lock(task);
                    break;
                case RELEASE_QUEUE:
                    q.remove(this, task, REQUIRE_RUNNABLE);
                    Invariants.require(!isLocked(), "%s was locked reentrantly while %s was taking the lock", key, task);
                    if (maybeUnwrap(q, task))
                        break;
                    Invariants.expect(queue == q, "%s: queue was replaced reentrantly while %s was taking the lock", key, task);
                case UNQUEUED:
                    Invariants.paranoidLinearCost(!q.contains(task));
                    q.lock(task);
            }
        }

        status |= lockMode.ordinal() << LOCKED_SHIFT;
        return getExclusive();
    }

    final boolean hasFifoOrLocked()
    {
        if (isLocked())
            return true;

        if (queue == null)
            return false;

        if (queue instanceof SafeTask<?>)
            return ((SafeTask<?>) queue).isCacheQueuedFifo();

        return ((AccordCacheEntryQueue)queue).hasFifo();
    }

    final boolean hasFifo()
    {
        if (queue == null)
            return false;

        if (queue instanceof SafeTask<?>)
            return (!isLocked() || isLockedHoldingQueue()) && ((SafeTask<?>) queue).isCacheQueuedFifo();

        if (queue instanceof AccordCacheEntryMiniQueue)
            return isLockedHoldingQueue() || miniQueue().next.isCacheQueuedFifo();

        return ((AccordCacheEntryQueue)queue).hasFifo();
    }

    final void unlink()
    {
        remove();
    }

    final boolean isUnqueued()
    {
        return isFree();
    }

    /** whether no task holds a position here, nor our lock: nothing can be notified of this entry again */
    final boolean isUnclaimed()
    {
        return queue == null;
    }

    public final K key()
    {
        return key;
    }

    public final int references()
    {
        return references;
    }

    public final int increment()
    {
        return referencesUpdater.incrementAndGet(this);
    }

    public final int decrement()
    {
        return referencesUpdater.decrementAndGet(this);
    }

    final boolean isLocked()
    {
        return (status & LOCKED_MASK) != 0;
    }

    /** whether {@code task} holds our lock: for a bare queue it is the lone occupant, otherwise the queue records it */
    final boolean isLockedBy(SafeTask<?> task)
    {
        if (!isLocked())
            return false;

        if (queue instanceof SafeTask<?>)
            return queue == task;

        if (queue instanceof AccordCacheEntryMiniQueue)
            return miniQueue().lockedBy == task;
        return ((AccordCacheEntryQueue) queue).isLocked(task);
    }

    final SafeTask<?> lockedBy()
    {
        if (!isLocked())
            return null;

        if (queue instanceof SafeTask<?>)
            return (SafeTask<?>) queue;

        if (queue instanceof AccordCacheEntryMiniQueue)
            return miniQueue().lockedBy;

        return ((AccordCacheEntryQueue) queue).lockedBy();
    }

    final boolean isLockedHoldingQueue()
    {
        return (status & LOCKED_MASK) == LOCKED_HOLDING_QUEUE;
    }

    /**
     * Whether {@code q} is still the queue this entry is using: a mutation holds a local reference across the
     * notifications it issues, and a reentrant {@code lockExclusive} may unwrap the queue in between.
     */
    final boolean isLiveQueue(Object q)
    {
        return queue == q;
    }

    /** either a lone SafeTask or an AccordCacheEntryQueue */
    @VisibleForTesting
    Object unsafeGetQueue()
    {
        return queue;
    }

    /** force a status without the usual transition check */
    @VisibleForTesting
    void unsafeSetStatus(Status status)
    {
        setStatusUnsafe(status);
    }

    /** every task holding a position here, in wait order, runnable prefix first. Excludes the lock holder. */
    @VisibleForTesting
    List<SafeTask<?>> unsafeQueuedTasks()
    {
        List<SafeTask<?>> tasks = new ArrayList<>();
        if (queue == null)
            return tasks;

        if (queue instanceof SafeTask<?>)
        {
            SafeTask<?> lone = (SafeTask<?>) queue;
            // a locked entry that is not holding its queue records only the holder, which occupies no position
            if (!isLocked() || isLockedHoldingQueue())
                tasks.add(lone);
            return tasks;
        }

        if (queue instanceof AccordCacheEntryMiniQueue)
        {
            AccordCacheEntryMiniQueue q = miniQueue();
            // a locked entry that is not holding its queue records only the holder, which occupies no position
            if (isLockedHoldingQueue())
                tasks.add(q.lockedBy);
            tasks.add(q.next);
            return tasks;
        }

        AccordCacheEntryQueue q = (AccordCacheEntryQueue) queue;
        for (int i = q.fifoHead ; i > q.fifoTail ; --i)
            tasks.add(q.tasks[i]);
        for (int i = q.priorityHead ; i < q.priorityTail + q.unsequencedSize ; ++i)
            tasks.add(q.tasks[i]);
        return tasks;
    }

    /** how many of {@link #unsafeQueuedTasks()} may run: the fifo or sorted head, else the whole bag */
    @VisibleForTesting
    int unsafeRunnablePrefix()
    {
        if (queue == null) return 0;
        if (queue instanceof AccordCacheEntryQueue) return ((AccordCacheEntryQueue) queue).runnablePrefix();
        if (queue instanceof AccordCacheEntryMiniQueue) return 1;
        return unsafeQueuedTasks().size();
    }

    /** how many of {@link #unsafeQueuedTasks()} are fifo claims; they are the leading entries of that list */
    @VisibleForTesting
    int unsafeFifoSize()
    {
        if (queue == null) return 0;
        if (queue instanceof AccordCacheEntryQueue) return ((AccordCacheEntryQueue) queue).fifoSize();
        if (queue instanceof AccordCacheEntryMiniQueue)
        {
            // the holder leads the fifo region if it kept its position, and the other claim follows it, so the fifo
            // claims remain the leading entries of unsafeQueuedTasks()
            int size = isLockedHoldingQueue() ? 1 : 0;
            if (miniQueue().next.isCacheQueuedFifo())
                ++size;
            return size;
        }
        return hasFifo() ? unsafeQueuedTasks().size() : 0;
    }

    final boolean isLoaded()
    {
        return (status & IS_LOADED) != 0;
    }

    public final boolean isModified()
    {
        return (status & IS_NOT_EVICTED) >= MODIFIED.ordinal();
    }

    final boolean isNested()
    {
        Invariants.require(isLoaded());
        return (status & IS_NESTED) != 0;
    }

    final boolean isShrunk()
    {
        return (status & SHRUNK) != 0;
    }

    public final boolean is(Status status)
    {
        return (this.status & STATUS_MASK) == status.ordinal();
    }

    final boolean isLoading()
    {
        return (status & IS_LOADING_OR_WAITING_MASK) == IS_LOADING_OR_WAITING;
    }

    final boolean isSavingOrWaiting()
    {
        return (status & IS_SAVING_OR_WAITING_MASK) == IS_SAVING_OR_WAITING;
    }

    public final boolean isComplete()
    {
        return !is(LOADING) && !is(SAVING);
    }

    final int noEvictGeneration()
    {
        Invariants.require(isNoEvict());
        return (status >>> GENERATION_SHIFT) & GENERATION_MASK;
    }

    final int noEvictMaxAge()
    {
        Invariants.require(isNoEvict());
        return status >>> AGE_SHIFT;
    }

    final boolean isNoEvict()
    {
        return (status & NO_EVICT) != 0;
    }

    final int sizeOnHeap()
    {
        return sizeOnHeap;
    }

    final void updateSize(AccordCache.Type<K, V, ?> parent)
    {
        // TODO (expected): we aren't weighing the keys
        int newSizeOnHeap = Ints.saturatedCast(EMPTY_SIZE + estimateOnHeapSize(parent.adapter()));
        parent.updateSize(newSizeOnHeap, newSizeOnHeap - sizeOnHeap, references == 0, true);
        sizeOnHeap = newSizeOnHeap;
    }

    final void initSize(AccordCache.Type<K, V, ?> parent)
    {
        // TODO (expected): we aren't weighing the keys
        sizeOnHeap = Ints.saturatedCast(EMPTY_SIZE);
        parent.updateSize(sizeOnHeap, sizeOnHeap, false, false);
        parent.objectSize.increment(EMPTY_SIZE);
    }

    @Override
    public final String toString()
    {
        return "Node{" + status() +
               ", key=" + key() +
               ", references=" + references +
               "}@" + Integer.toHexString(System.identityHashCode(this));
    }

    public final Status status()
    {
        return Status.VALUES[(status & STATUS_MASK)];
    }

    private void setStatus(Status newStatus)
    {
        Invariants.require((newStatus.permittedFrom & (1 << (status & STATUS_MASK))) != 0, "%s not permitted from %s", newStatus, status());
        setStatusUnsafe(newStatus);
    }

    private void setStatusUnsafe(Status newStatus)
    {
        status &= ~STATUS_MASK;
        status |= newStatus.ordinal();
    }

    public final void initialize(V value)
    {
        Invariants.require(state == null);
        setStatus(LOADED);
        state = value;
    }

    public final void readyToLoad()
    {
        Invariants.require(state == null);
        setStatus(WAITING_TO_LOAD);
    }

    public final void markNoEvict(int generation, int maxAge)
    {
        Invariants.require((maxAge & ~AGE_MASK) == 0);
        Invariants.require((generation & ~GENERATION_MASK) == 0);
        status |= NO_EVICT;
        status |= generation << GENERATION_SHIFT;
        status |= maxAge << AGE_SHIFT;
    }

    final void notifyListeners(BiConsumer<AccordCache.Listener<K, V>, AccordCacheEntry<K, V, ?>> notify)
    {
        owner.notifyListeners(notify, this);
    }

    interface LoadExecutor<P1, P2>
    {
        <K, V> IOTask load(P1 p1, P2 p2, AccordCacheEntry<K, V, ?> entry);
    }

    // functions as both an identity object, and a register of listeners
    public static class UniqueSave
    {
        @Nullable List<Runnable> onSuccess;
        void onSuccess(Runnable onSuccess)
        {
            if (this.onSuccess == null)
                this.onSuccess = new ArrayList<>();
            this.onSuccess.add(onSuccess);
        }

        static void notify(List<Runnable> onSuccess)
        {
            if (onSuccess != null)
            {
                onSuccess.forEach(run -> {
                    try { run.run(); }
                    catch (Throwable t)
                    {
                        Thread thread = Thread.currentThread();
                        thread.getUncaughtExceptionHandler().uncaughtException(thread, t);
                    }
                });
            }
        }
    }

    interface SaveExecutor
    {
        Cancellable save(AccordCacheEntry<?, ?, ?> saving, UniqueSave identity, Runnable save);
    }

    final <P1, P2> Loading load(LoadExecutor<P1, P2> loadExecutor, P1 p1, P2 p2)
    {
        Invariants.require(is(WAITING_TO_LOAD), "%s", this);

        Loading loading = new Loading(loadExecutor.load(p1, p2, this));
        setStatus(LOADING);
        state = loading;
        return loading;
    }

    public final Loading testLoad()
    {
        Invariants.require(is(WAITING_TO_LOAD));
        Loading loading = new Loading(null);
        setStatus(LOADING);
        state = loading;
        return loading;
    }

    public final Loading loading()
    {
        Invariants.require(is(LOADING), "%s", this);
        return (Loading) state;
    }

    // must own the cache's lock when invoked. this is true of most methods in the class,
    // but this one is less obvious so named as to draw attention
    public final V getExclusive()
    {
        Invariants.require(owner == null || owner.commandStore == null || owner.commandStore.executor().isOwningThread());
        Invariants.require(isLoaded(), "%s", this);
        if (isShrunk())
        {
            AccordCache.Type<K, V, ?> parent = owner.parent();
            inflate(owner.commandStore, key, parent.adapter());
            updateSize(parent);
        }

        return (V) maybeUnwrap();
    }

    public final void releaseExclusive(S safeState, SafeTask<?> task)
    {
        owner.release(safeState, task);
    }

    public final Object getOrShrunkExclusive()
    {
        Invariants.require(owner == null || owner.commandStore == null || owner.commandStore.executor().isOwningThread());
        Invariants.require(isLoaded(), "%s", this);
        return maybeUnwrap();
    }

    public V tryGetExclusive()
    {
        Invariants.require(owner == null || owner.commandStore == null || owner.commandStore.executor().isOwningThread());
        if (!isLoaded() || isShrunk())
            return null;
        return (V) maybeUnwrap();
    }

    private Object maybeUnwrap()
    {
        return isNested() ? ((Nested)state).state : state;
    }

    // must own the cache's lock when invoked
    void setExclusive(V value)
    {
        if (value == state)
            return;

        Saving cancel = is(SAVING) ? ((Saving)state) : null;
        if (is(WAITING_TO_SAVE))
        {
            ((WaitingToSave<K, V>) state).state = value;
            if (canSave())
                save();
        }
        else
        {
            setStatus(MODIFIED);
            state = value;
        }
        updateSize(owner.parent());
        // TODO (expected): do we want to cancel in-progress saving?
        if (cancel != null && cancel.identity.onSuccess == null)
            cancel.saving.cancel();
    }

    public void loaded(V value)
    {
        setStatus(LOADED);
        state = value;
        updateSize(owner.parent());
    }

    public void testLoaded(V value)
    {
        setStatus(LOADED);
        state = value;
    }

    public void failedToLoad()
    {
        setStatus(FAILED_TO_LOAD);
        state = null;
    }

    Shrink tryShrink()
    {
        if (!isLoaded())
            return Shrink.EVICT;

        AccordCache.Type<K, V, ?> parent = owner.parent();
        Adapter<K, V, ?> adapter = parent.adapter();
        if (isShrunk() || state == null)
            return Shrink.EVICT;

        V cur = (V) maybeUnwrap();
        Shrink shrink = adapter.decideFullShrink(key, cur);
        if (shrink == Shrink.PERFORM_WITHOUT_LOCK)
            return Shrink.PERFORM_WITHOUT_LOCK;

        Object upd = adapter.fullShrink(key, cur);
        if (upd == null || upd == cur)
            return Shrink.EVICT;
        applyShrink(parent, cur, upd);
        return Shrink.DONE;
    }

    V tryGetFull()
    {
        return isShrunk() ? null : (V) maybeUnwrap();
    }

    Object tryGetShrunk()
    {
        return isShrunk() ? maybeUnwrap() : null;
    }

    boolean isNull()
    {
        return state == null;
    }

    private boolean canSave()
    {
        V full = tryGetFull();
        Object shrunk = tryGetShrunk();
        return owner.parent().adapter().canSave(full, shrunk);
    }

    /**
     * @return whether a save or wait is now in flight (otherwise we have saved already)
     */
    boolean saveWhenReady()
    {
        if (is(WAITING_TO_SAVE))
            return !canSave() || save();

        if (canSave())
            return save();

        setStatus(WAITING_TO_SAVE);
        UniqueSave identity = new UniqueSave();
        state = new WaitingToSave<>(identity, state);
        return true;
    }

    /**
     * Submits a save runnable to the specified executor. When the runnable
     * has completed, the state save will have either completed or failed.
     */
    @VisibleForTesting
    boolean save()
    {
        WaitingToSave<K, V> waitingToSave = is(WAITING_TO_SAVE) ? (WaitingToSave<K, V>)state : null;
        Object state = waitingToSave == null ? this.state : waitingToSave.state;
        V full = isShrunk() ? null : (V)state;
        Object shrunk = isShrunk() ? state : null;
        Runnable save = owner.parent().adapter().save(owner.commandStore, key, full, shrunk);

        UniqueSave identity = waitingToSave == null ? new UniqueSave() : waitingToSave.identity;
        if (null == save) // null mutation -> null Runnable -> no change on disk
        {
            setStatus(LOADED);
            if (waitingToSave != null)
                this.state = state;
            owner.parent().parent().enqueueIfEvictable(this);
            UniqueSave.notify(identity.onSuccess);
            return false;
        }
        else
        {
            setStatus(SAVING);
            Cancellable saving = owner.parent().parent().saveExecutor.save(this, identity, save);
            this.state = new Saving(saving, identity, state);
            return true;
        }
    }

    boolean saved(Object identity, Throwable fail)
    {
        if (identity instanceof UniqueSave)
            UniqueSave.notify(((UniqueSave) identity).onSuccess);

        if (!is(SAVING))
            return false;

        Saving saving = (Saving) state;
        if (saving.identity != identity)
            return false;

        if (fail != null)
        {
            setStatus(FAILED_TO_SAVE);
            state = new FailedToSave(fail, ((Saving)state).state);
            return false;
        }
        else
        {
            setStatus(LOADED);
            state = saving.state;
            return true;
        }
    }

    protected void saved()
    {
        Invariants.require(is(MODIFIED));
        setStatus(LOADED);
    }

    public SavingOrWaitingToSave savingOrWaitingToSave()
    {
        return (SavingOrWaitingToSave) state;
    }

    public AccordCacheEntry<K, V, ?> evicted()
    {
        if (isNoEvict())
            setStatusUnsafe(EVICTED);
        else setStatus(EVICTED);
        state = null;
        return this;
    }

    public Throwable failure()
    {
        return ((FailedToSave)state).cause;
    }

    void tryApplyShrink(Object cur, Object upd, IntrusiveLinkedList<AccordCacheEntry<?,?, ?>> queue)
    {
        if (references() > 0 || !isUnqueued())
            return;

        if (isLoaded() && maybeUnwrap() == cur && upd != cur && upd != null)
            applyShrink(owner.parent(), cur, upd);
        queue.addLast(this);
    }

    private void applyShrink(AccordCache.Type<K, V, ?> parent, Object cur, Object upd)
    {
        if (isNested()) ((Nested)this.state).state = upd;
        else this.state = upd;
        status |= SHRUNK;
        updateSize(parent);
    }

    private void inflate(AccordCommandStore commandStore, K key, Adapter<K, V, ?> adapter)
    {
        Invariants.require(isShrunk());
        if (isNested())
        {
            Nested nested = (Nested) state;
            nested.state = adapter.inflate(commandStore, key, nested.state);
        }
        else
        {
            state = adapter.inflate(commandStore, key, state);
        }
        status &= ~SHRUNK;
    }

    private long estimateOnHeapSize(Adapter<K, V, ?> adapter)
    {
        Object current = maybeUnwrap();
        if (current == null) return 0;
        else if (isShrunk()) return adapter.estimateShrunkHeapSize(current);
        return adapter.estimateHeapSize((V)current);
    }

    public static class Loading
    {
        final IOTask loading;

        Loading(IOTask loading)
        {
            this.loading = loading;
        }
    }

    static class Nested
    {
        Object state;
    }

    static class SavingOrWaitingToSave extends Nested
    {
        final UniqueSave identity;

        SavingOrWaitingToSave(UniqueSave identity, Object state)
        {
            this.identity = identity;
            this.state = state;
        }
    }

    static class Saving extends SavingOrWaitingToSave
    {
        final Cancellable saving;

        Saving(Cancellable saving, UniqueSave identity, Object state)
        {
            super(identity, state);
            this.saving = saving;
        }
    }

    static class WaitingToSave<K, V> extends SavingOrWaitingToSave
    {
        WaitingToSave(UniqueSave identity, Object state)
        {
            super(identity, state);
        }
    }

    static class FailedToSave extends Nested
    {
        final Throwable cause;

        FailedToSave(Throwable cause, Object state)
        {
            this.cause = cause;
            this.state = state;
        }

        public Throwable failure()
        {
            return cause;
        }
    }

    public static <K, V, S extends SafeState<V> & SaferState<K, V, S>> AccordCacheEntry<K, V, S> createReadyToLoad(K key, AccordCache.Type<K, V, S>.Instance owner)
    {
        AccordCacheEntry<K, V, S> node = new AccordCacheEntry<>(key, owner);
        node.readyToLoad();
        return node;
    }
}
