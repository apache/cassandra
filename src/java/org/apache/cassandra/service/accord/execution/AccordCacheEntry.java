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
import java.util.function.BiPredicate;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import accord.local.SafeState;
import accord.utils.ArrayBuffers.BufferList;
import accord.utils.IntrusiveLinkedList;
import accord.utils.IntrusiveLinkedListNode;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import accord.utils.async.Cancellable;

import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.execution.AccordCache.Adapter;
import org.apache.cassandra.service.accord.execution.AccordCache.Adapter.Shrink;
import org.apache.cassandra.utils.ObjectSizes;

import static accord.utils.Invariants.nonNull;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode.HOLD_QUEUE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode.UNLOCKED;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.NEWLY_BLOCKING_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.NEWLY_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.NOT_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.STILL_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.STILL_RUNNABLE_NEWLY_BLOCKING;
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
import static org.apache.cassandra.service.accord.execution.AccordCacheEntryQueue.compare;

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
     * Either a single AccordTask or a Queue object. The meaning of a single task is defined by various flags.
     * If locked, then the task is not logically part of the queue unless LOCKED_HOLDING_QUEUE.
     * If unlocked, or LOCKED_HOLDING_QUEUE, the task represents a single-item queue.
     * If the task forms a single-item queue, whether it is FIFO or prioritised is determined by the task's isCacheQueuedFifo flag.
     */
    private Object queue; //
    private int status;
    private int unsequenced;
    int sizeOnHeap;
    private volatile int references;
    private static final AtomicIntegerFieldUpdater<AccordCacheEntry> referencesUpdater = AtomicIntegerFieldUpdater.newUpdater(AccordCacheEntry.class, "references");

    AccordCacheEntry(K key, AccordCache.Type<K, V, S>.Instance owner)
    {
        this.key = key;
        this.owner = owner;
    }

    private RunnableStatus validate(RunnableStatus status)
    {
        Invariants.require(queue != null);
        SafeTask<?> head = queue instanceof AccordCacheEntryQueue ? ((AccordCacheEntryQueue) queue).peek() : (SafeTask<?>) queue;
        Invariants.require(isRunnable(head) || status == NOT_RUNNABLE);
        return status;
    }

    // TODO (expected): don't unwrap when only one entry, since this may cause us to flap when locking unsequenced tasks
    private void maybeUnwrap(AccordCacheEntryQueue q)
    {
        int size = q.sequencedSize();
        switch (size)
        {
            case 0:
                Invariants.require(q.unsequencedSize() == 0);
                queue = isLocked() ? nonNull(q.lockedBy()) : null;
                break;

            case 1:
                if (isLocked() || q.unsequencedSize() > 0)
                    break;
                queue = q.peek();
        }
    }

    private boolean maybeUnwrap(AccordCacheEntryQueue q, SafeTask<?> lockedBy)
    {
        if (q.sequencedSize() == 0)
        {
            Invariants.require(q.unsequencedSize() == 0);
            queue = lockedBy;
            return true;
        }
        return false;
    }

    // assumes already queued with priority
    final RunnableStatus moveToFifo(SafeTask<?> task)
    {
        if (queue != task)
        {
            AccordCacheEntryQueue q = (AccordCacheEntryQueue) queue;
            RunnableStatus status = q.ensureHeadFifo(task);
            if (status == NEWLY_BLOCKING_RUNNABLE && isLoaded())
                onChangedHead(q, null, q.peekBehind());
            return validate(isRunnable(task) ? status : NOT_RUNNABLE);
        }
        return validate(isRunnable(task) ? STILL_RUNNABLE : NOT_RUNNABLE);
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
                for (int i = q.priorityHead ; i < q.priorityTail ; ++i)
                {
                    list.add(q.tasks[i]);
                    q.tasks[i] = null;
                }
                q.priorityHead = q.priorityTail = PRIORITY_START_INDEX;
                for (int i = q.fifoHead ; i > q.fifoTail ; --i)
                    list.add(q.tasks[i]);

                maybeUnwrap(q);
            }
            else
            {
                SafeTask<?> task = (SafeTask<?>) queue;
                list.add(task);
                Invariants.require(!isLocked());
                if (!task.isCacheQueuedFifo())
                    queue = null;
            }
        }
        return list;
    }

    final void remove(SafeTask<?> task, boolean ownsLock)
    {
        if (queue instanceof AccordCacheEntryQueue)
        {
            AccordCacheEntryQueue q = (AccordCacheEntryQueue) queue;
            boolean remove;
            boolean isLocked = isLocked() && q.isLocked(task);
            Invariants.require(isLocked || !ownsLock);
            if (isLocked)
            {
                // if locked, we've already released unsequenced/pririty/fifo positions unless isLockedHoldingQueue
                remove = isLockedHoldingQueue();
                status &= ~LOCKED_MASK;
                q.unlock(task);
            }
            else if (task.isUnsequenced(this))
            {
                if (task.isCacheQueued())
                {
                    if (!q.removeUnsequenced(task))
                        releaseUnsequenced(q, task);
                }
                remove = false;
            }
            else remove = task.isCacheQueued();

            if (remove)
            {
                boolean wasHead = remove && q.removeFifoOrPriority(task, false);
                if (isLoaded() && wasHead)
                {
                    unsequenced += q.drainUnsequenced(SafeTask::onChangeHeadStatus, this, NEWLY_RUNNABLE);
                    onChangedHead(q, q.peek(), null);
                }
            }

            if (remove || isLocked)
                maybeUnwrap(q);
        }
        else if (queue == task)
        {
            boolean isLocked = isLocked();
            Invariants.require(isLocked || !ownsLock);
            if (isLocked)
            {
                status &= ~LOCKED_MASK;
            }
            else if (task.isUnsequenced(this))
            {
                if (task.isCacheQueued()) --unsequenced; // nothing to release if we hit zero
                else Invariants.require(isLoading());
            }
            queue = null;
        }
        else
        {
            Invariants.require(!ownsLock);
            if (task.isUnsequenced(this) && task.isCacheQueued())
                --unsequenced; // nothing to release if we hit zero
        }
    }

    final boolean isCommandsForKey()
    {
        return getClass() == SaferCommandsForKey.CommandsForKeyCacheEntry.class;
    }

    final RunnableStatus headStatus(SafeTask<?> task)
    {
        if (queue == task)
            return validate(isRunnable(task) ? NEWLY_RUNNABLE : NOT_RUNNABLE);

        AccordCacheEntryQueue q = (AccordCacheEntryQueue) queue;
        if (q.peek() != task || !isRunnable(task))
            return NOT_RUNNABLE;

        return validate(q.totalSize() == 1 ? NEWLY_RUNNABLE : NEWLY_BLOCKING_RUNNABLE);
    }

    private AccordCacheEntryQueue ensureQueue()
    {
        if (queue instanceof AccordCacheEntryQueue)
            return (AccordCacheEntryQueue) queue;

        SafeTask<?> head = (SafeTask<?>) this.queue;
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        if (isLocked())
        {
            if (isLockedHoldingQueue())
                q.addFifo(head);
            q.lock(head);
        }
        else if (head.isCacheQueuedFifo()) q.addFifo(head);
        else q.addPrioritised(head);
        this.queue = q;
        return q;
    }

    final void addWaitingToLoad(SafeTask<?> task)
    {
        Invariants.require(isLoading());
        if (queue == null) queue = task;
        else ensureQueue().addWaitingToLoad(task);
    }

    final SafeTask<?> head()
    {
        if (queue == null)
            return null;

        if (queue instanceof AccordCacheEntryQueue)
            return ((AccordCacheEntryQueue) queue).peek();

        if (isLocked() && !isLockedHoldingQueue())
            return null;

        return (SafeTask<?>) queue;
    }

    final RunnableStatus addUnsequenced(SafeTask<?> task)
    {
        Invariants.require(isLoaded());

        SafeTask<?> head = head();
        if (head != null && head.holdsLocksBetweenRuns())
        {
            boolean wait = compare(task, head) > 0 || (unsequenced == 0 && head.hasIncrementalStarted());
            if (wait)
            {
                if (ensureQueue().addUnsequenced(task) && isLoaded() && unsequenced == 0)
                    head.onChangeHeadStatus(this, STILL_RUNNABLE_NEWLY_BLOCKING);
                return NOT_RUNNABLE;
            }
            else
            {
                ++unsequenced;
                if (unsequenced == 1 && isLoaded())
                    head.onChangeHeadStatus(this, NOT_RUNNABLE);
                return NEWLY_RUNNABLE;
            }
        }

        ++unsequenced;
        return NEWLY_RUNNABLE;
    }

    final int waitingCount()
    {
        Invariants.require(isLoading());
        return queue == null ? 0 : queue instanceof AccordCacheEntryQueue
                                   ? ((AccordCacheEntryQueue)queue).sequencedSize()
                                   : isLocked() == isLockedHoldingQueue() ? 1 : 0;
    }

    public enum RunnableStatus
    {
        NOT_RUNNABLE, STILL_RUNNABLE, NEWLY_RUNNABLE, NEWLY_BLOCKING_RUNNABLE, STILL_RUNNABLE_NEWLY_BLOCKING
    }

    private boolean isRunnable(SafeTask<?> head)
    {
        return !head.holdsLocksBetweenRuns() || unsequenced == 0;
    }

    private RunnableStatus add(SafeTask<?> task, BiPredicate<AccordCacheEntryQueue, SafeTask<?>> add)
    {
        Object prev = this.queue;
        if (prev == null)
        {
            queue = task;
            return validate(isRunnable(task) ? NEWLY_RUNNABLE : NOT_RUNNABLE);
        }

        AccordCacheEntryQueue q = ensureQueue();
        if (!add.test(q, task))
        {
            if (isLoaded() && q.totalSize() == 2)
            {
                SafeTask<?> head = q.peek();
                if (isRunnable(head))
                    head.onChangeHeadStatus(this, STILL_RUNNABLE_NEWLY_BLOCKING);
            }
            return NOT_RUNNABLE;
        }

        boolean isRunnable = isRunnable(task);
        int sequencedSize = q.sequencedSize();
        int unsequencedSize = q.unsequencedSize();
        if (sequencedSize + unsequencedSize == 1) // could have one locked and one waiting
            return validate(isRunnable ? NEWLY_RUNNABLE : NOT_RUNNABLE);

        if (isLoaded() && sequencedSize > 1)
            onChangedHead(q, null, q.peekBehind());

        return validate(isRunnable ? NEWLY_BLOCKING_RUNNABLE : NOT_RUNNABLE);
    }

    final RunnableStatus addPrioritised(SafeTask<?> task)
    {
        Invariants.require(!isLoading());
        return add(task, AccordCacheEntryQueue::addPrioritised);
    }

    final RunnableStatus addFifo(SafeTask<?> task)
    {
        return add(task, AccordCacheEntryQueue::addFifo);
    }

    private void onChangedHead(AccordCacheEntryQueue q, @Nullable SafeTask<?> notifyNewHead, @Nullable SafeTask<?> notifyPrevHead)
    {
        if (notifyNewHead != null && isRunnable(notifyNewHead))
            notifyNewHead.onChangeHeadStatus(this, q.totalSize() == 1 ? NEWLY_RUNNABLE : NEWLY_BLOCKING_RUNNABLE);
        if (notifyPrevHead != null && isRunnable(notifyPrevHead))
            notifyPrevHead.onChangeHeadStatus(this, NOT_RUNNABLE);
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
    public final V lockExclusive(SafeTask<?> owner, LockMode lockMode)
    {
        Invariants.require(!isLocked());
        Invariants.require(isRunnable(owner) || owner.isUnsequenced(this));

        if (queue == owner)
        {
            Invariants.require(lockMode != UNLOCKED);
        }
        else if (queue == null)
        {
            queue = owner;
            switch (lockMode)
            {
                default: throw UnhandledEnum.unknown(lockMode);
                case UNLOCKED: throw UnhandledEnum.invalid(UNLOCKED);
                case HOLD_QUEUE: throw UnhandledEnum.invalid(HOLD_QUEUE, "Must already be head of the queue");
                case RELEASE_QUEUE:
                    Invariants.require(owner.isUnsequenced(this) && owner.isCacheQueued(), "Must already be head of the queue");
                    --unsequenced;
                case UNQUEUED:
            }
        }
        else
        {
            AccordCacheEntryQueue q = ensureQueue();
            switch (lockMode)
            {
                default: throw UnhandledEnum.unknown(lockMode);
                case UNLOCKED: throw UnhandledEnum.invalid(UNLOCKED);
                case HOLD_QUEUE:
                    Invariants.require(!owner.isUnsequenced(this));
                    if (q.hasFifo()) Invariants.require(q.peekFifo() == owner);
                    else
                    {
                        boolean wasHead = q.removeIfPriorityHead(owner);
                        Invariants.require(wasHead);
                        q.addFifo(owner);
                    }
                    q.lock(owner);
                    break;
                case RELEASE_QUEUE:
                    if (owner.isUnsequenced(this)) releaseUnsequenced(q, owner);
                    else
                    {
                        boolean wasHead = q.removeIfHead(owner);
                        Invariants.require(wasHead);
                        if (isLoaded())
                        {
                            unsequenced += q.drainUnsequenced(SafeTask::onChangeHeadStatus, this, NEWLY_RUNNABLE);
                            onChangedHead(q, q.peek(), null);
                        }
                        if (maybeUnwrap(q, owner))
                            break;
                    }
                case UNQUEUED:
                    q.lock(owner);
            }
        }

        status |= lockMode.ordinal() << LOCKED_SHIFT;
        return getExclusive();
    }

    private void releaseUnsequenced(AccordCacheEntryQueue q, SafeTask<?> release)
    {
        Invariants.require(release.isCacheQueued());
        if (--unsequenced == 0)
        {
            SafeTask<?> head = q.peek();
            if (head != null && head.holdsLocksBetweenRuns())
                onChangedHead(q, head, null);
        }
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

    final void unlink()
    {
        remove();
    }

    final boolean isUnqueued()
    {
        return isFree();
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

    final boolean isLockedHoldingQueue()
    {
        return (status & LOCKED_MASK) == LOCKED_HOLDING_QUEUE;
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
            if (owner.parent().adapter().canSave(value, null))
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

    boolean saveWhenReady()
    {
        V full = isShrunk() ? null : (V)state;
        Object shrunk = isShrunk() ? state : null;
        if (owner.parent().adapter().canSave(full, shrunk))
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
