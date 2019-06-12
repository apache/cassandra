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
package org.apache.cassandra.net;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.min;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;

/**
 * A composite queue holding messages to be delivered by an {@link OutboundConnection}.
 *
 * Contains two queues:
 *  1. An external MPSC {@link ManyToOneConcurrentLinkedQueue} for producers to enqueue messages onto
 *  2. An internal intermediate {@link PrunableArrayQueue} into which the external queue is
 *     drained with exclusive access and from which actual deliveries happen
 * The second, intermediate queue exists to enable efficient in-place pruning of expired messages.
 *
 * Said pruning will be attempted in several scenarios:
 *  1. By callers invoking {@link #add(Message)} - if metadata indicates presence of expired messages
 *     in the queue, and if exclusive access can be immediately obtained (non-blockingly)
 *  2. By {@link OutboundConnection}, periodically, while disconnected
 *  3. As an optimisation, in an attempt to free up endpoint capacity on {@link OutboundConnection#enqueue(Message)}
 *     if current endpoint reserve was insufficient
 */
class OutboundMessageQueue
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundMessageQueue.class);

    interface MessageConsumer<Produces extends Throwable>
    {
        boolean accept(Message<?> message) throws Produces;
    }

    private final MessageConsumer<RuntimeException> onExpired;

    private final ManyToOneConcurrentLinkedQueue<Message<?>> externalQueue = new ManyToOneConcurrentLinkedQueue<>();
    private final PrunableArrayQueue<Message<?>> internalQueue = new PrunableArrayQueue<>(256);

    private volatile long earliestExpiresAt = Long.MAX_VALUE;
    private static final AtomicLongFieldUpdater<OutboundMessageQueue> earliestExpiresAtUpdater =
        AtomicLongFieldUpdater.newUpdater(OutboundMessageQueue.class, "earliestExpiresAt");

    OutboundMessageQueue(MessageConsumer<RuntimeException> onExpired)
    {
        this.onExpired = onExpired;
    }

    /**
     * Add the provided message to the queue. Always succeeds.
     */
    void add(Message<?> m)
    {
        maybePruneExpired();
        externalQueue.offer(m);
        maybeUpdateMinimumExpiryTime(m.expiresAtNanos());
    }

    /**
     * Try to obtain the lock; if this fails, a callback will be registered to be invoked when
     * the lock is relinquished.
     *
     * This callback will run WITHOUT ownership of the lock, so must re-obtain the lock.
     *
     * @return null if failed to obtain the lock
     */
    WithLock lockOrCallback(long nowNanos, Runnable callbackIfDeferred)
    {
        if (!lockOrCallback(callbackIfDeferred))
            return null;

        return new WithLock(nowNanos);
    }

    /**
     * Try to obtain the lock. If successful, invoke the provided consumer immediately, otherwise
     * register it to be invoked when the lock is relinquished.
     */
    void runEventually(Consumer<WithLock> runEventually)
    {
        try (WithLock withLock = lockOrCallback(approxTime.now(), () -> runEventually(runEventually)))
        {
            if (withLock != null)
                runEventually.accept(withLock);
        }
    }

    /**
     * If succeeds to obtain the lock, polls the queue, otherwise registers the provided callback
     * to be invoked when the lock is relinquished.
     *
     * May return null when the queue is non-empty - if the lock could not be acquired.
     */
    Message<?> tryPoll(long nowNanos, Runnable elseIfDeferred)
    {
        try (WithLock withLock = lockOrCallback(nowNanos, elseIfDeferred))
        {
            if (withLock == null)
                return null;

            return withLock.poll();
        }
    }

    class WithLock implements AutoCloseable
    {
        private final long nowNanos;

        private WithLock(long nowNanos)
        {
            this.nowNanos = nowNanos;
            earliestExpiresAt = Long.MAX_VALUE;
            externalQueue.drain(internalQueue::offer);
        }

        Message<?> poll()
        {
            Message<?> m;
            while (null != (m = internalQueue.poll()))
            {
                if (shouldSend(m, nowNanos))
                    break;

                onExpired.accept(m);
            }

            return m;
        }

        void removeHead(Message<?> expectHead)
        {
            assert expectHead == internalQueue.peek();
            internalQueue.poll();
        }

        Message<?> peek()
        {
            Message<?> m;
            while (null != (m = internalQueue.peek()))
            {
                if (shouldSend(m, nowNanos))
                    break;

                internalQueue.poll();
                onExpired.accept(m);
            }

            return m;
        }

        void consume(Consumer<Message<?>> consumer)
        {
            Message<?> m;
            while (null != (m = poll()))
                consumer.accept(m);
        }

        @Override
        public void close()
        {
            pruneInternalQueueWithLock(nowNanos);
            unlock();
        }
    }

    /**
     * Call periodically if cannot expect to promptly invoke consume()
     */
    boolean maybePruneExpired()
    {
        return maybePruneExpired(approxTime.now());
    }

    private boolean maybePruneExpired(long nowNanos)
    {
        if (approxTime.isAfter(nowNanos, earliestExpiresAt))
            return tryRun(() -> pruneWithLock(nowNanos));
        return false;
    }

    private void maybeUpdateMinimumExpiryTime(long newTime)
    {
        if (newTime < earliestExpiresAt)
            earliestExpiresAtUpdater.accumulateAndGet(this, newTime, Math::min);
    }

    /*
     * Drain external queue into the internal one and prune the latter in-place.
     */
    private void pruneWithLock(long nowNanos)
    {
        earliestExpiresAt = Long.MAX_VALUE;
        externalQueue.drain(internalQueue::offer);
        pruneInternalQueueWithLock(nowNanos);
    }

    /*
     * Prune the internal queue in-place.
     */
    private void pruneInternalQueueWithLock(long nowNanos)
    {
        class Pruner implements PrunableArrayQueue.Pruner<Message<?>>
        {
            private long earliestExpiresAt = Long.MAX_VALUE;

            public boolean shouldPrune(Message<?> message)
            {
                return !shouldSend(message, nowNanos);
            }

            public void onPruned(Message<?> message)
            {
                onExpired.accept(message);
            }

            public void onKept(Message<?> message)
            {
                earliestExpiresAt = min(message.expiresAtNanos(), earliestExpiresAt);
            }
        }

        Pruner pruner = new Pruner();
        internalQueue.prune(pruner);

        maybeUpdateMinimumExpiryTime(pruner.earliestExpiresAt);
    }

    private static class Locked implements Runnable
    {
        final Runnable run;
        final Locked next;
        private Locked(Runnable run, Locked next)
        {
            this.run = run;
            this.next = next;
        }

        Locked andThen(Runnable next)
        {
            return new Locked(next, this);
        }

        public void run()
        {
            Locked cur = this;
            while (cur != null)
            {
                try
                {
                    cur.run.run();
                }
                catch (Throwable t)
                {
                    logger.error("Unexpected error when executing deferred lock-intending functions", t);
                }
                cur = cur.next;
            }
        }
    }

    private static final Locked LOCKED = new Locked(() -> {}, null);

    private volatile Locked locked = null;
    private static final AtomicReferenceFieldUpdater<OutboundMessageQueue, Locked> lockedUpdater =
        AtomicReferenceFieldUpdater.newUpdater(OutboundMessageQueue.class, Locked.class, "locked");

    /**
     * Run runOnceLocked either immediately in the calling thread if we can obtain the lock, or ask the lock's current
     * owner attempt to run it when the lock is released.  This may be passed between a sequence of owners, as the present
     * owner releases the lock before trying to acquire it again and execute the task.
     */
    private void runEventually(Runnable runEventually)
    {
        if (!lockOrCallback(() -> runEventually(runEventually)))
            return;

        try
        {
            runEventually.run();
        }
        finally
        {
            unlock();
        }
    }

    /**
     * If we can immediately obtain the lock, execute runIfLocked and return true;
     * otherwise do nothing and return false.
     */
    private boolean tryRun(Runnable runIfAvailable)
    {
        if (!tryLock())
            return false;

        try
        {
            runIfAvailable.run();
            return true;
        }
        finally
        {
            unlock();
        }
    }

    /**
     * @return true iff the caller now owns the lock
     */
    private boolean tryLock()
    {
        return locked == null && lockedUpdater.compareAndSet(this, null, LOCKED);
    }

    /**
     * Try to obtain the lock; if this fails, a callback will be registered to be invoked when the lock is relinquished.
     * This callback will run WITHOUT ownership of the lock, so must re-obtain the lock.
     *
     * @return true iff the caller now owns the lock
     */
    private boolean lockOrCallback(Runnable callbackWhenAvailable)
    {
        if (callbackWhenAvailable == null)
            return tryLock();

        while (true)
        {
            Locked current = locked;
            if (current == null && lockedUpdater.compareAndSet(this, null, LOCKED))
                return true;
            else if (current != null && lockedUpdater.compareAndSet(this, current, current.andThen(callbackWhenAvailable)))
                return false;
        }
    }

    private void unlock()
    {
        Locked locked = lockedUpdater.getAndSet(this, null);
        locked.run();
    }


    /**
     * While removal happens extremely infrequently, it seems possible for many to still interleave with a connection
     * being closed, as experimentally we have encountered enough pending removes to overflow the Locked call stack
     * (prior to making its evaluation iterative).
     *
     * While the stack can no longer be exhausted, this suggests a high potential cost for evaluating all removals,
     * so to ensure system stability we aggregate all pending removes into a single shared object that evaluate
     * together with only a single lock acquisition.
     */
    private volatile RemoveRunner removeRunner = null;
    private static final AtomicReferenceFieldUpdater<OutboundMessageQueue, RemoveRunner> removeRunnerUpdater =
        AtomicReferenceFieldUpdater.newUpdater(OutboundMessageQueue.class, RemoveRunner.class, "removeRunner");

    static class Remove
    {
        final Message<?> message;
        final Remove next;

        Remove(Message<?> message, Remove next)
        {
            this.message = message;
            this.next = next;
        }
    }

    private class RemoveRunner extends AtomicReference<Remove> implements Runnable
    {
        final CountDownLatch done = new CountDownLatch(1);
        final Set<Message<?>> removed = Collections.newSetFromMap(new IdentityHashMap<>());

        RemoveRunner() { super(new Remove(null, null)); }

        boolean undo(Message<?> message)
        {
            return null != updateAndGet(prev -> prev == null ? null : new Remove(message, prev));
        }

        public void run()
        {
            Set<Message<?>> remove = Collections.newSetFromMap(new IdentityHashMap<>());
            removeRunner = null;
            Remove undo = getAndSet(null);
            while (undo.message != null)
            {
                remove.add(undo.message);
                undo = undo.next;
            }

            class Remover implements PrunableArrayQueue.Pruner<Message<?>>
            {
                private long earliestExpiresAt = Long.MAX_VALUE;

                @Override
                public boolean shouldPrune(Message<?> message)
                {
                    return remove.contains(message);
                }

                @Override
                public void onPruned(Message<?> message)
                {
                    removed.add(message);
                }

                @Override
                public void onKept(Message<?> message)
                {
                    earliestExpiresAt = min(message.expiresAtNanos(), earliestExpiresAt);
                }
            }

            Remover remover = new Remover();
            earliestExpiresAt = Long.MAX_VALUE;
            externalQueue.drain(internalQueue::offer);
            internalQueue.prune(remover);
            maybeUpdateMinimumExpiryTime(remover.earliestExpiresAt);
            done.countDown();
        }
    }

    /**
     * Remove the provided Message from the queue, if present.
     *
     * WARNING: This is a blocking call.
     */
    boolean remove(Message<?> remove)
    {
        if (remove == null)
            throw new NullPointerException();
        
        RemoveRunner runner;
        while (true)
        {
            runner = removeRunner;
            if (runner != null && runner.undo(remove))
                break;

            if (runner == null && removeRunnerUpdater.compareAndSet(this, null, runner = new RemoveRunner()))
            {
                runner.undo(remove);
                runEventually(runner);
                break;
            }
        }

        //noinspection UnstableApiUsage
        Uninterruptibles.awaitUninterruptibly(runner.done);
        return runner.removed.contains(remove);
    }

    private static boolean shouldSend(Message<?> m, long nowNanos)
    {
        return !approxTime.isAfter(nowNanos, m.expiresAtNanos());
    }
}
