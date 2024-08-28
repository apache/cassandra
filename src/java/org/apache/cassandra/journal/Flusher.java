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
package org.apache.cassandra.journal;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;
import com.codahale.metrics.Timer;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.Interruptible.TerminateException;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Simulate;
import org.apache.cassandra.utils.concurrent.Semaphore;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Daemon.NON_DAEMON;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Interrupts.SYNCHRONIZED;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;
import static org.apache.cassandra.concurrent.Interruptible.State.NORMAL;
import static org.apache.cassandra.concurrent.Interruptible.State.SHUTTING_DOWN;
import static org.apache.cassandra.journal.Params.FlushMode.PERIODIC;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;
import static org.apache.cassandra.utils.Simulate.With.GLOBAL_CLOCK;
import static org.apache.cassandra.utils.Simulate.With.LOCK_SUPPORT;
import static org.apache.cassandra.utils.Simulate.With.MONITORS;
import static org.apache.cassandra.utils.concurrent.Semaphore.newSemaphore;
import static org.apache.cassandra.utils.concurrent.WaitQueue.newWaitQueue;

final class Flusher<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(Flusher.class);

    private final Journal<K, V> journal;
    private final Params params;

    private volatile Interruptible flushExecutor;
    private volatile Interruptible fsyncExecutor;

    // counts of total pending write and written entries
    private final AtomicLong pending = new AtomicLong(0);
    private final AtomicLong written = new AtomicLong(0);

    // the time of the last initiated flush
    volatile long flushStartedAt = nanoTime();
    // the time of the earliest flush that has completed an fsync; all Allocations written before this time are durable
    volatile long fsyncFinishedFor = flushStartedAt;

    // a signal that writers can wait on to be notified of a completed flush in PERIODIC FlushMode
    private final WaitQueue fsyncComplete = newWaitQueue(); // TODO (expected): this is only used for testing, can we remove this?

    // a signal and flag that callers outside the flusher thread can use
    // to signal they want the journal segments to be flushed to disk
    private final Semaphore haveWork = newSemaphore(1);
    private volatile boolean flushRequested;

    private final FlushMethod<K, V> syncFlushMethod;
    private final FlushMethod<K, V> asyncFlushMethod;
    private final Callbacks callbacks;

    Flusher(Journal<K, V> journal, Callbacks callbacks)
    {
        this.journal = journal;
        this.params = journal.params;
        this.syncFlushMethod = syncFlushMethod(params);
        this.asyncFlushMethod = asyncFlushMethod(params);
        this.callbacks = callbacks;
    }

    void start()
    {
        String flushExecutorName = journal.name + "-disk-flusher-" + params.flushMode().toString().toLowerCase();
        flushExecutor = executorFactory().infiniteLoop(flushExecutorName, new FlushRunnable(preciseTime), SAFE, NON_DAEMON, SYNCHRONIZED);
    }

    void shutdown() throws InterruptedException
    {
        flushExecutor.shutdown();
        flushExecutor.awaitTermination(1, MINUTES);
        if (fsyncExecutor != null)
        {
            fsyncExecutor.shutdownNow(); // `now` to interrupt potentially parked runnable
            fsyncExecutor.awaitTermination(1, MINUTES);
        }
    }

    @Simulate(with={MONITORS,GLOBAL_CLOCK,LOCK_SUPPORT})
    private class FlushRunnable implements Interruptible.Task
    {
        @Simulate(with={MONITORS,GLOBAL_CLOCK,LOCK_SUPPORT})
        private class FSyncRunnable implements Interruptible.Task
        {
            // this is written only by the Flusher thread, and read only by the Fsync thread
            ActiveSegment<K, V> fsyncUpTo;
            ActiveSegment<K, V> fsyncing;

            private volatile Thread awaitingWork;

            // all Allocations written before this time will be written to at least the OS page cache;
            volatile long fsyncWaitingSince = 0;
            // the time of the earliest flush that has begun participating in an fsync
            volatile long fsyncStartedFor = 0;

            @Override
            public void run(Interruptible.State state) throws InterruptedException
            {
                try
                {
                    doRun(state);
                }
                catch (Throwable t)
                {
                    if (!journal.handleError("Failed to flush segments to disk", t))
                        throw new TerminateException();
                }
            }

            private void awaitWork() throws InterruptedException
            {
                long lastStartedAt = fsyncStartedFor;
                if (fsyncWaitingSince != lastStartedAt)
                    return;

                awaitingWork = Thread.currentThread();
                do
                {
                    if (Thread.interrupted())
                    {
                        awaitingWork = null;
                        throw new InterruptedException();
                    }

                    LockSupport.park();
                }
                while (fsyncWaitingSince == lastStartedAt);

                awaitingWork = null;
            }

            void notify(Thread notify)
            {
                if (notify != null)
                    LockSupport.unpark(notify);
            }

            public void doRun(Interruptible.State state) throws InterruptedException
            {
                awaitWork();
                if (fsyncing == null)
                    fsyncing = journal.oldestActiveSegment();

                // invert order of access; we might see a future fsyncTo, but at worst this means redundantly invoking fsync before updating fsyncStartedFor
                long startedAt = fsyncWaitingSince;
                ActiveSegment<K, V> fsyncTo = this.fsyncUpTo;
                fsyncStartedFor = startedAt;
                // synchronized to prevent thread interrupts while performing IO operations and also
                // clear interrupted status to prevent ClosedByInterruptException in ActiveSegment::flush
                synchronized (this)
                {
                    boolean ignore = Thread.interrupted();
                    while (fsyncing != fsyncTo)
                    {
                        fsyncing.fsync();
                        journal.closeActiveSegmentAndOpenAsStatic(fsyncing);
                        fsyncing = journal.getActiveSegment(fsyncing.descriptor.timestamp + 1);
                    }
                    fsyncing.fsync();
                }
                fsyncFinishedFor = startedAt;
                fsyncComplete.signalAll();
                long finishedAt = clock.now();
                processDuration(startedAt, finishedAt);
            }

            void afterFlush(long startedAt, ActiveSegment<K, V> segment, int syncedOffset)
            {
                long requireFsyncTo = startedAt - periodicFlushLagBlockNanos();

                fsyncUpTo = segment;
                fsyncWaitingSince = startedAt;

                notify(awaitingWork);

                if (requireFsyncTo > fsyncFinishedFor)
                    awaitFsyncAt(requireFsyncTo, journal.metrics.waitingOnFlush.time());
                callbacks.onFlush(segment.descriptor.timestamp, syncedOffset);
            }

            private void doNoOpFlush(long startedAt)
            {
                if (fsyncFinishedFor >= fsyncWaitingSince)
                {
                    fsyncFinishedFor = startedAt;
                }
                else
                {
                    // if the flusher is still running, update the waitingSince register
                    fsyncWaitingSince = startedAt;
                    notify(awaitingWork);
                }
            }
        }

        private final NoSpamLogger noSpamLogger;
        private final MonotonicClock clock;
        private final @Nullable FSyncRunnable fSyncRunnable;

        private ActiveSegment<K, V> current = null;

        private long firstLaggedAt = Long.MIN_VALUE; // first lag ever or since last logged warning
        private int fsyncCount = 0;                  // flush count since firstLaggedAt
        private int lagCount = 0;                    // lag count since firstLaggedAt
        private long duration = 0;              // time spent flushing since firstLaggedAt
        private long lagDuration = 0;                // cumulative lag since firstLaggedAt

        FlushRunnable(MonotonicClock clock)
        {
            this.noSpamLogger = NoSpamLogger.wrap(logger, 5, MINUTES);
            this.clock = clock;
            this.fSyncRunnable = params.flushMode() == PERIODIC ? newFsyncRunnable() : null;
        }

        @Override
        public void run(Interruptible.State state) throws InterruptedException
        {
            try
            {
                doRun(state);
            }
            catch (Throwable t)
            {
                if (!journal.handleError("Failed to flush segments to disk", t))
                    throw new TerminateException();
                else // sleep for full poll-interval after an error, so we don't spam the log file
                    haveWork.tryAcquire(1, flushPeriodNanos(), NANOSECONDS);
            }
        }

        public void doRun(Interruptible.State state) throws InterruptedException
        {
            long startedAt = clock.now();
            long flushPeriodNanos = flushPeriodNanos();
            boolean flushToDisk = flushStartedAt + flushPeriodNanos <= startedAt || state != NORMAL || flushRequested;

            // synchronized to prevent thread interrupts while performing IO operations and also
            // clear interrupted status to prevent ClosedByInterruptException in ActiveSegment::flush
            synchronized (this)
            {
                boolean ignore = Thread.interrupted();
                if (flushToDisk)
                {
                    flushRequested = false;
                    flushStartedAt = startedAt;
                    doFlush(startedAt);
                }
            }

            if (state == SHUTTING_DOWN)
                return;

            if (flushPeriodNanos <= 0)
            {
                haveWork.acquire(1);
            }
            else
            {
                long wakeUpAt = startedAt + flushPeriodNanos;
                haveWork.tryAcquireUntil(1, wakeUpAt);
            }
        }

        private void doFlush(long startedAt) throws InterruptedException
        {
            boolean synchronousFsync = fSyncRunnable == null;

            if (current == null)
                current = journal.oldestActiveSegment();
            ActiveSegment<K, V> newCurrent = journal.currentActiveSegment();

            if (newCurrent == current && (newCurrent == null || !newCurrent.shouldFlush()))
            {
                if (synchronousFsync) fsyncFinishedFor = startedAt;
                else fSyncRunnable.doNoOpFlush(startedAt);

                if (current != null)
                    callbacks.onFlush(current.descriptor.timestamp, (int) current.lastFlushedOffset());
                return;
            }

            Invariants.checkState(newCurrent != null);

            try
            {
                while (current != newCurrent)
                {
                    current.discardUnusedTail();
                    current.flush(synchronousFsync);
                    if (synchronousFsync)
                        journal.closeActiveSegmentAndOpenAsStatic(current);
                    current = journal.getActiveSegment(current.descriptor.timestamp + 1);
                }
                int syncedOffset = current.flush(synchronousFsync);

                if (synchronousFsync) afterFSync(startedAt, current.descriptor.timestamp, syncedOffset);
                else fSyncRunnable.afterFlush(startedAt, current, syncedOffset);
            }
            catch (Throwable t)
            {
                callbacks.onFlushFailed(t);
                throw t;
            }
        }

        private void processDuration(long startedFlushAt, long finishedFsyncAt)
        {
            fsyncCount++;
            duration += (finishedFsyncAt - startedFlushAt);

            long flushPeriodNanos = flushPeriodNanos();
            long lag = finishedFsyncAt - (startedFlushAt + flushPeriodNanos);
            if (flushPeriodNanos <= 0 || lag <= 0)
                return;

            lagCount++;
            lagDuration += lag;

            if (firstLaggedAt == Long.MIN_VALUE)
                firstLaggedAt = finishedFsyncAt;

            boolean logged =
            noSpamLogger.warn(finishedFsyncAt,
                              "Out of {} {} journal flushes over the past {}s with average duration of {}ms, " +
                              "{} have exceeded the configured flush period by an average of {}ms",
                              fsyncCount,
                              journal.name,
                              format("%.2f", (finishedFsyncAt - firstLaggedAt) * 1e-9d),
                              format("%.2f", duration * 1e-6d / fsyncCount),
                              lagCount,
                              format("%.2f", lagDuration * 1e-6d / lagCount));

            if (logged) // reset metrics for next log statement
            {
                firstLaggedAt = Long.MIN_VALUE;
                fsyncCount = lagCount = 0;
                duration = lagDuration = 0;
            }
        }

        private void afterFSync(long startedAt, long syncedSegment, int syncedOffset)
        {
            fsyncFinishedFor = startedAt;
            callbacks.onFlush(syncedSegment, syncedOffset);
            fsyncComplete.signalAll();
            long finishedAt = clock.now();
            processDuration(startedAt, finishedAt);
        }

        private FSyncRunnable newFsyncRunnable()
        {
            final FSyncRunnable fSyncRunnable = new FSyncRunnable();
            fsyncExecutor = executorFactory().infiniteLoop(journal.name + "-fsync", fSyncRunnable, SAFE, NON_DAEMON, SYNCHRONIZED);
            return fSyncRunnable;
        }
    }

    @FunctionalInterface
    private interface FlushMethod<K, V>
    {
        void flush(ActiveSegment<K, V>.Allocation allocation);
    }

    private FlushMethod<K, V> syncFlushMethod(Params params)
    {
        switch (params.flushMode())
        {
            default: throw new IllegalArgumentException();
            case    BATCH: return this::waitForFlushBatch;
            case    GROUP: return this::waitForFlushGroup;
            case PERIODIC: return this::waitForFlushPeriodic;
        }
    }

    private FlushMethod<K, V> asyncFlushMethod(Params params)
    {
        switch (params.flushMode())
        {
            default: throw new IllegalArgumentException();
            case    BATCH: return this::asyncFlushBatch;
            case    GROUP: return this::asyncFlushGroup;
            case PERIODIC: return this::asyncFlushPeriodic;
        }
    }

    void waitForFlush(ActiveSegment<K, V>.Allocation alloc)
    {
        syncFlushMethod.flush(alloc);
    }

    void asyncFlush(ActiveSegment<K, V>.Allocation alloc)
    {
        asyncFlushMethod.flush(alloc);
    }

    private void waitForFlushBatch(ActiveSegment<K, V>.Allocation alloc)
    {
        pending.incrementAndGet();
        requestExtraFlush();
        alloc.awaitFlush(journal.metrics.waitingOnFlush);
        pending.decrementAndGet();
        written.incrementAndGet();
    }

    private void waitForFlushGroup(ActiveSegment<K, V>.Allocation alloc)
    {
        pending.incrementAndGet();
        alloc.awaitFlush(journal.metrics.waitingOnFlush);
        pending.decrementAndGet();
        written.incrementAndGet();
    }

    private void waitForFlushPeriodic(ActiveSegment<K, V>.Allocation ignore)
    {
        long expectedFlushTime = nanoTime() - periodicFlushLagBlockNanos();
        if (fsyncFinishedFor < expectedFlushTime)
        {
            pending.incrementAndGet();
            awaitFsyncAt(expectedFlushTime, journal.metrics.waitingOnFlush.time());
            pending.decrementAndGet();
        }
        written.incrementAndGet();
    }

    private void asyncFlushBatch(ActiveSegment<K, V>.Allocation alloc)
    {
        requestExtraFlush();
        written.incrementAndGet();
    }

    private void asyncFlushGroup(ActiveSegment<K, V>.Allocation alloc)
    {
        written.incrementAndGet();
    }

    private void asyncFlushPeriodic(ActiveSegment<K, V>.Allocation ignore)
    {
        requestExtraFlush();
        written.incrementAndGet();
    }

    /**
     * Request an additional flush cycle without blocking
     */
    void requestExtraFlush()
    {
        // note: cannot simply invoke executor.interrupt() as some filesystems don't like it (jimfs, at least)
        flushRequested = true;
        haveWork.release(1);
    }

    private void awaitFsyncAt(long flushTime, Timer.Context context)
    {
        do
        {
            WaitQueue.Signal signal = fsyncComplete.register(context, Timer.Context::stop);
            if (fsyncFinishedFor < flushTime)
                signal.awaitUninterruptibly();
            else
                signal.cancel();
        }
        while (fsyncFinishedFor < flushTime);
    }

    private long flushPeriodNanos()
    {
        return 1_000_000L * params.flushPeriodMillis();
    }

    private long periodicFlushLagBlockNanos()
    {
        return 1_000_000L * params.periodicFlushLagBlock();
    }

    long pendingEntries()
    {
        return pending.get();
    }

    long writtenEntries()
    {
        return written.get();
    }

    public interface Callbacks
    {
        /**
         * Invoked after {@link Flusher} successfully flushes a segment or multiple segments to disk.
         * Invocation of this callback implies that any segments older than {@code segment} have been
         * completed and also flushed.
         * callbacks for all entries earlier than (segment, position) have finished execution.
         */
        void onFlush(long segment, int position);

        void onFlushFailed(Throwable cause);
    }
}
