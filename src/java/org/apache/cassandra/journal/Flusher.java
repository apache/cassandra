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
import static org.apache.cassandra.concurrent.ExecutorFactory.SystemThreadTag.NON_DAEMON;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Interrupts.SYNCHRONIZED;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;
import static org.apache.cassandra.concurrent.Interruptible.State.NORMAL;
import static org.apache.cassandra.concurrent.Interruptible.State.SHUTTING_DOWN;
import static org.apache.cassandra.journal.Params.FlushMode.PERIODIC;
import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;
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
    volatile long flushStartedAt;
    // the time of the earliest flush that has completed an fsync; all Allocations written before this time are durable
    volatile long fsyncFinishedFor = flushStartedAt;
    volatile RecordPointer fsyncFinishedForPosition = new RecordPointer(0, 0);

    // a signal that writers can wait on to be notified of a completed flush in PERIODIC FlushMode
    private final WaitQueue fsyncComplete = newWaitQueue(); // TODO (expected): this is only used for testing, can we remove this?
    private final MonotonicClock clock = preciseTime;

    // a signal and flag that callers outside the flusher thread can use
    // to signal they want the journal segments to be flushed to disk
    private final Semaphore haveWork = newSemaphore(1);
    private volatile boolean flushRequested;

    private final Mode<K, V> mode;
    private final Callbacks callbacks;

    Flusher(Journal<K, V> journal, Callbacks callbacks)
    {
        this.journal = journal;
        this.params = journal.params;
        this.mode = mode(params);
        this.callbacks = callbacks;
    }

    void start()
    {
        String flushExecutorName = journal.name + "-disk-flusher-" + toLowerCaseLocalized(params.flushMode().toString());
        flushStartedAt = clock.now();
        flushExecutor = executorFactory().infiniteLoop(flushExecutorName, new FlushRunnable(), SAFE, NON_DAEMON, SYNCHRONIZED);
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
    // waits for writes to complete before triggering an fsync
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

            private boolean hasWork()
            {
                return hasWork(fsyncStartedFor);
            }

            private boolean hasWork(long lastStartedAt)
            {
                return fsyncWaitingSince != lastStartedAt;
            }

            private void awaitWork() throws InterruptedException
            {
                long lastStartedAt = fsyncStartedFor;
                if (hasWork(lastStartedAt))
                    return;

                awaitingWork = Thread.currentThread();
                while (true)
                {
                    if (Thread.interrupted())
                    {
                        awaitingWork = null;
                        throw new InterruptedException();
                    }

                    if (hasWork(lastStartedAt))
                        break;

                    LockSupport.park();
                }

                awaitingWork = null;
            }

            void notify(Thread notify)
            {
                if (notify != null)
                    LockSupport.unpark(notify);
            }

            public void doRun(Interruptible.State state) throws InterruptedException
            {
                if (state == NORMAL) awaitWork();
                else if (!hasWork()) return;

                if (fsyncing == null)
                    fsyncing = journal.oldestActiveSegment();

                // invert order of access; we might see a future fsyncTo, but at worst this means redundantly invoking fsync before updating fsyncStartedFor
                long startedAt = fsyncWaitingSince;
                ActiveSegment<K, V> fsyncTo = this.fsyncUpTo;
                fsyncStartedFor = startedAt;
                // synchronized to prevent thread interrupts while performing IO operations and also
                // clear interrupted status to prevent ClosedByInterruptException in ActiveSegment::flush
                int fsyncedTo;
                synchronized (this)
                {
                    boolean ignore = Thread.interrupted();
                    while (fsyncing != fsyncTo)
                    {
                        fsyncing.fsync();
                        journal.closeActiveSegmentAndOpenAsStatic(fsyncing);
                        fsyncing = journal.getActiveSegment(fsyncing.descriptor.timestamp + 1);
                    }
                    fsyncedTo = fsyncTo.writtenToAtLeast();
                    fsyncTo.fsync();
                }
                fsyncFinishedForPosition = new RecordPointer(fsyncTo.descriptor.timestamp, fsyncedTo, startedAt);
                fsyncFinishedFor = startedAt;
                fsyncComplete.signalAll();
                long finishedAt = clock.now();
                processDuration(startedAt, finishedAt);
            }

            void afterFlush(long startedAt, ActiveSegment<K, V> segment)
            {
                long requireFsyncTo = startedAt - periodicBlockNanos();

                fsyncUpTo = segment;
                fsyncWaitingSince = startedAt;

                notify(awaitingWork);

                if (requireFsyncTo > fsyncFinishedFor)
                    awaitFsyncAt(requireFsyncTo, journal.metrics.waitingOnFlush.time());
                callbacks.onFsync();
            }
        }

        private final NoSpamLogger noSpamLogger;
        private final @Nullable FSyncRunnable fSyncRunnable;

        private ActiveSegment<K, V> current = null;

        private long firstLaggedAt = Long.MIN_VALUE; // first lag ever or since last logged warning
        private int fsyncCount = 0;                  // flush count since firstLaggedAt
        private int lagCount = 0;                    // lag count since firstLaggedAt
        private long duration = 0;              // time spent flushing since firstLaggedAt
        private long lagDuration = 0;                // cumulative lag since firstLaggedAt

        FlushRunnable()
        {
            this.noSpamLogger = NoSpamLogger.wrap(logger, 5, MINUTES);
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
                Invariants.require(params.flushMode() != PERIODIC);
                haveWork.acquire(1);
            }
            else
            {
                long wakeUpAt = startedAt + flushPeriodNanos;
                haveWork.tryAcquireUntil(1, wakeUpAt);
            }
        }

        private void doFlush(long startedAt)
        {
            boolean synchronousFsync = fSyncRunnable == null;

            if (current == null)
                current = journal.oldestActiveSegment();
            ActiveSegment<K, V> newCurrent = journal.currentActiveSegment();

            if (newCurrent == null)
                return;

            try
            {
                while (current != newCurrent)
                {
                    current.discardUnusedTail();
                    current.updateWrittenTo();
                    if (synchronousFsync)
                    {
                        current.fsync();
                        journal.closeActiveSegmentAndOpenAsStatic(current);
                    }
                    current = journal.getActiveSegment(current.descriptor.timestamp + 1);
                }

                int writtenTo = current.updateWrittenTo();
                if (synchronousFsync)
                {
                    current.fsync();
                    afterFSync(startedAt, current.descriptor.timestamp, writtenTo);
                }
                else
                {
                    fSyncRunnable.afterFlush(startedAt, current);
                }
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

        private void afterFSync(long startedAt, long segment, int position)
        {
            fsyncFinishedForPosition = new RecordPointer(segment, position, startedAt);
            fsyncFinishedFor = startedAt;
            callbacks.onFsync();
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

    private interface Mode<K, V>
    {
        void flushAndAwaitDurable(ActiveSegment<K, V>.Allocation alloc);
        RecordPointer flushAsync(ActiveSegment<K, V>.Allocation alloc);
        boolean isDurable(RecordPointer recordPointer);
    }

    private class BatchMode implements Mode<K, V>
    {
        @Override
        public void flushAndAwaitDurable(ActiveSegment<K, V>.Allocation alloc)
        {
            pending.incrementAndGet();
            requestExtraFlush();
            alloc.awaitDurable(journal.metrics.waitingOnFlush);
            pending.decrementAndGet();
            written.incrementAndGet();
        }

        @Override
        public RecordPointer flushAsync(ActiveSegment<K, V>.Allocation alloc)
        {
            requestExtraFlush();
            written.incrementAndGet();
            return new RecordPointer(alloc.descriptor().timestamp, alloc.start());
        }

        @Override
        public boolean isDurable(RecordPointer pointer)
        {
            return pointer.compareTo(fsyncFinishedForPosition) <= 0;
        }
    }

    private class GroupMode implements Mode<K, V>
    {
        @Override
        public void flushAndAwaitDurable(ActiveSegment<K, V>.Allocation alloc)
        {
            pending.incrementAndGet();
            alloc.awaitDurable(journal.metrics.waitingOnFlush);
            pending.decrementAndGet();
            written.incrementAndGet();
        }

        @Override
        public RecordPointer flushAsync(ActiveSegment<K, V>.Allocation alloc)
        {
            written.incrementAndGet();
            return new RecordPointer(alloc.descriptor().timestamp, alloc.start());
        }

        @Override
        public boolean isDurable(RecordPointer pointer)
        {
            return pointer.compareTo(fsyncFinishedForPosition) <= 0;
        }
    }

    private class PeriodicMode implements Mode<K, V>
    {
        @Override
        public void flushAndAwaitDurable(ActiveSegment<K, V>.Allocation alloc)
        {
            RecordPointer pointer = flushAsync(alloc);

            long expectedFsyncTime = pointer.writtenAt - periodicBlockNanos();
            if (expectedFsyncTime > fsyncFinishedFor)
            {
                pending.incrementAndGet();
                awaitFsyncAt(expectedFsyncTime, journal.metrics.waitingOnFlush.time());
                pending.decrementAndGet();
            }
        }

        @Override
        public RecordPointer flushAsync(ActiveSegment<K, V>.Allocation alloc)
        {
            written.incrementAndGet();
            return new RecordPointer(alloc.descriptor().timestamp, alloc.start(), clock.now());
        }

        @Override
        public boolean isDurable(RecordPointer alloc)
        {
            long expectedFsyncTime = alloc.writtenAt - periodicBlockNanos();
            return expectedFsyncTime <= fsyncFinishedFor;
        }
    }

    Mode<K, V> mode(Params params)
    {
        switch (params.flushMode())
        {
            default: throw new AssertionError("Unexpected FlushMode: " + params.flushMode());
            case BATCH: return new BatchMode();
            case GROUP: return new GroupMode();
            case PERIODIC: return new PeriodicMode();
        }
    }

    RecordPointer flush(ActiveSegment<K, V>.Allocation alloc)
    {
        return mode.flushAsync(alloc);
    }

    void flushAndAwaitDurable(ActiveSegment<K, V>.Allocation alloc)
    {
        mode.flushAndAwaitDurable(alloc);
    }

    boolean isDurable(RecordPointer pointer)
    {
        return mode.isDurable(pointer);
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
            {
                signal.awaitThrowUncheckedOnInterrupt();

                Journal.State state = journal.state.get();
                Invariants.require(state == Journal.State.NORMAL,
                                      "Thread %s outlived journal, which is in %s state", Thread.currentThread(), state);
            }
            else
                signal.cancel();
        }
        while (fsyncFinishedFor < flushTime);
    }

    private long flushPeriodNanos()
    {
        return params.flushPeriod(NANOSECONDS);
    }

    private long periodicBlockNanos()
    {
        return params.periodicBlockPeriod(NANOSECONDS);
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
        void onFsync();

        // TODO (required): tie this to specific allocations..
        void onFlushFailed(Throwable cause);
    }
}
