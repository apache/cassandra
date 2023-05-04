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

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.Interruptible.TerminateException;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;
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
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;
import static org.apache.cassandra.utils.concurrent.Semaphore.newSemaphore;
import static org.apache.cassandra.utils.concurrent.WaitQueue.newWaitQueue;

final class Flusher<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(Flusher.class);

    private final Journal<K, V> journal;
    private final Params params;

    private volatile Interruptible flushExecutor;

    // counts of total pending write and written entries
    private final AtomicLong pending = new AtomicLong(0);
    private final AtomicLong written = new AtomicLong(0);

    // all Allocations written before this time will be flushed
    volatile long lastFlushedAt = currentTimeMillis();

    // a signal that writers can wait on to be notified of a completed flush in PERIODIC FlushMode
    private final WaitQueue flushComplete = newWaitQueue();

    // a signal and flag that callers outside the flusher thread can use
    // to signal they want the journal segments to be flushed to disk
    private final Semaphore haveWork = newSemaphore(1);
    private volatile boolean flushRequested;

    private final FlushMethod<K> syncFlushMethod;
    private final FlushMethod<K> asyncFlushMethod;

    Flusher(Journal<K, V> journal)
    {
        this.journal = journal;
        this.params = journal.params;
        this.syncFlushMethod = syncFlushMethod(params);
        this.asyncFlushMethod = asyncFlushMethod(params);
    }

    void start()
    {
        String flushExecutorName = journal.name + "-disk-flusher-" + params.flushMode().toString().toLowerCase();
        flushExecutor = executorFactory().infiniteLoop(flushExecutorName, new FlushRunnable(preciseTime), SAFE, NON_DAEMON, SYNCHRONIZED);
    }

    void shutdown()
    {
        flushExecutor.shutdown();
    }

    private class FlushRunnable implements Interruptible.Task
    {
        private final MonotonicClock clock;
        private final NoSpamLogger noSpamLogger;

        private final ArrayList<ActiveSegment<K>> segmentsToFlush = new ArrayList<>();

        FlushRunnable(MonotonicClock clock)
        {
            this.clock = clock;
            this.noSpamLogger = NoSpamLogger.wrap(logger, 5, MINUTES);
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
            long startedRunAt = clock.now();
            boolean flushToDisk = lastFlushedAt + flushPeriodNanos() <= startedRunAt || state != NORMAL || flushRequested;

            // synchronized to prevent thread interrupts while performing IO operations and also
            // clear interrupted status to prevent ClosedByInterruptException in ActiveSegment::flush
            synchronized (this)
            {
                boolean ignore = Thread.interrupted();
                if (flushToDisk)
                {
                    flushRequested = false;
                    doFlush();
                    lastFlushedAt = startedRunAt;
                    flushComplete.signalAll();
                }
            }

            long now = clock.now();
            if (flushToDisk)
                processFlushDuration(startedRunAt, now);

            if (state == SHUTTING_DOWN)
                return;

            long wakeUpAt = startedRunAt + flushPeriodNanos();
            if (wakeUpAt > now)
                haveWork.tryAcquireUntil(1, wakeUpAt);
        }

        private void doFlush()
        {
            journal.selectSegmentToFlush(segmentsToFlush);
            // only schedule onSuccess callbacks for a segment if the preceding segments
            // have been fully flushed, to preserve 1:1 mapping between record's position
            // in the journal and onSuccess callback scheduling order
            boolean scheduleOnSuccessCallbacks = true;
            try
            {
                for (ActiveSegment<K> segment : segmentsToFlush)
                {
                    try
                    {
                        scheduleOnSuccessCallbacks = doFlush(segment, scheduleOnSuccessCallbacks) && scheduleOnSuccessCallbacks;
                    }
                    catch (Throwable t)
                    {
                        segmentsToFlush.forEach(s -> s.scheduleOnFailureCallbacks(t));
                        throw t;
                    }
                }
            }
            finally
            {
                segmentsToFlush.clear();
            }
        }

        // flush the segment, schedule write callbacks if requested, return whether the segment has been flushed fully
        private boolean doFlush(ActiveSegment<K> segment, boolean scheduleCallbacks)
        {
            int syncedOffset = segment.flush();
            if (scheduleCallbacks)
                segment.scheduleOnSuccessCallbacks(syncedOffset);
            return segment.isFullyFlushed(syncedOffset);
        }

        private long firstLaggedAt = Long.MIN_VALUE; // first lag ever or since last logged warning
        private int flushCount = 0;                  // flush count since firstLaggedAt
        private int lagCount = 0;                    // lag count since firstLaggedAt
        private long flushDuration = 0;              // time spent flushing since firstLaggedAt
        private long lagDuration = 0;                // cumulative lag since firstLaggedAt

        private void processFlushDuration(long startedFlushAt, long finishedFlushAt)
        {
            flushCount++;
            flushDuration += (finishedFlushAt - startedFlushAt);

            long lag = finishedFlushAt - (startedFlushAt + flushPeriodNanos());
            if (lag <= 0)
                return;

            lagCount++;
            lagDuration += lag;

            if (firstLaggedAt == Long.MIN_VALUE)
                firstLaggedAt = finishedFlushAt;

            boolean logged =
                noSpamLogger.warn(finishedFlushAt,
                                  "Out of {} {} journal flushes over the past {}s with average duration of {}ms, " +
                                      "{} have exceeded the configured flush period by an average of {}ms",
                                  flushCount,
                                  journal.name,
                                  format("%.2f", (finishedFlushAt - firstLaggedAt) * 1e-9d),
                                  format("%.2f", flushDuration * 1e-6d / flushCount),
                                  lagCount,
                                  format("%.2f", lagDuration * 1e-6d / lagCount));

            if (logged) // reset metrics for next log statement
            {
                firstLaggedAt = Long.MIN_VALUE;
                flushCount = lagCount = 0;
                flushDuration = lagDuration = 0;
            }
        }
    }

    @FunctionalInterface
    private interface FlushMethod<K>
    {
        void flush(ActiveSegment<K>.Allocation allocation);
    }

    private FlushMethod<K> syncFlushMethod(Params params)
    {
        switch (params.flushMode())
        {
            default: throw new IllegalArgumentException();
            case    BATCH: return this::waitForFlushBatch;
            case    GROUP: return this::waitForFlushGroup;
            case PERIODIC: return this::waitForFlushPeriodic;
        }
    }

    private FlushMethod<K> asyncFlushMethod(Params params)
    {
        switch (params.flushMode())
        {
            default: throw new IllegalArgumentException();
            case    BATCH: return this::asyncFlushBatch;
            case    GROUP: return this::asyncFlushGroup;
            case PERIODIC: return this::asyncFlushPeriodic;
        }
    }

    void waitForFlush(ActiveSegment<K>.Allocation alloc)
    {
        syncFlushMethod.flush(alloc);
    }

    void asyncFlush(ActiveSegment<K>.Allocation alloc)
    {
        asyncFlushMethod.flush(alloc);
    }

    private void waitForFlushBatch(ActiveSegment<K>.Allocation alloc)
    {
        pending.incrementAndGet();
        requestExtraFlush();
        alloc.awaitFlush(journal.metrics.waitingOnFlush);
        pending.decrementAndGet();
        written.incrementAndGet();
    }

    private void asyncFlushBatch(ActiveSegment<K>.Allocation alloc)
    {
        pending.incrementAndGet();
        requestExtraFlush();
        // alloc.awaitFlush(journal.metrics.waitingOnFlush); // TODO FIXME
        pending.decrementAndGet();
        written.incrementAndGet();
    }

    private void waitForFlushGroup(ActiveSegment<K>.Allocation alloc)
    {
        pending.incrementAndGet();
        alloc.awaitFlush(journal.metrics.waitingOnFlush);
        pending.decrementAndGet();
        written.incrementAndGet();
    }

    private void asyncFlushGroup(ActiveSegment<K>.Allocation alloc)
    {
        pending.incrementAndGet();
        // alloc.awaitFlush(journal.metrics.waitingOnFlush); // TODO FIXME
        pending.decrementAndGet();
        written.incrementAndGet();
    }

    private void waitForFlushPeriodic(ActiveSegment<K>.Allocation alloc)
    {
        long expectedFlushTime = nanoTime() - periodicFlushLagBlockNanos();
        if (lastFlushedAt < expectedFlushTime)
        {
            pending.incrementAndGet();
            awaitFlushAt(expectedFlushTime, journal.metrics.waitingOnFlush.time());
            pending.decrementAndGet();
        }
        written.incrementAndGet();
    }

    private void asyncFlushPeriodic(ActiveSegment<K>.Allocation ignore)
    {
        pending.incrementAndGet();
        // awaitFlushAt(expectedFlushTime, journal.metrics.waitingOnFlush.time()); // TODO FIXME
        pending.decrementAndGet();
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

    private void awaitFlushAt(long flushTime, Timer.Context context)
    {
        do
        {
            WaitQueue.Signal signal = flushComplete.register(context, Timer.Context::stop);
            if (lastFlushedAt < flushTime)
                signal.awaitUninterruptibly();
            else
                signal.cancel();
        }
        while (lastFlushedAt < flushTime);
    }

    private long flushPeriodNanos()
    {
        return 1_000_000L * params.flushPeriod();
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
}
