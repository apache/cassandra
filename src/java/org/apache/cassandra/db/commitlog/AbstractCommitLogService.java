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
package org.apache.cassandra.db.commitlog;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.Interruptible.TerminateException;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.Semaphore;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static com.codahale.metrics.Timer.Context;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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

public abstract class AbstractCommitLogService
{
    /**
     * When in {@link Config.CommitLogSync#periodic} mode, the default number of milliseconds to wait between updating
     * the commit log chained markers.
     */
    static final long DEFAULT_MARKER_INTERVAL_MILLIS = 100;

    private volatile Interruptible executor;

    // all Allocations written before this time will be synced
    protected volatile long lastSyncedAt = currentTimeMillis();

    // counts of total written, and pending, log messages
    private final AtomicLong written = new AtomicLong(0);
    protected final AtomicLong pending = new AtomicLong(0);

    // signal that writers can wait on to be notified of a completed sync
    protected final WaitQueue syncComplete = newWaitQueue();
    protected final Semaphore haveWork = newSemaphore(1);

    final CommitLog commitLog;
    private final String name;

    /**
     * The duration between syncs to disk.
     */
    final long syncIntervalNanos;

    /**
     * The duration between updating the chained markers in the the commit log file. This value should be
     * 0 < {@link #markerIntervalNanos} <= {@link #syncIntervalNanos}.
     */
    final long markerIntervalNanos;

    /**
     * A flag that callers outside of the sync thread can use to signal they want the commitlog segments
     * to be flushed to disk. Note: this flag is primarily to support commit log's batch mode, which requires
     * an immediate flush to disk on every mutation; see {@link BatchCommitLogService#maybeWaitForSync(Allocation)}.
     */
    private volatile boolean syncRequested;

    private static final Logger logger = LoggerFactory.getLogger(AbstractCommitLogService.class);

    /**
     * CommitLogService provides a fsync service for Allocations, fulfilling either the
     * Batch or Periodic contract.
     *
     * Subclasses may be notified when a sync finishes by using the syncComplete WaitQueue.
     */
    AbstractCommitLogService(final CommitLog commitLog, final String name, long syncIntervalMillis)
    {
        this (commitLog, name, syncIntervalMillis, false);
    }

    /**
     * CommitLogService provides a fsync service for Allocations, fulfilling either the
     * Batch or Periodic contract.
     *
     * Subclasses may be notified when a sync finishes by using the syncComplete WaitQueue.
     *
     * @param markHeadersFaster true if the chained markers should be updated more frequently than on the disk sync bounds.
     */
    AbstractCommitLogService(final CommitLog commitLog, final String name, long syncIntervalMillis, boolean markHeadersFaster)
    {
        this.commitLog = commitLog;
        this.name = name;

        final long markerIntervalMillis;
        if (syncIntervalMillis < 0)
        {
            markerIntervalMillis = -1;
        }
        else if (markHeadersFaster && syncIntervalMillis > DEFAULT_MARKER_INTERVAL_MILLIS)
        {
            markerIntervalMillis = DEFAULT_MARKER_INTERVAL_MILLIS;
            long modulo = syncIntervalMillis % markerIntervalMillis;
            if (modulo != 0)
            {
                // quantize syncIntervalMillis to a multiple of markerIntervalMillis
                syncIntervalMillis -= modulo;

                if (modulo >= markerIntervalMillis / 2)
                    syncIntervalMillis += markerIntervalMillis;
            }
            assert syncIntervalMillis % markerIntervalMillis == 0;
            logger.debug("Will update the commitlog markers every {}ms and flush every {}ms", markerIntervalMillis, syncIntervalMillis);
        }
        else
        {
            markerIntervalMillis = syncIntervalMillis;
        }
        this.markerIntervalNanos = NANOSECONDS.convert(markerIntervalMillis, MILLISECONDS);
        this.syncIntervalNanos = NANOSECONDS.convert(syncIntervalMillis, MILLISECONDS);
    }

    // Separated into individual method to ensure relevant objects are constructed before this is started.
    void start()
    {
        if (syncIntervalNanos < 1 && !(this instanceof BatchCommitLogService)) // permit indefinite waiting with batch, as perfectly sensible
            throw new IllegalArgumentException(String.format("Commit log flush interval must be positive: %fms",
                                                             syncIntervalNanos * 1e-6));

        SyncRunnable sync = new SyncRunnable(preciseTime);
        executor = executorFactory().infiniteLoop(name, sync, SAFE, NON_DAEMON, SYNCHRONIZED);
    }

    class SyncRunnable implements Interruptible.Task
    {
        private final MonotonicClock clock;
        private long firstLagAt = 0;
        private long totalSyncDuration = 0; // total time spent syncing since firstLagAt
        private long syncExceededIntervalBy = 0; // time that syncs exceeded pollInterval since firstLagAt
        private int lagCount = 0;
        private int syncCount = 0;

        SyncRunnable(MonotonicClock clock)
        {
            this.clock = clock;
        }

        public void run(Interruptible.State state) throws InterruptedException
        {
            try
            {
                // sync and signal
                long pollStarted = clock.now();
                boolean flushToDisk = lastSyncedAt + syncIntervalNanos <= pollStarted || state != NORMAL || syncRequested;
                // synchronized to prevent thread interrupts while performing IO operations and also
                // clear interrupted status to prevent ClosedByInterruptException in CommitLog::sync
                synchronized (this)
                {
                    Thread.interrupted();
                    if (flushToDisk)
                    {
                        // in this branch, we want to flush the commit log to disk
                        syncRequested = false;
                        commitLog.sync(true);
                        lastSyncedAt = pollStarted;
                        syncComplete.signalAll();
                        syncCount++;
                    }
                    else
                    {
                        // in this branch, just update the commit log sync headers
                        commitLog.sync(false);
                    }
                }

                if (state == SHUTTING_DOWN)
                    return;

                if (markerIntervalNanos <= 0)
                {
                    haveWork.acquire(1);
                }
                else
                {
                    long now = clock.now();
                    if (flushToDisk)
                        maybeLogFlushLag(pollStarted, now);

                    long wakeUpAt = pollStarted + markerIntervalNanos;
                    if (wakeUpAt > now)
                        haveWork.tryAcquireUntil(1, wakeUpAt);
                }
            }
            catch (Throwable t)
            {
                if (!CommitLog.handleCommitError("Failed to persist commits to disk", t))
                    throw new TerminateException();
                else // sleep for full poll-interval after an error, so we don't spam the log file
                    haveWork.tryAcquire(1, markerIntervalNanos, NANOSECONDS);
            }
        }

        /**
         * Add a log entry whenever the time to flush the commit log to disk exceeds {@link #syncIntervalNanos}.
         */
        @VisibleForTesting
        boolean maybeLogFlushLag(long pollStarted, long now)
        {
            long flushDuration = now - pollStarted;
            totalSyncDuration += flushDuration;

            // this is the timestamp by which we should have completed the flush
            long maxFlushTimestamp = pollStarted + syncIntervalNanos;
            if (maxFlushTimestamp > now)
                return false;

            // if we have lagged noticeably, update our lag counter
            if (firstLagAt == 0)
            {
                firstLagAt = now;
                syncExceededIntervalBy = lagCount = 0;
                syncCount = 1;
                totalSyncDuration = flushDuration;
            }
            syncExceededIntervalBy += now - maxFlushTimestamp;
            lagCount++;

            if (firstLagAt > 0)
            {
                //Only reset the lag tracking if it actually logged this time
                boolean logged = NoSpamLogger.log(logger,
                                                  NoSpamLogger.Level.WARN,
                                                  5,
                                                  MINUTES,
                                                  "Out of {} commit log syncs over the past {}s with average duration of {}ms, {} have exceeded the configured commit interval by an average of {}ms",
                                                  syncCount,
                                                  String.format("%.2f", (now - firstLagAt) * 1e-9d),
                                                  String.format("%.2f", totalSyncDuration * 1e-6d / syncCount),
                                                  lagCount,
                                                  String.format("%.2f", syncExceededIntervalBy * 1e-6d / lagCount));
                if (logged)
                    firstLagAt = 0;
            }
            return true;
        }

        @VisibleForTesting
        long getTotalSyncDuration()
        {
            return totalSyncDuration;
        }
    }

    /**
     * Block for @param alloc to be sync'd as necessary, and handle bookkeeping
     */
    public void finishWriteFor(Allocation alloc)
    {
        maybeWaitForSync(alloc);
        written.incrementAndGet();
    }

    protected abstract void maybeWaitForSync(Allocation alloc);

    /**
     * Request an additional sync cycle without blocking.
     */
    void requestExtraSync()
    {
        // note: cannot simply invoke executor.interrupt() as some filesystems don't like it (jimfs, at least)
        syncRequested = true;
        haveWork.release(1);
    }

    public void shutdown()
    {
        executor.shutdown();
    }

    /**
     * Request sync and wait until the current state is synced.
     *
     * Note: If a sync is in progress at the time of this request, the call will return after both it and a cycle
     * initiated immediately afterwards complete.
     */
    public void syncBlocking()
    {
        long requestTime = nanoTime();
        requestExtraSync();
        awaitSyncAt(requestTime, null);
    }

    void awaitSyncAt(long syncTime, Context context)
    {
        do
        {
            WaitQueue.Signal signal = context != null ? syncComplete.register(context, Context::stop) : syncComplete.register();
            if (lastSyncedAt < syncTime)
                signal.awaitUninterruptibly();
            else
                signal.cancel();
        }
        while (lastSyncedAt < syncTime);
    }

    public void awaitTermination() throws InterruptedException
    {
        executor.awaitTermination(5L, MINUTES);
    }

    public long getCompletedTasks()
    {
        return written.get();
    }

    public long getPendingTasks()
    {
        return pending.get();
    }
}
