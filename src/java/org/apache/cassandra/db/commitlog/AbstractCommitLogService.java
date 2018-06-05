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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer.Context;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.WaitQueue;

public abstract class AbstractCommitLogService
{
    /**
     * When in {@link Config.CommitLogSync#periodic} mode, the default number of milliseconds to wait between updating
     * the commit log chained markers.
     */
    static final long DEFAULT_MARKER_INTERVAL_MILLIS = 100;

    private volatile Thread thread;
    private volatile boolean shutdown = false;

    // all Allocations written before this time will be synced
    protected volatile long lastSyncedAt = System.currentTimeMillis();

    // counts of total written, and pending, log messages
    private final AtomicLong written = new AtomicLong(0);
    protected final AtomicLong pending = new AtomicLong(0);

    // signal that writers can wait on to be notified of a completed sync
    protected final WaitQueue syncComplete = new WaitQueue();

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
        if (markHeadersFaster && syncIntervalMillis > DEFAULT_MARKER_INTERVAL_MILLIS)
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
            logger.debug("Will update the commitlog markers every {}ms and flush every {}ms", markerIntervalMillis, syncIntervalMillis);
        }
        else
        {
            markerIntervalMillis = syncIntervalMillis;
        }
        assert syncIntervalMillis % markerIntervalMillis == 0;
        this.markerIntervalNanos = TimeUnit.NANOSECONDS.convert(markerIntervalMillis, TimeUnit.MILLISECONDS);
        this.syncIntervalNanos = TimeUnit.NANOSECONDS.convert(syncIntervalMillis, TimeUnit.MILLISECONDS);
    }

    // Separated into individual method to ensure relevant objects are constructed before this is started.
    void start()
    {
        if (syncIntervalNanos < 1)
            throw new IllegalArgumentException(String.format("Commit log flush interval must be positive: %fms",
                                                             syncIntervalNanos * 1e-6));
        shutdown = false;
        Runnable runnable = new SyncRunnable(new Clock());
        thread = NamedThreadFactory.createThread(runnable, name);
        thread.start();
    }

    class SyncRunnable implements Runnable
    {
        private final Clock clock;
        private long firstLagAt = 0;
        private long totalSyncDuration = 0; // total time spent syncing since firstLagAt
        private long syncExceededIntervalBy = 0; // time that syncs exceeded pollInterval since firstLagAt
        private int lagCount = 0;
        private int syncCount = 0;

        SyncRunnable(Clock clock)
        {
            this.clock = clock;
        }

        public void run()
        {
            while (true)
            {
                if (!sync())
                    break;
            }
        }

        boolean sync()
        {
            // always run once after shutdown signalled
            boolean shutdownRequested = shutdown;

            try
            {
                // sync and signal
                long pollStarted = clock.nanoTime();
                boolean flushToDisk = lastSyncedAt + syncIntervalNanos <= pollStarted || shutdownRequested || syncRequested;
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

                long now = clock.nanoTime();
                if (flushToDisk)
                    maybeLogFlushLag(pollStarted, now);

                if (shutdownRequested)
                    return false;

                long wakeUpAt = pollStarted + markerIntervalNanos;
                if (wakeUpAt > now)
                    LockSupport.parkNanos(wakeUpAt - now);
            }
            catch (Throwable t)
            {
                if (!CommitLog.handleCommitError("Failed to persist commits to disk", t))
                    return false;

                // sleep for full poll-interval after an error, so we don't spam the log file
                LockSupport.parkNanos(markerIntervalNanos);
            }

            return true;
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
                                                  TimeUnit.MINUTES,
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
        syncRequested = true;
        LockSupport.unpark(thread);
    }

    public void shutdown()
    {
        shutdown = true;
        requestExtraSync();
    }

    /**
     * Request sync and wait until the current state is synced.
     *
     * Note: If a sync is in progress at the time of this request, the call will return after both it and a cycle
     * initiated immediately afterwards complete.
     */
    public void syncBlocking()
    {
        long requestTime = System.nanoTime();
        requestExtraSync();
        awaitSyncAt(requestTime, null);
    }

    void awaitSyncAt(long syncTime, Context context)
    {
        do
        {
            WaitQueue.Signal signal = context != null ? syncComplete.register(context) : syncComplete.register();
            if (lastSyncedAt < syncTime)
                signal.awaitUninterruptibly();
            else
                signal.cancel();
        }
        while (lastSyncedAt < syncTime);
    }

    public void awaitTermination() throws InterruptedException
    {
        thread.join();
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
