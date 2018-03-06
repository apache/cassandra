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

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.WaitQueue;
import org.slf4j.*;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
    protected final Semaphore haveWork = new Semaphore(1);

    final CommitLog commitLog;
    private final String name;

    /**
     * The duration between syncs to disk.
     */
    final long syncIntervalMillis;

    /**
     * The duration between updating the chained markers in the the commit log file. This value should be
     * 0 < {@link #markerIntervalMillis} <= {@link #syncIntervalMillis}.
     */
    final long markerIntervalMillis;

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
        this.syncIntervalMillis = syncIntervalMillis;
    }

    // Separated into individual method to ensure relevant objects are constructed before this is started.
    void start()
    {
        if (syncIntervalMillis < 1)
            throw new IllegalArgumentException(String.format("Commit log flush interval must be positive: %dms",
                                                             syncIntervalMillis));
        shutdown = false;
        Runnable runnable = new SyncRunnable(new Clock());
        thread = new Thread(NamedThreadFactory.threadLocalDeallocator(runnable), name);
        thread.start();
    }

    class SyncRunnable implements Runnable
    {
        final Clock clock;
        long firstLagAt = 0;
        long totalSyncDuration = 0; // total time spent syncing since firstLagAt
        long syncExceededIntervalBy = 0; // time that syncs exceeded pollInterval since firstLagAt
        int lagCount = 0;
        int syncCount = 0;

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
            try
            {
                // always run once after shutdown signalled
                boolean run = !shutdown;

                // sync and signal
                long pollStarted = clock.currentTimeMillis();
                if (lastSyncedAt + syncIntervalMillis <= pollStarted || shutdown || syncRequested)
                {
                    // in this branch, we want to flush the commit log to disk
                    syncRequested = false;
                    commitLog.sync(shutdown, true);
                    lastSyncedAt = pollStarted;
                    syncComplete.signalAll();
                }
                else
                {
                    // in this branch, just update the commit log sync headers
                    commitLog.sync(false, false);
                }

                // sleep any time we have left before the next one is due
                long now = clock.currentTimeMillis();
                long sleep = pollStarted + markerIntervalMillis - now;
                if (sleep < 0)
                {
                    // if we have lagged noticeably, update our lag counter
                    if (firstLagAt == 0)
                    {
                        firstLagAt = now;
                        totalSyncDuration = syncExceededIntervalBy = syncCount = lagCount = 0;
                    }
                    syncExceededIntervalBy -= sleep;
                    lagCount++;
                }
                syncCount++;
                totalSyncDuration += now - pollStarted;

                if (firstLagAt > 0)
                {
                    //Only reset the lag tracking if it actually logged this time
                    boolean logged = NoSpamLogger.log(
                    logger,
                    NoSpamLogger.Level.WARN,
                    5,
                    TimeUnit.MINUTES,
                    "Out of {} commit log syncs over the past {}s with average duration of {}ms, {} have exceeded the configured commit interval by an average of {}ms",
                    syncCount, (now - firstLagAt) / 1000, String.format("%.2f", (double) totalSyncDuration / syncCount), lagCount, String.format("%.2f", (double) syncExceededIntervalBy / lagCount));
                    if (logged)
                        firstLagAt = 0;
                }

                if (!run)
                    return false;

                // if we have lagged this round, we probably have work to do already so we don't sleep
                if (sleep < 0)
                    return true;

                try
                {
                    haveWork.tryAcquire(sleep, TimeUnit.MILLISECONDS);
                    haveWork.drainPermits();
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError();
                }
            }
            catch (Throwable t)
            {
                if (!CommitLog.handleCommitError("Failed to persist commits to disk", t))
                    return false;

                // sleep for full poll-interval after an error, so we don't spam the log file
                try
                {
                    haveWork.tryAcquire(markerIntervalMillis, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError();
                }
            }
            return true;
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
     * Sync immediately, but don't block for the sync to cmplete
     */
    public WaitQueue.Signal requestExtraSync()
    {
        WaitQueue.Signal signal = syncComplete.register();
        requestSync();
        return signal;
    }

    protected void requestSync()
    {
        syncRequested = true;
        haveWork.release(1);
    }

    public void shutdown()
    {
        shutdown = true;
        haveWork.release(1);
    }

    /**
     * FOR TESTING ONLY
     */
    public void restartUnsafe()
    {
        while (haveWork.availablePermits() < 1)
            haveWork.release();

        while (haveWork.availablePermits() > 1)
        {
            try
            {
                haveWork.acquire();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
        shutdown = false;
        start();
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
