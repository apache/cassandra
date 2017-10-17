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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.WaitQueue;
import org.slf4j.*;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractCommitLogService
{

    private Thread thread;
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
    private final long syncIntervalMillis;

    /**
     * The duration between updating the chained markers in the the commit log file. This value should be
     * 0 < {@link #markerIntervalMillis} <= {@link #syncIntervalMillis}.
     */
    private final long markerIntervalMillis;

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
    AbstractCommitLogService(final CommitLog commitLog, final String name, final long syncIntervalMillis)
    {
        this(commitLog, name, syncIntervalMillis, syncIntervalMillis);
    }

    /**
     * CommitLogService provides a fsync service for Allocations, fulfilling either the
     * Batch or Periodic contract.
     *
     * Subclasses may be notified when a sync finishes by using the syncComplete WaitQueue.
     */
    AbstractCommitLogService(final CommitLog commitLog, final String name, final long syncIntervalMillis, long markerIntervalMillis)
    {
        this.commitLog = commitLog;
        this.name = name;
        this.syncIntervalMillis = syncIntervalMillis;

        // if we are not using periodic mode, or we using compression, we shouldn't update the chained markers
        // faster than the sync interval
        if (DatabaseDescriptor.getCommitLogSync() != Config.CommitLogSync.periodic || commitLog.configuration.useCompression())
            markerIntervalMillis = syncIntervalMillis;

        // apply basic bounds checking on the marker interval
        if (markerIntervalMillis <= 0 || markerIntervalMillis > syncIntervalMillis)
        {
            logger.debug("commit log marker interval {} is less than zero or above the sync interval {}; setting value to sync interval",
                        markerIntervalMillis, syncIntervalMillis);
            markerIntervalMillis = syncIntervalMillis;
        }

        this.markerIntervalMillis = markerIntervalMillis;
    }

    // Separated into individual method to ensure relevant objects are constructed before this is started.
    void start()
    {
        if (syncIntervalMillis < 1)
            throw new IllegalArgumentException(String.format("Commit log flush interval must be positive: %fms",
                                                             syncIntervalMillis * 1e-6));

        Runnable runnable = new Runnable()
        {
            public void run()
            {
                long firstLagAt = 0;
                long totalSyncDuration = 0; // total time spent syncing since firstLagAt
                long syncExceededIntervalBy = 0; // time that syncs exceeded pollInterval since firstLagAt
                int lagCount = 0;
                int syncCount = 0;

                boolean run = true;
                while (run)
                {
                    try
                    {
                        // always run once after shutdown signalled
                        run = !shutdown;

                        // sync and signal
                        long pollStarted = System.currentTimeMillis();
                        if (lastSyncedAt + syncIntervalMillis <= pollStarted || shutdown || syncRequested)
                        {
                            // in this branch, we want to flush the commit log to disk
                            commitLog.sync(shutdown, true);
                            syncRequested = false;
                            lastSyncedAt = pollStarted;
                            syncComplete.signalAll();
                        }
                        else
                        {
                            // in this branch, just update the commit log sync headers
                            commitLog.sync(false, false);
                        }

                        // sleep any time we have left before the next one is due
                        long now = System.currentTimeMillis();
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

                        // if we have lagged this round, we probably have work to do already so we don't sleep
                        if (sleep < 0 || !run)
                            continue;

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
                            break;

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
                }
            }
        };

        thread = new Thread(NamedThreadFactory.threadLocalDeallocator(runnable), name);
        thread.start();
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
        syncRequested = true;
        WaitQueue.Signal signal = syncComplete.register();
        haveWork.release(1);
        return signal;
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
