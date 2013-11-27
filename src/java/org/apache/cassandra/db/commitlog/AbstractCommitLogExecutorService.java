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

import org.apache.cassandra.utils.WaitQueue;
import org.slf4j.*;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;

abstract class AbstractCommitLogExecutorService
{
    private final Thread thread;
    private volatile boolean shutdown = false;

    // updated before and after any sync
    protected volatile long lastAliveAt = System.currentTimeMillis();

    // counts of total written, and pending, log messages
    private final AtomicLong written = new AtomicLong(0);
    protected final AtomicLong pending = new AtomicLong(0);

    // signal that writers can wait on to be notified of a completed sync
    protected final WaitQueue syncComplete = new WaitQueue();
    private final Semaphore haveWork = new Semaphore(1);

    private static final Logger logger = LoggerFactory.getLogger(AbstractCommitLogExecutorService.class);

    AbstractCommitLogExecutorService(final CommitLog commitLog, final String name, final long pollIntervalMillis)
    {
        if (pollIntervalMillis < 1)
            throw new IllegalArgumentException(String.format("Commit log flush interval must be positive: %dms", pollIntervalMillis));
        thread = new Thread(new Runnable()
        {
            public void run()
            {
                boolean run = true;
                while (run)
                {
                    try
                    {
                        // always run once after shutdown signalled
                        run = !shutdown;

                        // heartbeat and set time we plan to sleep until
                        long nextSync;
                        lastAliveAt = nextSync = System.currentTimeMillis();
                        nextSync += pollIntervalMillis;

                        // perform sync
                        commitLog.sync(shutdown);

                        // heartbeat
                        lastAliveAt = System.currentTimeMillis();

                        // signal any threads blocking on a slow sync
                        syncComplete.signalAll();

                        long sleep = nextSync - System.currentTimeMillis();
                        if (sleep < 0)
                        {
                            logger.warn(String.format("Commit log sync took longer than sync interval (by %.2fs), indicating it is a bottleneck", sleep / -1000d));
                            // don't sleep, as we probably have work to do
                            continue;
                        }

                        try
                        {
                            // wait for the shutdown signal
                            haveWork.tryAcquire(sleep, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e)
                        {
                            // ignore (shouldn't happen, but no point in panicking if it does)
                        }

                    }
                    catch (Throwable t)
                    {

                        logger.error("Commit log sync failed", t);
                        // sleep for full poll-interval after an error, so we don't spam the log file
                        try
                        {
                            haveWork.tryAcquire(pollIntervalMillis, TimeUnit.MILLISECONDS);
                        }
                        catch (InterruptedException e)
                        {
                            // ignore
                        }

                    }
                }
            }
        }, name);
        thread.start();
    }

    public void finishWriteFor(Allocation alloc)
    {
        maybeWaitForSync(alloc);
        written.incrementAndGet();
    }

    protected abstract void maybeWaitForSync(Allocation alloc);

    public void requestExtraSync()
    {
        haveWork.release();
    }

    public void shutdown()
    {
        shutdown = true;
        haveWork.release(1);
    }

    public void awaitTermination() throws InterruptedException
    {
        thread.join();
    }

    public long getCompletedTasks()
    {
        return written.incrementAndGet();
    }

    public long getPendingTasks()
    {
        return pending.incrementAndGet();
    }
}