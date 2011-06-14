package org.apache.cassandra.db.commitlog;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.IOException;
import java.util.concurrent.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.WrappedRunnable;

class PeriodicCommitLogExecutorService implements ICommitLogExecutorService
{
    private final BlockingQueue<Runnable> queue;
    protected volatile long completedTaskCount = 0;
    private final Thread appendingThread;
    private volatile boolean run = true;

    public PeriodicCommitLogExecutorService(final CommitLog commitLog)
    {
        queue = new LinkedBlockingQueue<Runnable>(1024 * Runtime.getRuntime().availableProcessors());
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                while (run)
                {
                    Runnable r = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (r == null)
                        continue;
                    r.run();
                    completedTaskCount++;
                }
                commitLog.sync();
            }
        };
        appendingThread = new Thread(runnable, "COMMIT-LOG-WRITER");
        appendingThread.start();

        final Callable syncer = new Callable()
        {
            public Object call() throws Exception
            {
                commitLog.sync();
                return null;
            }
        };

        new Thread(new Runnable()
        {
            public void run()
            {
                while (run)
                {
                    try
                    {
                        submit(syncer).get();
                        Thread.sleep(DatabaseDescriptor.getCommitLogSyncPeriod());
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError(e);
                    }
                    catch (ExecutionException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            }
        }, "PERIODIC-COMMIT-LOG-SYNCER").start();

    }

    public void add(CommitLog.LogRecordAdder adder)
    {
        try
        {
            queue.put(adder);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public <T> Future<T> submit(Callable<T> task)
    {
        FutureTask<T> ft = new FutureTask<T>(task);
        try
        {
            queue.put(ft);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        return ft;
    }

    public void shutdown()
    {
        new Thread(new WrappedRunnable()
        {
            public void runMayThrow() throws InterruptedException, IOException
            {
                while (!queue.isEmpty())
                    Thread.sleep(100);
                run = false;
                appendingThread.join();
            }
        }, "Commitlog Shutdown").start();
    }

    public void awaitTermination() throws InterruptedException
    {
        appendingThread.join();
    }

    public long getPendingTasks()
    {
        return queue.size();
    }

    public long getCompletedTasks()
    {
        return completedTaskCount;
    }

}