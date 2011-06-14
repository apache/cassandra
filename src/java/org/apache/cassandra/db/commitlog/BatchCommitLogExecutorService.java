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
import java.util.ArrayList;
import java.util.concurrent.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.WrappedRunnable;

class BatchCommitLogExecutorService extends AbstractCommitLogExecutorService
{
    private final BlockingQueue<CheaterFutureTask> queue;
    private final Thread appendingThread;
    private volatile boolean run = true;

    public BatchCommitLogExecutorService()
    {
        this(DatabaseDescriptor.getConcurrentWriters());
    }

    public BatchCommitLogExecutorService(int queueSize)
    {
        queue = new LinkedBlockingQueue<CheaterFutureTask>(queueSize);
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                while (run)
                {
                    if (processWithSyncBatch())
                        completedTaskCount++;
                }
            }
        };
        appendingThread = new Thread(runnable, "COMMIT-LOG-WRITER");
        appendingThread.start();

    }

    public long getPendingTasks()
    {
        return queue.size();
    }

    private final ArrayList<CheaterFutureTask> incompleteTasks = new ArrayList<CheaterFutureTask>();
    private final ArrayList taskValues = new ArrayList(); // TODO not sure how to generify this
    private boolean processWithSyncBatch() throws Exception
    {
        CheaterFutureTask firstTask = queue.poll(100, TimeUnit.MILLISECONDS);
        if (firstTask == null)
            return false;
        if (!(firstTask.getRawCallable() instanceof CommitLog.LogRecordAdder))
        {
            firstTask.run();
            return true;
        }

        // attempt to do a bunch of LogRecordAdder ops before syncing
        // (this is a little clunky since there is no blocking peek method,
        //  so we have to break it into firstTask / extra tasks)
        incompleteTasks.clear();
        taskValues.clear();
        long end = System.nanoTime() + (long)(1000000 * DatabaseDescriptor.getCommitLogSyncBatchWindow());

        // it doesn't seem worth bothering future-izing the exception
        // since if a commitlog op throws, we're probably screwed anyway
        incompleteTasks.add(firstTask);
        taskValues.add(firstTask.getRawCallable().call());
        while (!queue.isEmpty()
               && queue.peek().getRawCallable() instanceof CommitLog.LogRecordAdder
               && System.nanoTime() < end)
        {
            CheaterFutureTask task = queue.remove();
            incompleteTasks.add(task);
            taskValues.add(task.getRawCallable().call());
        }

        // now sync and set the tasks' values (which allows thread calling get() to proceed)
        try
        {
            CommitLog.instance.sync();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < incompleteTasks.size(); i++)
        {
            incompleteTasks.get(i).set(taskValues.get(i));
        }
        return true;
    }


    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value)
    {
        return newTaskFor(Executors.callable(runnable, value));
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable)
    {
        return new CheaterFutureTask(callable);
    }

    public void execute(Runnable command)
    {
        try
        {
            queue.put((CheaterFutureTask)command);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void add(CommitLog.LogRecordAdder adder)
    {
        try
        {
            submit((Callable)adder).get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
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

    private static class CheaterFutureTask<V> extends FutureTask<V>
    {
        private final Callable rawCallable;

        public CheaterFutureTask(Callable<V> callable)
        {
            super(callable);
            rawCallable = callable;
        }

        public Callable getRawCallable()
        {
            return rawCallable;
        }

        @Override
        public void set(V v)
        {
            super.set(v);
        }
    }

}
