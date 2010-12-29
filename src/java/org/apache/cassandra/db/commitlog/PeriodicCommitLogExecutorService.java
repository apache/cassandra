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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.WrappedRunnable;

class PeriodicCommitLogExecutorService implements ICommitLogExecutorService, PeriodicCommitLogExecutorServiceMBean
{
    private final BlockingQueue<Runnable> queue;
    protected volatile long completedTaskCount = 0;

    public PeriodicCommitLogExecutorService()
    {
        this(1024 * Runtime.getRuntime().availableProcessors());
    }

    public PeriodicCommitLogExecutorService(int queueSize)
    {
        queue = new LinkedBlockingQueue<Runnable>(queueSize);
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                while (true)
                {
                    queue.take().run();
                    completedTaskCount++;
                }
            }
        };
        new Thread(runnable, "COMMIT-LOG-WRITER").start();

        AbstractCommitLogExecutorService.registerMBean(this);
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

    public long getPendingTasks()
    {
        return queue.size();
    }

    public int getActiveCount()
    {
        return 1;
    }

    public long getCompletedTasks()
    {
        return completedTaskCount;
    }
}