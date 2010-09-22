package org.apache.cassandra.concurrent;
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


import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebuggableThreadPoolExecutor extends ThreadPoolExecutor
{
    protected static Logger logger = LoggerFactory.getLogger(JMXEnabledThreadPoolExecutor.class);

    public DebuggableThreadPoolExecutor(String threadPoolName, int priority)
    {
        this(1, 1, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(threadPoolName, priority));
    }

    public DebuggableThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory)
    {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);

        if (maximumPoolSize > 1)
        {
            // clearly strict serialization is not a requirement.  just make the calling thread execute.
            this.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        }
        else
        {
            // preserve task serialization.  this is more complicated than it needs to be,
            // since TPE rejects if queue.offer reports a full queue.  we'll just
            // override this with a handler that retries until it gets in.  ugly, but effective.
            // (there is an extensive analysis of the options here at
            //  http://today.java.net/pub/a/today/2008/10/23/creating-a-notifying-blocking-thread-pool-executor.html)
            this.setRejectedExecutionHandler(new RejectedExecutionHandler()
            {
                public void rejectedExecution(Runnable task, ThreadPoolExecutor executor)
                {
                    BlockingQueue<Runnable> queue = executor.getQueue();
                    while (true)
                    {
                        if (executor.isShutdown())
                            throw new RejectedExecutionException("ThreadPoolExecutor has shut down");
                        try
                        {
                            if (queue.offer(task, 1000, TimeUnit.MILLISECONDS))
                                break;
                        }
                        catch (InterruptedException e)
                        {
                            throw new AssertionError(e);    
                        }
                    }
                }
            });
        }
    }

    public void afterExecute(Runnable r, Throwable t)
    {
        super.afterExecute(r,t);

        // exceptions wrapped by FutureTask
        if (r instanceof FutureTask)
        {
            try
            {
                ((FutureTask) r).get();
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            catch (ExecutionException e)
            {
                if (Thread.getDefaultUncaughtExceptionHandler() != null)
                    Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), e.getCause());
            }
        }

        // exceptions for non-FutureTask runnables [i.e., added via execute() instead of submit()]
        if (t != null)
        {
            logger.error("Error in ThreadPoolExecutor", t);
        }
    }
}
