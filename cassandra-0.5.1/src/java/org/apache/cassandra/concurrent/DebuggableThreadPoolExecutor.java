/**
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

package org.apache.cassandra.concurrent;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.*;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

/**
 * This is a wrapper class for the <i>ScheduledThreadPoolExecutor</i>. It provides an implementation
 * for the <i>afterExecute()</i> found in the <i>ThreadPoolExecutor</i> class to log any unexpected 
 * Runtime Exceptions.
 */

public class DebuggableThreadPoolExecutor extends ThreadPoolExecutor implements DebuggableThreadPoolExecutorMBean
{
    private static Logger logger_ = Logger.getLogger(DebuggableThreadPoolExecutor.class);
    private final String mbeanName;

    public DebuggableThreadPoolExecutor(String threadPoolName) 
    {
        this(1, 1, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(threadPoolName));
    }

    public DebuggableThreadPoolExecutor(int corePoolSize,
                                        int maximumPoolSize,
                                        long keepAliveTime,
                                        TimeUnit unit,
                                        BlockingQueue<Runnable> workQueue,
                                        NamedThreadFactory threadFactory)
    {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        super.prestartAllCoreThreads();

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        mbeanName = "org.apache.cassandra.concurrent:type=" + threadFactory.id;
        try
        {
            mbs.registerMBean(this, new ObjectName(mbeanName));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

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

    private void unregisterMBean()
    {
        try
        {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(mbeanName));
        }
        catch (Exception ex)
        {
            // don't let it get in the way, but notify.
            logger_.error(ex.getMessage(), ex);
        }
    }

    @Override
    public void shutdown()
    {
        unregisterMBean();
        super.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        unregisterMBean();
        return super.shutdownNow();
    }

    /**
     * Get the number of completed tasks
     */
    public long getCompletedTasks()
    {
        return getCompletedTaskCount();
    }

    /**
     * Get the number of tasks waiting to be executed
     */
    public long getPendingTasks()
    {
        return getTaskCount() - getCompletedTaskCount();
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
                logger_.error("Error in executor futuretask", e);
            }
        }

        // exceptions for non-FutureTask runnables [i.e., added via execute() instead of submit()]
        if (t != null)
        {
            logger_.error("Error in ThreadPoolExecutor", t);
        }
    }

}
