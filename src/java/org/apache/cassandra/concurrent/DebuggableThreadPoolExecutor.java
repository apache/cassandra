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
        try
        {
            mbs.registerMBean(this, new ObjectName("org.apache.cassandra.concurrent:type=" + threadFactory.id));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        if (maximumPoolSize > 1)
        {
            this.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        }
        else
        {
            // preserve task serialization.  this is more complicated than it needs to be,
            // since TPE rejects if queue.offer reports a full queue.
            // the easiest option (since most of TPE.execute deals with private members)
            // appears to be to wrap the given queue class with one whose offer
            // simply delegates to put().  this would be ugly, since it violates both
            // the spirit and letter of queue.offer, but effective.
            // so far, though, all our serialized executors use unbounded queues,
            // so actually implementing this has not been necessary.
            this.setRejectedExecutionHandler(new RejectedExecutionHandler()
            {
                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
                {
                    throw new AssertionError("Blocking serialized executor is not yet implemented");
                }
            });
        }
    }

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
