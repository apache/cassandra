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

    private ObjectName objName;
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
            objName = new ObjectName("org.apache.cassandra.concurrent:type=" + threadFactory.id);
            mbs.registerMBean(this, objName);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public long getPendingTasks()
    {
        return getTaskCount() - getCompletedTaskCount();
    }

    /*
     * 
     *  (non-Javadoc)
     * @see java.util.concurrent.ThreadPoolExecutor#afterExecute(java.lang.Runnable, java.lang.Throwable)
     * Helps us in figuring out why sometimes the threads are getting 
     * killed and replaced by new ones.
     */
    public void afterExecute(Runnable r, Throwable t)
    {
        super.afterExecute(r,t);

        logFutureExceptions(r);
        if (t != null)
        {
            logger_.error("Error in ThreadPoolExecutor", t);
        }
    }

    public static void logFutureExceptions(Runnable r)
    {
        if (r instanceof FutureTask)
        {
            try
            {
                ((FutureTask)r).get();
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
    }
}
