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
package org.apache.cassandra.metrics;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.cassandra.concurrent.LocalAwareExecutorService;
import org.apache.cassandra.concurrent.SEPExecutor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;


/**
 * Metrics for {@link ThreadPoolExecutor}.
 */
public class ThreadPoolMetrics
{
    public static final String CURRENTLY_BLOCKED_TASKS = "CurrentlyBlockedTasks";
    public static final String TOTAL_BLOCKED_TASKS = "TotalBlockedTasks";
    public static final String MAX_POOL_SIZE = "MaxPoolSize";
    public static final String COMPLETED_TASKS = "CompletedTasks";
    public static final String PENDING_TASKS = "PendingTasks";
    public static final String ACTIVE_TASKS = "ActiveTasks";

    /** Number of active tasks. */
    public final Gauge<Integer> activeTasks;
    /** Number of tasks that had blocked before being accepted (or rejected). */
    public final Counter totalBlocked;
    /**
     * Number of tasks currently blocked, waiting to be accepted by
     * the executor (because all threads are busy and the backing queue is full).
     */
    public final Counter currentBlocked;
    /** Number of completed tasks. */
    public final Gauge<Long> completedTasks;
    /** Number of tasks waiting to be executed. */
    public final Gauge<Long> pendingTasks;
    /** Maximum number of threads before it will start queuing tasks */
    public final Gauge<Integer> maxPoolSize;

    public static final String MAX_TASKS_QUEUED = "MaxTasksQueued";

    /** Maximum number of tasks queued before a task get blocked */
    public final Gauge<Integer> maxTasksQueued;


    public final String path;
    public final String poolName;

    /**
     * Create metrics for given ThreadPoolExecutor.
     *
     * @param executor Thread pool
     * @param path Type of thread pool
     * @param poolName Name of thread pool to identify metrics
     */
    public ThreadPoolMetrics(final LocalAwareExecutorService executor, String path, String poolName)
    {
        this.path = path;
        this.poolName = poolName;

        activeTasks = executor::getActiveCount;
        totalBlocked = new Counter();
        currentBlocked = new Counter();
        completedTasks = executor::getCompletedTaskCount;
        pendingTasks = executor::getPendingTaskCount;
        maxPoolSize = executor::getMaximumPoolSize;
        maxTasksQueued = () -> -1;
    }

    /**
     * Create metrics for given ThreadPoolExecutor.
    *
    * @param executor Thread pool
    * @param path Type of thread pool
    * @param poolName Name of thread pool to identify metrics
    */
   public ThreadPoolMetrics(final SEPExecutor executor, String path, String poolName)
   {
       this.path = path;
       this.poolName = poolName;

       activeTasks = executor::getActiveCount;
       totalBlocked = new Counter();
       currentBlocked = new Counter();
       completedTasks = executor::getCompletedTaskCount;
       pendingTasks = executor::getPendingTaskCount;
       maxPoolSize = executor::getMaximumPoolSize;
       maxTasksQueued = executor::getMaxTasksQueued;
   }

}
