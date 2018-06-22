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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;

import org.apache.cassandra.concurrent.SEPExecutor;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class SEPMetrics
{
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
    /** Maximum number of tasks queued before a task get blocked */
    public final Gauge<Integer> maxTasksQueued;

    private MetricNameFactory factory;

    /**
     * Create metrics for the given LowSignalExecutor.
     *
     * @param executor Thread pool
     * @param path Type of thread pool
     * @param poolName Name of thread pool to identify metrics
     */
    public SEPMetrics(final SEPExecutor executor, String path, String poolName)
    {
        this.factory = new ThreadPoolMetricNameFactory("ThreadPools", path, poolName);
        activeTasks = Metrics.register(factory.createMetricName(ThreadPoolMetrics.ACTIVE_TASKS), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return executor.getActiveCount();
            }
        });
        pendingTasks = Metrics.register(factory.createMetricName(ThreadPoolMetrics.PENDING_TASKS), new Gauge<Long>()
        {
            public Long getValue()
            {
                return executor.getPendingTasks();
            }
        });
        totalBlocked = Metrics.counter(factory.createMetricName(ThreadPoolMetrics.TOTAL_BLOCKED_TASKS));
        currentBlocked = Metrics.counter(factory.createMetricName(ThreadPoolMetrics.CURRENTLY_BLOCKED_TASKS));

        completedTasks = Metrics.register(factory.createMetricName(ThreadPoolMetrics.COMPLETED_TASKS), new Gauge<Long>()
        {
            public Long getValue()
            {
                return executor.getCompletedTasks();
            }
        });
        maxPoolSize =  Metrics.register(factory.createMetricName(ThreadPoolMetrics.MAX_POOL_SIZE), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return executor.maxWorkers;
            }
        });
        maxTasksQueued =  Metrics.register(factory.createMetricName(ThreadPoolMetrics.MAX_TASKS_QUEUED), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return executor.maxTasksQueued;
            }
        });
    }

    public void release()
    {
        Metrics.remove(factory.createMetricName(ThreadPoolMetrics.ACTIVE_TASKS));
        Metrics.remove(factory.createMetricName(ThreadPoolMetrics.PENDING_TASKS));
        Metrics.remove(factory.createMetricName(ThreadPoolMetrics.COMPLETED_TASKS));
        Metrics.remove(factory.createMetricName(ThreadPoolMetrics.TOTAL_BLOCKED_TASKS));
        Metrics.remove(factory.createMetricName(ThreadPoolMetrics.CURRENTLY_BLOCKED_TASKS));
        Metrics.remove(factory.createMetricName(ThreadPoolMetrics.MAX_POOL_SIZE));
        Metrics.remove(factory.createMetricName(ThreadPoolMetrics.MAX_TASKS_QUEUED));
    }
}
