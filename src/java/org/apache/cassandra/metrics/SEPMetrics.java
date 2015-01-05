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
        activeTasks = Metrics.register(factory.createMetricName("ActiveTasks"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return executor.getActiveCount();
            }
        });
        pendingTasks = Metrics.register(factory.createMetricName("PendingTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return executor.getPendingTasks();
            }
        });
        totalBlocked = Metrics.counter(factory.createMetricName("TotalBlockedTasks"));
        currentBlocked = Metrics.counter(factory.createMetricName("CurrentlyBlockedTasks"));

        completedTasks = Metrics.register(factory.createMetricName("CompletedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return executor.getCompletedTasks();
            }
        });
        maxPoolSize =  Metrics.register(factory.createMetricName("MaxPoolSize"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return executor.maxWorkers;
            }
        });
    }

    public void release()
    {
        Metrics.remove(factory.createMetricName("ActiveTasks"));
        Metrics.remove(factory.createMetricName("PendingTasks"));
        Metrics.remove(factory.createMetricName("CompletedTasks"));
        Metrics.remove(factory.createMetricName("TotalBlockedTasks"));
        Metrics.remove(factory.createMetricName("CurrentlyBlockedTasks"));
        Metrics.remove(factory.createMetricName("MaxPoolSize"));
    }
}
