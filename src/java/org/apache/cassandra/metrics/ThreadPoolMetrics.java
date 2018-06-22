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

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;


/**
 * Metrics for {@link ThreadPoolExecutor}.
 */
public class ThreadPoolMetrics
{
    public static final String CURRENTLY_BLOCKED_TASKS = "CurrentlyBlockedTasks";
    public static final String TOTAL_BLOCKED_TASKS = "TotalBlockedTasks";
    public static final String MAX_POOL_SIZE = "MaxPoolSize";
    public static final String MAX_TASKS_QUEUED = "MaxTasksQueued";
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

    private MetricNameFactory factory;

    /**
     * Create metrics for given ThreadPoolExecutor.
     *
     * @param executor Thread pool
     * @param path Type of thread pool
     * @param poolName Name of thread pool to identify metrics
     */
    public ThreadPoolMetrics(final ThreadPoolExecutor executor, String path, String poolName)
    {
        this.factory = new ThreadPoolMetricNameFactory("ThreadPools", path, poolName);

        activeTasks = Metrics.register(factory.createMetricName(ACTIVE_TASKS), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return executor.getActiveCount();
            }
        });
        totalBlocked = Metrics.counter(factory.createMetricName(TOTAL_BLOCKED_TASKS));
        currentBlocked = Metrics.counter(factory.createMetricName(CURRENTLY_BLOCKED_TASKS));
        completedTasks = Metrics.register(factory.createMetricName(COMPLETED_TASKS), new Gauge<Long>()
        {
            public Long getValue()
            {
                return executor.getCompletedTaskCount();
            }
        });
        pendingTasks = Metrics.register(factory.createMetricName(PENDING_TASKS), new Gauge<Long>()
        {
            public Long getValue()
            {
                return executor.getTaskCount() - executor.getCompletedTaskCount();
            }
        });
        maxPoolSize = Metrics.register(factory.createMetricName(MAX_POOL_SIZE), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return executor.getMaximumPoolSize();
            }
        });
    }

    public void release()
    {
        Metrics.remove(factory.createMetricName(ACTIVE_TASKS));
        Metrics.remove(factory.createMetricName(PENDING_TASKS));
        Metrics.remove(factory.createMetricName(COMPLETED_TASKS));
        Metrics.remove(factory.createMetricName(TOTAL_BLOCKED_TASKS));
        Metrics.remove(factory.createMetricName(CURRENTLY_BLOCKED_TASKS));
        Metrics.remove(factory.createMetricName(MAX_POOL_SIZE));
    }

    public static Object getJmxMetric(MBeanServerConnection mbeanServerConn, String jmxPath, String poolName, String metricName)
    {
        String name = String.format("org.apache.cassandra.metrics:type=ThreadPools,path=%s,scope=%s,name=%s", jmxPath, poolName, metricName);

        try
        {
            ObjectName oName = new ObjectName(name);
            if (!mbeanServerConn.isRegistered(oName))
            {
                return "N/A";
            }

            switch (metricName)
            {
                case ACTIVE_TASKS:
                case PENDING_TASKS:
                case COMPLETED_TASKS:
                case MAX_POOL_SIZE:
                    return JMX.newMBeanProxy(mbeanServerConn, oName, JmxReporter.JmxGaugeMBean.class).getValue();
                case TOTAL_BLOCKED_TASKS:
                case CURRENTLY_BLOCKED_TASKS:
                    return JMX.newMBeanProxy(mbeanServerConn, oName, JmxReporter.JmxCounterMBean.class).getCount();
                default:
                    throw new AssertionError("Unknown metric name " + metricName);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error reading: " + name, e);
        }
    }

    public static Multimap<String, String> getJmxThreadPools(MBeanServerConnection mbeanServerConn)
    {
        try
        {
            Multimap<String, String> threadPools = HashMultimap.create();
            Set<ObjectName> threadPoolObjectNames = mbeanServerConn.queryNames(new ObjectName("org.apache.cassandra.metrics:type=ThreadPools,*"),
                                                                               null);
            for (ObjectName oName : threadPoolObjectNames)
            {
                threadPools.put(oName.getKeyProperty("path"), oName.getKeyProperty("scope"));
            }

            return threadPools;
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException("Bad query to JMX server: ", e);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error getting threadpool names from JMX", e);
        }
    }

}
