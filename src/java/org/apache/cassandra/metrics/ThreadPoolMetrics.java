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

        activeTasks = Metrics.register(factory.createMetricName("ActiveTasks"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return executor.getActiveCount();
            }
        });
        totalBlocked = Metrics.counter(factory.createMetricName("TotalBlockedTasks"));
        currentBlocked = Metrics.counter(factory.createMetricName("CurrentlyBlockedTasks"));
        completedTasks = Metrics.register(factory.createMetricName("CompletedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return executor.getCompletedTaskCount();
            }
        });
        pendingTasks = Metrics.register(factory.createMetricName("PendingTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return executor.getTaskCount() - executor.getCompletedTaskCount();
            }
        });
        maxPoolSize = Metrics.register(factory.createMetricName("MaxPoolSize"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return executor.getMaximumPoolSize();
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
                case "ActiveTasks":
                case "PendingTasks":
                case "CompletedTasks":
                    return JMX.newMBeanProxy(mbeanServerConn, oName, JmxReporter.JmxGaugeMBean.class).getValue();
                case "TotalBlockedTasks":
                case "CurrentlyBlockedTasks":
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
