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

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.cassandra.concurrent.LocalAwareExecutorService;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Metric;
import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;


/**
 * Metrics for {@link ThreadPoolExecutor}.
 */
public class ThreadPoolMetrics
{
    public static enum Type
    {
        CURRENTLY_BLOCKED_TASKS("CurrentlyBlockedTasks"),
        TOTAL_BLOCKED_TASKS("TotalBlockedTasks"),
        MAX_POOL_SIZE("MaxPoolSize"),
        COMPLETED_TASKS("CompletedTasks"),
        PENDING_TASKS("PendingTasks"),
        MAX_TASKS_QUEUED("MaxTasksQueued"),
        ACTIVE_TASKS("ActiveTasks");

        /* legacy name used in JMX */
        public final String name;

        private Type(String name)
        {
            this.name = name;
        }

        public static Type fromName(String name)
        {
            for (Type t : Type.values())
                if (t.name.equals(name))
                    return t;
            throw new IllegalArgumentException("Invalid ThreadPoolMetrics.Type " + name);
        }
    }

    private final static SetMultimap<String, ThreadPoolMetric> threadPools = Multimaps.synchronizedSetMultimap(HashMultimap.create());

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

    private final MetricNameFactory factory;

    /**
     * Create metrics for given ThreadPoolExecutor.
     *
     * @param executor Thread pool
     * @param path Type of thread pool
     * @param poolName Name of thread pool to identify metrics
     */
    public ThreadPoolMetrics(final LocalAwareExecutorService executor, String path, String poolName)
    {
        this.factory = newMetricNameFactory(path, poolName);

        activeTasks = register(poolName, Type.ACTIVE_TASKS, executor::getActiveCount);
        totalBlocked = counter(poolName, Type.TOTAL_BLOCKED_TASKS);
        currentBlocked = counter(poolName, Type.CURRENTLY_BLOCKED_TASKS);
        completedTasks = register(poolName, Type.COMPLETED_TASKS, executor::getCompletedTaskCount);
        pendingTasks = register(poolName, Type.PENDING_TASKS, executor::getPendingTaskCount);
        maxPoolSize = register(poolName, Type.MAX_POOL_SIZE, executor::getMaximumPoolSize);
    }

    public void release()
    {
        remove(Type.ACTIVE_TASKS);
        remove(Type.PENDING_TASKS);
        remove(Type.COMPLETED_TASKS);
        remove(Type.TOTAL_BLOCKED_TASKS);
        remove(Type.CURRENTLY_BLOCKED_TASKS);
        remove(Type.MAX_POOL_SIZE);
    }

    public static Object getJmxMetric(MBeanServerConnection mbeanServerConn, String jmxPath, String poolName, String metricName)
    {
        String name = mbeanName(jmxPath, poolName, metricName);

        try
        {
            ObjectName oName = new ObjectName(name);
            if (!mbeanServerConn.isRegistered(oName))
            {
                return "N/A";
            }

            switch (Type.fromName(metricName))
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

    public static Collection<String> poolNames()
    {
        return threadPools.keySet();
    }

    public static Collection<ThreadPoolMetric> getPoolMetrics(String poolName)
    {
        return Collections.unmodifiableCollection(threadPools.get(poolName));
    }

    private static String mbeanName(String jmxPath, String poolName, String metricName)
    {
        return String.format("org.apache.cassandra.metrics:type=ThreadPools,path=%s,scope=%s,name=%s", jmxPath, poolName, metricName);
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

    private MetricNameFactory newMetricNameFactory(String path, String poolName)
    {
        return metricName -> new CassandraMetricsRegistry.MetricName("org.apache.cassandra.metrics",
                "ThreadPools",
                metricName, path + '.' + poolName,
                mbeanName(path, poolName, metricName));
    }

    protected final Counter counter(String pool, Type name)
    {
        ThreadPoolCounter counter = new ThreadPoolCounter(name);
        threadPools.put(pool, counter);
        return Metrics.register(factory.createMetricName(counter.getType().name), counter);
    }

    protected final <T extends Number> Gauge<T> register(String pool, Type name, Gauge<T> gauge)
    {
        ThreadPoolGauge<T> tpg = new ThreadPoolGauge<>(name, gauge);
        threadPools.put(pool, tpg);
        return Metrics.register(factory.createMetricName(tpg.getType().name), tpg);
    }

    protected final void remove(Type type)
    {
        Metrics.remove(factory.createMetricName(type.name));
    }

    public static interface ThreadPoolMetric extends Metric
    {
        public Type getType();

        public Long getLongValue();
    }

    private static class ThreadPoolCounter extends Counter implements ThreadPoolMetric
    {
        private final Type metric;

        public ThreadPoolCounter(Type metric)
        {
            this.metric = metric;
        }

        public Type getType()
        {
            return metric;
        }

        public Long getLongValue()
        {
            return getCount();
        }

        public int hashCode()
        {
            return Objects.hashCode(metric);
        }

        public boolean equals(Object obj)
        {
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            final ThreadPoolCounter other = (ThreadPoolCounter) obj;
            return Objects.equal(metric, other.metric);
        }
    }

    private static class ThreadPoolGauge<T extends Number> implements Gauge<T>, ThreadPoolMetric
    {
        private final Type metric;
        private final Gauge<T> wrapped;

        public ThreadPoolGauge(Type metric, Gauge<T> wrapped)
        {
            this.metric = metric;
            this.wrapped = wrapped;
        }

        public T getValue()
        {
            return wrapped.getValue();
        }

        public Type getType()
        {
            return metric;
        }

        public Long getLongValue()
        {
            return ((Number) wrapped.getValue()).longValue();
        }

        public int hashCode()
        {
            return Objects.hashCode(metric);
        }

        public boolean equals(Object obj)
        {
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            final ThreadPoolCounter other = (ThreadPoolCounter) obj;
            return Objects.equal(metric, other.metric);
        }
    }
}
