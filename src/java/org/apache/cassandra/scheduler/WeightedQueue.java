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
package org.apache.cassandra.scheduler;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.metrics.LatencyMetrics;

class WeightedQueue implements WeightedQueueMBean
{
    private final LatencyMetrics metric;

    public final String key;
    public final int weight;
    private final SynchronousQueue<Entry> queue;
    public WeightedQueue(String key, int weight)
    {
        this.key = key;
        this.weight = weight;
        this.queue = new SynchronousQueue<Entry>(true);
        this.metric =  new LatencyMetrics("scheduler", "WeightedQueue", key);
    }

    public void register()
    {
        // expose monitoring data
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName("org.apache.cassandra.scheduler:type=WeightedQueue,queue=" + key));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void put(Thread t, long timeoutMS) throws InterruptedException, TimeoutException
    {
        if (!queue.offer(new WeightedQueue.Entry(t), timeoutMS, TimeUnit.MILLISECONDS))
            throw new TimeoutException("Failed to acquire request scheduler slot for '" + key + "'");
    }

    public Thread poll()
    {
        Entry e = queue.poll();
        if (e == null)
            return null;
        metric.addNano(System.nanoTime() - e.creationTime);
        return e.thread;
    }

    @Override
    public String toString()
    {
        return "RoundRobinScheduler.WeightedQueue(key=" + key + " weight=" + weight + ")";
    }

    private final static class Entry
    {
        public final long creationTime = System.nanoTime();
        public final Thread thread;
        public Entry(Thread thread)
        {
            this.thread = thread;
        }
    }

    /** MBean related methods */

    public long getOperations()
    {
        return metric.latency.count();
    }

    public long getTotalLatencyMicros()
    {
        return metric.totalLatency.count();
    }

    public double getRecentLatencyMicros()
    {
        return metric.getRecentLatency();
    }

    public long[] getTotalLatencyHistogramMicros()
    {
        return metric.totalLatencyHistogram.getBuckets(false);
    }

    public long[] getRecentLatencyHistogramMicros()
    {
        return metric.recentLatencyHistogram.getBuckets(true);
    }
}
