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

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for compaction.
 */
public class CompactionMetrics implements CompactionManager.CompactionExecutorStatsCollector
{
    public static final MetricNameFactory factory = new DefaultNameFactory("Compaction");

    // a synchronized identity set of running tasks to their compaction info
    private static final Set<CompactionInfo.Holder> compactions = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<CompactionInfo.Holder, Boolean>()));

    /** Estimated number of compactions remaining to perform */
    public final Gauge<Integer> pendingTasks;
    /** Number of completed compactions since server [re]start */
    public final Gauge<Long> completedTasks;
    /** Total number of compactions since server [re]start */
    public final Meter totalCompactionsCompleted;
    /** Total number of bytes compacted since server [re]start */
    public final Counter bytesCompacted;

    public CompactionMetrics(final ThreadPoolExecutor... collectors)
    {
        pendingTasks = Metrics.register(factory.createMetricName("PendingTasks"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                int n = 0;
                for (String keyspaceName : Schema.instance.getKeyspaces())
                {
                    for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
                        n += cfs.getCompactionStrategy().getEstimatedRemainingTasks();
                }
                for (ThreadPoolExecutor collector : collectors)
                    n += collector.getTaskCount() - collector.getCompletedTaskCount();
                return n;
            }
        });
        completedTasks = Metrics.register(factory.createMetricName("CompletedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                long completedTasks = 0;
                for (ThreadPoolExecutor collector : collectors)
                    completedTasks += collector.getCompletedTaskCount();
                return completedTasks;
            }
        });
        totalCompactionsCompleted = Metrics.meter(factory.createMetricName("TotalCompactionsCompleted"));
        bytesCompacted = Metrics.counter(factory.createMetricName("BytesCompacted"));
    }

    public void beginCompaction(CompactionInfo.Holder ci)
    {
        // notify
        ci.started();
        compactions.add(ci);
    }

    public void finishCompaction(CompactionInfo.Holder ci)
    {
        // notify
        ci.finished();
        compactions.remove(ci);
        bytesCompacted.inc(ci.getCompactionInfo().getTotal());
        totalCompactionsCompleted.mark();
    }

    public static List<CompactionInfo.Holder> getCompactions()
    {
        return new ArrayList<CompactionInfo.Holder>(compactions);
    }
}
