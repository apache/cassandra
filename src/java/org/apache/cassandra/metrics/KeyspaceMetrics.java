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


import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;

/**
 * Metrics for {@link ColumnFamilyStore}.
 */
public class KeyspaceMetrics
{
    /** Total amount of data stored in the memtable, including column related overhead. */
    public final Gauge<Long> memtableDataSize;
    /** Total number of columns present in the memtable. */
    public final Gauge<Long> memtableColumnsCount;
    /** Number of times flush has resulted in the memtable being switched out. */
    public final Gauge<Long> memtableSwitchCount;
    /** Estimated number of tasks pending for this column family */
    public final Gauge<Integer> pendingTasks;
    /** Disk space used by SSTables belonging to this CF */
    public final Gauge<Long> liveDiskSpaceUsed;
    /** Total disk space used by SSTables belonging to this CF, including obsolete ones waiting to be GC'd */
    public final Gauge<Long> totalDiskSpaceUsed;
    /** Disk space used by bloom filter */
    public final Gauge<Long> bloomFilterDiskSpaceUsed;

    private final MetricNameFactory factory;

    /**
     * Creates metrics for given {@link ColumnFamilyStore}.
     *
     * @param ks Keyspace to measure metrics
     */
    public KeyspaceMetrics(final Table ks)
    {
        factory = new KeyspaceMetricNameFactory(ks);

        memtableColumnsCount = Metrics.newGauge(factory.createMetricName("MemtableColumnsCount"), new Gauge<Long>()
        {
            public Long value()
            {
                long total = 0;
                for (ColumnFamilyStore cf : ks.getColumnFamilyStores())
                {
                    total += cf.metric.memtableColumnsCount.value();
                }
                return total;
            }
        });
        memtableDataSize = Metrics.newGauge(factory.createMetricName("MemtableDataSize"), new Gauge<Long>()
        {
            public Long value()
            {
                long total = 0;
                for (ColumnFamilyStore cf : ks.getColumnFamilyStores())
                {
                    total += cf.metric.memtableDataSize.value();
                }
                return total;
            }
        });
        memtableSwitchCount = Metrics.newGauge(factory.createMetricName("MemtableSwitchCount"), new Gauge<Long>()
        {
            public Long value()
            {
                long sum = 0;
                for (ColumnFamilyStore cf : ks.getColumnFamilyStores())
                    sum += cf.metric.memtableSwitchCount.count();
                return sum;
            }
        });
        pendingTasks = Metrics.newGauge(factory.createMetricName("PendingTasks"), new Gauge<Integer>()
        {
            public Integer value()
            {
                return Table.switchLock.getQueueLength();
            }
        });
        liveDiskSpaceUsed = Metrics.newGauge(factory.createMetricName("LiveDiskSpaceUsed"), new Gauge<Long>()
        {
            public Long value()
            {
                long sum = 0;
                for (ColumnFamilyStore cf : ks.getColumnFamilyStores())
                {
                    sum += cf.metric.liveDiskSpaceUsed.count();
                }
                return sum;
            }
        });
        totalDiskSpaceUsed = Metrics.newGauge(factory.createMetricName("TotalDiskSpaceUsed"), new Gauge<Long>()
        {
            public Long value()
            {
                long sum = 0;
                for (ColumnFamilyStore cf : ks.getColumnFamilyStores())
                {
                    sum += cf.metric.totalDiskSpaceUsed.count();
                }
                return sum;
            }
        });
        bloomFilterDiskSpaceUsed = Metrics.newGauge(factory.createMetricName("BloomFilterDiskSpaceUsed"), new Gauge<Long>()
        {
            public Long value()
            {
                long total = 0;
                for (ColumnFamilyStore cf : ks.getColumnFamilyStores())
                    total += cf.metric.bloomFilterDiskSpaceUsed.value();
                return total;
            }
        });
    }

    /**
     * Release all associated metrics.
     */
    public void release()
    {
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("AllMemtablesDataSize"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("MemtableColumnsCount"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("MemtableDataSize"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("MemtableSwitchCount"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("PendingTasks"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("LiveDiskSpaceUsed"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("TotalDiskSpaceUsed"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("BloomFilterDiskSpaceUsed"));
    }

    class KeyspaceMetricNameFactory implements MetricNameFactory
    {
        private final String keyspaceName;

        KeyspaceMetricNameFactory(Table ks)
        {
            this.keyspaceName = ks.name;
        }

        public MetricName createMetricName(String metricName)
        {
            String groupName = ColumnFamilyMetrics.class.getPackage().getName();

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=Keyspace");
            mbeanName.append(",keyspace=").append(keyspaceName);
            mbeanName.append(",name=").append(metricName);

            return new MetricName(groupName, "keyspace", metricName, keyspaceName, mbeanName.toString());
        }
    }
}
