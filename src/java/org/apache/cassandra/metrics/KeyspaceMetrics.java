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

import java.util.ArrayList;
import java.util.List;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.stats.Snapshot;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

/**
 * Metrics for {@link ColumnFamilyStore}.
 */
public class KeyspaceMetrics
{
    /** Total amount of data stored in the memtable, including column related overhead. */
    public final Gauge<Long> memtableLiveDataSize;
    /** Total amount of data stored in the memtable that resides on-heap, including column related overhead and overwritten rows. */
    public final Gauge<Long> memtableOnHeapDataSize;
    /** Total amount of data stored in the memtable that resides off-heap, including column related overhead and overwritten rows. */
    public final Gauge<Long> memtableOffHeapDataSize;
    /** Total amount of data stored in the memtables (2i and pending flush memtables included) that resides on-heap. */
    public final Gauge<Long> allMemtablesOnHeapDataSize;
    /** Total amount of data stored in the memtables (2i and pending flush memtables included) that resides off-heap. */
    public final Gauge<Long> allMemtablesOffHeapDataSize;
    /** Total amount of live data stored in the memtables (2i and pending flush memtables included) that resides off-heap, excluding any data structure overhead */
    public final Gauge<Long> allMemtablesLiveDataSize;
    /** Total number of columns present in the memtable. */
    public final Gauge<Long> memtableColumnsCount;
    /** Number of times flush has resulted in the memtable being switched out. */
    public final Gauge<Long> memtableSwitchCount;
    /** Estimated number of tasks pending for this column family */
    public final Gauge<Integer> pendingFlushes;
    /** Estimate of number of pending compactios for this CF */
    public final Gauge<Integer> pendingCompactions;
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
    public KeyspaceMetrics(final Keyspace ks)
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
        memtableOnHeapDataSize = Metrics.newGauge(factory.createMetricName("MemtableOnHeapDataSize"), new Gauge<Long>()
        {
            public Long value()
            {
                long total = 0;
                for (ColumnFamilyStore cf : ks.getColumnFamilyStores())
                {
                    total += cf.metric.memtableOnHeapSize.value();
                }
                return total;
            }
        });
        memtableOffHeapDataSize = Metrics.newGauge(factory.createMetricName("MemtableOffHeapDataSize"), new Gauge<Long>()
        {
            public Long value()
            {
                long total = 0;
                for (ColumnFamilyStore cf : ks.getColumnFamilyStores())
                {
                    total += cf.metric.memtableOffHeapSize.value();
                }
                return total;
            }
        });
        memtableLiveDataSize = Metrics.newGauge(factory.createMetricName("MemtableLiveDataSize"), new Gauge<Long>()
        {
            public Long value()
            {
                long total = 0;
                for (ColumnFamilyStore cf : ks.getColumnFamilyStores())
                {
                    total += cf.metric.memtableLiveDataSize.value();
                }
                return total;
            }
        });
        allMemtablesOnHeapDataSize = Metrics.newGauge(factory.createMetricName("AllMemtablesOnHeapDataSize"), new Gauge<Long>()
        {
            public Long value()
            {
                long total = 0;
                for (ColumnFamilyStore cf : ks.getColumnFamilyStores())
                {
                    total += cf.metric.allMemtablesOnHeapSize.value();
                }
                return total;
            }
        });
        allMemtablesOffHeapDataSize = Metrics.newGauge(factory.createMetricName("AllMemtablesOffHeapDataSize"), new Gauge<Long>()
        {
            public Long value()
            {
                long total = 0;
                for (ColumnFamilyStore cf : ks.getColumnFamilyStores())
                {
                    total += cf.metric.allMemtablesOffHeapSize.value();
                }
                return total;
            }
        });
        allMemtablesLiveDataSize = Metrics.newGauge(factory.createMetricName("AllMemtablesLiveDataSize"), new Gauge<Long>()
        {
            public Long value()
            {
                long total = 0;
                for (ColumnFamilyStore cf : ks.getColumnFamilyStores())
                {
                    total += cf.metric.allMemtablesLiveDataSize.value();
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
        pendingCompactions = Metrics.newGauge(factory.createMetricName("PendingCompactions"), new Gauge<Integer>()
        {
            public Integer value()
            {
                int sum = 0;
                for (ColumnFamilyStore cf : ks.getColumnFamilyStores())
                {
                    sum += cf.metric.pendingCompactions.value();
                }
                return sum;
            }
        });
        pendingFlushes = Metrics.newGauge(factory.createMetricName("PendingFlushes"), new Gauge<Integer>()
        {
            public Integer value()
            {
                int sum = 0;
                for (ColumnFamilyStore cf : ks.getColumnFamilyStores())
                {
                    sum += cf.metric.pendingFlushes.count();
                }
                return sum;
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
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("AllMemtablesLiveDataSize"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("AllMemtablesOnHeapDataSize"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("AllMemtablesOffHeapDataSize"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("MemtableLiveDataSize"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("MemtableOnHeapDataSize"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("MemtableOffHeapDataSize"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("MemtableColumnsCount"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("MemtableSwitchCount"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("PendingFlushes"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("LiveDiskSpaceUsed"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("TotalDiskSpaceUsed"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("BloomFilterDiskSpaceUsed"));
    }

    class KeyspaceMetricNameFactory implements MetricNameFactory
    {
        private final String keyspaceName;

        KeyspaceMetricNameFactory(Keyspace ks)
        {
            this.keyspaceName = ks.getName();
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
