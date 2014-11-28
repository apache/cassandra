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

import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;

/**
 * Metrics for {@link ColumnFamilyStore}.
 */
public class KeyspaceMetrics
{
    /** Total amount of live data stored in the memtable, excluding any data structure overhead */
    public final Gauge<Long> memtableLiveDataSize;
    /** Total amount of data stored in the memtable that resides on-heap, including column related overhead and overwritten rows. */
    public final Gauge<Long> memtableOnHeapDataSize;
    /** Total amount of data stored in the memtable that resides off-heap, including column related overhead and overwritten rows. */
    public final Gauge<Long> memtableOffHeapDataSize;
    /** Total amount of live data stored in the memtables (2i and pending flush memtables included) that resides off-heap, excluding any data structure overhead */
    public final Gauge<Long> allMemtablesLiveDataSize;
    /** Total amount of data stored in the memtables (2i and pending flush memtables included) that resides on-heap. */
    public final Gauge<Long> allMemtablesOnHeapDataSize;
    /** Total amount of data stored in the memtables (2i and pending flush memtables included) that resides off-heap. */
    public final Gauge<Long> allMemtablesOffHeapDataSize;
    /** Total number of columns present in the memtable. */
    public final Gauge<Long> memtableColumnsCount;
    /** Number of times flush has resulted in the memtable being switched out. */
    public final Gauge<Long> memtableSwitchCount;
    /** Estimated number of tasks pending for this column family */
    public final Gauge<Long> pendingFlushes;
    /** Estimate of number of pending compactios for this CF */
    public final Gauge<Long> pendingCompactions;
    /** Disk space used by SSTables belonging to this CF */
    public final Gauge<Long> liveDiskSpaceUsed;
    /** Total disk space used by SSTables belonging to this CF, including obsolete ones waiting to be GC'd */
    public final Gauge<Long> totalDiskSpaceUsed;
    /** Disk space used by bloom filter */
    public final Gauge<Long> bloomFilterDiskSpaceUsed;
    /** Off heap memory used by bloom filter */
    public final Gauge<Long> bloomFilterOffHeapMemoryUsed;
    /** Off heap memory used by index summary */
    public final Gauge<Long> indexSummaryOffHeapMemoryUsed;
    /** Off heap memory used by compression meta data*/
    public final Gauge<Long> compressionMetadataOffHeapMemoryUsed;
    /** (Local) read metrics */
    public final LatencyMetrics readLatency;
    /** (Local) range slice metrics */
    public final LatencyMetrics rangeLatency;
    /** (Local) write metrics */
    public final LatencyMetrics writeLatency;
    /** Histogram of the number of sstable data files accessed per read */
    public final Histogram sstablesPerReadHistogram;
    /** Tombstones scanned in queries on this Keyspace */
    public final Histogram tombstoneScannedHistogram;
    /** Live cells scanned in queries on this Keyspace */
    public final Histogram liveScannedHistogram;
    /** Column update time delta on this Keyspace */
    public final Histogram colUpdateTimeDeltaHistogram;
    /** CAS Prepare metric */
    public final LatencyMetrics casPrepare;
    /** CAS Propose metrics */
    public final LatencyMetrics casPropose;
    /** CAS Commit metrics */
    public final LatencyMetrics casCommit;
    
    public final MetricNameFactory factory;
    private Keyspace keyspace;
    
    /** set containing names of all the metrics stored here, for releasing later */
    private Set<String> allMetrics = Sets.newHashSet();
    
    /**
     * Creates metrics for given {@link ColumnFamilyStore}.
     *
     * @param ks Keyspace to measure metrics
     */
    public KeyspaceMetrics(final Keyspace ks)
    {
        factory = new KeyspaceMetricNameFactory(ks);
        keyspace = ks;
        memtableColumnsCount = createKeyspaceGauge("MemtableColumnsCount", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return metric.memtableColumnsCount.value();
            }
        });
        memtableLiveDataSize = createKeyspaceGauge("MemtableLiveDataSize", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return metric.memtableLiveDataSize.value();
            }
        }); 
        memtableOnHeapDataSize = createKeyspaceGauge("MemtableOnHeapDataSize", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return metric.memtableOnHeapSize.value();
            }
        });
        memtableOffHeapDataSize = createKeyspaceGauge("MemtableOffHeapDataSize", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return metric.memtableOffHeapSize.value();
            }
        });
        allMemtablesLiveDataSize = createKeyspaceGauge("AllMemtablesLiveDataSize", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return metric.allMemtablesLiveDataSize.value();
            }
        });
        allMemtablesOnHeapDataSize = createKeyspaceGauge("AllMemtablesOnHeapDataSize", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return metric.allMemtablesOnHeapSize.value();
            }
        });
        allMemtablesOffHeapDataSize = createKeyspaceGauge("AllMemtablesOffHeapDataSize", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return metric.allMemtablesOffHeapSize.value();
            }
        });
        memtableSwitchCount = createKeyspaceGauge("MemtableSwitchCount", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return metric.memtableSwitchCount.count();
            }
        });
        pendingCompactions = createKeyspaceGauge("PendingCompactions", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return (long) metric.pendingCompactions.value();
            }
        });
        pendingFlushes = createKeyspaceGauge("PendingFlushes", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return (long) metric.pendingFlushes.count();
            }
        });
        liveDiskSpaceUsed = createKeyspaceGauge("LiveDiskSpaceUsed", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return metric.liveDiskSpaceUsed.count();
            }
        });
        totalDiskSpaceUsed = createKeyspaceGauge("TotalDiskSpaceUsed", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return metric.totalDiskSpaceUsed.count();
            }
        });
        bloomFilterDiskSpaceUsed = createKeyspaceGauge("BloomFilterDiskSpaceUsed", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return metric.bloomFilterDiskSpaceUsed.value();
            }
        });
        bloomFilterOffHeapMemoryUsed = createKeyspaceGauge("BloomFilterOffHeapMemoryUsed", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return metric.bloomFilterOffHeapMemoryUsed.value();
            }
        });
        indexSummaryOffHeapMemoryUsed = createKeyspaceGauge("IndexSummaryOffHeapMemoryUsed", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return metric.indexSummaryOffHeapMemoryUsed.value();
            }
        });
        compressionMetadataOffHeapMemoryUsed = createKeyspaceGauge("CompressionMetadataOffHeapMemoryUsed", new MetricValue()
        {
            public Long getValue(ColumnFamilyMetrics metric)
            {
                return metric.compressionMetadataOffHeapMemoryUsed.value();
            }
        });
        // latency metrics for ColumnFamilyMetrics to update
        readLatency = new LatencyMetrics(factory, "Read");
        writeLatency = new LatencyMetrics(factory, "Write");
        rangeLatency = new LatencyMetrics(factory, "Range");
        // create histograms for ColumnFamilyMetrics to replicate updates to
        sstablesPerReadHistogram = Metrics.newHistogram(factory.createMetricName("SSTablesPerReadHistogram"), true);
        tombstoneScannedHistogram = Metrics.newHistogram(factory.createMetricName("TombstoneScannedHistogram"), true);
        liveScannedHistogram = Metrics.newHistogram(factory.createMetricName("LiveScannedHistogram"), true);
        colUpdateTimeDeltaHistogram = Metrics.newHistogram(factory.createMetricName("ColUpdateTimeDeltaHistogram"), true);
        // add manually since histograms do not use createKeyspaceGauge method
        allMetrics.addAll(Lists.newArrayList("SSTablesPerReadHistogram", "TombstoneScannedHistogram", "LiveScannedHistogram"));

        casPrepare = new LatencyMetrics(factory, "CasPrepare");
        casPropose = new LatencyMetrics(factory, "CasPropose");
        casCommit = new LatencyMetrics(factory, "CasCommit");
    }

    /**
     * Release all associated metrics.
     */
    public void release()
    {
        for(String name : allMetrics) 
        {
            Metrics.defaultRegistry().removeMetric(factory.createMetricName(name));
        }
        // latency metrics contain multiple metrics internally and need to be released manually
        readLatency.release();
        writeLatency.release();
        rangeLatency.release();
    }
    
    /**
     * Represents a column family metric value.
     */
    private interface MetricValue
    {
        /**
         * get value of a metric
         * @param columnfamilymetrics of a column family in this keyspace
         * @return current value of a metric
         */
        public Long getValue(ColumnFamilyMetrics metric);
    }

    /**
     * Creates a gauge that will sum the current value of a metric for all column families in this keyspace
     * @param name
     * @param MetricValue 
     * @return Gauge&gt;Long> that computes sum of MetricValue.getValue()
     */
    private <T extends Number> Gauge<Long> createKeyspaceGauge(String name, final MetricValue extractor)
    {
        allMetrics.add(name);
        return Metrics.newGauge(factory.createMetricName(name), new Gauge<Long>()
        {
            public Long value()
            {
                long sum = 0;
                for (ColumnFamilyStore cf : keyspace.getColumnFamilyStores())
                {
                    sum += extractor.getValue(cf.metric);
                }
                return sum;
            }
        });
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
