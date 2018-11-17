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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for {@link ColumnFamilyStore}.
 */
public class KeyspaceMetrics
{
    /** Total amount of live data stored in the memtable, excluding any data structure overhead */
    public final Gauge<Long> memtableLiveDataSize;
    /** Total amount of data stored in the memtable that resides on-heap, including column related overhead and partitions overwritten. */
    public final Gauge<Long> memtableOnHeapDataSize;
    /** Total amount of data stored in the memtable that resides off-heap, including column related overhead and partitions overwritten. */
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
    /** time taken acquiring the partition lock for materialized view updates on this keyspace */
    public final Timer viewLockAcquireTime;
    /** time taken during the local read of a materialized view update */
    public final Timer viewReadTime;
    /** CAS Prepare metric */
    public final LatencyMetrics casPrepare;
    /** CAS Propose metrics */
    public final LatencyMetrics casPropose;
    /** CAS Commit metrics */
    public final LatencyMetrics casCommit;
    /** Writes failed ideal consistency **/
    public final Counter writeFailedIdealCL;
    /** Ideal CL write latency metrics */
    public final LatencyMetrics idealCLWriteLatency;
    /** Speculative retries **/
    public final Counter speculativeRetries;
    /** Speculative retry occured but still timed out **/
    public final Counter speculativeFailedRetries;
    /** Needed to speculate, but didn't have enough replicas **/
    public final Counter speculativeInsufficientReplicas;
    /** Needed to write to a transient replica to satisfy quorum **/
    public final Counter additionalWrites;
    /** Number of started repairs as coordinator on this keyspace */
    public final Counter repairsStarted;
    /** Number of completed repairs as coordinator on this keyspace */
    public final Counter repairsCompleted;
    /** total time spent as a repair coordinator */
    public final Timer repairTime;
    /** total time spent preparing for repair */
    public final Timer repairPrepareTime;
    /** Time spent anticompacting */
    public final Timer anticompactionTime;
    /** total time spent creating merkle trees */
    public final Timer validationTime;
    /** total time spent syncing data after repair */
    public final Timer repairSyncTime;
    /** histogram over the number of bytes we have validated */
    public final Histogram bytesValidated;
    /** histogram over the number of partitions we have validated */
    public final Histogram partitionsValidated;

    /*
     * Metrics for inconsistencies detected between repaired data sets across replicas. These
     * are tracked on the coordinator.
     */

    /**
     * Incremented where an inconsistency is detected and there are no pending repair sessions affecting
     * the data being read, indicating a genuine mismatch between replicas' repaired data sets.
     */
    public final Meter confirmedRepairedInconsistencies;
    /**
     * Incremented where an inconsistency is detected, but there are pending & uncommitted repair sessions
     * in play on at least one replica. This may indicate a false positive as the inconsistency could be due to
     * replicas marking the repair session as committed at slightly different times and so some consider it to
     * be part of the repaired set whilst others do not.
     */
    public final Meter unconfirmedRepairedInconsistencies;

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
            public Long getValue(TableMetrics metric)
            {
                return metric.memtableColumnsCount.getValue();
            }
        });
        memtableLiveDataSize = createKeyspaceGauge("MemtableLiveDataSize", new MetricValue()
        {
            public Long getValue(TableMetrics metric)
            {
                return metric.memtableLiveDataSize.getValue();
            }
        });
        memtableOnHeapDataSize = createKeyspaceGauge("MemtableOnHeapDataSize", new MetricValue()
        {
            public Long getValue(TableMetrics metric)
            {
                return metric.memtableOnHeapSize.getValue();
            }
        });
        memtableOffHeapDataSize = createKeyspaceGauge("MemtableOffHeapDataSize", new MetricValue()
        {
            public Long getValue(TableMetrics metric)
            {
                return metric.memtableOffHeapSize.getValue();
            }
        });
        allMemtablesLiveDataSize = createKeyspaceGauge("AllMemtablesLiveDataSize", new MetricValue()
        {
            public Long getValue(TableMetrics metric)
            {
                return metric.allMemtablesLiveDataSize.getValue();
            }
        });
        allMemtablesOnHeapDataSize = createKeyspaceGauge("AllMemtablesOnHeapDataSize", new MetricValue()
        {
            public Long getValue(TableMetrics metric)
            {
                return metric.allMemtablesOnHeapSize.getValue();
            }
        });
        allMemtablesOffHeapDataSize = createKeyspaceGauge("AllMemtablesOffHeapDataSize", new MetricValue()
        {
            public Long getValue(TableMetrics metric)
            {
                return metric.allMemtablesOffHeapSize.getValue();
            }
        });
        memtableSwitchCount = createKeyspaceGauge("MemtableSwitchCount", new MetricValue()
        {
            public Long getValue(TableMetrics metric)
            {
                return metric.memtableSwitchCount.getCount();
            }
        });
        pendingCompactions = createKeyspaceGauge("PendingCompactions", new MetricValue()
        {
            public Long getValue(TableMetrics metric)
            {
                return (long) metric.pendingCompactions.getValue();
            }
        });
        pendingFlushes = createKeyspaceGauge("PendingFlushes", new MetricValue()
        {
            public Long getValue(TableMetrics metric)
            {
                return (long) metric.pendingFlushes.getCount();
            }
        });
        liveDiskSpaceUsed = createKeyspaceGauge("LiveDiskSpaceUsed", new MetricValue()
        {
            public Long getValue(TableMetrics metric)
            {
                return metric.liveDiskSpaceUsed.getCount();
            }
        });
        totalDiskSpaceUsed = createKeyspaceGauge("TotalDiskSpaceUsed", new MetricValue()
        {
            public Long getValue(TableMetrics metric)
            {
                return metric.totalDiskSpaceUsed.getCount();
            }
        });
        bloomFilterDiskSpaceUsed = createKeyspaceGauge("BloomFilterDiskSpaceUsed", new MetricValue()
        {
            public Long getValue(TableMetrics metric)
            {
                return metric.bloomFilterDiskSpaceUsed.getValue();
            }
        });
        bloomFilterOffHeapMemoryUsed = createKeyspaceGauge("BloomFilterOffHeapMemoryUsed", new MetricValue()
        {
            public Long getValue(TableMetrics metric)
            {
                return metric.bloomFilterOffHeapMemoryUsed.getValue();
            }
        });
        indexSummaryOffHeapMemoryUsed = createKeyspaceGauge("IndexSummaryOffHeapMemoryUsed", new MetricValue()
        {
            public Long getValue(TableMetrics metric)
            {
                return metric.indexSummaryOffHeapMemoryUsed.getValue();
            }
        });
        compressionMetadataOffHeapMemoryUsed = createKeyspaceGauge("CompressionMetadataOffHeapMemoryUsed", new MetricValue()
        {
            public Long getValue(TableMetrics metric)
            {
                return metric.compressionMetadataOffHeapMemoryUsed.getValue();
            }
        });
        // latency metrics for TableMetrics to update
        readLatency = new LatencyMetrics(factory, "Read");
        writeLatency = new LatencyMetrics(factory, "Write");
        rangeLatency = new LatencyMetrics(factory, "Range");
        // create histograms for TableMetrics to replicate updates to
        sstablesPerReadHistogram = Metrics.histogram(factory.createMetricName("SSTablesPerReadHistogram"), true);
        tombstoneScannedHistogram = Metrics.histogram(factory.createMetricName("TombstoneScannedHistogram"), false);
        liveScannedHistogram = Metrics.histogram(factory.createMetricName("LiveScannedHistogram"), false);
        colUpdateTimeDeltaHistogram = Metrics.histogram(factory.createMetricName("ColUpdateTimeDeltaHistogram"), false);
        viewLockAcquireTime =  Metrics.timer(factory.createMetricName("ViewLockAcquireTime"));
        viewReadTime = Metrics.timer(factory.createMetricName("ViewReadTime"));
        // add manually since histograms do not use createKeyspaceGauge method
        allMetrics.addAll(Lists.newArrayList("SSTablesPerReadHistogram", "TombstoneScannedHistogram", "LiveScannedHistogram"));

        casPrepare = new LatencyMetrics(factory, "CasPrepare");
        casPropose = new LatencyMetrics(factory, "CasPropose");
        casCommit = new LatencyMetrics(factory, "CasCommit");
        writeFailedIdealCL = Metrics.counter(factory.createMetricName("WriteFailedIdealCL"));
        idealCLWriteLatency = new LatencyMetrics(factory, "IdealCLWrite");

        speculativeRetries = createKeyspaceCounter("SpeculativeRetries", metric -> metric.speculativeRetries.getCount());
        speculativeFailedRetries = createKeyspaceCounter("SpeculativeFailedRetries", metric -> metric.speculativeFailedRetries.getCount());
        speculativeInsufficientReplicas = createKeyspaceCounter("SpeculativeInsufficientReplicas", metric -> metric.speculativeInsufficientReplicas.getCount());
        additionalWrites = createKeyspaceCounter("AdditionalWrites", metric -> metric.additionalWrites.getCount());
        repairsStarted = createKeyspaceCounter("RepairJobsStarted", metric -> metric.repairsStarted.getCount());
        repairsCompleted = createKeyspaceCounter("RepairJobsCompleted", metric -> metric.repairsCompleted.getCount());
        repairTime = Metrics.timer(factory.createMetricName("RepairTime"));
        repairPrepareTime = Metrics.timer(factory.createMetricName("RepairPrepareTime"));
        anticompactionTime = Metrics.timer(factory.createMetricName("AntiCompactionTime"));
        validationTime = Metrics.timer(factory.createMetricName("ValidationTime"));
        repairSyncTime = Metrics.timer(factory.createMetricName("RepairSyncTime"));
        partitionsValidated = Metrics.histogram(factory.createMetricName("PartitionsValidated"), false);
        bytesValidated = Metrics.histogram(factory.createMetricName("BytesValidated"), false);

        confirmedRepairedInconsistencies = Metrics.meter(factory.createMetricName("RepairedDataInconsistenciesConfirmed"));
        unconfirmedRepairedInconsistencies = Metrics.meter(factory.createMetricName("RepairedDataInconsistenciesUnconfirmed"));
    }

    /**
     * Release all associated metrics.
     */
    public void release()
    {
        for(String name : allMetrics)
        {
            Metrics.remove(factory.createMetricName(name));
        }
        // latency metrics contain multiple metrics internally and need to be released manually
        readLatency.release();
        writeLatency.release();
        rangeLatency.release();
        idealCLWriteLatency.release();
    }

    /**
     * Represents a column family metric value.
     */
    private interface MetricValue
    {
        /**
         * get value of a metric
         * @param metric of a column family in this keyspace
         * @return current value of a metric
         */
        public Long getValue(TableMetrics metric);
    }

    /**
     * Creates a gauge that will sum the current value of a metric for all column families in this keyspace
     * @param name
     * @param extractor
     * @return Gauge&gt;Long> that computes sum of MetricValue.getValue()
     */
    private Gauge<Long> createKeyspaceGauge(String name, final MetricValue extractor)
    {
        allMetrics.add(name);
        return Metrics.register(factory.createMetricName(name), new Gauge<Long>()
        {
            public Long getValue()
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

    /**
     * Creates a counter that will sum the current value of a metric for all column families in this keyspace
     * @param name
     * @param extractor
     * @return Counter that computes sum of MetricValue.getValue()
     */
    private Counter createKeyspaceCounter(String name, final MetricValue extractor)
    {
        allMetrics.add(name);
        return Metrics.register(factory.createMetricName(name), new Counter()
        {
            @Override
            public long getCount()
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

    static class KeyspaceMetricNameFactory implements MetricNameFactory
    {
        private final String keyspaceName;

        KeyspaceMetricNameFactory(Keyspace ks)
        {
            this.keyspaceName = ks.getName();
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            String groupName = TableMetrics.class.getPackage().getName();

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=Keyspace");
            mbeanName.append(",keyspace=").append(keyspaceName);
            mbeanName.append(",name=").append(metricName);

            return new CassandraMetricsRegistry.MetricName(groupName, "keyspace", metricName, keyspaceName, mbeanName.toString());
        }
    }
}
