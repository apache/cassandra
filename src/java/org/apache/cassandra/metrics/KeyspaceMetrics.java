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
import java.util.function.ToLongFunction;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.GaugeProvider;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;
import org.apache.cassandra.metrics.TableMetrics.ReleasableMetric;

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
    /** Disk space used by SSTables belonging to tables in this keyspace */
    public final Gauge<Long> liveDiskSpaceUsed;
    /** Disk space used by SSTables belonging to tables in this keyspace, scaled down by replication factor */
    public final Gauge<Long> unreplicatedLiveDiskSpaceUsed;
    /** Uncompressed/logical size of SSTables belonging to tables in this keyspace */
    public final Gauge<Long> uncompressedLiveDiskSpaceUsed;
    /** Uncompressed/logical size of SSTables belonging to tables in this keyspace, scaled down by replication factor */
    public final Gauge<Long> unreplicatedUncompressedLiveDiskSpaceUsed;
    public final Gauge<Long> totalDiskSpaceUsed;
    /** Off heap memory used by compression meta data*/
    public final Gauge<Long> compressionMetadataOffHeapMemoryUsed;
    /** (Local) read metrics */
    public final LatencyMetrics readLatency;
    /** (Local) range slice metrics */
    public final LatencyMetrics rangeLatency;
    /** (Local) write metrics */
    public final LatencyMetrics writeLatency;
    /** Histogram of the number of sstable data files accessed per single partition read */
    public final Histogram sstablesPerReadHistogram;
    /** Histogram of the number of sstable data files accessed per partition range read */
    public final Histogram sstablesPerRangeReadHistogram;
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

    /**
     * Tracks the amount overreading of repaired data replicas perform in order to produce digests
     * at query time. For each query, on a full data read following an initial digest mismatch, the replicas
     * may read extra repaired data, up to the DataLimit of the command, so that the coordinator can compare
     * the repaired data on each replica. These are tracked on each replica.
     */
    public final Histogram repairedDataTrackingOverreadRows;
    public final Timer repairedDataTrackingOverreadTime;

    public final Meter clientTombstoneWarnings;
    public final Meter clientTombstoneAborts;

    public final Meter coordinatorReadSizeWarnings;
    public final Meter coordinatorReadSizeAborts;
    public final Histogram coordinatorReadSize;

    public final Meter localReadSizeWarnings;
    public final Meter localReadSizeAborts;
    public final Histogram localReadSize;

    public final Meter rowIndexSizeWarnings;
    public final Meter rowIndexSizeAborts;
    public final Histogram rowIndexSize;

    public final Meter tooManySSTableIndexesReadWarnings;
    public final Meter tooManySSTableIndexesReadAborts;

    public final ImmutableMap<SSTableFormat<?, ?>, ImmutableMap<String, Gauge<? extends Number>>> formatSpecificGauges;

    public final MetricNameFactory factory;
    private final Keyspace keyspace;

    /** set containing names of all the metrics stored here, for releasing later */
    private Set<ReleasableMetric> allMetrics = Sets.newHashSet();

    /**
     * Creates metrics for given {@link ColumnFamilyStore}.
     *
     * @param ks Keyspace to measure metrics
     */
    public KeyspaceMetrics(final Keyspace ks)
    {
        factory = new KeyspaceMetricNameFactory(ks);
        keyspace = ks;
        memtableColumnsCount = createKeyspaceGauge("MemtableColumnsCount",
                metric -> metric.memtableColumnsCount.getValue());
        memtableLiveDataSize = createKeyspaceGauge("MemtableLiveDataSize",
                metric -> metric.memtableLiveDataSize.getValue());
        memtableOnHeapDataSize = createKeyspaceGauge("MemtableOnHeapDataSize",
                metric -> metric.memtableOnHeapDataSize.getValue());
        memtableOffHeapDataSize = createKeyspaceGauge("MemtableOffHeapDataSize",
                metric -> metric.memtableOffHeapDataSize.getValue());
        allMemtablesLiveDataSize = createKeyspaceGauge("AllMemtablesLiveDataSize",
                metric -> metric.allMemtablesLiveDataSize.getValue());
        allMemtablesOnHeapDataSize = createKeyspaceGauge("AllMemtablesOnHeapDataSize",
                metric -> metric.allMemtablesOnHeapDataSize.getValue());
        allMemtablesOffHeapDataSize = createKeyspaceGauge("AllMemtablesOffHeapDataSize",
                metric -> metric.allMemtablesOffHeapDataSize.getValue());
        memtableSwitchCount = createKeyspaceGauge("MemtableSwitchCount",
                metric -> metric.memtableSwitchCount.getCount());
        pendingCompactions = createKeyspaceGauge("PendingCompactions", metric -> metric.pendingCompactions.getValue());
        pendingFlushes = createKeyspaceGauge("PendingFlushes", metric -> metric.pendingFlushes.getCount());

        liveDiskSpaceUsed = createKeyspaceGauge("LiveDiskSpaceUsed", metric -> metric.liveDiskSpaceUsed.getCount());
        uncompressedLiveDiskSpaceUsed = createKeyspaceGauge("UncompressedLiveDiskSpaceUsed", metric -> metric.uncompressedLiveDiskSpaceUsed.getCount());
        unreplicatedLiveDiskSpaceUsed = createKeyspaceGauge("UnreplicatedLiveDiskSpaceUsed",
                                                            metric -> metric.liveDiskSpaceUsed.getCount() / keyspace.getReplicationStrategy().getReplicationFactor().fullReplicas);
        unreplicatedUncompressedLiveDiskSpaceUsed = createKeyspaceGauge("UnreplicatedUncompressedLiveDiskSpaceUsed",
                                                                        metric -> metric.uncompressedLiveDiskSpaceUsed.getCount() / keyspace.getReplicationStrategy().getReplicationFactor().fullReplicas);
        totalDiskSpaceUsed = createKeyspaceGauge("TotalDiskSpaceUsed", metric -> metric.totalDiskSpaceUsed.getCount());

        compressionMetadataOffHeapMemoryUsed = createKeyspaceGauge("CompressionMetadataOffHeapMemoryUsed",
                metric -> metric.compressionMetadataOffHeapMemoryUsed.getValue());

        // latency metrics for TableMetrics to update
        readLatency = createLatencyMetrics("Read");
        writeLatency = createLatencyMetrics("Write");
        rangeLatency = createLatencyMetrics("Range");

        // create histograms for TableMetrics to replicate updates to
        sstablesPerReadHistogram = createKeyspaceHistogram("SSTablesPerReadHistogram", true);
        sstablesPerRangeReadHistogram = createKeyspaceHistogram("SSTablesPerRangeReadHistogram", true);
        tombstoneScannedHistogram = createKeyspaceHistogram("TombstoneScannedHistogram", false);
        liveScannedHistogram = createKeyspaceHistogram("LiveScannedHistogram", false);
        colUpdateTimeDeltaHistogram = createKeyspaceHistogram("ColUpdateTimeDeltaHistogram", false);
        viewLockAcquireTime = createKeyspaceTimer("ViewLockAcquireTime");
        viewReadTime = createKeyspaceTimer("ViewReadTime");

        casPrepare = createLatencyMetrics("CasPrepare");
        casPropose = createLatencyMetrics("CasPropose");
        casCommit = createLatencyMetrics("CasCommit");
        writeFailedIdealCL = createKeyspaceCounter("WriteFailedIdealCL");
        idealCLWriteLatency = createLatencyMetrics("IdealCLWrite");

        speculativeRetries = createKeyspaceCounter("SpeculativeRetries", metric -> metric.speculativeRetries.getCount());
        speculativeFailedRetries = createKeyspaceCounter("SpeculativeFailedRetries", metric -> metric.speculativeFailedRetries.getCount());
        speculativeInsufficientReplicas = createKeyspaceCounter("SpeculativeInsufficientReplicas", metric -> metric.speculativeInsufficientReplicas.getCount());
        additionalWrites = createKeyspaceCounter("AdditionalWrites", metric -> metric.additionalWrites.getCount());
        repairsStarted = createKeyspaceCounter("RepairJobsStarted", metric -> metric.repairsStarted.getCount());
        repairsCompleted = createKeyspaceCounter("RepairJobsCompleted", metric -> metric.repairsCompleted.getCount());
        repairTime =createKeyspaceTimer("RepairTime");
        repairPrepareTime = createKeyspaceTimer("RepairPrepareTime");
        anticompactionTime = createKeyspaceTimer("AntiCompactionTime");
        validationTime = createKeyspaceTimer("ValidationTime");
        repairSyncTime = createKeyspaceTimer("RepairSyncTime");
        partitionsValidated = createKeyspaceHistogram("PartitionsValidated", false);
        bytesValidated = createKeyspaceHistogram("BytesValidated", false);

        confirmedRepairedInconsistencies = createKeyspaceMeter("RepairedDataInconsistenciesConfirmed");
        unconfirmedRepairedInconsistencies = createKeyspaceMeter("RepairedDataInconsistenciesUnconfirmed");

        repairedDataTrackingOverreadRows = createKeyspaceHistogram("RepairedDataTrackingOverreadRows", false);
        repairedDataTrackingOverreadTime = createKeyspaceTimer("RepairedDataTrackingOverreadTime");

        clientTombstoneWarnings = createKeyspaceMeter("ClientTombstoneWarnings");
        clientTombstoneAborts = createKeyspaceMeter("ClientTombstoneAborts");

        coordinatorReadSizeWarnings = createKeyspaceMeter("CoordinatorReadSizeWarnings");
        coordinatorReadSizeAborts = createKeyspaceMeter("CoordinatorReadSizeAborts");
        coordinatorReadSize = createKeyspaceHistogram("CoordinatorReadSize", false);

        localReadSizeWarnings = createKeyspaceMeter("LocalReadSizeWarnings");
        localReadSizeAborts = createKeyspaceMeter("LocalReadSizeAborts");
        localReadSize = createKeyspaceHistogram("LocalReadSize", false);

        rowIndexSizeWarnings = createKeyspaceMeter("RowIndexSizeWarnings");
        rowIndexSizeAborts = createKeyspaceMeter("RowIndexSizeAborts");
        rowIndexSize = createKeyspaceHistogram("RowIndexSize", false);

        tooManySSTableIndexesReadWarnings = createKeyspaceMeter("TooManySSTableIndexesReadWarnings");
        tooManySSTableIndexesReadAborts = createKeyspaceMeter("TooManySSTableIndexesReadAborts");

        formatSpecificGauges = createFormatSpecificGauges(keyspace);
    }

    /**
     * Release all associated metrics.
     */
    public void release()
    {
        for (ReleasableMetric metric : allMetrics)
        {
            metric.release();
        }
    }

    private ImmutableMap<SSTableFormat<?, ?>, ImmutableMap<String, Gauge<? extends Number>>> createFormatSpecificGauges(Keyspace keyspace)
    {
        ImmutableMap.Builder<SSTableFormat<? ,?>, ImmutableMap<String, Gauge<? extends Number>>> builder = ImmutableMap.builder();
        for (SSTableFormat<?, ?> format : DatabaseDescriptor.getSSTableFormats().values())
        {
            ImmutableMap.Builder<String, Gauge<? extends Number>> gauges = ImmutableMap.builder();
            for (GaugeProvider<?> gaugeProvider : format.getFormatSpecificMetricsProviders().getGaugeProviders())
            {
                String finalName = gaugeProvider.name;
                allMetrics.add(() -> releaseMetric(finalName));
                Gauge<? extends Number> gauge = Metrics.register(factory.createMetricName(finalName), gaugeProvider.getKeyspaceGauge(keyspace));
                gauges.put(gaugeProvider.name, gauge);
            }
            builder.put(format, gauges.build());
        }
        return builder.build();
    }

    /**
     * Creates a gauge that will sum the current value of a metric for all column families in this keyspace
     *
     * @param name the name of the metric being created
     * @param extractor a function that produces a specified metric value for a given table
     *
     * @return Gauge&gt;Long> that computes sum of MetricValue.getValue()
     */
    private Gauge<Long> createKeyspaceGauge(String name, final ToLongFunction<TableMetrics> extractor)
    {
        allMetrics.add(() -> releaseMetric(name));
        return Metrics.register(factory.createMetricName(name), new Gauge<Long>()
        {
            public Long getValue()
            {
                long sum = 0;
                for (ColumnFamilyStore cf : keyspace.getColumnFamilyStores())
                {
                    sum += extractor.applyAsLong(cf.metric);
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
    private Counter createKeyspaceCounter(String name, final ToLongFunction<TableMetrics> extractor)
    {
        allMetrics.add(() -> releaseMetric(name));
        return Metrics.register(factory.createMetricName(name), new Counter()
        {
            @Override
            public long getCount()
            {
                long sum = 0;
                for (ColumnFamilyStore cf : keyspace.getColumnFamilyStores())
                {
                    sum += extractor.applyAsLong(cf.metric);
                }
                return sum;
            }
        });
    }

    protected Counter createKeyspaceCounter(String name)
    {
        allMetrics.add(() -> releaseMetric(name));
        return Metrics.counter(factory.createMetricName(name));
    }

    protected Histogram createKeyspaceHistogram(String name, boolean considerZeroes)
    {
        allMetrics.add(() -> releaseMetric(name));
        return Metrics.histogram(factory.createMetricName(name), considerZeroes);
    }

    protected Timer createKeyspaceTimer(String name)
    {
        allMetrics.add(() -> releaseMetric(name));
        return Metrics.timer(factory.createMetricName(name));
    }

    protected Meter createKeyspaceMeter(String name)
    {
        allMetrics.add(() -> releaseMetric(name));
        return Metrics.meter(factory.createMetricName(name));
    }

    private LatencyMetrics createLatencyMetrics(String name)
    {
        LatencyMetrics metric = new LatencyMetrics(factory, name);
        allMetrics.add(() -> metric.release());
        return metric;
    }

    private void releaseMetric(String name)
    {
        Metrics.remove(factory.createMetricName(name));
    }

    static class KeyspaceMetricNameFactory implements MetricNameFactory
    {
        private final String keyspaceName;

        KeyspaceMetricNameFactory(Keyspace ks)
        {
            this.keyspaceName = ks.getName();
        }

        @Override
        public MetricName createMetricName(String metricName)
        {
            String groupName = TableMetrics.class.getPackage().getName();

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=Keyspace");
            mbeanName.append(",keyspace=").append(keyspaceName);
            mbeanName.append(",name=").append(metricName);

            return new MetricName(groupName, "keyspace", metricName, keyspaceName, mbeanName.toString());
        }
    }
}
