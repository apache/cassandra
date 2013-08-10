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


import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.metrics.CassandraMetricRegistry;
import org.apache.cassandra.io.sstable.SSTableMetadata;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.EstimatedHistogram;


/**
 * Metrics for {@link ColumnFamilyStore}.
 */
public class ColumnFamilyMetrics
{
    /** Total amount of data stored in the memtable, including column related overhead. */
    public final Gauge<Long> memtableDataSize;
    /** Total number of columns present in the memtable. */
    public final Gauge<Long> memtableColumnsCount;
    /** Number of times flush has resulted in the memtable being switched out. */
    public Counter memtableSwitchCount;
    /** Current compression ratio for all SSTables */
    public final Gauge<Double> compressionRatio;
    /** Histogram of estimated row size (in bytes). */
    public final Gauge<long[]> estimatedRowSizeHistogram;
    /** Histogram of estimated number of columns. */
    public final Gauge<long[]> estimatedColumnCountHistogram;
    /** Histogram of the number of sstable data files accessed per read */
    public final Histogram sstablesPerReadHistogram;
    /** Read metrics */
    public final LatencyMetrics readLatency;
    /** Write metrics */
    public final LatencyMetrics writeLatency;
    /** Estimated number of tasks pending for this column family */
    public final Gauge<Integer> pendingTasks;
    /** Number of SSTables on disk for this CF */
    public final Gauge<Integer> liveSSTableCount;
    /** Disk space used by SSTables belonging to this CF */
    public final Counter liveDiskSpaceUsed;
    /** Total disk space used by SSTables belonging to this CF, including obsolete ones waiting to be GC'd */
    public final Counter totalDiskSpaceUsed;
    /** Size of the smallest compacted row */
    public final Gauge<Long> minRowSize;
    /** Size of the largest compacted row */
    public final Gauge<Long> maxRowSize;
    /** Size of the smallest compacted row */
    public final Gauge<Long> meanRowSize;
    /** Number of false positives in bloom filter */
    public final Gauge<Long> bloomFilterFalsePositives;
    /** Number of false positives in bloom filter from last read */
    public final Gauge<Long> recentBloomFilterFalsePositives;
    /** False positive ratio of bloom filter */
    public final Gauge<Double> bloomFilterFalseRatio;
    /** False positive ratio of bloom filter from last read */
    public final Gauge<Double> recentBloomFilterFalseRatio;
    /** Disk space used by bloom filter */
    public final Gauge<Long> bloomFilterDiskSpaceUsed;
    /** Key cache hit rate  for this CF */
    public final Gauge<Double> keyCacheHitRate;

    private final MetricNameFactory factory;

    public final Counter speculativeRetry;

    // for backward compatibility
    @Deprecated public final EstimatedHistogram sstablesPerRead = new EstimatedHistogram(35);
    @Deprecated public final EstimatedHistogram recentSSTablesPerRead = new EstimatedHistogram(35);

    /**
     * Creates metrics for given {@link ColumnFamilyStore}.
     *
     * @param cfs ColumnFamilyStore to measure metrics
     */
    public ColumnFamilyMetrics(final ColumnFamilyStore cfs)
    {
        factory = new ColumnFamilyMetricNameFactory(cfs);

        memtableColumnsCount = CassandraMetricRegistry.register(factory.createMetricName("MemtableColumnsCount"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return cfs.getDataTracker().getMemtable().getOperations();
            }
        });
        memtableDataSize = CassandraMetricRegistry.register(factory.createMetricName("MemtableDataSize"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return cfs.getDataTracker().getMemtable().getLiveSize();
            }
        });
        memtableSwitchCount = CassandraMetricRegistry.get().counter(factory.createMetricName("MemtableSwitchCount"));
        estimatedRowSizeHistogram = CassandraMetricRegistry.register(factory.createMetricName("EstimatedRowSizeHistogram"), new Gauge<long[]>()
        {
            public long[] getValue()
            {
                long[] histogram = new long[90];
                for (SSTableReader sstable : cfs.getSSTables())
                {
                    long[] rowSize = sstable.getEstimatedRowSize().getBuckets(false);
                    for (int i = 0; i < histogram.length; i++)
                        histogram[i] += rowSize[i];
                }
                return histogram;
            }
        });
        estimatedColumnCountHistogram = CassandraMetricRegistry.register(factory.createMetricName("EstimatedColumnCountHistogram"), new Gauge<long[]>()
        {
            public long[] getValue()
            {
                long[] histogram = new long[90];
                for (SSTableReader sstable : cfs.getSSTables())
                {
                    long[] columnSize = sstable.getEstimatedColumnCount().getBuckets(false);
                    for (int i = 0; i < histogram.length; i++)
                        histogram[i] += columnSize[i];
                }
                return histogram;
            }
        });
        sstablesPerReadHistogram = CassandraMetricRegistry.get().histogram(factory.createMetricName("SSTablesPerReadHistogram"));
        compressionRatio = CassandraMetricRegistry.register(factory.createMetricName("CompressionRatio"), new Gauge<Double>()
        {
            public Double getValue()
            {
                double sum = 0;
                int total = 0;
                for (SSTableReader sstable : cfs.getSSTables())
                {
                    if (sstable.getCompressionRatio() != SSTableMetadata.NO_COMPRESSION_RATIO)
                    {
                        sum += sstable.getCompressionRatio();
                        total++;
                    }
                }
                return total != 0 ? (double)sum/total: 0;
            }
        });
        readLatency = new LatencyMetrics(factory, "Read");
        writeLatency = new LatencyMetrics(factory, "Write");
        pendingTasks = CassandraMetricRegistry.register(factory.createMetricName("PendingTasks"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                // TODO this actually isn't a good measure of pending tasks
                return Keyspace.switchLock.getQueueLength();
            }
        });
        liveSSTableCount = CassandraMetricRegistry.register(factory.createMetricName("LiveSSTableCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return cfs.getDataTracker().getSSTables().size();
            }
        });
        liveDiskSpaceUsed = CassandraMetricRegistry.get().counter(factory.createMetricName("LiveDiskSpaceUsed"));
        totalDiskSpaceUsed = CassandraMetricRegistry.get().counter(factory.createMetricName("TotalDiskSpaceUsed"));
        minRowSize = CassandraMetricRegistry.register(factory.createMetricName("MinRowSize"), new Gauge<Long>()
        {
            public Long getValue()
            {
                long min = 0;
                for (SSTableReader sstable : cfs.getSSTables())
                {
                    if (min == 0 || sstable.getEstimatedRowSize().min() < min)
                        min = sstable.getEstimatedRowSize().min();
                }
                return min;
            }
        });
        maxRowSize = CassandraMetricRegistry.register(factory.createMetricName("MaxRowSize"), new Gauge<Long>()
        {
            public Long getValue()
            {
                long max = 0;
                for (SSTableReader sstable : cfs.getSSTables())
                {
                    if (sstable.getEstimatedRowSize().max() > max)
                        max = sstable.getEstimatedRowSize().max();
                }
                return max;
            }
        });
        meanRowSize = CassandraMetricRegistry.register(factory.createMetricName("MeanRowSize"), new Gauge<Long>()
        {
            public Long getValue()
            {
                long sum = 0;
                long count = 0;
                for (SSTableReader sstable : cfs.getSSTables())
                {
                    sum += sstable.getEstimatedRowSize().mean();
                    count++;
                }
                return count > 0 ? sum / count : 0;
            }
        });
        bloomFilterFalsePositives = CassandraMetricRegistry.register(factory.createMetricName("BloomFilterFalsePositives"), new Gauge<Long>()
        {
            public Long getValue()
            {
                long count = 0L;
                for (SSTableReader sstable: cfs.getSSTables())
                    count += sstable.getBloomFilterFalsePositiveCount();
                return count;
            }
        });
        recentBloomFilterFalsePositives = CassandraMetricRegistry.register(factory.createMetricName("RecentBloomFilterFalsePositives"), new Gauge<Long>()
        {
            public Long getValue()
            {
                long count = 0L;
                for (SSTableReader sstable: cfs.getSSTables())
                    count += sstable.getRecentBloomFilterFalsePositiveCount();
                return count;
            }
        });
        bloomFilterFalseRatio = CassandraMetricRegistry.register(factory.createMetricName("BloomFilterFalseRatio"), new Gauge<Double>()
        {
            public Double getValue()
            {
                long falseCount = 0L;
                long trueCount = 0L;
                for (SSTableReader sstable: cfs.getSSTables())
                {
                    falseCount += sstable.getBloomFilterFalsePositiveCount();
                    trueCount += sstable.getBloomFilterTruePositiveCount();
                }
                if (falseCount == 0L && trueCount == 0L)
                    return 0d;
                return (double) falseCount / (trueCount + falseCount);
            }
        });
        recentBloomFilterFalseRatio = CassandraMetricRegistry.register(factory.createMetricName("RecentBloomFilterFalseRatio"), new Gauge<Double>()
        {
            public Double getValue()
            {
                long falseCount = 0L;
                long trueCount = 0L;
                for (SSTableReader sstable: cfs.getSSTables())
                {
                    falseCount += sstable.getRecentBloomFilterFalsePositiveCount();
                    trueCount += sstable.getRecentBloomFilterTruePositiveCount();
                }
                if (falseCount == 0L && trueCount == 0L)
                    return 0d;
                return (double) falseCount / (trueCount + falseCount);
            }
        });
        bloomFilterDiskSpaceUsed = CassandraMetricRegistry.register(factory.createMetricName("BloomFilterDiskSpaceUsed"), new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
                for (SSTableReader sst : cfs.getSSTables())
                    total += sst.getBloomFilterSerializedSize();
                return total;
            }
        });

        speculativeRetry = CassandraMetricRegistry.get().counter(factory.createMetricName("SpeculativeRetry"));
        keyCacheHitRate = CassandraMetricRegistry.register(factory.createMetricName("KeyCacheHitRate"), new RatioGauge()
        {
            protected double getNumerator()
            {
                long hits = 0L;
                for (SSTableReader sstable : cfs.getSSTables())
                    hits += sstable.getKeyCacheHit();
                return hits;
            }

            protected double getDenominator()
            {
                long requests = 0L;
                for (SSTableReader sstable : cfs.getSSTables())
                    requests += sstable.getKeyCacheRequest();
                return Math.max(requests, 1); // to avoid NaN.
            }

            @Override
            protected Ratio getRatio() 
            {
                return Ratio.of(getNumerator(), getDenominator());
            }
        });
    }
    
    public void resetMemTableSwitchCount()
    {
        memtableSwitchCount = CassandraMetricRegistry.get().counter(factory.createMetricName("MemtableSwitchCount"));
    }

    public void updateSSTableIterated(int count)
    {
        sstablesPerReadHistogram.update(count);
        recentSSTablesPerRead.add(count);
        sstablesPerRead.add(count);
    }

    /**
     * Release all associated metrics.
     */
    public void release()
    {
        readLatency.release();
        writeLatency.release();
        CassandraMetricRegistry.unregister(factory.createMetricName("MemtableColumnsCount"));
        CassandraMetricRegistry.unregister(factory.createMetricName("MemtableDataSize"));
        CassandraMetricRegistry.unregister(factory.createMetricName("MemtableSwitchCount"));
        CassandraMetricRegistry.unregister(factory.createMetricName("CompressionRatio"));
        CassandraMetricRegistry.unregister(factory.createMetricName("EstimatedRowSizeHistogram"));
        CassandraMetricRegistry.unregister(factory.createMetricName("EstimatedColumnCountHistogram"));
        CassandraMetricRegistry.unregister(factory.createMetricName("SSTablesPerReadHistogram"));
        CassandraMetricRegistry.unregister(factory.createMetricName("PendingTasks"));
        CassandraMetricRegistry.unregister(factory.createMetricName("LiveSSTableCount"));
        CassandraMetricRegistry.unregister(factory.createMetricName("LiveDiskSpaceUsed"));
        CassandraMetricRegistry.unregister(factory.createMetricName("TotalDiskSpaceUsed"));
        CassandraMetricRegistry.unregister(factory.createMetricName("MinRowSize"));
        CassandraMetricRegistry.unregister(factory.createMetricName("MaxRowSize"));
        CassandraMetricRegistry.unregister(factory.createMetricName("MeanRowSize"));
        CassandraMetricRegistry.unregister(factory.createMetricName("BloomFilterFalsePositives"));
        CassandraMetricRegistry.unregister(factory.createMetricName("RecentBloomFilterFalsePositives"));
        CassandraMetricRegistry.unregister(factory.createMetricName("BloomFilterFalseRatio"));
        CassandraMetricRegistry.unregister(factory.createMetricName("RecentBloomFilterFalseRatio"));
        CassandraMetricRegistry.unregister(factory.createMetricName("BloomFilterDiskSpaceUsed"));
    }

    class ColumnFamilyMetricNameFactory implements MetricNameFactory
    {
        private final String keyspaceName;
        private final String columnFamilyName;
        private final boolean isIndex;

        ColumnFamilyMetricNameFactory(ColumnFamilyStore cfs)
        {
            this.keyspaceName = cfs.keyspace.getName();
            this.columnFamilyName = cfs.name;
            isIndex = cfs.isIndex();
        }

        public String createMetricName(String metricName)
        {
            String groupName = ColumnFamilyMetrics.class.getPackage().getName();
            String type = isIndex ? "IndexColumnFamily" : "ColumnFamily";

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=").append(type);
            mbeanName.append(",keyspace=").append(keyspaceName);
            mbeanName.append(",scope=").append(columnFamilyName);
            mbeanName.append(",name=").append(metricName);

            return MetricRegistry.name(groupName, type, metricName, keyspaceName + "." + columnFamilyName, mbeanName.toString());
        }
    }
}
