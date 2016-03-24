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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.TopKSampler;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for {@link ColumnFamilyStore}.
 */
public class TableMetrics
{

    public static final long[] EMPTY = new long[0];

    /** Total amount of data stored in the memtable that resides on-heap, including column related overhead and partitions overwritten. */
    public final Gauge<Long> memtableOnHeapSize;
    /** Total amount of data stored in the memtable that resides off-heap, including column related overhead and partitions overwritten. */
    public final Gauge<Long> memtableOffHeapSize;
    /** Total amount of live data stored in the memtable, excluding any data structure overhead */
    public final Gauge<Long> memtableLiveDataSize;
    /** Total amount of data stored in the memtables (2i and pending flush memtables included) that resides on-heap. */
    public final Gauge<Long> allMemtablesOnHeapSize;
    /** Total amount of data stored in the memtables (2i and pending flush memtables included) that resides off-heap. */
    public final Gauge<Long> allMemtablesOffHeapSize;
    /** Total amount of live data stored in the memtables (2i and pending flush memtables included) that resides off-heap, excluding any data structure overhead */
    public final Gauge<Long> allMemtablesLiveDataSize;
    /** Total number of columns present in the memtable. */
    public final Gauge<Long> memtableColumnsCount;
    /** Number of times flush has resulted in the memtable being switched out. */
    public final Counter memtableSwitchCount;
    /** Current compression ratio for all SSTables */
    public final Gauge<Double> compressionRatio;
    /** Histogram of estimated partition size (in bytes). */
    public final Gauge<long[]> estimatedPartitionSizeHistogram;
    /** Approximate number of keys in table. */
    public final Gauge<Long> estimatedPartitionCount;
    /** Histogram of estimated number of columns. */
    public final Gauge<long[]> estimatedColumnCountHistogram;
    /** Histogram of the number of sstable data files accessed per read */
    public final TableHistogram sstablesPerReadHistogram;
    /** (Local) read metrics */
    public final LatencyMetrics readLatency;
    /** (Local) range slice metrics */
    public final LatencyMetrics rangeLatency;
    /** (Local) write metrics */
    public final LatencyMetrics writeLatency;
    /** Estimated number of tasks pending for this table */
    public final Counter pendingFlushes;
    /** Total number of bytes flushed since server [re]start */
    public final Counter bytesFlushed;
    /** Total number of bytes written by compaction since server [re]start */
    public final Counter compactionBytesWritten;
    /** Estimate of number of pending compactios for this table */
    public final Gauge<Integer> pendingCompactions;
    /** Number of SSTables on disk for this CF */
    public final Gauge<Integer> liveSSTableCount;
    /** Disk space used by SSTables belonging to this table */
    public final Counter liveDiskSpaceUsed;
    /** Total disk space used by SSTables belonging to this table, including obsolete ones waiting to be GC'd */
    public final Counter totalDiskSpaceUsed;
    /** Size of the smallest compacted partition */
    public final Gauge<Long> minPartitionSize;
    /** Size of the largest compacted partition */
    public final Gauge<Long> maxPartitionSize;
    /** Size of the smallest compacted partition */
    public final Gauge<Long> meanPartitionSize;
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
    /** Off heap memory used by bloom filter */
    public final Gauge<Long> bloomFilterOffHeapMemoryUsed;
    /** Off heap memory used by index summary */
    public final Gauge<Long> indexSummaryOffHeapMemoryUsed;
    /** Off heap memory used by compression meta data*/
    public final Gauge<Long> compressionMetadataOffHeapMemoryUsed;
    /** Key cache hit rate  for this CF */
    public final Gauge<Double> keyCacheHitRate;
    /** Tombstones scanned in queries on this CF */
    public final TableHistogram tombstoneScannedHistogram;
    /** Live cells scanned in queries on this CF */
    public final TableHistogram liveScannedHistogram;
    /** Column update time delta on this CF */
    public final TableHistogram colUpdateTimeDeltaHistogram;
    /** time taken acquiring the partition lock for materialized view updates for this table */
    public final TableTimer viewLockAcquireTime;
    /** time taken during the local read of a materialized view update */
    public final TableTimer viewReadTime;
    /** Disk space used by snapshot files which */
    public final Gauge<Long> trueSnapshotsSize;
    /** Row cache hits, but result out of range */
    public final Counter rowCacheHitOutOfRange;
    /** Number of row cache hits */
    public final Counter rowCacheHit;
    /** Number of row cache misses */
    public final Counter rowCacheMiss;
    /** CAS Prepare metrics */
    public final LatencyMetrics casPrepare;
    /** CAS Propose metrics */
    public final LatencyMetrics casPropose;
    /** CAS Commit metrics */
    public final LatencyMetrics casCommit;

    public final Timer coordinatorReadLatency;
    public final Timer coordinatorScanLatency;

    /** Time spent waiting for free memtable space, either on- or off-heap */
    public final Histogram waitingOnFreeMemtableSpace;

    /** Dropped Mutations Count */
    public final Counter droppedMutations;

    private final MetricNameFactory factory;
    private final MetricNameFactory aliasFactory;
    private static final MetricNameFactory globalFactory = new AllTableMetricNameFactory("Table");
    private static final MetricNameFactory globalAliasFactory = new AllTableMetricNameFactory("ColumnFamily");

    public final Counter speculativeRetries;

    public final static LatencyMetrics globalReadLatency = new LatencyMetrics(globalFactory, globalAliasFactory, "Read");
    public final static LatencyMetrics globalWriteLatency = new LatencyMetrics(globalFactory, globalAliasFactory, "Write");
    public final static LatencyMetrics globalRangeLatency = new LatencyMetrics(globalFactory, globalAliasFactory, "Range");

    public final Map<Sampler, TopKSampler<ByteBuffer>> samplers;
    /**
     * stores metrics that will be rolled into a single global metric
     */
    public final static ConcurrentMap<String, Set<Metric>> allTableMetrics = Maps.newConcurrentMap();

    /**
     * Stores all metric names created that can be used when unregistering, optionally mapped to an alias name.
     */
    public final static Map<String, String> all = Maps.newHashMap();

    private interface GetHistogram
    {
        EstimatedHistogram getHistogram(SSTableReader reader);
    }

    private static long[] combineHistograms(Iterable<SSTableReader> sstables, GetHistogram getHistogram)
    {
        Iterator<SSTableReader> iterator = sstables.iterator();
        if (!iterator.hasNext())
        {
            return EMPTY;
        }
        long[] firstBucket = getHistogram.getHistogram(iterator.next()).getBuckets(false);
        long[] values = new long[firstBucket.length];
        System.arraycopy(firstBucket, 0, values, 0, values.length);

        while (iterator.hasNext())
        {
            long[] nextBucket = getHistogram.getHistogram(iterator.next()).getBuckets(false);
            if (nextBucket.length > values.length)
            {
                long[] newValues = new long[nextBucket.length];
                System.arraycopy(firstBucket, 0, newValues, 0, firstBucket.length);
                for (int i = 0; i < newValues.length; i++)
                {
                    newValues[i] += nextBucket[i];
                }
                values = newValues;
            }
            else
            {
                for (int i = 0; i < values.length; i++)
                {
                    values[i] += nextBucket[i];
                }
            }
        }
        return values;
    }

    /**
     * Creates metrics for given {@link ColumnFamilyStore}.
     *
     * @param cfs ColumnFamilyStore to measure metrics
     */
    public TableMetrics(final ColumnFamilyStore cfs)
    {
        factory = new TableMetricNameFactory(cfs, "Table");
        aliasFactory = new TableMetricNameFactory(cfs, "ColumnFamily");

        samplers = Maps.newHashMap();
        for (Sampler sampler : Sampler.values())
        {
            samplers.put(sampler, new TopKSampler<>());
        }

        memtableColumnsCount = createTableGauge("MemtableColumnsCount", new Gauge<Long>()
        {
            public Long getValue()
            {
                return cfs.getTracker().getView().getCurrentMemtable().getOperations();
            }
        });
        memtableOnHeapSize = createTableGauge("MemtableOnHeapSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                return cfs.getTracker().getView().getCurrentMemtable().getAllocator().onHeap().owns();
            }
        });
        memtableOffHeapSize = createTableGauge("MemtableOffHeapSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                return cfs.getTracker().getView().getCurrentMemtable().getAllocator().offHeap().owns();
            }
        });
        memtableLiveDataSize = createTableGauge("MemtableLiveDataSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                return cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize();
            }
        });
        allMemtablesOnHeapSize = createTableGauge("AllMemtablesHeapSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (ColumnFamilyStore cfs2 : cfs.concatWithIndexes())
                    size += cfs2.getTracker().getView().getCurrentMemtable().getAllocator().onHeap().owns();
                return size;
            }
        });
        allMemtablesOffHeapSize = createTableGauge("AllMemtablesOffHeapSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (ColumnFamilyStore cfs2 : cfs.concatWithIndexes())
                    size += cfs2.getTracker().getView().getCurrentMemtable().getAllocator().offHeap().owns();
                return size;
            }
        });
        allMemtablesLiveDataSize = createTableGauge("AllMemtablesLiveDataSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (ColumnFamilyStore cfs2 : cfs.concatWithIndexes())
                    size += cfs2.getTracker().getView().getCurrentMemtable().getLiveDataSize();
                return size;
            }
        });
        memtableSwitchCount = createTableCounter("MemtableSwitchCount");
        estimatedPartitionSizeHistogram = Metrics.register(factory.createMetricName("EstimatedPartitionSizeHistogram"),
                                                           aliasFactory.createMetricName("EstimatedRowSizeHistogram"),
                                                           new Gauge<long[]>()
                                                           {
                                                               public long[] getValue()
                                                               {
                                                                   return combineHistograms(cfs.getSSTables(SSTableSet.CANONICAL), new GetHistogram()
                                                                   {
                                                                       public EstimatedHistogram getHistogram(SSTableReader reader)
                                                                       {
                                                                           return reader.getEstimatedPartitionSize();
                                                                       }
                                                                   });
                                                               }
                                                           });
        estimatedPartitionCount = Metrics.register(factory.createMetricName("EstimatedPartitionCount"),
                                                   aliasFactory.createMetricName("EstimatedRowCount"),
                                                   new Gauge<Long>()
                                                   {
                                                       public Long getValue()
                                                       {
                                                           long memtablePartitions = 0;
                                                           for (Memtable memtable : cfs.getTracker().getView().getAllMemtables())
                                                               memtablePartitions += memtable.partitionCount();
                                                           return SSTableReader.getApproximateKeyCount(cfs.getSSTables(SSTableSet.CANONICAL)) + memtablePartitions;
                                                       }
                                                   });
        estimatedColumnCountHistogram = Metrics.register(factory.createMetricName("EstimatedColumnCountHistogram"),
                                                         aliasFactory.createMetricName("EstimatedColumnCountHistogram"),
                                                         new Gauge<long[]>()
                                                         {
                                                             public long[] getValue()
                                                             {
                                                                 return combineHistograms(cfs.getSSTables(SSTableSet.CANONICAL), new GetHistogram()
                                                                 {
                                                                     public EstimatedHistogram getHistogram(SSTableReader reader)
                                                                     {
                                                                         return reader.getEstimatedColumnCount();
                                                                     }
                                                                 });
            }
        });
        sstablesPerReadHistogram = createTableHistogram("SSTablesPerReadHistogram", cfs.keyspace.metric.sstablesPerReadHistogram, true);
        compressionRatio = createTableGauge("CompressionRatio", new Gauge<Double>()
        {
            public Double getValue()
            {
                return computeCompressionRatio(cfs.getSSTables(SSTableSet.CANONICAL));
            }
        }, new Gauge<Double>() // global gauge
        {
            public Double getValue()
            {
                return computeCompressionRatio(Iterables.concat(Iterables.transform(Keyspace.all(),
                                                                                    p -> p.getAllSSTables(SSTableSet.CANONICAL))));
            }
        });
        readLatency = new LatencyMetrics(factory, "Read", cfs.keyspace.metric.readLatency, globalReadLatency);
        writeLatency = new LatencyMetrics(factory, "Write", cfs.keyspace.metric.writeLatency, globalWriteLatency);
        rangeLatency = new LatencyMetrics(factory, "Range", cfs.keyspace.metric.rangeLatency, globalRangeLatency);
        pendingFlushes = createTableCounter("PendingFlushes");
        bytesFlushed = createTableCounter("BytesFlushed");
        compactionBytesWritten = createTableCounter("CompactionBytesWritten");
        pendingCompactions = createTableGauge("PendingCompactions", new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return cfs.getCompactionStrategyManager().getEstimatedRemainingTasks();
            }
        });
        liveSSTableCount = createTableGauge("LiveSSTableCount", new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return cfs.getTracker().getView().liveSSTables().size();
            }
        });
        liveDiskSpaceUsed = createTableCounter("LiveDiskSpaceUsed");
        totalDiskSpaceUsed = createTableCounter("TotalDiskSpaceUsed");
        minPartitionSize = createTableGauge("MinPartitionSize", "MinRowSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long min = 0;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
                {
                    if (min == 0 || sstable.getEstimatedPartitionSize().min() < min)
                        min = sstable.getEstimatedPartitionSize().min();
                }
                return min;
            }
        }, new Gauge<Long>() // global gauge
        {
            public Long getValue()
            {
                long min = Long.MAX_VALUE;
                for (Metric cfGauge : allTableMetrics.get("MinPartitionSize"))
                {
                    min = Math.min(min, ((Gauge<? extends Number>) cfGauge).getValue().longValue());
                }
                return min;
            }
        });
        maxPartitionSize = createTableGauge("MaxPartitionSize", "MaxRowSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long max = 0;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
                {
                    if (sstable.getEstimatedPartitionSize().max() > max)
                        max = sstable.getEstimatedPartitionSize().max();
                }
                return max;
            }
        }, new Gauge<Long>() // global gauge
        {
            public Long getValue()
            {
                long max = 0;
                for (Metric cfGauge : allTableMetrics.get("MaxPartitionSize"))
                {
                    max = Math.max(max, ((Gauge<? extends Number>) cfGauge).getValue().longValue());
                }
                return max;
            }
        });
        meanPartitionSize = createTableGauge("MeanPartitionSize", "MeanRowSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long sum = 0;
                long count = 0;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
                {
                    long n = sstable.getEstimatedPartitionSize().count();
                    sum += sstable.getEstimatedPartitionSize().mean() * n;
                    count += n;
                }
                return count > 0 ? sum / count : 0;
            }
        }, new Gauge<Long>() // global gauge
        {
            public Long getValue()
            {
                long sum = 0;
                long count = 0;
                for (Keyspace keyspace : Keyspace.all())
                {
                    for (SSTableReader sstable : keyspace.getAllSSTables(SSTableSet.CANONICAL))
                    {
                        long n = sstable.getEstimatedPartitionSize().count();
                        sum += sstable.getEstimatedPartitionSize().mean() * n;
                        count += n;
                    }
                }
                return count > 0 ? sum / count : 0;
            }
        });
        bloomFilterFalsePositives = createTableGauge("BloomFilterFalsePositives", new Gauge<Long>()
        {
            public Long getValue()
            {
                long count = 0L;
                for (SSTableReader sstable: cfs.getSSTables(SSTableSet.LIVE))
                    count += sstable.getBloomFilterFalsePositiveCount();
                return count;
            }
        });
        recentBloomFilterFalsePositives = createTableGauge("RecentBloomFilterFalsePositives", new Gauge<Long>()
        {
            public Long getValue()
            {
                long count = 0L;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.LIVE))
                    count += sstable.getRecentBloomFilterFalsePositiveCount();
                return count;
            }
        });
        bloomFilterFalseRatio = createTableGauge("BloomFilterFalseRatio", new Gauge<Double>()
        {
            public Double getValue()
            {
                long falseCount = 0L;
                long trueCount = 0L;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.LIVE))
                {
                    falseCount += sstable.getBloomFilterFalsePositiveCount();
                    trueCount += sstable.getBloomFilterTruePositiveCount();
                }
                if (falseCount == 0L && trueCount == 0L)
                    return 0d;
                return (double) falseCount / (trueCount + falseCount);
            }
        }, new Gauge<Double>() // global gauge
        {
            public Double getValue()
            {
                long falseCount = 0L;
                long trueCount = 0L;
                for (Keyspace keyspace : Keyspace.all())
                {
                    for (SSTableReader sstable : keyspace.getAllSSTables(SSTableSet.LIVE))
                    {
                        falseCount += sstable.getBloomFilterFalsePositiveCount();
                        trueCount += sstable.getBloomFilterTruePositiveCount();
                    }
                }
                if (falseCount == 0L && trueCount == 0L)
                    return 0d;
                return (double) falseCount / (trueCount + falseCount);
            }
        });
        recentBloomFilterFalseRatio = createTableGauge("RecentBloomFilterFalseRatio", new Gauge<Double>()
        {
            public Double getValue()
            {
                long falseCount = 0L;
                long trueCount = 0L;
                for (SSTableReader sstable: cfs.getSSTables(SSTableSet.LIVE))
                {
                    falseCount += sstable.getRecentBloomFilterFalsePositiveCount();
                    trueCount += sstable.getRecentBloomFilterTruePositiveCount();
                }
                if (falseCount == 0L && trueCount == 0L)
                    return 0d;
                return (double) falseCount / (trueCount + falseCount);
            }
        }, new Gauge<Double>() // global gauge
        {
            public Double getValue()
            {
                long falseCount = 0L;
                long trueCount = 0L;
                for (Keyspace keyspace : Keyspace.all())
                {
                    for (SSTableReader sstable : keyspace.getAllSSTables(SSTableSet.LIVE))
                    {
                        falseCount += sstable.getRecentBloomFilterFalsePositiveCount();
                        trueCount += sstable.getRecentBloomFilterTruePositiveCount();
                    }
                }
                if (falseCount == 0L && trueCount == 0L)
                    return 0d;
                return (double) falseCount / (trueCount + falseCount);
            }
        });
        bloomFilterDiskSpaceUsed = createTableGauge("BloomFilterDiskSpaceUsed", new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
                for (SSTableReader sst : cfs.getSSTables(SSTableSet.CANONICAL))
                    total += sst.getBloomFilterSerializedSize();
                return total;
            }
        });
        bloomFilterOffHeapMemoryUsed = createTableGauge("BloomFilterOffHeapMemoryUsed", new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
                for (SSTableReader sst : cfs.getSSTables(SSTableSet.LIVE))
                    total += sst.getBloomFilterOffHeapSize();
                return total;
            }
        });
        indexSummaryOffHeapMemoryUsed = createTableGauge("IndexSummaryOffHeapMemoryUsed", new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
                for (SSTableReader sst : cfs.getSSTables(SSTableSet.LIVE))
                    total += sst.getIndexSummaryOffHeapSize();
                return total;
            }
        });
        compressionMetadataOffHeapMemoryUsed = createTableGauge("CompressionMetadataOffHeapMemoryUsed", new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
                for (SSTableReader sst : cfs.getSSTables(SSTableSet.LIVE))
                    total += sst.getCompressionMetadataOffHeapSize();
                return total;
            }
        });
        speculativeRetries = createTableCounter("SpeculativeRetries");
        keyCacheHitRate = Metrics.register(factory.createMetricName("KeyCacheHitRate"),
                                           aliasFactory.createMetricName("KeyCacheHitRate"),
                                           new RatioGauge()
        {
            @Override
            public Ratio getRatio()
            {
                return Ratio.of(getNumerator(), getDenominator());
            }

            protected double getNumerator()
            {
                long hits = 0L;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.LIVE))
                    hits += sstable.getKeyCacheHit();
                return hits;
            }

            protected double getDenominator()
            {
                long requests = 0L;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.LIVE))
                    requests += sstable.getKeyCacheRequest();
                return Math.max(requests, 1); // to avoid NaN.
            }
        });
        tombstoneScannedHistogram = createTableHistogram("TombstoneScannedHistogram", cfs.keyspace.metric.tombstoneScannedHistogram, false);
        liveScannedHistogram = createTableHistogram("LiveScannedHistogram", cfs.keyspace.metric.liveScannedHistogram, false);
        colUpdateTimeDeltaHistogram = createTableHistogram("ColUpdateTimeDeltaHistogram", cfs.keyspace.metric.colUpdateTimeDeltaHistogram, false);
        coordinatorReadLatency = Metrics.timer(factory.createMetricName("CoordinatorReadLatency"));
        coordinatorScanLatency = Metrics.timer(factory.createMetricName("CoordinatorScanLatency"));
        waitingOnFreeMemtableSpace = Metrics.histogram(factory.createMetricName("WaitingOnFreeMemtableSpace"), false);

        // We do not want to capture view mutation specific metrics for a view
        // They only makes sense to capture on the base table
        if (cfs.metadata.isView())
        {
            viewLockAcquireTime = null;
            viewReadTime = null;
        }
        else
        {
            viewLockAcquireTime = createTableTimer("ViewLockAcquireTime", cfs.keyspace.metric.viewLockAcquireTime);
            viewReadTime = createTableTimer("ViewReadTime", cfs.keyspace.metric.viewReadTime);
        }

        trueSnapshotsSize = createTableGauge("SnapshotsSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                return cfs.trueSnapshotsSize();
            }
        });
        rowCacheHitOutOfRange = createTableCounter("RowCacheHitOutOfRange");
        rowCacheHit = createTableCounter("RowCacheHit");
        rowCacheMiss = createTableCounter("RowCacheMiss");
        droppedMutations = createTableCounter("DroppedMutations");

        casPrepare = new LatencyMetrics(factory, "CasPrepare", cfs.keyspace.metric.casPrepare);
        casPropose = new LatencyMetrics(factory, "CasPropose", cfs.keyspace.metric.casPropose);
        casCommit = new LatencyMetrics(factory, "CasCommit", cfs.keyspace.metric.casCommit);
    }

    public void updateSSTableIterated(int count)
    {
        sstablesPerReadHistogram.update(count);
    }

    /**
     * Release all associated metrics.
     */
    public void release()
    {
        for(Map.Entry<String, String> entry : all.entrySet())
        {
            CassandraMetricsRegistry.MetricName name = factory.createMetricName(entry.getKey());
            CassandraMetricsRegistry.MetricName alias = aliasFactory.createMetricName(entry.getValue());
            allTableMetrics.get(entry.getKey()).remove(Metrics.getMetrics().get(name.getMetricName()));
            Metrics.remove(name, alias);
        }
        readLatency.release();
        writeLatency.release();
        rangeLatency.release();
        Metrics.remove(factory.createMetricName("EstimatedPartitionSizeHistogram"), aliasFactory.createMetricName("EstimatedRowSizeHistogram"));
        Metrics.remove(factory.createMetricName("EstimatedPartitionCount"), aliasFactory.createMetricName("EstimatedRowCount"));
        Metrics.remove(factory.createMetricName("EstimatedColumnCountHistogram"), aliasFactory.createMetricName("EstimatedColumnCountHistogram"));
        Metrics.remove(factory.createMetricName("KeyCacheHitRate"), aliasFactory.createMetricName("KeyCacheHitRate"));
        Metrics.remove(factory.createMetricName("CoordinatorReadLatency"), aliasFactory.createMetricName("CoordinatorReadLatency"));
        Metrics.remove(factory.createMetricName("CoordinatorScanLatency"), aliasFactory.createMetricName("CoordinatorScanLatency"));
        Metrics.remove(factory.createMetricName("WaitingOnFreeMemtableSpace"), aliasFactory.createMetricName("WaitingOnFreeMemtableSpace"));
    }


    /**
     * Create a gauge that will be part of a merged version of all column families.  The global gauge
     * will merge each CF gauge by adding their values
     */
    protected <T extends Number> Gauge<T> createTableGauge(final String name, Gauge<T> gauge)
    {
        return createTableGauge(name, gauge, new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
                for (Metric cfGauge : allTableMetrics.get(name))
                {
                    total = total + ((Gauge<? extends Number>) cfGauge).getValue().longValue();
                }
                return total;
            }
        });
    }

    /**
     * Create a gauge that will be part of a merged version of all column families.  The global gauge
     * is defined as the globalGauge parameter
     */
    protected <G,T> Gauge<T> createTableGauge(String name, Gauge<T> gauge, Gauge<G> globalGauge)
    {
        return createTableGauge(name, name, gauge, globalGauge);
    }

    protected <G,T> Gauge<T> createTableGauge(String name, String alias, Gauge<T> gauge, Gauge<G> globalGauge)
    {
        Gauge<T> cfGauge = Metrics.register(factory.createMetricName(name), aliasFactory.createMetricName(alias), gauge);
        if (register(name, alias, cfGauge))
        {
            Metrics.register(globalFactory.createMetricName(name), globalAliasFactory.createMetricName(alias), globalGauge);
        }
        return cfGauge;
    }

    /**
     * Creates a counter that will also have a global counter thats the sum of all counters across
     * different column families
     */
    protected Counter createTableCounter(final String name)
    {
        return createTableCounter(name, name);
    }

    protected Counter createTableCounter(final String name, final String alias)
    {
        Counter cfCounter = Metrics.counter(factory.createMetricName(name), aliasFactory.createMetricName(alias));
        if (register(name, alias, cfCounter))
        {
            Metrics.register(globalFactory.createMetricName(name),
                             globalAliasFactory.createMetricName(alias),
                             new Gauge<Long>()
            {
                public Long getValue()
                {
                    long total = 0;
                    for (Metric cfGauge : allTableMetrics.get(name))
                    {
                        total += ((Counter) cfGauge).getCount();
                    }
                    return total;
                }
            });
        }
        return cfCounter;
    }

    /**
     * Computes the compression ratio for the specified SSTables
     *
     * @param sstables the SSTables
     * @return the compression ratio for the specified SSTables
     */
    private static Double computeCompressionRatio(Iterable<SSTableReader> sstables)
    {
        double compressedLengthSum = 0;
        double dataLengthSum = 0;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.compression)
            {
                // We should not have any sstable which are in an open early mode as the sstable were selected
                // using SSTableSet.CANONICAL.
                assert sstable.openReason != SSTableReader.OpenReason.EARLY;

                CompressionMetadata compressionMetadata = sstable.getCompressionMetadata();
                compressedLengthSum += compressionMetadata.compressedFileLength;
                dataLengthSum += compressionMetadata.dataLength;
            }
        }
        return dataLengthSum != 0 ? compressedLengthSum / dataLengthSum : 0;
    }

    /**
     * Create a histogram-like interface that will register both a CF, keyspace and global level
     * histogram and forward any updates to both
     */
    protected TableHistogram createTableHistogram(String name, Histogram keyspaceHistogram, boolean considerZeroes)
    {
        return createTableHistogram(name, name, keyspaceHistogram, considerZeroes);
    }

    protected TableHistogram createTableHistogram(String name, String alias, Histogram keyspaceHistogram, boolean considerZeroes)
    {
        Histogram cfHistogram = Metrics.histogram(factory.createMetricName(name), aliasFactory.createMetricName(alias), considerZeroes);
        register(name, alias, cfHistogram);
        return new TableHistogram(cfHistogram,
                                  keyspaceHistogram,
                                  Metrics.histogram(globalFactory.createMetricName(name),
                                                    globalAliasFactory.createMetricName(alias),
                                                    considerZeroes));
    }

    protected TableTimer createTableTimer(String name, Timer keyspaceTimer)
    {
        return createTableTimer(name, name, keyspaceTimer);
    }

    protected TableTimer createTableTimer(String name, String alias, Timer keyspaceTimer)
    {
        Timer cfTimer = Metrics.timer(factory.createMetricName(name), aliasFactory.createMetricName(alias));
        register(name, alias, cfTimer);
        return new TableTimer(cfTimer,
                              keyspaceTimer,
                              Metrics.timer(globalFactory.createMetricName(name),
                                            globalAliasFactory.createMetricName(alias)));
    }

    /**
     * Registers a metric to be removed when unloading CF.
     * @return true if first time metric with that name has been registered
     */
    private boolean register(String name, String alias, Metric metric)
    {
        boolean ret = allTableMetrics.putIfAbsent(name,  new HashSet<>()) == null;
        allTableMetrics.get(name).add(metric);
        all.put(name, alias);
        return ret;
    }

    public static class TableHistogram
    {
        public final Histogram[] all;
        public final Histogram cf;
        private TableHistogram(Histogram cf, Histogram keyspace, Histogram global)
        {
            this.cf = cf;
            this.all = new Histogram[]{cf, keyspace, global};
        }

        public void update(long i)
        {
            for(Histogram histo : all)
            {
                histo.update(i);
            }
        }
    }

    public static class TableTimer
    {
        public final Timer[] all;
        public final Timer cf;
        private TableTimer(Timer cf, Timer keyspace, Timer global)
        {
            this.cf = cf;
            this.all = new Timer[]{cf, keyspace, global};
        }

        public void update(long i, TimeUnit unit)
        {
            for(Timer timer : all)
            {
                timer.update(i, unit);
            }
        }
    }

    static class TableMetricNameFactory implements MetricNameFactory
    {
        private final String keyspaceName;
        private final String tableName;
        private final boolean isIndex;
        private final String type;

        TableMetricNameFactory(ColumnFamilyStore cfs, String type)
        {
            this.keyspaceName = cfs.keyspace.getName();
            this.tableName = cfs.name;
            this.isIndex = cfs.isIndex();
            this.type = type;
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            String groupName = TableMetrics.class.getPackage().getName();
            String type = isIndex ? "Index" + this.type : this.type;

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=").append(type);
            mbeanName.append(",keyspace=").append(keyspaceName);
            mbeanName.append(",scope=").append(tableName);
            mbeanName.append(",name=").append(metricName);

            return new CassandraMetricsRegistry.MetricName(groupName, type, metricName, keyspaceName + "." + tableName, mbeanName.toString());
        }
    }

    static class AllTableMetricNameFactory implements MetricNameFactory
    {
        private final String type;
        public AllTableMetricNameFactory(String type)
        {
            this.type = type;
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            String groupName = TableMetrics.class.getPackage().getName();
            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=" + type);
            mbeanName.append(",name=").append(metricName);
            return new CassandraMetricsRegistry.MetricName(groupName, type, metricName, "all", mbeanName.toString());
        }
    }

    public enum Sampler
    {
        READS, WRITES
    }
}
