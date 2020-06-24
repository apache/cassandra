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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.codahale.metrics.Timer;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.metrics.Sampler.SamplerType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.Pair;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.RatioGauge;

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
    /** Number of SSTables with old version on disk for this CF */
    public final Gauge<Integer> oldVersionSSTableCount;
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
    /** Live rows scanned in queries on this CF */
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
    /**
     * Number of tombstone read failures
     */
    public final Counter tombstoneFailures;
    /**
     * Number of tombstone read warnings
     */
    public final Counter tombstoneWarnings;
    /** CAS Prepare metrics */
    public final LatencyMetrics casPrepare;
    /** CAS Propose metrics */
    public final LatencyMetrics casPropose;
    /** CAS Commit metrics */
    public final LatencyMetrics casCommit;
    /** percent of the data that is repaired */
    public final Gauge<Double> percentRepaired;
    /** Reports the size of sstables in repaired, unrepaired, and any ongoing repair buckets */
    public final Gauge<Long> bytesRepaired;
    public final Gauge<Long> bytesUnrepaired;
    public final Gauge<Long> bytesPendingRepair;
    /** Number of started repairs as coordinator on this table */
    public final Counter repairsStarted;
    /** Number of completed repairs as coordinator on this table */
    public final Counter repairsCompleted;
    /** time spent anticompacting data before participating in a consistent repair */
    public final TableTimer anticompactionTime;
    /** time spent creating merkle trees */
    public final TableTimer validationTime;
    /** time spent syncing data in a repair */
    public final TableTimer syncTime;
    /** approximate number of bytes read while creating merkle trees */
    public final TableHistogram bytesValidated;
    /** number of partitions read creating merkle trees */
    public final TableHistogram partitionsValidated;
    /** number of bytes read while doing anticompaction */
    public final Counter bytesAnticompacted;
    /** number of bytes where the whole sstable was contained in a repairing range so that we only mutated the repair status */
    public final Counter bytesMutatedAnticompaction;
    /** ratio of how much we anticompact vs how much we could mutate the repair status*/
    public final Gauge<Double> mutatedAnticompactionGauge;

    public final Timer coordinatorReadLatency;
    public final Timer coordinatorScanLatency;
    public final Timer coordinatorWriteLatency;

    /** Time spent waiting for free memtable space, either on- or off-heap */
    public final Histogram waitingOnFreeMemtableSpace;

    @Deprecated
    public final Counter droppedMutations;

    private final MetricNameFactory factory;
    private final MetricNameFactory aliasFactory;
    private static final MetricNameFactory globalFactory = new AllTableMetricNameFactory("Table");
    private static final MetricNameFactory globalAliasFactory = new AllTableMetricNameFactory("ColumnFamily");

    public final Counter speculativeRetries;
    public final Counter speculativeFailedRetries;
    public final Counter speculativeInsufficientReplicas;
    public final Gauge<Long> speculativeSampleLatencyNanos;

    public final Counter additionalWrites;
    public final Gauge<Long> additionalWriteLatencyNanos;

    public final Gauge<Integer> unleveledSSTables;

    /**
     * Metrics for inconsistencies detected between repaired data sets across replicas. These
     * are tracked on the coordinator.
     */
    // Incremented where an inconsistency is detected and there are no pending repair sessions affecting
    // the data being read, indicating a genuine mismatch between replicas' repaired data sets.
    public final TableMeter confirmedRepairedInconsistencies;
    // Incremented where an inconsistency is detected, but there are pending & uncommitted repair sessions
    // in play on at least one replica. This may indicate a false positive as the inconsistency could be due to
    // replicas marking the repair session as committed at slightly different times and so some consider it to
    // be part of the repaired set whilst others do not.
    public final TableMeter unconfirmedRepairedInconsistencies;

    // Tracks the amount overreading of repaired data replicas perform in order to produce digests
    // at query time. For each query, on a full data read following an initial digest mismatch, the replicas
    // may read extra repaired data, up to the DataLimit of the command, so that the coordinator can compare
    // the repaired data on each replica. These are tracked on each replica.
    public final TableHistogram repairedDataTrackingOverreadRows;
    public final TableTimer repairedDataTrackingOverreadTime;

    public final static LatencyMetrics globalReadLatency = new LatencyMetrics(globalFactory, globalAliasFactory, "Read");
    public final static LatencyMetrics globalWriteLatency = new LatencyMetrics(globalFactory, globalAliasFactory, "Write");
    public final static LatencyMetrics globalRangeLatency = new LatencyMetrics(globalFactory, globalAliasFactory, "Range");

    /** When sampler activated, will track the most frequently read partitions **/
    public final Sampler<ByteBuffer> topReadPartitionFrequency;
    /** When sampler activated, will track the most frequently written to partitions **/
    public final Sampler<ByteBuffer> topWritePartitionFrequency;
    /** When sampler activated, will track the largest mutations **/
    public final Sampler<ByteBuffer> topWritePartitionSize;
    /** When sampler activated, will track the most frequent partitions with cas contention **/
    public final Sampler<ByteBuffer> topCasPartitionContention;
    /** When sampler activated, will track the slowest local reads **/
    public final Sampler<String> topLocalReadQueryTime;

    private static Pair<Long, Long> totalNonSystemTablesSize(Predicate<SSTableReader> predicate)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13774
        long total = 0;
        long filtered = 0;
        for (String keyspace : Schema.instance.getNonSystemKeyspaces())
        {

            Keyspace k = Schema.instance.getKeyspaceInstance(keyspace);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9054
            if (SchemaConstants.DISTRIBUTED_KEYSPACE_NAME.equals(k.getName()))
                continue;
            if (k.getReplicationStrategy().getReplicationFactor().allReplicas < 2)
                continue;

            for (ColumnFamilyStore cf : k.getColumnFamilyStores())
            {
                if (!SecondaryIndexManager.isIndexColumnFamily(cf.name))
                {
                    for (SSTableReader sstable : cf.getSSTables(SSTableSet.CANONICAL))
                    {
                        if (predicate.test(sstable))
                        {
                            filtered += sstable.uncompressedLength();
                        }
                        total += sstable.uncompressedLength();
                    }
                }
            }
        }
        return Pair.create(filtered, total);
    }

    public static final Gauge<Double> globalPercentRepaired = Metrics.register(globalFactory.createMetricName("PercentRepaired"),
                                                                               new Gauge<Double>()
    {
        public Double getValue()
        {
            Pair<Long, Long> result = totalNonSystemTablesSize(SSTableReader::isRepaired);
            double repaired = result.left;
            double total = result.right;
            return total > 0 ? (repaired / total) * 100 : 100.0;
        }
    });

    public static final Gauge<Long> globalBytesRepaired = Metrics.register(globalFactory.createMetricName("BytesRepaired"),
                                                                           new Gauge<Long>()
    {
        public Long getValue()
        {
            return totalNonSystemTablesSize(SSTableReader::isRepaired).left;
        }
    });

    public static final Gauge<Long> globalBytesUnrepaired = Metrics.register(globalFactory.createMetricName("BytesUnrepaired"),
                                                                             new Gauge<Long>()
    {
        public Long getValue()
        {
            return totalNonSystemTablesSize(s -> !s.isRepaired() && !s.isPendingRepair()).left;
        }
    });

    public static final Gauge<Long> globalBytesPendingRepair = Metrics.register(globalFactory.createMetricName("BytesPendingRepair"),
                                                                                new Gauge<Long>()
    {
        public Long getValue()
        {
            return totalNonSystemTablesSize(SSTableReader::isPendingRepair).left;
        }
    });

    public final Meter readRepairRequests;
    public final Meter shortReadProtectionRequests;
    public final Meter replicaSideFilteringProtectionRequests;

    public final EnumMap<SamplerType, Sampler<?>> samplers;
    /**
     * stores metrics that will be rolled into a single global metric
     */
    public final static ConcurrentMap<String, Set<Metric>> allTableMetrics = Maps.newConcurrentMap();

    /**
     * Stores all metrics created that can be used when unregistering
     */
    public final static Set<ReleasableMetric> all = Sets.newHashSet();

    private interface GetHistogram
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
        EstimatedHistogram getHistogram(SSTableReader reader);
    }

    private static long[] combineHistograms(Iterable<SSTableReader> sstables, GetHistogram getHistogram)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8028
        Iterator<SSTableReader> iterator = sstables.iterator();
        if (!iterator.hasNext())
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10750
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
        factory = new TableMetricNameFactory(cfs, "Table");
        aliasFactory = new TableMetricNameFactory(cfs, "ColumnFamily");

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14436
        samplers = new EnumMap<>(SamplerType.class);
        topReadPartitionFrequency = new FrequencySampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };
        topWritePartitionFrequency = new FrequencySampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };
        topWritePartitionSize = new MaxSampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };
        topCasPartitionContention = new FrequencySampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };
        topLocalReadQueryTime = new MaxSampler<String>()
        {
            public String toString(String value)
            {
                return value;
            }
        };

        samplers.put(SamplerType.READS, topReadPartitionFrequency);
        samplers.put(SamplerType.WRITES, topWritePartitionFrequency);
        samplers.put(SamplerType.WRITE_SIZE, topWritePartitionSize);
        samplers.put(SamplerType.CAS_CONTENTIONS, topCasPartitionContention);
        samplers.put(SamplerType.LOCAL_READ_TIME, topLocalReadQueryTime);

        memtableColumnsCount = createTableGauge("MemtableColumnsCount", new Gauge<Long>()
        {
            public Long getValue()
            {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8568
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8568
                    size += cfs2.getTracker().getView().getCurrentMemtable().getAllocator().onHeap().owns();
                return size;
            }
        });
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
        allMemtablesOffHeapSize = createTableGauge("AllMemtablesOffHeapSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (ColumnFamilyStore cfs2 : cfs.concatWithIndexes())
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8568
                    size += cfs2.getTracker().getView().getCurrentMemtable().getAllocator().offHeap().owns();
                return size;
            }
        });
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
        allMemtablesLiveDataSize = createTableGauge("AllMemtablesLiveDataSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (ColumnFamilyStore cfs2 : cfs.concatWithIndexes())
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8568
                    size += cfs2.getTracker().getView().getCurrentMemtable().getLiveDataSize();
                return size;
            }
        });
        memtableSwitchCount = createTableCounter("MemtableSwitchCount");
        estimatedPartitionSizeHistogram = createTableGauge("EstimatedPartitionSizeHistogram", "EstimatedRowSizeHistogram", new Gauge<long[]>()
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
        }, null);
        estimatedPartitionCount = createTableGauge("EstimatedPartitionCount", "EstimatedRowCount", new Gauge<Long>()
        {
            public Long getValue()
            {
                long memtablePartitions = 0;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8568
                for (Memtable memtable : cfs.getTracker().getView().getAllMemtables())
                   memtablePartitions += memtable.partitionCount();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14647
                try(ColumnFamilyStore.RefViewFragment refViewFragment = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
                {
                    return SSTableReader.getApproximateKeyCount(refViewFragment.sstables) + memtablePartitions;
                }
            }
        }, null);
        estimatedColumnCountHistogram = createTableGauge("EstimatedColumnCountHistogram", "EstimatedColumnCountHistogram", new Gauge<long[]>()
        {
            public long[] getValue()
            {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
                return combineHistograms(cfs.getSSTables(SSTableSet.CANONICAL), new GetHistogram()
                {
                    public EstimatedHistogram getHistogram(SSTableReader reader)
                    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15285
                        return reader.getEstimatedCellPerPartitionCount();
                    }
                });
            }
        }, null);
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12528
                List<SSTableReader> sstables = new ArrayList<>();
                Keyspace.all().forEach(ks -> sstables.addAll(ks.getAllSSTables(SSTableSet.CANONICAL)));
                return computeCompressionRatio(sstables);
            }
        });
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11503
        percentRepaired = createTableGauge("PercentRepaired", new Gauge<Double>()
        {
            public Double getValue()
            {
                double repaired = 0;
                double total = 0;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
                {
                    if (sstable.isRepaired())
                    {
                        repaired += sstable.uncompressedLength();
                    }
                    total += sstable.uncompressedLength();
                }
                return total > 0 ? (repaired / total) * 100 : 100.0;
            }
        });

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13774
        bytesRepaired = createTableGauge("BytesRepaired", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (SSTableReader sstable: Iterables.filter(cfs.getSSTables(SSTableSet.CANONICAL), SSTableReader::isRepaired))
                {
                    size += sstable.uncompressedLength();
                }
                return size;
            }
        });

        bytesUnrepaired = createTableGauge("BytesUnrepaired", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (SSTableReader sstable: Iterables.filter(cfs.getSSTables(SSTableSet.CANONICAL), s -> !s.isRepaired() && !s.isPendingRepair()))
                {
                    size += sstable.uncompressedLength();
                }
                return size;
            }
        });

        bytesPendingRepair = createTableGauge("BytesPendingRepair", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (SSTableReader sstable: Iterables.filter(cfs.getSSTables(SSTableSet.CANONICAL), SSTableReader::isPendingRepair))
                {
                    size += sstable.uncompressedLength();
                }
                return size;
            }
        });

        readLatency = createLatencyMetrics("Read", cfs.keyspace.metric.readLatency, globalReadLatency);
        writeLatency = createLatencyMetrics("Write", cfs.keyspace.metric.writeLatency, globalWriteLatency);
        rangeLatency = createLatencyMetrics("Range", cfs.keyspace.metric.rangeLatency, globalRangeLatency);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
        pendingFlushes = createTableCounter("PendingFlushes");
        bytesFlushed = createTableCounter("BytesFlushed");
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11420

        compactionBytesWritten = createTableCounter("CompactionBytesWritten");
        pendingCompactions = createTableGauge("PendingCompactions", new Gauge<Integer>()
        {
            public Integer getValue()
            {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9342
                return cfs.getCompactionStrategyManager().getEstimatedRemainingTasks();
            }
        });
        liveSSTableCount = createTableGauge("LiveSSTableCount", new Gauge<Integer>()
        {
            public Integer getValue()
            {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
                return cfs.getTracker().getView().liveSSTables().size();
            }
        });
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14197
        oldVersionSSTableCount = createTableGauge("OldVersionSSTableCount", new Gauge<Integer>()
        {
            public Integer getValue()
            {
                int count = 0;
                for (SSTableReader sstable : cfs.getLiveSSTables())
                    if (!sstable.descriptor.version.isLatestVersion())
                        count++;
                return count;
            }
        });
        liveDiskSpaceUsed = createTableCounter("LiveDiskSpaceUsed");
        totalDiskSpaceUsed = createTableCounter("TotalDiskSpaceUsed");
        minPartitionSize = createTableGauge("MinPartitionSize", "MinRowSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long min = 0;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
                for (Metric cfGauge : allTableMetrics.get("MinPartitionSize"))
                {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5657
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
                for (Metric cfGauge : allTableMetrics.get("MaxPartitionSize"))
                {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5657
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
                    for (SSTableReader sstable : keyspace.getAllSSTables(SSTableSet.CANONICAL))
                    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.LIVE))
                    count += sstable.getRecentBloomFilterFalsePositiveCount();
                return count;
            }
        });
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
        bloomFilterFalseRatio = createTableGauge("BloomFilterFalseRatio", new Gauge<Double>()
        {
            public Double getValue()
            {
                long falseCount = 0L;
                long trueCount = 0L;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.LIVE))
                {
                    falseCount += sstable.getBloomFilterFalsePositiveCount();
                    trueCount += sstable.getBloomFilterTruePositiveCount();
                }
                if (falseCount == 0L && trueCount == 0L)
                    return 0d;
                return (double) falseCount / (trueCount + falseCount);
            }
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7273
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7273
        }, new Gauge<Double>() // global gauge
        {
            public Double getValue()
            {
                long falseCount = 0L;
                long trueCount = 0L;
                for (Keyspace keyspace : Keyspace.all())
                {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
                for (SSTableReader sstable: cfs.getSSTables(SSTableSet.LIVE))
                {
                    falseCount += sstable.getRecentBloomFilterFalsePositiveCount();
                    trueCount += sstable.getRecentBloomFilterTruePositiveCount();
                }
                if (falseCount == 0L && trueCount == 0L)
                    return 0d;
                return (double) falseCount / (trueCount + falseCount);
            }
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7273
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7273
        }, new Gauge<Double>() // global gauge
        {
            public Double getValue()
            {
                long falseCount = 0L;
                long trueCount = 0L;
                for (Keyspace keyspace : Keyspace.all())
                {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
        bloomFilterDiskSpaceUsed = createTableGauge("BloomFilterDiskSpaceUsed", new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
                for (SSTableReader sst : cfs.getSSTables(SSTableSet.CANONICAL))
                    total += sst.getBloomFilterSerializedSize();
                return total;
            }
        });
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
        bloomFilterOffHeapMemoryUsed = createTableGauge("BloomFilterOffHeapMemoryUsed", new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
                for (SSTableReader sst : cfs.getSSTables(SSTableSet.LIVE))
                    total += sst.getBloomFilterOffHeapSize();
                return total;
            }
        });
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
        indexSummaryOffHeapMemoryUsed = createTableGauge("IndexSummaryOffHeapMemoryUsed", new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
                for (SSTableReader sst : cfs.getSSTables(SSTableSet.LIVE))
                    total += sst.getIndexSummaryOffHeapSize();
                return total;
            }
        });
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
        compressionMetadataOffHeapMemoryUsed = createTableGauge("CompressionMetadataOffHeapMemoryUsed", new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
                for (SSTableReader sst : cfs.getSSTables(SSTableSet.LIVE))
                    total += sst.getCompressionMetadataOffHeapSize();
                return total;
            }
        });
        speculativeRetries = createTableCounter("SpeculativeRetries");
        speculativeFailedRetries = createTableCounter("SpeculativeFailedRetries");
        speculativeInsufficientReplicas = createTableCounter("SpeculativeInsufficientReplicas");
        speculativeSampleLatencyNanos = createTableGauge("SpeculativeSampleLatencyNanos", () -> cfs.sampleReadLatencyNanos);

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14820
        additionalWrites = createTableCounter("AdditionalWrites");
        additionalWriteLatencyNanos = createTableGauge("AdditionalWriteLatencyNanos", () -> cfs.additionalWriteLatencyNanos);

        keyCacheHitRate = createTableGauge("KeyCacheHitRate", "KeyCacheHitRate", new RatioGauge()
        {
            @Override
            public Ratio getRatio()
            {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5657
                return Ratio.of(getNumerator(), getDenominator());
            }

            protected double getNumerator()
            {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5868
                long hits = 0L;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.LIVE))
                    hits += sstable.getKeyCacheHit();
                return hits;
            }

            protected double getDenominator()
            {
                long requests = 0L;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9699
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.LIVE))
                    requests += sstable.getKeyCacheRequest();
                return Math.max(requests, 1); // to avoid NaN.
            }
        }, null);
        tombstoneScannedHistogram = createTableHistogram("TombstoneScannedHistogram", cfs.keyspace.metric.tombstoneScannedHistogram, false);
        liveScannedHistogram = createTableHistogram("LiveScannedHistogram", cfs.keyspace.metric.liveScannedHistogram, false);
        colUpdateTimeDeltaHistogram = createTableHistogram("ColUpdateTimeDeltaHistogram", cfs.keyspace.metric.colUpdateTimeDeltaHistogram, false);
        coordinatorReadLatency = createTableTimer("CoordinatorReadLatency");
        coordinatorScanLatency = createTableTimer("CoordinatorScanLatency");
        coordinatorWriteLatency = createTableTimer("CoordinatorWriteLatency");
        waitingOnFreeMemtableSpace = createTableHistogram("WaitingOnFreeMemtableSpace", false);

        // We do not want to capture view mutation specific metrics for a view
        // They only makes sense to capture on the base table
        if (cfs.metadata().isView())
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10323
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
        rowCacheHitOutOfRange = createTableCounter("RowCacheHitOutOfRange");
        rowCacheHit = createTableCounter("RowCacheHit");
        rowCacheMiss = createTableCounter("RowCacheMiss");

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13771
        tombstoneFailures = createTableCounter("TombstoneFailures");
        tombstoneWarnings = createTableCounter("TombstoneWarnings");

        droppedMutations = createTableCounter("DroppedMutations");
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10866

        casPrepare = createLatencyMetrics("CasPrepare", cfs.keyspace.metric.casPrepare);
        casPropose = createLatencyMetrics("CasPropose", cfs.keyspace.metric.casPropose);
        casCommit = createLatencyMetrics("CasCommit", cfs.keyspace.metric.casCommit);

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13598
        repairsStarted = createTableCounter("RepairJobsStarted");
        repairsCompleted = createTableCounter("RepairJobsCompleted");

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13531
        anticompactionTime = createTableTimer("AnticompactionTime", cfs.keyspace.metric.anticompactionTime);
        validationTime = createTableTimer("ValidationTime", cfs.keyspace.metric.validationTime);
        syncTime = createTableTimer("SyncTime", cfs.keyspace.metric.repairSyncTime);

        bytesValidated = createTableHistogram("BytesValidated", cfs.keyspace.metric.bytesValidated, false);
        partitionsValidated = createTableHistogram("PartitionsValidated", cfs.keyspace.metric.partitionsValidated, false);
        bytesAnticompacted = createTableCounter("BytesAnticompacted");
        bytesMutatedAnticompaction = createTableCounter("BytesMutatedAnticompaction");
        mutatedAnticompactionGauge = createTableGauge("MutatedAnticompactionGauge", () ->
        {
            double bytesMutated = bytesMutatedAnticompaction.getCount();
            double bytesAnticomp = bytesAnticompacted.getCount();
            if (bytesAnticomp + bytesMutated > 0)
                return bytesMutated / (bytesAnticomp + bytesMutated);
            return 0.0;
        });

        readRepairRequests = createTableMeter("ReadRepairRequests");
        shortReadProtectionRequests = createTableMeter("ShortReadProtectionRequests");
        replicaSideFilteringProtectionRequests = createTableMeter("ReplicaSideFilteringProtectionRequests");

        confirmedRepairedInconsistencies = createTableMeter("RepairedDataInconsistenciesConfirmed", cfs.keyspace.metric.confirmedRepairedInconsistencies);
        unconfirmedRepairedInconsistencies = createTableMeter("RepairedDataInconsistenciesUnconfirmed", cfs.keyspace.metric.unconfirmedRepairedInconsistencies);

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15601
        repairedDataTrackingOverreadRows = createTableHistogram("RepairedDataTrackingOverreadRows", cfs.keyspace.metric.repairedDataTrackingOverreadRows, false);
        repairedDataTrackingOverreadTime = createTableTimer("RepairedDataTrackingOverreadTime", cfs.keyspace.metric.repairedDataTrackingOverreadTime);

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15620
        unleveledSSTables = createTableGauge("UnleveledSSTables", cfs::getUnleveledSSTables, () -> {
            // global gauge
            int cnt = 0;
            for (Metric cfGauge : allTableMetrics.get("UnleveledSSTables"))
            {
                cnt += ((Gauge<? extends Number>) cfGauge).getValue().intValue();
            }
            return cnt;
        });
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
        for (ReleasableMetric entry : all)
        {
            entry.release();
        }
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5657
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
        return createTableGauge(name, name, gauge, globalGauge);
    }

    protected <G,T> Gauge<T> createTableGauge(String name, String alias, Gauge<T> gauge, Gauge<G> globalGauge)
    {
        Gauge<T> cfGauge = Metrics.register(factory.createMetricName(name), aliasFactory.createMetricName(alias), gauge);
        if (register(name, alias, cfGauge) && globalGauge != null)
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

    private Meter createTableMeter(final String name)
    {
        return createTableMeter(name, name);
    }

    private Meter createTableMeter(final String name, final String alias)
    {
        Meter tableMeter = Metrics.meter(factory.createMetricName(name), aliasFactory.createMetricName(alias));
        register(name, alias, tableMeter);
        return tableMeter;
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10225
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12366
        return dataLengthSum != 0 ? compressedLengthSum / dataLengthSum : MetadataCollector.NO_COMPRESSION_RATIO;
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

    protected Histogram createTableHistogram(String name, boolean considerZeroes)
    {
        return createTableHistogram(name, name, considerZeroes);
    }

    protected Histogram createTableHistogram(String name, String alias, boolean considerZeroes)
    {
        Histogram tableHistogram = Metrics.histogram(factory.createMetricName(name), aliasFactory.createMetricName(alias), considerZeroes);
        register(name, alias, tableHistogram);
        return tableHistogram;
    }

    protected TableTimer createTableTimer(String name, Timer keyspaceTimer)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10323
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

    protected Timer createTableTimer(String name)
    {
        return createTableTimer(name, name);
    }

    protected Timer createTableTimer(String name, String alias)
    {
        Timer tableTimer = Metrics.timer(factory.createMetricName(name), aliasFactory.createMetricName(alias));
        register(name, alias, tableTimer);
        return tableTimer;
    }

    protected TableMeter createTableMeter(String name, Meter keyspaceMeter)
    {
        return createTableMeter(name, name, keyspaceMeter);
    }

    protected TableMeter createTableMeter(String name, String alias, Meter keyspaceMeter)
    {
        Meter meter = Metrics.meter(factory.createMetricName(name), aliasFactory.createMetricName(alias));
        register(name, alias, meter);
        return new TableMeter(meter,
                              keyspaceMeter,
                              Metrics.meter(globalFactory.createMetricName(name),
                                            globalAliasFactory.createMetricName(alias)));
    }

    private LatencyMetrics createLatencyMetrics(String namePrefix, LatencyMetrics ... parents)
    {
        LatencyMetrics metric = new LatencyMetrics(factory, namePrefix, parents);
        all.add(() -> metric.release());
        return metric;
    }

    /**
     * Registers a metric to be removed when unloading CF.
     * @return true if first time metric with that name has been registered
     */
    private boolean register(String name, String alias, Metric metric)
    {
        boolean ret = allTableMetrics.putIfAbsent(name, ConcurrentHashMap.newKeySet()) == null;
        allTableMetrics.get(name).add(metric);
        all.add(() -> releaseMetric(name, alias));
        return ret;
    }

    private void releaseMetric(String metricName, String metricAlias)
    {
        CassandraMetricsRegistry.MetricName name = factory.createMetricName(metricName);
        CassandraMetricsRegistry.MetricName alias = aliasFactory.createMetricName(metricAlias);
        final Metric metric = Metrics.getMetrics().get(name.getMetricName());
        if (metric != null)
        {   // Metric will be null if we are releasing a view metric.  Views have null for ViewLockAcquireTime and ViewLockReadTime
            allTableMetrics.get(metricName).remove(metric);
            Metrics.remove(name, alias);
        }
    }

    public static class TableMeter
    {
        public final Meter[] all;
        public final Meter table;
        private TableMeter(Meter table, Meter keyspace, Meter global)
        {
            this.table = table;
            this.all = new Meter[]{table, keyspace, global};
        }

        public void mark()
        {
            for (Meter meter : all)
            {
                meter.mark();
            }
        }
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10323
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

        public Context time()
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13531
            return new Context(all);
        }

        public static class Context implements AutoCloseable
        {
            private final long start;
            private final Timer [] all;

            private Context(Timer [] all)
            {
                this.all = all;
                start = System.nanoTime();
            }

            public void close()
            {
                long duration = System.nanoTime() - start;
                for (Timer t : all)
                    t.update(duration, TimeUnit.NANOSECONDS);
            }
        }
    }

    static class TableMetricNameFactory implements MetricNameFactory
    {
        private final String keyspaceName;
        private final String tableName;
        private final boolean isIndex;
        private final String type;

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9448
        TableMetricNameFactory(ColumnFamilyStore cfs, String type)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5613
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
            mbeanName.append("type=").append(type);
            mbeanName.append(",name=").append(metricName);
            return new CassandraMetricsRegistry.MetricName(groupName, type, metricName, "all", mbeanName.toString());
        }
    }

    @FunctionalInterface
    public interface ReleasableMetric
    {
        void release();
    }
}
