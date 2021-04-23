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
package org.apache.cassandra.io.sstable.metadata;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionStatisticsCollector;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.streamhist.StreamingTombstoneHistogramBuilder;
import org.apache.cassandra.utils.streamhist.TombstoneHistogram;

public class MetadataCollector implements PartitionStatisticsCollector
{
    public static final double NO_COMPRESSION_RATIO = -1.0;
    private static final ByteBuffer[] EMPTY_CLUSTERING = new ByteBuffer[0];

    private long currentPartitionCells = 0;

    static EstimatedHistogram defaultCellPerPartitionCountHistogram()
    {
        // EH of 118 can track a max value of 4139110981, i.e., > 4B cells
        return new EstimatedHistogram(118);
    }

    static EstimatedHistogram defaultPartitionSizeHistogram()
    {
        // EH of 150 can track a max value of 1697806495183, i.e., > 1.5PB
        return new EstimatedHistogram(150);
    }

    static TombstoneHistogram defaultTombstoneDropTimeHistogram()
    {
        return TombstoneHistogram.createDefault();
    }

    public static StatsMetadata defaultStatsMetadata()
    {
        return new StatsMetadata(defaultPartitionSizeHistogram(),
                                 defaultCellPerPartitionCountHistogram(),
                                 IntervalSet.empty(),
                                 Long.MIN_VALUE,
                                 Long.MAX_VALUE,
                                 Integer.MAX_VALUE,
                                 Integer.MAX_VALUE,
                                 0,
                                 Integer.MAX_VALUE,
                                 NO_COMPRESSION_RATIO,
                                 defaultTombstoneDropTimeHistogram(),
                                 0,
                                 Collections.emptyList(),
                                 Slice.ALL,
                                 true,
                                 true,
                                 ActiveRepairService.UNREPAIRED_SSTABLE,
                                 -1,
                                 -1,
                                 null,
                                 null,
                                 false,
                                 Collections.emptyMap());
    }

    protected EstimatedHistogram estimatedPartitionSize = defaultPartitionSizeHistogram();
    // TODO: cound the number of row per partition (either with the number of cells, or instead)
    protected EstimatedHistogram estimatedCellPerPartitionCount = defaultCellPerPartitionCountHistogram();
    protected IntervalSet<CommitLogPosition> commitLogIntervals = IntervalSet.empty();
    protected final MinMaxLongTracker timestampTracker = new MinMaxLongTracker();
    protected final MinMaxIntTracker localDeletionTimeTracker = new MinMaxIntTracker(Cell.NO_DELETION_TIME, Cell.NO_DELETION_TIME);
    protected final MinMaxIntTracker ttlTracker = new MinMaxIntTracker(Cell.NO_TTL, Cell.NO_TTL);
    protected double compressionRatio = NO_COMPRESSION_RATIO;
    protected StreamingTombstoneHistogramBuilder estimatedTombstoneDropTime = new StreamingTombstoneHistogramBuilder(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE, SSTable.TOMBSTONE_HISTOGRAM_SPOOL_SIZE, SSTable.TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS);
    protected int sstableLevel;

    /**
     * The smallest clustering prefix for any {@link Unfiltered} in the sstable.
     *
     * <p>This is always either a Clustering, or a start bound (since for any end range tombstone bound, there should
     * be a corresponding start bound that is smaller).
     */
    private ClusteringPrefix<?> minClustering = ClusteringBound.MAX_START;
    /**
     * The largest clustering prefix for any {@link Unfiltered} in the sstable.
     *
     * <p>This is always either a Clustering, or an end bound (since for any start range tombstone bound, there should
     * be a corresponding end bound that is bigger).
     */
    private ClusteringPrefix<?> maxClustering = ClusteringBound.MIN_END;
    private boolean clusteringInitialized = false;

    protected boolean hasLegacyCounterShards = false;
    private boolean hasPartitionLevelDeletions = false;
    protected long totalColumnsSet;
    protected long totalRows;

    private final AbstractType<?>[] comparators;
    /**
     * Default cardinality estimation method is to use HyperLogLog++.
     * Parameter here(p=13, sp=25) should give reasonable estimation
     * while lowering bytes required to hold information.
     * See CASSANDRA-5906 for detail.
     */
    protected ICardinality cardinality = new HyperLogLogPlus(13, 25);
    private final ClusteringComparator comparator;

    private final UUID originatingHostId;

    public MetadataCollector(ClusteringComparator comparator)
    {
        this(comparator, StorageService.instance.getLocalHostUUID());
    }

    public MetadataCollector(ClusteringComparator comparator, UUID originatingHostId)
    {
        this.comparator = comparator;

        int clusteringTypesNum = comparator.size();
        this.comparators = new AbstractType[clusteringTypesNum];
        for (int i = 0; i < clusteringTypesNum; i++)
            comparators[i] = comparator.subtype(i);
        this.originatingHostId = originatingHostId;
    }

    public MetadataCollector(Iterable<SSTableReader> sstables, ClusteringComparator comparator, int level)
    {
        this(comparator);

        IntervalSet.Builder<CommitLogPosition> intervals = new IntervalSet.Builder<>();
        if (originatingHostId != null)
        {
            for (SSTableReader sstable : sstables)
            {
                if (originatingHostId.equals(sstable.getSSTableMetadata().originatingHostId))
                    intervals.addAll(sstable.getSSTableMetadata().commitLogIntervals);
            }
        }
        commitLogIntervals(intervals.build());
        sstableLevel(level);
    }

    public MetadataCollector addKey(ByteBuffer key)
    {
        long hashed = MurmurHash.hash2_64(key, key.position(), key.remaining(), 0);
        cardinality.offerHashed(hashed);
        return this;
    }

    public MetadataCollector addPartitionSizeInBytes(long partitionSize)
    {
        estimatedPartitionSize.add(partitionSize);
        return this;
    }

    public MetadataCollector addCellPerPartitionCount(long cellCount)
    {
        estimatedCellPerPartitionCount.add(cellCount);
        return this;
    }

    public MetadataCollector addCellPerPartitionCount()
    {
        estimatedCellPerPartitionCount.add(currentPartitionCells);
        currentPartitionCells = 0;
        return this;
    }

    /**
     * Ratio is compressed/uncompressed and it is
     * if you have 1.x then compression isn't helping
     */
    public MetadataCollector addCompressionRatio(long compressed, long uncompressed)
    {
        compressionRatio = (double) compressed/uncompressed;
        return this;
    }

    public void update(LivenessInfo newInfo)
    {
        if (newInfo.isEmpty())
            return;

        updateTimestamp(newInfo.timestamp());
        updateTTL(newInfo.ttl());
        updateLocalDeletionTime(newInfo.localExpirationTime());
    }

    public void update(Cell<?> cell)
    {
        ++currentPartitionCells;
        updateTimestamp(cell.timestamp());
        updateTTL(cell.ttl());
        updateLocalDeletionTime(cell.localDeletionTime());
    }

    public void updatePartitionDeletion(DeletionTime dt)
    {
        if (!dt.isLive())
            hasPartitionLevelDeletions = true;
        update(dt);
    }

    public void update(DeletionTime dt)
    {
        if (!dt.isLive())
        {
            updateTimestamp(dt.markedForDeleteAt());
            updateLocalDeletionTime(dt.localDeletionTime());
        }
    }

    public void updateColumnSetPerRow(long columnSetInRow)
    {
        totalColumnsSet += columnSetInRow;
        ++totalRows;
    }

    private void updateTimestamp(long newTimestamp)
    {
        timestampTracker.update(newTimestamp);
    }

    private void updateLocalDeletionTime(int newLocalDeletionTime)
    {
        localDeletionTimeTracker.update(newLocalDeletionTime);
        if (newLocalDeletionTime != Cell.NO_DELETION_TIME)
            estimatedTombstoneDropTime.update(newLocalDeletionTime);
    }

    private void updateTTL(int newTTL)
    {
        ttlTracker.update(newTTL);
    }

    public MetadataCollector commitLogIntervals(IntervalSet<CommitLogPosition> commitLogIntervals)
    {
        this.commitLogIntervals = commitLogIntervals;
        return this;
    }

    public MetadataCollector sstableLevel(int sstableLevel)
    {
        this.sstableLevel = sstableLevel;
        return this;
    }

    public void updateClusteringValues(Clustering<?> clustering)
    {
        if (clustering == Clustering.STATIC_CLUSTERING)
            return;

        if (!clusteringInitialized)
        {
            clusteringInitialized = true;
            minClustering = clustering.minimize();
            maxClustering = minClustering;
        }
        else if (comparator.compare((ClusteringPrefix<?>) clustering, (ClusteringPrefix<?>) maxClustering) > 0)
        {
            maxClustering = clustering.minimize();
        }
        else if (comparator.compare((ClusteringPrefix<?>) clustering, (ClusteringPrefix<?>) minClustering) < 0)
        {
            minClustering = clustering.minimize();
        }
    }

    public void updateClusteringValuesByBoundOrBoundary(ClusteringBoundOrBoundary<?> clusteringBoundOrBoundary)
    {
        // In a SSTable, every opening marker will be closed, so the start of a range tombstone marker will never be
        // be the maxClustering (the corresponding close might though) and there is no point in doing the comparison
        // (and vice-versa for the close). By the same reasoning, a boundary will never be either the min or max
        // clustering and we can save comparisons.
        if (clusteringBoundOrBoundary.isBoundary())
            return;

        if (!clusteringInitialized)
        {
            clusteringInitialized = true;
            minClustering = clusteringBoundOrBoundary.minimize();
            maxClustering = minClustering;
        }
        else if (clusteringBoundOrBoundary.kind().isStart())
        {
            if (comparator.compare(clusteringBoundOrBoundary, minClustering) < 0)
                minClustering = clusteringBoundOrBoundary.minimize();
        }
        else
        {
            if (comparator.compare(clusteringBoundOrBoundary, maxClustering) > 0)
                maxClustering = clusteringBoundOrBoundary.minimize();
        }
    }

    public void updateHasLegacyCounterShards(boolean hasLegacyCounterShards)
    {
        this.hasLegacyCounterShards = this.hasLegacyCounterShards || hasLegacyCounterShards;
    }

    public Map<MetadataType, MetadataComponent> finalizeMetadata(String partitioner, double bloomFilterFPChance, long repairedAt, UUID pendingRepair, boolean isTransient, SerializationHeader header)
    {
        Map<MetadataType, MetadataComponent> components = new EnumMap<>(MetadataType.class);
        components.put(MetadataType.VALIDATION, new ValidationMetadata(partitioner, bloomFilterFPChance));
        components.put(MetadataType.STATS, new StatsMetadata(estimatedPartitionSize,
                                                             estimatedCellPerPartitionCount,
                                                             commitLogIntervals,
                                                             timestampTracker.min(),
                                                             timestampTracker.max(),
                                                             localDeletionTimeTracker.min(),
                                                             localDeletionTimeTracker.max(),
                                                             ttlTracker.min(),
                                                             ttlTracker.max(),
                                                             compressionRatio,
                                                             estimatedTombstoneDropTime.build(),
                                                             sstableLevel,
                                                             comparator.subtypes(),
                                                             Slice.make(minClustering, maxClustering),
                                                             hasLegacyCounterShards,
                                                             hasPartitionLevelDeletions,
                                                             repairedAt,
                                                             totalColumnsSet,
                                                             totalRows,
                                                             originatingHostId,
                                                             pendingRepair,
                                                             isTransient,
                                                             Collections.emptyMap()));
        components.put(MetadataType.COMPACTION, new CompactionMetadata(cardinality));
        components.put(MetadataType.HEADER, header.toComponent());
        return components;
    }

    /**
     * Release large memory objects while keeping metrics intact
     */
    public void release()
    {
        estimatedTombstoneDropTime.releaseBuffers();
    }

    public static class MinMaxLongTracker
    {
        private final long defaultMin;
        private final long defaultMax;

        private boolean isSet = false;
        private long min;
        private long max;

        public MinMaxLongTracker()
        {
            this(Long.MIN_VALUE, Long.MAX_VALUE);
        }

        public MinMaxLongTracker(long defaultMin, long defaultMax)
        {
            this.defaultMin = defaultMin;
            this.defaultMax = defaultMax;
        }

        public void update(long value)
        {
            if (!isSet)
            {
                min = max = value;
                isSet = true;
            }
            else
            {
                if (value < min)
                    min = value;
                if (value > max)
                    max = value;
            }
        }

        public long min()
        {
            return isSet ? min : defaultMin;
        }

        public long max()
        {
            return isSet ? max : defaultMax;
        }
    }

    public static class MinMaxIntTracker
    {
        private final int defaultMin;
        private final int defaultMax;

        private boolean isSet = false;
        private int min;
        private int max;

        public MinMaxIntTracker()
        {
            this(Integer.MIN_VALUE, Integer.MAX_VALUE);
        }

        public MinMaxIntTracker(int defaultMin, int defaultMax)
        {
            this.defaultMin = defaultMin;
            this.defaultMax = defaultMax;
        }

        public void update(int value)
        {
            if (!isSet)
            {
                min = max = value;
                isSet = true;
            }
            else
            {
                if (value < min)
                    min = value;
                if (value > max)
                    max = value;
            }
        }

        public int min()
        {
            return isSet ? min : defaultMin;
        }

        public int max()
        {
            return isSet ? max : defaultMax;
        }
    }
}
