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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.streamhist.TombstoneHistogram;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * SSTable metadata that always stay on heap.
 */
public class StatsMetadata extends MetadataComponent
{
    public static final IMetadataComponentSerializer serializer = new StatsMetadataSerializer();
    public static final ISerializer<IntervalSet<CommitLogPosition>> commitLogPositionSetSerializer = IntervalSet.serializer(CommitLogPosition.serializer);

    public final EstimatedHistogram estimatedPartitionSize;
    public final EstimatedHistogram estimatedCellPerPartitionCount;
    public final IntervalSet<CommitLogPosition> commitLogIntervals;
    public final long minTimestamp;
    public final long maxTimestamp;
    public final int minLocalDeletionTime;
    public final int maxLocalDeletionTime;
    public final int minTTL;
    public final int maxTTL;
    public final double compressionRatio;
    public final TombstoneHistogram estimatedTombstoneDropTime;
    public final int sstableLevel;
    public final List<ByteBuffer> minClusteringValues;
    public final List<ByteBuffer> maxClusteringValues;
    public final boolean hasLegacyCounterShards;
    public final long repairedAt;
    public final long totalColumnsSet;
    public final long totalRows;
    public final UUID originatingHostId;
    public final TimeUUID pendingRepair;
    public final boolean isTransient;
    // just holds the current encoding stats to avoid allocating - it is not serialized
    public final EncodingStats encodingStats;

    public StatsMetadata(EstimatedHistogram estimatedPartitionSize,
                         EstimatedHistogram estimatedCellPerPartitionCount,
                         IntervalSet<CommitLogPosition> commitLogIntervals,
                         long minTimestamp,
                         long maxTimestamp,
                         int minLocalDeletionTime,
                         int maxLocalDeletionTime,
                         int minTTL,
                         int maxTTL,
                         double compressionRatio,
                         TombstoneHistogram estimatedTombstoneDropTime,
                         int sstableLevel,
                         List<ByteBuffer> minClusteringValues,
                         List<ByteBuffer> maxClusteringValues,
                         boolean hasLegacyCounterShards,
                         long repairedAt,
                         long totalColumnsSet,
                         long totalRows,
                         UUID originatingHostId,
                         TimeUUID pendingRepair,
                         boolean isTransient)
    {
        this.estimatedPartitionSize = estimatedPartitionSize;
        this.estimatedCellPerPartitionCount = estimatedCellPerPartitionCount;
        this.commitLogIntervals = commitLogIntervals;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.minLocalDeletionTime = minLocalDeletionTime;
        this.maxLocalDeletionTime = maxLocalDeletionTime;
        this.minTTL = minTTL;
        this.maxTTL = maxTTL;
        this.compressionRatio = compressionRatio;
        this.estimatedTombstoneDropTime = estimatedTombstoneDropTime;
        this.sstableLevel = sstableLevel;
        this.minClusteringValues = minClusteringValues;
        this.maxClusteringValues = maxClusteringValues;
        this.hasLegacyCounterShards = hasLegacyCounterShards;
        this.repairedAt = repairedAt;
        this.totalColumnsSet = totalColumnsSet;
        this.totalRows = totalRows;
        this.originatingHostId = originatingHostId;
        this.pendingRepair = pendingRepair;
        this.isTransient = isTransient;
        this.encodingStats = new EncodingStats(minTimestamp, minLocalDeletionTime, minTTL);
    }

    public MetadataType getType()
    {
        return MetadataType.STATS;
    }

    /**
     * @param gcBefore gc time in seconds
     * @return estimated droppable tombstone ratio at given gcBefore time.
     */
    public double getEstimatedDroppableTombstoneRatio(int gcBefore)
    {
        long estimatedColumnCount = this.estimatedCellPerPartitionCount.mean() * this.estimatedCellPerPartitionCount.count();
        if (estimatedColumnCount > 0)
        {
            double droppable = getDroppableTombstonesBefore(gcBefore);
            return droppable / estimatedColumnCount;
        }
        return 0.0f;
    }

    /**
     * @param gcBefore gc time in seconds
     * @return amount of droppable tombstones
     */
    public double getDroppableTombstonesBefore(int gcBefore)
    {
        return estimatedTombstoneDropTime.sum(gcBefore);
    }

    public StatsMetadata mutateLevel(int newLevel)
    {
        return new StatsMetadata(estimatedPartitionSize,
                                 estimatedCellPerPartitionCount,
                                 commitLogIntervals,
                                 minTimestamp,
                                 maxTimestamp,
                                 minLocalDeletionTime,
                                 maxLocalDeletionTime,
                                 minTTL,
                                 maxTTL,
                                 compressionRatio,
                                 estimatedTombstoneDropTime,
                                 newLevel,
                                 minClusteringValues,
                                 maxClusteringValues,
                                 hasLegacyCounterShards,
                                 repairedAt,
                                 totalColumnsSet,
                                 totalRows,
                                 originatingHostId,
                                 pendingRepair,
                                 isTransient);
    }

    public StatsMetadata mutateRepairedMetadata(long newRepairedAt, TimeUUID newPendingRepair, boolean newIsTransient)
    {
        return new StatsMetadata(estimatedPartitionSize,
                                 estimatedCellPerPartitionCount,
                                 commitLogIntervals,
                                 minTimestamp,
                                 maxTimestamp,
                                 minLocalDeletionTime,
                                 maxLocalDeletionTime,
                                 minTTL,
                                 maxTTL,
                                 compressionRatio,
                                 estimatedTombstoneDropTime,
                                 sstableLevel,
                                 minClusteringValues,
                                 maxClusteringValues,
                                 hasLegacyCounterShards,
                                 newRepairedAt,
                                 totalColumnsSet,
                                 totalRows,
                                 originatingHostId,
                                 newPendingRepair,
                                 newIsTransient);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsMetadata that = (StatsMetadata) o;
        return new EqualsBuilder()
                       .append(estimatedPartitionSize, that.estimatedPartitionSize)
                       .append(estimatedCellPerPartitionCount, that.estimatedCellPerPartitionCount)
                       .append(commitLogIntervals, that.commitLogIntervals)
                       .append(minTimestamp, that.minTimestamp)
                       .append(maxTimestamp, that.maxTimestamp)
                       .append(minLocalDeletionTime, that.minLocalDeletionTime)
                       .append(maxLocalDeletionTime, that.maxLocalDeletionTime)
                       .append(minTTL, that.minTTL)
                       .append(maxTTL, that.maxTTL)
                       .append(compressionRatio, that.compressionRatio)
                       .append(estimatedTombstoneDropTime, that.estimatedTombstoneDropTime)
                       .append(sstableLevel, that.sstableLevel)
                       .append(repairedAt, that.repairedAt)
                       .append(maxClusteringValues, that.maxClusteringValues)
                       .append(minClusteringValues, that.minClusteringValues)
                       .append(hasLegacyCounterShards, that.hasLegacyCounterShards)
                       .append(totalColumnsSet, that.totalColumnsSet)
                       .append(totalRows, that.totalRows)
                       .append(originatingHostId, that.originatingHostId)
                       .append(pendingRepair, that.pendingRepair)
                       .build();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
                       .append(estimatedPartitionSize)
                       .append(estimatedCellPerPartitionCount)
                       .append(commitLogIntervals)
                       .append(minTimestamp)
                       .append(maxTimestamp)
                       .append(minLocalDeletionTime)
                       .append(maxLocalDeletionTime)
                       .append(minTTL)
                       .append(maxTTL)
                       .append(compressionRatio)
                       .append(estimatedTombstoneDropTime)
                       .append(sstableLevel)
                       .append(repairedAt)
                       .append(maxClusteringValues)
                       .append(minClusteringValues)
                       .append(hasLegacyCounterShards)
                       .append(totalColumnsSet)
                       .append(totalRows)
                       .append(originatingHostId)
                       .append(pendingRepair)
                       .build();
    }

    public static class StatsMetadataSerializer implements IMetadataComponentSerializer<StatsMetadata>
    {
        private static final Logger logger = LoggerFactory.getLogger(StatsMetadataSerializer.class);

        public int serializedSize(Version version, StatsMetadata component) throws IOException
        {
            int size = 0;
            size += EstimatedHistogram.serializer.serializedSize(component.estimatedPartitionSize);
            size += EstimatedHistogram.serializer.serializedSize(component.estimatedCellPerPartitionCount);
            size += CommitLogPosition.serializer.serializedSize(component.commitLogIntervals.upperBound().orElse(CommitLogPosition.NONE));
            size += 8 + 8 + 4 + 4 + 4 + 4 + 8 + 8; // mix/max timestamp(long), min/maxLocalDeletionTime(int), min/max TTL, compressionRatio(double), repairedAt (long)
            size += TombstoneHistogram.serializer.serializedSize(component.estimatedTombstoneDropTime);
            size += TypeSizes.sizeof(component.sstableLevel);
            // min column names
            size += 4;
            for (ByteBuffer value : component.minClusteringValues)
                size += 2 + value.remaining(); // with short length
            // max column names
            size += 4;
            for (ByteBuffer value : component.maxClusteringValues)
                size += 2 + value.remaining(); // with short length
            size += TypeSizes.sizeof(component.hasLegacyCounterShards);
            size += 8 + 8; // totalColumnsSet, totalRows
            if (version.hasCommitLogLowerBound())
                size += CommitLogPosition.serializer.serializedSize(component.commitLogIntervals.lowerBound().orElse(CommitLogPosition.NONE));
            if (version.hasCommitLogIntervals())
                size += commitLogPositionSetSerializer.serializedSize(component.commitLogIntervals);

            if (version.hasPendingRepair())
            {
                size += 1;
                if (component.pendingRepair != null)
                    size += TimeUUID.sizeInBytes();
            }

            if (version.hasIsTransient())
            {
                size += TypeSizes.sizeof(component.isTransient);
            }

            if (version.hasOriginatingHostId())
            {
                size += 1; // boolean: is originatingHostId present
                if (component.originatingHostId != null)
                    size += UUIDSerializer.serializer.serializedSize(component.originatingHostId, version.correspondingMessagingVersion());
            }

            return size;
        }

        public void serialize(Version version, StatsMetadata component, DataOutputPlus out) throws IOException
        {
            EstimatedHistogram.serializer.serialize(component.estimatedPartitionSize, out);
            EstimatedHistogram.serializer.serialize(component.estimatedCellPerPartitionCount, out);
            CommitLogPosition.serializer.serialize(component.commitLogIntervals.upperBound().orElse(CommitLogPosition.NONE), out);
            out.writeLong(component.minTimestamp);
            out.writeLong(component.maxTimestamp);
            out.writeInt(component.minLocalDeletionTime);
            out.writeInt(component.maxLocalDeletionTime);
            out.writeInt(component.minTTL);
            out.writeInt(component.maxTTL);
            out.writeDouble(component.compressionRatio);
            TombstoneHistogram.serializer.serialize(component.estimatedTombstoneDropTime, out);
            out.writeInt(component.sstableLevel);
            out.writeLong(component.repairedAt);
            out.writeInt(component.minClusteringValues.size());
            for (ByteBuffer value : component.minClusteringValues)
                ByteBufferUtil.writeWithShortLength(value, out);
            out.writeInt(component.maxClusteringValues.size());
            for (ByteBuffer value : component.maxClusteringValues)
                ByteBufferUtil.writeWithShortLength(value, out);
            out.writeBoolean(component.hasLegacyCounterShards);

            out.writeLong(component.totalColumnsSet);
            out.writeLong(component.totalRows);

            if (version.hasCommitLogLowerBound())
                CommitLogPosition.serializer.serialize(component.commitLogIntervals.lowerBound().orElse(CommitLogPosition.NONE), out);
            if (version.hasCommitLogIntervals())
                commitLogPositionSetSerializer.serialize(component.commitLogIntervals, out);

            if (version.hasPendingRepair())
            {
                if (component.pendingRepair != null)
                {
                    out.writeByte(1);
                    component.pendingRepair.serialize(out);
                }
                else
                {
                    out.writeByte(0);
                }
            }

            if (version.hasIsTransient())
            {
                out.writeBoolean(component.isTransient);
            }

            if (version.hasOriginatingHostId())
            {
                if (component.originatingHostId != null)
                {
                    out.writeByte(1);
                    UUIDSerializer.serializer.serialize(component.originatingHostId, out, 0);
                }
                else
                {
                    out.writeByte(0);
                }
            }
        }

        public StatsMetadata deserialize(Version version, DataInputPlus in) throws IOException
        {
            EstimatedHistogram partitionSizes = EstimatedHistogram.serializer.deserialize(in);

            if (partitionSizes.isOverflowed())
            {
                logger.warn("Deserialized partition size histogram with {} values greater than the maximum of {}. " +
                            "Clearing the overflow bucket to allow for degraded mean and percentile calculations...",
                            partitionSizes.overflowCount(), partitionSizes.getLargestBucketOffset());

                partitionSizes.clearOverflow();
            }

            EstimatedHistogram columnCounts = EstimatedHistogram.serializer.deserialize(in);

            if (columnCounts.isOverflowed())
            {
                logger.warn("Deserialized partition cell count histogram with {} values greater than the maximum of {}. " +
                            "Clearing the overflow bucket to allow for degraded mean and percentile calculations...",
                            columnCounts.overflowCount(), columnCounts.getLargestBucketOffset());

                columnCounts.clearOverflow();
            }

            CommitLogPosition commitLogLowerBound = CommitLogPosition.NONE, commitLogUpperBound;
            commitLogUpperBound = CommitLogPosition.serializer.deserialize(in);
            long minTimestamp = in.readLong();
            long maxTimestamp = in.readLong();
            int minLocalDeletionTime = in.readInt();
            int maxLocalDeletionTime = in.readInt();
            int minTTL = in.readInt();
            int maxTTL = in.readInt();
            double compressionRatio = in.readDouble();
            TombstoneHistogram tombstoneHistogram = TombstoneHistogram.serializer.deserialize(in);
            int sstableLevel = in.readInt();
            long repairedAt = in.readLong();

            // for legacy sstables, we skip deserializing the min and max clustering value
            // to prevent erroneously excluding sstables from reads (see CASSANDRA-14861)
            int colCount = in.readInt();
            List<ByteBuffer> minClusteringValues = new ArrayList<>(colCount);
            for (int i = 0; i < colCount; i++)
            {
                ByteBuffer val = ByteBufferUtil.readWithShortLength(in);
                if (version.hasAccurateMinMax())
                    minClusteringValues.add(val);
            }

            colCount = in.readInt();
            List<ByteBuffer> maxClusteringValues = new ArrayList<>(colCount);
            for (int i = 0; i < colCount; i++)
            {
                ByteBuffer val = ByteBufferUtil.readWithShortLength(in);
                if (version.hasAccurateMinMax())
                    maxClusteringValues.add(val);
            }

            boolean hasLegacyCounterShards = in.readBoolean();

            long totalColumnsSet = in.readLong();
            long totalRows = in.readLong();

            if (version.hasCommitLogLowerBound())
                commitLogLowerBound = CommitLogPosition.serializer.deserialize(in);
            IntervalSet<CommitLogPosition> commitLogIntervals;
            if (version.hasCommitLogIntervals())
                commitLogIntervals = commitLogPositionSetSerializer.deserialize(in);
            else
                commitLogIntervals = new IntervalSet<CommitLogPosition>(commitLogLowerBound, commitLogUpperBound);

            TimeUUID pendingRepair = null;
            if (version.hasPendingRepair() && in.readByte() != 0)
            {
                pendingRepair = TimeUUID.deserialize(in);
            }

            boolean isTransient = version.hasIsTransient() && in.readBoolean();

            UUID originatingHostId = null;
            if (version.hasOriginatingHostId() && in.readByte() != 0)
                originatingHostId = UUIDSerializer.serializer.deserialize(in, 0);

            return new StatsMetadata(partitionSizes,
                                     columnCounts,
                                     commitLogIntervals,
                                     minTimestamp,
                                     maxTimestamp,
                                     minLocalDeletionTime,
                                     maxLocalDeletionTime,
                                     minTTL,
                                     maxTTL,
                                     compressionRatio,
                                     tombstoneHistogram,
                                     sstableLevel,
                                     minClusteringValues,
                                     maxClusteringValues,
                                     hasLegacyCounterShards,
                                     repairedAt,
                                     totalColumnsSet,
                                     totalRows,
                                     originatingHostId,
                                     pendingRepair,
                                     isTransient);
        }
    }
}
