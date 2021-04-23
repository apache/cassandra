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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.UnsupportedSSTableException;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.serializers.AbstractTypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.streamhist.TombstoneHistogram;

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
    public final Slice coveredClustering;
    public final boolean hasLegacyCounterShards;
    /**
     * This boolean is used as an approximation of whether a given key can be guaranteed not to have partition
     * deletions in this sstable. Obviously, this is pretty imprecise: a single partition deletion in the sstable
     * means we have to assume _any_ key may have a partition deletion. This is still likely useful as workloads that
     * does not use partition level deletions, or only very rarely, are probably not that rare.
     *
     * TODO we could replace this by a small bloom-filter instead; the only downside being that we'd have to care about
     *  the size of this bloom filters not getting out of hands, and it's a tiny bit unclear if it's worth the added
     *  complexity.
     */
    public final boolean hasPartitionLevelDeletions;
    public final long repairedAt;
    public final long totalColumnsSet;
    public final long totalRows;
    public final UUID originatingHostId;
    public final UUID pendingRepair;
    public final boolean isTransient;
    // just holds the current encoding stats to avoid allocating - it is not serialized
    public final EncodingStats encodingStats;

    // Used to serialize min/max clustering. Can be null if the metadata was deserialized from a legacy version
    private final List<AbstractType<?>> clusteringTypes;

    private final ImmutableMap<ByteBuffer, Integer> maxColumnValueLengths;

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
                         List<AbstractType<?>> clusteringTypes,
                         Slice coveredClustering,
                         boolean hasLegacyCounterShards,
                         boolean hasPartitionLevelDeletions,
                         long repairedAt,
                         long totalColumnsSet,
                         long totalRows,
                         UUID originatingHostId,
                         UUID pendingRepair,
                         boolean isTransient,
                         Map<ByteBuffer, Integer> maxColumnValueLengths)
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
        this.clusteringTypes = clusteringTypes;
        this.coveredClustering = coveredClustering;
        this.hasLegacyCounterShards = hasLegacyCounterShards;
        this.hasPartitionLevelDeletions = hasPartitionLevelDeletions;
        this.repairedAt = repairedAt;
        this.totalColumnsSet = totalColumnsSet;
        this.totalRows = totalRows;
        this.originatingHostId = originatingHostId;
        this.pendingRepair = pendingRepair;
        this.isTransient = isTransient;
        this.encodingStats = new EncodingStats(minTimestamp, minLocalDeletionTime, minTTL);
        this.maxColumnValueLengths = ImmutableMap.copyOf(maxColumnValueLengths);
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
                                 clusteringTypes,
                                 coveredClustering,
                                 hasLegacyCounterShards,
                                 hasPartitionLevelDeletions,
                                 repairedAt,
                                 totalColumnsSet,
                                 totalRows,
                                 originatingHostId,
                                 pendingRepair,
                                 isTransient,
                                 maxColumnValueLengths);
    }

    public StatsMetadata mutateRepairedMetadata(long newRepairedAt, UUID newPendingRepair, boolean newIsTransient)
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
                                 clusteringTypes,
                                 coveredClustering,
                                 hasLegacyCounterShards,
                                 hasPartitionLevelDeletions,
                                 newRepairedAt,
                                 totalColumnsSet,
                                 totalRows,
                                 originatingHostId,
                                 newPendingRepair,
                                 newIsTransient,
                                 maxColumnValueLengths);
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
                       .append(coveredClustering, that.coveredClustering)
                       .append(hasLegacyCounterShards, that.hasLegacyCounterShards)
                       .append(hasPartitionLevelDeletions, that.hasPartitionLevelDeletions)
                       .append(totalColumnsSet, that.totalColumnsSet)
                       .append(totalRows, that.totalRows)
                       .append(originatingHostId, that.originatingHostId)
                       .append(pendingRepair, that.pendingRepair)
                       .append(maxColumnValueLengths, that.maxColumnValueLengths)
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
                       .append(coveredClustering)
                       .append(hasLegacyCounterShards)
                       .append(hasPartitionLevelDeletions)
                       .append(totalColumnsSet)
                       .append(totalRows)
                       .append(originatingHostId)
                       .append(pendingRepair)
                       .append(maxColumnValueLengths)
                       .build();
    }

    public static class StatsMetadataSerializer implements IMetadataComponentSerializer<StatsMetadata>
    {
        private static final Logger logger = LoggerFactory.getLogger(StatsMetadataSerializer.class);

        private final AbstractTypeSerializer typeSerializer = new AbstractTypeSerializer();

        public int serializedSize(Version version, StatsMetadata component) throws IOException
        {
            int size = 0;
            size += EstimatedHistogram.serializer.serializedSize(component.estimatedPartitionSize);
            size += EstimatedHistogram.serializer.serializedSize(component.estimatedCellPerPartitionCount);
            size += CommitLogPosition.serializer.serializedSize(component.commitLogIntervals.upperBound().orElse(CommitLogPosition.NONE));
            size += 8 + 8 + 4 + 4 + 4 + 4 + 8 + 8; // mix/max timestamp(long), min/maxLocalDeletionTime(int), min/max TTL, compressionRatio(double), repairedAt (long)
            size += TombstoneHistogram.serializer.serializedSize(component.estimatedTombstoneDropTime);
            size += TypeSizes.sizeof(component.sstableLevel);

            if (version.hasImprovedMinMax())
            {
                size += typeSerializer.serializedListSize(component.clusteringTypes);
                size += Slice.serializer.serializedSize(component.coveredClustering,
                                                        version.correspondingMessagingVersion(),
                                                        component.clusteringTypes);
            }
            else
            {
                // min column names
                size += 4;
                ClusteringBound<?> minClusteringValues = component.coveredClustering.start();
                size += minClusteringValues.size() * 2 /* short length */ + minClusteringValues.dataSize();
                // max column names
                size += 4;
                ClusteringBound<?> maxClusteringValues = component.coveredClustering.end();
                size += maxClusteringValues.size() * 2 /* short length */ + maxClusteringValues.dataSize();
            }

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
                    size += UUIDSerializer.serializer.serializedSize(component.pendingRepair, 0);
            }

            // we do not have zero copy metadata
            if (version.hasZeroCopyMetadata())
            {
                size += 1;
            }

            // we do not have node sync metadata
            if (version.hasIncrementalNodeSyncMetadata())
            {
                size += Long.BYTES;
            }

            if (version.hasMaxColumnValueLengths())
            {
                size += 4; // num columns
                for (Map.Entry<ByteBuffer, Integer> entry : component.maxColumnValueLengths.entrySet())
                    size += ByteBufferUtil.serializedSizeWithVIntLength(entry.getKey()) + 4; // column name, max value length
            }

            if (version.hasIsTransient())
            {
                size += TypeSizes.sizeof(component.isTransient);
            }

            if (version.hasPartitionLevelDeletionsPresenceMarker())
                size += TypeSizes.sizeof(component.hasPartitionLevelDeletions);

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

            if (version.hasImprovedMinMax())
            {
                assert component.clusteringTypes != null;
                typeSerializer.serializeList(component.clusteringTypes, out);
                Slice.serializer.serialize(component.coveredClustering,
                                           out,
                                           version.correspondingMessagingVersion(),
                                           component.clusteringTypes);
            }
            else
            {
                ClusteringBound<?> minClusteringValues = component.coveredClustering.start();
                out.writeInt(minClusteringValues.size());
                for (ByteBuffer value : minClusteringValues.getBufferArray())
                    ByteBufferUtil.writeWithShortLength(value, out);
                ClusteringBound<?> maxClusteringValues = component.coveredClustering.end();
                out.writeInt(maxClusteringValues.size());
                for (ByteBuffer value : maxClusteringValues.getBufferArray())
                    ByteBufferUtil.writeWithShortLength(value, out);
            }

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
                    UUIDSerializer.serializer.serialize(component.pendingRepair, out, 0);
                }
                else
                {
                    out.writeByte(0);
                }
            }

            // we do not have zero copy metadata
            if (version.hasZeroCopyMetadata())
            {
                out.writeByte(0);
            }

            // we do not have node sync metadata
            if (version.hasIncrementalNodeSyncMetadata())
            {
                out.writeLong(Long.MAX_VALUE);
            }

            // left for being able to import DSE sstables, not used
            if (version.hasMaxColumnValueLengths())
            {
                out.writeInt(component.maxColumnValueLengths.size());
                for (Map.Entry<ByteBuffer, Integer> entry : component.maxColumnValueLengths.entrySet())
                {
                    ByteBufferUtil.writeWithVIntLength(entry.getKey(), out);
                    out.writeInt(entry.getValue());
                }
            }

            if (version.hasIsTransient())
            {
                out.writeBoolean(component.isTransient);
            }

            if (version.hasPartitionLevelDeletionsPresenceMarker())
                out.writeBoolean(component.hasPartitionLevelDeletions);

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

            List<AbstractType<?>> clusteringTypes = null;
            Slice coveredClustering = Slice.ALL;
            if (version.hasImprovedMinMax())
            {
                clusteringTypes = typeSerializer.deserializeList(in);
                coveredClustering = Slice.serializer.deserialize(in, version.correspondingMessagingVersion(), clusteringTypes);
            }
            else
            {
                // for legacy sstables, we skip deserializing the min and max clustering value
                // to prevent erroneously excluding sstables from reads (see CASSANDRA-14861)
                int colCount = in.readInt();
                ByteBuffer[] minClusteringValues = new ByteBuffer[colCount];
                for (int i = 0; i < colCount; i++)
                    minClusteringValues[i] = ByteBufferUtil.readWithShortLength(in);

                colCount = in.readInt();
                ByteBuffer[] maxClusteringValues = new ByteBuffer[colCount];
                for (int i = 0; i < colCount; i++)
                    maxClusteringValues[i] = ByteBufferUtil.readWithShortLength(in);

                if (version.hasAccurateMinMax())
                    coveredClustering = Slice.make(BufferClusteringBound.inclusiveStartOf(minClusteringValues),
                                                   BufferClusteringBound.inclusiveEndOf(maxClusteringValues));

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
                commitLogIntervals = new IntervalSet<>(commitLogLowerBound, commitLogUpperBound);

            UUID pendingRepair = null;
            if (version.hasPendingRepair() && in.readByte() != 0)
            {
                pendingRepair = UUIDSerializer.serializer.deserialize(in, 0);
            }

            if (version.hasZeroCopyMetadata() && in.readByte() != 0)
            {
                throw new UnsupportedSSTableException(String.format("Found DSE zero copy metadata in %s. " +
                                                                    "Files copied over using partial zero-copy " +
                                                                    "streaming in DSE are not currently supported.", in),
                                                      null,
                                                      in instanceof FileDataInput ? new File(((FileDataInput) in).getPath()) : null);
            }

            if (version.hasIncrementalNodeSyncMetadata())
            {
                logger.warn("Ignoring incremental node sync metadata from {} as it is not supported", in);
                in.readLong();
            }

            // left for being able to import DSE sstables, not used
            final Map<ByteBuffer, Integer> maxColumnValueLengths;
            if (version.hasMaxColumnValueLengths())
            {
                int colCount = in.readInt();
                ImmutableMap.Builder<ByteBuffer, Integer> builder = ImmutableMap.builderWithExpectedSize(colCount);

                for (int i = 0; i < colCount; i++)
                    builder.put(ByteBufferUtil.readWithVIntLength(in), in.readInt());
                maxColumnValueLengths = builder.build();
            }
            else
            {
                maxColumnValueLengths = Collections.emptyMap();
            }

            boolean isTransient = version.hasIsTransient() && in.readBoolean();

            // If not recorded, the only time we can guarantee there is no partition level deletion is if there is no
            // deletion at all. Otherwise, we have to assume there may be some.
            boolean hasPartitionLevelDeletions = version.hasPartitionLevelDeletionsPresenceMarker()
                                                 ? in.readBoolean()
                                                 : minLocalDeletionTime != Cell.NO_DELETION_TIME;

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
                                     clusteringTypes,
                                     coveredClustering,
                                     hasLegacyCounterShards,
                                     hasPartitionLevelDeletions,
                                     repairedAt,
                                     totalColumnsSet,
                                     totalRows,
                                     originatingHostId,
                                     pendingRepair,
                                     isTransient,
                                     maxColumnValueLengths);
        }
    }
}
