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
package org.apache.cassandra.io.sstable;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.StreamingHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.EstimatedHistogram;

/**
 * Metadata for a SSTable.
 * Metadata includes:
 *  - estimated row size histogram
 *  - estimated column count histogram
 *  - replay position
 *  - max column timestamp
 *  - max local deletion time
 *  - bloom filter fp chance
 *  - compression ratio
 *  - partitioner
 *  - generations of sstables from which this sstable was compacted, if any
 *  - tombstone drop time histogram
 *
 * An SSTableMetadata should be instantiated via the Collector, openFromDescriptor()
 * or createDefaultInstance()
 */
public class SSTableMetadata
{
    public static final double NO_BLOOM_FLITER_FP_CHANCE = -1.0;
    public static final double NO_COMPRESSION_RATIO = -1.0;
    public static final SSTableMetadataSerializer serializer = new SSTableMetadataSerializer();

    public final EstimatedHistogram estimatedRowSize;
    public final EstimatedHistogram estimatedColumnCount;
    public final ReplayPosition replayPosition;
    public final long minTimestamp;
    public final long maxTimestamp;
    public final int maxLocalDeletionTime;
    public final double bloomFilterFPChance;
    public final double compressionRatio;
    public final String partitioner;
    public final StreamingHistogram estimatedTombstoneDropTime;
    public final int sstableLevel;
    public final List<ByteBuffer> maxColumnNames;
    public final List<ByteBuffer> minColumnNames;

    private SSTableMetadata()
    {
        this(defaultRowSizeHistogram(),
             defaultColumnCountHistogram(),
             ReplayPosition.NONE,
             Long.MIN_VALUE,
             Long.MAX_VALUE,
             Integer.MAX_VALUE,
             NO_BLOOM_FLITER_FP_CHANCE,
             NO_COMPRESSION_RATIO,
             null,
             defaultTombstoneDropTimeHistogram(),
             0,
             Collections.<ByteBuffer>emptyList(),
             Collections.<ByteBuffer>emptyList());
    }

    private SSTableMetadata(EstimatedHistogram rowSizes,
                            EstimatedHistogram columnCounts,
                            ReplayPosition replayPosition,
                            long minTimestamp,
                            long maxTimestamp,
                            int maxLocalDeletionTime,
                            double bloomFilterFPChance,
                            double compressionRatio,
                            String partitioner,
                            StreamingHistogram estimatedTombstoneDropTime,
                            int sstableLevel,
                            List<ByteBuffer> minColumnNames,
                            List<ByteBuffer> maxColumnNames)
    {
        this.estimatedRowSize = rowSizes;
        this.estimatedColumnCount = columnCounts;
        this.replayPosition = replayPosition;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.maxLocalDeletionTime = maxLocalDeletionTime;
        this.bloomFilterFPChance = bloomFilterFPChance;
        this.compressionRatio = compressionRatio;
        this.partitioner = partitioner;
        this.estimatedTombstoneDropTime = estimatedTombstoneDropTime;
        this.sstableLevel = sstableLevel;
        this.minColumnNames = minColumnNames;
        this.maxColumnNames = maxColumnNames;
    }

    public static Collector createCollector(AbstractType<?> columnNameComparator)
    {
        return new Collector(columnNameComparator);
    }

    public static Collector createCollector(Collection<SSTableReader> sstables, AbstractType<?> columnNameComparator, int level)
    {
        Collector collector = new Collector(columnNameComparator);

        collector.replayPosition(ReplayPosition.getReplayPosition(sstables));
        collector.sstableLevel(level);
        // Get the max timestamp of the precompacted sstables
        // and adds generation of live ancestors
        for (SSTableReader sstable : sstables)
        {
            collector.addAncestor(sstable.descriptor.generation);
            for (Integer i : sstable.getAncestors())
            {
                if (new File(sstable.descriptor.withGeneration(i).filenameFor(Component.DATA)).exists())
                    collector.addAncestor(i);
            }
        }

        return collector;
    }

    /**
     * Used when updating sstablemetadata files with an sstable level
     * @param metadata
     * @param sstableLevel
     * @return
     */
    @Deprecated
    public static SSTableMetadata copyWithNewSSTableLevel(SSTableMetadata metadata, int sstableLevel)
    {
        return new SSTableMetadata(metadata.estimatedRowSize,
                                   metadata.estimatedColumnCount,
                                   metadata.replayPosition,
                                   metadata.minTimestamp,
                                   metadata.maxTimestamp,
                                   metadata.maxLocalDeletionTime,
                                   metadata.bloomFilterFPChance,
                                   metadata.compressionRatio,
                                   metadata.partitioner,
                                   metadata.estimatedTombstoneDropTime,
                                   sstableLevel,
                                   metadata.minColumnNames,
                                   metadata.maxColumnNames);

    }

    static EstimatedHistogram defaultColumnCountHistogram()
    {
        // EH of 114 can track a max value of 2395318855, i.e., > 2B columns
        return new EstimatedHistogram(114);
    }

    static EstimatedHistogram defaultRowSizeHistogram()
    {
        // EH of 150 can track a max value of 1697806495183, i.e., > 1.5PB
        return new EstimatedHistogram(150);
    }

    static StreamingHistogram defaultTombstoneDropTimeHistogram()
    {
        return new StreamingHistogram(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE);
    }

    /**
     * @param gcBefore
     * @return estimated droppable tombstone ratio at given gcBefore time.
     */
    public double getEstimatedDroppableTombstoneRatio(int gcBefore)
    {
        long estimatedColumnCount = this.estimatedColumnCount.mean() * this.estimatedColumnCount.count();
        if (estimatedColumnCount > 0)
        {
            double droppable = getDroppableTombstonesBefore(gcBefore);
            return droppable / estimatedColumnCount;
        }
        return 0.0f;
    }

    /**
     * Get the amount of droppable tombstones
     * @param gcBefore the gc time
     * @return amount of droppable tombstones
     */
    public double getDroppableTombstonesBefore(int gcBefore)
    {
        return estimatedTombstoneDropTime.sum(gcBefore);
    }

    public static class Collector
    {
        protected EstimatedHistogram estimatedRowSize = defaultRowSizeHistogram();
        protected EstimatedHistogram estimatedColumnCount = defaultColumnCountHistogram();
        protected ReplayPosition replayPosition = ReplayPosition.NONE;
        protected long minTimestamp = Long.MAX_VALUE;
        protected long maxTimestamp = Long.MIN_VALUE;
        protected int maxLocalDeletionTime = Integer.MIN_VALUE;
        protected double compressionRatio = NO_COMPRESSION_RATIO;
        protected Set<Integer> ancestors = new HashSet<Integer>();
        protected StreamingHistogram estimatedTombstoneDropTime = defaultTombstoneDropTimeHistogram();
        protected int sstableLevel;
        protected List<ByteBuffer> minColumnNames = Collections.emptyList();
        protected List<ByteBuffer> maxColumnNames = Collections.emptyList();
        private final AbstractType<?> columnNameComparator;

        private Collector(AbstractType<?> columnNameComparator)
        {
            this.columnNameComparator = columnNameComparator;
        }
        public void addRowSize(long rowSize)
        {
            estimatedRowSize.add(rowSize);
        }

        public void addColumnCount(long columnCount)
        {
            estimatedColumnCount.add(columnCount);
        }

        public void mergeTombstoneHistogram(StreamingHistogram histogram)
        {
            estimatedTombstoneDropTime.merge(histogram);
        }

        /**
         * Ratio is compressed/uncompressed and it is
         * if you have 1.x then compression isn't helping
         */
        public void addCompressionRatio(long compressed, long uncompressed)
        {
            compressionRatio = (double) compressed/uncompressed;
        }

        public void updateMinTimestamp(long potentialMin)
        {
            minTimestamp = Math.min(minTimestamp, potentialMin);
        }

        public void updateMaxTimestamp(long potentialMax)
        {
            maxTimestamp = Math.max(maxTimestamp, potentialMax);
        }

        public void updateMaxLocalDeletionTime(int maxLocalDeletionTime)
        {
            this.maxLocalDeletionTime = Math.max(this.maxLocalDeletionTime, maxLocalDeletionTime);
        }

        public SSTableMetadata finalizeMetadata(String partitioner, double bloomFilterFPChance)
        {
            return new SSTableMetadata(estimatedRowSize,
                                       estimatedColumnCount,
                                       replayPosition,
                                       minTimestamp,
                                       maxTimestamp,
                                       maxLocalDeletionTime,
                                       bloomFilterFPChance,
                                       compressionRatio,
                                       partitioner,
                                       estimatedTombstoneDropTime,
                                       sstableLevel,
                                       minColumnNames,
                                       maxColumnNames);
        }

        public Collector estimatedRowSize(EstimatedHistogram estimatedRowSize)
        {
            this.estimatedRowSize = estimatedRowSize;
            return this;
        }

        public Collector estimatedColumnCount(EstimatedHistogram estimatedColumnCount)
        {
            this.estimatedColumnCount = estimatedColumnCount;
            return this;
        }

        public Collector replayPosition(ReplayPosition replayPosition)
        {
            this.replayPosition = replayPosition;
            return this;
        }

        public Collector addAncestor(int generation)
        {
            this.ancestors.add(generation);
            return this;
        }

        void update(long size, ColumnStats stats)
        {
            updateMinTimestamp(stats.minTimestamp);
            /*
             * The max timestamp is not always collected here (more precisely, row.maxTimestamp() may return Long.MIN_VALUE),
             * to avoid deserializing an EchoedRow.
             * This is the reason why it is collected first when calling ColumnFamilyStore.createCompactionWriter
             * However, for old sstables without timestamp, we still want to update the timestamp (and we know
             * that in this case we will not use EchoedRow, since CompactionControler.needsDeserialize() will be true).
            */
            updateMaxTimestamp(stats.maxTimestamp);
            updateMaxLocalDeletionTime(stats.maxLocalDeletionTime);
            addRowSize(size);
            addColumnCount(stats.columnCount);
            mergeTombstoneHistogram(stats.tombstoneHistogram);
            updateMinColumnNames(stats.minColumnNames);
            updateMaxColumnNames(stats.maxColumnNames);
        }

        public Collector sstableLevel(int sstableLevel)
        {
            this.sstableLevel = sstableLevel;
            return this;
        }

        public Collector updateMinColumnNames(List<ByteBuffer> minColumnNames)
        {
            if (minColumnNames.size() > 0)
                this.minColumnNames = ColumnNameHelper.mergeMin(this.minColumnNames, minColumnNames, columnNameComparator);
            return this;
        }

        public Collector updateMaxColumnNames(List<ByteBuffer> maxColumnNames)
        {
            if (maxColumnNames.size() > 0)
                this.maxColumnNames = ColumnNameHelper.mergeMax(this.maxColumnNames, maxColumnNames, columnNameComparator);
            return this;
        }
    }

    public static class SSTableMetadataSerializer
    {
        private static final Logger logger = LoggerFactory.getLogger(SSTableMetadataSerializer.class);

        public void serialize(SSTableMetadata sstableStats, Set<Integer> ancestors, DataOutput out) throws IOException
        {
            assert sstableStats.partitioner != null;

            EstimatedHistogram.serializer.serialize(sstableStats.estimatedRowSize, out);
            EstimatedHistogram.serializer.serialize(sstableStats.estimatedColumnCount, out);
            ReplayPosition.serializer.serialize(sstableStats.replayPosition, out);
            out.writeLong(sstableStats.minTimestamp);
            out.writeLong(sstableStats.maxTimestamp);
            out.writeInt(sstableStats.maxLocalDeletionTime);
            out.writeDouble(sstableStats.bloomFilterFPChance);
            out.writeDouble(sstableStats.compressionRatio);
            out.writeUTF(sstableStats.partitioner);
            out.writeInt(ancestors.size());
            for (Integer g : ancestors)
                out.writeInt(g);
            StreamingHistogram.serializer.serialize(sstableStats.estimatedTombstoneDropTime, out);
            out.writeInt(sstableStats.sstableLevel);
            serializeMinMaxColumnNames(sstableStats.minColumnNames, sstableStats.maxColumnNames, out);
        }

        private void serializeMinMaxColumnNames(List<ByteBuffer> minColNames, List<ByteBuffer> maxColNames, DataOutput out) throws IOException
        {
            out.writeInt(minColNames.size());
            for (ByteBuffer columnName : minColNames)
                ByteBufferUtil.writeWithShortLength(columnName, out);
            out.writeInt(maxColNames.size());
            for (ByteBuffer columnName : maxColNames)
                ByteBufferUtil.writeWithShortLength(columnName, out);
        }
        /**
         * Used to serialize to an old version - needed to be able to update sstable level without a full compaction.
         *
         * @deprecated will be removed when it is assumed that the minimum upgrade-from-version is the version that this
         * patch made it into
         *
         * @param sstableStats
         * @param legacyDesc
         * @param out
         * @throws IOException
         */
        @Deprecated
        public void legacySerialize(SSTableMetadata sstableStats, Set<Integer> ancestors, Descriptor legacyDesc, DataOutput out) throws IOException
        {
            EstimatedHistogram.serializer.serialize(sstableStats.estimatedRowSize, out);
            EstimatedHistogram.serializer.serialize(sstableStats.estimatedColumnCount, out);
            ReplayPosition.serializer.serialize(sstableStats.replayPosition, out);
            out.writeLong(sstableStats.minTimestamp);
            out.writeLong(sstableStats.maxTimestamp);
            if (legacyDesc.version.tracksMaxLocalDeletionTime)
                out.writeInt(sstableStats.maxLocalDeletionTime);
            if (legacyDesc.version.hasBloomFilterFPChance)
                out.writeDouble(sstableStats.bloomFilterFPChance);
            out.writeDouble(sstableStats.compressionRatio);
            out.writeUTF(sstableStats.partitioner);
            out.writeInt(ancestors.size());
            for (Integer g : ancestors)
                out.writeInt(g);
            StreamingHistogram.serializer.serialize(sstableStats.estimatedTombstoneDropTime, out);
            out.writeInt(sstableStats.sstableLevel);
            if (legacyDesc.version.tracksMaxMinColumnNames)
                serializeMinMaxColumnNames(sstableStats.minColumnNames, sstableStats.maxColumnNames, out);
        }

        /**
         * deserializes the metadata
         *
         * returns a pair containing the part of the metadata meant to be kept-in memory and the part
         * that should not.
         *
         * @param descriptor the descriptor
         * @return a pair containing data that needs to be in memory and data that is potentially big and is not needed
         *         in memory
         * @throws IOException
         */
        public Pair<SSTableMetadata, Set<Integer>> deserialize(Descriptor descriptor) throws IOException
        {
            return deserialize(descriptor, true);
        }

        public Pair<SSTableMetadata, Set<Integer>> deserialize(Descriptor descriptor, boolean loadSSTableLevel) throws IOException
        {
            logger.debug("Load metadata for {}", descriptor);
            File statsFile = new File(descriptor.filenameFor(SSTable.COMPONENT_STATS));
            if (!statsFile.exists())
            {
                logger.debug("No sstable stats for {}", descriptor);
                return Pair.create(new SSTableMetadata(), Collections.<Integer>emptySet());
            }

            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(statsFile)));
            try
            {
                return deserialize(in, descriptor, loadSSTableLevel);
            }
            finally
            {
                FileUtils.closeQuietly(in);
            }
        }
        public Pair<SSTableMetadata, Set<Integer>> deserialize(DataInputStream in, Descriptor desc) throws IOException
        {
            return deserialize(in, desc, true);
        }

        public Pair<SSTableMetadata, Set<Integer>> deserialize(DataInputStream in, Descriptor desc, boolean loadSSTableLevel) throws IOException
        {
            EstimatedHistogram rowSizes = EstimatedHistogram.serializer.deserialize(in);
            EstimatedHistogram columnCounts = EstimatedHistogram.serializer.deserialize(in);
            ReplayPosition replayPosition = ReplayPosition.serializer.deserialize(in);
            long minTimestamp = in.readLong();
            long maxTimestamp = in.readLong();
            int maxLocalDeletionTime = desc.version.tracksMaxLocalDeletionTime ? in.readInt() : Integer.MAX_VALUE;
            double bloomFilterFPChance = desc.version.hasBloomFilterFPChance ? in.readDouble() : NO_BLOOM_FLITER_FP_CHANCE;
            double compressionRatio = in.readDouble();
            String partitioner = in.readUTF();
            int nbAncestors = in.readInt();
            Set<Integer> ancestors = new HashSet<Integer>(nbAncestors);
            for (int i = 0; i < nbAncestors; i++)
                ancestors.add(in.readInt());
            StreamingHistogram tombstoneHistogram = StreamingHistogram.serializer.deserialize(in);
            int sstableLevel = 0;

            if (loadSSTableLevel && in.available() > 0)
                sstableLevel = in.readInt();

            List<ByteBuffer> minColumnNames;
            List<ByteBuffer> maxColumnNames;
            if (desc.version.tracksMaxMinColumnNames)
            {
                int colCount = in.readInt();
                minColumnNames = new ArrayList<ByteBuffer>(colCount);
                for (int i = 0; i < colCount; i++)
                {
                    minColumnNames.add(ByteBufferUtil.readWithShortLength(in));
                }
                colCount = in.readInt();
                maxColumnNames = new ArrayList<ByteBuffer>(colCount);
                for (int i = 0; i < colCount; i++)
                {
                    maxColumnNames.add(ByteBufferUtil.readWithShortLength(in));
                }
            }
            else
            {
                minColumnNames = Collections.emptyList();
                maxColumnNames = Collections.emptyList();
            }
            return Pair.create(new SSTableMetadata(rowSizes,
                                       columnCounts,
                                       replayPosition,
                                       minTimestamp,
                                       maxTimestamp,
                                       maxLocalDeletionTime,
                                       bloomFilterFPChance,
                                       compressionRatio,
                                       partitioner,
                                       tombstoneHistogram,
                                       sstableLevel,
                                       minColumnNames,
                                       maxColumnNames), ancestors);
        }
    }
}
