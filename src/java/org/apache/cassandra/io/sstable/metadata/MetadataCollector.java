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
import java.nio.ByteBuffer;
import java.util.*;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.collect.Maps;

import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.StreamingHistogram;

public class MetadataCollector
{
    public static final double NO_COMPRESSION_RATIO = -1.0;

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

    public static StatsMetadata defaultStatsMetadata()
    {
        return new StatsMetadata(defaultRowSizeHistogram(),
                                 defaultColumnCountHistogram(),
                                 ReplayPosition.NONE,
                                 Long.MIN_VALUE,
                                 Long.MAX_VALUE,
                                 Integer.MAX_VALUE,
                                 NO_COMPRESSION_RATIO,
                                 defaultTombstoneDropTimeHistogram(),
                                 0,
                                 Collections.<ByteBuffer>emptyList(),
                                 Collections.<ByteBuffer>emptyList(),
                                 ActiveRepairService.UNREPAIRED_SSTABLE);
    }

    protected EstimatedHistogram estimatedRowSize = defaultRowSizeHistogram();
    protected EstimatedHistogram estimatedColumnCount = defaultColumnCountHistogram();
    protected ReplayPosition replayPosition = ReplayPosition.NONE;
    protected long minTimestamp = Long.MAX_VALUE;
    protected long maxTimestamp = Long.MIN_VALUE;
    protected int maxLocalDeletionTime = Integer.MIN_VALUE;
    protected double compressionRatio = NO_COMPRESSION_RATIO;
    protected Set<Integer> ancestors = new HashSet<>();
    protected StreamingHistogram estimatedTombstoneDropTime = defaultTombstoneDropTimeHistogram();
    protected int sstableLevel;
    protected List<ByteBuffer> minColumnNames = Collections.emptyList();
    protected List<ByteBuffer> maxColumnNames = Collections.emptyList();
    /**
     * Default cardinality estimation method is to use HyperLogLog++.
     * Parameter here(p=13, sp=25) should give reasonable estimation
     * while lowering bytes required to hold information.
     * See CASSANDRA-5906 for detail.
     */
    protected ICardinality cardinality = new HyperLogLogPlus(13, 25);
    private final CellNameType columnNameComparator;

    public MetadataCollector(CellNameType columnNameComparator)
    {
        this.columnNameComparator = columnNameComparator;
    }

    public MetadataCollector(Collection<SSTableReader> sstables, CellNameType columnNameComparator, int level)
    {
        this(columnNameComparator);

        replayPosition(ReplayPosition.getReplayPosition(sstables));
        sstableLevel(level);
        // Get the max timestamp of the precompacted sstables
        // and adds generation of live ancestors
        for (SSTableReader sstable : sstables)
        {
            addAncestor(sstable.descriptor.generation);
            for (Integer i : sstable.getAncestors())
            {
                if (new File(sstable.descriptor.withGeneration(i).filenameFor(Component.DATA)).exists())
                    addAncestor(i);
            }
        }
    }

    public void addKey(ByteBuffer key)
    {
        long hashed = MurmurHash.hash2_64(key, key.position(), key.remaining(), 0);
        cardinality.offerHashed(hashed);
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

    public MetadataCollector estimatedRowSize(EstimatedHistogram estimatedRowSize)
    {
        this.estimatedRowSize = estimatedRowSize;
        return this;
    }

    public MetadataCollector estimatedColumnCount(EstimatedHistogram estimatedColumnCount)
    {
        this.estimatedColumnCount = estimatedColumnCount;
        return this;
    }

    public MetadataCollector replayPosition(ReplayPosition replayPosition)
    {
        this.replayPosition = replayPosition;
        return this;
    }

    public MetadataCollector addAncestor(int generation)
    {
        this.ancestors.add(generation);
        return this;
    }

    public void update(long size, ColumnStats stats)
    {
        updateMinTimestamp(stats.minTimestamp);
        updateMaxTimestamp(stats.maxTimestamp);
        updateMaxLocalDeletionTime(stats.maxLocalDeletionTime);
        addRowSize(size);
        addColumnCount(stats.columnCount);
        mergeTombstoneHistogram(stats.tombstoneHistogram);
        updateMinColumnNames(stats.minColumnNames);
        updateMaxColumnNames(stats.maxColumnNames);
    }

    public MetadataCollector sstableLevel(int sstableLevel)
    {
        this.sstableLevel = sstableLevel;
        return this;
    }

    public MetadataCollector updateMinColumnNames(List<ByteBuffer> minColumnNames)
    {
        if (minColumnNames.size() > 0)
            this.minColumnNames = ColumnNameHelper.mergeMin(this.minColumnNames, minColumnNames, columnNameComparator);
        return this;
    }

    public MetadataCollector updateMaxColumnNames(List<ByteBuffer> maxColumnNames)
    {
        if (maxColumnNames.size() > 0)
            this.maxColumnNames = ColumnNameHelper.mergeMax(this.maxColumnNames, maxColumnNames, columnNameComparator);
        return this;
    }

    public Map<MetadataType, MetadataComponent> finalizeMetadata(String partitioner, double bloomFilterFPChance, long repairedAt)
    {
        Map<MetadataType, MetadataComponent> components = Maps.newHashMap();
        components.put(MetadataType.VALIDATION, new ValidationMetadata(partitioner, bloomFilterFPChance));
        components.put(MetadataType.STATS, new StatsMetadata(estimatedRowSize,
                                                             estimatedColumnCount,
                                                             replayPosition,
                                                             minTimestamp,
                                                             maxTimestamp,
                                                             maxLocalDeletionTime,
                                                             compressionRatio,
                                                             estimatedTombstoneDropTime,
                                                             sstableLevel,
                                                             minColumnNames,
                                                             maxColumnNames,
                                                             repairedAt));
        components.put(MetadataType.COMPACTION, new CompactionMetadata(ancestors, cardinality));
        return components;
    }

}
