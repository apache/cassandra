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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.io.sstable.ColumnNameHelper;
import org.apache.cassandra.io.sstable.ColumnStats;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
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
                                 true,
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
    protected boolean hasLegacyCounterShards = false;

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
                if (new File(sstable.descriptor.withGeneration(i).filenameFor(Component.DATA)).exists())
                    addAncestor(i);
        }
    }

    public MetadataCollector addKey(ByteBuffer key)
    {
        long hashed = MurmurHash.hash2_64(key, key.position(), key.remaining(), 0);
        cardinality.offerHashed(hashed);
        return this;
    }

    public MetadataCollector addRowSize(long rowSize)
    {
        estimatedRowSize.add(rowSize);
        return this;
    }

    public MetadataCollector addColumnCount(long columnCount)
    {
        estimatedColumnCount.add(columnCount);
        return this;
    }

    public MetadataCollector mergeTombstoneHistogram(StreamingHistogram histogram)
    {
        estimatedTombstoneDropTime.merge(histogram);
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

    public MetadataCollector updateMinTimestamp(long potentialMin)
    {
        minTimestamp = Math.min(minTimestamp, potentialMin);
        return this;
    }

    public MetadataCollector updateMaxTimestamp(long potentialMax)
    {
        maxTimestamp = Math.max(maxTimestamp, potentialMax);
        return this;
    }

    public MetadataCollector updateMaxLocalDeletionTime(int maxLocalDeletionTime)
    {
        this.maxLocalDeletionTime = Math.max(this.maxLocalDeletionTime, maxLocalDeletionTime);
        return this;
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

    public MetadataCollector updateHasLegacyCounterShards(boolean hasLegacyCounterShards)
    {
        this.hasLegacyCounterShards = this.hasLegacyCounterShards || hasLegacyCounterShards;
        return this;
    }

    public MetadataCollector update(long rowSize, ColumnStats stats)
    {
        updateMinTimestamp(stats.minTimestamp);
        updateMaxTimestamp(stats.maxTimestamp);
        updateMaxLocalDeletionTime(stats.maxLocalDeletionTime);
        addRowSize(rowSize);
        addColumnCount(stats.columnCount);
        mergeTombstoneHistogram(stats.tombstoneHistogram);
        updateMinColumnNames(stats.minColumnNames);
        updateMaxColumnNames(stats.maxColumnNames);
        updateHasLegacyCounterShards(stats.hasLegacyCounterShards);
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
                                                             ImmutableList.copyOf(minColumnNames),
                                                             ImmutableList.copyOf(maxColumnNames),
                                                             hasLegacyCounterShards,
                                                             repairedAt));
        components.put(MetadataType.COMPACTION, new CompactionMetadata(ancestors, cardinality));
        return components;
    }
}
