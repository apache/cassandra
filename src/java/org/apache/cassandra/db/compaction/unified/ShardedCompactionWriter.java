/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction.unified;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.ArenaSelector;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A {@link CompactionAwareWriter} that splits the output sstable at the partition boundaries of the compaction
 * shards used by {@link org.apache.cassandra.db.compaction.UnifiedCompactionStrategy} as long as the size of
 * the sstable so far is sufficiently large.
 */
public class ShardedCompactionWriter extends CompactionAwareWriter
{
    protected final static Logger logger = LoggerFactory.getLogger(ShardedCompactionWriter.class);

    private final long minSstableSizeInBytes;
    private final List<PartitionPosition> boundaries;
    private final double overwriteRatio;

    private int currentIndex;

    public ShardedCompactionWriter(ColumnFamilyStore cfs,
                                   Directories directories,
                                   LifecycleTransaction txn,
                                   Set<SSTableReader> nonExpiredSSTables,
                                   boolean keepOriginals,
                                   long minSstableSizeInBytes,
                                   List<PartitionPosition> boundaries)
    {
        super(cfs, directories, txn, nonExpiredSSTables, keepOriginals);

        this.minSstableSizeInBytes = minSstableSizeInBytes;
        this.boundaries = boundaries;
        this.currentIndex = 0;
        long totalKeyCount = nonExpiredSSTables.stream()
                                               .mapToLong(SSTableReader::estimatedKeys)
                                               .sum();
        this.overwriteRatio = 1.0 * SSTableReader.getApproximateKeyCount(nonExpiredSSTables) / totalKeyCount;
    }

    @Override
    protected boolean shouldSwitchWriterInCurrentLocation(DecoratedKey key)
    {
        boolean boundaryCrossed = false;
        /*
        The comparison to detect a boundary is costly, but if we only do this when the size is above the threshold,
        we may detect a boundary change in the middle of a shard and split sstables at the wrong place.
         */
        while (currentIndex < boundaries.size() && key.compareTo(boundaries.get(currentIndex)) >= 0)
        {
            currentIndex++;
            boundaryCrossed = true;
        }

        if (boundaryCrossed && sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten() >= minSstableSizeInBytes)
        {
            logger.debug("Switching writer at boundary {}/{} index {}, with size {} for {}.{}",
                         key.getToken(), boundaries.get(currentIndex-1), currentIndex-1,
                         FBUtilities.prettyPrintMemory(sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten()),
                         cfs.getKeyspaceName(), cfs.getTableName());
            return true;
        }

        return false;
    }

    @Override
    @SuppressWarnings("resource")
    protected SSTableWriter sstableWriter(Directories.DataDirectory directory, PartitionPosition diskBoundary)
    {
        while (diskBoundary != null && currentIndex < boundaries.size() && diskBoundary.compareTo(boundaries.get(currentIndex)) < 0)
            currentIndex++;

        return SSTableWriter.create(cfs.newSSTableDescriptor(getDirectories().getLocationForDisk(directory)),
                                    shardAdjustedKeyCount(currentIndex, boundaries, minSstableSizeInBytes, nonExpiredSSTables, overwriteRatio),
                                    minRepairedAt,
                                    pendingRepair,
                                    isTransient,
                                    cfs.metadata,
                                    new MetadataCollector(txn.originals(), cfs.metadata().comparator, 0),
                                    SerializationHeader.make(cfs.metadata(), nonExpiredSSTables),
                                    cfs.indexManager.listIndexGroups(),
                                    txn);
    }

    private long shardAdjustedKeyCount(int shardIdx,
                                       List<PartitionPosition> boundaries,
                                       long minSstableSizeInBytes,
                                       Set<SSTableReader> sstables,
                                       double overwriteRatio)
    {
        long shardAdjustedSize = 0;
        long shardAdjustedKeyCount = 0;
        for (int i = shardIdx; i < boundaries.size(); i++)
        {
            Set<SSTableReader> sstablesForShard = ArenaSelector.sstablesFor(i, boundaries, sstables);
            for (SSTableReader sstable : sstablesForShard)
            {
                int shardsSpanned = ArenaSelector.shardsSpanned(sstable, boundaries);
                // calculating manually instead of calling ArenaSelector.shardAdjustedSize to save 1 call to ArenaSelector.shardsSpanned
                shardAdjustedSize += sstable.onDiskLength() / shardsSpanned;
                shardAdjustedKeyCount += sstable.estimatedKeys() / shardsSpanned;
            }

            if (shardAdjustedSize > minSstableSizeInBytes)
                break;
        }

        return Math.round(shardAdjustedKeyCount * overwriteRatio);
    }
}