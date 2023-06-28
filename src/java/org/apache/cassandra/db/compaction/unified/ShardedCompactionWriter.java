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

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.CompactionRealm;
import org.apache.cassandra.db.compaction.ShardTracker;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A {@link CompactionAwareWriter} that splits the output sstable at the partition boundaries of the compaction
 * shards used by {@link org.apache.cassandra.db.compaction.UnifiedCompactionStrategy}.
 */
public class ShardedCompactionWriter extends CompactionAwareWriter
{
    protected final static Logger logger = LoggerFactory.getLogger(ShardedCompactionWriter.class);

    private final double uniqueKeyRatio;

    private final ShardTracker boundaries;

    public ShardedCompactionWriter(CompactionRealm realm,
                                   Directories directories,
                                   LifecycleTransaction txn,
                                   Set<SSTableReader> nonExpiredSSTables,
                                   boolean keepOriginals,
                                   ShardTracker boundaries)
    {
        super(realm, directories, txn, nonExpiredSSTables, keepOriginals);

        this.boundaries = boundaries;
        long totalKeyCount = nonExpiredSSTables.stream()
                                               .mapToLong(SSTableReader::estimatedKeys)
                                               .sum();
        this.uniqueKeyRatio = 1.0 * SSTableReader.getApproximateKeyCount(nonExpiredSSTables) / totalKeyCount;
    }

    @Override
    protected boolean shouldSwitchWriterInCurrentLocation(DecoratedKey key)
    {
        // If we have written anything and cross a shard boundary, switch to a new writer. We use the uncompressed
        // file pointer here because there may be writes that are not yet reflected in the on-disk size, and we want
        // to split as soon as there is content, regardless how small.
        final long uncompressedBytesWritten = sstableWriter.currentWriter().getFilePointer();
        if (boundaries.advanceTo(key.getToken()) && uncompressedBytesWritten > 0)
        {
            logger.debug("Switching writer at boundary {}/{} index {}, with uncompressed size {} for {}.{}",
                         key.getToken(), boundaries.shardStart(),
                         boundaries.shardIndex(),
                         FBUtilities.prettyPrintMemory(uncompressedBytesWritten),
                         realm.getKeyspaceName(), realm.getTableName());
            return true;
        }

        return false;
    }

    @Override
    @SuppressWarnings("resource")
    protected SSTableWriter sstableWriter(Directories.DataDirectory directory, Token nextKey)
    {
        if (nextKey != null)
            boundaries.advanceTo(nextKey);

        return SSTableWriter.create(realm.newSSTableDescriptor(getDirectories().getLocationForDisk(directory)),
                                    shardAdjustedKeyCount(boundaries, nonExpiredSSTables, uniqueKeyRatio),
                                    minRepairedAt,
                                    pendingRepair,
                                    isTransient,
                                    realm.metadataRef(),
                                    new MetadataCollector(txn.originals(), realm.metadata().comparator, 0),
                                    SerializationHeader.make(realm.metadata(), nonExpiredSSTables),
                                    realm.getIndexManager().listIndexGroups(),
                                    txn);
    }

    private static long shardAdjustedKeyCount(ShardTracker boundaries,
                                              Set<SSTableReader> sstables,
                                              double survivalRatio)
    {
        // Note: computationally non-trivial; can be optimized if we save start/stop shards and size per table.
        return Math.round(boundaries.shardAdjustedKeyCount(sstables) * survivalRatio);
    }

    @Override
    protected void doPrepare()
    {
        sstableWriter.forEachWriter(boundaries::applyTokenSpaceCoverage);
        super.doPrepare();
    }
}