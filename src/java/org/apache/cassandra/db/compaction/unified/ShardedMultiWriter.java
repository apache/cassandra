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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.SimpleSSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A {@link SSTableMultiWriter} that splits the output sstable at the partition boundaries of the compaction
 * shards used by {@link org.apache.cassandra.db.compaction.UnifiedCompactionStrategy} as long as the size of
 * the sstable so far is sufficiently large.
 * <p/>
 * This is class is similar to {@link ShardedMultiWriter} but for flushing. Unfortunately
 * we currently have 2 separate writers hierarchy that are not compatible and so we must
 * duplicate the functionality of splitting sstables over compaction shards if they have
 * reached a minimum size.
 */
public class ShardedMultiWriter implements SSTableMultiWriter
{
    protected final static Logger logger = LoggerFactory.getLogger(ShardedMultiWriter.class);

    private final ColumnFamilyStore cfs;
    private final Descriptor descriptor;
    private final long keyCount;
    private final long repairedAt;
    private final UUID pendingRepair;
    private final boolean isTransient;
    private final MetadataCollector meta;
    private final SerializationHeader header;
    private final Collection<Index.Group> indexGroups;
    private final LifecycleNewTracker lifecycleNewTracker;
    private final long minSstableSizeInBytes;
    private final List<PartitionPosition> boundaries;
    private final SSTableMultiWriter[] writers;
    private final int estimatedSSTables;
    private int currentBoundary;
    private int currentWriter;

    public ShardedMultiWriter(ColumnFamilyStore cfs,
                              Descriptor descriptor,
                              long keyCount,
                              long repairedAt,
                              UUID pendingRepair,
                              boolean isTransient,
                              MetadataCollector meta,
                              SerializationHeader header,
                              Collection<Index.Group> indexGroups,
                              LifecycleNewTracker lifecycleNewTracker,
                              long minSstableSizeInBytes,
                              List<PartitionPosition> boundaries)
    {
        this.cfs = cfs;
        this.descriptor = descriptor;
        this.keyCount = keyCount;
        this.repairedAt = repairedAt;
        this.pendingRepair = pendingRepair;
        this.isTransient = isTransient;
        this.meta = meta;
        this.header = header;
        this.indexGroups = indexGroups;
        this.lifecycleNewTracker = lifecycleNewTracker;
        this.minSstableSizeInBytes = minSstableSizeInBytes;
        this.boundaries = boundaries;
        this.writers = new SSTableMultiWriter[boundaries.size()];
        this.estimatedSSTables = (int) Math.max(1, Math.ceil(cfs.getFlushSizeOnDisk() / minSstableSizeInBytes));

        this.currentBoundary = 0;
        this.currentWriter = 0;
        this.writers[currentWriter] = createWriter(descriptor);
    }

    private SSTableMultiWriter createWriter()
    {
        Descriptor newDesc = cfs.newSSTableDescriptor(descriptor.directory);
        return createWriter(newDesc);
    }

    private SSTableMultiWriter createWriter(Descriptor desc)
    {
        return SimpleSSTableMultiWriter.create(desc,
                                               forSplittingKeysBy(estimatedSSTables),
                                               repairedAt,
                                               pendingRepair,
                                               isTransient,
                                               cfs.metadata,
                                               meta,
                                               header,
                                               indexGroups,
                                               lifecycleNewTracker);
    }

    private long forSplittingKeysBy(long splits) {
        return splits <= 1 ? keyCount : keyCount / splits;
    }

    @Override
    public boolean append(UnfilteredRowIterator partition)
    {
        DecoratedKey key = partition.partitionKey();

        boolean boundaryCrossed = false;
        /*
        The comparison to detect a boundary is costly, but if we only do this when the size is above the threshold,
        we may detect a boundary change in the middle of a shard and split sstables at the wrong place.
         */
        while (currentBoundary < boundaries.size() && key.compareTo(boundaries.get(currentBoundary)) >= 0)
        {
            currentBoundary++;
            if (!boundaryCrossed)
                boundaryCrossed = true;
        }

        if (boundaryCrossed && writers[currentWriter].getOnDiskBytesWritten() >= minSstableSizeInBytes)
        {
            logger.debug("Switching writer at boundary {}/{} index {}/{}, with size {} for {}.{}",
                         key.getToken(), boundaries.get(currentBoundary-1), currentBoundary-1, currentWriter,
                         FBUtilities.prettyPrintMemory(writers[currentWriter].getBytesWritten()),
                         cfs.getKeyspaceName(), cfs.getTableName());

            writers[++currentWriter] = createWriter();
        }

        return writers[currentWriter].append(partition);
    }

    @Override
    public Collection<SSTableReader> finish(long repairedAt, long maxDataAge, boolean openResult)
    {
        List<SSTableReader> sstables = new ArrayList<>(writers.length);
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                sstables.addAll(writer.finish(repairedAt, maxDataAge, openResult));
        return sstables;
    }

    @Override
    public Collection<SSTableReader> finish(boolean openResult)
    {
        List<SSTableReader> sstables = new ArrayList<>(writers.length);
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                sstables.addAll(writer.finish(openResult));
        return sstables;
    }

    @Override
    public Collection<SSTableReader> finished()
    {
        List<SSTableReader> sstables = new ArrayList<>(writers.length);
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                sstables.addAll(writer.finished());
        return sstables;
    }

    @Override
    public SSTableMultiWriter setOpenResult(boolean openResult)
    {
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                writer.setOpenResult(openResult);
        return this;
    }

    @Override
    public String getFilename()
    {
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                return writer.getFilename();
        return "";
    }

    @Override
    public long getBytesWritten()
    {
        long bytesWritten = 0;
        for (int i = 0; i <= currentWriter; ++i)
            bytesWritten += writers[i].getBytesWritten();
        return bytesWritten;
    }

    @Override
    public long getOnDiskBytesWritten()
    {
        long bytesWritten = 0;
        for (int i = 0; i <= currentWriter; ++i)
            bytesWritten += writers[i].getOnDiskBytesWritten();
        return bytesWritten;
    }

    public int getSegmentCount()
    {
        return currentWriter + 1;
    }

    @Override
    public TableId getTableId()
    {
        return cfs.metadata.id;
    }

    @Override
    public Throwable commit(Throwable accumulate)
    {
        Throwable t = accumulate;
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                t = writer.commit(t);
        return t;
    }

    @Override
    public Throwable abort(Throwable accumulate)
    {
        Throwable t = accumulate;
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                t = writer.abort(t);
        return t;
    }

    @Override
    public void prepareToCommit()
    {
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                writer.prepareToCommit();
    }

    @Override
    public void close()
    {
        for (SSTableMultiWriter writer : writers)
            if (writer != null)
                writer.close();
    }
}