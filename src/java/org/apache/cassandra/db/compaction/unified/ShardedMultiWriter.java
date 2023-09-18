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

package org.apache.cassandra.db.compaction.unified;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.compaction.ShardTracker;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

/**
 * A {@link SSTableMultiWriter} that splits the output sstable at the partition boundaries of the compaction
 * shards used by {@link org.apache.cassandra.db.compaction.UnifiedCompactionStrategy}.
 * <p/>
 * This is class is similar to {@link ShardedCompactionWriter} but for flushing. Unfortunately
 * we currently have 2 separate writers hierarchy that are not compatible and so we must
 * duplicate the functionality.
 */
public class ShardedMultiWriter implements SSTableMultiWriter
{
    protected final static Logger logger = LoggerFactory.getLogger(ShardedMultiWriter.class);

    private final ColumnFamilyStore cfs;
    private final Descriptor descriptor;
    private final long keyCount;
    private final long repairedAt;
    private final TimeUUID pendingRepair;
    private final boolean isTransient;
    private final IntervalSet<CommitLogPosition> commitLogPositions;
    private final SerializationHeader header;
    private final Collection<Index.Group> indexGroups;
    private final LifecycleNewTracker lifecycleNewTracker;
    private final ShardTracker boundaries;
    private final SSTableWriter[] writers;
    private int currentWriter;

    public ShardedMultiWriter(ColumnFamilyStore cfs,
                              Descriptor descriptor,
                              long keyCount,
                              long repairedAt,
                              TimeUUID pendingRepair,
                              boolean isTransient,
                              IntervalSet<CommitLogPosition> commitLogPositions,
                              SerializationHeader header,
                              Collection<Index.Group> indexGroups,
                              LifecycleNewTracker lifecycleNewTracker,
                              ShardTracker boundaries)
    {
        this.cfs = cfs;
        this.descriptor = descriptor;
        this.keyCount = keyCount;
        this.repairedAt = repairedAt;
        this.pendingRepair = pendingRepair;
        this.isTransient = isTransient;
        this.commitLogPositions = commitLogPositions;
        this.header = header;
        this.indexGroups = indexGroups;
        this.lifecycleNewTracker = lifecycleNewTracker;
        this.boundaries = boundaries;
        this.writers = new SSTableWriter[this.boundaries.count()]; // at least one

        this.currentWriter = 0;
        this.writers[currentWriter] = createWriter(descriptor);
    }

    private SSTableWriter createWriter()
    {
        Descriptor newDesc = cfs.newSSTableDescriptor(descriptor.directory);
        return createWriter(newDesc);
    }

    private SSTableWriter createWriter(Descriptor descriptor)
    {
        MetadataCollector metadataCollector = new MetadataCollector(cfs.metadata().comparator)
                                              .commitLogIntervals(commitLogPositions != null ? commitLogPositions : IntervalSet.empty());
        return descriptor.getFormat().getWriterFactory().builder(descriptor)
                         .setKeyCount(forSplittingKeysBy(boundaries.count()))
                         .setRepairedAt(repairedAt)
                         .setPendingRepair(pendingRepair)
                         .setTransientSSTable(isTransient)
                         .setTableMetadataRef(cfs.metadata)
                         .setMetadataCollector(metadataCollector)
                         .setSerializationHeader(header)
                         .addDefaultComponents(indexGroups)
                         .setSecondaryIndexGroups(indexGroups)
                         .build(lifecycleNewTracker, cfs);
    }

    private long forSplittingKeysBy(long splits) {
        return splits <= 1 ? keyCount : keyCount / splits;
    }

    @Override
    public void append(UnfilteredRowIterator partition)
    {
        DecoratedKey key = partition.partitionKey();

        // If we have written anything and cross a shard boundary, switch to a new writer.
        final long currentUncompressedSize = writers[currentWriter].getFilePointer();
        if (boundaries.advanceTo(key.getToken()) && currentUncompressedSize > 0)
        {
            logger.debug("Switching writer at boundary {}/{} index {}, with uncompressed size {} for {}.{}",
                         key.getToken(), boundaries.shardStart(), currentWriter,
                         FBUtilities.prettyPrintMemory(currentUncompressedSize),
                         cfs.getKeyspaceName(), cfs.getTableName());

            writers[++currentWriter] = createWriter();
        }

        writers[currentWriter].append(partition);
    }

    @Override
    public Collection<SSTableReader> finish(boolean openResult)
    {
        List<SSTableReader> sstables = new ArrayList<>(writers.length);
        for (SSTableWriter writer : writers)
            if (writer != null)
            {
                boundaries.applyTokenSpaceCoverage(writer);
                sstables.add(writer.finish(openResult));
            }
        return sstables;
    }

    @Override
    public Collection<SSTableReader> finished()
    {
        List<SSTableReader> sstables = new ArrayList<>(writers.length);
        for (SSTableWriter writer : writers)
            if (writer != null)
                sstables.add(writer.finished());
        return sstables;
    }

    @Override
    public SSTableMultiWriter setOpenResult(boolean openResult)
    {
        for (SSTableWriter writer : writers)
            if (writer != null)
                writer.setOpenResult(openResult);
        return this;
    }

    @Override
    public String getFilename()
    {
        for (SSTableWriter writer : writers)
            if (writer != null)
                return writer.getFilename();
        return "";
    }

    @Override
    public long getBytesWritten()
    {
        long bytesWritten = 0;
        for (int i = 0; i <= currentWriter; ++i)
            bytesWritten += writers[i].getFilePointer();
        return bytesWritten;
    }

    @Override
    public long getOnDiskBytesWritten()
    {
        long bytesWritten = 0;
        for (int i = 0; i <= currentWriter; ++i)
            bytesWritten += writers[i].getEstimatedOnDiskBytesWritten();
        return bytesWritten;
    }

    @Override
    public TableId getTableId()
    {
        return cfs.metadata().id;
    }

    @Override
    public Throwable commit(Throwable accumulate)
    {
        Throwable t = accumulate;
        for (SSTableWriter writer : writers)
            if (writer != null)
                t = writer.commit(t);
        return t;
    }

    @Override
    public Throwable abort(Throwable accumulate)
    {
        Throwable t = accumulate;
        for (SSTableWriter writer : writers)
            if (writer != null)
            {
                lifecycleNewTracker.untrackNew(writer);
                t = writer.abort(t);
            }
        return t;
    }

    @Override
    public void prepareToCommit()
    {
        for (SSTableWriter writer : writers)
            if (writer != null)
            {
                boundaries.applyTokenSpaceCoverage(writer);
                writer.prepareToCommit();
    }
    }

    @Override
    public void close()
    {
        for (SSTableWriter writer : writers)
            if (writer != null)
                writer.close();
    }
}
