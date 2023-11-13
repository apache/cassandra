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

import java.util.Collection;
import java.util.Collections;

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.TimeUUID;

public class SimpleSSTableMultiWriter implements SSTableMultiWriter
{
    private final SSTableWriter writer;
    private final LifecycleNewTracker lifecycleNewTracker;

    protected SimpleSSTableMultiWriter(SSTableWriter writer, LifecycleNewTracker lifecycleNewTracker)
    {
        this.lifecycleNewTracker = lifecycleNewTracker;
        this.writer = writer;
    }

    public void append(UnfilteredRowIterator partition)
    {
        writer.append(partition);
    }

    public Collection<SSTableReader> finish(boolean openResult)
    {
        return Collections.singleton(writer.finish(openResult));
    }

    public Collection<SSTableReader> finished()
    {
        return Collections.singleton(writer.finished());
    }

    public SSTableMultiWriter setOpenResult(boolean openResult)
    {
        writer.setOpenResult(openResult);
        return this;
    }

    public String getFilename()
    {
        return writer.getFilename();
    }

    public long getBytesWritten()
    {
        return writer.getFilePointer();
    }

    public long getOnDiskBytesWritten()
    {
        return writer.getEstimatedOnDiskBytesWritten();
    }

    public TableId getTableId()
    {
        return writer.metadata().id;
    }

    public Throwable commit(Throwable accumulate)
    {
        return writer.commit(accumulate);
    }

    public Throwable abort(Throwable accumulate)
    {
        lifecycleNewTracker.untrackNew(writer);
        return writer.abort(accumulate);
    }

    public void prepareToCommit()
    {
        writer.prepareToCommit();
    }

    public void close()
    {
        writer.close();
    }

    public static SSTableMultiWriter create(Descriptor descriptor,
                                            long keyCount,
                                            long repairedAt,
                                            TimeUUID pendingRepair,
                                            boolean isTransient,
                                            TableMetadataRef metadata,
                                            IntervalSet<CommitLogPosition> commitLogPositions,
                                            int sstableLevel,
                                            SerializationHeader header,
                                            Collection<Index.Group> indexGroups,
                                            LifecycleNewTracker lifecycleNewTracker,
                                            SSTable.Owner owner)
    {
        MetadataCollector metadataCollector = new MetadataCollector(metadata.get().comparator)
                                              .commitLogIntervals(commitLogPositions != null ? commitLogPositions : IntervalSet.empty())
                                              .sstableLevel(sstableLevel);
        SSTableWriter writer = descriptor.getFormat().getWriterFactory().builder(descriptor)
                                            .setKeyCount(keyCount)
                                            .setRepairedAt(repairedAt)
                                            .setPendingRepair(pendingRepair)
                                            .setTransientSSTable(isTransient)
                                            .setTableMetadataRef(metadata)
                                            .setMetadataCollector(metadataCollector)
                                            .setSerializationHeader(header)
                                            .addDefaultComponents(indexGroups)
                                            .setSecondaryIndexGroups(indexGroups)
                                            .build(lifecycleNewTracker, owner);
        return new SimpleSSTableMultiWriter(writer, lifecycleNewTracker);
    }
}
