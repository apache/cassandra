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

package org.apache.cassandra.io.sstable.format.big;

import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.io.sstable.DataComponent;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SortedTableWriterBuilder;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.Throwables;

public class BigTableWriterBuilder extends SortedTableWriterBuilder<RowIndexEntry, BigFormatPartitionWriter, BigTableWriter, BigTableWriterBuilder>
{
    private RowIndexEntry.IndexSerializer rowIndexEntrySerializer;
    private BigTableWriter.IndexWriter indexWriter;
    private SequentialWriter dataWriter;
    private BigFormatPartitionWriter partitionWriter;

    public BigTableWriterBuilder(Descriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    public SequentialWriter getDataWriter()
    {
        return dataWriter;
    }

    @Override
    public BigFormatPartitionWriter getPartitionWriter()
    {
        return partitionWriter;
    }

    public RowIndexEntry.IndexSerializer getRowIndexEntrySerializer()
    {
        return rowIndexEntrySerializer;
    }

    public BigTableWriter.IndexWriter getIndexWriter()
    {
        return indexWriter;
    }

    @Override
    protected BigTableWriter buildInternal(LifecycleNewTracker lifecycleNewTracker)
    {
        try
        {
            rowIndexEntrySerializer = new RowIndexEntry.Serializer(descriptor.version, getSerializationHeader());
            dataWriter = DataComponent.buildWriter(descriptor,
                                                   getTableMetadataRef().getLocal(),
                                                   getIOOptions().writerOptions,
                                                   getMetadataCollector(),
                                                   lifecycleNewTracker.opType(),
                                                   getIOOptions().flushCompression);
            indexWriter = new BigTableWriter.IndexWriter(this);
            partitionWriter = new BigFormatPartitionWriter(getSerializationHeader(), dataWriter, descriptor.version, rowIndexEntrySerializer.indexInfoSerializer());
            return new BigTableWriter(this, lifecycleNewTracker);
        }
        catch (RuntimeException | Error ex)
        {
            Throwables.closeAndAddSuppressed(ex, partitionWriter, indexWriter, dataWriter);
            throw ex;
        }
        finally
        {
            rowIndexEntrySerializer = null;
            indexWriter = null;
            dataWriter = null;
            partitionWriter = null;
        }
    }
}