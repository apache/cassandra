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

package org.apache.cassandra.io.sstable.format.trieindex;

import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.io.sstable.DataComponent;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SortedTableWriterBuilder;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.Throwables;

public class BtiTableWriterBuilder extends SortedTableWriterBuilder<TrieIndexEntry, BtiFormatPartitionWriter, BtiTableWriter, BtiTableWriterBuilder>
{
    private SequentialWriter dataWriter;
    private BtiFormatPartitionWriter partitionWriter;
    private BtiTableWriter.IndexWriter indexWriter;

    public BtiTableWriterBuilder(Descriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    public SequentialWriter getDataWriter()
    {
        return dataWriter;
    }

    @Override
    public BtiFormatPartitionWriter getPartitionWriter()
    {
        return partitionWriter;
    }

    public BtiTableWriter.IndexWriter getIndexWriter()
    {
        return indexWriter;
    }

    @Override
    protected BtiTableWriter buildInternal(LifecycleNewTracker lifecycleNewTracker)
    {
        try
        {
            dataWriter = DataComponent.buildWriter(descriptor,
                                                   getTableMetadataRef().getLocal(),
                                                   getIOOptions().writerOptions,
                                                   getMetadataCollector(),
                                                   lifecycleNewTracker.opType(),
                                                   getIOOptions().flushCompression);

            indexWriter = new BtiTableWriter.IndexWriter(this);
            partitionWriter = new BtiFormatPartitionWriter(getSerializationHeader(),
                                                           getTableMetadataRef().getLocal().comparator,
                                                           dataWriter,
                                                           indexWriter.rowIndexWriter,
                                                           descriptor.version);


            return new BtiTableWriter(this, lifecycleNewTracker);
        }
        catch (RuntimeException | Error ex)
        {
            Throwables.closeAndAddSuppressed(ex, partitionWriter, indexWriter, dataWriter);
            throw ex;
        }
        finally
        {
            dataWriter = null;
            partitionWriter = null;
        }
    }
}
