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

import java.io.File;
import java.io.IOException;

import com.google.common.base.Throwables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;

/**
 * A SSTable writer that assumes rows are in (partitioner) sorted order.
 * <p>
 * Contrarily to SSTableSimpleUnsortedWriter, this writer does not buffer
 * anything into memory, however it assumes that row are added in sorted order
 * (an exception will be thrown otherwise), which for the RandomPartitioner
 * means that rows should be added by increasing md5 of the row key. This is
 * rarely possible and SSTableSimpleUnsortedWriter should most of the time be
 * prefered.
 */
class SSTableSimpleWriter extends AbstractSSTableSimpleWriter
{
    protected DecoratedKey currentKey;
    protected PartitionUpdate update;

    private SSTableTxnWriter writer;

    protected SSTableSimpleWriter(File directory, CFMetaData metadata, PartitionColumns columns)
    {
        super(directory, metadata, columns);
    }

    private SSTableTxnWriter getOrCreateWriter()
    {
        if (writer == null)
            writer = createWriter();

        return writer;
    }

    PartitionUpdate getUpdateFor(DecoratedKey key) throws IOException
    {
        assert key != null;

        // If that's not the current key, write the current one if necessary and create a new
        // update for the new key.
        if (!key.equals(currentKey))
        {
            if (update != null)
                writePartition(update);
            currentKey = key;
            update = new PartitionUpdate(metadata, currentKey, columns, 4);
        }

        assert update != null;
        return update;
    }

    public void close()
    {
        try
        {
            if (update != null)
                writePartition(update);
            if (writer != null)
                writer.finish(false);
        }
        catch (Throwable t)
        {
            throw Throwables.propagate(writer == null ? t : writer.abort(t));
        }
    }

    private void writePartition(PartitionUpdate update) throws IOException
    {
        getOrCreateWriter().append(update.unfilteredIterator());
    }
}
