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

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadataRef;

/**
 * A SSTable writer that assumes rows are in (partitioner) sorted order.
 * <p>
 * Contrarily to SSTableSimpleUnsortedWriter, this writer does not buffer
 * anything into memory, however it assumes that row are added in sorted order
 * (an exception will be thrown otherwise), which for the RandomPartitioner
 * means that rows should be added by increasing md5 of the row key. This is
 * rarely possible and SSTableSimpleUnsortedWriter should most of the time be
 * prefered.
 * <p>
 * Optionally, the writer can be configured with a max SSTable size for SSTables.
 * The output will be a series of SSTables that do not exceed a specified size.
 * By default, all sorted data are written into a single SSTable.
 */
class SSTableSimpleWriter extends AbstractSSTableSimpleWriter
{
    private final long maxSSTableSizeInBytes;

    protected DecoratedKey currentKey;
    protected PartitionUpdate.Builder update;

    private SSTableTxnWriter writer;

    /**
     * Create a SSTable writer for sorted input data.
     * When a positive {@param maxSSTableSizeInMiB} is defined, the writer outputs a sequence of SSTables,
     * whose sizes do not exceed the specified value.
     *
     * @param directory directory to store the sstable files
     * @param metadata table metadata
     * @param columns columns to update
     * @param maxSSTableSizeInMiB defines the max SSTable size if the value is positive.
     *                            Any non-positive value indicates the sstable size is unlimited.
     */
    protected SSTableSimpleWriter(File directory, TableMetadataRef metadata, RegularAndStaticColumns columns, long maxSSTableSizeInMiB)
    {
        super(directory, metadata, columns);
        this.maxSSTableSizeInBytes = maxSSTableSizeInMiB * 1024L * 1024L;
    }

    @Override
    PartitionUpdate.Builder getUpdateFor(DecoratedKey key) throws IOException
    {
        Preconditions.checkArgument(key != null, "Partition update cannot have null key");

        // update for the first partition or a new partition
        if (update == null || !key.equals(currentKey))
        {
            // write the previous update if not absent
            if (update != null)
                writePartition(update.build()); // might switch to a new sstable writer and reset currentSize

            currentKey = key;
            update = new PartitionUpdate.Builder(metadata.get(), currentKey, columns, 4);
        }

        Preconditions.checkState(update != null, "Partition update to write cannot be null");
        return update;
    }

    @Override
    public void close()
    {
        writeLastPartitionUpdate(update);
        maybeCloseWriter(writer);
    }

    /**
     * Switch to a new writer when writer is absent or the file size has exceeded the configured max
     */
    private boolean shouldSwitchToNewWriter()
    {
        return writer == null || (maxSSTableSizeInBytes > 0 && writer.getOnDiskBytesWritten() > maxSSTableSizeInBytes);
    }

    /**
     * Get the current writer, or create a new writer if needed, e.g. writer does not exist
     * or writer has reached to the configured max size.
     */
    private SSTableTxnWriter getOrCreateWriter() throws IOException
    {
        if (shouldSwitchToNewWriter())
        {
            maybeCloseWriter(writer);
            writer = createWriter(null);
        }

        return writer;
    }

    private void writeLastPartitionUpdate(PartitionUpdate.Builder update)
    {
        try
        {
            if (update != null)
                writePartition(update.build());
        }
        catch (Throwable t)
        {
            Throwable e = writer == null ? t : writer.abort(t);
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private void maybeCloseWriter(SSTableTxnWriter writer)
    {
        try
        {
            if (writer != null)
                writer.finish(false);
        }
        catch (Throwable t)
        {
            Throwable e = writer.abort(t);
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private void writePartition(PartitionUpdate update) throws IOException
    {
        getOrCreateWriter().append(update.unfilteredIterator());
    }
}
