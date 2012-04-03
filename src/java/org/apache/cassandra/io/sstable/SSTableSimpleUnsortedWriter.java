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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.net.MessagingService;

/**
 * A SSTable writer that doesn't assume rows are in sorted order.
 * This writer buffers rows in memory and then write them all in sorted order.
 * To avoid loading the entire data set in memory, the amount of rows buffered
 * is configurable. Each time the threshold is met, one SSTable will be
 * created (and the buffer be reseted).
 *
 * @see AbstractSSTableSimpleWriter
 */
public class SSTableSimpleUnsortedWriter extends AbstractSSTableSimpleWriter
{
    private static final Buffer SENTINEL = new Buffer();

    private Buffer buffer = new Buffer();
    private final long bufferSize;
    private long currentSize;

    private final BlockingQueue<Buffer> writeQueue = new SynchronousQueue<Buffer>();
    private final DiskWriter diskWriter = new DiskWriter();

    /**
     * Create a new buffering writer.
     * @param directory the directory where to write the sstables
     * @param partitioner  the partitioner
     * @param keyspace the keyspace name
     * @param columnFamily the column family name
     * @param comparator the column family comparator
     * @param subComparator the column family subComparator or null if not a Super column family.
     * @param bufferSizeInMB the data size in MB before which a sstable is written and the buffer reseted. This correspond roughly to the written
     * data size (i.e. the size of the create sstable). The actual size used in memory will be higher (by how much depends on the size of the
     * columns you add). For 1GB of heap, a 128 bufferSizeInMB is probably a reasonable choice. If you experience OOM, this value should be lowered.
     */
    public SSTableSimpleUnsortedWriter(File directory,
                                       IPartitioner partitioner,
                                       String keyspace,
                                       String columnFamily,
                                       AbstractType<?> comparator,
                                       AbstractType<?> subComparator,
                                       int bufferSizeInMB,
                                       CompressionParameters compressParameters) throws IOException
    {
        super(directory, new CFMetaData(keyspace, columnFamily, subComparator == null ? ColumnFamilyType.Standard : ColumnFamilyType.Super, comparator, subComparator).compressionParameters(compressParameters), partitioner);
        this.bufferSize = bufferSizeInMB * 1024L * 1024L;
        this.diskWriter.start();
    }

    public SSTableSimpleUnsortedWriter(File directory,
                                       IPartitioner partitioner,
                                       String keyspace,
                                       String columnFamily,
                                       AbstractType<?> comparator,
                                       AbstractType<?> subComparator,
                                       int bufferSizeInMB) throws IOException
    {
        this(directory, partitioner, keyspace, columnFamily, comparator, subComparator, bufferSizeInMB, new CompressionParameters(null));
    }

    protected void writeRow(DecoratedKey key, ColumnFamily columnFamily) throws IOException
    {
        currentSize += key.key.remaining() + ColumnFamily.serializer.serializedSize(columnFamily, MessagingService.current_version) * 1.2;

        if (currentSize > bufferSize)
            sync();
    }

    protected ColumnFamily getColumnFamily()
    {
        ColumnFamily previous = buffer.get(currentKey);
        // If the CF already exist in memory, we'll just continue adding to it
        if (previous == null)
        {
            previous = ColumnFamily.create(metadata, TreeMapBackedSortedColumns.factory());
            buffer.put(currentKey, previous);
        }
        else
        {
            // We will reuse a CF that we have counted already. But because it will be easier to add the full size
            // of the CF in the next writeRow call than to find out the delta, we just remove the size until that next call
            currentSize -= currentKey.key.remaining() + ColumnFamily.serializer.serializedSize(previous, MessagingService.current_version) * 1.2;
        }
        return previous;
    }

    public void close() throws IOException
    {
        sync();
        try
        {
            writeQueue.put(SENTINEL);
            diskWriter.join();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        checkForWriterException();
    }

    private void sync() throws IOException
    {
        if (buffer.isEmpty())
            return;

        checkForWriterException();

        try
        {
            writeQueue.put(buffer);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        buffer = new Buffer();
        currentSize = 0;
    }

    private void checkForWriterException() throws IOException
    {
        // slightly lame way to report exception from the writer, but that should be good enough
        if (diskWriter.exception != null)
        {
            if (diskWriter.exception instanceof IOException)
                throw (IOException) diskWriter.exception;
            else
                throw new RuntimeException(diskWriter.exception);
        }
    }

    // typedef
    private static class Buffer extends TreeMap<DecoratedKey, ColumnFamily> {}

    private class DiskWriter extends Thread
    {
        volatile Exception exception = null;

        public void run()
        {
            SSTableWriter writer = null;
            try
            {
                while (true)
                {
                    Buffer b = writeQueue.take();
                    if (b == SENTINEL)
                        return;

                    writer = getWriter();
                    for (Map.Entry<DecoratedKey, ColumnFamily> entry : b.entrySet())
                        writer.append(entry.getKey(), entry.getValue());
                    writer.closeAndOpenReader();
                }
            }
            catch (Exception e)
            {
                if (writer != null)
                    writer.abort();
                exception = e;
            }
        }
    }
}
