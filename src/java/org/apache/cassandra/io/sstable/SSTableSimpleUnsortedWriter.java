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
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.JVMStabilityInspector;

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
                                       CompressionParameters compressParameters)
    {
        this(directory, CFMetaData.denseCFMetaData(keyspace, columnFamily, comparator, subComparator).compressionParameters(compressParameters), partitioner, bufferSizeInMB);
    }

    public SSTableSimpleUnsortedWriter(File directory,
                                       IPartitioner partitioner,
                                       String keyspace,
                                       String columnFamily,
                                       AbstractType<?> comparator,
                                       AbstractType<?> subComparator,
                                       int bufferSizeInMB)
    {
        this(directory, partitioner, keyspace, columnFamily, comparator, subComparator, bufferSizeInMB, new CompressionParameters(null));
    }

    public SSTableSimpleUnsortedWriter(File directory, CFMetaData metadata, IPartitioner partitioner, long bufferSizeInMB)
    {
        super(directory, metadata, partitioner);
        this.bufferSize = bufferSizeInMB * 1024L * 1024L;
        this.diskWriter.start();
    }

    protected void writeRow(DecoratedKey key, ColumnFamily columnFamily) throws IOException
    {
        // Nothing to do since we'll sync if needed in addColumn.
    }

    @Override
    protected void addColumn(Cell cell) throws IOException
    {
        super.addColumn(cell);
        countColumn(cell);
    }

    protected void countColumn(Cell cell) throws IOException
    {
        currentSize += cell.serializedSize(metadata.comparator, TypeSizes.NATIVE);

        // We don't want to sync in writeRow() only as this might blow up the bufferSize for wide rows.
        if (currentSize > bufferSize)
            replaceColumnFamily();
    }

    protected ColumnFamily getColumnFamily() throws IOException
    {
        ColumnFamily previous = buffer.get(currentKey);
        // If the CF already exist in memory, we'll just continue adding to it
        if (previous == null)
        {
            previous = createColumnFamily();
            buffer.put(currentKey, previous);

            // Since this new CF will be written by the next sync(), count its header. And a CF header
            // on disk is:
            //   - the row key: 2 bytes size + key size bytes
            //   - the row level deletion infos: 4 + 8 bytes
            currentSize += 14 + currentKey.getKey().remaining();
        }
        return previous;
    }

    protected ColumnFamily createColumnFamily() throws IOException
    {
        return ArrayBackedSortedColumns.factory.create(metadata);
    }

    public void close() throws IOException
    {
        sync();
        put(SENTINEL);
        try
        {
            diskWriter.join();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    // This is overridden by CQLSSTableWriter to hold off replacing column family until the next iteration through
    protected void replaceColumnFamily() throws IOException
    {
        sync();
    }

    protected void sync() throws IOException
    {
        if (buffer.isEmpty())
            return;

        columnFamily = null;
        put(buffer);
        buffer = new Buffer();
        currentSize = 0;
        columnFamily = getColumnFamily();
        buffer.setFirstInsertedKey(currentKey);
    }

    private void put(Buffer buffer) throws IOException
    {
        while (true)
        {
            checkForWriterException();
            try
            {
                if (writeQueue.offer(buffer, 1, TimeUnit.SECONDS))
                    break;
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private void checkForWriterException() throws IOException
    {
        // slightly lame way to report exception from the writer, but that should be good enough
        if (diskWriter.exception != null)
        {
            if (diskWriter.exception instanceof IOException)
                throw (IOException) diskWriter.exception;
            else
                throw Throwables.propagate(diskWriter.exception);
        }
    }

    // typedef
    private static class Buffer extends TreeMap<DecoratedKey, ColumnFamily> {
        private DecoratedKey firstInsertedKey;

        public void setFirstInsertedKey(DecoratedKey firstInsertedKey) {
            this.firstInsertedKey = firstInsertedKey;
        }

        public DecoratedKey getFirstInsertedKey() {
            return firstInsertedKey;
        }
    }

    private class DiskWriter extends Thread
    {
        volatile Throwable exception = null;

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
                    {
                        if (entry.getValue().getColumnCount() > 0)
                            writer.append(entry.getKey(), entry.getValue());
                        else if (!entry.getKey().equals(b.getFirstInsertedKey()))
                            throw new AssertionError("Empty partition");
                    }
                    writer.close();
                }
            }
            catch (Throwable e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                if (writer != null)
                    writer.abort();
                exception = e;
            }
        }
    }
}
