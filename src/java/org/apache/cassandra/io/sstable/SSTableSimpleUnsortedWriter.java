/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.HeapAllocator;

import org.apache.cassandra.utils.ByteBufferUtil;

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
    private final Map<DecoratedKey, ColumnFamily> keys = new TreeMap<DecoratedKey, ColumnFamily>();
    private final long bufferSize;
    private long currentSize;

    /**
     * Create a new buffering writer.
     * @param directory the directory where to write the sstables
     * @param keyspace the keyspace name
     * @param columnFamily the column family name
     * @param comparator the column family comparator
     * @param subComparator the column family subComparator or null if not a Super column family.
     * @param bufferSizeInMB the data size in MB before which a sstable is written and the buffer reseted. This correspond roughly to the written
     * data size (i.e. the size of the create sstable). The actual size used in memory will be higher (by how much depends on the size of the
     * columns you add). For 1GB of heap, a 128 bufferSizeInMB is probably a reasonable choice. If you experience OOM, this value should be lowered.
     */
    public SSTableSimpleUnsortedWriter(File directory,
                                       String keyspace,
                                       String columnFamily,
                                       AbstractType comparator,
                                       AbstractType subComparator,
                                       int bufferSizeInMB) throws IOException
    {
        super(directory, new CFMetaData(keyspace, columnFamily, subComparator == null ? ColumnFamilyType.Standard : ColumnFamilyType.Super, comparator, subComparator));
        this.bufferSize = bufferSizeInMB * 1024L * 1024L;
    }

    protected void writeRow(DecoratedKey key, ColumnFamily columnFamily) throws IOException
    {
        currentSize += key.key.remaining() + columnFamily.serializedSize() * 1.2;

        if (currentSize > bufferSize)
            sync();
    }

    protected ColumnFamily getColumnFamily()
    {
        ColumnFamily previous = keys.get(currentKey);
        // If the CF already exist in memory, we'll just continue adding to it
        if (previous == null)
        {
            previous = ColumnFamily.create(metadata, TreeMapBackedSortedColumns.factory());
            keys.put(currentKey, previous);
        }
        else
        {
            // We will reuse a CF that we have counted already. But because it will be easier to add the full size
            // of the CF in the next writeRow call than to find out the delta, we just remove the size until that next call
            currentSize -= currentKey.key.remaining() + previous.serializedSize() * 1.2;
        }
        return previous;
    }

    public void close() throws IOException
    {
        sync();
    }

    private void sync() throws IOException
    {
        if (keys.isEmpty())
            return;

        SSTableWriter writer = getWriter();
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : keys.entrySet())
        {
            writer.append(entry.getKey(), entry.getValue());
        }
        writer.closeAndOpenReader();
        currentSize = 0;
        keys.clear();
    }
}
