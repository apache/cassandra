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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;

/**
 * A SSTable writer that assumes rows are in (partitioner) sorted order.
 * Contrarily to SSTableSimpleUnsortedWriter, this writer does not buffer
 * anything into memory, however it assumes that row are added in sorted order
 * (an exception will be thrown otherwise), which for the RandomPartitioner
 * means that rows should be added by increasing md5 of the row key. This is
 * rarely possible and SSTableSimpleUnsortedWriter should most of the time be
 * prefered.
 *
 * @see AbstractSSTableSimpleWriter
 */
public class SSTableSimpleWriter extends AbstractSSTableSimpleWriter
{
    private final SSTableWriter writer;

    /**
     * Create a new writer.
     * @param directory the directory where to write the sstable
     * @param keyspace the keyspace name
     * @param columnFamily the column family name
     * @param comparator the column family comparator
     * @param subComparator the column family subComparator or null if not a Super column family.
     */
    public SSTableSimpleWriter(File directory,
                               String keyspace,
                               String columnFamily,
                               AbstractType comparator,
                               AbstractType subComparator) throws IOException
    {
        this(directory,
             new CFMetaData(keyspace, columnFamily, subComparator == null ? ColumnFamilyType.Standard : ColumnFamilyType.Super, comparator, subComparator));
    }

    public SSTableSimpleWriter(File directory, CFMetaData metadata) throws IOException
    {
        super(directory, metadata);
        writer = getWriter();
    }

    public void close() throws IOException
    {
        if (currentKey != null)
            writeRow(currentKey, columnFamily);
        writer.closeAndOpenReader();
    }

    protected void writeRow(DecoratedKey key, ColumnFamily columnFamily) throws IOException
    {
        writer.append(key, columnFamily);
    }

    protected ColumnFamily getColumnFamily()
    {
        return ColumnFamily.create(metadata, TreeMapBackedSortedColumns.factory());
    }
}
