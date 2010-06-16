/**
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

package org.apache.cassandra.db;

import java.io.IOError;
import java.io.IOException;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.utils.BloomFilter;


/**
 * Help to create an index for a column family based on size of columns
 */
public class ColumnIndexer
{
	/**
	 * Given a column family this, function creates an in-memory structure that represents the
	 * column index for the column family, and subsequently writes it to disk.
	 * @param columnFamily Column family to create index for
	 * @param dos data output stream
	 * @throws IOException
	 */
    public static void serialize(ColumnFamily columnFamily, DataOutput dos)
    {
        try
        {
            serializeInternal(columnFamily, dos);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public static void serializeInternal(ColumnFamily columnFamily, DataOutput dos) throws IOException
    {
        Collection<IColumn> columns = columnFamily.getSortedColumns();
        int columnCount = 0;
        for (IColumn column : columns)
        {
            columnCount += column.getObjectCount();
        }

        BloomFilter bf = BloomFilter.getFilter(columnCount, 4);

        if (columns.isEmpty())
        {
            // write empty bloom filter and index
            writeBloomFilter(dos, bf);
            dos.writeInt(0);

            return;
        }

        /*
         * Maintains a list of ColumnIndexInfo objects for the columns in this
         * column family. The key is the column name and the position is the
         * relative offset of that column name from the start of the list.
         * We do this so that we don't read all the columns into memory.
        */
        List<IndexHelper.IndexInfo> indexList = new ArrayList<IndexHelper.IndexInfo>();

        int endPosition = 0, startPosition = -1;
        int indexSizeInBytes = 0;
        IColumn lastColumn = null, firstColumn = null;
        /* column offsets at the right thresholds into the index map. */
        for (IColumn column : columns)
        {
            bf.add(column.name());
            /* If this is SuperColumn type Column Family we need to get the subColumns too. */
            if (column instanceof SuperColumn)
            {
                Collection<IColumn> subColumns = column.getSubColumns();
                for (IColumn subColumn : subColumns)
                {
                    bf.add(subColumn.name());
                }
            }

            if (firstColumn == null)
            {
                firstColumn = column;
                startPosition = endPosition;
            }
            endPosition += column.serializedSize();
            /* if we hit the column index size that we have to index after, go ahead and index it. */
            if (endPosition - startPosition >= DatabaseDescriptor.getColumnIndexSize())
            {
                IndexHelper.IndexInfo cIndexInfo = new IndexHelper.IndexInfo(firstColumn.name(), column.name(), startPosition, endPosition - startPosition);
                indexList.add(cIndexInfo);
                indexSizeInBytes += cIndexInfo.serializedSize();
                firstColumn = null;
            }

            lastColumn = column;
        }
        // the last column may have fallen on an index boundary already.  if not, index it explicitly.
        if (indexList.isEmpty() || columnFamily.getComparator().compare(indexList.get(indexList.size() - 1).lastName, lastColumn.name()) != 0)
        {
            IndexHelper.IndexInfo cIndexInfo = new IndexHelper.IndexInfo(firstColumn.name(), lastColumn.name(), startPosition, endPosition - startPosition);
            indexList.add(cIndexInfo);
            indexSizeInBytes += cIndexInfo.serializedSize();
        }

        /* Write out the bloom filter. */
        writeBloomFilter(dos, bf);

        // write the index
        assert indexSizeInBytes > 0;
        dos.writeInt(indexSizeInBytes);
        for (IndexHelper.IndexInfo cIndexInfo : indexList)
        {
            cIndexInfo.serialize(dos);
        }
	}

    private static void writeBloomFilter(DataOutput dos, BloomFilter bf) throws IOException
    {
        DataOutputBuffer bufOut = new DataOutputBuffer();
        BloomFilter.serializer().serialize(bf, bufOut);
        dos.writeInt(bufOut.getLength());
        dos.write(bufOut.getData(), 0, bufOut.getLength());
    }

}
