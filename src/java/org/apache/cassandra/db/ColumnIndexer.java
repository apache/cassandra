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

import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.IIterableColumns;
import org.apache.cassandra.utils.BloomFilter;


/**
 * Help to create an index for a column family based on size of columns
 */
public class ColumnIndexer
{
	/**
	 * Given a column family this, function creates an in-memory structure that represents the
	 * column index for the column family, and subsequently writes it to disk.
     *
	 * @param columns Column family to create index for
	 * @param dos data output stream
	 */
    public static void serialize(IIterableColumns columns, DataOutput dos)
    {
        try
        {
            writeIndex(serialize(columns), dos);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public static void serialize(RowHeader indexInfo, DataOutput dos)
    {
        try
        {
            writeIndex(indexInfo, dos);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * Serializes the index into in-memory structure with all required components
     * such as Bloom Filter, index block size, IndexInfo list
     *
     * @param columns Column family to create index for
     *
     * @return information about index - it's Bloom Filter, block size and IndexInfo list
     */
    public static RowHeader serialize(IIterableColumns columns)
    {
        int columnCount = columns.getEstimatedColumnCount();

        BloomFilter bf = BloomFilter.getFilter(columnCount, 4);

        if (columnCount == 0)
            return new RowHeader(bf, Collections.<IndexHelper.IndexInfo>emptyList());

        // update bloom filter and create a list of IndexInfo objects marking the first and last column
        // in each block of ColumnIndexSize
        List<IndexHelper.IndexInfo> indexList = new ArrayList<IndexHelper.IndexInfo>();
        long endPosition = 0, startPosition = -1;
        IColumn lastColumn = null, firstColumn = null;

        for (IColumn column : columns)
        {
            bf.add(column.name());

            if (firstColumn == null)
            {
                firstColumn = column;
                startPosition = endPosition;
            }

            endPosition += column.serializedSize();

            // if we hit the column index size that we have to index after, go ahead and index it.
            if (endPosition - startPosition >= DatabaseDescriptor.getColumnIndexSize())
            {
                IndexHelper.IndexInfo cIndexInfo = new IndexHelper.IndexInfo(firstColumn.name(), column.name(), startPosition, endPosition - startPosition);
                indexList.add(cIndexInfo);
                firstColumn = null;
            }

            lastColumn = column;
        }

        // all columns were GC'd after all
        if (lastColumn == null)
            return new RowHeader(bf, Collections.<IndexHelper.IndexInfo>emptyList());

        // the last column may have fallen on an index boundary already.  if not, index it explicitly.
        if (indexList.isEmpty() || columns.getComparator().compare(indexList.get(indexList.size() - 1).lastName, lastColumn.name()) != 0)
        {
            IndexHelper.IndexInfo cIndexInfo = new IndexHelper.IndexInfo(firstColumn.name(), lastColumn.name(), startPosition, endPosition - startPosition);
            indexList.add(cIndexInfo);
        }

        // we should always have at least one computed index block, but we only write it out if there is more than that.
        assert indexList.size() > 0;
        return new RowHeader(bf, indexList);
    }

    private static void writeIndex(RowHeader indexInfo, DataOutput dos) throws IOException
    {
        assert indexInfo != null;

        /* Write out the bloom filter. */
        writeBloomFilter(dos, indexInfo.bloomFilter);

        dos.writeInt(indexInfo.entriesSize);
        if (indexInfo.indexEntries.size() > 1)
        {
            for (IndexHelper.IndexInfo cIndexInfo : indexInfo.indexEntries)
                cIndexInfo.serialize(dos);
        }
    }

    /**
     * Write a Bloom filter into file
     *
     * @param dos file to serialize Bloom Filter
     * @param bf Bloom Filter
     *
     * @throws IOException on any I/O error.
     */
    private static void writeBloomFilter(DataOutput dos, BloomFilter bf) throws IOException
    {
        DataOutputBuffer bufOut = new DataOutputBuffer();
        BloomFilter.serializer().serialize(bf, bufOut);
        dos.writeInt(bufOut.getLength());
        dos.write(bufOut.getData(), 0, bufOut.getLength());
        bufOut.flush();
    }

    /**
     * Holds information about serialized index and bloom filter
     */
    public static class RowHeader
    {
        public final BloomFilter bloomFilter;
        public final List<IndexHelper.IndexInfo> indexEntries;
        public final int entriesSize;

        public RowHeader(BloomFilter bf, List<IndexHelper.IndexInfo> indexes)
        {
            assert bf != null;
            assert indexes != null;
            bloomFilter = bf;
            indexEntries = indexes;
            int entriesSize = 0;
            if (indexEntries.size() > 1)
            {
                for (IndexHelper.IndexInfo info : indexEntries)
                    entriesSize += info.serializedSize();
            }
            this.entriesSize = entriesSize;
        }

        public long serializedSize()
        {
            return DBConstants.intSize  // length of Bloom Filter
                   + bloomFilter.serializedSize() // BF data
                   + DBConstants.intSize // length of index block
                   + entriesSize; // index block
        }
    }
}
