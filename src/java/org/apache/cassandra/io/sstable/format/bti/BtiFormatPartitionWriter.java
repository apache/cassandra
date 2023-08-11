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

package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.format.SortedTablePartitionWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.bti.RowIndexReader.IndexInfo;
import org.apache.cassandra.io.util.SequentialWriter;

/**
 * Partition writer used by {@link BtiTableWriter}.
 * <p>
 * Writes all passed data to the given SequentialWriter and if necessary builds a RowIndex by constructing an entry
 * for each row within a partition that follows {@link org.apache.cassandra.config.Config#column_index_size} of written
 * data.
 */
class BtiFormatPartitionWriter extends SortedTablePartitionWriter
{
    private static final int DEFAULT_GRANULARITY = 16 * 1024;
    private final RowIndexWriter rowTrie;
    private final int rowIndexBlockSize;
    private int rowIndexBlockCount;

    BtiFormatPartitionWriter(SerializationHeader header,
                             ClusteringComparator comparator,
                             SequentialWriter dataWriter,
                             SequentialWriter rowIndexWriter,
                             Version version)
    {
        this(header, comparator, dataWriter, rowIndexWriter, version, DatabaseDescriptor.getColumnIndexSize(DEFAULT_GRANULARITY));
    }


    BtiFormatPartitionWriter(SerializationHeader header,
                             ClusteringComparator comparator,
                             SequentialWriter dataWriter,
                             SequentialWriter rowIndexWriter,
                             Version version,
                             int rowIndexBlockSize)
    {
        super(header, dataWriter, version);
        this.rowIndexBlockSize = rowIndexBlockSize;
        this.rowTrie = new RowIndexWriter(comparator, rowIndexWriter, version);
    }

    @Override
    public void reset()
    {
        super.reset();
        rowTrie.reset();
        rowIndexBlockCount = 0;
    }

    @Override
    public void addUnfiltered(Unfiltered unfiltered) throws IOException
    {
        super.addUnfiltered(unfiltered);

        // if we hit the column index size that we have to index after, go ahead and index it.
        if (currentPosition() - startPosition >= rowIndexBlockSize)
            addIndexBlock();
    }

    @Override
    public void close()
    {
        rowTrie.close();
    }

    public long finish() throws IOException
    {
        long endPosition = super.finish();

        // the last row may have fallen on an index boundary already.  if not, index the last block explicitly.
        if (rowIndexBlockCount > 0 && firstClustering != null)
            addIndexBlock();

        if (rowIndexBlockCount > 1)
        {
            return rowTrie.complete(endPosition);
        }
        else
        {
            // Otherwise we don't complete the trie as an index of one block adds no information and we are better off
            // without a row index for such partitions. Even if we did write something to the file (which shouldn't be
            // the case as the first entry has an empty key and root isn't filled), that's not a problem.
            return -1;
        }
    }

    protected void addIndexBlock() throws IOException
    {
        IndexInfo cIndexInfo = new IndexInfo(startPosition, startOpenMarker);
        rowTrie.add(firstClustering, lastClustering, cIndexInfo);
        firstClustering = null;
        ++rowIndexBlockCount;
    }

    public int getRowIndexBlockCount()
    {
        return rowIndexBlockCount;
    }
}
