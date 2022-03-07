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

import java.io.IOException;
import java.util.Collection;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SortedTablePartitionWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.trieindex.RowIndexReader.IndexInfo;
import org.apache.cassandra.io.util.SequentialWriter;

/**
 * Partition writer used by {@link TrieIndexSSTableWriter}.
 *
 * Writes all passed data to the given SequentialWriter and if necessary builds a RowIndex by constructing an entry
 * for each row within a partition that follows {@link org.apache.cassandra.config.Config#column_index_cache_size_in_kb}
 * kilobytes of written data.
 */
class PartitionWriter extends SortedTablePartitionWriter
{
    private final RowIndexWriter rowTrie;
    int rowIndexCount;

    PartitionWriter(SerializationHeader header,
                    ClusteringComparator comparator,
                    SequentialWriter writer,
                    SequentialWriter indexWriter,
                    Version version,
                    Collection<SSTableFlushObserver> observers)
    {
        super(header, writer, version, observers);
        this.rowTrie = new RowIndexWriter(comparator, indexWriter);
    }

    @Override
    public void reset()
    {
        super.reset();
        rowTrie.reset();
        rowIndexCount = 0;
    }

    @Override
    public void close()
    {
        rowTrie.close();
    }

    public long finish() throws IOException
    {
        long endPosition = super.endPartition();

        // the last row may have fallen on an index boundary already.  if not, index it explicitly.
        if (rowIndexCount > 0 && firstClustering != null)
            addIndexBlock();

        if (rowIndexCount > 1)
            return rowTrie.complete(endPosition);
        else
        {
            // Otherwise we don't complete the trie. Even if we did write something (which shouldn't be the case as the
            // first entry has an empty key and root isn't filled), that's not a problem.
            return -1;
        }
    }

    protected void addIndexBlock() throws IOException
    {
        IndexInfo cIndexInfo = new IndexInfo(startPosition,
                                             startOpenMarker);
        rowTrie.add(firstClustering, lastClustering, cIndexInfo);
        firstClustering = null;
        ++rowIndexCount;
    }
}
