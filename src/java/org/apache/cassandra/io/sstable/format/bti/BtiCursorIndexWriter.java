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
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.ReusableDecoratedKey;
import org.apache.cassandra.io.sstable.ClusteringDescriptor;
import org.apache.cassandra.io.sstable.CursorIndexWriter;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.bti.RowIndexReader.IndexInfo;

/**
 * BTI index production for the cursor writer: a row-index trie per partition
 * ({@link RowIndexWriter}) and a partition index entry ({@link TrieIndexEntry}) appended
 * through {@link BtiTableWriter.IndexWriter}.
 *
 * It cuts blocks exactly as {@link BtiFormatPartitionWriter} does. A partition with one block
 * writes no row trie and takes a trie root of -1.
 */
public class BtiCursorIndexWriter extends CursorIndexWriter
{
    private final BtiTableWriter.IndexWriter indexWriter;
    private final RowIndexWriter rowTrie;
    private final int rowIndexBlockSize;

    private final ClusteringDescriptor firstClustering;
    private final AbstractType<?>[] clusteringTypes;
    private boolean blockOpen; // a first clustering has been captured for the current block
    private int rowIndexBlockCount;
    private DeletionTime blockStartOpenMarker = DeletionTime.LIVE;
    // PartitionIndexBuilder still reads the previous two keys while it adds the next one, so three keys are
    // reused in turn. It copies the keys it keeps for longer.
    private final ReusableDecoratedKey[] keySlots = new ReusableDecoratedKey[3];
    private int nextKeySlot;

    public BtiCursorIndexWriter(BtiTableWriter writer,
                                ClusteringComparator comparator,
                                AbstractType<?>[] clusteringTypes)
    {
        this.indexWriter = writer.indexWriter;
        this.rowTrie = new RowIndexWriter(comparator, indexWriter.rowIndexWriter, writer.descriptor.version);
        this.rowIndexBlockSize = DatabaseDescriptor.getColumnIndexSize(BtiFormatPartitionWriter.DEFAULT_GRANULARITY);
        this.firstClustering = new ClusteringDescriptor(clusteringTypes);
        this.clusteringTypes = clusteringTypes;
        for (int i = 0; i < keySlots.length; i++)
            keySlots[i] = writer.getPartitioner().createReusableKey(0);
    }

    @Override
    protected void reset()
    {
        rowTrie.reset();
        rowIndexBlockCount = 0;
        blockOpen = false;
        blockStartOpenMarker = DeletionTime.LIVE;
    }

    @Override
    public void rowWritten(UnfilteredDescriptor descriptor, long rowStart, long rowEnd,
                           DeletionTime openMarker) throws IOException
    {
        if (!blockOpen)
        {
            firstClustering.copy(descriptor);
            blockOpen = true;
        }

        /** {@link BtiFormatPartitionWriter#addUnfiltered} */
        if (currentOffsetInPartition(rowEnd) - indexBlockStartOffset >= rowIndexBlockSize)
            addIndexBlock(rowEnd, openMarker, descriptor);
    }

    /** {@link BtiFormatPartitionWriter#addIndexBlock()} */
    private void addIndexBlock(long endOfRowPosition, DeletionTime openMarkerAtEnd,
                               ClusteringDescriptor lastClustering) throws IOException
    {
        IndexInfo info = new IndexInfo(indexBlockStartOffset, blockStartOpenMarker);
        // copies: RowIndexWriter holds the prefixes across add() calls, and the descriptors are reused
        rowTrie.add(firstClustering.toClusteringPrefix(clusteringTypes),
                    lastClustering.toClusteringPrefix(clusteringTypes), info);
        blockOpen = false;
        ++rowIndexBlockCount;
        notePosition(endOfRowPosition);
        // copy: the trie holds the IndexInfo until complete(), and the caller's DeletionTime is
        // a reusable instance
        blockStartOpenMarker = openMarkerAtEnd.isLive() ? DeletionTime.LIVE
                                                        : DeletionTime.build(openMarkerAtEnd.markedForDeleteAt(),
                                                                             openMarkerAtEnd.localDeletionTime());
    }

    @Override
    public void endPartition(ReusableDecoratedKey key, byte[] keyBytes, int keyLength, int headerLength,
                             DeletionTime partitionDeletionTime, long partitionEnd,
                             ClusteringDescriptor lastName) throws IOException
    {
        /** {@link BtiFormatPartitionWriter#finish()} + {@link BtiTableWriter#createRowIndexEntry} */
        // the last row may not fall on a block boundary; cut the final block here
        if (rowIndexBlockCount > 0 && blockOpen)
            addIndexBlock(partitionEnd, DeletionTime.LIVE, lastName);

        // SortedTablePartitionWriter.finish measures the partition length before it writes the
        // end-of-partition marker, and complete() takes that length. partitionEnd here is the
        // position after the marker, so subtract its one byte
        long trieRoot = rowIndexBlockCount > 1 ? rowTrie.complete(partitionEnd - 1 - partitionStart) : -1;
        TrieIndexEntry entry = TrieIndexEntry.create(partitionStart, trieRoot,
                                                     partitionDeletionTime, rowIndexBlockCount);
        ReusableDecoratedKey slot = keySlots[nextKeySlot];
        nextKeySlot = (nextKeySlot + 1) % keySlots.length;
        slot.copyKey(key);
        indexWriter.append(slot, entry);
    }

    @Override
    public void close()
    {
        /** {@link BtiFormatPartitionWriter#close()} */
        // clears the trie builder's in-heap stack and prev state; the Rows.db writer belongs to
        // BtiTableWriter
        rowTrie.close();
    }
}
