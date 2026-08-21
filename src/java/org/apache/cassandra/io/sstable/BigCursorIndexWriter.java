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

import com.google.common.primitives.Ints;

import org.agrona.collections.IntArrayList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.format.big.BigFormatPartitionWriter;
import org.apache.cassandra.io.sstable.format.big.BigTableWriter;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteArrayUtil;

/**
 * BIG-format index production for the cursor writer: promoted index blocks serialized
 * incrementally (IndexInfo wire format) and Index.db entries with the tail-counted
 * promotion decision, byte-identical to the iterator path
 * (BigFormatPartitionWriter + RowIndexEntry.create + BigTableWriter.IndexWriter.append).
 */
public class BigCursorIndexWriter extends CursorIndexWriter
{
    private final BigTableWriter.IndexWriter indexWriter;
    private final DeletionTime.Serializer deletionTimeSerializer;
    // The garbage-free add() overload exists only on the concrete BloomFilter. With
    // bloom_filter_fp_chance = 1.0 FilterFactory hands out the AlwaysPresentFilter instead,
    // whose interface add() is a no-op (the iterator path calls it through IFilter) — null
    // here means "nothing to add to".
    private final BloomFilter bloomFilter;
    /**
     * See: {@link BloomFilter#reusableIndexes}
     */
    private final long[] reusableIndexes = new long[21];

    private final DataOutputBuffer rowIndexEntries = new DataOutputBuffer();
    private final IntArrayList rowIndexEntriesOffsets = new IntArrayList();
    /** Whether the current index block holds more than one unfiltered, so its first name is not also its last. */
    private boolean hasDistinctLastClustering = false;
    private int rowIndexEntryOffset;
    private final int indexBlockThreshold;

    public BigCursorIndexWriter(BigTableWriter.IndexWriter indexWriter,
                                DeletionTime.Serializer deletionTimeSerializer)
    {
        this.indexWriter = indexWriter;
        this.deletionTimeSerializer = deletionTimeSerializer;
        this.indexBlockThreshold = DatabaseDescriptor.getColumnIndexSize(BigFormatPartitionWriter.DEFAULT_GRANULARITY);
        this.bloomFilter = indexWriter.bf instanceof BloomFilter ? (BloomFilter) indexWriter.bf : null;
    }

    @Override
    protected void reset()
    {
        rowIndexEntries.clear();
        rowIndexEntriesOffsets.clear();
        rowIndexEntryOffset = 0;
        hasDistinctLastClustering = false;
    }

    @Override
    public void rowWritten(UnfilteredDescriptor unfilteredDescriptor, long unfilteredStartPosition,
                           long unfilteredEndPosition, DeletionTime openMarker) throws IOException
    {
        // write the first clustering into rowIndexEntries buffer (we will need it unless we never write the first entry)
        if (currentOffsetInPartition(unfilteredStartPosition) == indexBlockStartOffset || (rowIndexEntryOffset == rowIndexEntries.position()))
        {
            writeClusteringToRowIndexEntries(unfilteredDescriptor);
        }
        else
        {
            hasDistinctLastClustering = true;
        }

        /** {@link BigFormatPartitionWriter#addUnfiltered(org.apache.cassandra.db.rows.Unfiltered)} */
        // if we hit the index block size that we have to index after, go ahead and index it.
        long indexBlockSize = currentOffsetInPartition(unfilteredEndPosition) - indexBlockStartOffset;
        if (indexBlockSize >= this.indexBlockThreshold)
            addIndexBlock(unfilteredEndPosition, indexBlockSize, openMarker, unfilteredDescriptor);
    }

    /**
     *  See:
     *  {@link BigFormatPartitionWriter#addIndexBlock()}
     *  - {@link org.apache.cassandra.io.sstable.IndexInfo.Serializer#serialize(org.apache.cassandra.io.sstable.IndexInfo, org.apache.cassandra.io.util.DataOutputPlus)}
     *
     * @param lastName the clustering of the block's last unfiltered. A mid-partition cut happens inside
     *                 {@link #rowWritten}, where that is the live descriptor; the trailing cut happens at
     *                 partition end, where it is the descriptor the write side detached from the cursor.
     */
    private void addIndexBlock(long endOfRowPosition, long indexBlockSize, DeletionTime openMarker,
                               ClusteringDescriptor lastName) throws IOException
    {
        if (rowIndexEntriesOffsets.isEmpty() && rowIndexEntryOffset != 0) {
            throw new IllegalStateException();
        }

        // serialize the index info
        /** {@link org.apache.cassandra.io.sstable.IndexInfo.Serializer#serialize(org.apache.cassandra.io.sstable.IndexInfo, org.apache.cassandra.io.util.DataOutputPlus)}*/
        rowIndexEntriesOffsets.addInt(rowIndexEntryOffset);

        // The block's FIRST clustering was serialized into this buffer eagerly when the
        // block's first row was written (descriptors are transient — by block-cut time that
        // row's clustering no longer exists anywhere else). The IndexInfo wire format wants
        // [firstName][lastName][offset][width][openMarker]; the first name is already in
        // place, so only the last name is appended here.
        if (!hasDistinctLastClustering)
        {
            // single-unfiltered block: first IS last, so duplicate the already-serialized
            // first entry bytes by self-copy from this same buffer (no re-serialization)
            byte[] entriesData = rowIndexEntries.getData();
            long endOfFirstEntry = rowIndexEntries.position();
            rowIndexEntries.write(entriesData, rowIndexEntryOffset, (int) (endOfFirstEntry - rowIndexEntryOffset));
        }
        else
        {
            // hasDistinctLastClustering means two or more unfiltereds have been written to this block, so
            // the write side's last written unfiltered is this block's last and belongs to this partition.
            assert lastName != null : "an index block with a distinct last name has no last name";
            writeClusteringToRowIndexEntries(lastName);
        }
        hasDistinctLastClustering = false;

        rowIndexEntries.writeUnsignedVInt((long)indexBlockStartOffset);
        rowIndexEntries.writeVInt(indexBlockSize - IndexInfo.Serializer.WIDTH_BASE);

        boolean isDeleteTimePresent = !openMarker.isLive();
        rowIndexEntries.writeBoolean(isDeleteTimePresent);
        if (isDeleteTimePresent)
            deletionTimeSerializer.serialize(openMarker, rowIndexEntries);
        // next block starts
        rowIndexEntryOffset = Ints.checkedCast(rowIndexEntries.position());
        notePosition(endOfRowPosition);
    }

    private void writeClusteringToRowIndexEntries(ClusteringDescriptor clustering) throws IOException
    {
        ClusteringPrefix.Kind kind = clustering.clusteringKind();
        rowIndexEntries.writeByte(kind.ordinal());
        if (kind != ClusteringPrefix.Kind.CLUSTERING)
            rowIndexEntries.writeShort(clustering.clusteringColumnsBound());
        rowIndexEntries.write(clustering.clusteringBytes(), 0, clustering.clusteringLength());
    }

    @Override
    public void endPartition(byte[] key, int keyLength, int headerLength,
                             DeletionTime partitionDeletionTime, long partitionEnd,
                             ClusteringDescriptor lastName) throws IOException
    {
        /**
         * {@link BigTableWriter#createRowIndexEntry(org.apache.cassandra.db.DecoratedKey, DeletionTime, long)}
         * {@link BigTableWriter.IndexWriter#append(org.apache.cassandra.db.DecoratedKey, RowIndexEntry, long, java.nio.ByteBuffer)}
         *
         */
        SequentialWriter indexFileWriter = indexWriter.writer;
        if (bloomFilter != null)
            bloomFilter.add(key, 0, keyLength, reusableIndexes);
        long indexStart = indexFileWriter.position();
        try
        {
            ByteArrayUtil.writeWithShortLength(key, 0, keyLength, indexFileWriter);

            indexFileWriter.writeUnsignedVInt(partitionStart);

            // The trailing block must be counted BEFORE deciding whether to promote the index:
            // the iterator (BigFormatPartitionWriter.finish + RowIndexEntry.create) promotes when
            // the total block count INCLUDING the tail is > 1. A tail size of exactly 1 means only
            // the end-of-partition marker remains since the last cut (the iterator's
            // firstClustering == null case) and no tail block exists.
            // The tail width itself includes the end-of-partition marker byte, matching the
            // iterator, which indexes the final block AFTER SortedTablePartitionWriter.finish()
            // has written the marker.
            long tailBlockSize = (partitionEnd - partitionStart) - indexBlockStartOffset;
            boolean hasTailBlock = tailBlockSize > 1;
            int totalBlocks = rowIndexEntriesOffsets.size() + (hasTailBlock ? 1 : 0);

            /** See: {@link org.apache.cassandra.io.sstable.format.big.RowIndexEntry#create} */
            if (totalBlocks <= 1)
            {
                /**
                 * {@link RowIndexEntry#serialize(org.apache.cassandra.io.util.DataOutputPlus, java.nio.ByteBuffer)}
                 */
                indexFileWriter.writeUnsignedVInt32(0); // size
            }
            else {
                // add last block
                if (hasTailBlock) {
                    addIndexBlock(partitionEnd, tailBlockSize, DeletionTime.LIVE, lastName);
                }
                // if we have intermeddiate index info elements we also need to serialize the partitionDeletionTime
                /** {@link RowIndexEntry.IndexedEntry#serialize(org.apache.cassandra.io.util.DataOutputPlus, java.nio.ByteBuffer) */
                // size up to the offsets?
                int endOfEntries = rowIndexEntries.getLength();
                // Write the headerLength, partitionDeletionTime and rowIndexEntriesOffsets.size() after the entries,
                // just to calculate size.
                rowIndexEntries.writeUnsignedVInt((long)headerLength);
                deletionTimeSerializer.serialize(partitionDeletionTime, rowIndexEntries);

                rowIndexEntries.writeUnsignedVInt32(rowIndexEntriesOffsets.size()); // number of entries

                // bytes until offsets
                int entriesAndOffsetsSize = rowIndexEntries.getLength() + rowIndexEntriesOffsets.size() * 4;
                assert entriesAndOffsetsSize > 0;
                indexFileWriter.writeUnsignedVInt32(entriesAndOffsetsSize); // size != 0
                // copy the header elements
                indexFileWriter.write(rowIndexEntries.getData(), endOfEntries, rowIndexEntries.getLength() - endOfEntries);
                indexFileWriter.write(rowIndexEntries.getData(), 0, endOfEntries);
                for (int i = 0; i < rowIndexEntriesOffsets.size(); i++)
                {
                    int offset = rowIndexEntriesOffsets.getInt(i);
                    indexFileWriter.writeInt(offset);
                }
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, indexFileWriter.getPath());
        }
        indexWriter.summary.maybeAddEntry(key, 0, keyLength, indexStart);
    }
}
