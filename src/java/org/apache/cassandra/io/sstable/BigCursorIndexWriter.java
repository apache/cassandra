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
 * BIG-format index production for the cursor writer: promoted index blocks and Index.db entries.
 * The output is byte-identical to the iterator path (BigFormatPartitionWriter,
 * RowIndexEntry.create, BigTableWriter.IndexWriter.append).
 */
public class BigCursorIndexWriter extends CursorIndexWriter
{
    private final BigTableWriter.IndexWriter indexWriter;
    private final DeletionTime.Serializer deletionTimeSerializer;
    // The garbage-free add() overload exists only on the concrete BloomFilter. With
    // bloom_filter_fp_chance = 1.0 FilterFactory hands out the AlwaysPresentFilter instead,
    // whose add() is a no-op. Null here means "nothing to add to".
    private final BloomFilter bloomFilter;
    /** Scratch array for the garbage-free bloom filter add(), sized like {@link BloomFilter#reusableIndexes}. */
    private final long[] reusableIndexes = new long[21];

    private final DataOutputBuffer rowIndexEntries = new DataOutputBuffer();
    private final IntArrayList rowIndexEntriesOffsets = new IntArrayList();
    /** Whether the current index block holds more than one unfiltered. */
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
        // Serialize the block's first clustering now. The descriptor does not survive to the block cut.
        if (currentOffsetInPartition(unfilteredStartPosition) == indexBlockStartOffset || (rowIndexEntryOffset == rowIndexEntries.position()))
        {
            writeClusteringToRowIndexEntries(unfilteredDescriptor);
        }
        else
        {
            hasDistinctLastClustering = true;
        }

        /** {@link BigFormatPartitionWriter#addUnfiltered(org.apache.cassandra.db.rows.Unfiltered)} */
        long indexBlockSize = currentOffsetInPartition(unfilteredEndPosition) - indexBlockStartOffset;
        if (indexBlockSize >= this.indexBlockThreshold)
            addIndexBlock(unfilteredEndPosition, indexBlockSize, openMarker, unfilteredDescriptor);
    }

    /**
     *  See:
     *  {@link BigFormatPartitionWriter#addIndexBlock()}
     *  - {@link org.apache.cassandra.io.sstable.IndexInfo.Serializer#serialize(org.apache.cassandra.io.sstable.IndexInfo, org.apache.cassandra.io.util.DataOutputPlus)}
     *
     * @param lastName the clustering of the block's last unfiltered. {@link #rowWritten} passes the
     *                 live descriptor for a mid-partition cut. {@link #endPartition} passes the
     *                 descriptor the write side detached from the cursor for the trailing cut.
     */
    private void addIndexBlock(long endOfRowPosition, long indexBlockSize, DeletionTime openMarker,
                               ClusteringDescriptor lastName) throws IOException
    {
        if (rowIndexEntriesOffsets.isEmpty() && rowIndexEntryOffset != 0) {
            throw new IllegalStateException();
        }

        /** {@link org.apache.cassandra.io.sstable.IndexInfo.Serializer#serialize(org.apache.cassandra.io.sstable.IndexInfo, org.apache.cassandra.io.util.DataOutputPlus)}*/
        rowIndexEntriesOffsets.addInt(rowIndexEntryOffset);

        // The block's first clustering went into this buffer when the block's first row was
        // written. Descriptors are transient, so that clustering is gone by block-cut time.
        // The IndexInfo wire format is [firstName][lastName][offset][width][openMarker].
        if (!hasDistinctLastClustering)
        {
            // Single-unfiltered block: the first name is also the last, so copy the bytes
            // already written for the first name.
            byte[] entriesData = rowIndexEntries.getData();
            long endOfFirstEntry = rowIndexEntries.position();
            rowIndexEntries.write(entriesData, rowIndexEntryOffset, (int) (endOfFirstEntry - rowIndexEntryOffset));
        }
        else
        {
            // Two or more unfiltereds went into this block. The write side's last unfiltered is
            // therefore this block's last, and belongs to this partition.
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
        // The next block's entry starts at this offset.
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

            // Count the trailing block before the promotion decision. The iterator
            // (BigFormatPartitionWriter.finish + RowIndexEntry.create) promotes when the total
            // block count, including the tail, exceeds 1. A tail size of exactly 1 leaves only
            // the end-of-partition marker since the last cut, so no tail block exists. That is
            // the iterator's firstClustering == null case.
            // The tail width includes the end-of-partition marker byte. The iterator matches,
            // because it indexes the final block after SortedTablePartitionWriter.finish()
            // writes the marker.
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
                if (hasTailBlock) {
                    addIndexBlock(partitionEnd, tailBlockSize, DeletionTime.LIVE, lastName);
                }
                // An indexed entry also carries the partition deletion time.
                /** {@link RowIndexEntry.IndexedEntry#serialize(org.apache.cassandra.io.util.DataOutputPlus, java.nio.ByteBuffer)} */
                int endOfEntries = rowIndexEntries.getLength();
                // Append the header fields after the entries, to measure their size.
                rowIndexEntries.writeUnsignedVInt((long)headerLength);
                deletionTimeSerializer.serialize(partitionDeletionTime, rowIndexEntries);

                rowIndexEntries.writeUnsignedVInt32(rowIndexEntriesOffsets.size()); // number of entries

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
