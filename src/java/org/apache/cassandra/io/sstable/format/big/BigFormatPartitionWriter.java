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

package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.sstable.format.SortedTablePartitionWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.SequentialWriter;

/**
 * Column index builder used by {@link org.apache.cassandra.io.sstable.format.big.BigTableWriter}.
 * For index entries that exceed {@link org.apache.cassandra.config.Config#column_index_cache_size},
 * this uses the serialization logic as in {@link RowIndexEntry}.
 */
public class BigFormatPartitionWriter extends SortedTablePartitionWriter
{
    @VisibleForTesting
    public static final int DEFAULT_GRANULARITY = 64 * 1024;

    // used, if the row-index-entry reaches config switchIndexInfoToBufferThreshold
    private DataOutputBuffer rowIndexEntryBuffer;
    // used to track the total serialized size of indexSamples (unused for buffer)
    private int indexSamplesSerializedSize;
    // used, until the row-index-entry reaches switchIndexInfoToBufferThreshold (from config column_index_cache_size, or default 64k)
    private final List<IndexInfo> indexSamples = new ArrayList<>();

    private DataOutputBuffer reusableBuffer;

    private int columnIndexCount;
    private int[] indexOffsets;

    private final ISerializer<IndexInfo> idxSerializer;

    /** Beyond this limit we switch from storing IndexInfo in the list to directly serializing them into a buffer */
    private final int switchIndexInfoToBufferThreshold;
    /** If a partition grows beyond this size we store inter-partition index data in IndexInfo */
    private final int indexBlockThreshold;

    BigFormatPartitionWriter(SerializationHeader header,
                             SequentialWriter writer,
                             Version version,
                             ISerializer<IndexInfo> indexInfoSerializer)
    {
        this(header, writer, version, indexInfoSerializer, DatabaseDescriptor.getColumnIndexCacheSize(), DatabaseDescriptor.getColumnIndexSize(DEFAULT_GRANULARITY));
    }

    BigFormatPartitionWriter(SerializationHeader header,
                             SequentialWriter writer,
                             Version version,
                             ISerializer<IndexInfo> indexInfoSerializer,
                             int cacheSizeThreshold,
                             int indexSize)
    {
        super(header, writer, version);
        this.idxSerializer = indexInfoSerializer;
        this.switchIndexInfoToBufferThreshold = cacheSizeThreshold;
        this.indexBlockThreshold = indexSize;
    }

    public void reset()
    {
        super.reset();
        this.columnIndexCount = 0;
        this.indexSamplesSerializedSize = 0;
        this.indexSamples.clear();

        if (this.rowIndexEntryBuffer != null)
            this.reusableBuffer = this.rowIndexEntryBuffer;
        this.rowIndexEntryBuffer = null;
    }

    public int getColumnIndexCount()
    {
        return columnIndexCount;
    }

    public ByteBuffer buffer()
    {
        return rowIndexEntryBuffer != null ? rowIndexEntryBuffer.buffer() : null;
    }

    public List<IndexInfo> indexSamples()
    {
        if (indexSamplesSerializedSize + columnIndexCount * TypeSizes.sizeof(0) <= switchIndexInfoToBufferThreshold)
        {
            return indexSamples;
        }

        return null;
    }

    public int[] offsets()
    {
        return indexOffsets != null
               ? Arrays.copyOf(indexOffsets, columnIndexCount)
               : null;
    }

    private void addIndexBlock() throws IOException
    {
        IndexInfo cIndexInfo = new IndexInfo(firstClustering,
                                             lastClustering,
                                             indexBlockStartOffset,
                                             currentOffsetInPartition() - indexBlockStartOffset,
                                             !openMarker.isLive() ? openMarker : null);

        // indexOffsets is used for both shallow (ShallowIndexedEntry) and non-shallow IndexedEntry.
        // For shallow ones, we need it to serialize the offsts in finish().
        // For non-shallow ones, the offsts are passed into IndexedEntry, so we don't have to
        // calculate the offsets again.

        // indexOffsets contains the offsets of the serialized IndexInfo objects.
        // I.e. indexOffsets[0] is always 0 so we don't have to deal with a special handling
        // for index #0 and always subtracting 1 for the index (which could be error-prone).
        if (indexOffsets == null)
            indexOffsets = new int[10];
        else
        {
            if (columnIndexCount >= indexOffsets.length)
                indexOffsets = Arrays.copyOf(indexOffsets, indexOffsets.length + 10);

            //the 0th element is always 0
            if (columnIndexCount == 0)
            {
                indexOffsets[columnIndexCount] = 0;
            }
            else
            {
                indexOffsets[columnIndexCount] =
                rowIndexEntryBuffer != null
                ? Ints.checkedCast(rowIndexEntryBuffer.position())
                : indexSamplesSerializedSize;
            }
        }
        columnIndexCount++;

        // First, we collect the IndexInfo objects until we reach Config.column_index_cache_size in an ArrayList.
        // When column_index_cache_size is reached, we switch to byte-buffer mode.
        if (rowIndexEntryBuffer == null)
        {
            indexSamplesSerializedSize += idxSerializer.serializedSize(cIndexInfo);
            if (indexSamplesSerializedSize + columnIndexCount * TypeSizes.INT_SIZE > switchIndexInfoToBufferThreshold)
            {
                rowIndexEntryBuffer = reuseOrAllocateBuffer();
                // serialize pre-existing samples
                for (IndexInfo indexSample : indexSamples)
                {
                    /** {@link IndexInfo.Serializer#serialize} */
                    idxSerializer.serialize(indexSample, rowIndexEntryBuffer);
                }
                // release pre-existing samples
                indexSamples.clear();
            }
            else
            {
                indexSamples.add(cIndexInfo);
            }
        }
        // don't put an else here since buffer may be allocated in preceding if block
        if (rowIndexEntryBuffer != null)
        {
            /** {@link IndexInfo.Serializer#serialize} */
            idxSerializer.serialize(cIndexInfo, rowIndexEntryBuffer);
        }

        firstClustering = null;
    }

    private DataOutputBuffer reuseOrAllocateBuffer()
    {
        // Check whether a reusable DataOutputBuffer already exists for this
        // ColumnIndex instance and return it.
        if (reusableBuffer != null)
        {
            DataOutputBuffer buffer = reusableBuffer;
            buffer.clear();
            return buffer;
        }
        // don't use the standard RECYCLER as that only recycles up to 1MB and requires proper cleanup
        return new DataOutputBuffer(switchIndexInfoToBufferThreshold * 2);
    }

    @Override
    public void addUnfiltered(Unfiltered unfiltered) throws IOException
    {
        super.addUnfiltered(unfiltered);

        // if we hit the column index size that we have to index after, go ahead and index it.
        long sizeSinceLastIndexBlock = currentOffsetInPartition() - indexBlockStartOffset;
        if (sizeSinceLastIndexBlock >= this.indexBlockThreshold)
            addIndexBlock();
    }

    @Override
    public long finish() throws IOException
    {
        long endPosition = super.finish();

        // It's possible we add no rows, just a top level deletion
        if (written == 0)
            return endPosition;

        // the last column may have fallen on an index boundary already.  if not, index it explicitly.
        if (firstClustering != null)
            addIndexBlock();

        // If we serialize the IndexInfo objects directly in the code above into 'buffer',
        // we have to write the offsts to these here. The offsets have already been collected
        // in indexOffsets[]. buffer is != null, if it exceeds Config.column_index_cache_size.
        // In the other case, when buffer==null, the offsets are serialized in RowIndexEntry.IndexedEntry.serialize().
        if (rowIndexEntryBuffer != null)
        {
            for (int i = 0; i < columnIndexCount; i++)
                rowIndexEntryBuffer.writeInt(indexOffsets[i]);
        }

        // we should always have at least one computed index block, but we only write it out if there is more than that.
        assert columnIndexCount > 0 && getHeaderLength() >= 0;

        return endPosition;
    }

    public int indexInfoSerializedSize()
    {
        return rowIndexEntryBuffer != null
               ? rowIndexEntryBuffer.buffer().limit()
               : indexSamplesSerializedSize + columnIndexCount * TypeSizes.sizeof(0);
    }

    @Override
    public void close()
    {
        // no-op
    }
}