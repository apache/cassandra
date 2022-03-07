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
import java.util.*;

import com.google.common.primitives.Ints;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SortedTablePartitionWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.SequentialWriter;

/**
 * Column index builder used by {@link org.apache.cassandra.io.sstable.format.big.BigTableWriter}.
 * For index entries that exceed {@link org.apache.cassandra.config.Config#column_index_cache_size_in_kb},
 * this uses the serialization logic as in {@link BigTableRowIndexEntry}.
 */
public class ColumnIndex extends SortedTablePartitionWriter
{
    // used, if the row-index-entry reaches config column_index_cache_size_in_kb
    private DataOutputBuffer buffer;
    // used to track the size of the serialized size of row-index-entry (unused for buffer)
    private int indexSamplesSerializedSize;
    // used, until the row-index-entry reaches config column_index_cache_size_in_kb
    private final List<IndexInfo> indexSamples = new ArrayList<>();

    private DataOutputBuffer reusableBuffer;

    public int columnIndexCount;
    private int[] indexOffsets;

    private final  ISerializer<IndexInfo> idxSerializer;
    public long headerLength;
    private int cacheSizeThreshold;

    public ColumnIndex(SerializationHeader header,
                        SequentialWriter writer,
                        Version version,
                        Collection<SSTableFlushObserver> observers,
                        ISerializer<IndexInfo> indexInfoSerializer)
    {
        super(header, writer, version, observers);
        this.idxSerializer = indexInfoSerializer;
    }

    @Override
    public void close()
    {
        // nothing to close
    }

    @Override
    public void reset()
    {
        super.reset();
        this.headerLength = -1;
        this.indexSamples.clear();
        this.indexSamplesSerializedSize = 0;
        this.columnIndexCount = 0;

        int newCacheSizeThreshold = DatabaseDescriptor.getColumnIndexCacheSize();
        if (this.buffer != null && this.cacheSizeThreshold == newCacheSizeThreshold)
            this.reusableBuffer = this.buffer;
        this.buffer = null;
        this.cacheSizeThreshold = newCacheSizeThreshold;
    }

    @Override
    public void writePartitionHeader(DecoratedKey partitionKey, DeletionTime partitionLevelDeletion) throws IOException
    {
        super.writePartitionHeader(partitionKey, partitionLevelDeletion);
        this.headerLength = currentPosition();    // this may be updated after the static row is written
    }

    @Override
    protected void doWriteStaticRow(Row staticRow) throws IOException
    {
        super.doWriteStaticRow(staticRow);
        this.headerLength = currentPosition();
    }

    public ByteBuffer buffer()
    {
        return buffer != null ? buffer.buffer() : null;
    }

    public List<IndexInfo> indexSamples()
    {
        if (indexSamplesSerializedSize + columnIndexCount * TypeSizes.sizeof(0) <= cacheSizeThreshold)
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

    protected void addIndexBlock() throws IOException
    {
        IndexInfo cIndexInfo = new IndexInfo(firstClustering,
                                             lastClustering,
                                             startPosition,
                                             currentPosition() - startPosition,
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
                buffer != null
                ? Ints.checkedCast(buffer.position())
                : indexSamplesSerializedSize;
            }
        }
        columnIndexCount++;

        // First, we collect the IndexInfo objects until we reach Config.column_index_cache_size_in_kb in an ArrayList.
        // When column_index_cache_size_in_kb is reached, we switch to byte-buffer mode.
        if (buffer == null)
        {
            indexSamplesSerializedSize += idxSerializer.serializedSize(cIndexInfo);
            if (indexSamplesSerializedSize + columnIndexCount * TypeSizes.sizeof(0) > cacheSizeThreshold)
            {
                buffer = reuseOrAllocateBuffer();
                for (IndexInfo indexSample : indexSamples)
                {
                    idxSerializer.serialize(indexSample, buffer);
                }
            }
            else
            {
                indexSamples.add(cIndexInfo);
            }
        }
        // don't put an else here since buffer may be allocated in preceding if block
        if (buffer != null)
        {
            idxSerializer.serialize(cIndexInfo, buffer);
        }

        firstClustering = null;
    }

    private DataOutputBuffer reuseOrAllocateBuffer()
    {
        // Check whether a reusable DataOutputBuffer already exists for this
        // ColumnIndex instance and return it.
        if (reusableBuffer != null) {
            DataOutputBuffer buffer = reusableBuffer;
            buffer.clear();
            return buffer;
        }
        // don't use the standard RECYCLER as that only recycles up to 1MB and requires proper cleanup
        return new DataOutputBuffer(cacheSizeThreshold * 2);
    }

    @Override
    public long endPartition() throws IOException
    {
        long endPosition = super.endPartition();

        // It's possible we add no rows, just a top level deletion
        if (written == 0)
            return endPosition;

        // the last column may have fallen on an index boundary already.  if not, index it explicitly.
        if (firstClustering != null)
            addIndexBlock();

        // If we serialize the IndexInfo objects directly in the code above into 'buffer',
        // we have to write the offsts to these here. The offsets have already been are collected
        // in indexOffsets[]. buffer is != null, if it exceeds Config.column_index_cache_size_in_kb.
        // In the other case, when buffer==null, the offsets are serialized in RowIndexEntry.IndexedEntry.serialize().
        if (buffer != null)
            BigTableRowIndexEntry.Serializer.serializeOffsets(buffer, indexOffsets, columnIndexCount);

        // we should always have at least one computed index block, but we only write it out if there is more than that.
        assert columnIndexCount > 0 && headerLength >= 0;
        return endPosition;
    }

    public int indexInfoSerializedSize()
    {
        return buffer != null
               ? buffer.buffer().limit()
               : indexSamplesSerializedSize + columnIndexCount * TypeSizes.sizeof(0);
    }
}
