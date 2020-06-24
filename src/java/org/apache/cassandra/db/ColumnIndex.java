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

package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.primitives.Ints;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Column index builder used by {@link org.apache.cassandra.io.sstable.format.big.BigTableWriter}.
 * For index entries that exceed {@link org.apache.cassandra.config.Config#column_index_cache_size_in_kb},
 * this uses the serialization logic as in {@link RowIndexEntry}.
 */
public class ColumnIndex
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

    private final SerializationHelper helper;
    private final SerializationHeader header;
    private final int version;
    private final SequentialWriter writer;
    private long initialPosition;
    private final  ISerializer<IndexInfo> idxSerializer;
    public long headerLength;
    private long startPosition;

    private int written;
    private long previousRowStart;

    private ClusteringPrefix firstClustering;
    private ClusteringPrefix lastClustering;

    private DeletionTime openMarker;

    private int cacheSizeThreshold;

    private final Collection<SSTableFlushObserver> observers;

    public ColumnIndex(SerializationHeader header,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9766
                        SequentialWriter writer,
                        Version version,
                        Collection<SSTableFlushObserver> observers,
                        ISerializer<IndexInfo> indexInfoSerializer)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15389
        this.helper = new SerializationHelper(header);
        this.header = header;
        this.writer = writer;
        this.version = version.correspondingMessagingVersion();
        this.observers = observers;
        this.idxSerializer = indexInfoSerializer;
    }

    public void reset()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10349
        this.initialPosition = writer.position();
        this.headerLength = -1;
        this.startPosition = -1;
        this.previousRowStart = 0;
        this.columnIndexCount = 0;
        this.written = 0;
        this.indexSamplesSerializedSize = 0;
        this.indexSamples.clear();
        this.firstClustering = null;
        this.lastClustering = null;
        this.openMarker = null;

        int newCacheSizeThreshold = DatabaseDescriptor.getColumnIndexCacheSize();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15469
        if (this.buffer != null && this.cacheSizeThreshold == newCacheSizeThreshold)
            this.reusableBuffer = this.buffer;
        this.buffer = null;
        this.cacheSizeThreshold = newCacheSizeThreshold;
    }

    public void buildRowIndex(UnfilteredRowIterator iterator) throws IOException
    {
        writePartitionHeader(iterator);
        this.headerLength = writer.position() - initialPosition;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10349

        while (iterator.hasNext())
            add(iterator.next());

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9766
        finish();
    }

    private void writePartitionHeader(UnfilteredRowIterator iterator) throws IOException
    {
        ByteBufferUtil.writeWithShortLength(iterator.partitionKey().getKey(), writer);
        DeletionTime.serializer.serialize(iterator.partitionLevelDeletion(), writer);
        if (header.hasStatic())
        {
            Row staticRow = iterator.staticRow();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11183

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15389
            UnfilteredSerializer.serializer.serializeStaticRow(staticRow, helper, writer, version);
            if (!observers.isEmpty())
                observers.forEach((o) -> o.nextUnfilteredCluster(staticRow));
        }
    }

    private long currentPosition()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10349
        return writer.position() - initialPosition;
    }

    public ByteBuffer buffer()
    {
        return buffer != null ? buffer.buffer() : null;
    }

    public List<IndexInfo> indexSamples()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15469
        if (indexSamplesSerializedSize + columnIndexCount * TypeSizes.sizeof(0) <= cacheSizeThreshold)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9766
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9705
                                             lastClustering,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10314
                                             startPosition,
                                             currentPosition() - startPosition,
                                             openMarker);

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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9766
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15469
            if (indexSamplesSerializedSize + columnIndexCount * TypeSizes.sizeof(0) > cacheSizeThreshold)
            {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12502
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12502
        if (reusableBuffer != null) {
            DataOutputBuffer buffer = reusableBuffer;
            buffer.clear();
            return buffer;
        }
        // don't use the standard RECYCLER as that only recycles up to 1MB and requires proper cleanup
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15469
        return new DataOutputBuffer(cacheSizeThreshold * 2);
    }

    private void add(Unfiltered unfiltered) throws IOException
    {
        long pos = currentPosition();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10378

        if (firstClustering == null)
        {
            // Beginning of an index block. Remember the start and position
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9705
            firstClustering = unfiltered.clustering();
            startPosition = pos;
        }

        UnfilteredSerializer.serializer.serialize(unfiltered, helper, writer, pos - previousRowStart, version);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15389

        // notify observers about each new row
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10661
        if (!observers.isEmpty())
            observers.forEach((o) -> o.nextUnfilteredCluster(unfiltered));

        lastClustering = unfiltered.clustering();
        previousRowStart = pos;
        ++written;

        if (unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
        {
            RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
            openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
        }

        // if we hit the column index size that we have to index after, go ahead and index it.
        if (currentPosition() - startPosition >= DatabaseDescriptor.getColumnIndexSize())
            addIndexBlock();
    }

    private void finish() throws IOException
    {
        UnfilteredSerializer.serializer.writeEndOfPartition(writer);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9500

        // It's possible we add no rows, just a top level deletion
        if (written == 0)
            return;

        // the last column may have fallen on an index boundary already.  if not, index it explicitly.
        if (firstClustering != null)
            addIndexBlock();

        // If we serialize the IndexInfo objects directly in the code above into 'buffer',
        // we have to write the offsts to these here. The offsets have already been are collected
        // in indexOffsets[]. buffer is != null, if it exceeds Config.column_index_cache_size_in_kb.
        // In the other case, when buffer==null, the offsets are serialized in RowIndexEntry.IndexedEntry.serialize().
        if (buffer != null)
            RowIndexEntry.Serializer.serializeOffsets(buffer, indexOffsets, columnIndexCount);

        // we should always have at least one computed index block, but we only write it out if there is more than that.
        assert columnIndexCount > 0 && headerLength >= 0;
    }

    public int indexInfoSerializedSize()
    {
        return buffer != null
               ? buffer.buffer().limit()
               : indexSamplesSerializedSize + columnIndexCount * TypeSizes.sizeof(0);
    }
}
