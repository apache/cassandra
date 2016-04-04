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
import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnIndex
{
    public final long partitionHeaderLength;
    public final List<IndexHelper.IndexInfo> columnsIndex;

    private static final ColumnIndex EMPTY = new ColumnIndex(-1, Collections.<IndexHelper.IndexInfo>emptyList());

    private ColumnIndex(long partitionHeaderLength, List<IndexHelper.IndexInfo> columnsIndex)
    {
        assert columnsIndex != null;

        this.partitionHeaderLength = partitionHeaderLength;
        this.columnsIndex = columnsIndex;
    }

    public static ColumnIndex writeAndBuildIndex(UnfilteredRowIterator iterator,
                                                 SequentialWriter output,
                                                 SerializationHeader header,
                                                 Collection<SSTableFlushObserver> observers,
                                                 Version version) throws IOException
    {
        assert !iterator.isEmpty() && version.storeRows();

        Builder builder = new Builder(iterator, output, header, observers, version.correspondingMessagingVersion());
        return builder.build();
    }

    @VisibleForTesting
    public static ColumnIndex nothing()
    {
        return EMPTY;
    }

    /**
     * Help to create an index for a column family based on size of columns,
     * and write said columns to disk.
     */
    private static class Builder
    {
        private final UnfilteredRowIterator iterator;
        private final SequentialWriter writer;
        private final SerializationHeader header;
        private final int version;

        private final List<IndexHelper.IndexInfo> columnsIndex = new ArrayList<>();
        private final long initialPosition;
        private long headerLength = -1;

        private long startPosition = -1;

        private int written;
        private long previousRowStart;

        private ClusteringPrefix firstClustering;
        private ClusteringPrefix lastClustering;

        private DeletionTime openMarker;

        private final Collection<SSTableFlushObserver> observers;

        public Builder(UnfilteredRowIterator iterator,
                       SequentialWriter writer,
                       SerializationHeader header,
                       Collection<SSTableFlushObserver> observers,
                       int version)
        {
            this.iterator = iterator;
            this.writer = writer;
            this.header = header;
            this.version = version;
            this.observers = observers == null ? Collections.emptyList() : observers;
            this.initialPosition = writer.position();
        }

        private void writePartitionHeader(UnfilteredRowIterator iterator) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(iterator.partitionKey().getKey(), writer);
            DeletionTime.serializer.serialize(iterator.partitionLevelDeletion(), writer);
            if (header.hasStatic())
            {
                Row staticRow = iterator.staticRow();

                UnfilteredSerializer.serializer.serializeStaticRow(staticRow, header, writer, version);
                if (!observers.isEmpty())
                    observers.forEach((o) -> o.nextUnfilteredCluster(staticRow));
            }
        }

        public ColumnIndex build() throws IOException
        {
            writePartitionHeader(iterator);
            this.headerLength = writer.position() - initialPosition;

            while (iterator.hasNext())
                add(iterator.next());

            return close();
        }

        private long currentPosition()
        {
            return writer.position() - initialPosition;
        }

        private void addIndexBlock()
        {
            IndexHelper.IndexInfo cIndexInfo = new IndexHelper.IndexInfo(firstClustering,
                                                                         lastClustering,
                                                                         startPosition,
                                                                         currentPosition() - startPosition,
                                                                         openMarker);
            columnsIndex.add(cIndexInfo);
            firstClustering = null;
        }

        private void add(Unfiltered unfiltered) throws IOException
        {
            long pos = currentPosition();

            if (firstClustering == null)
            {
                // Beginning of an index block. Remember the start and position
                firstClustering = unfiltered.clustering();
                startPosition = pos;
            }

            UnfilteredSerializer.serializer.serialize(unfiltered, header, writer, pos - previousRowStart, version);

            // notify observers about each new row
            if (!observers.isEmpty())
                observers.forEach((o) -> o.nextUnfilteredCluster(unfiltered));

            lastClustering = unfiltered.clustering();
            previousRowStart = pos;
            ++written;

            if (unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
            {
                RangeTombstoneMarker marker = (RangeTombstoneMarker)unfiltered;
                openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
            }

            // if we hit the column index size that we have to index after, go ahead and index it.
            if (currentPosition() - startPosition >= DatabaseDescriptor.getColumnIndexSize())
                addIndexBlock();

        }

        private ColumnIndex close() throws IOException
        {
            UnfilteredSerializer.serializer.writeEndOfPartition(writer);

            // It's possible we add no rows, just a top level deletion
            if (written == 0)
                return ColumnIndex.EMPTY;

            // the last column may have fallen on an index boundary already.  if not, index it explicitly.
            if (firstClustering != null)
                addIndexBlock();

            // we should always have at least one computed index block, but we only write it out if there is more than that.
            assert columnsIndex.size() > 0 && headerLength >= 0;
            return new ColumnIndex(headerLength, columnsIndex);
        }
    }
}
