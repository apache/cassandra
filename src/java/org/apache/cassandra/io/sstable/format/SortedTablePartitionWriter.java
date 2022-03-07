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

package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
import java.util.Collection;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.format.trieindex.TrieIndexSSTableWriter;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Partition writer used by {@link TrieIndexSSTableWriter}.
 *
 * Writes all passed data to the given SequentialWriter and if necessary builds a RowIndex by constructing an entry
 * for each row within a partition that follows {@link org.apache.cassandra.config.Config#column_index_cache_size_in_kb}
 * kilobytes of written data.
 */
public abstract class SortedTablePartitionWriter implements AutoCloseable
{
    private final UnfilteredSerializer unfilteredSerializer;
    private final SerializationHeader header;
    private final SequentialWriter writer;
    private final Collection<SSTableFlushObserver> observers;
    private final SerializationHelper helper;
    private final int version;

    protected long initialPosition;
    protected long startPosition;

    protected int written;
    protected long previousRowStart;

    protected ClusteringPrefix<?> firstClustering;
    protected ClusteringPrefix<?> lastClustering;

    protected DeletionTime openMarker = DeletionTime.LIVE;
    protected DeletionTime startOpenMarker = DeletionTime.LIVE;

    // Sequence control, also used to add empty static row if `addStaticRow` is not called.
    private enum State
    {
        AWAITING_PARTITION_HEADER,
        AWAITING_STATIC_ROW,
        AWAITING_ROWS,
        COMPLETED
    }
    State state;

    protected SortedTablePartitionWriter(SerializationHeader header,
                                         SequentialWriter writer,
                                         Version version,
                                         Collection<SSTableFlushObserver> observers)
    {
        this.header = header;
        this.writer = writer;
        this.observers = observers;
        this.unfilteredSerializer = UnfilteredSerializer.serializer;
        this.helper = new SerializationHelper(header);
        this.version = version.correspondingMessagingVersion();
    }

    public void reset()
    {
        this.initialPosition = writer.position();
        this.startPosition = -1;
        this.previousRowStart = 0;
        this.written = 0;
        this.firstClustering = null;
        this.lastClustering = null;
        this.openMarker = DeletionTime.LIVE;
        this.state = State.AWAITING_PARTITION_HEADER;
    }

    @VisibleForTesting
    public void buildRowIndex(UnfilteredRowIterator iterator) throws IOException
    {
        reset();
        writePartitionHeader(iterator.partitionKey(), iterator.partitionLevelDeletion());
        if (!iterator.staticRow().isEmpty())
            addUnfiltered(iterator.staticRow());

        while (iterator.hasNext())
            addUnfiltered(iterator.next());

        endPartition();
    }

    public void writePartitionHeader(DecoratedKey partitionKey, DeletionTime partitionLevelDeletion) throws IOException
    {
        assert state == State.AWAITING_PARTITION_HEADER;
        ByteBufferUtil.writeWithShortLength(partitionKey.getKey(), writer);

        long partitionDeletionPosition = writer.position();
        DeletionTime.serializer.serialize(partitionLevelDeletion, writer);
        if (!observers.isEmpty())
            observers.forEach((o) -> o.partitionLevelDeletion(partitionLevelDeletion, partitionDeletionPosition));
        state = header.hasStatic() ? State.AWAITING_STATIC_ROW : State.AWAITING_ROWS;
    }

    protected void doWriteStaticRow(Row staticRow) throws IOException
    {
        assert state == State.AWAITING_STATIC_ROW;
        long staticRowPosition = writer.position();
        UnfilteredSerializer.serializer.serializeStaticRow(staticRow, helper, writer, version);
        if (!observers.isEmpty())
            observers.forEach(o -> o.staticRow(staticRow, staticRowPosition));
        state = State.AWAITING_ROWS;
    }

    public void addUnfiltered(Unfiltered unfiltered) throws IOException
    {
        if (state == State.AWAITING_STATIC_ROW)
        {
            if (unfiltered.isRow() && ((Row) unfiltered).isStatic())
            {
                doWriteStaticRow((Row) unfiltered);
                return;
            }

            doWriteStaticRow(Rows.EMPTY_STATIC_ROW);
        }

        assert state == State.AWAITING_ROWS;

        long pos = currentPosition();

        if (firstClustering == null)
        {
            // Beginning of an index block. Remember the start and position
            firstClustering = unfiltered.clustering();
            startOpenMarker = openMarker;
            startPosition = pos;
        }

        long unfilteredPosition = writer.position();
        unfilteredSerializer.serialize(unfiltered, helper, writer, pos - previousRowStart, version);

        // notify observers about each new row
        if (!observers.isEmpty())
            observers.forEach(o -> o.nextUnfilteredCluster(unfiltered, unfilteredPosition));

        lastClustering = unfiltered.clustering();
        previousRowStart = pos;
        ++written;

        if (unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
        {
            RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
            openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : DeletionTime.LIVE;
        }

        // if we hit the row index size that we have to index after, go ahead and index it.
        if (currentPosition() - startPosition >= DatabaseDescriptor.getColumnIndexSize())
            addIndexBlock();
    }

    public long endPartition() throws IOException
    {
        if (state == State.AWAITING_STATIC_ROW)
            doWriteStaticRow(Rows.EMPTY_STATIC_ROW);
        assert state == State.AWAITING_ROWS;
        state = State.COMPLETED;

        long endPosition = currentPosition();
        unfilteredSerializer.writeEndOfPartition(writer);

        return endPosition;
    }

    protected long currentPosition()
    {
        return writer.position() - initialPosition;
    }

    protected abstract void addIndexBlock() throws IOException;
}
