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
package org.apache.cassandra.db.partitions;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

public class ArrayBackedCachedPartition extends ArrayBackedPartition implements CachedPartition
{
    private final int createdAtInSec;

    // Note that those fields are really immutable, but we can't easily pass their values to
    // the ctor so they are not final.
    private int cachedLiveRows;
    private int rowsWithNonExpiringCells;

    private int nonTombstoneCellCount;
    private int nonExpiringLiveCells;

    private ArrayBackedCachedPartition(CFMetaData metadata,
                                       DecoratedKey partitionKey,
                                       DeletionTime deletionTime,
                                       PartitionColumns columns,
                                       int initialRowCapacity,
                                       boolean sortable,
                                       int createdAtInSec)
    {
        super(metadata, partitionKey, deletionTime, columns, initialRowCapacity, sortable);
        this.createdAtInSec = createdAtInSec;
    }

    /**
     * Creates an {@code ArrayBackedCachedPartition} holding all the data of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     *
     * @param iterator the iterator got gather in memory.
     * @param nowInSec the time of the creation in seconds. This is the time at which {@link #cachedLiveRows} applies.
     * @return the created partition.
     */
    public static ArrayBackedCachedPartition create(UnfilteredRowIterator iterator, int nowInSec)
    {
        return create(iterator, 4, nowInSec);
    }

    /**
     * Creates an {@code ArrayBackedCachedPartition} holding all the data of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     *
     * @param iterator the iterator got gather in memory.
     * @param initialRowCapacity sizing hint (in rows) to use for the created partition. It should ideally
     * correspond or be a good estimation of the number or rows in {@code iterator}.
     * @param nowInSec the time of the creation in seconds. This is the time at which {@link #cachedLiveRows} applies.
     * @return the created partition.
     */
    public static ArrayBackedCachedPartition create(UnfilteredRowIterator iterator, int initialRowCapacity, int nowInSec)
    {
        ArrayBackedCachedPartition partition = new ArrayBackedCachedPartition(iterator.metadata(),
                                                                              iterator.partitionKey(),
                                                                              iterator.partitionLevelDeletion(),
                                                                              iterator.columns(),
                                                                              initialRowCapacity,
                                                                              iterator.isReverseOrder(),
                                                                              nowInSec);

        partition.staticRow = iterator.staticRow().takeAlias();

        Writer writer = partition.new Writer(nowInSec);
        RangeTombstoneCollector markerCollector = partition.new RangeTombstoneCollector(iterator.isReverseOrder());

        copyAll(iterator, writer, markerCollector, partition);

        return partition;
    }

    public Row lastRow()
    {
        if (rows == 0)
            return null;

        return new InternalReusableRow().setTo(rows - 1);
    }

    /**
     * The number of rows that were live at the time the partition was cached.
     *
     * See {@link ColumnFamilyStore#isFilterFullyCoveredBy} to see why we need this.
     *
     * @return the number of rows in this partition that were live at the time the
     * partition was cached (this can be different from the number of live rows now
     * due to expiring cells).
     */
    public int cachedLiveRows()
    {
        return cachedLiveRows;
    }

    /**
     * The number of rows in this cached partition that have at least one non-expiring
     * non-deleted cell.
     *
     * Note that this is generally not a very meaningful number, but this is used by
     * {@link DataLimits#hasEnoughLiveData} as an optimization.
     *
     * @return the number of row that have at least one non-expiring non-deleted cell.
     */
    public int rowsWithNonExpiringCells()
    {
        return rowsWithNonExpiringCells;
    }

    public int nonTombstoneCellCount()
    {
        return nonTombstoneCellCount;
    }

    public int nonExpiringLiveCells()
    {
        return nonExpiringLiveCells;
    }

    // Writers that collect the values for 'cachedLiveRows', 'rowsWithNonExpiringCells', 'nonTombstoneCellCount'
    // and 'nonExpiringLiveCells'.
    protected class Writer extends AbstractPartitionData.Writer
    {
        private final int nowInSec;

        private boolean hasLiveData;
        private boolean hasNonExpiringCell;

        protected Writer(int nowInSec)
        {
            super(true);
            this.nowInSec = nowInSec;
        }

        @Override
        public void writePartitionKeyLivenessInfo(LivenessInfo info)
        {
            super.writePartitionKeyLivenessInfo(info);
            if (info.isLive(nowInSec))
                hasLiveData = true;
        }

        @Override
        public void writeCell(ColumnDefinition column, boolean isCounter, ByteBuffer value, LivenessInfo info, CellPath path)
        {
            super.writeCell(column, isCounter, value, info, path);

            if (info.isLive(nowInSec))
            {
                hasLiveData = true;
                if (!info.hasTTL())
                {
                    hasNonExpiringCell = true;
                    ++ArrayBackedCachedPartition.this.nonExpiringLiveCells;
                }
            }

            if (!info.hasLocalDeletionTime() || info.hasTTL())
                ++ArrayBackedCachedPartition.this.nonTombstoneCellCount;
        }

        @Override
        public void endOfRow()
        {
            super.endOfRow();
            if (hasLiveData)
                ++ArrayBackedCachedPartition.this.cachedLiveRows;
            if (hasNonExpiringCell)
                ++ArrayBackedCachedPartition.this.rowsWithNonExpiringCells;

            hasLiveData = false;
            hasNonExpiringCell = false;
        }
    }

    static class Serializer implements ISerializer<CachedPartition>
    {
        public void serialize(CachedPartition partition, DataOutputPlus out) throws IOException
        {
            assert partition instanceof ArrayBackedCachedPartition;
            ArrayBackedCachedPartition p = (ArrayBackedCachedPartition)partition;

            out.writeInt(p.createdAtInSec);
            try (UnfilteredRowIterator iter = p.sliceableUnfilteredIterator())
            {
                UnfilteredRowIteratorSerializer.serializer.serialize(iter, out, MessagingService.current_version, p.rows);
            }
        }

        public CachedPartition deserialize(DataInput in) throws IOException
        {
            // Note that it would be slightly simpler to just do
            //   ArrayBackedCachedPiartition.create(UnfilteredRowIteratorSerializer.serializer.deserialize(...));
            // However deserializing the header separatly is not a lot harder and allows us to:
            //   1) get the capacity of the partition so we can size it properly directly
            //   2) saves the creation of a temporary iterator: rows are directly written to the partition, which
            //      is slightly faster.

            int createdAtInSec = in.readInt();

            UnfilteredRowIteratorSerializer.Header h = UnfilteredRowIteratorSerializer.serializer.deserializeHeader(in, MessagingService.current_version, SerializationHelper.Flag.LOCAL);
            assert !h.isReversed && h.rowEstimate >= 0;

            ArrayBackedCachedPartition partition = new ArrayBackedCachedPartition(h.metadata, h.key, h.partitionDeletion, h.sHeader.columns(), h.rowEstimate, false, createdAtInSec);
            partition.staticRow = h.staticRow;

            Writer writer = partition.new Writer(createdAtInSec);
            RangeTombstoneMarker.Writer markerWriter = partition.new RangeTombstoneCollector(false);

            UnfilteredRowIteratorSerializer.serializer.deserialize(in, new SerializationHelper(MessagingService.current_version, SerializationHelper.Flag.LOCAL), h.sHeader, writer, markerWriter);
            return partition;
        }

        public long serializedSize(CachedPartition partition, TypeSizes sizes)
        {
            assert partition instanceof ArrayBackedCachedPartition;
            ArrayBackedCachedPartition p = (ArrayBackedCachedPartition)partition;

            try (UnfilteredRowIterator iter = p.sliceableUnfilteredIterator())
            {
                return sizes.sizeof(p.createdAtInSec)
                     + UnfilteredRowIteratorSerializer.serializer.serializedSize(iter, MessagingService.current_version, p.rows, sizes);
            }
        }
    }
}

