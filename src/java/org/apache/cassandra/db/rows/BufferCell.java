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
package org.apache.cassandra.db.rows;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public class BufferCell extends AbstractCell
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new BufferCell(ColumnDefinition.regularDef("", "", "", ByteType.instance), 0L, 0, 0, ByteBufferUtil.EMPTY_BYTE_BUFFER, null));

    private final long timestamp;
    private final int ttl;
    private final int localDeletionTime;

    private final ByteBuffer value;
    private final CellPath path;

    public BufferCell(ColumnDefinition column, long timestamp, int ttl, int localDeletionTime, ByteBuffer value, CellPath path)
    {
        super(column);
        assert column.isComplex() == (path != null);
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.localDeletionTime = localDeletionTime;
        this.value = value;
        this.path = path;
    }

    public static BufferCell live(CFMetaData metadata, ColumnDefinition column, long timestamp, ByteBuffer value)
    {
        return live(metadata, column, timestamp, value, null);
    }

    public static BufferCell live(CFMetaData metadata, ColumnDefinition column, long timestamp, ByteBuffer value, CellPath path)
    {
        if (metadata.params.defaultTimeToLive != NO_TTL)
            return expiring(column, timestamp, metadata.params.defaultTimeToLive, FBUtilities.nowInSeconds(), value, path);

        return new BufferCell(column, timestamp, NO_TTL, NO_DELETION_TIME, value, path);
    }

    public static BufferCell expiring(ColumnDefinition column, long timestamp, int ttl, int nowInSec, ByteBuffer value)
    {
        return expiring(column, timestamp, ttl, nowInSec, value, null);
    }

    public static BufferCell expiring(ColumnDefinition column, long timestamp, int ttl, int nowInSec, ByteBuffer value, CellPath path)
    {
        assert ttl != NO_TTL;
        return new BufferCell(column, timestamp, ttl, nowInSec + ttl, value, path);
    }

    public static BufferCell tombstone(ColumnDefinition column, long timestamp, int nowInSec)
    {
        return tombstone(column, timestamp, nowInSec, null);
    }

    public static BufferCell tombstone(ColumnDefinition column, long timestamp, int nowInSec, CellPath path)
    {
        return new BufferCell(column, timestamp, NO_TTL, nowInSec, ByteBufferUtil.EMPTY_BYTE_BUFFER, path);
    }

    public boolean isCounterCell()
    {
        return !isTombstone() && column.cellValueType().isCounter();
    }

    public boolean isLive(int nowInSec)
    {
        return localDeletionTime == NO_DELETION_TIME || (ttl != NO_TTL && nowInSec < localDeletionTime);
    }

    public boolean isTombstone()
    {
        return localDeletionTime != NO_DELETION_TIME && ttl == NO_TTL;
    }

    public boolean isExpiring()
    {
        return ttl != NO_TTL;
    }

    public long timestamp()
    {
        return timestamp;
    }

    public int ttl()
    {
        return ttl;
    }

    public int localDeletionTime()
    {
        return localDeletionTime;
    }

    public ByteBuffer value()
    {
        return value;
    }

    public CellPath path()
    {
        return path;
    }

    public Cell withUpdatedValue(ByteBuffer newValue)
    {
        return new BufferCell(column, timestamp, ttl, localDeletionTime, newValue, path);
    }

    public Cell copy(AbstractAllocator allocator)
    {
        if (!value.hasRemaining())
            return this;

        return new BufferCell(column, timestamp, ttl, localDeletionTime, allocator.clone(value), path == null ? null : path.copy(allocator));
    }

    public Cell markCounterLocalToBeCleared()
    {
        if (!isCounterCell())
            return this;

        ByteBuffer marked = CounterContext.instance().markLocalToBeCleared(value());
        return marked == value() ? this : new BufferCell(column, timestamp, ttl, localDeletionTime, marked, path);
    }

    public Cell purge(DeletionPurger purger, int nowInSec)
    {
        if (!isLive(nowInSec))
        {
            if (purger.shouldPurge(timestamp, localDeletionTime))
                return null;

            // We slightly hijack purging to convert expired but not purgeable columns to tombstones. The reason we do that is
            // that once a column has expired it is equivalent to a tombstone but actually using a tombstone is more compact since
            // we don't keep the column value. The reason we do it here is that 1) it's somewhat related to dealing with tombstones
            // so hopefully not too surprising and 2) we want to this and purging at the same places, so it's simpler/more efficient
            // to do both here.
            if (isExpiring())
            {
                // Note that as long as the expiring column and the tombstone put together live longer than GC grace seconds,
                // we'll fulfil our responsibility to repair. See discussion at
                // http://cassandra-user-incubator-apache-org.3065146.n2.nabble.com/repair-compaction-and-tombstone-rows-td7583481.html
                return BufferCell.tombstone(column, timestamp, localDeletionTime - ttl);
            }
        }
        return this;
    }

    public Cell updateAllTimestamp(long newTimestamp)
    {
        return new BufferCell(column, isTombstone() ? newTimestamp - 1 : newTimestamp, ttl, localDeletionTime, value, path);
    }

    public int dataSize()
    {
        return TypeSizes.sizeof(timestamp)
             + TypeSizes.sizeof(ttl)
             + TypeSizes.sizeof(localDeletionTime)
             + value.remaining()
             + (path == null ? 0 : path.dataSize());
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOnHeapExcludingData(value) + (path == null ? 0 : path.unsharedHeapSizeExcludingData());
    }

    /**
     * The serialization format for cell is:
     *     [ flags ][ timestamp ][ deletion time ][    ttl    ][ path size ][ path ][ value size ][ value ]
     *     [   1b  ][ 8b (vint) ][   4b (vint)   ][ 4b (vint) ][ 4b (vint) ][  arb ][  4b (vint) ][  arb  ]
     *
     * where not all field are always present (in fact, only the [ flags ] are guaranteed to be present). The fields have the following
     * meaning:
     *   - [ flags ] is the cell flags. It is a byte for which each bit represents a flag whose meaning is explained below (*_MASK constants)
     *   - [ timestamp ] is the cell timestamp. Present unless the cell has the USE_TIMESTAMP_MASK.
     *   - [ deletion time]: the local deletion time for the cell. Present if either the cell is deleted (IS_DELETED_MASK)
     *       or it is expiring (IS_EXPIRING_MASK) but doesn't have the USE_ROW_TTL_MASK.
     *   - [ ttl ]: the ttl for the cell. Present if the row is expiring (IS_EXPIRING_MASK) but doesn't have the
     *       USE_ROW_TTL_MASK.
     *   - [ value size ] is the size of the [ value ] field. It's present unless either the cell has the HAS_EMPTY_VALUE_MASK, or the value
     *       for columns of this type have a fixed length.
     *   - [ path size ] is the size of the [ path ] field. Present iff this is the cell of a complex column.
     *   - [ value ]: the cell value, unless it has the HAS_EMPTY_VALUE_MASK.
     *   - [ path ]: the cell path if the column this is a cell of is complex.
     */
    static class Serializer implements Cell.Serializer
    {
        private final static int IS_DELETED_MASK             = 0x01; // Whether the cell is a tombstone or not.
        private final static int IS_EXPIRING_MASK            = 0x02; // Whether the cell is expiring.
        private final static int HAS_EMPTY_VALUE_MASK        = 0x04; // Wether the cell has an empty value. This will be the case for tombstone in particular.
        private final static int USE_ROW_TIMESTAMP_MASK      = 0x08; // Wether the cell has the same timestamp than the row this is a cell of.
        private final static int USE_ROW_TTL_MASK            = 0x10; // Wether the cell has the same ttl than the row this is a cell of.

        public void serialize(Cell cell, DataOutputPlus out, LivenessInfo rowLiveness, SerializationHeader header) throws IOException
        {
            assert cell != null;
            boolean hasValue = cell.value().hasRemaining();
            boolean isDeleted = cell.isTombstone();
            boolean isExpiring = cell.isExpiring();
            boolean useRowTimestamp = !rowLiveness.isEmpty() && cell.timestamp() == rowLiveness.timestamp();
            boolean useRowTTL = isExpiring && rowLiveness.isExpiring() && cell.ttl() == rowLiveness.ttl() && cell.localDeletionTime() == rowLiveness.localExpirationTime();
            int flags = 0;
            if (!hasValue)
                flags |= HAS_EMPTY_VALUE_MASK;

            if (isDeleted)
                flags |= IS_DELETED_MASK;
            else if (isExpiring)
                flags |= IS_EXPIRING_MASK;

            if (useRowTimestamp)
                flags |= USE_ROW_TIMESTAMP_MASK;
            if (useRowTTL)
                flags |= USE_ROW_TTL_MASK;

            out.writeByte((byte)flags);

            if (!useRowTimestamp)
                header.writeTimestamp(cell.timestamp(), out);

            if ((isDeleted || isExpiring) && !useRowTTL)
                header.writeLocalDeletionTime(cell.localDeletionTime(), out);
            if (isExpiring && !useRowTTL)
                header.writeTTL(cell.ttl(), out);

            if (cell.column().isComplex())
                cell.column().cellPathSerializer().serialize(cell.path(), out);

            if (hasValue)
                header.getType(cell.column()).writeValue(cell.value(), out);
        }

        public Cell deserialize(DataInputPlus in, LivenessInfo rowLiveness, ColumnDefinition column, SerializationHeader header, SerializationHelper helper) throws IOException
        {
            int flags = in.readUnsignedByte();
            boolean hasValue = (flags & HAS_EMPTY_VALUE_MASK) == 0;
            boolean isDeleted = (flags & IS_DELETED_MASK) != 0;
            boolean isExpiring = (flags & IS_EXPIRING_MASK) != 0;
            boolean useRowTimestamp = (flags & USE_ROW_TIMESTAMP_MASK) != 0;
            boolean useRowTTL = (flags & USE_ROW_TTL_MASK) != 0;

            long timestamp = useRowTimestamp ? rowLiveness.timestamp() : header.readTimestamp(in);

            int localDeletionTime = useRowTTL
                                  ? rowLiveness.localExpirationTime()
                                  : (isDeleted || isExpiring ? header.readLocalDeletionTime(in) : NO_DELETION_TIME);

            int ttl = useRowTTL ? rowLiveness.ttl() : (isExpiring ? header.readTTL(in) : NO_TTL);

            CellPath path = column.isComplex()
                          ? column.cellPathSerializer().deserialize(in)
                          : null;

            boolean isCounter = localDeletionTime == NO_DELETION_TIME && column.type.isCounter();

            ByteBuffer value = ByteBufferUtil.EMPTY_BYTE_BUFFER;
            if (hasValue)
            {
                if (helper.canSkipValue(column) || (path != null && helper.canSkipValue(path)))
                {
                    header.getType(column).skipValue(in);
                }
                else
                {
                    value = header.getType(column).readValue(in);
                    if (isCounter)
                        value = helper.maybeClearCounterValue(value);
                }
            }

            return new BufferCell(column, timestamp, ttl, localDeletionTime, value, path);
        }

        public long serializedSize(Cell cell, LivenessInfo rowLiveness, SerializationHeader header)
        {
            long size = 1; // flags
            boolean hasValue = cell.value().hasRemaining();
            boolean isDeleted = cell.isTombstone();
            boolean isExpiring = cell.isExpiring();
            boolean useRowTimestamp = !rowLiveness.isEmpty() && cell.timestamp() == rowLiveness.timestamp();
            boolean useRowTTL = isExpiring && rowLiveness.isExpiring() && cell.ttl() == rowLiveness.ttl() && cell.localDeletionTime() == rowLiveness.localExpirationTime();

            if (!useRowTimestamp)
                size += header.timestampSerializedSize(cell.timestamp());

            if ((isDeleted || isExpiring) && !useRowTTL)
                size += header.localDeletionTimeSerializedSize(cell.localDeletionTime());
            if (isExpiring && !useRowTTL)
                size += header.ttlSerializedSize(cell.ttl());

            if (cell.column().isComplex())
                size += cell.column().cellPathSerializer().serializedSize(cell.path());

            if (hasValue)
                size += header.getType(cell.column()).writtenLength(cell.value());

            return size;
        }

        // Returns if the skipped cell was an actual cell (i.e. it had its presence flag).
        public boolean skip(DataInputPlus in, ColumnDefinition column, SerializationHeader header) throws IOException
        {
            int flags = in.readUnsignedByte();
            boolean hasValue = (flags & HAS_EMPTY_VALUE_MASK) == 0;
            boolean isDeleted = (flags & IS_DELETED_MASK) != 0;
            boolean isExpiring = (flags & IS_EXPIRING_MASK) != 0;
            boolean useRowTimestamp = (flags & USE_ROW_TIMESTAMP_MASK) != 0;
            boolean useRowTTL = (flags & USE_ROW_TTL_MASK) != 0;

            if (!useRowTimestamp)
                header.skipTimestamp(in);

            if (!useRowTTL && (isDeleted || isExpiring))
                header.skipLocalDeletionTime(in);

            if (!useRowTTL && isExpiring)
                header.skipTTL(in);

            if (column.isComplex())
                column.cellPathSerializer().skip(in);

            if (hasValue)
                header.getType(column).skipValue(in);

            return true;
        }
    }
}
