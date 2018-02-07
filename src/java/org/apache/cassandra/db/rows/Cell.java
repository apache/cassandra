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
import java.util.Comparator;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * A cell is our atomic unit for a single value of a single column.
 * <p>
 * A cell always holds at least a timestamp that gives us how the cell reconcile. We then
 * have 3 main types of cells:
 *   1) live regular cells: those will also have a value and, if for a complex column, a path.
 *   2) expiring cells: on top of regular cells, those have a ttl and a local deletion time (when they are expired).
 *   3) tombstone cells: those won't have value, but they have a local deletion time (when the tombstone was created).
 */
public abstract class Cell extends ColumnData
{
    public static final int NO_TTL = 0;
    public static final int NO_DELETION_TIME = Integer.MAX_VALUE;
    public static final int MAX_DELETION_TIME = Integer.MAX_VALUE - 1;

    public final static Comparator<Cell> comparator = (c1, c2) ->
    {
        int cmp = c1.column().compareTo(c2.column());
        if (cmp != 0)
            return cmp;

        Comparator<CellPath> pathComparator = c1.column().cellPathComparator();
        return pathComparator == null ? 0 : pathComparator.compare(c1.path(), c2.path());
    };

    public static final Serializer serializer = new BufferCell.Serializer();

    protected Cell(ColumnDefinition column)
    {
        super(column);
    }

    /**
     * Whether the cell is a counter cell or not.
     *
     * @return whether the cell is a counter cell or not.
     */
    public abstract boolean isCounterCell();

    /**
     * The cell value.
     *
     * @return the cell value.
     */
    public abstract ByteBuffer value();

    /**
     * The cell timestamp.
     * <p>
     * @return the cell timestamp.
     */
    public abstract long timestamp();

    /**
     * The cell ttl.
     *
     * @return the cell ttl, or {@code NO_TTL} if the cell isn't an expiring one.
     */
    public abstract int ttl();

    /**
     * The cell local deletion time.
     *
     * @return the cell local deletion time, or {@code NO_DELETION_TIME} if the cell is neither
     * a tombstone nor an expiring one.
     */
    public abstract int localDeletionTime();

    /**
     * Whether the cell is a tombstone or not.
     *
     * @return whether the cell is a tombstone or not.
     */
    public abstract boolean isTombstone();

    /**
     * Whether the cell is an expiring one or not.
     * <p>
     * Note that this only correspond to whether the cell liveness info
     * have a TTL or not, but doesn't tells whether the cell is already expired
     * or not. You should use {@link #isLive} for that latter information.
     *
     * @return whether the cell is an expiring one or not.
     */
    public abstract boolean isExpiring();

    /**
     * Whether the cell is live or not given the current time.
     *
     * @param nowInSec the current time in seconds. This is used to
     * decide if an expiring cell is expired or live.
     * @return whether the cell is live or not at {@code nowInSec}.
     */
    public abstract boolean isLive(int nowInSec);

    /**
     * For cells belonging to complex types (non-frozen collection and UDT), the
     * path to the cell.
     *
     * @return the cell path for cells of complex column, and {@code null} for other cells.
     */
    public abstract CellPath path();

    public abstract Cell withUpdatedColumn(ColumnDefinition newColumn);

    public abstract Cell withUpdatedValue(ByteBuffer newValue);

    public abstract Cell withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, int newLocalDeletionTime);

    public abstract Cell copy(AbstractAllocator allocator);

    @Override
    // Overrides super type to provide a more precise return type.
    public abstract Cell markCounterLocalToBeCleared();

    @Override
    // Overrides super type to provide a more precise return type.
    public abstract Cell purge(DeletionPurger purger, int nowInSec);

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
    static class Serializer
    {
        private final static int IS_DELETED_MASK             = 0x01; // Whether the cell is a tombstone or not.
        private final static int IS_EXPIRING_MASK            = 0x02; // Whether the cell is expiring.
        private final static int HAS_EMPTY_VALUE_MASK        = 0x04; // Wether the cell has an empty value. This will be the case for tombstone in particular.
        private final static int USE_ROW_TIMESTAMP_MASK      = 0x08; // Wether the cell has the same timestamp than the row this is a cell of.
        private final static int USE_ROW_TTL_MASK            = 0x10; // Wether the cell has the same ttl than the row this is a cell of.

        public void serialize(Cell cell, ColumnDefinition column, DataOutputPlus out, LivenessInfo rowLiveness, SerializationHeader header) throws IOException
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

            if (column.isComplex())
                column.cellPathSerializer().serialize(cell.path(), out);

            if (hasValue)
                header.getType(column).writeValue(cell.value(), out);
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

            ByteBuffer value = ByteBufferUtil.EMPTY_BYTE_BUFFER;
            if (hasValue)
            {
                if (helper.canSkipValue(column) || (path != null && helper.canSkipValue(path)))
                {
                    header.getType(column).skipValue(in);
                }
                else
                {
                    boolean isCounter = localDeletionTime == NO_DELETION_TIME && column.type.isCounter();

                    value = header.getType(column).readValue(in, DatabaseDescriptor.getMaxValueSize());
                    if (isCounter)
                        value = helper.maybeClearCounterValue(value);
                }
            }

            return new BufferCell(column, timestamp, ttl, localDeletionTime, value, path);
        }

        public long serializedSize(Cell cell, ColumnDefinition column, LivenessInfo rowLiveness, SerializationHeader header)
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

            if (column.isComplex())
                size += column.cellPathSerializer().serializedSize(cell.path());

            if (hasValue)
                size += header.getType(column).writtenLength(cell.value());

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
