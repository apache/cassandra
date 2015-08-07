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

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataInputPlus;
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

    public abstract Cell withUpdatedValue(ByteBuffer newValue);

    public abstract Cell copy(AbstractAllocator allocator);

    @Override
    // Overrides super type to provide a more precise return type.
    public abstract Cell markCounterLocalToBeCleared();

    @Override
    // Overrides super type to provide a more precise return type.
    public abstract Cell purge(DeletionPurger purger, int nowInSec);

    public interface Serializer
    {
        public void serialize(Cell cell, DataOutputPlus out, LivenessInfo rowLiveness, SerializationHeader header) throws IOException;

        public Cell deserialize(DataInputPlus in, LivenessInfo rowLiveness, ColumnDefinition column, SerializationHeader header, SerializationHelper helper) throws IOException;

        public long serializedSize(Cell cell, LivenessInfo rowLiveness, SerializationHeader header);

        // Returns if the skipped cell was an actual cell (i.e. it had its presence flag).
        public boolean skip(DataInputPlus in, ColumnDefinition column, SerializationHeader header) throws IOException;
    }
}
