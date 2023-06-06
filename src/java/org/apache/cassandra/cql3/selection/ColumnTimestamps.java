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

package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Represents a list of timestamps associated to a CQL column. Those timestamps can either be writetimes or TTLs,
 * according to {@link TimestampsType}.
 */
abstract class ColumnTimestamps
{
    /**
     * The timestamps type.
     */
    protected final TimestampsType type;

    protected ColumnTimestamps(TimestampsType type)
    {
        this.type = type;
    }

    /**
     * @return the timestamps type
     */
    public TimestampsType type()
    {
        return type;
    }

    /**
     * Retrieves the timestamps at the specified position.
     *
     * @param index the timestamps position
     * @return the timestamps at the specified position or a {@link #NO_TIMESTAMP}
     */
    public abstract ColumnTimestamps get(int index);

    public abstract ColumnTimestamps max();

    /**
     * Returns a view of the portion of the timestamps within the specified range.
     *
     * @param range the indexes range
     * @return a view of the specified range within this {@link ColumnTimestamps}
     */
    public abstract ColumnTimestamps slice(Range<Integer> range);

    /**
     * Converts the timestamps into their serialized form.
     *
     * @param protocolVersion the protocol version to use for the serialization
     * @return the serialized timestamps
     */
    public abstract ByteBuffer toByteBuffer(ProtocolVersion protocolVersion);

    /**
     * Appends an empty timestamp at the end of this list.
     */
    public abstract void addNoTimestamp();

    /**
     * Appends the timestamp of the specified cell at the end of this list.
     */
    public abstract void addTimestampFrom(Cell<?> cell, long nowInSecond);

    /**
     * Creates a new {@link ColumnTimestamps} instance for the specified column type.
     *
     * @param timestampType the timestamps type
     * @param columnType    the column type
     * @return a {@link ColumnTimestamps} instance for the specified column type
     */
    static ColumnTimestamps newTimestamps(TimestampsType timestampType, AbstractType<?> columnType)
    {
        if (!columnType.isMultiCell())
            return new SingleTimestamps(timestampType);

        // For UserType we know that the size will not change, so we can initialize the array with the proper capacity.
        if (columnType instanceof UserType)
            return new MultipleTimestamps(timestampType, ((UserType) columnType).size());

        return new MultipleTimestamps(timestampType, 0);
    }

    /**
     * The type of represented timestamps.
     */
    public enum TimestampsType
    {
        WRITETIMES
        {
            @Override
            long getTimestamp(Cell<?> cell, long nowInSecond)
            {
                return cell.timestamp();
            }

            @Override
            long defaultValue()
            {
                return Long.MIN_VALUE;
            }

            @Override
            ByteBuffer toByteBuffer(long timestamp)
            {
                return timestamp == defaultValue() ? null : ByteBufferUtil.bytes(timestamp);
            }
        },
        TTLS
        {
            @Override
            long getTimestamp(Cell<?> cell, long nowInSecond)
            {
                if (!cell.isExpiring())
                    return defaultValue();

                long remaining = cell.localDeletionTime() - nowInSecond;
                return remaining >= 0 ? remaining : defaultValue();
            }

            @Override
            long defaultValue()
            {
                return -1;
            }

            @Override
            ByteBuffer toByteBuffer(long timestamp)
            {
                return timestamp == defaultValue() ? null : ByteBufferUtil.bytes((int) timestamp);
            }
        };

        /**
         * Extracts the timestamp from the specified cell.
         *
         * @param cell        the cell
         * @param nowInSecond the query timestamp insecond
         * @return the timestamp corresponding to this type
         */
        abstract long getTimestamp(Cell<?> cell, long nowInSecond);

        /**
         * Returns the value to use when there is no timestamp.
         *
         * @return the value to use when there is no timestamp
         */
        abstract long defaultValue();

        /**
         * Serializes the specified timestamp.
         *
         * @param timestamp the timestamp to serialize
         * @return the bytes corresponding to the specified timestamp
         */
        abstract ByteBuffer toByteBuffer(long timestamp);
    }

    /**
     * A {@link ColumnTimestamps} that doesn't contain any timestamps.
     */
    static final ColumnTimestamps NO_TIMESTAMP = new ColumnTimestamps(null)
    {
        @Override
        public ColumnTimestamps get(int index)
        {
            return this;
        }

        @Override
        public ColumnTimestamps max()
        {
            return this;
        }

        @Override
        public ColumnTimestamps slice(Range<Integer> range)
        {
            return this;
        }

        @Override
        public ByteBuffer toByteBuffer(ProtocolVersion protocolVersion)
        {
            return null;
        }

        @Override
        public void addNoTimestamp()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addTimestampFrom(Cell<?> cell, long nowInSecond)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "no timestamp";
        }
    };

    /**
     * A {@link ColumnTimestamps} that can contains a single timestamp (for columns that aren't multicell).
     */
    private static class SingleTimestamps extends ColumnTimestamps
    {
        protected long timestamp;

        public SingleTimestamps(TimestampsType type)
        {
            this(type, type.defaultValue());
        }

        public SingleTimestamps(TimestampsType type, long timestamp)
        {
            super(type);
            this.timestamp = timestamp;
        }

        @Override
        public void addNoTimestamp()
        {
            timestamp = type.defaultValue();
        }

        @Override
        public void addTimestampFrom(Cell<?> cell, long nowInSecond)
        {
            timestamp = type.getTimestamp(cell, nowInSecond);
        }

        @Override
        public ColumnTimestamps get(int index)
        {
            // If this method is called it means that it is an element selection on a frozen collection/UDT,
            // so we can safely return this Timestamps as all the elements also share that timestamp
            return this;
        }

        @Override
        public ColumnTimestamps max()
        {
            return this;
        }

        @Override
        public ColumnTimestamps slice(Range<Integer> range)
        {
            return range.isEmpty() ? NO_TIMESTAMP : this;
        }

        @Override
        public ByteBuffer toByteBuffer(ProtocolVersion protocolVersion)
        {
            return timestamp == type.defaultValue() ? null : type.toByteBuffer(timestamp);
        }

        @Override
        public String toString()
        {
            return type + ": " + timestamp;
        }
    }

    /**
     * A {@link ColumnTimestamps} that can contain multiple timestamps (for unfrozen collections or UDTs).
     */
    private static final class MultipleTimestamps extends ColumnTimestamps
    {
        private final List<Long> timestamps;

        public MultipleTimestamps(TimestampsType type, int initialCapacity)
        {
            this(type, new ArrayList<>(initialCapacity));
        }

        public MultipleTimestamps(TimestampsType type, List<Long> timestamps)
        {
            super(type);
            this.timestamps = timestamps;
        }

        @Override
        public void addNoTimestamp()
        {
            timestamps.add(type.defaultValue());
        }

        @Override
        public void addTimestampFrom(Cell<?> cell, long nowInSecond)
        {
            timestamps.add(type.getTimestamp(cell, nowInSecond));
        }

        @Override
        public ColumnTimestamps get(int index)
        {
            return timestamps.isEmpty()
                   ? NO_TIMESTAMP
                   : new SingleTimestamps(type, timestamps.get(index));
        }

        @Override
        public ColumnTimestamps max()
        {
            return timestamps.isEmpty()
                   ? NO_TIMESTAMP
                   : new SingleTimestamps(type, Collections.max(timestamps));
        }

        @Override
        public ColumnTimestamps slice(Range<Integer> range)
        {
            if (range.isEmpty())
                return NO_TIMESTAMP;

            // Prepare the "from" argument for the call to List#sublist below. That argument is always specified and
            // inclusive, whereas the range lower bound can be open, closed or not specified.
            int from = 0;
            if (range.hasLowerBound())
            {
                from = range.lowerBoundType() == BoundType.CLOSED
                       ? range.lowerEndpoint() // inclusive range lower bound, inclusive "from" is the same list position
                       : range.lowerEndpoint() + 1; // exclusive range lower bound, inclusive "from" is the next list position
            }

            // Prepare the "to" argument for the call to List#sublist below. That argument is always specified and
            // exclusive, whereas the range upper bound can be open, closed or not specified.
            int to = timestamps.size();
            if (range.hasUpperBound())
            {
                to = range.upperBoundType() == BoundType.CLOSED
                     ? range.upperEndpoint() + 1 // inclusive range upper bound, exclusive "to" is the next list position
                     : range.upperEndpoint(); // exclusive range upper bound, exclusive "to" is the same list position
            }

            return new MultipleTimestamps(type, timestamps.subList(from, to));
        }

        @Override
        public ByteBuffer toByteBuffer(ProtocolVersion protocolVersion)
        {
            if (timestamps.isEmpty())
                return null;

            List<ByteBuffer> buffers = new ArrayList<>(timestamps.size());
            timestamps.forEach(timestamp -> buffers.add(type.toByteBuffer(timestamp)));

            return CollectionSerializer.pack(buffers, timestamps.size());
        }

        @Override
        public String toString()
        {
            return type + ": " + timestamps;
        }
    }
}
