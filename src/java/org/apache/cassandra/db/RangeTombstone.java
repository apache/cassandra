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
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.memory.AbstractAllocator;


/**
 * A range tombstone is a tombstone that covers a slice/range of rows.
 * <p>
 * Note that in most of the storage engine, a range tombstone is actually represented by its separated
 * opening and closing bound, see {@link RangeTombstoneMarker}. So in practice, this is only used when
 * full partitions are materialized in memory in a {@code Partition} object, and more precisely through
 * the use of a {@code RangeTombstoneList} in a {@code DeletionInfo} object.
 */
public class RangeTombstone
{
    private final Slice slice;
    private final DeletionTime deletion;

    public RangeTombstone(Slice slice, DeletionTime deletion)
    {
        this.slice = slice;
        this.deletion = deletion;
    }

    /**
     * The slice of rows that is deleted by this range tombstone.
     *
     * @return the slice of rows that is deleted by this range tombstone.
     */
    public Slice deletedSlice()
    {
        return slice;
    }

    /**
     * The deletion time for this (range) tombstone.
     *
     * @return the deletion time for this range tombstone.
     */
    public DeletionTime deletionTime()
    {
        return deletion;
    }

    public String toString(ClusteringComparator comparator)
    {
        return slice.toString(comparator) + '@' + deletion;
    }

    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof RangeTombstone))
            return false;

        RangeTombstone that = (RangeTombstone)other;
        return this.deletedSlice().equals(that.deletedSlice())
            && this.deletionTime().equals(that.deletionTime());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(deletedSlice(), deletionTime());
    }

    /**
     * The bound of a range tombstone.
     * <p>
     * This is the same than for a slice but it includes "boundaries" between ranges. A boundary simply condensed
     * a close and an opening "bound" into a single object. There is 2 main reasons for these "shortcut" boundaries:
     *   1) When merging multiple iterators having range tombstones (that are represented by their start and end markers),
     *      we need to know when a range is close on an iterator, if it is reopened right away. Otherwise, we cannot
     *      easily produce the markers on the merged iterators within risking to fail the sorting guarantees of an
     *      iterator. See this comment for more details: https://goo.gl/yyB5mR.
     *   2) This saves some storage space.
     */
    public static class Bound extends Slice.Bound
    {
        public static final Serializer serializer = new Serializer();

        /** The smallest start bound, i.e. the one that starts before any row. */
        public static final Bound BOTTOM = new Bound(Kind.INCL_START_BOUND, EMPTY_VALUES_ARRAY);
        /** The biggest end bound, i.e. the one that ends after any row. */
        public static final Bound TOP = new Bound(Kind.INCL_END_BOUND, EMPTY_VALUES_ARRAY);

        public Bound(Kind kind, ByteBuffer[] values)
        {
            super(kind, values);
            assert values.length > 0 || !kind.isBoundary();
        }

        public boolean isBoundary()
        {
            return kind.isBoundary();
        }

        public boolean isOpen(boolean reversed)
        {
            return kind.isOpen(reversed);
        }

        public boolean isClose(boolean reversed)
        {
            return kind.isClose(reversed);
        }

        public static RangeTombstone.Bound inclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
        {
            return new Bound(reversed ? Kind.INCL_END_BOUND : Kind.INCL_START_BOUND, boundValues);
        }

        public static RangeTombstone.Bound exclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
        {
            return new Bound(reversed ? Kind.EXCL_END_BOUND : Kind.EXCL_START_BOUND, boundValues);
        }

        public static RangeTombstone.Bound inclusiveClose(boolean reversed, ByteBuffer[] boundValues)
        {
            return new Bound(reversed ? Kind.INCL_START_BOUND : Kind.INCL_END_BOUND, boundValues);
        }

        public static RangeTombstone.Bound exclusiveClose(boolean reversed, ByteBuffer[] boundValues)
        {
            return new Bound(reversed ? Kind.EXCL_START_BOUND : Kind.EXCL_END_BOUND, boundValues);
        }

        public static RangeTombstone.Bound inclusiveCloseExclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
        {
            return new Bound(reversed ? Kind.EXCL_END_INCL_START_BOUNDARY : Kind.INCL_END_EXCL_START_BOUNDARY, boundValues);
        }

        public static RangeTombstone.Bound exclusiveCloseInclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
        {
            return new Bound(reversed ? Kind.INCL_END_EXCL_START_BOUNDARY : Kind.EXCL_END_INCL_START_BOUNDARY, boundValues);
        }

        public static RangeTombstone.Bound fromSliceBound(Slice.Bound sliceBound)
        {
            return new RangeTombstone.Bound(sliceBound.kind(), sliceBound.getRawValues());
        }

        public RangeTombstone.Bound copy(AbstractAllocator allocator)
        {
            ByteBuffer[] newValues = new ByteBuffer[size()];
            for (int i = 0; i < size(); i++)
                newValues[i] = allocator.clone(get(i));
            return new Bound(kind(), newValues);
        }

        @Override
        public Bound withNewKind(Kind kind)
        {
            return new Bound(kind, values);
        }

        public static class Serializer
        {
            public void serialize(RangeTombstone.Bound bound, DataOutputPlus out, int version, List<AbstractType<?>> types) throws IOException
            {
                out.writeByte(bound.kind().ordinal());
                out.writeShort(bound.size());
                ClusteringPrefix.serializer.serializeValuesWithoutSize(bound, out, version, types);
            }

            public long serializedSize(RangeTombstone.Bound bound, int version, List<AbstractType<?>> types)
            {
                return 1 // kind ordinal
                     + TypeSizes.sizeof((short)bound.size())
                     + ClusteringPrefix.serializer.valuesWithoutSizeSerializedSize(bound, version, types);
            }

            public RangeTombstone.Bound deserialize(DataInputPlus in, int version, List<AbstractType<?>> types) throws IOException
            {
                Kind kind = Kind.values()[in.readByte()];
                return deserializeValues(in, kind, version, types);
            }

            public RangeTombstone.Bound deserializeValues(DataInputPlus in, Kind kind, int version,
                    List<AbstractType<?>> types) throws IOException
            {
                int size = in.readUnsignedShort();
                if (size == 0)
                    return kind.isStart() ? BOTTOM : TOP;

                ByteBuffer[] values = ClusteringPrefix.serializer.deserializeValuesWithoutSize(in, size, version, types);
                return new RangeTombstone.Bound(kind, values);
            }
        }
    }
}
