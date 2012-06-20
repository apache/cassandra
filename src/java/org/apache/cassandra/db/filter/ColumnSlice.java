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
package org.apache.cassandra.db.filter;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnSlice
{
    public static final Serializer serializer = new Serializer();

    public static final ColumnSlice ALL_COLUMNS = new ColumnSlice(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER);
    public static final ColumnSlice[] ALL_COLUMNS_ARRAY = new ColumnSlice[]{ ALL_COLUMNS };

    public final ByteBuffer start;
    public final ByteBuffer finish;

    public ColumnSlice(ByteBuffer start, ByteBuffer finish)
    {
        assert start != null && finish != null;
        this.start = start;
        this.finish = finish;
    }

    /**
     * Validate an array of column slices.
     * To be valid, the slices must be sorted and non-overlapping and each slice must be valid.
     *
     * @throws IllegalArgumentException if the input slices are not valid.
     */
    public static void validate(ColumnSlice[] slices, AbstractType<?> comparator, boolean reversed)
    {
        for (int i = 0; i < slices.length; i++)
        {
            ColumnSlice slice = slices[i];
            validate(slice, comparator, reversed);
            if (i > 0)
            {
                if (slices[i - 1].finish.remaining() == 0 || slice.start.remaining() == 0)
                    throw new IllegalArgumentException("Invalid column slices: slices must be sorted and non-overlapping");

                int cmp = comparator.compare(slices[i -1].finish, slice.start);
                if (reversed ? cmp <= 0 : cmp >= 0)
                    throw new IllegalArgumentException("Invalid column slices: slices must be sorted and non-overlapping");
            }
        }
    }

    /**
     * Validate a column slices.
     * To be valid, the slice start must sort before the slice end.
     *
     * @throws IllegalArgumentException if the slice is not valid.
     */
    public static void validate(ColumnSlice slice, AbstractType<?> comparator, boolean reversed)
    {
        Comparator<ByteBuffer> orderedComparator = reversed ? comparator.reverseComparator : comparator;
        if (slice.start.remaining() > 0 && slice.finish.remaining() > 0 && orderedComparator.compare(slice.start, slice.finish) > 0)
            throw new IllegalArgumentException("Slice finish must come after start in traversal order");
    }

    public boolean includes(Comparator<ByteBuffer> cmp, ByteBuffer name)
    {
        return cmp.compare(start, name) <= 0 && (finish.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER) || cmp.compare(finish, name) >= 0);
    }

    @Override
    public final int hashCode()
    {
        int hashCode = 31 + start.hashCode();
        return 31*hashCode + finish.hashCode();
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof ColumnSlice))
            return false;
        ColumnSlice that = (ColumnSlice)o;
        return start.equals(that.start) && finish.equals(that.finish);
    }

    @Override
    public String toString()
    {
        return "[" + ByteBufferUtil.bytesToHex(start) + ", " + ByteBufferUtil.bytesToHex(finish) + "]";
    }

    public static class Serializer implements IVersionedSerializer<ColumnSlice>
    {
        public void serialize(ColumnSlice cs, DataOutput dos, int version) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(cs.start, dos);
            ByteBufferUtil.writeWithShortLength(cs.finish, dos);
        }

        public ColumnSlice deserialize(DataInput dis, int version) throws IOException
        {
            ByteBuffer start = ByteBufferUtil.readWithShortLength(dis);
            ByteBuffer finish = ByteBufferUtil.readWithShortLength(dis);
            return new ColumnSlice(start, finish);
        }

        public long serializedSize(ColumnSlice cs, int version)
        {
            TypeSizes sizes = TypeSizes.NATIVE;

            int startSize = cs.start.remaining();
            int finishSize = cs.finish.remaining();

            int size = 0;
            size += sizes.sizeof((short) startSize) + startSize;
            size += sizes.sizeof((short) finishSize) + finishSize;
            return size;
        }
    }

    public static class NavigableMapIterator extends AbstractIterator<IColumn>
    {
        private final NavigableMap<ByteBuffer, IColumn> map;
        private final ColumnSlice[] slices;

        private int idx = 0;
        private Iterator<IColumn> currentSlice;

        public NavigableMapIterator(NavigableMap<ByteBuffer, IColumn> map, ColumnSlice[] slices)
        {
            this.map = map;
            this.slices = slices;
        }

        protected IColumn computeNext()
        {
            if (currentSlice == null)
            {
                if (idx >= slices.length)
                    return endOfData();

                ColumnSlice slice = slices[idx++];
                // Note: we specialize the case of start == "" and finish = "" because it is slightly more efficient, but also they have a specific
                // meaning (namely, they always extend to the beginning/end of the range).
                if (slice.start.remaining() == 0)
                {
                    if (slice.finish.remaining() == 0)
                        currentSlice = map.values().iterator();
                    else
                        currentSlice = map.headMap(slice.finish, true).values().iterator();
                }
                else if (slice.finish.remaining() == 0)
                {
                    currentSlice = map.tailMap(slice.start, true).values().iterator();
                }
                else
                {
                    currentSlice = map.subMap(slice.start, true, slice.finish, true).values().iterator();
                }
            }

            if (currentSlice.hasNext())
                return currentSlice.next();

            currentSlice = null;
            return computeNext();
        }
    }
}
