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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnSlice
{
    public static final ColumnSlice ALL_COLUMNS = new ColumnSlice(Composites.EMPTY, Composites.EMPTY);
    public static final ColumnSlice[] ALL_COLUMNS_ARRAY = new ColumnSlice[]{ ALL_COLUMNS };

    public final Composite start;
    public final Composite finish;

    public ColumnSlice(Composite start, Composite finish)
    {
        assert start != null && finish != null;
        this.start = start;
        this.finish = finish;
    }

    public boolean isAlwaysEmpty(CellNameType comparator, boolean reversed)
    {
        Comparator<Composite> orderedComparator = reversed ? comparator.reverseComparator() : comparator;
        return !start.isEmpty() && !finish.isEmpty() && orderedComparator.compare(start, finish) > 0;
    }

    public boolean includes(Comparator<Composite> cmp, Composite name)
    {
        return (start.isEmpty() || cmp.compare(start, name) <= 0) && (finish.isEmpty() || cmp.compare(finish, name) >= 0);
    }

    public boolean isBefore(Comparator<Composite> cmp, Composite name)
    {
        return !finish.isEmpty() && cmp.compare(finish, name) < 0;
    }

    public boolean intersects(List<ByteBuffer> minCellNames, List<ByteBuffer> maxCellNames, CellNameType comparator, boolean reversed)
    {
        Composite sStart = reversed ? finish : start;
        Composite sEnd = reversed ? start : finish;

        if (compare(sStart, maxCellNames, comparator, true) > 0 || compare(sEnd, minCellNames, comparator, false) < 0)
            return false;

        // We could safely return true here, but there's a minor optimization: if the first component is restricted
        // to a single value, we can check that the second component falls within the min/max for that component
        // (and repeat for all components).
        for (int i = 0; i < minCellNames.size() && i < maxCellNames.size(); i++)
        {
            AbstractType<?> t = comparator.subtype(i);
            ByteBuffer s = i < sStart.size() ? sStart.get(i) : ByteBufferUtil.EMPTY_BYTE_BUFFER;
            ByteBuffer f = i < sEnd.size() ? sEnd.get(i) : ByteBufferUtil.EMPTY_BYTE_BUFFER;

            // we already know the first component falls within its min/max range (otherwise we wouldn't get here)
            if (i > 0 && (i < sEnd.size() && t.compare(f, minCellNames.get(i)) < 0 ||
                          i < sStart.size() && t.compare(s, maxCellNames.get(i)) > 0))
                return false;

            // if this component isn't equal in the start and finish, we don't need to check any more
            if (i >= sStart.size() || i >= sEnd.size() || t.compare(s, f) != 0)
                break;
        }

        return true;
    }

    /** Helper method for intersects() */
    private int compare(Composite sliceBounds, List<ByteBuffer> sstableBounds, CellNameType comparator, boolean isSliceStart)
    {
        for (int i = 0; i < sstableBounds.size(); i++)
        {
            if (i >= sliceBounds.size())
            {
                // When isSliceStart is true, we're comparing the end of the slice against the min cell name for the sstable,
                // so the slice is something like [(1, 0), (1, 0)], and the sstable max is something like (1, 0, 1).
                // We want to return -1 (slice start is smaller than max column name) so that we say the slice intersects.
                // The opposite is true when dealing with the end slice.  For example, with the same slice and a min
                // cell name of (1, 0, 1), we want to return 1 (slice end is bigger than min column name).
                return isSliceStart ? -1 : 1;
            }

            int comparison = comparator.subtype(i).compare(sliceBounds.get(i), sstableBounds.get(i));
            if (comparison != 0)
                return comparison;
        }

        // the slice bound and sstable bound have been equal in all components so far
        if (sliceBounds.size() > sstableBounds.size())
        {
            // We have the opposite situation from the one described above.  With a slice of [(1, 0), (1, 0)],
            // and a min/max cell name of (1), we want to say the slice start is smaller than the max and the slice
            // end is larger than the min.
            return isSliceStart ? -1 : 1;
        }

        return 0;
    }

    /**
     * Validates that the provided slice array contains only non-overlapped slices valid for a query {@code reversed}
     * or not on a table using {@code comparator}.
     */
    public static boolean validateSlices(ColumnSlice[] slices, CellNameType type, boolean reversed)
    {
        Comparator<Composite> comparator = reversed ? type.reverseComparator() : type;

        for (int i = 0; i < slices.length; i++)
        {
            Composite start = slices[i].start;
            Composite finish = slices[i].finish;

            if (start.isEmpty() || finish.isEmpty())
            {
                if (start.isEmpty() && i > 0)
                    return false;

                if (finish.isEmpty())
                    return i == slices.length - 1;
            }
            else
            {
                // !finish.isEmpty() is imposed by prior loop
                if (i > 0 && comparator.compare(slices[i - 1].finish, start) >= 0)
                    return false;

                if (comparator.compare(start, finish) > 0)
                    return false;
            }
        }
        return true;
    }

    /**
     * Takes an array of slices (potentially overlapping and in any order, though each individual slice must have
     * its start before or equal its end in {@code comparator} orde) and return an equivalent array of non-overlapping
     * slices in {@code comparator order}.
     *
     * @param slices an array of slices. This may be modified by this method.
     * @param comparator the order in which to sort the slices.
     * @return the smallest possible array of non-overlapping slices in {@code compator} order. If the original
     * slices are already non-overlapping and in comparator order, this may or may not return the provided slices
     * directly.
     */
    public static ColumnSlice[] deoverlapSlices(ColumnSlice[] slices, final Comparator<Composite> comparator)
    {
        if (slices.length <= 1)
            return slices;

        Arrays.sort(slices, new Comparator<ColumnSlice>()
        {
            @Override
            public int compare(ColumnSlice s1, ColumnSlice s2)
            {
                if (s1.start.isEmpty() || s2.start.isEmpty())
                {
                    if (s1.start.isEmpty() != s2.start.isEmpty())
                        return s1.start.isEmpty() ? -1 : 1;
                }
                else
                {
                    int c = comparator.compare(s1.start, s2.start);
                    if (c != 0)
                        return c;
                }

                // For the finish, empty always means greater
                return s1.finish.isEmpty() || s2.finish.isEmpty()
                     ? (s1.finish.isEmpty() ? 1 : -1)
                     : comparator.compare(s1.finish, s2.finish);
            }
        });

        List<ColumnSlice> slicesCopy = new ArrayList<>(slices.length);

        ColumnSlice last = slices[0];

        for (int i = 1; i < slices.length; i++)
        {
            ColumnSlice s2 = slices[i];

            boolean includesStart = last.includes(comparator, s2.start);
            boolean includesFinish = s2.finish.isEmpty() ? last.finish.isEmpty() : last.includes(comparator, s2.finish);

            if (includesStart && includesFinish)
                continue;

            if (!includesStart && !includesFinish)
            {
                slicesCopy.add(last);
                last = s2;
                continue;
            }

            if (includesStart)
            {
                last = new ColumnSlice(last.start, s2.finish);
                continue;
            }

            assert !includesFinish;
        }

        slicesCopy.add(last);

        return slicesCopy.toArray(new ColumnSlice[slicesCopy.size()]);
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
        return "[" + ByteBufferUtil.bytesToHex(start.toByteBuffer()) + ", " + ByteBufferUtil.bytesToHex(finish.toByteBuffer()) + "]";
    }

    public static class Serializer implements IVersionedSerializer<ColumnSlice>
    {
        private final CType type;

        public Serializer(CType type)
        {
            this.type = type;
        }

        public void serialize(ColumnSlice cs, DataOutputPlus out, int version) throws IOException
        {
            ISerializer<Composite> serializer = type.serializer();
            serializer.serialize(cs.start, out);
            serializer.serialize(cs.finish, out);
        }

        public ColumnSlice deserialize(DataInput in, int version) throws IOException
        {
            ISerializer<Composite> serializer = type.serializer();
            Composite start = serializer.deserialize(in);
            Composite finish = serializer.deserialize(in);
            return new ColumnSlice(start, finish);
        }

        public long serializedSize(ColumnSlice cs, int version)
        {
            ISerializer<Composite> serializer = type.serializer();
            return serializer.serializedSize(cs.start, TypeSizes.NATIVE) + serializer.serializedSize(cs.finish, TypeSizes.NATIVE);
        }
    }
}
