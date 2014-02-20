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
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.memory.PoolAllocator;

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
        return cmp.compare(start, name) <= 0 && (finish.isEmpty() || cmp.compare(finish, name) >= 0);
    }

    public boolean isBefore(Comparator<Composite> cmp, Composite name)
    {
        return !finish.isEmpty() && cmp.compare(finish, name) < 0;
    }

    public boolean intersects(List<ByteBuffer> minCellNames, List<ByteBuffer> maxCellNames, CellNameType comparator, boolean reversed)
    {
        assert minCellNames.size() == maxCellNames.size();

        Composite sStart = reversed ? finish : start;
        Composite sEnd = reversed ? start : finish;

        for (int i = 0; i < minCellNames.size(); i++)
        {
            AbstractType<?> t = comparator.subtype(i);
            if (  (i < sEnd.size() && t.compare(sEnd.get(i), minCellNames.get(i)) < 0)
               || (i < sStart.size() && t.compare(sStart.get(i), maxCellNames.get(i)) > 0))
                return false;
        }
        return true;
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

        public void serialize(ColumnSlice cs, DataOutput out, int version) throws IOException
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

    public static class NavigableSetIterator extends AbstractIterator<Cell>
    {
        private final NavigableSet<Cell> set;
        private final ColumnSlice[] slices;

        private int idx = 0;
        private Iterator<Cell> currentSlice;

        public NavigableSetIterator(NavigableSet<Cell> set, ColumnSlice[] slices)
        {
            this.set = set;
            this.slices = slices;
        }

        protected Cell computeNext()
        {
            if (currentSlice == null)
            {
                if (idx >= slices.length)
                    return endOfData();

                ColumnSlice slice = slices[idx++];
                // We specialize the case of start == "" and finish = "" because it is slightly more efficient,
                // but also they have a specific meaning (namely, they always extend to the beginning/end of the range).
                if (slice.start.isEmpty())
                {
                    if (slice.finish.isEmpty())
                        currentSlice = set.iterator();
                    else
                        currentSlice = set.headSet(fakeCell(slice.finish), true).iterator();
                }
                else if (slice.finish.isEmpty())
                {
                    currentSlice = set.tailSet(fakeCell(slice.start), true).iterator();
                }
                else
                {
                    currentSlice = set.subSet(fakeCell(slice.start), true, fakeCell(slice.finish), true).iterator();
                }
            }

            if (currentSlice.hasNext())
                return currentSlice.next();

            currentSlice = null;
            return computeNext();
        }
    }

    private static Cell fakeCell(Composite name)
    {
        return new Cell(new FakeCellName(name), ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    /*
    * We need to take a slice (headMap/tailMap/subMap) of a CellName map
    * based on a Composite. While CellName and Composite are comparable
    * and so this should work, I haven't found how to generify it properly.
    * So instead we create a "fake" CellName object that just encapsulate
    * the prefix. I might not be a valid CellName with respect to the CF
    * CellNameType, but this doesn't matter here (since we only care about
    * comparison). This is arguably a bit of a hack.
    */
    private static class FakeCellName extends AbstractComposite implements CellName
    {
        private final Composite prefix;

        private FakeCellName(Composite prefix)
        {
            this.prefix = prefix;
        }

        public int size()
        {
            return prefix.size();
        }

        public boolean isStatic()
        {
            return prefix.isStatic();
        }

        public ByteBuffer get(int i)
        {
            return prefix.get(i);
        }

        public Composite.EOC eoc()
        {
            return prefix.eoc();
        }

        public int clusteringSize()
        {
            throw new UnsupportedOperationException();
        }

        public ColumnIdentifier cql3ColumnName()
        {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer collectionElement()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isCollectionCell()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isSameCQL3RowAs(CellName other)
        {
            throw new UnsupportedOperationException();
        }

        public CellName copy(AbstractAllocator allocator)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long excessHeapSizeExcludingData()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void free(PoolAllocator<?> allocator)
        {
            throw new UnsupportedOperationException();
        }

        public long unsharedHeapSize()
        {
            throw new UnsupportedOperationException();
        }
    }
}
