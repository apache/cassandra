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
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.columniterator.SSTableSliceIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SliceQueryFilter implements IFilter
{
    private static final Logger logger = LoggerFactory.getLogger(SliceQueryFilter.class);
    public static final Serializer serializer = new Serializer();

    public final ColumnSlice[] slices;
    public final boolean reversed;
    public volatile int count;

    public SliceQueryFilter(ByteBuffer start, ByteBuffer finish, boolean reversed, int count)
    {
        this(new ColumnSlice[] { new ColumnSlice(start, finish) }, reversed, count);
    }

    /**
     * Constructor that accepts multiple slices. All slices are assumed to be in the same direction (forward or
     * reversed).
     */
    public SliceQueryFilter(ColumnSlice[] slices, boolean reversed, int count)
    {
        this.slices = slices;
        this.reversed = reversed;
        this.count = count;
    }

    public OnDiskAtomIterator getMemtableColumnIterator(ColumnFamily cf, DecoratedKey key)
    {
        return Memtable.getSliceIterator(key, cf, this);
    }

    public OnDiskAtomIterator getSSTableColumnIterator(SSTableReader sstable, DecoratedKey key)
    {
        return new SSTableSliceIterator(sstable, key, slices, reversed);
    }

    public OnDiskAtomIterator getSSTableColumnIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry)
    {
        return new SSTableSliceIterator(sstable, file, key, slices, reversed, indexEntry);
    }

    public SuperColumn filterSuperColumn(SuperColumn superColumn, int gcBefore)
    {
        // we clone shallow, then add, under the theory that generally we're interested in a relatively small number of subcolumns.
        // this may be a poor assumption.
        SuperColumn scFiltered = superColumn.cloneMeShallow();
        Iterator<IColumn> subcolumns;
        if (reversed)
        {
            List<IColumn> columnsAsList = new ArrayList<IColumn>(superColumn.getSubColumns());
            subcolumns = Lists.reverse(columnsAsList).iterator();
        }
        else
        {
            subcolumns = superColumn.getSubColumns().iterator();
        }

        // iterate until we get to the "real" start column
        Comparator<ByteBuffer> comparator = reversed ? superColumn.getComparator().reverseComparator : superColumn.getComparator();
        while (subcolumns.hasNext())
        {
            IColumn column = subcolumns.next();
            if (comparator.compare(column.name(), start()) >= 0)
            {
                subcolumns = Iterators.concat(Iterators.singletonIterator(column), subcolumns);
                break;
            }
        }
        // subcolumns is either empty now, or has been redefined in the loop above. either is ok.
        collectReducedColumns(scFiltered, subcolumns, gcBefore);
        return scFiltered;
    }

    public Comparator<IColumn> getColumnComparator(AbstractType<?> comparator)
    {
        return reversed ? comparator.columnReverseComparator : comparator.columnComparator;
    }

    public void collectReducedColumns(IColumnContainer container, Iterator<IColumn> reducedColumns, int gcBefore)
    {
        int liveColumns = 0;
        AbstractType<?> comparator = container.getComparator();

        while (reducedColumns.hasNext())
        {
            if (liveColumns >= count)
                break;

            IColumn column = reducedColumns.next();
            if (logger.isDebugEnabled())
                logger.debug(String.format("collecting %s of %s: %s",
                                           liveColumns, count, column.getString(comparator)));

            // only count live columns towards the `count` criteria
            if (column.isLive()
                && (!container.deletionInfo().isDeleted(column)))
            {
                liveColumns++;
            }

            // but we need to add all non-gc-able columns to the result for read repair:
            if (QueryFilter.isRelevant(column, container, gcBefore))
                container.addColumn(column);
        }
    }

    public ByteBuffer start()
    {
        return this.slices[0].start;
    }

    public ByteBuffer finish()
    {
        return this.slices[slices.length - 1].finish;
    }

    public void setStart(ByteBuffer start)
    {
        assert slices.length == 1;
        this.slices[0] = new ColumnSlice(start, this.slices[0].finish);
    }

    @Override
    public String toString()
    {
        return "SliceQueryFilter [reversed=" + reversed + ", slices=" + Arrays.toString(slices) + ", count=" + count + "]";
    }

    public boolean isReversed()
    {
        return reversed;
    }

    public void updateColumnsLimit(int newLimit)
    {
        count = newLimit;
    }

    public static class Serializer implements IVersionedSerializer<SliceQueryFilter>
    {
        public void serialize(SliceQueryFilter f, DataOutput dos, int version) throws IOException
        {
            if (version < MessagingService.VERSION_12)
            {
                // It's kind of lame, but probably better than throwing an exception
                ColumnSlice slice = new ColumnSlice(f.start(), f.finish());
                ColumnSlice.serializer.serialize(slice, dos, version);
            }
            else
            {
                dos.writeInt(f.slices.length);
                for (ColumnSlice slice : f.slices)
                    ColumnSlice.serializer.serialize(slice, dos, version);
            }
            dos.writeBoolean(f.reversed);
            dos.writeInt(f.count);
        }

        public SliceQueryFilter deserialize(DataInput dis, int version) throws IOException
        {
            ColumnSlice[] slices;
            if (version < MessagingService.VERSION_12)
            {
                slices = new ColumnSlice[]{ ColumnSlice.serializer.deserialize(dis, version) };
            }
            else
            {
                slices = new ColumnSlice[dis.readInt()];
                for (int i = 0; i < slices.length; i++)
                    slices[i] = ColumnSlice.serializer.deserialize(dis, version);
            }
            boolean reversed = dis.readBoolean();
            int count = dis.readInt();
            return new SliceQueryFilter(slices, reversed, count);
        }

        public long serializedSize(SliceQueryFilter f, int version)
        {
            TypeSizes sizes = TypeSizes.NATIVE;

            int size = 0;
            if (version < MessagingService.VERSION_12)
            {
                size += ColumnSlice.serializer.serializedSize(new ColumnSlice(f.start(), f.finish()), version);
            }
            else
            {
                size += sizes.sizeof(f.slices.length);
                for (ColumnSlice slice : f.slices)
                    size += ColumnSlice.serializer.serializedSize(slice, version);
            }
            size += sizes.sizeof(f.reversed);
            size += sizes.sizeof(f.count);
            return size;
        }
    }
}
