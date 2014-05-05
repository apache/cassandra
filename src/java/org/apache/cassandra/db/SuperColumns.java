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

import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SuperColumns
{
    public static Iterator<OnDiskAtom> onDiskIterator(DataInput in, int superColumnCount, ColumnSerializer.Flag flag, int expireBefore, CellNameType type)
    {
        return new SCIterator(in, superColumnCount, flag, expireBefore, type);
    }

    public static void deserializerSuperColumnFamily(DataInput in, ColumnFamily cf, ColumnSerializer.Flag flag, int version) throws IOException
    {
        // Note that there was no way to insert a range tombstone in a SCF in 1.2
        cf.delete(cf.getComparator().deletionInfoSerializer().deserialize(in, version));
        assert !cf.deletionInfo().rangeIterator().hasNext();

        Iterator<OnDiskAtom> iter = onDiskIterator(in, in.readInt(), flag, Integer.MIN_VALUE, cf.getComparator());
        while (iter.hasNext())
            cf.addAtom(iter.next());
    }

    private static class SCIterator implements Iterator<OnDiskAtom>
    {
        private final DataInput in;
        private final int scCount;

        private final ColumnSerializer.Flag flag;
        private final int expireBefore;

        private final CellNameType type;

        private int read;
        private ByteBuffer scName;
        private Iterator<Cell> subColumnsIterator;

        private SCIterator(DataInput in, int superColumnCount, ColumnSerializer.Flag flag, int expireBefore, CellNameType type)
        {
            this.in = in;
            this.scCount = superColumnCount;
            this.flag = flag;
            this.expireBefore = expireBefore;
            this.type = type;
        }

        public boolean hasNext()
        {
            return (subColumnsIterator != null && subColumnsIterator.hasNext()) || read < scCount;
        }

        public OnDiskAtom next()
        {
            try
            {
                if (subColumnsIterator != null && subColumnsIterator.hasNext())
                {
                    Cell c = subColumnsIterator.next();
                    return c.withUpdatedName(type.makeCellName(scName, c.name().toByteBuffer()));
                }

                // Read one more super column
                ++read;

                scName = ByteBufferUtil.readWithShortLength(in);
                DeletionInfo delInfo = new DeletionInfo(DeletionTime.serializer.deserialize(in));

                /* read the number of columns */
                int size = in.readInt();
                List<Cell> subCells = new ArrayList<>(size);

                ColumnSerializer colSer = subType(type).columnSerializer();
                for (int i = 0; i < size; ++i)
                    subCells.add(colSer.deserialize(in, flag, expireBefore));

                subColumnsIterator = subCells.iterator();

                // If the SC was deleted, return that first, otherwise return the first subcolumn
                DeletionTime dtime = delInfo.getTopLevelDeletion();
                if (!dtime.equals(DeletionTime.LIVE))
                    return new RangeTombstone(startOf(scName), endOf(scName), dtime);

                return next();
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static CellNameType subType(CellNameType type)
    {
        return new SimpleDenseCellNameType(type.subtype(1));
    }

    public static CellNameType scNameType(CellNameType type)
    {
        return new SimpleDenseCellNameType(type.subtype(0));
    }

    public static AbstractType<?> getComparatorFor(CFMetaData metadata, ByteBuffer superColumn)
    {
        return getComparatorFor(metadata, superColumn != null);
    }

    public static AbstractType<?> getComparatorFor(CFMetaData metadata, boolean subColumn)
    {
        return metadata.isSuper()
             ? metadata.comparator.subtype(subColumn ? 1 : 0)
             : metadata.comparator.asAbstractType();
    }

    // Extract the first component of a columnName, i.e. the super column name
    public static ByteBuffer scName(Composite columnName)
    {
        return columnName.get(0);
    }

    // Extract the 2nd component of a columnName, i.e. the sub-column name
    public static ByteBuffer subName(Composite columnName)
    {
        return columnName.get(1);
    }

    public static Composite startOf(ByteBuffer scName)
    {
        return CellNames.compositeDense(scName).start();
    }

    public static Composite endOf(ByteBuffer scName)
    {
        return CellNames.compositeDense(scName).end();
    }

    public static IDiskAtomFilter fromSCFilter(CellNameType type, ByteBuffer scName, IDiskAtomFilter filter)
    {
        if (filter instanceof NamesQueryFilter)
            return fromSCNamesFilter(type, scName, (NamesQueryFilter)filter);
        else
            return fromSCSliceFilter(type, scName, (SliceQueryFilter)filter);
    }

    public static IDiskAtomFilter fromSCNamesFilter(CellNameType type, ByteBuffer scName, NamesQueryFilter filter)
    {
        if (scName == null)
        {
            ColumnSlice[] slices = new ColumnSlice[filter.columns.size()];
            int i = 0;
            for (CellName name : filter.columns)
            {
                // Note that, because the filter in argument is the one from thrift, 'name' are SimpleDenseCellName.
                // So calling name.slice() would be incorrect, as simple cell names don't handle the EOC properly.
                // This is why we call toByteBuffer() and rebuild a  Composite of the right type before call slice().
                slices[i++] = type.make(name.toByteBuffer()).slice();
            }
            return new SliceQueryFilter(slices, false, slices.length, 1);
        }
        else
        {
            SortedSet<CellName> newColumns = new TreeSet<>(type);
            for (CellName c : filter.columns)
                newColumns.add(type.makeCellName(scName, c.toByteBuffer()));
            return filter.withUpdatedColumns(newColumns);
        }
    }

    public static SliceQueryFilter fromSCSliceFilter(CellNameType type, ByteBuffer scName, SliceQueryFilter filter)
    {
        assert filter.slices.length == 1;
        if (scName == null)
        {
            // The filter is on the super column name
            CBuilder builder = type.builder();
            Composite start = filter.start().isEmpty()
                            ? Composites.EMPTY
                            : builder.buildWith(filter.start().toByteBuffer()).withEOC(filter.reversed ? Composite.EOC.END : Composite.EOC.START);
            Composite finish = filter.finish().isEmpty()
                             ? Composites.EMPTY
                             : builder.buildWith(filter.finish().toByteBuffer()).withEOC(filter.reversed ? Composite.EOC.START : Composite.EOC.END);
            return new SliceQueryFilter(start, finish, filter.reversed, filter.count, 1);
        }
        else
        {
            CBuilder builder = type.builder().add(scName);
            Composite start = filter.start().isEmpty()
                            ? builder.build().withEOC(filter.reversed ? Composite.EOC.END : Composite.EOC.START)
                            : builder.buildWith(filter.start().toByteBuffer());
            Composite end = filter.finish().isEmpty()
                          ? builder.build().withEOC(filter.reversed ? Composite.EOC.START : Composite.EOC.END)
                          : builder.buildWith(filter.finish().toByteBuffer());
            return new SliceQueryFilter(start, end, filter.reversed, filter.count);
        }
    }
}
