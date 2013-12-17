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
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    public static void serializeSuperColumnFamily(ColumnFamily scf, DataOutput out, int version) throws IOException
    {
        /*
         * There is 2 complications:
         *   1) We need to know the number of super columns in the column
         *   family to write in the header (so we do a first pass to group
         *   columns before serializing).
         *   2) For deletion infos, we need to figure out which are top-level
         *   deletions and which are super columns deletions (i.e. the
         *   subcolumns range deletions).
         */
        DeletionInfo delInfo = scf.deletionInfo();
        Map<CellName, List<Cell>> scMap = groupSuperColumns(scf);

        // Actually Serialize
        scf.getComparator().deletionInfoSerializer().serialize(new DeletionInfo(delInfo.getTopLevelDeletion()), out, version);
        out.writeInt(scMap.size());

        CellNameType subComparator = subType(scf.getComparator());
        for (Map.Entry<CellName, List<Cell>> entry : scMap.entrySet())
        {
            scf.getComparator().cellSerializer().serialize(entry.getKey(), out);

            DeletionTime delTime = delInfo.rangeCovering(entry.getKey());
            DeletionInfo scDelInfo = delTime == null ? DeletionInfo.live() : new DeletionInfo(delTime);
            DeletionTime.serializer.serialize(scDelInfo.getTopLevelDeletion(), out);

            out.writeInt(entry.getValue().size());
            ColumnSerializer serializer = subComparator.columnSerializer();
            for (Cell subCell : entry.getValue())
                serializer.serialize(subCell, out);
        }
    }

    private static Map<CellName, List<Cell>> groupSuperColumns(ColumnFamily scf)
    {
        CellNameType type = scf.getComparator();
        // The order of insertion matters!
        Map<CellName, List<Cell>> scMap = new LinkedHashMap<>();

        CellName scName = null;
        List<Cell> subCells = null;
        CellNameType scType = scType(type);
        CellNameType subType = subType(type);
        for (Cell cell : scf)
        {
            CellName newScName = scType.makeCellName(scName(cell.name()));
            CellName newSubName = subType.makeCellName(subName(cell.name()));

            if (scName == null || scType.compare(scName, newScName) != 0)
            {
                // new super cell
                scName = newScName;
                subCells = new ArrayList<>();
                scMap.put(scName, subCells);
            }

            subCells.add(((Cell) cell).withUpdatedName(newSubName));
        }
        return scMap;
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

    public static long serializedSize(ColumnFamily scf, TypeSizes typeSizes, int version)
    {
        Map<CellName, List<Cell>> scMap = groupSuperColumns(scf);
        DeletionInfo delInfo = scf.deletionInfo();

        // Actually Serialize
        long size = scType(scf.getComparator()).deletionInfoSerializer().serializedSize(new DeletionInfo(delInfo.getTopLevelDeletion()), version);

        CellNameType scType = scType(scf.getComparator());
        CellNameType subType = subType(scf.getComparator());
        ColumnSerializer colSer = subType.columnSerializer();
        for (Map.Entry<CellName, List<Cell>> entry : scMap.entrySet())
        {
            size += scType.cellSerializer().serializedSize(entry.getKey(), typeSizes);

            DeletionTime delTime = delInfo.rangeCovering(entry.getKey());
            DeletionInfo scDelInfo = delTime == null ? DeletionInfo.live() : new DeletionInfo(delTime);
            size += DeletionTime.serializer.serializedSize(scDelInfo.getTopLevelDeletion(), TypeSizes.NATIVE);

            size += typeSizes.sizeof(entry.getValue().size());
            for (Cell subCell : entry.getValue())
                size += colSer.serializedSize(subCell, typeSizes);
        }
        return size;
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

    private static CellNameType scType(CellNameType type)
    {
        return new SimpleDenseCellNameType(type.subtype(0));
    }

    private static CellNameType subType(CellNameType type)
    {
        return new SimpleDenseCellNameType(type.subtype(1));
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

    public static SCFilter filterToSC(CellNameType type, IDiskAtomFilter filter)
    {
        if (filter instanceof NamesQueryFilter)
            return namesFilterToSC(type, (NamesQueryFilter)filter);
        else
            return sliceFilterToSC(type, (SliceQueryFilter)filter);
    }

    public static SCFilter namesFilterToSC(CellNameType type, NamesQueryFilter filter)
    {
        ByteBuffer scName = null;
        CellNameType subComparator = subType(type);
        SortedSet<CellName> newColumns = new TreeSet<CellName>(subComparator);
        for (CellName name : filter.columns)
        {
            ByteBuffer newScName = scName(name);

            if (scName == null)
            {
                scName = newScName;
            }
            else if (type.subtype(0).compare(scName, newScName) != 0)
            {
                // If we're selecting column across multiple SC, it's not something we can translate for an old node
                throw new RuntimeException("Cannot convert filter to old super column format. Update all nodes to Cassandra 2.0 first.");
            }

            newColumns.add(subComparator.makeCellName(name));
        }
        return new SCFilter(scName, new NamesQueryFilter(newColumns));
    }

    private static boolean isEndOfRange(Composite c)
    {
        return c.eoc() == Composite.EOC.END;
    }

    public static SCFilter sliceFilterToSC(CellNameType type, SliceQueryFilter filter)
    {
        /*
         * There is 3 main cases that we can translate back into super column
         * queries:
         *   1) We have only one slice where the first component of start and
         *   finish is the same, we translate as a slice query on one SC.
         *   2) We have only one slice, neither the start and finish have a 2nd
         *   component, and end has the 'end of component' set, we translate
         *   as a slice of SCs.
         *   3) Each slice has the same first component for start and finish, no
         *   2nd component and each finish has the 'end of component' set, we
         *   translate as a names query of SCs (the filter must then not be reversed).
         * Otherwise, we can't do much.
         */

        boolean reversed = filter.reversed;
        if (filter.slices.length == 1)
        {
            Composite start = filter.slices[0].start;
            Composite finish = filter.slices[0].start;

            if (filter.compositesToGroup == 1)
            {
                // Note: all the resulting filter must have compositeToGroup == 0 because this
                // make no sense for super column on the destination node otherwise
                if (start.isEmpty())
                {
                    if (finish.isEmpty())
                        // An 'IdentityFilter', keep as is (except for the compositeToGroup)
                        return new SCFilter(null, new SliceQueryFilter(filter.start(), filter.finish(), reversed, filter.count));

                    if (subName(finish) == null
                            && ((!reversed && !isEndOfRange(finish)) || (reversed && isEndOfRange(finish))))
                        return new SCFilter(null, new SliceQueryFilter(Composites.EMPTY, CellNames.simpleDense(scName(finish)), reversed, filter.count));
                }
                else if (finish.isEmpty())
                {
                    if (subName(start) == null
                            && ((!reversed && isEndOfRange(start)) || (reversed && !isEndOfRange(start))))
                        return new SCFilter(null, new SliceQueryFilter(CellNames.simpleDense(scName(start)), Composites.EMPTY, reversed, filter.count));
                }
                else if (subName(start) == null && subName(finish) == null
                        && ((   reversed && !isEndOfRange(start) && isEndOfRange(finish))
                            || (!reversed &&  isEndOfRange(start) && !isEndOfRange(finish))))
                {
                    // A slice of supercolumns
                    return new SCFilter(null, new SliceQueryFilter(CellNames.simpleDense(scName(start)),
                                                                   CellNames.simpleDense(scName(finish)),
                                                                   reversed,
                                                                   filter.count));
                }
            }
            else if (filter.compositesToGroup == 0 && type.subtype(0).compare(scName(start), scName(finish)) == 0)
            {
                // A slice of subcolumns
                return new SCFilter(scName(start), filter.withUpdatedSlice(CellNames.simpleDense(subName(start)), CellNames.simpleDense(subName(finish))));
            }
        }
        else if (!reversed)
        {
            SortedSet<CellName> columns = new TreeSet<CellName>(scType(type));
            for (int i = 0; i < filter.slices.length; ++i)
            {
                Composite start = filter.slices[i].start;
                Composite finish = filter.slices[i].finish;

                if (subName(start) != null || subName(finish) != null
                  || type.subtype(0).compare(scName(start), scName(finish)) != 0
                  || isEndOfRange(start) || !isEndOfRange(finish))
                    throw new RuntimeException("Cannot convert filter to old super column format. Update all nodes to Cassandra 2.0 first.");

                columns.add(CellNames.simpleDense(scName(start)));
            }
            return new SCFilter(null, new NamesQueryFilter(columns));
        }
        throw new RuntimeException("Cannot convert filter to old super column format. Update all nodes to Cassandra 2.0 first.");
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
                slices[i++] = name.slice();
            }
            return new SliceQueryFilter(slices, false, slices.length, 1);
        }
        else
        {
            SortedSet<CellName> newColumns = new TreeSet<CellName>(type);
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

    public static class SCFilter
    {
        public final ByteBuffer scName;
        public final IDiskAtomFilter updatedFilter;

        public SCFilter(ByteBuffer scName, IDiskAtomFilter updatedFilter)
        {
            this.scName = scName;
            this.updatedFilter = updatedFilter;
        }
    }
}
