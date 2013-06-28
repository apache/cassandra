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
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SuperColumns
{
    public static Iterator<OnDiskAtom> onDiskIterator(DataInput in, int superColumnCount, ColumnSerializer.Flag flag, int expireBefore)
    {
        return new SCIterator(in, superColumnCount, flag, expireBefore);
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
        Map<ByteBuffer, List<Column>> scMap = groupSuperColumns(scf);

        // Actually Serialize
        DeletionInfo.serializer().serialize(new DeletionInfo(delInfo.getTopLevelDeletion()), out, version);
        out.writeInt(scMap.size());

        for (Map.Entry<ByteBuffer, List<Column>> entry : scMap.entrySet())
        {
            ByteBufferUtil.writeWithShortLength(entry.getKey(), out);

            DeletionTime delTime = delInfo.rangeCovering(entry.getKey());
            DeletionInfo scDelInfo = delTime == null ? DeletionInfo.live() : new DeletionInfo(delTime);
            DeletionTime.serializer.serialize(scDelInfo.getTopLevelDeletion(), out);

            out.writeInt(entry.getValue().size());
            for (Column subColumn : entry.getValue())
                Column.serializer.serialize(subColumn, out);
        }
    }

    private static Map<ByteBuffer, List<Column>> groupSuperColumns(ColumnFamily scf)
    {
        CompositeType type = (CompositeType)scf.getComparator();
        // The order of insertion matters!
        Map<ByteBuffer, List<Column>> scMap = new LinkedHashMap<ByteBuffer, List<Column>>();

        ByteBuffer scName = null;
        List<Column> subColumns = null;
        for (Column column : scf)
        {
            ByteBuffer newScName = scName(column.name());
            ByteBuffer newSubName = subName(column.name());

            if (scName == null || type.types.get(0).compare(scName, newScName) != 0)
            {
                // new super column
                scName = newScName;
                subColumns = new ArrayList<Column>();
                scMap.put(scName, subColumns);
            }

            subColumns.add(((Column)column).withUpdatedName(newSubName));
        }
        return scMap;
    }

    public static void deserializerSuperColumnFamily(DataInput in, ColumnFamily cf, ColumnSerializer.Flag flag, int version) throws IOException
    {
        // Note that there was no way to insert a range tombstone in a SCF in 1.2
        cf.delete(DeletionInfo.serializer().deserialize(in, version, cf.getComparator()));
        assert !cf.deletionInfo().rangeIterator().hasNext();

        Iterator<OnDiskAtom> iter = onDiskIterator(in, in.readInt(), flag, Integer.MIN_VALUE);
        while (iter.hasNext())
            cf.addAtom(iter.next());
    }

    public static long serializedSize(ColumnFamily scf, TypeSizes typeSizes, int version)
    {
        Map<ByteBuffer, List<Column>> scMap = groupSuperColumns(scf);
        DeletionInfo delInfo = scf.deletionInfo();

        // Actually Serialize
        long size = DeletionInfo.serializer().serializedSize(new DeletionInfo(delInfo.getTopLevelDeletion()), version);
        for (Map.Entry<ByteBuffer, List<Column>> entry : scMap.entrySet())
        {
            int nameSize = entry.getKey().remaining();
            size += typeSizes.sizeof((short) nameSize) + nameSize;

            DeletionTime delTime = delInfo.rangeCovering(entry.getKey());
            DeletionInfo scDelInfo = delTime == null ? DeletionInfo.live() : new DeletionInfo(delTime);
            size += DeletionTime.serializer.serializedSize(scDelInfo.getTopLevelDeletion(), TypeSizes.NATIVE);

            size += typeSizes.sizeof(entry.getValue().size());
            for (Column subColumn : entry.getValue())
                size += Column.serializer.serializedSize(subColumn, typeSizes);
        }
        return size;
    }

    private static class SCIterator implements Iterator<OnDiskAtom>
    {
        private final DataInput in;
        private final int scCount;

        private final ColumnSerializer.Flag flag;
        private final int expireBefore;

        private int read;
        private ByteBuffer scName;
        private Iterator<Column> subColumnsIterator;

        private SCIterator(DataInput in, int superColumnCount, ColumnSerializer.Flag flag, int expireBefore)
        {
            this.in = in;
            this.scCount = superColumnCount;
            this.flag = flag;
            this.expireBefore = expireBefore;
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
                    Column c = subColumnsIterator.next();
                    return c.withUpdatedName(CompositeType.build(scName, c.name()));
                }

                // Read one more super column
                ++read;

                scName = ByteBufferUtil.readWithShortLength(in);
                DeletionInfo delInfo = new DeletionInfo(DeletionTime.serializer.deserialize(in));
                assert !delInfo.rangeIterator().hasNext(); // We assume no range tombstone (there was no way to insert some in a SCF in 1.2)

                /* read the number of columns */
                int size = in.readInt();
                List<Column> subColumns = new ArrayList<Column>(size);

                for (int i = 0; i < size; ++i)
                    subColumns.add(Column.serializer.deserialize(in, flag, expireBefore));

                subColumnsIterator = subColumns.iterator();

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

    public static AbstractType<?> getComparatorFor(CFMetaData metadata, ByteBuffer superColumn)
    {
        return getComparatorFor(metadata, superColumn != null);
    }

    public static AbstractType<?> getComparatorFor(CFMetaData metadata, boolean subColumn)
    {
        return metadata.isSuper()
             ? ((CompositeType)metadata.comparator).types.get(subColumn ? 1 : 0)
             : metadata.comparator;
    }

    // Extract the first component of a columnName, i.e. the super column name
    public static ByteBuffer scName(ByteBuffer columnName)
    {
        return CompositeType.extractComponent(columnName, 0);
    }

    // Extract the 2nd component of a columnName, i.e. the sub-column name
    public static ByteBuffer subName(ByteBuffer columnName)
    {
        return CompositeType.extractComponent(columnName, 1);
    }

    // We don't use CompositeType.Builder mostly because we want to avoid having to provide the comparator.
    public static ByteBuffer startOf(ByteBuffer scName)
    {
        int length = scName.remaining();
        ByteBuffer bb = ByteBuffer.allocate(2 + length + 1);

        bb.put((byte) ((length >> 8) & 0xFF));
        bb.put((byte) (length & 0xFF));
        bb.put(scName.duplicate());
        bb.put((byte) 0);
        bb.flip();
        return bb;
    }

    public static ByteBuffer endOf(ByteBuffer scName)
    {
        ByteBuffer bb = startOf(scName);
        bb.put(bb.remaining() - 1, (byte)1);
        return bb;
    }

    public static SCFilter filterToSC(CompositeType type, IDiskAtomFilter filter)
    {
        if (filter instanceof NamesQueryFilter)
            return namesFilterToSC(type, (NamesQueryFilter)filter);
        else
            return sliceFilterToSC(type, (SliceQueryFilter)filter);
    }

    public static SCFilter namesFilterToSC(CompositeType type, NamesQueryFilter filter)
    {
        ByteBuffer scName = null;
        SortedSet<ByteBuffer> newColumns = new TreeSet<ByteBuffer>(filter.columns.comparator());
        for (ByteBuffer name : filter.columns)
        {
            ByteBuffer newScName = scName(name);

            if (scName == null)
            {
                scName = newScName;
            }
            else if (type.types.get(0).compare(scName, newScName) != 0)
            {
                // If we're selecting column across multiple SC, it's not something we can translate for an old node
                throw new RuntimeException("Cannot convert filter to old super column format. Update all nodes to Cassandra 2.0 first.");
            }

            newColumns.add(subName(name));
        }
        return new SCFilter(scName, new NamesQueryFilter(newColumns));
    }

    public static SCFilter sliceFilterToSC(CompositeType type, SliceQueryFilter filter)
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
            ByteBuffer start = filter.slices[0].start;
            ByteBuffer finish = filter.slices[0].start;

            if (filter.compositesToGroup == 1)
            {
                // Note: all the resulting filter must have compositeToGroup == 0 because this
                // make no sense for super column on the destination node otherwise
                if (start.remaining() == 0)
                {
                    if (finish.remaining() == 0)
                        // An 'IdentityFilter', keep as is (except for the compositeToGroup)
                        return new SCFilter(null, new SliceQueryFilter(filter.start(), filter.finish(), reversed, filter.count));

                    if (subName(finish) == null
                            && ((!reversed && !firstEndOfComponent(finish)) || (reversed && firstEndOfComponent(finish))))
                        return new SCFilter(null, new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER, scName(finish), reversed, filter.count));
                }
                else if (finish.remaining() == 0)
                {
                    if (subName(start) == null
                            && ((!reversed && firstEndOfComponent(start)) || (reversed && !firstEndOfComponent(start))))
                        return new SCFilter(null, new SliceQueryFilter(scName(start), ByteBufferUtil.EMPTY_BYTE_BUFFER, reversed, filter.count));
                }
                else if (subName(start) == null && subName(finish) == null
                        && ((   reversed && !firstEndOfComponent(start) &&  firstEndOfComponent(finish))
                            || (!reversed &&  firstEndOfComponent(start) && !firstEndOfComponent(finish))))
                {
                    // A slice of supercolumns
                    return new SCFilter(null, new SliceQueryFilter(scName(start), scName(finish), reversed, filter.count));
                }
            }
            else if (filter.compositesToGroup == 0 && type.types.get(0).compare(scName(start), scName(finish)) == 0)
            {
                // A slice of subcolumns
                return new SCFilter(scName(start), filter.withUpdatedSlice(subName(start), subName(finish)));
            }
        }
        else if (!reversed)
        {
            SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(type.types.get(0));
            for (int i = 0; i < filter.slices.length; ++i)
            {
                ByteBuffer start = filter.slices[i].start;
                ByteBuffer finish = filter.slices[i].finish;

                if (subName(start) != null || subName(finish) != null
                  || type.types.get(0).compare(scName(start), scName(finish)) != 0
                  || firstEndOfComponent(start) || !firstEndOfComponent(finish))
                    throw new RuntimeException("Cannot convert filter to old super column format. Update all nodes to Cassandra 2.0 first.");

                columns.add(scName(start));
            }
            return new SCFilter(null, new NamesQueryFilter(columns));
        }
        throw new RuntimeException("Cannot convert filter to old super column format. Update all nodes to Cassandra 2.0 first.");
    }

    public static IDiskAtomFilter fromSCFilter(CompositeType type, ByteBuffer scName, IDiskAtomFilter filter)
    {
        if (filter instanceof NamesQueryFilter)
            return fromSCNamesFilter(type, scName, (NamesQueryFilter)filter);
        else
            return fromSCSliceFilter(type, scName, (SliceQueryFilter)filter);
    }

    public static IDiskAtomFilter fromSCNamesFilter(CompositeType type, ByteBuffer scName, NamesQueryFilter filter)
    {
        if (scName == null)
        {
            ColumnSlice[] slices = new ColumnSlice[filter.columns.size()];
            int i = 0;
            for (ByteBuffer bb : filter.columns)
            {
                CompositeType.Builder builder = type.builder().add(bb);
                slices[i++] = new ColumnSlice(builder.build(), builder.buildAsEndOfRange());
            }
            return new SliceQueryFilter(slices, false, slices.length, 1);
        }
        else
        {
            SortedSet<ByteBuffer> newColumns = new TreeSet<ByteBuffer>(type);
            for (ByteBuffer c : filter.columns)
                newColumns.add(CompositeType.build(scName, c));
            return filter.withUpdatedColumns(newColumns);
        }
    }

    public static SliceQueryFilter fromSCSliceFilter(CompositeType type, ByteBuffer scName, SliceQueryFilter filter)
    {
        assert filter.slices.length == 1;
        if (scName == null)
        {
            ByteBuffer start = filter.start().remaining() == 0
                             ? filter.start()
                             : (filter.reversed ? type.builder().add(filter.start()).buildAsEndOfRange()
                                                : type.builder().add(filter.start()).build());
            ByteBuffer finish = filter.finish().remaining() == 0
                              ? filter.finish()
                              : (filter.reversed ? type.builder().add(filter.finish()).build()
                                                 : type.builder().add(filter.finish()).buildAsEndOfRange());
            return new SliceQueryFilter(start, finish, filter.reversed, filter.count, 1);
        }
        else
        {
            CompositeType.Builder builder = type.builder().add(scName);
            ByteBuffer start = filter.start().remaining() == 0
                             ? filter.reversed ? builder.buildAsEndOfRange() : builder.build()
                             : builder.copy().add(filter.start()).build();
            ByteBuffer end = filter.finish().remaining() == 0
                             ? filter.reversed ? builder.build() : builder.buildAsEndOfRange()
                             : builder.add(filter.finish()).build();
            return new SliceQueryFilter(start, end, filter.reversed, filter.count);
        }
    }

    private static boolean firstEndOfComponent(ByteBuffer bb)
    {
        bb = bb.duplicate();
        int length = (bb.get() & 0xFF) << 8;
        length |= (bb.get() & 0xFF);

        return bb.get(length + 2) == 1;
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
