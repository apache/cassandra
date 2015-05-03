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

import java.io.DataInput;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.columniterator.SSTableNamesIterator;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileDataInput;

public class NamesQueryFilter implements IDiskAtomFilter
{
    public final SortedSet<CellName> columns;

    // If true, getLiveCount will always return either 0 or 1. This uses the fact that we know 
    // CQL3 will never use a name filter with cell names spanning multiple CQL3 rows.
    private final boolean countCQL3Rows;

    public NamesQueryFilter(SortedSet<CellName> columns)
    {
        this(columns, false);
    }

    public NamesQueryFilter(SortedSet<CellName> columns, boolean countCQL3Rows)
    {
        this.columns = columns;
        this.countCQL3Rows = countCQL3Rows;
    }

    public NamesQueryFilter cloneShallow()
    {
        // NQF is immutable as far as shallow cloning is concerned, so save the allocation.
        return this;
    }

    public NamesQueryFilter withUpdatedColumns(SortedSet<CellName> newColumns)
    {
       return new NamesQueryFilter(newColumns, countCQL3Rows);
    }

    @SuppressWarnings("unchecked")
    public Iterator<Cell> getColumnIterator(ColumnFamily cf)
    {
        assert cf != null;
        return (Iterator<Cell>) (Iterator<?>) new ByNameColumnIterator(columns.iterator(), null, cf);
    }

    public OnDiskAtomIterator getColumnIterator(DecoratedKey key, ColumnFamily cf)
    {
        assert cf != null;
        return new ByNameColumnIterator(columns.iterator(), key, cf);
    }

    public OnDiskAtomIterator getSSTableColumnIterator(SSTableReader sstable, DecoratedKey key)
    {
        return new SSTableNamesIterator(sstable, key, columns);
    }

    public OnDiskAtomIterator getSSTableColumnIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry)
    {
        return new SSTableNamesIterator(sstable, file, key, columns, indexEntry);
    }

    public void collectReducedColumns(ColumnFamily container, Iterator<Cell> reducedColumns, DecoratedKey key, int gcBefore, long now)
    {
        DeletionInfo.InOrderTester tester = container.inOrderDeletionTester();
        while (reducedColumns.hasNext())
            container.maybeAppendColumn(reducedColumns.next(), tester, gcBefore);
    }

    public Comparator<Cell> getColumnComparator(CellNameType comparator)
    {
        return comparator.columnComparator(false);
    }

    @Override
    public String toString()
    {
        return "NamesQueryFilter(" +
               "columns=" + StringUtils.join(columns, ",") +
               ')';
    }

    public boolean isReversed()
    {
        return false;
    }

    public void updateColumnsLimit(int newLimit)
    {
    }

    public int getLiveCount(ColumnFamily cf, long now)
    {
        // Note: we could use columnCounter() but we save the object allocation as it's simple enough

        if (countCQL3Rows)
            return cf.hasOnlyTombstones(now) ? 0 : 1;

        int count = 0;
        for (Cell cell : cf)
        {
            if (cell.isLive(now))
                count++;
        }
        return count;
    }

    public boolean maySelectPrefix(CType type, Composite prefix)
    {
        for (CellName column : columns)
        {
            if (prefix.isPrefixOf(type, column))
                return true;
        }
        return false;
    }

    public boolean shouldInclude(SSTableReader sstable)
    {
        return true;
    }

    public boolean isFullyCoveredBy(ColumnFamily cf, long now)
    {
        // cf will cover all the requested columns if the range it covers include
        // all said columns
        CellName first = cf.iterator(ColumnSlice.ALL_COLUMNS_ARRAY).next().name();
        CellName last = cf.reverseIterator(ColumnSlice.ALL_COLUMNS_ARRAY).next().name();

        return cf.getComparator().compare(first, columns.first()) <= 0
            && cf.getComparator().compare(columns.last(), last) <= 0;
    }

    public boolean isHeadFilter()
    {
        return false;
    }

    public boolean countCQL3Rows(CellNameType comparator)
    {
        return countCQL3Rows;
    }

    public boolean countCQL3Rows()
    {
        return countCQL3Rows(null);
    }

    public ColumnCounter columnCounter(CellNameType comparator, long now)
    {
        return countCQL3Rows
             ? new ColumnCounter.GroupByPrefix(now, null, 0)
             : new ColumnCounter(now);
    }

    private static class ByNameColumnIterator extends AbstractIterator<OnDiskAtom> implements OnDiskAtomIterator
    {
        private final ColumnFamily cf;
        private final DecoratedKey key;
        private final Iterator<CellName> iter;

        public ByNameColumnIterator(Iterator<CellName> iter, DecoratedKey key, ColumnFamily cf)
        {
            this.iter = iter;
            this.cf = cf;
            this.key = key;
        }

        protected OnDiskAtom computeNext()
        {
            while (iter.hasNext())
            {
                CellName current = iter.next();
                Cell cell = cf.getColumn(current);
                if (cell != null)
                    return cell;
            }
            return endOfData();
        }

        public ColumnFamily getColumnFamily()
        {
            return cf;
        }

        public DecoratedKey getKey()
        {
            return key;
        }

        public void close() throws IOException { }
    }

    public static class Serializer implements IVersionedSerializer<NamesQueryFilter>
    {
        private CellNameType type;

        public Serializer(CellNameType type)
        {
            this.type = type;
        }

        public void serialize(NamesQueryFilter f, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(f.columns.size());
            ISerializer<CellName> serializer = type.cellSerializer();
            for (CellName cName : f.columns)
            {
                serializer.serialize(cName, out);
            }
            out.writeBoolean(f.countCQL3Rows);
        }

        public NamesQueryFilter deserialize(DataInput in, int version) throws IOException
        {
            int size = in.readInt();
            SortedSet<CellName> columns = new TreeSet<CellName>(type);
            ISerializer<CellName> serializer = type.cellSerializer();
            for (int i = 0; i < size; ++i)
                columns.add(serializer.deserialize(in));
            boolean countCQL3Rows = in.readBoolean();
            return new NamesQueryFilter(columns, countCQL3Rows);
        }

        public long serializedSize(NamesQueryFilter f, int version)
        {
            TypeSizes sizes = TypeSizes.NATIVE;
            int size = sizes.sizeof(f.columns.size());
            ISerializer<CellName> serializer = type.cellSerializer();
            for (CellName cName : f.columns)
                size += serializer.serializedSize(cName, sizes);
            size += sizes.sizeof(f.countCQL3Rows);
            return size;
        }
    }

    public Iterator<RangeTombstone> getRangeTombstoneIterator(final ColumnFamily source)
    {
        if (!source.deletionInfo().hasRanges())
            return Iterators.<RangeTombstone>emptyIterator();

        return new AbstractIterator<RangeTombstone>()
        {
            private final Iterator<CellName> names = columns.iterator();
            private RangeTombstone lastFindRange;

            protected RangeTombstone computeNext()
            {
                while (names.hasNext())
                {
                    CellName next = names.next();
                    if (lastFindRange != null && lastFindRange.includes(source.getComparator(), next))
                        return lastFindRange;

                    // We keep the last range around as since names are in sort order, it's
                    // possible it will match the next name too.
                    lastFindRange = source.deletionInfo().rangeCovering(next);
                    if (lastFindRange != null)
                        return lastFindRange;
                }
                return endOfData();
            }
        };
    }
}
