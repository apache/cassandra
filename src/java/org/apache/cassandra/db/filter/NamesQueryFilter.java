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
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.columniterator.SSTableNamesIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class NamesQueryFilter implements IDiskAtomFilter
{
    public static final Serializer serializer = new Serializer();

    public final SortedSet<ByteBuffer> columns;

    // If true, getLiveCount will always return either 0 or 1. This uses the fact that we know 
    // CQL3 will never use a name filter with cell names spanning multiple CQL3 rows.
    private final boolean countCQL3Rows;

    public NamesQueryFilter(SortedSet<ByteBuffer> columns)
    {
        this(columns, false);
    }

    public NamesQueryFilter(SortedSet<ByteBuffer> columns, boolean countCQL3Rows)
    {
        this.columns = columns;
        this.countCQL3Rows = countCQL3Rows;
    }

    public NamesQueryFilter(ByteBuffer column)
    {
        this(FBUtilities.singleton(column));
    }

    public NamesQueryFilter cloneShallow()
    {
        // NQF is immutable as far as shallow cloning is concerned, so save the allocation.
        return this;
    }

    public NamesQueryFilter withUpdatedColumns(SortedSet<ByteBuffer> newColumns)
    {
       return new NamesQueryFilter(newColumns, countCQL3Rows);
    }

    public OnDiskAtomIterator getColumnFamilyIterator(DecoratedKey key, ColumnFamily cf)
    {
        assert cf != null;
        return new ByNameColumnIterator(columns.iterator(), cf, key);
    }

    public OnDiskAtomIterator getSSTableColumnIterator(SSTableReader sstable, DecoratedKey key)
    {
        return new SSTableNamesIterator(sstable, key, columns);
    }

    public OnDiskAtomIterator getSSTableColumnIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry)
    {
        return new SSTableNamesIterator(sstable, file, key, columns, indexEntry);
    }

    public void collectReducedColumns(ColumnFamily container, Iterator<Column> reducedColumns, int gcBefore, long now)
    {
        DeletionInfo.InOrderTester tester = container.inOrderDeletionTester();
        while (reducedColumns.hasNext())
            container.addIfRelevant(reducedColumns.next(), tester, gcBefore);
    }

    public Comparator<Column> getColumnComparator(AbstractType<?> comparator)
    {
        return comparator.columnComparator;
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
        for (Column column : cf)
        {
            if (column.isLive(now))
                count++;
        }
        return count;
    }

    public boolean maySelectPrefix(Comparator<ByteBuffer> cmp, ByteBuffer prefix)
    {
        for (ByteBuffer column : columns)
        {
            if (ByteBufferUtil.isPrefix(prefix, column))
                return true;
        }
        return false;
    }

    public boolean shouldInclude(SSTableReader sstable)
    {
        return true;
    }

    public boolean countCQL3Rows()
    {
        return countCQL3Rows;
    }

    public ColumnCounter columnCounter(AbstractType<?> comparator, long now)
    {
        return countCQL3Rows
             ? new ColumnCounter.GroupByPrefix(now, null, 0)
             : new ColumnCounter(now);
    }

    private static class ByNameColumnIterator extends AbstractIterator<OnDiskAtom> implements OnDiskAtomIterator
    {
        private final ColumnFamily cf;
        private final DecoratedKey key;
        private final Iterator<ByteBuffer> iter;

        public ByNameColumnIterator(Iterator<ByteBuffer> iter, ColumnFamily cf, DecoratedKey key)
        {
            this.iter = iter;
            this.cf = cf;
            this.key = key;
        }

        public ColumnFamily getColumnFamily()
        {
            return cf;
        }

        public DecoratedKey getKey()
        {
            return key;
        }

        protected OnDiskAtom computeNext()
        {
            while (iter.hasNext())
            {
                ByteBuffer current = iter.next();
                Column column = cf.getColumn(current);
                if (column != null)
                    return column;
            }
            return endOfData();
        }

        public void close() throws IOException { }
    }

    public static class Serializer implements IVersionedSerializer<NamesQueryFilter>
    {
        public void serialize(NamesQueryFilter f, DataOutput out, int version) throws IOException
        {
            out.writeInt(f.columns.size());
            for (ByteBuffer cName : f.columns)
            {
                ByteBufferUtil.writeWithShortLength(cName, out);
            }
            out.writeBoolean(f.countCQL3Rows);
        }

        public NamesQueryFilter deserialize(DataInput in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public NamesQueryFilter deserialize(DataInput in, int version, AbstractType comparator) throws IOException
        {
            int size = in.readInt();
            SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(comparator);
            for (int i = 0; i < size; ++i)
                columns.add(ByteBufferUtil.readWithShortLength(in));
            boolean countCQL3Rows = in.readBoolean();
            return new NamesQueryFilter(columns, countCQL3Rows);
        }

        public long serializedSize(NamesQueryFilter f, int version)
        {
            TypeSizes sizes = TypeSizes.NATIVE;
            int size = sizes.sizeof(f.columns.size());
            for (ByteBuffer cName : f.columns)
            {
                int cNameSize = cName.remaining();
                size += sizes.sizeof((short) cNameSize) + cNameSize;
            }
            size += sizes.sizeof(f.countCQL3Rows);
            return size;
        }
    }
}
