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
import java.util.Comparator;
import java.util.Iterator;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.ISSTableColumnIterator;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;

/**
 * Given an implementation-specific description of what columns to look for, provides methods
 * to extract the desired columns from a Memtable, SSTable, or SuperColumn.  Either the get*ColumnIterator
 * methods will be called, or filterSuperColumn, but not both on the same object.  QueryFilter
 * takes care of putting the two together if subcolumn filtering needs to be done, based on the
 * querypath that it knows (but that IFilter implementations are oblivious to).
 */
public interface IDiskAtomFilter
{
    /**
     * returns an iterator that returns columns from the given memtable
     * matching the Filter criteria in sorted order.
     */
    public OnDiskAtomIterator getMemtableColumnIterator(ColumnFamily cf, DecoratedKey key);

    /**
     * Get an iterator that returns columns from the given SSTable using the opened file
     * matching the Filter criteria in sorted order.
     * @param sstable
     * @param file Already opened file data input, saves us opening another one
     * @param key The key of the row we are about to iterate over
     */
    public ISSTableColumnIterator getSSTableColumnIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry);

    /**
     * returns an iterator that returns columns from the given SSTable
     * matching the Filter criteria in sorted order.
     */
    public ISSTableColumnIterator getSSTableColumnIterator(SSTableReader sstable, DecoratedKey key);

    /**
     * collects columns from reducedColumns into returnCF.  Termination is determined
     * by the filter code, which should have some limit on the number of columns
     * to avoid running out of memory on large rows.
     */
    public void collectReducedColumns(IColumnContainer container, Iterator<IColumn> reducedColumns, int gcBefore);

    /**
     * subcolumns of a supercolumn are unindexed, so to pick out parts of those we operate in-memory.
     * @param superColumn may be modified by filtering op.
     */
    public SuperColumn filterSuperColumn(SuperColumn superColumn, int gcBefore);

    public Comparator<IColumn> getColumnComparator(AbstractType<?> comparator);

    public boolean isReversed();
    public void updateColumnsLimit(int newLimit);

    public int getLiveCount(ColumnFamily cf);

    public static class Serializer implements IVersionedSerializer<IDiskAtomFilter>
    {
        public static Serializer instance = new Serializer();

        public void serialize(IDiskAtomFilter filter, DataOutput dos, int version) throws IOException
        {
            if (filter instanceof SliceQueryFilter)
            {
                dos.writeByte(0);
                SliceQueryFilter.serializer.serialize((SliceQueryFilter)filter, dos, version);
            }
            else
            {
                dos.writeByte(1);
                NamesQueryFilter.serializer.serialize((NamesQueryFilter)filter, dos, version);
            }
        }

        public IDiskAtomFilter deserialize(DataInput dis, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public IDiskAtomFilter deserialize(DataInput dis, int version, AbstractType<?> comparator) throws IOException
        {
            int type = dis.readByte();
            if (type == 0)
            {
                return SliceQueryFilter.serializer.deserialize(dis, version);
            }
            else
            {
                assert type == 1;
                return NamesQueryFilter.serializer.deserialize(dis, version, comparator);
            }
        }

        public long serializedSize(IDiskAtomFilter filter, int version)
        {
            int size = 1;
            if (filter instanceof SliceQueryFilter)
                size += SliceQueryFilter.serializer.serializedSize((SliceQueryFilter)filter, version);
            else
                size += NamesQueryFilter.serializer.serializedSize((NamesQueryFilter)filter, version);
            return size;
        }
    }
}
