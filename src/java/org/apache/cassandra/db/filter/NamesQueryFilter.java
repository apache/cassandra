package org.apache.cassandra.db.filter;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.IOException;
import java.util.*;

import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.ReducingIterator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.config.DatabaseDescriptor;

public class NamesQueryFilter extends QueryFilter
{
    public final SortedSet<byte[]> columns;

    public NamesQueryFilter(String key, QueryPath columnParent, SortedSet<byte[]> columns)
    {
        super(key, columnParent);
        this.columns = columns;
    }

    public NamesQueryFilter(String key, QueryPath columnParent, byte[] column)
    {
        this(key, columnParent, getSingleColumnSet(column));
    }

    private static TreeSet<byte[]> getSingleColumnSet(byte[] column)
    {
        Comparator<byte[]> singleColumnComparator = new Comparator<byte[]>()
        {
            public int compare(byte[] o1, byte[] o2)
            {
                return Arrays.equals(o1, o2) ? 0 : -1;
            }
        };
        TreeSet<byte[]> set = new TreeSet<byte[]>(singleColumnComparator);
        set.add(column);
        return set;
    }

    public ColumnIterator getMemColumnIterator(Memtable memtable, ColumnFamily cf, AbstractType comparator)
    {
        return memtable.getNamesIterator(cf, this);
    }

    public ColumnIterator getSSTableColumnIterator(SSTableReader sstable) throws IOException
    {
        return new SSTableNamesIterator(sstable, key, columns);
    }

    public SuperColumn filterSuperColumn(SuperColumn superColumn, int gcBefore)
    {
        for (IColumn column : superColumn.getSubColumns())
        {
            if (!columns.contains(column.name()))
            {
                superColumn.remove(column.name());
            }
        }
        return superColumn;
    }

    public void collectReducedColumns(IColumnContainer container, Iterator<IColumn> reducedColumns, int gcBefore)
    {
        while (reducedColumns.hasNext())
        {
            IColumn column = reducedColumns.next();
            if (QueryFilter.isRelevant(column, container, gcBefore))
                container.addColumn(column);
        }
    }
}
