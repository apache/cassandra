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
import java.util.Comparator;
import java.util.Iterator;
import java.util.Arrays;

import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.utils.ReducingIterator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;

public abstract class QueryFilter
{
    public final String key;
    public final QueryPath path;

    protected QueryFilter(String key, QueryPath path)
    {
        this.key = key;
        this.path = path;
    }

    /**
     * returns an iterator that returns columns from the given memtable
     * matching the Filter criteria in sorted order.
     */
    public abstract ColumnIterator getMemColumnIterator(Memtable memtable, AbstractType comparator);

    /**
     * returns an iterator that returns columns from the given SSTable
     * matching the Filter criteria in sorted order.
     */
    public abstract ColumnIterator getSSTableColumnIterator(SSTableReader sstable) throws IOException;

    /**
     * collects columns from reducedColumns into returnCF.  Termination is determined
     * by the filter code, which should have some limit on the number of columns
     * to avoid running out of memory on large rows.
     */
    public abstract void collectReducedColumns(IColumnContainer container, Iterator<IColumn> reducedColumns, int gcBefore);

    /**
     * subcolumns of a supercolumn are unindexed, so to pick out parts of those we operate in-memory.
     * @param superColumn may be modified by filtering op.
     */
    public abstract SuperColumn filterSuperColumn(SuperColumn superColumn, int gcBefore);

    public Comparator<IColumn> getColumnComparator(final AbstractType comparator)
    {
        return new Comparator<IColumn>()
        {
            public int compare(IColumn c1, IColumn c2)
            {
                return comparator.compare(c1.name(), c2.name());
            }
        };
    }
    
    public void collectCollatedColumns(final ColumnFamily returnCF, Iterator<IColumn> collatedColumns, int gcBefore)
    {
        // define a 'reduced' iterator that merges columns w/ the same name, which
        // greatly simplifies computing liveColumns in the presence of tombstones.
        ReducingIterator<IColumn, IColumn> reduced = new ReducingIterator<IColumn, IColumn>(collatedColumns)
        {
            ColumnFamily curCF = returnCF.cloneMeShallow();

            protected boolean isEqual(IColumn o1, IColumn o2)
            {
                return Arrays.equals(o1.name(), o2.name());
            }

            public void reduce(IColumn current)
            {
                curCF.addColumn(current);
            }

            protected IColumn getReduced()
            {
                IColumn c = curCF.getSortedColumns().iterator().next();
                curCF.clear();
                return c;
            }
        };

        collectReducedColumns(returnCF, reduced, gcBefore);
    }

    public String getColumnFamilyName()
    {
        return path.columnFamilyName;
    }
}
