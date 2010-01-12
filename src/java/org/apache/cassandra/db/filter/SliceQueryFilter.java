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
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.commons.collections.iterators.ReverseListIterator;
import org.apache.commons.collections.IteratorUtils;

import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;

public class SliceQueryFilter extends QueryFilter
{
    private static Logger logger = Logger.getLogger(SliceQueryFilter.class);

    public final byte[] start, finish;
    public final boolean reversed;
    public final int count;

    public SliceQueryFilter(String key, QueryPath columnParent, byte[] start, byte[] finish, boolean reversed, int count)
    {
        super(key, columnParent);
        this.start = start;
        this.finish = finish;
        this.reversed = reversed;
        this.count = count;
    }

    public ColumnIterator getMemColumnIterator(Memtable memtable, ColumnFamily cf, AbstractType comparator)
    {
        return memtable.getSliceIterator(cf, this, comparator);
    }

    public ColumnIterator getSSTableColumnIterator(SSTableReader sstable) throws IOException
    {
        return new SSTableSliceIterator(sstable, key, start, finish, reversed);
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
            subcolumns = new ReverseListIterator(columnsAsList);
        }
        else
        {
            subcolumns = superColumn.getSubColumns().iterator();
        }

        // iterate until we get to the "real" start column
        Comparator<byte[]> comparator = reversed ? superColumn.getComparator().getReverseComparator() : superColumn.getComparator();
        while (subcolumns.hasNext())
        {
            IColumn column = subcolumns.next();
            if (comparator.compare(column.name(), start) >= 0)
            {
                subcolumns = IteratorUtils.chainedIterator(IteratorUtils.singletonIterator(column), subcolumns);
                break;
            }
        }
        // subcolumns is either empty now, or has been redefined in the loop above.  either is ok.
        collectReducedColumns(scFiltered, subcolumns, gcBefore);
        return scFiltered;
    }

    @Override
    public Comparator<IColumn> getColumnComparator(AbstractType comparator)
    {
        return reversed ? new ReverseComparator(super.getColumnComparator(comparator)) : super.getColumnComparator(comparator);
    }

    public void collectReducedColumns(IColumnContainer container, Iterator<IColumn> reducedColumns, int gcBefore)
    {
        int liveColumns = 0;
        AbstractType comparator = container.getComparator();

        while (reducedColumns.hasNext())
        {
            if (liveColumns >= count)
                break;

            IColumn column = reducedColumns.next();
            if (logger.isDebugEnabled())
                logger.debug("collecting " + column.getString(comparator));

            if (finish.length > 0
                && ((!reversed && comparator.compare(column.name(), finish) > 0))
                    || (reversed && comparator.compare(column.name(), finish) < 0))
                break;

            // only count live columns towards the `count` criteria
            if (!column.isMarkedForDelete()
                && (!container.isMarkedForDelete()
                    || column.mostRecentLiveChangeAt() > container.getMarkedForDeleteAt()))
            {
                liveColumns++;
            }

            // but we need to add all non-gc-able columns to the result for read repair:
            if (QueryFilter.isRelevant(column, container, gcBefore))
                container.addColumn(column);
        }
    }
}
