/**
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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Allocator;

public class TreeMapBackedSortedColumns extends TreeMap<ByteBuffer, IColumn> implements ISortedColumns
{
    public static final ISortedColumns.Factory factory = new Factory()
    {
        public ISortedColumns create(AbstractType<?> comparator, boolean insertReversed)
        {
            return new TreeMapBackedSortedColumns(comparator);
        }

        public ISortedColumns fromSorted(SortedMap<ByteBuffer, IColumn> sortedMap, boolean insertReversed)
        {
            return new TreeMapBackedSortedColumns(sortedMap);
        }
    };

    public static ISortedColumns.Factory factory()
    {
        return factory;
    }

    public AbstractType<?> getComparator()
    {
        return (AbstractType)comparator();
    }

    private TreeMapBackedSortedColumns(AbstractType<?> comparator)
    {
        super(comparator);
    }

    private TreeMapBackedSortedColumns(SortedMap<ByteBuffer, IColumn> columns)
    {
        super(columns);
    }

    public ISortedColumns.Factory getFactory()
    {
        return factory();
    }

    public ISortedColumns cloneMe()
    {
        return new TreeMapBackedSortedColumns(this);
    }

    public boolean isInsertReversed()
    {
        return false;
    }

    /*
     * If we find an old column that has the same name
     * the ask it to resolve itself else add the new column
    */
    public void addColumn(IColumn column, Allocator allocator)
    {
        ByteBuffer name = column.name();
        // this is a slightly unusual way to structure this; a more natural way is shown in ThreadSafeSortedColumns,
        // but TreeMap lacks putAbsent.  Rather than split it into a "get, then put" check, we do it as follows,
        // which saves the extra "get" in the no-conflict case [for both normal and super columns],
        // in exchange for a re-put in the SuperColumn case.
        IColumn oldColumn = put(name, column);
        if (oldColumn != null)
        {
            if (oldColumn instanceof SuperColumn)
            {
                assert column instanceof SuperColumn;
                // since oldColumn is where we've been accumulating results, it's usually going to be faster to
                // add the new one to the old, then place old back in the Map, rather than copy the old contents
                // into the new Map entry.
                ((SuperColumn) oldColumn).putColumn((SuperColumn)column, allocator);
                put(name,  oldColumn);
            }
            else
            {
                // calculate reconciled col from old (existing) col and new col
                IColumn reconciledColumn = column.reconcile(oldColumn, allocator);
                put(name, reconciledColumn);
            }
        }
    }

    /**
     * We need to go through each column in the column container and resolve it before adding
     */
    public void addAll(ISortedColumns cm, Allocator allocator)
    {
        for (IColumn column : cm.getSortedColumns())
            addColumn(column, allocator);
    }

    public boolean replace(IColumn oldColumn, IColumn newColumn)
    {
        if (!oldColumn.name().equals(newColumn.name()))
            throw new IllegalArgumentException();

        // We are not supposed to put the newColumn is either there was not
        // column or the column was not equal to oldColumn (to be coherent
        // with other implementation). We optimize for the common case where
        // oldColumn do is present though.
        IColumn previous = put(oldColumn.name(), newColumn);
        if (previous == null)
        {
            remove(oldColumn.name());
            return false;
        }
        if (!previous.equals(oldColumn))
        {
            put(oldColumn.name(), previous);
            return false;
        }
        return true;
    }

    public IColumn getColumn(ByteBuffer name)
    {
        return get(name);
    }

    public void removeColumn(ByteBuffer name)
    {
        remove(name);
    }

    public Collection<IColumn> getSortedColumns()
    {
        return values();
    }

    public Collection<IColumn> getReverseSortedColumns()
    {
        return descendingMap().values();
    }

    public SortedSet<ByteBuffer> getColumnNames()
    {
        return navigableKeySet();
    }

    public int getEstimatedColumnCount()
    {
        return size();
    }

    public Iterator<IColumn> iterator()
    {
        return values().iterator();
    }

    public Iterator<IColumn> reverseIterator()
    {
        return getReverseSortedColumns().iterator();
    }
}
