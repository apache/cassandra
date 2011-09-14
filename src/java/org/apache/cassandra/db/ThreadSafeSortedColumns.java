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
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Allocator;

public class ThreadSafeSortedColumns extends ConcurrentSkipListMap<ByteBuffer, IColumn> implements ISortedColumns
{
    public static final ISortedColumns.Factory factory = new Factory()
    {
        public ISortedColumns create(AbstractType<?> comparator, boolean insertReversed)
        {
            return new ThreadSafeSortedColumns(comparator);
        }

        public ISortedColumns fromSorted(SortedMap<ByteBuffer, IColumn> sortedMap, boolean insertReversed)
        {
            return new ThreadSafeSortedColumns(sortedMap);
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

    private ThreadSafeSortedColumns(AbstractType<?> comparator)
    {
        super(comparator);
    }

    private ThreadSafeSortedColumns(SortedMap<ByteBuffer, IColumn> columns)
    {
        super(columns);
    }

    public ISortedColumns.Factory getFactory()
    {
        return factory();
    }

    public ISortedColumns cloneMe()
    {
        return new ThreadSafeSortedColumns(this);
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
        IColumn oldColumn;
        while ((oldColumn = putIfAbsent(name, column)) != null)
        {
            if (oldColumn instanceof SuperColumn)
            {
                assert column instanceof SuperColumn;
                ((SuperColumn) oldColumn).putColumn((SuperColumn)column, allocator);
                break;  // Delegated to SuperColumn
            }
            else
            {
                // calculate reconciled col from old (existing) col and new col
                IColumn reconciledColumn = column.reconcile(oldColumn, allocator);
                if (replace(name, oldColumn, reconciledColumn))
                    break;

                // We failed to replace column due to a concurrent update or a concurrent removal. Keep trying.
                // (Currently, concurrent removal should not happen (only updates), but let us support that anyway.)
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

        return replace(oldColumn.name(), oldColumn, newColumn);
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
        return keySet();
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
