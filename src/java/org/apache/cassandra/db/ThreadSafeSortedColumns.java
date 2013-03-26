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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.base.Function;

import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Allocator;

public class ThreadSafeSortedColumns extends AbstractThreadUnsafeSortedColumns implements ISortedColumns
{
    private final ConcurrentSkipListMap<ByteBuffer, Column> map;

    public static final ISortedColumns.Factory factory = new Factory()
    {
        public ISortedColumns create(AbstractType<?> comparator, boolean insertReversed)
        {
            return new ThreadSafeSortedColumns(comparator);
        }

        public ISortedColumns fromSorted(SortedMap<ByteBuffer, Column> sortedMap, boolean insertReversed)
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
        return (AbstractType<?>)map.comparator();
    }

    private ThreadSafeSortedColumns(AbstractType<?> comparator)
    {
        this.map = new ConcurrentSkipListMap<ByteBuffer, Column>(comparator);
    }

    private ThreadSafeSortedColumns(SortedMap<ByteBuffer, Column> columns)
    {
        this.map = new ConcurrentSkipListMap<ByteBuffer, Column>(columns);
    }

    public ISortedColumns.Factory getFactory()
    {
        return factory();
    }

    public ISortedColumns cloneMe()
    {
        return new ThreadSafeSortedColumns(map);
    }

    public boolean isInsertReversed()
    {
        return false;
    }

    /*
     * If we find an old column that has the same name
     * the ask it to resolve itself else add the new column
    */
    public void addColumn(Column column, Allocator allocator)
    {
        addColumnInternal(column, allocator);
    }

    private long addColumnInternal(Column column, Allocator allocator)
    {
        ByteBuffer name = column.name();
        while (true)
        {
            Column oldColumn = map.putIfAbsent(name, column);
            if (oldColumn == null)
                return column.dataSize();

            // calculate reconciled col from old (existing) col and new col
            Column reconciledColumn = column.reconcile(oldColumn, allocator);
            if (map.replace(name, oldColumn, reconciledColumn))
                return reconciledColumn.dataSize() - oldColumn.dataSize();

            // We failed to replace column due to a concurrent update or a concurrent removal. Keep trying.
            // (Currently, concurrent removal should not happen (only updates), but let us support that anyway.)
        }
    }

    /**
     * We need to go through each column in the column container and resolve it before adding
     */
    public void addAll(ISortedColumns cm, Allocator allocator, Function<Column, Column> transformation)
    {
        addAllWithSizeDelta(cm, allocator, transformation, null);
    }

    public long addAllWithSizeDelta(ISortedColumns cm, Allocator allocator, Function<Column, Column> transformation, SecondaryIndexManager.Updater indexer)
    {
        delete(cm.getDeletionInfo());
        long sizeDelta = 0;
        for (Column column : cm.getSortedColumns())
            sizeDelta += addColumnInternal(transformation.apply(column), allocator);
        return sizeDelta;
    }

    public boolean replace(Column oldColumn, Column newColumn)
    {
        if (!oldColumn.name().equals(newColumn.name()))
            throw new IllegalArgumentException();

        return map.replace(oldColumn.name(), oldColumn, newColumn);
    }

    public Column getColumn(ByteBuffer name)
    {
        return map.get(name);
    }

    public void removeColumn(ByteBuffer name)
    {
        map.remove(name);
    }

    public void clear()
    {
        map.clear();
    }

    public int size()
    {
        return map.size();
    }

    public Collection<Column> getSortedColumns()
    {
        return map.values();
    }

    public Collection<Column> getReverseSortedColumns()
    {
        return map.descendingMap().values();
    }

    public SortedSet<ByteBuffer> getColumnNames()
    {
        return map.navigableKeySet();
    }

    public Iterator<Column> iterator()
    {
        return map.values().iterator();
    }

    public Iterator<Column> iterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(map, slices);
    }

    public Iterator<Column> reverseIterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(map.descendingMap(), slices);
    }
}
