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
import java.util.SortedSet;

import com.google.common.base.Function;
import com.google.common.base.Functions;

import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.IIterableColumns;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.HeapAllocator;

public abstract class AbstractColumnContainer implements IColumnContainer, IIterableColumns
{
    protected final ISortedColumns columns;

    protected AbstractColumnContainer(ISortedColumns columns)
    {
        this.columns = columns;
    }

    public void delete(AbstractColumnContainer cc2)
    {
        delete(cc2.columns.getDeletionInfo());
    }

    public void delete(DeletionInfo delInfo)
    {
        columns.delete(delInfo);
    }

    // Contrarily to delete(), this will use the provided info even if those
    // are older that the current ones. Used for SuperColumn in QueryFilter.
    // delete() is probably the right method in all other cases.
    public void setDeletionInfo(DeletionInfo delInfo)
    {
        columns.setDeletionInfo(delInfo);
    }

    public boolean isMarkedForDelete()
    {
        return !deletionInfo().isLive();
    }

    public DeletionInfo deletionInfo()
    {
        return columns.getDeletionInfo();
    }

    public AbstractType<?> getComparator()
    {
        return columns.getComparator();
    }

    /**
     * Drops expired row-level tombstones.  Normally, these are dropped once the row no longer exists, but
     * if new columns are inserted into the row post-deletion, they can keep the row tombstone alive indefinitely,
     * with non-intuitive results.  See https://issues.apache.org/jira/browse/CASSANDRA-2317
     */
    public void maybeResetDeletionTimes(int gcBefore)
    {
        columns.maybeResetDeletionTimes(gcBefore);
    }

    public long addAllWithSizeDelta(AbstractColumnContainer cc, Allocator allocator, Function<IColumn, IColumn> transformation, SecondaryIndexManager.Updater indexer)
    {
        return columns.addAllWithSizeDelta(cc.columns, allocator, transformation, indexer);
    }

    public void addAll(AbstractColumnContainer cc, Allocator allocator, Function<IColumn, IColumn> transformation)
    {
        columns.addAll(cc.columns, allocator, transformation);
    }

    public void addAll(AbstractColumnContainer cc, Allocator allocator)
    {
        addAll(cc, allocator, Functions.<IColumn>identity());
    }

    public void addColumn(IColumn column)
    {
        addColumn(column, HeapAllocator.instance);
    }

    public void addColumn(IColumn column, Allocator allocator)
    {
        columns.addColumn(column, allocator);
    }

    public IColumn getColumn(ByteBuffer name)
    {
        return columns.getColumn(name);
    }

    public boolean replace(IColumn oldColumn, IColumn newColumn)
    {
        return columns.replace(oldColumn, newColumn);
    }

    /*
     * Note that for some of the implementation backing the container, the
     * return set may not have implementation for tailSet, headSet and subSet.
     * See ColumnNamesSet in ArrayBackedSortedColumns for more details.
     */
    public SortedSet<ByteBuffer> getColumnNames()
    {
        return columns.getColumnNames();
    }

    public Collection<IColumn> getSortedColumns()
    {
        return columns.getSortedColumns();
    }

    public Collection<IColumn> getReverseSortedColumns()
    {
        return columns.getReverseSortedColumns();
    }

    public void remove(ByteBuffer columnName)
    {
        columns.removeColumn(columnName);
    }

    public void retainAll(AbstractColumnContainer container)
    {
        columns.retainAll(container.columns);
    }

    public int getColumnCount()
    {
        return columns.size();
    }

    public boolean isEmpty()
    {
        return columns.isEmpty();
    }

    public int getEstimatedColumnCount()
    {
        return getColumnCount();
    }

    public int getLiveColumnCount()
    {
        int count = 0;

        for (IColumn column : columns)
        {
            if (column.isLive())
                count++;
        }

        return count;
    }

    public Iterator<IColumn> iterator()
    {
        return columns.iterator();
    }

    public Iterator<IColumn> iterator(ColumnSlice[] slices)
    {
        return columns.iterator(slices);
    }

    public Iterator<IColumn> reverseIterator(ColumnSlice[] slices)
    {
        return columns.reverseIterator(slices);
    }

    public boolean hasIrrelevantData(int gcBefore)
    {
        // Do we have gcable deletion infos?
        if (!deletionInfo().purge(gcBefore).equals(deletionInfo()))
            return true;

        // Do we have colums that are either deleted by the container or gcable tombstone?
        for (IColumn column : columns)
            if (deletionInfo().isDeleted(column) || column.hasIrrelevantData(gcBefore))
                return true;

        return false;
    }
}
