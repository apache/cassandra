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
import java.security.MessageDigest;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.IIterableColumns;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.HeapAllocator;

public abstract class AbstractColumnContainer implements IColumnContainer, IIterableColumns
{
    private static Logger logger = LoggerFactory.getLogger(AbstractColumnContainer.class);

    protected final AtomicReference<DeletionInfo> deletionInfo = new AtomicReference<DeletionInfo>(new DeletionInfo());
    protected final ISortedColumns columns;

    protected AbstractColumnContainer(ISortedColumns columns)
    {
        this.columns = columns;
    }

    @Deprecated // TODO this is a hack to set initial value outside constructor
    public void delete(int localtime, long timestamp)
    {
        deletionInfo.set(new DeletionInfo(timestamp, localtime));
    }

    public void delete(AbstractColumnContainer cc2)
    {
        // Keeping deletion info for max markedForDeleteAt value
        DeletionInfo current;
        DeletionInfo cc2Info = cc2.deletionInfo.get();
        while (true)
        {
             current = deletionInfo.get();
             if (current.markedForDeleteAt >= cc2Info.markedForDeleteAt || deletionInfo.compareAndSet(current, cc2Info))
                 break;
        }
    }

    public boolean isMarkedForDelete()
    {
        return getMarkedForDeleteAt() > Long.MIN_VALUE;
    }

    public long getMarkedForDeleteAt()
    {
        return deletionInfo.get().markedForDeleteAt;
    }

    public int getLocalDeletionTime()
    {
        return deletionInfo.get().localDeletionTime;
    }

    public AbstractType getComparator()
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
        while (true)
        {
            DeletionInfo current = deletionInfo.get();
            // Stop if either we don't need to change the deletion info (it's
            // still MIN_VALUE or not expired yet) or we've succesfully changed it
            if (current.localDeletionTime == Integer.MIN_VALUE
                || current.localDeletionTime > gcBefore
                || deletionInfo.compareAndSet(current, new DeletionInfo()))
            {
                break;
            }
        }
    }

    /**
     * We need to go through each column in the column container and resolve it before adding
     */
    public void addAll(AbstractColumnContainer cc, Allocator allocator)
    {
        columns.addAll(cc.columns, allocator);
        delete(cc);
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
        Iterator<IColumn> iter = iterator();
        Iterator<IColumn> toRetain = container.iterator();
        IColumn current = iter.hasNext() ? iter.next() : null;
        IColumn retain = toRetain.hasNext() ? toRetain.next() : null;
        AbstractType comparator = getComparator();
        while (current != null && retain != null)
        {
            int c = comparator.compare(current.name(), retain.name());
            if (c == 0)
            {
                if (current instanceof SuperColumn)
                {
                    assert retain instanceof SuperColumn;
                    ((SuperColumn)current).retainAll((SuperColumn)retain);
                }
                current = iter.hasNext() ? iter.next() : null;
                retain = toRetain.hasNext() ? toRetain.next() : null;
            }
            else if (c < 0)
            {
                iter.remove();
                current = iter.hasNext() ? iter.next() : null;
            }
            else // c > 0
            {
                retain = toRetain.hasNext() ? toRetain.next() : null;
            }
        }
        while (current != null)
        {
            iter.remove();
            current = iter.hasNext() ? iter.next() : null;
        }
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

    protected static class DeletionInfo
    {
        public final long markedForDeleteAt;
        public final int localDeletionTime;

        public DeletionInfo()
        {
            this(Long.MIN_VALUE, Integer.MIN_VALUE);
        }

        public DeletionInfo(long markedForDeleteAt, int localDeletionTime)
        {
            this.markedForDeleteAt = markedForDeleteAt;
            this.localDeletionTime = localDeletionTime;
        }
    }

    public boolean hasExpiredTombstones(int gcBefore)
    {
        if (isMarkedForDelete() && getLocalDeletionTime() < gcBefore)
            return true;

        for (IColumn column : columns)
            if (column.hasExpiredTombstones(gcBefore))
                return true;

        return false;
    }
}
