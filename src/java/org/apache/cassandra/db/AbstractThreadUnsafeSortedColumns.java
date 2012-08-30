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

import java.util.Iterator;

import com.google.common.base.Function;

import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Allocator;

public abstract class AbstractThreadUnsafeSortedColumns implements ISortedColumns
{
    private DeletionInfo deletionInfo;

    public AbstractThreadUnsafeSortedColumns()
    {
        deletionInfo = DeletionInfo.LIVE;
    }

    public DeletionInfo getDeletionInfo()
    {
        return deletionInfo;
    }

    public void delete(DeletionInfo newInfo)
    {
        deletionInfo = deletionInfo.add(newInfo);
    }

    public void setDeletionInfo(DeletionInfo newInfo)
    {
        deletionInfo = newInfo;
    }

    public void maybeResetDeletionTimes(int gcBefore)
    {
        deletionInfo = deletionInfo.purge(gcBefore);
    }

    public void retainAll(ISortedColumns columns)
    {
        Iterator<IColumn> iter = iterator();
        Iterator<IColumn> toRetain = columns.iterator();
        IColumn current = iter.hasNext() ? iter.next() : null;
        IColumn retain = toRetain.hasNext() ? toRetain.next() : null;
        AbstractType<?> comparator = getComparator();
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

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public int getEstimatedColumnCount()
    {
        return size();
    }
}
