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

import java.util.Arrays;
import java.util.Iterator;

import org.apache.cassandra.db.filter.*;

public class SliceQueryPager implements Iterator<ColumnFamily>
{
    public static final int DEFAULT_PAGE_SIZE = 10000;

    public final ColumnFamilyStore cfs;
    public final DecoratedKey key;

    private ColumnSlice[] slices;
    private ColumnFamily current;
    private boolean exhausted;

    public SliceQueryPager(ColumnFamilyStore cfs, DecoratedKey key, ColumnSlice[] slices)
    {
        this.cfs = cfs;
        this.key = key;
        this.slices = slices;
    }

    // This will *not* do a query
    public boolean hasNext()
    {
        return !exhausted;
    }

    // This might return an empty column family (but never a null one)
    public ColumnFamily next()
    {
        if (exhausted)
            return null;

        QueryPath path = new QueryPath(cfs.getColumnFamilyName());
        QueryFilter filter = new QueryFilter(key, path, new SliceQueryFilter(slices, false, DEFAULT_PAGE_SIZE));
        ColumnFamily cf = cfs.getColumnFamily(filter);
        if (cf == null || cf.getLiveColumnCount() < DEFAULT_PAGE_SIZE)
        {
            exhausted = true;
        }
        else
        {
            Iterator<IColumn> iter = cf.getReverseSortedColumns().iterator();
            IColumn lastColumn = iter.next();
            while (lastColumn.isMarkedForDelete())
                lastColumn = iter.next();

            int i = 0;
            for (; i < slices.length; ++i)
            {
                ColumnSlice current = slices[i];
                if (cfs.getComparator().compare(lastColumn.name(), current.finish) <= 0)
                    break;
            }
            if (i >= slices.length)
                exhausted = true;
            else
                slices = Arrays.copyOfRange(slices, i, slices.length);
        }
        return cf == null ? ColumnFamily.create(cfs.metadata) : cf;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
