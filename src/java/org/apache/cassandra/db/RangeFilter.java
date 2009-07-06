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

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.SSTableReader;

/**
 * Filters columns to satisfy colmin <= colname <= colmax
 *
 */
public class RangeFilter implements IFilter
{
    private final String colMin_;
    private final String colMax_;
    int count_;

    RangeFilter(String colMin, String colMax)
    {
        colMin_ = colMin;
        colMax_ = colMax;
        count_ = -1;
    }
    
    RangeFilter(String colMin, String colMax, int count)
    {
        colMin_ = colMin;
        colMax_ = colMax;
        count_ = count;
    }

    public ColumnFamily filter(String cfName, ColumnFamily cf)
    {
        if (cf == null)
            return null;

        ColumnFamily filteredColumnFamily = cf.cloneMeShallow();

        Collection<IColumn> columns = cf.getAllColumns();
        int i = 0;
        for (IColumn c : columns)
        {
            if ((count_ >= 0) && (i >= count_))
                break;
            if (c.name().compareTo(colMin_) >= 0
                    && c.name().compareTo(colMax_) <= 0)
            {
                filteredColumnFamily.addColumn(c);
                i++;
            }
        }
        return filteredColumnFamily;
    }

    public IColumn filter(IColumn column, DataInputStream dis)
            throws IOException
    {
        if (column == null)
            return null;

        if (column.name().compareTo(colMin_) >= 0
                && column.name().compareTo(colMax_) <= 0)
        {
            return column;
        }
        return null;
    }

    public DataInputBuffer next(String key, String cf, SSTableReader ssTable)
            throws IOException
    {
        return ssTable.next(key, cf);
    }

}
