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
import org.apache.cassandra.io.IndexHelper;
import org.apache.cassandra.io.SSTableReader;


/**
 * This class provides a filter for fitering out columns
 * that are older than a specific time.
 *
 * @author pmalik
 */
class TimeFilter implements IFilter
{
    private long timeLimit_;

    TimeFilter(long timeLimit)
    {
        timeLimit_ = timeLimit;
    }

    public ColumnFamily filter(String cf, ColumnFamily columnFamily)
    {
        if (columnFamily == null)
            return null;

        String[] values = RowMutation.getColumnAndColumnFamily(cf);
        ColumnFamily filteredCf = columnFamily.cloneMeShallow();
        if (values.length == 1 && !columnFamily.isSuper())
        {
            Collection<IColumn> columns = columnFamily.getAllColumns();
            for (IColumn column : columns)
            {
                if (column.timestamp() >= timeLimit_)
                {
                    filteredCf.addColumn(column);
                }
                else
                {
                    break;
                }
            }
        }
        else if (values.length == 2 && columnFamily.isSuper())
        {
            /*
                * TODO : For super columns we need to re-visit this issue.
                * For now this fn will set done to true if we are done with
                * atleast one super column
                */
            Collection<IColumn> columns = columnFamily.getAllColumns();
            for (IColumn column : columns)
            {
                SuperColumn superColumn = (SuperColumn) column;
                SuperColumn filteredSuperColumn = new SuperColumn(superColumn.name());
                filteredSuperColumn.markForDeleteAt(column.getLocalDeletionTime(), column.getMarkedForDeleteAt());
                filteredCf.addColumn(filteredSuperColumn);
                Collection<IColumn> subColumns = superColumn.getSubColumns();
                for (IColumn subColumn : subColumns)
                {
                    if (subColumn.timestamp() >= timeLimit_)
                    {
                        filteredSuperColumn.addColumn(subColumn);
                    }
                    else
                    {
                        break;
                    }
                }
            }
        }
        else
        {
            throw new UnsupportedOperationException();
        }
        return filteredCf;
    }

    public IColumn filter(IColumn column, DataInputStream dis) throws IOException
    {
        long timeStamp = 0;
        /*
           * If its a column instance we need the timestamp to verify if
           * it should be filtered , but at this instance the timestamp is not read
           * so we read the timestamp and set the buffer back so that the rest of desrialization
           * logic does not change.
           */
        if (column instanceof Column)
        {
            dis.mark(1000);
            dis.readBoolean();
            timeStamp = dis.readLong();
            dis.reset();
            if (timeStamp < timeLimit_)
            {
                return null;
            }
        }
        return column;
    }

    public DataInputBuffer next(String key, String cfName, SSTableReader ssTable) throws IOException
    {
        return ssTable.next(key, cfName, null, new IndexHelper.TimeRange(timeLimit_, Long.MAX_VALUE));
    }
}
