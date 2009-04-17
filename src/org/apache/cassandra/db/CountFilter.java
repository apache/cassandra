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
import org.apache.cassandra.io.SSTable;


/**
 * This class provides a filter for fitering out columns
 * greater than a certain count.
 * 
 * @author pmalik
 *
 */
public class CountFilter implements IFilter
{
	private long countLimit_;
	private boolean isDone_;

	CountFilter(int countLimit)
	{
		countLimit_ = countLimit;
		isDone_ = false;
	}

	public ColumnFamily filter(String cfNameParam, ColumnFamily columnFamily)
	{
    	String[] values = RowMutation.getColumnAndColumnFamily(cfNameParam);
        if ( columnFamily == null )
            return columnFamily;

        ColumnFamily filteredCf = new ColumnFamily(columnFamily.name(), columnFamily.type());
		if( countLimit_ <= 0 )
		{
			isDone_ = true;
			return filteredCf;
		}
		if( values.length == 1)
		{
    		Collection<IColumn> columns = columnFamily.getAllColumns();
    		for(IColumn column : columns)
    		{
    			filteredCf.addColumn(column);
    			countLimit_--;
    			if( countLimit_ <= 0 )
    			{
    				isDone_ = true;
    				return filteredCf;
    			}
    		}
		}
		else if(values.length == 2 && columnFamily.isSuper())
		{
    		Collection<IColumn> columns = columnFamily.getAllColumns();
    		for(IColumn column : columns)
    		{
    			SuperColumn superColumn = (SuperColumn)column;
    			SuperColumn filteredSuperColumn = new SuperColumn(superColumn.name());
				filteredCf.addColumn(filteredSuperColumn);
        		Collection<IColumn> subColumns = superColumn.getSubColumns();
        		for(IColumn subColumn : subColumns)
        		{
		            filteredSuperColumn.addColumn(subColumn.name(), subColumn);
	    			countLimit_--;
	    			if( countLimit_ <= 0 )
	    			{
	    				isDone_ = true;
	    				return filteredCf;
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
		countLimit_--;
		if( countLimit_ <= 0 )
		{
			isDone_ = true;
		}
		return column;
    }

	public boolean isDone()
	{
		return isDone_;
	}

	public void setDone()
	{
		isDone_ = true;
	}

    public DataInputBuffer next(String key, String cf, SSTable ssTable) throws IOException
    {
    	return ssTable.next(key, cf);
    }
}
