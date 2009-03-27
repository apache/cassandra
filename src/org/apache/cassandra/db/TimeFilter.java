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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.IndexHelper;
import org.apache.cassandra.io.SSTable;


/**
 * This class provides a filter for fitering out columns
 * that are older than a specific time.
 * 
 * @author pmalik
 *
 */
class TimeFilter implements IFilter
{
	private long timeLimit_;
	private boolean isDone_;
	
	TimeFilter(long timeLimit)
	{
		timeLimit_ = timeLimit;		
		isDone_ = false;
	}

	public ColumnFamily filter(String cf, ColumnFamily columnFamily)
	{
    	if (columnFamily == null)
    		return columnFamily;

        String[] values = RowMutation.getColumnAndColumnFamily(cf);
		String cfName = columnFamily.name();
		ColumnFamily filteredCf = new ColumnFamily(cfName);
		if( values.length == 1 && !DatabaseDescriptor.getColumnType(cfName).equals("Super"))
		{
    		Collection<IColumn> columns = columnFamily.getAllColumns();
    		int i =0; 
    		for(IColumn column : columns)
    		{
    			if ( column.timestamp() >=  timeLimit_ )
    			{
    				filteredCf.addColumn(column);
    				++i;
    			}
    			else
    			{
    				break;
    			}
    		}
    		if( i < columns.size() )
    		{
    			isDone_ = true;
    		}
		}    	
    	else if ( values.length == 2 && DatabaseDescriptor.getColumnType(cfName).equals("Super") )
    	{
    		/* 
    		 * TODO : For super columns we need to re-visit this issue.
    		 * For now this fn will set done to true if we are done with
    		 * atleast one super column
    		 */
    		Collection<IColumn> columns = columnFamily.getAllColumns();
    		for(IColumn column : columns)
    		{
    			SuperColumn superColumn = (SuperColumn)column;
       			SuperColumn filteredSuperColumn = new SuperColumn(superColumn.name());
				filteredCf.addColumn(filteredSuperColumn);
        		Collection<IColumn> subColumns = superColumn.getSubColumns();
        		int i = 0;
        		for(IColumn subColumn : subColumns)
        		{
	    			if (  subColumn.timestamp()  >=  timeLimit_ )
	    			{
			            filteredSuperColumn.addColumn(subColumn.name(), subColumn);
	    				++i;
	    			}
	    			else
	    			{
	    				break;
	    			}
        		}
        		if( i < filteredSuperColumn.getColumnCount() )
        		{
        			isDone_ = true;
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
    	if(column instanceof Column)
    	{
	    	dis.mark(1000);
	        dis.readBoolean();
	        timeStamp = dis.readLong();
		    dis.reset();
	    	if( timeStamp < timeLimit_ )
	    	{
	    		isDone_ = true;
	    		return null;
	    	}
    	}
    	return column;
    }
    
	
	public boolean isDone()
	{
		return isDone_;
	}

	public DataInputBuffer next(String key, String cf, SSTable ssTable) throws IOException
    {
    	return ssTable.next( key, cf, null, new IndexHelper.TimeRange( timeLimit_, System.currentTimeMillis() ) );
    }
}
