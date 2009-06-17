/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.SSTable;


public class IdentityFilter implements IFilter
{
    private boolean isDone_ = false;
    
	public boolean isDone()
	{
		return isDone_;
	}

	public ColumnFamily filter(String cfString, ColumnFamily columnFamily)
	{
    	String[] values = RowMutation.getColumnAndColumnFamily(cfString);
    	if( columnFamily == null )
    		return columnFamily;

		if (values.length == 2 && !columnFamily.isSuper())
		{
			Collection<IColumn> columns = columnFamily.getAllColumns();
			if(columns.size() >= 1)
				isDone_ = true;
		}
		if (values.length == 3 && columnFamily.isSuper())
		{
    		Collection<IColumn> columns = columnFamily.getAllColumns();
    		for(IColumn column : columns)
    		{
    			SuperColumn superColumn = (SuperColumn)column;
        		Collection<IColumn> subColumns = superColumn.getSubColumns();
        		if( subColumns.size() >= 1 )
        			isDone_ = true;
    		}
		}
		return columnFamily;
	}

	public IColumn filter(IColumn column, DataInputStream dis) throws IOException
	{
		// TODO Auto-generated method stub
		return column;
	}

	public DataInputBuffer next(String key, String cf, SSTable ssTable) throws IOException
	{
		return ssTable.next(key, cf);
	}

	public void setDone()
	{
		isDone_ = true;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		// TODO Auto-generated method stub

	}

}
