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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.SSTable;



public class NamesFilter implements IFilter
{
    /* list of column names to filter against. */
    private List<String> names_;

    NamesFilter(List<String> names)
    {
        names_ = new ArrayList<String>(names);
    }

    public ColumnFamily filter(String cf, ColumnFamily columnFamily)
    {
        if ( columnFamily == null )
        {
            return columnFamily;
        }
    	String[] values = RowMutation.getColumnAndColumnFamily(cf);
		String cfName = columnFamily.name();
		ColumnFamily filteredCf = new ColumnFamily(cfName);
		if( values.length == 1 )
		{
			Collection<IColumn> columns = columnFamily.getAllColumns();
			for(IColumn column : columns)
			{
		        if ( names_.contains(column.name()) )
		        {
		            names_.remove(column.name());
					filteredCf.addColumn(column);
		        }
				if( isDone() )
				{
					return filteredCf;
				}
			}
		}
		else if ( values.length == 2 && DatabaseDescriptor.getColumnType(cfName).equals("Super"))
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
    		        if ( names_.contains(subColumn.name()) )
    		        {
    		            names_.remove(subColumn.name());
    		            filteredSuperColumn.addColumn(subColumn.name(), subColumn);
    		        }
    				if( isDone() )
    				{
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
        String columnName = column.name();
        if ( names_.contains(columnName) )
        {
            names_.remove(columnName);
        }
        else
        {
            column = null;
        }

        return column;
    }

    public boolean isDone()
    {
        return names_.isEmpty();
    }

    public DataInputBuffer next(String key, String cf, SSTable ssTable) throws IOException
    {
    	return ssTable.next(key, cf, names_);
    }

}
