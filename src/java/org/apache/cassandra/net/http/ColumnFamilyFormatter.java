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
package org.apache.cassandra.net.http;

import java.util.Collection;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.SuperColumn;


public class ColumnFamilyFormatter extends HTMLFormatter
{
	public ColumnFamilyFormatter()
	{
		super();
	}

	public ColumnFamilyFormatter(StringBuilder sb)
	{
		super(sb);
	}

    public void printKeyColumnFamily(StringBuilder sb, String sKey, ColumnFamily cf)
    {
    	// print the key
		sb.append("Key = " + sKey + "<br>");

		// print the column familie
		printColumnFamily(sb, cf);
    }

    public void printColumnFamily(StringBuilder sb, ColumnFamily cf)
    {
    	// first print the column family specific data
    	sb.append("ColumnFamily = " + cf.name() + "<br>");

    	Collection<IColumn> cols = cf.getAllColumns();
    	if (cf.isSuper())
    	{
    		printSuperColumns(sb, cols);
    	}
    	else
    	{
    		printSimpleColumns(sb, cols);
    	}
    }

    public void printSuperColumns(StringBuilder sb, Collection<IColumn> cols)
    {
		// print the super column summary
		sb.append("Number of super columns = " + cols.size() + "<br>");

		startTable();
		for(IColumn col : cols)
		{
	        addHeader(col.name());
			startRow();
			Collection<IColumn> simpleCols = ((SuperColumn)col).getSubColumns();
			printSimpleColumns(sb, simpleCols);
			endRow();
		}
		endTable();
    }

    public void printSimpleColumns(StringBuilder sb, Collection<IColumn> cols)
    {
		int numColumns = cols.size();
		String[] columnNames = new String[numColumns];
		String[] columnValues = new String[numColumns];

		// print the simple column summary
		//sb.append("Number of simple columns = " + cols.size() + "<br>");

		int i = 0;
		for(IColumn col : cols)
    	{
			columnNames[i] = col.name();
			columnValues[i] = new String(col.value());
			++i;
    	}

		startTable();
        addHeaders(columnNames);
		startRow();
		for(i = 0; i <  numColumns; ++i)
		{
			addCol(columnValues[i]);
		}
		endRow();
		endTable();
    }
}
