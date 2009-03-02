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

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;



/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

abstract class AbstractColumnFactory
{
    private static Map<String, AbstractColumnFactory> columnFactory_ = new HashMap<String, AbstractColumnFactory>();
	
	static
	{
		columnFactory_.put(ColumnFamily.getColumnType("Standard"),new ColumnFactory());
		columnFactory_.put(ColumnFamily.getColumnType("Super"),new SuperColumnFactory());
	}
	
	static AbstractColumnFactory getColumnFactory(String columnType)
	{
		/* Create based on the type required. */
		if ( columnType == null || columnType.equals("Standard") )
			return columnFactory_.get("Standard");
		else
			return columnFactory_.get("Super");
	}
    
	public abstract IColumn createColumn(String name);
	public abstract IColumn createColumn(String name, byte[] value);
	public abstract IColumn createColumn(String name, byte[] value, long timestamp);
    public abstract ICompactSerializer2<IColumn> createColumnSerializer();
}

class ColumnFactory extends AbstractColumnFactory
{
	public IColumn createColumn(String name)
	{
		return new Column(name);
	}
	
	public IColumn createColumn(String name, byte[] value)
	{
		return new Column(name, value);
	}
	
	public IColumn createColumn(String name, byte[] value, long timestamp)
	{
		return new Column(name, value, timestamp);
	}
    
    public ICompactSerializer2<IColumn> createColumnSerializer()
    {
        return Column.serializer();
    }
}

class SuperColumnFactory extends AbstractColumnFactory
{
    static String[] getSuperColumnAndColumn(String cName)
    {
        StringTokenizer st = new StringTokenizer(cName, ":");
        String[] values = new String[st.countTokens()];
        int i = 0;
        while ( st.hasMoreElements() )
        {
            values[i++] = (String)st.nextElement();
        }
        return values;
    }

	public IColumn createColumn(String name)
	{
		String[] values = SuperColumnFactory.getSuperColumnAndColumn(name);
        if ( values.length == 0 ||  values.length > 2 )
            throw new IllegalArgumentException("Super Column " + name + " in invalid format. Must be in <super column name>:<column name> format.");
        IColumn superColumn = new SuperColumn(values[0]);
        if(values.length == 2)
        {
	        IColumn subColumn = new Column(values[1]);
	        superColumn.addColumn(values[1], subColumn);
        }
		return superColumn;
	}
	
	public IColumn createColumn(String name, byte[] value)
	{
		String[] values = SuperColumnFactory.getSuperColumnAndColumn(name);
        if ( values.length != 2 )
            throw new IllegalArgumentException("Super Column " + name + " in invalid format. Must be in <super column name>:<column name> format.");
        IColumn superColumn = new SuperColumn(values[0]);
        IColumn subColumn = new Column(values[1], value);
        superColumn.addColumn(values[1], subColumn);
		return superColumn;
	}
	
	public IColumn createColumn(String name, byte[] value, long timestamp)
	{
		String[] values = SuperColumnFactory.getSuperColumnAndColumn(name);
        if ( values.length != 2 )
            throw new IllegalArgumentException("Super Column " + name + " in invalid format. Must be in <super column name>:<column name> format.");
        IColumn superColumn = new SuperColumn(values[0]);
        IColumn subColumn = new Column(values[1], value, timestamp);
        superColumn.addColumn(values[1], subColumn);
		return superColumn;
	}
    
    public ICompactSerializer2<IColumn> createColumnSerializer()
    {
        return SuperColumn.serializer();
    }
}