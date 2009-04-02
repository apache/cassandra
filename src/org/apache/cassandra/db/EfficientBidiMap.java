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

import java.io.Serializable;
import java.util.*;

import org.apache.cassandra.db.ColumnComparatorFactory.ComparatorType;
import org.apache.cassandra.service.StorageService;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class EfficientBidiMap implements Serializable
{
    private Map<String, IColumn> map_ = new HashMap<String, IColumn>();
    private SortedSet<IColumn> sortedSet_;
    private Comparator<IColumn> columnComparator_;

    EfficientBidiMap()
    {
    	this(ColumnComparatorFactory.getComparator(ComparatorType.TIMESTAMP));
    }

    EfficientBidiMap(Comparator<IColumn> columnComparator)
    {
    	columnComparator_ = columnComparator;
    	sortedSet_ = new TreeSet<IColumn>(columnComparator);
    }

    EfficientBidiMap(Map<String, IColumn> map, SortedSet<IColumn> set, Comparator<IColumn> comparator)
    {
    	map_ = map;
    	sortedSet_ = set;
    	columnComparator_ = comparator;
    }

    EfficientBidiMap(Object[] objects, Comparator<IColumn> columnComparator)
    {
    	columnComparator_ = columnComparator;
    	sortedSet_ = new TreeSet<IColumn>(columnComparator);
        for ( Object object : objects )
        {
            IColumn column = (IColumn)object;
            sortedSet_.add(column);
            map_.put(column.name(), column);
        }
    }

    public Comparator<IColumn> getComparator()
    {
    	return columnComparator_;
    }

    public void put(String key, IColumn column)
    {
        IColumn oldColumn = map_.put(key, column);
        if (oldColumn != null)
            sortedSet_.remove(oldColumn);
        sortedSet_.add(column);
    }

    public IColumn get(String key)
    {
        return map_.get(key);
    }

    public SortedSet<IColumn> getSortedColumns()
    {
    	return sortedSet_;
    }

    public Map<String, IColumn> getColumns()
    {
        return map_;
    }

    public int size()
    {
    	return map_.size();
    }

    public void remove (String columnName)
    {
    	sortedSet_.remove(map_.get(columnName));
    	map_.remove(columnName);
    }
    void clear()
    {
    	map_.clear();
    	sortedSet_.clear();
    }

    ColumnComparatorFactory.ComparatorType getComparatorType()
	{
		return ((AbstractColumnComparator)columnComparator_).getComparatorType();
	}

    EfficientBidiMap cloneMe()
    {
    	Map<String, IColumn> map = new HashMap<String, IColumn>(map_);
    	SortedSet<IColumn> set = new TreeSet<IColumn>(sortedSet_);
    	return new EfficientBidiMap(map, set, columnComparator_);
    }
}


