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
import java.util.Comparator;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class ColumnComparatorFactory
{
	public static enum ComparatorType
	{
		NAME,
		TIMESTAMP
	}

	private static Comparator<IColumn> nameComparator_ = new ColumnNameComparator();
	private static Comparator<IColumn> timestampComparator_ = new ColumnTimestampComparator();

	public static Comparator<IColumn> getComparator(ComparatorType comparatorType)
	{
		Comparator<IColumn> columnComparator = timestampComparator_;

		switch(comparatorType)
		{
			case NAME:
				columnComparator = nameComparator_;
				break;

			case TIMESTAMP:

			default:
				columnComparator = timestampComparator_;
				break;
		}

		return columnComparator;
	}

	public static Comparator<IColumn> getComparator(int comparatorTypeInt)
	{
		ComparatorType comparatorType = ComparatorType.NAME;

		if(comparatorTypeInt == ComparatorType.NAME.ordinal())
		{
			comparatorType = ComparatorType.NAME;
		}
		else if(comparatorTypeInt == ComparatorType.TIMESTAMP.ordinal())
		{
			comparatorType = ComparatorType.TIMESTAMP;
		}
		return getComparator(comparatorType);
	}

	public static void main(String[] args)
	{
		IColumn col1 = new Column("Column-9");
		IColumn col2 = new Column("Column-10");
		System.out.println("Result of compare: " + getComparator(ComparatorType.NAME).compare(col1, col2));
	}
}

abstract class AbstractColumnComparator implements Comparator<IColumn>, Serializable
{
	protected ColumnComparatorFactory.ComparatorType comparatorType_;

	public AbstractColumnComparator(ColumnComparatorFactory.ComparatorType comparatorType)
	{
		comparatorType_ = comparatorType;
	}

	ColumnComparatorFactory.ComparatorType getComparatorType()
	{
		return comparatorType_;
	}
}

class ColumnTimestampComparator extends AbstractColumnComparator
{
	ColumnTimestampComparator()
	{
		super(ColumnComparatorFactory.ComparatorType.TIMESTAMP);
	}

	/* if the time-stamps are the same then sort by names */
    public int compare(IColumn column1, IColumn column2)
    {
    	/* inverse sort by time to get hte latest first */
    	long result = column2.timestamp() - column1.timestamp();
    	int finalResult = 0;
    	if(result == 0)
    	{
    		result = column1.name().compareTo(column2.name());
    	}
    	if(result > 0)
    	{
    		finalResult = 1;
    	}
    	if( result < 0 )
    	{
    		finalResult = -1;
    	}
        return finalResult;
    }
}

class ColumnNameComparator extends AbstractColumnComparator
{
	ColumnNameComparator()
	{
		super(ColumnComparatorFactory.ComparatorType.NAME);
	}

    /* if the names are the same then sort by time-stamps */
    public int compare(IColumn column1, IColumn column2)
    {
    	long result = column1.name().compareTo(column2.name());
    	int finalResult = 0;
    	if(result == 0)
    	{
    		/* inverse sort by time to get hte latest first */
    		result = column2.timestamp() - column1.timestamp();
    	}
    	if(result > 0)
    	{
    		finalResult = 1;
    	}
    	if( result < 0 )
    	{
    		finalResult = -1;
    	}
        return finalResult;
    }
}
