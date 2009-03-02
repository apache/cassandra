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

    	String columnFamilyType = DatabaseDescriptor.getColumnType(cf.name());
    	Collection<IColumn> cols = cf.getAllColumns();
    	if("Super".equals(columnFamilyType))
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
