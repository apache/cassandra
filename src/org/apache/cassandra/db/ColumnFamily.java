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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.HashingSchemes;
import org.apache.log4j.Logger;
import org.apache.cassandra.io.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public final class ColumnFamily implements Serializable
{
    private static ICompactSerializer2<ColumnFamily> serializer_;
    public static final short utfPrefix_ = 2;   
    public static final String defaultColumnSortProperty_ = "Time";
    /* The column serializer for this Column Family. Create based on config. */

    private static Logger logger_ = Logger.getLogger( ColumnFamily.class );
    private static Map<String, String> columnTypes_ = new HashMap<String, String>();
    private static Map<String, String> indexTypes_ = new HashMap<String, String>();

    static
    {
        serializer_ = new ColumnFamilySerializer();
        /* TODO: These are the various column types. Hard coded for now. */
        columnTypes_.put("Standard", "Standard");
        columnTypes_.put("Super", "Super");
        
        indexTypes_.put("Name", "Name");
        indexTypes_.put("Time", "Time");
    }

    public static ICompactSerializer2<ColumnFamily> serializer()
    {
        return serializer_;
    }

    /*
     * This method returns the serializer whose methods are
     * preprocessed by a dynamic proxy.
    */
    public static ICompactSerializer2<ColumnFamily> serializer2()
    {
        return (ICompactSerializer2<ColumnFamily>)Proxy.newProxyInstance( ColumnFamily.class.getClassLoader(), new Class[]{ICompactSerializer2.class}, new CompactSerializerInvocationHandler<ColumnFamily>(serializer_) );
    }

    public static String getColumnType(String key)
    {
    	if ( key == null )
    		return columnTypes_.get("Standard");
    	return columnTypes_.get(key);
    }

    public static String getColumnSortProperty(String columnIndexProperty)
    {
    	if ( columnIndexProperty == null )
    		return indexTypes_.get("Time");
        String columnSortType = indexTypes_.get(columnIndexProperty);
    	return (columnSortType == null) ? ColumnFamily.defaultColumnSortProperty_ : columnSortType;
    }

    private transient AbstractColumnFactory columnFactory_;

    private String name_;

    private  transient ICompactSerializer2<IColumn> columnSerializer_;
    private transient AtomicBoolean isMarkedForDelete_;
    private  AtomicInteger size_ = new AtomicInteger(0);
    private EfficientBidiMap columns_;

    private Comparator<IColumn> columnComparator_;

	private Comparator<IColumn> getColumnComparator(String cfName, String columnType)
	{
		if(columnComparator_ == null)
		{
			/*
			 * if this columnfamily has supercolumns or there is an index on the column name,
			 * then sort by name
			*/
			if("Super".equals(columnType) || DatabaseDescriptor.isNameSortingEnabled(cfName))
			{
				columnComparator_ = ColumnComparatorFactory.getComparator(ColumnComparatorFactory.ComparatorType.NAME);
			}
			/* if this columnfamily has simple columns, and no index on name sort by timestamp */
			else
			{
				columnComparator_ = ColumnComparatorFactory.getComparator(ColumnComparatorFactory.ComparatorType.TIMESTAMP);
			}
		}

		return columnComparator_;
	}
    
    ColumnFamily()
    {
    }

    public ColumnFamily(String cf)
    {
        name_ = cf;
        createColumnFactoryAndColumnSerializer();
    }

    public ColumnFamily(String cf, String columnType)
    {
        name_ = cf;
        createColumnFactoryAndColumnSerializer(columnType);
    }

    void createColumnFactoryAndColumnSerializer(String columnType)
    {
        if ( columnFactory_ == null )
        {
            columnFactory_ = AbstractColumnFactory.getColumnFactory(columnType);
            columnSerializer_ = columnFactory_.createColumnSerializer();
            if(columns_ == null)
                columns_ = new EfficientBidiMap(getColumnComparator(name_, columnType));
        }
    }

    void createColumnFactoryAndColumnSerializer()
    {
    	String columnType = DatabaseDescriptor.getColumnFamilyType(name_);
        if ( columnType == null )
        {
        	List<String> tables = DatabaseDescriptor.getTables();
        	if ( tables.size() > 0 )
        	{
        		String table = tables.get(0);
        		columnType = Table.open(table).getColumnFamilyType(name_);
        	}
        }
        createColumnFactoryAndColumnSerializer(columnType);
    }

    ColumnFamily cloneMe()
    {
    	ColumnFamily cf = new ColumnFamily(name_);
    	cf.isMarkedForDelete_ = isMarkedForDelete_;
    	cf.columns_ = columns_.cloneMe();
    	return cf;
    }

    public String name()
    {
        return name_;
    }

    /**
     *  We need to go through each column
     *  in the column family and resolve it before adding
    */
    void addColumns(ColumnFamily cf)
    {
        for (IColumn column : cf.getAllColumns()) {
            addColumn(column);
        }
    }

    public ICompactSerializer2<IColumn> getColumnSerializer()
    {
        createColumnFactoryAndColumnSerializer();
    	return columnSerializer_;
    }

    public void createColumn(String name)
    {
    	IColumn column = columnFactory_.createColumn(name);
    	addColumn(column);
    }

    int getColumnCount()
    {
    	int count = 0;
    	Map<String, IColumn> columns = columns_.getColumns();
    	if( columns != null )
    	{
    		if(!DatabaseDescriptor.getColumnType(name_).equals("Super"))
    		{
    			count = columns.size();
    		}
    		else
    		{
    			Collection<IColumn> values = columns.values();
		    	for(IColumn column: values)
		    	{
		    		count += column.getObjectCount();
		    	}
    		}
    	}
    	return count;
    }

    public void createColumn(String name, byte[] value)
    {
    	IColumn column = columnFactory_.createColumn(name, value);
    	addColumn(column);
    }

	public void createColumn(String name, byte[] value, long timestamp)
	{
		IColumn column = columnFactory_.createColumn(name, value, timestamp);
		addColumn(column);
	}

    void clear()
    {
    	columns_.clear();
    }

    /*
     * If we find an old column that has the same name
     * the ask it to resolve itself else add the new column .
    */
    void addColumn(IColumn column)
    {
    	int newSize = 0;
        String name = column.name();
        IColumn oldColumn = columns_.get(name);
        if ( oldColumn != null )
        {
            int oldSize = oldColumn.size();
            if( oldColumn.putColumn(column))
            {
            	// This will never be called for super column as put column always returns false.
                columns_.put(name, column);
            	newSize = column.size();
            }
            else
            {
            	newSize = oldColumn.size();
            }
            size_.addAndGet(newSize - oldSize);
        }
        else
        {
            newSize = column.size();
            size_.addAndGet(newSize);
            columns_.put(name, column);
        }
    }

    public IColumn getColumn(String name)
    {
        return columns_.get( name );
    }

    public Collection<IColumn> getAllColumns()
    {
        return columns_.getSortedColumns();
    }

    Map<String, IColumn> getColumns()
    {
        return columns_.getColumns();
    }

    public void remove(String columnName)
    {
    	columns_.remove(columnName);
    }

    void delete()
    {
        if ( isMarkedForDelete_ == null )
            isMarkedForDelete_ = new AtomicBoolean(true);
        else
            isMarkedForDelete_.set(true);
    }

    boolean isMarkedForDelete()
    {
        return ( ( isMarkedForDelete_ == null ) ? false : isMarkedForDelete_.get() );
    }

    /*
     * This is used as oldCf.merge(newCf). Basically we take the newCf
     * and merge it into the oldCf.
    */
    void merge(ColumnFamily columnFamily)
    {
        Map<String, IColumn> columns = columnFamily.getColumns();
        Set<String> cNames = columns.keySet();

        for ( String cName : cNames )
        {
            columns_.put(cName, columns.get(cName));
        }
    }

    /*
     * This function will repair a list of columns
     * If there are any columns in the external list which are not present
     * internally then they are added ( this might have to change depending on
     * how we implement delete repairs).
     * Also if there are any columns in teh internal and not in the external
     * they are kept intact.
     * Else the one with the greatest timestamp is considered latest.
     */
    void repair(ColumnFamily columnFamily)
    {
        Map<String, IColumn> columns = columnFamily.getColumns();
        Set<String> cNames = columns.keySet();

        for ( String cName : cNames )
        {
        	IColumn columnInternal = columns_.get(cName);
        	IColumn columnExternal = columns.get(cName);

        	if( columnInternal == null )
        	{                
        		if(DatabaseDescriptor.getColumnFamilyType(name_).equals(ColumnFamily.getColumnType("Super")))
        		{
        			columnInternal = new SuperColumn(columnExternal.name());
        			columns_.put(cName, columnInternal);
        		}
        		if(DatabaseDescriptor.getColumnFamilyType(name_).equals(ColumnFamily.getColumnType("Standard")))
        		{
        			columnInternal = columnExternal;
        			columns_.put(cName, columnInternal);
        		}
        	}
       		columnInternal.repair(columnExternal);
        }
    }


    /*
     * This function will calculate the differnce between 2 column families
     * the external input is considered the superset of internal
     * so there are no deletes in the diff.
     */
    ColumnFamily diff(ColumnFamily columnFamily)
    {
    	ColumnFamily cfDiff = new ColumnFamily(columnFamily.name());
        Map<String, IColumn> columns = columnFamily.getColumns();
        Set<String> cNames = columns.keySet();

        for ( String cName : cNames )
        {
        	IColumn columnInternal = columns_.get(cName);
        	IColumn columnExternal = columns.get(cName);
        	if( columnInternal == null )
        	{
        		cfDiff.addColumn(columnExternal);
        	}
        	else
        	{
            	IColumn columnDiff = columnInternal.diff(columnExternal);
        		if(columnDiff != null)
        		{
        			cfDiff.addColumn(columnDiff);
        		}
        	}
        }
        if(cfDiff.getColumns().size() != 0)
        	return cfDiff;
        else
        	return null;
    }

    int size()
    {
        if ( size_.get() == 0 )
        {
            Set<String> cNames = columns_.getColumns().keySet();
            for ( String cName : cNames )
            {
                size_.addAndGet(columns_.get(cName).size());
            }
        }
        return size_.get();
    }

    public int hashCode()
    {
        return name().hashCode();
    }

    public boolean equals(Object o)
    {
        if ( !(o instanceof ColumnFamily) )
            return false;
        ColumnFamily cf = (ColumnFamily)o;
        return name().equals(cf.name());
    }

    public String toString()
    {
    	StringBuilder sb = new StringBuilder();
    	sb.append(name_);
    	sb.append(":");
    	sb.append(isMarkedForDelete());
    	sb.append(":");
    	Collection<IColumn> columns = getAllColumns();
        sb.append(columns.size());
        sb.append(":");

        for ( IColumn column : columns )
        {
            sb.append(column.toString());
        }
        sb.append(":");
    	return sb.toString();
    }

    public byte[] digest()
    {
    	Set<IColumn> columns = columns_.getSortedColumns();
    	byte[] xorHash = new byte[0];
    	byte[] tmpHash = new byte[0];
    	for(IColumn column : columns)
    	{
    		if(xorHash.length == 0)
    		{
    			xorHash = column.digest();
    		}
    		else
    		{
    			tmpHash = column.digest();
    			xorHash = FBUtilities.xor(xorHash, tmpHash);
    		}
    	}
    	return xorHash;
    }
}

class ColumnFamilySerializer implements ICompactSerializer2<ColumnFamily>
{
	/*
	 * We are going to create indexes, and write out that information as well. The format
	 * of the data serialized is as follows.
	 *
	 * 1) Without indexes:
     *  // written by the data
	 * 	<boolean false (index is not present)>
	 * 	<column family id>
	 * 	<is marked for delete>
	 * 	<total number of columns>
	 * 	<columns data>

	 * 	<boolean true (index is present)>
	 *
	 *  This part is written by the column indexer
	 * 	<size of index in bytes>
	 * 	<list of column names and their offsets relative to the first column>
	 *
	 *  <size of the cf in bytes>
	 * 	<column family id>
	 * 	<is marked for delete>
	 * 	<total number of columns>
	 * 	<columns data>
	*/
    public void serialize(ColumnFamily columnFamily, DataOutputStream dos) throws IOException
    {
    	Collection<IColumn> columns = columnFamily.getAllColumns();

        /* write the column family id */
        dos.writeUTF(columnFamily.name());
        /* write if this cf is marked for delete */
        dos.writeBoolean(columnFamily.isMarkedForDelete());
    	/* write the size is the number of columns */
        dos.writeInt(columns.size());                    
        /* write the column data */
    	for ( IColumn column : columns )
        {
            columnFamily.getColumnSerializer().serialize(column, dos);            
        }
    }

    /*
     * Use this method to create a bare bones Column Family. This column family
     * does not have any of the Column information.
    */
    private ColumnFamily defreezeColumnFamily(DataInputStream dis) throws IOException
    {
        String name = dis.readUTF();
        boolean delete = dis.readBoolean();
        ColumnFamily cf = new ColumnFamily(name);
        if ( delete )
            cf.delete();
        return cf;
    }

    /*
     * This method fills the Column Family object with the column information
     * from the DataInputStream. The "items" parameter tells us whether we need
     * all the columns or just a subset of all the Columns that make up the
     * Column Family. If "items" is -1 then we need all the columns if not we
     * deserialize only as many columns as indicated by the "items" parameter.
    */
    private void fillColumnFamily(ColumnFamily cf,  DataInputStream dis) throws IOException
    {
        int size = dis.readInt();        	        	
    	IColumn column = null;           
        for ( int i = 0; i < size; ++i )
        {
        	column = cf.getColumnSerializer().deserialize(dis);
        	if(column != null)
        	{
        		cf.addColumn(column);
        	}
        }
    }

    public ColumnFamily deserialize(DataInputStream dis) throws IOException
    {       
        ColumnFamily cf = defreezeColumnFamily(dis);
        if ( !cf.isMarkedForDelete() )
            fillColumnFamily(cf,dis);
        return cf;
    }

    /*
     * This version of deserialize is used when we need a specific set if columns for
     * a column family specified in the name cfName parameter.
    */
    public ColumnFamily deserialize(DataInputStream dis, IFilter filter) throws IOException
    {        
        ColumnFamily cf = defreezeColumnFamily(dis);
        if ( !cf.isMarkedForDelete() )
        {
            int size = dis.readInt();        	        	
        	IColumn column = null;
            for ( int i = 0; i < size; ++i )
            {
            	column = cf.getColumnSerializer().deserialize(dis, filter);
            	if(column != null)
            	{
            		cf.addColumn(column);
            		column = null;
            		if(filter.isDone())
            		{
            			break;
            		}
            	}
            }
        }
        return cf;
    }

    /*
     * Deserialize a particular column or super column or the entire columnfamily given a : seprated name
     * name could be of the form cf:superColumn:column  or cf:column or cf
     */
    public ColumnFamily deserialize(DataInputStream dis, String name, IFilter filter) throws IOException
    {        
        String[] names = RowMutation.getColumnAndColumnFamily(name);
        String columnName = "";
        if ( names.length == 1 )
            return deserialize(dis, filter);
        if( names.length == 2 )
            columnName = names[1];
        if( names.length == 3 )
            columnName = names[1]+ ":" + names[2];

        ColumnFamily cf = defreezeColumnFamily(dis);
        if ( !cf.isMarkedForDelete() )
        {
            /* read the number of columns */
            int size = dis.readInt();            
            for ( int i = 0; i < size; ++i )
            {
	            IColumn column = cf.getColumnSerializer().deserialize(dis, columnName, filter);
	            if ( column != null )
	            {
	                cf.addColumn(column);
	                break;
	            }
            }
        }
        return cf;
    }

    public void skip(DataInputStream dis) throws IOException
    {
        throw new UnsupportedOperationException("This operation is not yet supported.");
    }

}
