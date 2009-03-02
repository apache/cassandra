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
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.HashingSchemes;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public final class SuperColumn implements IColumn, Serializable
{
	private static Logger logger_ = Logger.getLogger(SuperColumn.class);
	private static ICompactSerializer2<IColumn> serializer_;
	private final static String seperator_ = ":";

    static
    {
        serializer_ = new SuperColumnSerializer();
    }

    static ICompactSerializer2<IColumn> serializer()
    {
        return serializer_;
    }

	private String name_;
    private EfficientBidiMap columns_ = new EfficientBidiMap(ColumnComparatorFactory.getComparator(ColumnComparatorFactory.ComparatorType.TIMESTAMP));
	private AtomicBoolean isMarkedForDelete_ = new AtomicBoolean(false);
    private AtomicInteger size_ = new AtomicInteger(0);

    SuperColumn()
    {
    }

    SuperColumn(String name)
    {
    	name_ = name;
    }

	public boolean isMarkedForDelete()
	{
		return isMarkedForDelete_.get();
	}

    public String name()
    {
    	return name_;
    }

    public Collection<IColumn> getSubColumns()
    {
    	return columns_.getSortedColumns();
    }

    public IColumn getSubColumn( String columnName )
    {
    	IColumn column = columns_.get(columnName);
    	if ( column instanceof SuperColumn )
    		throw new UnsupportedOperationException("A super column cannot hold other super columns.");
		return column;
    }

    public int compareTo(IColumn superColumn)
    {
        return (name_.compareTo(superColumn.name()));
    }


    public int size()
    {
        /*
         * return the size of the individual columns
         * that make up the super column. This is an
         * APPROXIMATION of the size used only from the
         * Memtable.
        */
        return size_.get();
    }

    /**
     * This returns the size of the super-column when serialized.
     * @see org.apache.cassandra.db.IColumn#serializedSize()
    */
    public int serializedSize()
    {
        /*
         * Size of a super-column is =
         *   size of a name (UtfPrefix + length of the string)
         * + 1 byte to indicate if the super-column has been deleted
         * + 4 bytes for size of the sub-columns
         * + 4 bytes for the number of sub-columns
         * + size of all the sub-columns.
        */

    	/*
    	 * We store the string as UTF-8 encoded, so when we calculate the length, it
    	 * should be converted to UTF-8.
    	 */
    	/*
    	 * We need to keep the way we are calculating the column size in sync with the
    	 * way we are calculating the size for the column family serializer.
    	 */
    	return IColumn.UtfPrefix_ + FBUtilities.getUTF8Length(name_) + DBConstants.boolSize_ + DBConstants.intSize_ + DBConstants.intSize_ + getSizeOfAllColumns();
    }

    /**
     * This calculates the exact size of the sub columns on the fly
     */
    int getSizeOfAllColumns()
    {
        int size = 0;
        Collection<IColumn> subColumns = getSubColumns();
        for ( IColumn subColumn : subColumns )
        {
            size += subColumn.serializedSize();
        }
        return size;
    }

    protected void remove(String columnName)
    {
    	columns_.remove(columnName);
    }

    public long timestamp()
    {
    	throw new UnsupportedOperationException("This operation is not supported for Super Columns.");
    }

    public long timestamp(String key)
    {
    	IColumn column = columns_.get(key);
    	if ( column instanceof SuperColumn )
    		throw new UnsupportedOperationException("A super column cannot hold other super columns.");
    	if ( column != null )
    		return column.timestamp();
    	throw new IllegalArgumentException("Timestamp was requested for a column that does not exist.");
    }

    public byte[] value()
    {
    	throw new UnsupportedOperationException("This operation is not supported for Super Columns.");
    }

    public byte[] value(String key)
    {
    	IColumn column = columns_.get(key);
    	if ( column instanceof SuperColumn )
    		throw new UnsupportedOperationException("A super column cannot hold other super columns.");
    	if ( column != null )
    		return column.value();
    	throw new IllegalArgumentException("Value was requested for a column that does not exist.");
    }

    public void addColumn(String name, IColumn column)
    {
    	if ( column instanceof SuperColumn )
    		throw new UnsupportedOperationException("A super column cannot hold other super columns.");
    	IColumn oldColumn = columns_.get(name);
    	if ( oldColumn == null )
        {
    		columns_.put(name, column);
            size_.addAndGet(column.size());
        }
    	else
    	{
    		if ( oldColumn.timestamp() <= column.timestamp() )
            {
    			columns_.put(name, column);
                int delta = (-1)*oldColumn.size();
                /* subtract the size of the oldColumn */
                size_.addAndGet(delta);
                /* add the size of the new column */
                size_.addAndGet(column.size());
            }
    	}
    }

    /*
     * Go through each sub column if it exists then as it to resolve itself
     * if the column does not exist then create it.
     */
    public boolean putColumn(IColumn column)
    {
    	if ( !(column instanceof SuperColumn))
    		throw new UnsupportedOperationException("Only Super column objects should be put here");
    	if( !name_.equals(column.name()))
    		throw new IllegalArgumentException("The name should match the name of the current column or super column");
    	Collection<IColumn> columns = column.getSubColumns();

        for ( IColumn subColumn : columns )
        {
       		addColumn(subColumn.name(), subColumn);
        }
        return false;
    }

    public int getObjectCount()
    {
    	return 1 + columns_.size();
    }

    public void delete()
    {
    	columns_.clear();
    	isMarkedForDelete_.set(true);
    }

    int getColumnCount()
    {
    	return columns_.size();
    }

    public void repair(IColumn column)
    {
    	Collection<IColumn> columns = column.getSubColumns();

        for ( IColumn subColumn : columns )
        {
        	IColumn columnInternal = columns_.get(subColumn.name());
        	if( columnInternal == null )
        		columns_.put(subColumn.name(), subColumn);
        	else
        		columnInternal.repair(subColumn);
        }
    }


    public IColumn diff(IColumn column)
    {
    	IColumn  columnDiff = new SuperColumn(column.name());
    	Collection<IColumn> columns = column.getSubColumns();

        for ( IColumn subColumn : columns )
        {
        	IColumn columnInternal = columns_.get(subColumn.name());
        	if(columnInternal == null )
        	{
        		columnDiff.addColumn(subColumn.name(), subColumn);
        	}
        	else
        	{
            	IColumn subColumnDiff = columnInternal.diff(subColumn);
        		if(subColumnDiff != null)
        		{
            		columnDiff.addColumn(subColumn.name(), subColumnDiff);
        		}
        	}
        }
        if(columnDiff.getSubColumns().size() != 0)
        	return columnDiff;
        else
        	return null;
    }

    public byte[] digest()
    {
    	Set<IColumn> columns = columns_.getSortedColumns();
    	byte[] xorHash = new byte[0];
    	if(name_ == null)
    		return xorHash;
    	xorHash = name_.getBytes();
    	for(IColumn column : columns)
    	{
			xorHash = FBUtilities.xor(xorHash, column.digest());
    	}
    	return xorHash;
    }


    public String toString()
    {
    	StringBuilder sb = new StringBuilder();
    	sb.append(name_);
    	sb.append(":");
        sb.append(isMarkedForDelete());
        sb.append(":");

        Collection<IColumn> columns  = getSubColumns();
        sb.append(columns.size());
        sb.append(":");
        sb.append(size());
        sb.append(":");
        for ( IColumn subColumn : columns )
        {
            sb.append(subColumn.toString());
        }
        sb.append(":");
        return sb.toString();
    }
}

class SuperColumnSerializer implements ICompactSerializer2<IColumn>
{
    public void serialize(IColumn column, DataOutputStream dos) throws IOException
    {
    	SuperColumn superColumn = (SuperColumn)column;
        dos.writeUTF(superColumn.name());
        dos.writeBoolean(superColumn.isMarkedForDelete());

        Collection<IColumn> columns  = column.getSubColumns();
        int size = columns.size();
        dos.writeInt(size);

        /*
         * Add the total size of the columns. This is useful
         * to skip over all the columns in this super column
         * if we are not interested in this super column.
        */
        dos.writeInt(superColumn.getSizeOfAllColumns());
        // dos.writeInt(superColumn.size());

        for ( IColumn subColumn : columns )
        {
            Column.serializer().serialize(subColumn, dos);
        }
    }

    /*
     * Use this method to create a bare bones Super Column. This super column
     * does not have any of the Column information.
    */
    private SuperColumn defreezeSuperColumn(DataInputStream dis) throws IOException
    {
        String name = dis.readUTF();
        boolean delete = dis.readBoolean();
        SuperColumn superColumn = new SuperColumn(name);
        if ( delete )
            superColumn.delete();
        return superColumn;
    }

    public IColumn deserialize(DataInputStream dis) throws IOException
    {
        SuperColumn superColumn = defreezeSuperColumn(dis);
        if ( !superColumn.isMarkedForDelete() )
            fillSuperColumn(superColumn, dis);
        return superColumn;
    }

    public void skip(DataInputStream dis) throws IOException
    {
        defreezeSuperColumn(dis);
        /* read the number of columns stored */
        dis.readInt();
        /* read the size of all columns to skip */
        int size = dis.readInt();
        dis.skip(size);
    }
    
    private void fillSuperColumn(IColumn superColumn, DataInputStream dis) throws IOException
    {
        if ( dis.available() == 0 )
            return;
        
        /* read the number of columns */
        int size = dis.readInt();
        /* read the size of all columns */
        dis.readInt();
        for ( int i = 0; i < size; ++i )
        {
            IColumn subColumn = Column.serializer().deserialize(dis);
            superColumn.addColumn(subColumn.name(), subColumn);
        }
    }

    public IColumn deserialize(DataInputStream dis, IFilter filter) throws IOException
    {
        if ( dis.available() == 0 )
            return null;
        
        IColumn superColumn = defreezeSuperColumn(dis);
        superColumn = filter.filter(superColumn, dis);
        if(superColumn != null)
        {
            if ( !superColumn.isMarkedForDelete() )
                fillSuperColumn(superColumn, dis);
            return superColumn;
        }
        else
        {
            /* read the number of columns stored */
            dis.readInt();
            /* read the size of all columns to skip */
            int size = dis.readInt();
            dis.skip(size);
        	return null;
        }
    }

    /*
     * Deserialize a particular column since the name is in the form of
     * superColumn:column.
    */
    public IColumn deserialize(DataInputStream dis, String name, IFilter filter) throws IOException
    {
        if ( dis.available() == 0 )
            return null;
        
        String[] names = RowMutation.getColumnAndColumnFamily(name);
        if ( names.length == 1 )
        {
            IColumn superColumn = defreezeSuperColumn(dis);
            if(name.equals(superColumn.name()))
            {
                if ( !superColumn.isMarkedForDelete() )
                {
                    /* read the number of columns stored */
                    int size = dis.readInt();
                    /* read the size of all columns */
                    dis.readInt();     
                	IColumn column = null;
                    for ( int i = 0; i < size; ++i )
                    {
                    	column = Column.serializer().deserialize(dis, filter);
                    	if(column != null)
                    	{
                            superColumn.addColumn(column.name(), column);
                    		column = null;
                    		if(filter.isDone())
                    		{
                    			break;
                    		}
                    	}
                    }
                }
                return superColumn;
            }
            else
            {
                /* read the number of columns stored */
                dis.readInt();
                /* read the size of all columns to skip */
                int size = dis.readInt();
                dis.skip(size);
            	return null;
            }
        }

        SuperColumn superColumn = defreezeSuperColumn(dis);
        if ( !superColumn.isMarkedForDelete() )
        {
            int size = dis.readInt();
            /* skip the size of the columns */
            dis.readInt();
            if ( size > 0 )
            {
                for ( int i = 0; i < size; ++i )
                {
                    IColumn subColumn = Column.serializer().deserialize(dis, names[1], filter);
                    if ( subColumn != null )
                    {
                        superColumn.addColumn(subColumn.name(), subColumn);
                        break;
                    }
                }
            }
        }
        return superColumn;
    }
}
