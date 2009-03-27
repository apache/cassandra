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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.IFileWriter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.HashingSchemes;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public final class Column implements IColumn, Serializable
{
	private static Logger logger_ = Logger.getLogger(SuperColumn.class);
    private static ICompactSerializer2<IColumn> serializer_;
	private final static String seperator_ = ":";
    static
    {
        serializer_ = new ColumnSerializer();
    }

    static ICompactSerializer2<IColumn> serializer()
    {
        return serializer_;
    }

    private String name_;
    private byte[] value_ = new byte[0];
    private long timestamp_ = 0;

    private transient AtomicBoolean isMarkedForDelete_;

    /* CTOR for JAXB */
    Column()
    {
    }

    Column(String name)
    {
        name_ = name;
    }

    Column(String name, byte[] value)
    {
        this(name, value, 0);
    }

    Column(String name, byte[] value, long timestamp)
    {
        this(name);
        value_ = value;
        timestamp_ = timestamp;
    }

    public String name()
    {
        return name_;
    }

    public byte[] value()
    {
        return value_;
    }

    public byte[] value(String key)
    {
    	throw new UnsupportedOperationException("This operation is unsupported on simple columns.");
    }

    public Collection<IColumn> getSubColumns()
    {
    	throw new UnsupportedOperationException("This operation is unsupported on simple columns.");
    }

    public IColumn getSubColumn( String columnName )
    {
    	throw new UnsupportedOperationException("This operation is unsupported on simple columns.");
    }

    public int getObjectCount()
    {
    	return 1;
    }

    public long timestamp()
    {
        return timestamp_;
    }

    public long timestamp(String key)
    {
    	throw new UnsupportedOperationException("This operation is unsupported on simple columns.");
    }

    public boolean isMarkedForDelete()
    {
        return (isMarkedForDelete_ != null) ? isMarkedForDelete_.get() : false;
    }

    public int size()
    {
        /*
         * Size of a column is =
         *   size of a name (UtfPrefix + length of the string)
         * + 1 byte to indicate if the column has been deleted
         * + 8 bytes for timestamp
         * + 4 bytes which basically indicates the size of the byte array
         * + entire byte array.
        */

    	/*
    	 * We store the string as UTF-8 encoded, so when we calculate the length, it
    	 * should be converted to UTF-8.
    	 */
        return IColumn.UtfPrefix_ + FBUtilities.getUTF8Length(name_) + DBConstants.boolSize_ + DBConstants.tsSize_ + DBConstants.intSize_ + value_.length;
    }

    /*
     * This returns the size of the column when serialized.
     * @see com.facebook.infrastructure.db.IColumn#serializedSize()
    */
    public int serializedSize()
    {
    	return size();
    }

    public void addColumn(String name, IColumn column)
    {
    	throw new UnsupportedOperationException("This operation is not supported for simple columns.");
    }

    public void delete()
    {
        if ( isMarkedForDelete_ == null )
            isMarkedForDelete_ = new AtomicBoolean(true);
        else
            isMarkedForDelete_.set(true);
    	value_ = new byte[0];
    }

    public void repair(IColumn column)
    {
    	if( timestamp() < column.timestamp() )
    	{
    		value_ = column.value();
    		timestamp_ = column.timestamp();
    	}
    }
    public IColumn diff(IColumn column)
    {
    	IColumn  columnDiff = null;
    	if( timestamp() < column.timestamp() )
    	{
    		columnDiff = new Column(column.name(),column.value(),column.timestamp());
    	}
    	return columnDiff;
    }

    /*
     * Resolve the column by comparing timestamps
     * if a newer vaue is being input
     * take the change else ignore .
     *
     */
    public boolean putColumn(IColumn column)
    {
    	if ( !(column instanceof Column))
    		throw new UnsupportedOperationException("Only Column objects should be put here");
    	if( !name_.equals(column.name()))
    		throw new IllegalArgumentException("The name should match the name of the current column or super column");
    	if(timestamp_ <= column.timestamp())
    	{
            return true;
    	}
        return false;
    }

    public String toString()
    {
    	StringBuilder sb = new StringBuilder();
    	sb.append(name_);
    	sb.append(":");
    	sb.append(isMarkedForDelete());
    	sb.append(":");
    	sb.append(timestamp());
    	sb.append(":");
    	sb.append(value().length);
    	sb.append(":");
    	sb.append(value());
    	sb.append(":");
    	return sb.toString();
    }

    public byte[] digest()
    {
    	StringBuilder stringBuilder = new StringBuilder();
  		stringBuilder.append(name_);
  		stringBuilder.append(seperator_);
  		stringBuilder.append(timestamp_);
    	return stringBuilder.toString().getBytes();
    }
    
    /**
     * This method is basically implemented for Writable interface
     * for M/R. 
     */
    public void readFields(DataInput in) throws IOException
    {
        name_ = in.readUTF();
        boolean delete = in.readBoolean();
        long ts = in.readLong();
        int size = in.readInt();
        byte[] value = new byte[size];
        in.readFully(value);        
        if ( delete )
            delete();
    }
    
    /**
     * This method is basically implemented for Writable interface
     * for M/R. 
     */
    public void write(DataOutput out) throws IOException
    {
        out.writeUTF(name_);
        out.writeBoolean(isMarkedForDelete());
        out.writeLong(timestamp_);
        out.writeInt(value().length);
        out.write(value());
    }

}

class ColumnSerializer implements ICompactSerializer2<IColumn>
{
    public void serialize(IColumn column, DataOutputStream dos) throws IOException
    {
        dos.writeUTF(column.name());
        dos.writeBoolean(column.isMarkedForDelete());
        dos.writeLong(column.timestamp());
        dos.writeInt(column.value().length);
        dos.write(column.value());
    }

    private IColumn defreeze(DataInputStream dis, String name) throws IOException
    {
        IColumn column = null;
        boolean delete = dis.readBoolean();
        long ts = dis.readLong();
        int size = dis.readInt();
        byte[] value = new byte[size];
        dis.readFully(value);
        column = new Column(name, value, ts);
        if ( delete )
            column.delete();
        return column;
    }

    public IColumn deserialize(DataInputStream dis) throws IOException
    {
        String name = dis.readUTF();
        return defreeze(dis, name);
    }

    /**
     * Here we need to get the column and apply the filter.
     */
    public IColumn deserialize(DataInputStream dis, IFilter filter) throws IOException
    {
        if ( dis.available() == 0 )
            return null;
                
        String name = dis.readUTF();
        IColumn column = new Column(name);
        column = filter.filter(column, dis);
        if ( column != null )
        {
            column = defreeze(dis, name);
        }
        else
        {
        	/* Skip a boolean and the timestamp */
        	dis.skip(DBConstants.boolSize_ + DBConstants.tsSize_);
            int size = dis.readInt();
            dis.skip(size);
        }
        return column;
    }

    /**
     * We know the name of the column here so just return it.
     * Filter is pretty much useless in this call and is ignored.
     */
    public IColumn deserialize(DataInputStream dis, String columnName, IFilter filter) throws IOException
    {
        if ( dis.available() == 0 )
            return null;
        IColumn column = null;
        String name = dis.readUTF();
        if ( name.equals(columnName) )
        {
            column = defreeze(dis, name);
            if( filter instanceof IdentityFilter )
            {
            	/*
            	 * If this is being called with identity filter
            	 * since a column name is passed in we know
            	 * that this is a final call 
            	 * Hence if the column is found set the filter to done 
            	 * so that we do not look for the column in further files
            	 */
            	IdentityFilter f = (IdentityFilter)filter;
            	f.setDone();
            }
        }
        else
        {
        	/* Skip a boolean and the timestamp */
        	dis.skip(DBConstants.boolSize_ + DBConstants.tsSize_);
            int size = dis.readInt();
            dis.skip(size);
        }
        return column;
    }

    public void skip(DataInputStream dis) throws IOException
    {
    	/* read the column name */
        dis.readUTF();
        /* boolean indicating if the column is deleted */
        dis.readBoolean();
        /* timestamp associated with the column */
        dis.readLong();
        /* size of the column */
        int size = dis.readInt();
        dis.skip(size);
    }    
}
