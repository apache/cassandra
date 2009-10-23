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

import java.io.*;
import java.util.Collection;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentSkipListMap;
import java.security.MessageDigest;

import org.apache.log4j.Logger;

import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.db.marshal.AbstractType;


public final class SuperColumn implements IColumn, IColumnContainer
{
	private static Logger logger_ = Logger.getLogger(SuperColumn.class);

    public static SuperColumnSerializer serializer(AbstractType comparator)
    {
        return new SuperColumnSerializer(comparator);
    }

    private byte[] name_;
    private ConcurrentSkipListMap<byte[], IColumn> columns_;
    private int localDeletionTime = Integer.MIN_VALUE;
	private long markedForDeleteAt = Long.MIN_VALUE;
    private AtomicInteger size_ = new AtomicInteger(0);

    SuperColumn(byte[] name, AbstractType comparator)
    {
        assert name != null;
        assert name.length <= IColumn.MAX_NAME_LENGTH;
    	name_ = name;
        columns_ = new ConcurrentSkipListMap<byte[], IColumn>(comparator);
    }

    public AbstractType getComparator()
    {
        return (AbstractType)columns_.comparator();
    }

    public SuperColumn cloneMeShallow()
    {
        SuperColumn sc = new SuperColumn(name_, getComparator());
        sc.markForDeleteAt(localDeletionTime, markedForDeleteAt);
        return sc;
    }

	public boolean isMarkedForDelete()
	{
		return markedForDeleteAt > Long.MIN_VALUE;
	}

    public byte[] name()
    {
    	return name_;
    }

    public Collection<IColumn> getSubColumns()
    {
    	return columns_.values();
    }

    public IColumn getSubColumn(byte[] columnName)
    {
        IColumn column = columns_.get(columnName);
        assert column == null || column instanceof Column;
        return column;
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
    	 * We need to keep the way we are calculating the column size in sync with the
    	 * way we are calculating the size for the column family serializer.
    	 */
    	return IColumn.UtfPrefix_ + name_.length + DBConstants.intSize_ + DBConstants.longSize_ + DBConstants.intSize_ + getSizeOfAllColumns();
    }

    /**
     * This calculates the exact size of the sub columns on the fly
     */
    int getSizeOfAllColumns()
    {
        int size = 0;
        for (IColumn subColumn : getSubColumns())
        {
            size += subColumn.serializedSize();
        }
        return size;
    }

    public void remove(byte[] columnName)
    {
    	columns_.remove(columnName);
    }

    public long timestamp()
    {
    	throw new UnsupportedOperationException("This operation is not supported for Super Columns.");
    }

    public long timestamp(byte[] columnName)
    {
    	IColumn column = columns_.get(columnName);
    	assert column instanceof Column;
    	if ( column != null )
    		return column.timestamp();
    	throw new IllegalArgumentException("Timestamp was requested for a column that does not exist.");
    }

    public long mostRecentChangeAt()
    {
        long max = Long.MIN_VALUE;
        for (IColumn column : columns_.values())
        {
            if (column.mostRecentChangeAt() > max)
            {
                max = column.mostRecentChangeAt();
            }
        }
        return max;
    }

    public byte[] value()
    {
    	throw new UnsupportedOperationException("This operation is not supported for Super Columns.");
    }

    public byte[] value(byte[] columnName)
    {
    	IColumn column = columns_.get(columnName);
    	if ( column != null )
    		return column.value();
    	throw new IllegalArgumentException("Value was requested for a column that does not exist.");
    }

    public void addColumn(IColumn column)
    {
    	if (!(column instanceof Column))
    		throw new UnsupportedOperationException("A super column can only contain simple columns.");
    	IColumn oldColumn = columns_.get(column.name());
    	if ( oldColumn == null )
        {
    		columns_.put(column.name(), column);
            size_.addAndGet(column.size());
        }
    	else
    	{
    		if (((Column)oldColumn).comparePriority((Column)column) <= 0)
            {
    			columns_.put(column.name(), column);
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
    public void putColumn(IColumn column)
    {
        assert column instanceof SuperColumn;

        for (IColumn subColumn : column.getSubColumns())
        {
        	addColumn(subColumn);
        }
        if (column.getMarkedForDeleteAt() > markedForDeleteAt)
        {
            markForDeleteAt(column.getLocalDeletionTime(),  column.getMarkedForDeleteAt());
        }
    }

    public int getObjectCount()
    {
    	return 1 + columns_.size();
    }

    public long getMarkedForDeleteAt() {
        return markedForDeleteAt;
    }

    int getColumnCount()
    {
    	return columns_.size();
    }

    public IColumn diff(IColumn columnNew)
    {
    	IColumn columnDiff = new SuperColumn(columnNew.name(), ((SuperColumn)columnNew).getComparator());
        if (columnNew.getMarkedForDeleteAt() > getMarkedForDeleteAt())
        {
            ((SuperColumn)columnDiff).markForDeleteAt(columnNew.getLocalDeletionTime(), columnNew.getMarkedForDeleteAt());
        }

        // (don't need to worry about columnNew containing subColumns that are shadowed by
        // the delete tombstone, since columnNew was generated by CF.resolve, which
        // takes care of those for us.)
        for (IColumn subColumn : columnNew.getSubColumns())
        {
        	IColumn columnInternal = columns_.get(subColumn.name());
        	if(columnInternal == null )
        	{
        		columnDiff.addColumn(subColumn);
        	}
        	else
        	{
            	IColumn subColumnDiff = columnInternal.diff(subColumn);
        		if(subColumnDiff != null)
        		{
            		columnDiff.addColumn(subColumnDiff);
        		}
        	}
        }

        if (!columnDiff.getSubColumns().isEmpty() || columnNew.isMarkedForDelete())
        	return columnDiff;
        else
        	return null;
    }

    public void updateDigest(MessageDigest digest)
    {
        assert name_ != null;
        digest.update(name_);
        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            buffer.writeLong(markedForDeleteAt);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        digest.update(buffer.getData(), 0, buffer.getLength());
        for (IColumn column : columns_.values())
        {
            column.updateDigest(digest);
        }
    }

    public String getString(AbstractType comparator)
    {
    	StringBuilder sb = new StringBuilder();
        sb.append("SuperColumn(");
    	sb.append(comparator.getString(name_));

        if (isMarkedForDelete()) {
            sb.append(" -delete at ").append(getMarkedForDeleteAt()).append("-");
        }

        sb.append(" [");
        sb.append(getComparator().getColumnsString(columns_.values()));
        sb.append("])");

        return sb.toString();
    }

    public int getLocalDeletionTime()
    {
        return localDeletionTime;
    }

    public void markForDeleteAt(int localDeleteTime, long timestamp)
    {
        this.localDeletionTime = localDeleteTime;
        this.markedForDeleteAt = timestamp;
    }
}

class SuperColumnSerializer implements ICompactSerializer2<IColumn>
{
    private AbstractType comparator;

    public SuperColumnSerializer(AbstractType comparator)
    {
        this.comparator = comparator;
    }

    public AbstractType getComparator()
    {
        return comparator;
    }

    public void serialize(IColumn column, DataOutput dos)
    {
    	SuperColumn superColumn = (SuperColumn)column;
        ColumnSerializer.writeName(column.name(), dos);
        try
        {
            dos.writeInt(superColumn.getLocalDeletionTime());
            dos.writeLong(superColumn.getMarkedForDeleteAt());

            Collection<IColumn> columns = column.getSubColumns();
            dos.writeInt(columns.size());
            for (IColumn subColumn : columns)
            {
                Column.serializer().serialize(subColumn, dos);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public IColumn deserialize(DataInput dis) throws IOException
    {
        byte[] name = ColumnSerializer.readName(dis);
        SuperColumn superColumn = new SuperColumn(name, comparator);
        int localDeleteTime = dis.readInt();
        if (localDeleteTime != Integer.MIN_VALUE && localDeleteTime <= 0)
        {
            throw new IOException("Invalid localDeleteTime read: " + localDeleteTime);
        }
        superColumn.markForDeleteAt(localDeleteTime, dis.readLong());

        /* read the number of columns */
        int size = dis.readInt();
        for ( int i = 0; i < size; ++i )
        {
            IColumn subColumn = Column.serializer().deserialize(dis);
            superColumn.addColumn(subColumn);
        }
        return superColumn;
    }
}
