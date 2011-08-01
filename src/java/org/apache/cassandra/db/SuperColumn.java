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
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.util.ColumnSortedMap;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.cliffc.high_scale_lib.NonBlockingHashMap;


public class SuperColumn implements IColumn, IColumnContainer
{
    private static NonBlockingHashMap<Comparator, SuperColumnSerializer> serializers = new NonBlockingHashMap<Comparator, SuperColumnSerializer>();
    public static SuperColumnSerializer serializer(AbstractType comparator)
    {
        SuperColumnSerializer serializer = serializers.get(comparator);
        if (serializer == null)
        {
            serializer = new SuperColumnSerializer(comparator);
            serializers.put(comparator, serializer);
        }
        return serializer;
    }

    private ByteBuffer name_;
    private ConcurrentSkipListMap<ByteBuffer, IColumn> columns_;
    private AtomicInteger localDeletionTime = new AtomicInteger(Integer.MIN_VALUE);
    private AtomicLong markedForDeleteAt = new AtomicLong(Long.MIN_VALUE);

    public SuperColumn(ByteBuffer name, AbstractType comparator)
    {
        this(name, new ConcurrentSkipListMap<ByteBuffer, IColumn>(comparator));
    }

    SuperColumn(ByteBuffer name, ConcurrentSkipListMap<ByteBuffer, IColumn> columns)
    {
        assert name != null;
        assert name.remaining() <= IColumn.MAX_NAME_LENGTH;
        name_ = name;
        columns_ = columns;
    }

    public AbstractType getComparator()
    {
        return (AbstractType)columns_.comparator();
    }

    public SuperColumn cloneMeShallow()
    {
        SuperColumn sc = new SuperColumn(name_, getComparator());
        sc.markForDeleteAt(localDeletionTime.get(), markedForDeleteAt.get());
        return sc;
    }

    public IColumn cloneMe()
    {
        SuperColumn sc = new SuperColumn(name_, new ConcurrentSkipListMap<ByteBuffer, IColumn>(columns_));
        sc.markForDeleteAt(localDeletionTime.get(), markedForDeleteAt.get());
        return sc;
    }

	public boolean isMarkedForDelete()
	{
        return markedForDeleteAt.get() > Long.MIN_VALUE;
	}

    public ByteBuffer name()
    {
    	return name_;
    }

    public Collection<IColumn> getSubColumns()
    {
    	return columns_.values();
    }

    public Collection<IColumn> getSortedColumns()
    {
        return getSubColumns();
    }

    public IColumn getSubColumn(ByteBuffer columnName)
    {
        IColumn column = columns_.get(columnName);
        assert column == null || column instanceof Column;
        return column;
    }

    /**
     * This calculates the exact size of the sub columns on the fly
     */
    public int size()
    {
        int size = 0;
        for (IColumn subColumn : getSubColumns())
        {
            size += subColumn.serializedSize();
        }
        return size;
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
      return DBConstants.shortSize_ + name_.remaining() + DBConstants.intSize_ + DBConstants.longSize_ + DBConstants.intSize_ + size();
    }

    public void remove(ByteBuffer columnName)
    {
    	columns_.remove(columnName);
    }

    public long timestamp()
    {
    	throw new UnsupportedOperationException("This operation is not supported for Super Columns.");
    }

    public long mostRecentLiveChangeAt()
    {
        long max = Long.MIN_VALUE;
        for (IColumn column : columns_.values())
        {
            if (!column.isMarkedForDelete() && column.timestamp() > max)
            {
                max = column.timestamp();
            }
        }
        return max;
    }

    public ByteBuffer value()
    {
    	throw new UnsupportedOperationException("This operation is not supported for Super Columns.");
    }

    public void addColumn(IColumn column)
    {
        assert column instanceof Column : "A super column can only contain simple columns";

        ByteBuffer name = column.name();
        IColumn oldColumn;
        while ((oldColumn = columns_.putIfAbsent(name, column)) != null)
        {
            IColumn reconciledColumn = column.reconcile(oldColumn);
            if (columns_.replace(name, oldColumn, reconciledColumn))
                break;

            // We failed to replace column due to a concurrent update or a concurrent removal. Keep trying.
            // (Currently, concurrent removal should not happen (only updates), but let us support that anyway.)
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
        FBUtilities.atomicSetMax(localDeletionTime, column.getLocalDeletionTime()); // do this first so we won't have a column that's "deleted" but has no local deletion time
        FBUtilities.atomicSetMax(markedForDeleteAt, column.getMarkedForDeleteAt());
    }

    public long getMarkedForDeleteAt()
    {
        return markedForDeleteAt.get();
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
        digest.update(name_.duplicate());
        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            buffer.writeLong(markedForDeleteAt.get());
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

    public boolean isLive()
    {
        return mostRecentLiveChangeAt() > markedForDeleteAt.get();
    }

    public int getLocalDeletionTime()
    {
        return localDeletionTime.get();
    }

    @Deprecated // TODO this is a hack to set initial value outside constructor
    public void markForDeleteAt(int localDeleteTime, long timestamp)
    {
        this.localDeletionTime.set(localDeleteTime);
        this.markedForDeleteAt.set(timestamp);
    }

    public IColumn shallowCopy()
    {
        SuperColumn sc = new SuperColumn(ByteBufferUtil.clone(name_), this.getComparator());
        sc.localDeletionTime = localDeletionTime;
        sc.markedForDeleteAt = markedForDeleteAt;
        return sc;
    }
    
    public IColumn localCopy(ColumnFamilyStore cfs)
    {
        // we don't try to intern supercolumn names, because if we're using Cassandra correctly it's almost
        // certainly just going to pollute our interning map with unique, dynamic values
        SuperColumn sc = new SuperColumn(ByteBufferUtil.clone(name_), this.getComparator());
        sc.localDeletionTime = localDeletionTime;
        sc.markedForDeleteAt = markedForDeleteAt;
        
        for(Map.Entry<ByteBuffer, IColumn> c : columns_.entrySet())
        {
            sc.addColumn(c.getValue().localCopy(cfs));
        }

        return sc;
    }

    public IColumn reconcile(IColumn c)
    {
        throw new UnsupportedOperationException("This operation is unsupported on super columns.");
    }

    public int serializationFlags()
    {
        throw new UnsupportedOperationException("Super columns don't have a serialization mask");
    }

    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        metadata.comparator.validate(name());
        for (IColumn column : getSubColumns())
        {
            column.validateFields(metadata);
        }
    }

    public void retainAll(SuperColumn sc)
    {
        Iterator<IColumn> iter = columns_.values().iterator();
        Iterator<IColumn> toRetain = sc.columns_.values().iterator();
        IColumn current = iter.hasNext() ? iter.next() : null;
        IColumn retain = toRetain.hasNext() ? toRetain.next() : null;
        AbstractType comparator = getComparator();
        while (current != null && retain != null)
        {
            int c = comparator.compare(current.name(), retain.name());
            if (c == 0)
            {
                current = iter.hasNext() ? iter.next() : null;
                retain = toRetain.hasNext() ? toRetain.next() : null;
            }
            else if (c < 0)
            {
                iter.remove();
                current = iter.hasNext() ? iter.next() : null;
            }
            else // c > 0
            {
                retain = toRetain.hasNext() ? toRetain.next() : null;
            }
        }
        while (current != null)
        {
            iter.remove();
            current = iter.hasNext() ? iter.next() : null;
        }
    }
}

class SuperColumnSerializer implements IColumnSerializer
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
        ByteBufferUtil.writeWithShortLength(column.name(), dos);
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
        return deserialize(dis, null, false);
    }

    public IColumn deserialize(DataInput dis, ColumnFamilyStore interner) throws IOException
    {
        return deserialize(dis, interner, false);
    }

    public IColumn deserialize(DataInput dis, ColumnFamilyStore interner, boolean fromRemote) throws IOException
    {
        return deserialize(dis, interner, fromRemote, (int)(System.currentTimeMillis() / 1000));
    }

    public IColumn deserialize(DataInput dis, ColumnFamilyStore interner, boolean fromRemote, int expireBefore) throws IOException
    {
        ByteBuffer name = ByteBufferUtil.readWithShortLength(dis);
        int localDeleteTime = dis.readInt();
        if (localDeleteTime != Integer.MIN_VALUE && localDeleteTime <= 0)
        {
            throw new IOException("Invalid localDeleteTime read: " + localDeleteTime);
        }
        long markedForDeleteAt = dis.readLong();

        /* read the number of columns */
        int size = dis.readInt();
        ColumnSerializer serializer = Column.serializer();
        ColumnSortedMap preSortedMap = new ColumnSortedMap(comparator, serializer, dis, interner, size, fromRemote, expireBefore);
        SuperColumn superColumn = new SuperColumn(name, new ConcurrentSkipListMap<ByteBuffer,IColumn>(preSortedMap));
        if (localDeleteTime != Integer.MIN_VALUE && localDeleteTime <= 0)
        {
            throw new IOException("Invalid localDeleteTime read: " + localDeleteTime);
        }
        superColumn.markForDeleteAt(localDeleteTime, markedForDeleteAt);
        return superColumn;
    }
}
