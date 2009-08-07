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
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.io.BufferedRandomAccessFile;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.marshal.LongType;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public final class ColumnFamily
{
    /* The column serializer for this Column Family. Create based on config. */
    private static ColumnFamilySerializer serializer_ = new ColumnFamilySerializer();
    public static final short utfPrefix_ = 2;   

    private static Logger logger_ = Logger.getLogger( ColumnFamily.class );
    private static Map<String, String> columnTypes_ = new HashMap<String, String>();
    private String type_;
    private String table_;

    static
    {
        /* TODO: These are the various column types. Hard coded for now. */
        columnTypes_.put("Standard", "Standard");
        columnTypes_.put("Super", "Super");
    }

    public static ColumnFamilySerializer serializer()
    {
        return serializer_;
    }

    /*
     * This method returns the serializer whose methods are
     * preprocessed by a dynamic proxy.
    */
    public static ICompactSerializer2<ColumnFamily> serializerWithIndexes()
    {
        return (ICompactSerializer2<ColumnFamily>)Proxy.newProxyInstance( ColumnFamily.class.getClassLoader(), new Class[]{ICompactSerializer2.class}, new CompactSerializerInvocationHandler<ColumnFamily>(serializer_) );
    }

    public static String getColumnType(String key)
    {
    	if ( key == null )
    		return columnTypes_.get("Standard");
    	return columnTypes_.get(key);
    }

    public static ColumnFamily create(String tableName, String cfName)
    {
        String columnType = DatabaseDescriptor.getColumnFamilyType(tableName, cfName);
        AbstractType comparator = DatabaseDescriptor.getComparator(tableName, cfName);
        AbstractType subcolumnComparator = DatabaseDescriptor.getSubComparator(tableName, cfName);
        return new ColumnFamily(cfName, columnType, comparator, subcolumnComparator);
    }

    private String name_;

    private transient ICompactSerializer2<IColumn> columnSerializer_;
    private long markedForDeleteAt = Long.MIN_VALUE;
    private int localDeletionTime = Integer.MIN_VALUE;
    private AtomicInteger size_ = new AtomicInteger(0);
    private ConcurrentSkipListMap<byte[], IColumn> columns_;

    public ColumnFamily(String cfName, String columnType, AbstractType comparator, AbstractType subcolumnComparator)
    {
        name_ = cfName;
        type_ = columnType;
        columnSerializer_ = columnType.equals("Standard") ? Column.serializer() : SuperColumn.serializer(subcolumnComparator);
        columns_ = new ConcurrentSkipListMap<byte[], IColumn>(comparator);
    }

    public ColumnFamily cloneMeShallow()
    {
        ColumnFamily cf = new ColumnFamily(name_, type_, getComparator(), getSubComparator());
        cf.markedForDeleteAt = markedForDeleteAt;
        cf.localDeletionTime = localDeletionTime;
        return cf;
    }

    private AbstractType getSubComparator()
    {
        return (columnSerializer_ instanceof SuperColumnSerializer) ? ((SuperColumnSerializer)columnSerializer_).getComparator() : null;
    }

    ColumnFamily cloneMe()
    {
        ColumnFamily cf = cloneMeShallow();
        cf.columns_ = columns_.clone();
    	return cf;
    }

    public String name()
    {
        return name_;
    }

    /*
     *  We need to go through each column
     *  in the column family and resolve it before adding
    */
    void addColumns(ColumnFamily cf)
    {
        for (IColumn column : cf.getSortedColumns())
        {
            addColumn(column);
        }
    }

    public ICompactSerializer2<IColumn> getColumnSerializer()
    {
    	return columnSerializer_;
    }

    int getColumnCount()
    {
    	int count = 0;
        if(!isSuper())
        {
            count = columns_.size();
        }
        else
        {
            for(IColumn column: columns_.values())
            {
                count += column.getObjectCount();
            }
        }
    	return count;
    }

    public boolean isSuper()
    {
        return type_.equals("Super");
    }

    public void addColumn(QueryPath path, byte[] value, long timestamp)
    {
        addColumn(path, value, timestamp, false);
    }

    /** In most places the CF must be part of a QueryPath but here it is ignored. */
    public void addColumn(QueryPath path, byte[] value, long timestamp, boolean deleted)
	{
        assert path.columnName != null : path;
		IColumn column;
        if (path.superColumnName == null)
        {
            try
            {
                getComparator().validate(path.columnName);
            }
            catch (Exception e)
            {
                throw new MarshalException("Invalid column name in " + path.columnFamilyName + " for " + getComparator().getClass().getName());
            }
            column = new Column(path.columnName, value, timestamp, deleted);
        }
        else
        {
            assert isSuper();
            try
            {
                getComparator().validate(path.superColumnName);
            }
            catch (Exception e)
            {
                throw new MarshalException("Invalid supercolumn name in " + path.columnFamilyName + " for " + getComparator().getClass().getName());
            }
            column = new SuperColumn(path.superColumnName, getSubComparator());
            column.addColumn(new Column(path.columnName, value, timestamp, deleted)); // checks subcolumn name
        }
		addColumn(column);
    }

    public void clear()
    {
    	columns_.clear();
    	size_.set(0);
    }

    /*
     * If we find an old column that has the same name
     * the ask it to resolve itself else add the new column .
    */
    public void addColumn(IColumn column)
    {
        byte[] name = column.name();
        IColumn oldColumn = columns_.get(name);
        if (oldColumn != null)
        {
            if (oldColumn instanceof SuperColumn)
            {
                int oldSize = oldColumn.size();
                ((SuperColumn) oldColumn).putColumn(column);
                size_.addAndGet(oldColumn.size() - oldSize);
            }
            else
            {
                if (((Column)oldColumn).comparePriority((Column)column) <= 0)
                {
                    columns_.put(name, column);
                    size_.addAndGet(column.size());
                }
            }
        }
        else
        {
            size_.addAndGet(column.size());
            columns_.put(name, column);
        }
    }

    public IColumn getColumn(byte[] name)
    {
        return columns_.get(name);
    }

    public SortedSet<byte[]> getColumnNames()
    {
        return columns_.keySet();
    }

    public Collection<IColumn> getSortedColumns()
    {
        return columns_.values();
    }

    public Map<byte[], IColumn> getColumnsMap()
    {
        return columns_;
    }

    public void remove(byte[] columnName)
    {
    	columns_.remove(columnName);
    }

    public void delete(int localtime, long timestamp)
    {
        localDeletionTime = localtime;
        markedForDeleteAt = timestamp;
    }

    public void delete(ColumnFamily cf2)
    {
        delete(Math.max(getLocalDeletionTime(), cf2.getLocalDeletionTime()),
               Math.max(getMarkedForDeleteAt(), cf2.getMarkedForDeleteAt()));
    }

    public boolean isMarkedForDelete()
    {
        return markedForDeleteAt > Long.MIN_VALUE;
    }

    /*
     * This function will calculate the difference between 2 column families.
     * The external input is assumed to be a superset of internal.
     */
    ColumnFamily diff(ColumnFamily cfComposite)
    {
    	ColumnFamily cfDiff = new ColumnFamily(cfComposite.name(), cfComposite.type_, getComparator(), getSubComparator());
        if (cfComposite.getMarkedForDeleteAt() > getMarkedForDeleteAt())
        {
            cfDiff.delete(cfComposite.getLocalDeletionTime(), cfComposite.getMarkedForDeleteAt());
        }

        // (don't need to worry about cfNew containing IColumns that are shadowed by
        // the delete tombstone, since cfNew was generated by CF.resolve, which
        // takes care of those for us.)
        Map<byte[], IColumn> columns = cfComposite.getColumnsMap();
        Set<byte[]> cNames = columns.keySet();
        for (byte[] cName : cNames)
        {
            IColumn columnInternal = columns_.get(cName);
            IColumn columnExternal = columns.get(cName);
            if (columnInternal == null)
            {
                cfDiff.addColumn(columnExternal);
            }
            else
            {
                IColumn columnDiff = columnInternal.diff(columnExternal);
                if (columnDiff != null)
                {
                    cfDiff.addColumn(columnDiff);
                }
            }
        }

        if (!cfDiff.getColumnsMap().isEmpty() || cfDiff.isMarkedForDelete())
        	return cfDiff;
        else
        	return null;
    }

    public AbstractType getComparator()
    {
        return (AbstractType)columns_.comparator();
    }

    int size()
    {
        if (size_.get() == 0)
        {
            for (IColumn column : columns_.values())
            {
                size_.addAndGet(column.size());
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
        sb.append("ColumnFamily(");
    	sb.append(name_);

        if (isMarkedForDelete()) {
            sb.append(" -delete at " + getMarkedForDeleteAt() + "-");
        }

    	sb.append(" [");
        sb.append(getComparator().getColumnsString(getSortedColumns()));
        sb.append("])");

    	return sb.toString();
    }

    public byte[] digest()
    {
        byte[] xorHash = ArrayUtils.EMPTY_BYTE_ARRAY;
        for (IColumn column : columns_.values())
        {
            if (xorHash.length == 0)
            {
                xorHash = column.digest();
            }
            else
            {
                xorHash = FBUtilities.xor(xorHash, column.digest());
            }
        }
        return xorHash;
    }

    public long getMarkedForDeleteAt()
    {
        return markedForDeleteAt;
    }

    public int getLocalDeletionTime()
    {
        return localDeletionTime;
    }

    public String type()
    {
        return type_;
    }

    private String getComparatorName()
    {
        return getComparator().getClass().getCanonicalName();
    }

    private String getSubComparatorName()
    {
        AbstractType subcolumnComparator = getSubComparator();
        return subcolumnComparator == null ? "" : subcolumnComparator.getClass().getCanonicalName();
    }

    public int serializedSize()
    {
        int subtotal = 4 * IColumn.UtfPrefix_ + name_.length() + type_.length() +  getComparatorName().length() + getSubComparatorName().length() + 4 + 8 + 4;
        for (IColumn column : columns_.values())
        {
            subtotal += column.serializedSize();
        }
        return subtotal;
    }

    /** merge all columnFamilies into a single instance, with only the newest versions of columns preserved. */
    static ColumnFamily resolve(List<ColumnFamily> columnFamilies)
    {
        int size = columnFamilies.size();
        if (size == 0)
            return null;

        // start from nothing so that we don't include potential deleted columns from the first instance
        ColumnFamily cf0 = columnFamilies.get(0);
        ColumnFamily cf = cf0.cloneMeShallow();

        // merge
        for (ColumnFamily cf2 : columnFamilies)
        {
            assert cf.name().equals(cf2.name());
            cf.addColumns(cf2);
            cf.delete(cf2);
        }
        return cf;
    }

    public static class ColumnFamilySerializer implements ICompactSerializer2<ColumnFamily>
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
        public void serialize(ColumnFamily columnFamily, DataOutput dos) throws IOException
        {
            Collection<IColumn> columns = columnFamily.getSortedColumns();

            dos.writeUTF(columnFamily.name());
            dos.writeUTF(columnFamily.type_);
            dos.writeUTF(columnFamily.getComparatorName());
            dos.writeUTF(columnFamily.getSubComparatorName());
            dos.writeInt(columnFamily.localDeletionTime);
            dos.writeLong(columnFamily.markedForDeleteAt);

            dos.writeInt(columns.size());
            for ( IColumn column : columns )
            {
                columnFamily.getColumnSerializer().serialize(column, dos);
            }
        }

        public ColumnFamily deserialize(DataInput dis) throws IOException
        {
            ColumnFamily cf = deserializeEmpty(dis);
            int size = dis.readInt();
            IColumn column;
            for (int i = 0; i < size; ++i)
            {
                column = cf.getColumnSerializer().deserialize(dis);
                cf.addColumn(column);
            }
            return cf;
        }

        private AbstractType readComparator(DataInput dis) throws IOException
        {
            String className = dis.readUTF();
            if (className.equals(""))
            {
                return null;
            }

            try
            {
                return (AbstractType)Class.forName(className).getConstructor().newInstance();
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException("Unable to load comparator class '" + className + "'.  probably this means you have obsolete sstables lying around", e);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        public ColumnFamily deserializeEmpty(DataInput input) throws IOException
        {
            ColumnFamily cf = new ColumnFamily(input.readUTF(),
                                               input.readUTF(),
                                               readComparator(input),
                                               readComparator(input));
            cf.delete(input.readInt(), input.readLong());
            return cf;
        }
    }
}

