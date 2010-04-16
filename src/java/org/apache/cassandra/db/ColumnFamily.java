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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.config.CFMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.FBUtilities;


public class ColumnFamily implements IColumnContainer
{
    /* The column serializer for this Column Family. Create based on config. */
    private static ColumnFamilySerializer serializer_ = new ColumnFamilySerializer();

    private static Logger logger_ = LoggerFactory.getLogger( ColumnFamily.class );
    private static Map<String, String> columnTypes_ = new HashMap<String, String>();
    String type_;

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
        int id = CFMetaData.getId(tableName, cfName);
        return new ColumnFamily(cfName, columnType, comparator, subcolumnComparator, id);
    }

    private String name_;
    private final int id_;

    private transient ICompactSerializer2<IColumn> columnSerializer_;
    AtomicLong markedForDeleteAt = new AtomicLong(Long.MIN_VALUE);
    AtomicInteger localDeletionTime = new AtomicInteger(Integer.MIN_VALUE);
    private ConcurrentSkipListMap<byte[], IColumn> columns_;

    public ColumnFamily(String cfName, String columnType, AbstractType comparator, AbstractType subcolumnComparator, int id)
    {
        name_ = cfName;
        type_ = columnType;
        columnSerializer_ = columnType.equals("Standard") ? Column.serializer() : SuperColumn.serializer(subcolumnComparator);
        columns_ = new ConcurrentSkipListMap<byte[], IColumn>(comparator);
        id_ = id;
    }
    
    /** called during CL recovery when it is determined that a CF name was changed. */
    public void rename(String newName)
    {
        name_ = newName;
    }

    public ColumnFamily cloneMeShallow()
    {
        ColumnFamily cf = new ColumnFamily(name_, type_, getComparator(), getSubComparator(), id_);
        cf.markedForDeleteAt = markedForDeleteAt;
        cf.localDeletionTime = localDeletionTime;
        return cf;
    }

    private AbstractType getSubComparator()
    {
        return (columnSerializer_ instanceof SuperColumnSerializer) ? ((SuperColumnSerializer)columnSerializer_).getComparator() : null;
    }

    public ColumnFamily cloneMe()
    {
        ColumnFamily cf = cloneMeShallow();
        cf.columns_ = columns_.clone();
    	return cf;
    }

    public String name()
    {
        return name_;
    }
    
    public int id()
    {
        return id_;
    }

    /*
     *  We need to go through each column
     *  in the column family and resolve it before adding
    */
    public void addAll(ColumnFamily cf)
    {
        for (IColumn column : cf.getSortedColumns())
        {
            addColumn(column);
        }
        delete(cf);
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
        assert path.columnName != null : path;
        addColumn(path.superColumnName, new Column(path.columnName, value, timestamp));
    }

    public void addTombstone(QueryPath path, byte[] localDeletionTime, long timestamp)
    {
        addColumn(path.superColumnName, new DeletedColumn(path.columnName, localDeletionTime, timestamp));
    }

    public void addColumn(QueryPath path, byte[] value, long timestamp, int timeToLive)
    {
        assert path.columnName != null : path;
        Column column;
        if (timeToLive > 0)
            column = new ExpiringColumn(path.columnName, value, timestamp, timeToLive);
        else
            column = new Column(path.columnName, value, timestamp);
        addColumn(path.superColumnName, column);
    }

    public void deleteColumn(QueryPath path, int localDeletionTime, long timestamp)
    {
        assert path.columnName != null : path;
        addColumn(path.superColumnName, new DeletedColumn(path.columnName, localDeletionTime, timestamp));
    }

    public void addColumn(byte[] superColumnName, Column column)
    {
        IColumn c;
        if (superColumnName == null)
        {
            c = column;
        }
        else
        {
            assert isSuper();
            c = new SuperColumn(superColumnName, getSubComparator());
            c.addColumn(column); // checks subcolumn name
        }
		addColumn(c);
    }

    public void clear()
    {
    	columns_.clear();
    }

    /*
     * If we find an old column that has the same name
     * the ask it to resolve itself else add the new column .
    */
    public void addColumn(IColumn column)
    {
        byte[] name = column.name();
        IColumn oldColumn = columns_.putIfAbsent(name, column);
        if (oldColumn != null)
        {
            if (oldColumn instanceof SuperColumn)
            {
                ((SuperColumn) oldColumn).putColumn(column);
            }
            else
            {
                while (((Column) oldColumn).comparePriority((Column)column) <= 0)
                {
                    if (columns_.replace(name, oldColumn, column))
                        break;
                    oldColumn = columns_.get(name);
                }
            }
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

    @Deprecated // TODO this is a hack to set initial value outside constructor
    public void delete(int localtime, long timestamp)
    {
        localDeletionTime.set(localtime);
        markedForDeleteAt.set(timestamp);
    }

    public void delete(ColumnFamily cf2)
    {
        FBUtilities.atomicSetMax(localDeletionTime, cf2.getLocalDeletionTime()); // do this first so we won't have a column that's "deleted" but has no local deletion time
        FBUtilities.atomicSetMax(markedForDeleteAt, cf2.getMarkedForDeleteAt());
    }

    public boolean isMarkedForDelete()
    {
        return markedForDeleteAt.get() > Long.MIN_VALUE;
    }

    /*
     * This function will calculate the difference between 2 column families.
     * The external input is assumed to be a superset of internal.
     */
    public ColumnFamily diff(ColumnFamily cfComposite)
    {
    	ColumnFamily cfDiff = new ColumnFamily(cfComposite.name(), cfComposite.type_, getComparator(), getSubComparator(), cfComposite.id());
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
        int size = 0;
        for (IColumn column : columns_.values())
        {
            size += column.size();
        }
        return size;
    }

    private transient int hash_ = 0;
    public int hashCode()
    {
        if (hash_ == 0)
        {
            int h = id_ * 7 + name().hashCode();
            hash_ = h;
        }
        return hash_;
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

    public static byte[] digest(ColumnFamily cf)
    {
        MessageDigest digest;
        try
        {
            digest = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new AssertionError(e);
        }
        if (cf != null)
            cf.updateDigest(digest);

        return digest.digest();
    }

    public void updateDigest(MessageDigest digest)
    {
        for (IColumn column : columns_.values())
        {
            column.updateDigest(digest);
        }
    }

    public long getMarkedForDeleteAt()
    {
        return markedForDeleteAt.get();
    }

    public int getLocalDeletionTime()
    {
        return localDeletionTime.get();
    }

    public String type()
    {
        return type_;
    }

    String getComparatorName()
    {
        return getComparator().getClass().getCanonicalName();
    }

    String getSubComparatorName()
    {
        AbstractType subcolumnComparator = getSubComparator();
        return subcolumnComparator == null ? "" : subcolumnComparator.getClass().getCanonicalName();
    }

    public static AbstractType getComparatorFor(String table, String columnFamilyName, byte[] superColumnName)
    {
        return superColumnName == null
               ? DatabaseDescriptor.getComparator(table, columnFamilyName)
               : DatabaseDescriptor.getSubComparator(table, columnFamilyName);
    }

    public static ColumnFamily diff(ColumnFamily cf1, ColumnFamily cf2)
    {
        if (cf1 == null)
            return cf2;
        return cf1.diff(cf2);
    }

    public static ColumnFamily resolve(ColumnFamily cf1, ColumnFamily cf2)
    {
        if (cf1 == null)
            return cf2;
        cf1.resolve(cf2);
        return cf1;
    }

    public void resolve(ColumnFamily cf)
    {
        // Row _does_ allow null CF objects :(  seems a necessary evil for efficiency
        if (cf == null)
            return;
        addAll(cf);
    }
}
