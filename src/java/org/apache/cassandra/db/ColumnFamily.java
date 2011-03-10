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

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractCommutativeType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.io.util.IIterableColumns;
import org.apache.cassandra.utils.FBUtilities;

public class ColumnFamily implements IColumnContainer, IIterableColumns
{
    private static Logger logger = LoggerFactory.getLogger(ColumnFamily.class);

    /* The column serializer for this Column Family. Create based on config. */
    private static ColumnFamilySerializer serializer = new ColumnFamilySerializer();

    public static ColumnFamilySerializer serializer()
    {
        return serializer;
    }

    public static ColumnFamily create(Integer cfId)
    {
        return create(DatabaseDescriptor.getCFMetaData(cfId));
    }

    public static ColumnFamily create(String tableName, String cfName)
    {
        return create(DatabaseDescriptor.getCFMetaData(tableName, cfName));
    }

    public static ColumnFamily create(CFMetaData cfm)
    {
        assert cfm != null;
        return new ColumnFamily(cfm.cfType, cfm.comparator, cfm.subcolumnComparator, cfm.cfId);
    }

    private final Integer cfid;
    private final ColumnFamilyType type;

    private transient IColumnSerializer columnSerializer;
    final AtomicLong markedForDeleteAt = new AtomicLong(Long.MIN_VALUE);
    final AtomicInteger localDeletionTime = new AtomicInteger(Integer.MIN_VALUE);
    private ConcurrentSkipListMap<ByteBuffer, IColumn> columns;
    
    public ColumnFamily(ColumnFamilyType type, AbstractType comparator, AbstractType subcolumnComparator, Integer cfid)
    {
        this.type = type;
        columnSerializer = type == ColumnFamilyType.Standard ? Column.serializer() : SuperColumn.serializer(subcolumnComparator);
        columns = new ConcurrentSkipListMap<ByteBuffer, IColumn>(comparator);
        this.cfid = cfid;
     }
    
    public ColumnFamily cloneMeShallow()
    {
        ColumnFamily cf = new ColumnFamily(type, getComparator(), getSubComparator(), cfid);
        cf.markedForDeleteAt.set(markedForDeleteAt.get());
        cf.localDeletionTime.set(localDeletionTime.get());
        return cf;
    }

    public AbstractType getSubComparator()
    {
        return (columnSerializer instanceof SuperColumnSerializer) ? ((SuperColumnSerializer)columnSerializer).getComparator() : null;
    }

    public ColumnFamilyType getColumnFamilyType()
    {
        return type;
    }

    public ColumnFamily cloneMe()
    {
        ColumnFamily cf = cloneMeShallow();
        cf.columns = columns.clone();
        return cf;
    }

    public Integer id()
    {
        return cfid;
    }

    /**
     * @return The CFMetaData for this row, or null if the column family was dropped.
     */
    public CFMetaData metadata()
    {
        return DatabaseDescriptor.getCFMetaData(cfid);
    }

    /*
     *  We need to go through each column
     *  in the column family and resolve it before adding
    */
    public void addAll(ColumnFamily cf)
    {
        for (IColumn column : cf.getSortedColumns())
            addColumn(column);
        delete(cf);
    }

    public IColumnSerializer getColumnSerializer()
    {
        return columnSerializer;
    }

    public int getColumnCount()
    {
        return columns.size();
    }

    public boolean isSuper()
    {
        return type == ColumnFamilyType.Super;
    }

    public void addColumn(QueryPath path, ByteBuffer value, long timestamp)
    {
        addColumn(path, value, timestamp, 0);
    }

    public void addColumn(QueryPath path, ByteBuffer value, long timestamp, int timeToLive)
    {
        assert path.columnName != null : path;
        Column column;
        AbstractType defaultValidator = metadata().getDefaultValidator();
        if (!defaultValidator.isCommutative())
        {
            if (timeToLive > 0)
                column = new ExpiringColumn(path.columnName, value, timestamp, timeToLive);
            else
                column = new Column(path.columnName, value, timestamp);
        }
        else
        {
            column = ((AbstractCommutativeType)defaultValidator).createColumn(path.columnName, value, timestamp);
        }
        addColumn(path.superColumnName, column);
    }

    public void addTombstone(QueryPath path, ByteBuffer localDeletionTime, long timestamp)
    {
        assert path.columnName != null : path;
        addColumn(path.superColumnName, new DeletedColumn(path.columnName, localDeletionTime, timestamp));
    }

    public void addTombstone(QueryPath path, int localDeletionTime, long timestamp)
    {
        assert path.columnName != null : path;
        addColumn(path.superColumnName, new DeletedColumn(path.columnName, localDeletionTime, timestamp));
    }

    public void addTombstone(ByteBuffer name, int localDeletionTime, long timestamp)
    {
        addColumn(null, new DeletedColumn(name, localDeletionTime, timestamp));
    }

    public void addColumn(ByteBuffer superColumnName, Column column)
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
        columns.clear();
    }

    /*
     * If we find an old column that has the same name
     * the ask it to resolve itself else add the new column .
    */
    public void addColumn(IColumn column)
    {
        ByteBuffer name = column.name();
        IColumn oldColumn;
        while ((oldColumn = columns.putIfAbsent(name, column)) != null)
        {
            if (oldColumn instanceof SuperColumn)
            {
                ((SuperColumn) oldColumn).putColumn(column);
                break;  // Delegated to SuperColumn
            }
            else
            {
                // calculate reconciled col from old (existing) col and new col
                IColumn reconciledColumn = column.reconcile(oldColumn);
                if (columns.replace(name, oldColumn, reconciledColumn))
                    break;

                // We failed to replace column due to a concurrent update or a concurrent removal. Keep trying.
                // (Currently, concurrent removal should not happen (only updates), but let us support that anyway.)
            }
        }
    }

    public IColumn getColumn(ByteBuffer name)
    {
        return columns.get(name);
    }

    public SortedSet<ByteBuffer> getColumnNames()
    {
        return columns.keySet();
    }

    public Collection<IColumn> getSortedColumns()
    {
        return columns.values();
    }

    public Collection<IColumn> getReverseSortedColumns()
    {
        return columns.descendingMap().values();
    }

    public Map<ByteBuffer, IColumn> getColumnsMap()
    {
        return columns;
    }

    public void remove(ByteBuffer columnName)
    {
        columns.remove(columnName);
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
        ColumnFamily cfDiff = new ColumnFamily(cfComposite.type, getComparator(), getSubComparator(), cfComposite.id());
        if (cfComposite.getMarkedForDeleteAt() > getMarkedForDeleteAt())
        {
            cfDiff.delete(cfComposite.getLocalDeletionTime(), cfComposite.getMarkedForDeleteAt());
        }

        // (don't need to worry about cfNew containing IColumns that are shadowed by
        // the delete tombstone, since cfNew was generated by CF.resolve, which
        // takes care of those for us.)
        Map<ByteBuffer, IColumn> columns = cfComposite.getColumnsMap();
        for (Map.Entry<ByteBuffer, IColumn> entry : columns.entrySet())
        {
            ByteBuffer cName = entry.getKey();
            IColumn columnInternal = this.columns.get(cName);
            IColumn columnExternal = entry.getValue();
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
        return null;
    }

    public AbstractType getComparator()
    {
        return (AbstractType)columns.comparator();
    }

    int size()
    {
        int size = 0;
        for (IColumn column : columns.values())
        {
            size += column.size();
        }
        return size;
    }

    public int hashCode()
    {
        throw new RuntimeException("Not implemented.");
    }

    public boolean equals(Object o)
    {
        throw new RuntimeException("Not implemented.");
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder("ColumnFamily(");
        CFMetaData cfm = metadata();
        sb.append(cfm == null ? "<anonymous>" : cfm.cfName);

        if (isMarkedForDelete())
            sb.append(" -deleted at ").append(getMarkedForDeleteAt()).append("-");

        sb.append(" [").append(getComparator().getColumnsString(getSortedColumns())).append("])");
        return sb.toString();
    }

    public static ByteBuffer digest(ColumnFamily cf)
    {
        MessageDigest digest = FBUtilities.threadLocalMD5Digest();
        if (cf != null)
            cf.updateDigest(digest);
        return ByteBuffer.wrap(digest.digest());
    }

    public void updateDigest(MessageDigest digest)
    {
        for (IColumn column : columns.values())
            column.updateDigest(digest);
    }

    public long getMarkedForDeleteAt()
    {
        return markedForDeleteAt.get();
    }

    public int getLocalDeletionTime()
    {
        return localDeletionTime.get();
    }

    public static AbstractType getComparatorFor(String table, String columnFamilyName, ByteBuffer superColumnName)
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

    public void resolve(ColumnFamily cf)
    {
        // Row _does_ allow null CF objects :(  seems a necessary evil for efficiency
        if (cf == null)
            return;
        addAll(cf);
    }

    public int getEstimatedColumnCount()
    {
        return getColumnCount();
    }

    public Iterator<IColumn> iterator()
    {
        return columns.values().iterator();
    }
}
