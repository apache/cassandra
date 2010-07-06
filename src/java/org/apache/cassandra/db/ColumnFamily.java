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

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.config.CFMetaData;

import org.apache.cassandra.db.IClock.ClockRelationship;
import org.apache.cassandra.db.clock.AbstractReconciler;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.IIterableColumns;
import org.apache.cassandra.utils.FBUtilities;

public class ColumnFamily implements IColumnContainer, IIterableColumns
{
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
        if (cfm == null)
            throw new IllegalArgumentException("Unknown column family.");
        return new ColumnFamily(cfm.cfType, cfm.clockType, cfm.comparator, cfm.subcolumnComparator, cfm.reconciler, cfm.cfId);
    }

    private final Integer cfid;
    private final ColumnFamilyType type;
    private final ClockType clockType;
    private final AbstractReconciler reconciler;

    private transient ICompactSerializer2<IColumn> columnSerializer;
    final AtomicReference<IClock> markedForDeleteAt;
    final AtomicInteger localDeletionTime = new AtomicInteger(Integer.MIN_VALUE);
    private ConcurrentSkipListMap<byte[], IColumn> columns;

    public ColumnFamily(ColumnFamilyType type, ClockType clockType, AbstractType comparator, AbstractType subcolumnComparator, AbstractReconciler reconciler, Integer cfid)
    {
        this.type = type;
        this.clockType = clockType;
        this.reconciler = reconciler;
        this.markedForDeleteAt = new AtomicReference<IClock>(clockType.minClock());
        columnSerializer = type == ColumnFamilyType.Standard ? Column.serializer(clockType) : SuperColumn.serializer(subcolumnComparator, clockType, reconciler);
        columns = new ConcurrentSkipListMap<byte[], IColumn>(comparator);
        this.cfid = cfid;
     }
    
    public ColumnFamily cloneMeShallow()
    {
        ColumnFamily cf = new ColumnFamily(type, clockType, getComparator(), getSubComparator(), reconciler, cfid);
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

    public ClockType getClockType()
    {
        return clockType;
    }
    
    public AbstractReconciler getReconciler()
    {
        return reconciler;
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
        {
            addColumn(column);
        }
        delete(cf);
    }

    /**
     * FIXME: Gross.
     */
    public ICompactSerializer2<IColumn> getColumnSerializer()
    {
        return columnSerializer;
    }

    int getColumnCount()
    {
        return columns.size();
    }

    public boolean isSuper()
    {
        return type == ColumnFamilyType.Super;
    }

    public void addColumn(QueryPath path, byte[] value, IClock clock)
    {
        assert path.columnName != null : path;
        addColumn(path.superColumnName, new Column(path.columnName, value, clock));
    }

    public void addTombstone(QueryPath path, byte[] localDeletionTime, IClock clock)
    {
        addColumn(path.superColumnName, new DeletedColumn(path.columnName, localDeletionTime, clock));
    }

    public void addColumn(QueryPath path, byte[] value, IClock clock, int timeToLive)
    {
        assert path.columnName != null : path;
        Column column;
        if (timeToLive > 0)
            column = new ExpiringColumn(path.columnName, value, clock, timeToLive);
        else
            column = new Column(path.columnName, value, clock);
        addColumn(path.superColumnName, column);
    }

    public void deleteColumn(QueryPath path, int localDeletionTime, IClock clock)
    {
        assert path.columnName != null : path;
        addColumn(path.superColumnName, new DeletedColumn(path.columnName, localDeletionTime, clock));
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
            c = new SuperColumn(superColumnName, getSubComparator(), clockType, reconciler);
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
        byte[] name = column.name();
        IColumn oldColumn = columns.putIfAbsent(name, column);
        if (oldColumn != null)
        {
            if (oldColumn instanceof SuperColumn)
            {
                ((SuperColumn) oldColumn).putColumn(column);
            }
            else
            {
                // calculate reconciled col from old (existing) col and new col
                IColumn reconciledColumn = reconciler.reconcile((Column)column, (Column)oldColumn);
                while (!columns.replace(name, oldColumn, reconciledColumn))
                {
                    // if unable to replace, then get updated old (existing) col
                    oldColumn = columns.get(name);
                    // re-calculate reconciled col from updated old col and original new col
                    reconciledColumn = reconciler.reconcile((Column)column, (Column)oldColumn);
                    // try to re-update value, again
                }
            }
        }
    }

    public IColumn getColumn(byte[] name)
    {
        return columns.get(name);
    }

    public SortedSet<byte[]> getColumnNames()
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

    public Map<byte[], IColumn> getColumnsMap()
    {
        return columns;
    }

    public void remove(byte[] columnName)
    {
        columns.remove(columnName);
    }

    @Deprecated // TODO this is a hack to set initial value outside constructor
    public void delete(int localtime, IClock clock)
    {
        localDeletionTime.set(localtime);
        markedForDeleteAt.set(clock);
    }

    public void delete(ColumnFamily cf2)
    {
        FBUtilities.atomicSetMax(localDeletionTime, cf2.getLocalDeletionTime()); // do this first so we won't have a column that's "deleted" but has no local deletion time
        FBUtilities.atomicSetMax(markedForDeleteAt, cf2.getMarkedForDeleteAt());
    }

    public boolean isMarkedForDelete()
    {
        IClock _markedForDeleteAt = markedForDeleteAt.get();
        return _markedForDeleteAt.compare(clockType.minClock()) == ClockRelationship.GREATER_THAN;
    }

    /*
     * This function will calculate the difference between 2 column families.
     * The external input is assumed to be a superset of internal.
     */
    public ColumnFamily diff(ColumnFamily cfComposite)
    {
        ColumnFamily cfDiff = new ColumnFamily(cfComposite.type, cfComposite.clockType, getComparator(), getSubComparator(), cfComposite.reconciler, cfComposite.id());
        ClockRelationship rel = cfComposite.getMarkedForDeleteAt().compare(getMarkedForDeleteAt());
        if (ClockRelationship.GREATER_THAN == rel)
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
            IColumn columnInternal = this.columns.get(cName);
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
        sb.append(cfm == null ? "-deleted-" : cfm.cfName);

        if (isMarkedForDelete())
            sb.append(" -deleted at " + getMarkedForDeleteAt().toString() + "-");

        sb.append(" [").append(getComparator().getColumnsString(getSortedColumns())).append("])");
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
        for (IColumn column : columns.values())
            column.updateDigest(digest);
    }

    public IClock getMarkedForDeleteAt()
    {
        return markedForDeleteAt.get();
    }

    public int getLocalDeletionTime()
    {
        return localDeletionTime.get();
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

    public int getEstimatedColumnCount()
    {
        return getColumnCount();
    }

    public Iterator<IColumn> iterator()
    {
        return columns.values().iterator();
    }
}
