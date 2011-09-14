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

import static org.apache.cassandra.db.DBConstants.*;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.HeapAllocator;

public class ColumnFamily extends AbstractColumnContainer
{
    /* The column serializer for this Column Family. Create based on config. */
    private static ColumnFamilySerializer serializer = new ColumnFamilySerializer();
    private final CFMetaData cfm;

    public static ColumnFamilySerializer serializer()
    {
        return serializer;
    }

    public static ColumnFamily create(Integer cfId)
    {
        return create(Schema.instance.getCFMetaData(cfId));
    }

    public static ColumnFamily create(Integer cfId, ISortedColumns.Factory factory)
    {
        return create(Schema.instance.getCFMetaData(cfId), factory);
    }

    public static ColumnFamily create(String tableName, String cfName)
    {
        return create(Schema.instance.getCFMetaData(tableName, cfName));
    }

    public static ColumnFamily create(CFMetaData cfm)
    {
        return create(cfm, ThreadSafeSortedColumns.factory());
    }

    public static ColumnFamily create(CFMetaData cfm, ISortedColumns.Factory factory)
    {
        return create(cfm, factory, false);
    }

    public static ColumnFamily create(CFMetaData cfm, ISortedColumns.Factory factory, boolean reversedInsertOrder)
    {
        return new ColumnFamily(cfm, factory.create(cfm.comparator, reversedInsertOrder));
    }

    private ColumnFamily(CFMetaData cfm, ISortedColumns map)
    {
        super(map);
        assert cfm != null;
        this.cfm = cfm;
    }
    
    public ColumnFamily cloneMeShallow(ISortedColumns.Factory factory, boolean reversedInsertOrder)
    {
        ColumnFamily cf = ColumnFamily.create(cfm, factory, reversedInsertOrder);
        // since deletion info is immutable, aliasing it is fine
        cf.deletionInfo.set(deletionInfo.get());
        return cf;
    }

    public ColumnFamily cloneMeShallow()
    {
        return cloneMeShallow(columns.getFactory(), columns.isInsertReversed());
    }

    public AbstractType getSubComparator()
    {
        IColumnSerializer s = getColumnSerializer();
        return (s instanceof SuperColumnSerializer) ? ((SuperColumnSerializer) s).getComparator() : null;
    }

    public ColumnFamilyType getType()
    {
        return cfm.cfType;
    }

    public ColumnFamily cloneMe()
    {
        ColumnFamily cf = new ColumnFamily(cfm, columns.cloneMe());
        // since deletion info is immutable, aliasing it is fine
        cf.deletionInfo.set(deletionInfo.get());
        return cf;
    }

    public Integer id()
    {
        return cfm.cfId;
    }

    /**
     * @return The CFMetaData for this row
     */
    public CFMetaData metadata()
    {
        return cfm;
    }

    /**
     * FIXME: shouldn't need to hold a reference to a serializer; worse, for super cfs,
     * it will be a _unique_ serializer object per row
     */
    public IColumnSerializer getColumnSerializer()
    {
        return cfm.getColumnSerializer();
    }

    public boolean isSuper()
    {
        return getType() == ColumnFamilyType.Super;
    }

    public void addColumn(QueryPath path, ByteBuffer value, long timestamp)
    {
        addColumn(path, value, timestamp, 0);
    }

    public void addColumn(QueryPath path, ByteBuffer value, long timestamp, int timeToLive)
    {
        assert path.columnName != null : path;
        assert !metadata().getDefaultValidator().isCommutative();
        Column column;
        if (timeToLive > 0)
            column = new ExpiringColumn(path.columnName, value, timestamp, timeToLive);
        else
            column = new Column(path.columnName, value, timestamp);
        addColumn(path.superColumnName, column);
    }

    public void addCounter(QueryPath path, long value)
    {
        assert path.columnName != null : path;
        addColumn(path.superColumnName, new CounterUpdateColumn(path.columnName, value, System.currentTimeMillis()));
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
     * This function will calculate the difference between 2 column families.
     * The external input is assumed to be a superset of internal.
     */
    public ColumnFamily diff(ColumnFamily cfComposite)
    {
        assert cfComposite.id().equals(id());
        ColumnFamily cfDiff = ColumnFamily.create(cfm);
        if (cfComposite.getMarkedForDeleteAt() > getMarkedForDeleteAt())
        {
            cfDiff.delete(cfComposite.getLocalDeletionTime(), cfComposite.getMarkedForDeleteAt());
        }

        // (don't need to worry about cfNew containing IColumns that are shadowed by
        // the delete tombstone, since cfNew was generated by CF.resolve, which
        // takes care of those for us.)
        for (IColumn columnExternal : cfComposite)
        {
            ByteBuffer cName = columnExternal.name();
            IColumn columnInternal = this.columns.getColumn(cName);
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

        if (!cfDiff.isEmpty() || cfDiff.isMarkedForDelete())
            return cfDiff;
        return null;
    }

    int size()
    {
        int size = 0;
        for (IColumn column : columns)
        {
            size += column.size();
        }
        return size;
    }

    public long maxTimestamp()
    {
        long maxTimestamp = Long.MIN_VALUE;
        for (IColumn column : columns)
            maxTimestamp = Math.max(maxTimestamp, column.maxTimestamp());
        return maxTimestamp;
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
        for (IColumn column : columns)
            column.updateDigest(digest);
    }

    public static AbstractType getComparatorFor(String table, String columnFamilyName, ByteBuffer superColumnName)
    {
        return superColumnName == null
               ? Schema.instance.getComparator(table, columnFamilyName)
               : Schema.instance.getSubComparator(table, columnFamilyName);
    }

    public static ColumnFamily diff(ColumnFamily cf1, ColumnFamily cf2)
    {
        if (cf1 == null)
            return cf2;
        return cf1.diff(cf2);
    }

    public void resolve(ColumnFamily cf)
    {
        resolve(cf, HeapAllocator.instance);
    }

    public void resolve(ColumnFamily cf, Allocator allocator)
    {
        // Row _does_ allow null CF objects :(  seems a necessary evil for efficiency
        if (cf == null)
            return;
        addAll(cf, allocator);
    }

    public long serializedSize()
    {
        return boolSize // nullness bool
               + intSize // id
               + serializedSizeForSSTable();
    }

    public long serializedSizeForSSTable()
    {
        int size = intSize // local deletion time
                 + longSize // client deletion time
                 + intSize; // column count
        for (IColumn column : columns)
            size += column.serializedSize();
        return size;
    }

    /**
     * Goes over all columns and check the fields are valid (as far as we can
     * tell).
     * This is used to detect corruption after deserialization.
     */
    public void validateColumnFields() throws MarshalException
    {
        CFMetaData metadata = metadata();
        for (IColumn column : this)
        {
            column.validateFields(metadata);
        }
    }
}
