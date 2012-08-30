/*
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
import java.util.UUID;

import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.utils.*;

import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.sstable.ColumnStats;

public class ColumnFamily extends AbstractColumnContainer implements IRowCacheEntry
{
    /* The column serializer for this Column Family. Create based on config. */
    public static final ColumnFamilySerializer serializer = new ColumnFamilySerializer();
    private final CFMetaData cfm;

    public static ColumnFamily create(UUID cfId)
    {
        return create(Schema.instance.getCFMetaData(cfId));
    }

    public static ColumnFamily create(UUID cfId, ISortedColumns.Factory factory)
    {
        return create(Schema.instance.getCFMetaData(cfId), factory);
    }

    public static ColumnFamily create(String tableName, String cfName)
    {
        return create(Schema.instance.getCFMetaData(tableName, cfName));
    }

    public static ColumnFamily create(CFMetaData cfm)
    {
        return create(cfm, TreeMapBackedSortedColumns.factory());
    }

    public static ColumnFamily create(CFMetaData cfm, ISortedColumns.Factory factory)
    {
        return create(cfm, factory, false);
    }

    public static ColumnFamily create(CFMetaData cfm, ISortedColumns.Factory factory, boolean reversedInsertOrder)
    {
        return new ColumnFamily(cfm, factory.create(cfm.comparator, reversedInsertOrder));
    }

    protected ColumnFamily(CFMetaData cfm, ISortedColumns map)
    {
        super(map);
        assert cfm != null;
        this.cfm = cfm;
    }

    public ColumnFamily cloneMeShallow(ISortedColumns.Factory factory, boolean reversedInsertOrder)
    {
        ColumnFamily cf = ColumnFamily.create(cfm, factory, reversedInsertOrder);
        cf.delete(this);
        return cf;
    }

    public ColumnFamily cloneMeShallow()
    {
        return cloneMeShallow(columns.getFactory(), columns.isInsertReversed());
    }

    public AbstractType<?> getSubComparator()
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
        cf.delete(this);
        return cf;
    }

    public UUID id()
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

    public IColumnSerializer getColumnSerializer()
    {
        return cfm.getColumnSerializer();
    }

    public OnDiskAtom.Serializer getOnDiskSerializer()
    {
        return cfm.getOnDiskSerializer();
    }

    public boolean isSuper()
    {
        return getType() == ColumnFamilyType.Super;
    }

    /**
     * Same as addAll() but do a cloneMe of SuperColumn if necessary to
     * avoid keeping references to the structure (see #3957).
     */
    public void addAllWithSCCopy(ColumnFamily cf, Allocator allocator)
    {
        if (cf.isSuper())
        {
            for (IColumn c : cf)
            {
                columns.addColumn(((SuperColumn)c).cloneMe(), allocator);
            }
            delete(cf);
        }
        else
        {
            addAll(cf, allocator);
        }
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

    public void addAtom(OnDiskAtom atom)
    {
        if (atom instanceof IColumn)
        {
            addColumn((IColumn)atom);
        }
        else
        {
            assert atom instanceof RangeTombstone;
            delete(new DeletionInfo((RangeTombstone)atom, getComparator()));
        }
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
        cfDiff.delete(cfComposite.deletionInfo());

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

    /** the size of user-provided data, not including internal overhead */
    int dataSize()
    {
        int size = deletionInfo().dataSize();
        for (IColumn column : columns)
        {
            size += column.dataSize();
        }
        return size;
    }

    public long maxTimestamp()
    {
        long maxTimestamp = deletionInfo().maxTimestamp();
        for (IColumn column : columns)
            maxTimestamp = Math.max(maxTimestamp, column.maxTimestamp());
        return maxTimestamp;
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(373, 75437)
                    .append(cfm)
                    .append(deletionInfo())
                    .append(columns).toHashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || this.getClass() != o.getClass())
            return false;

        ColumnFamily comparison = (ColumnFamily) o;

        return cfm.equals(comparison.cfm)
                && deletionInfo().equals(comparison.deletionInfo())
                && ByteBufferUtil.compareUnsigned(digest(this), digest(comparison)) == 0;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ColumnFamily(");
        CFMetaData cfm = metadata();
        sb.append(cfm == null ? "<anonymous>" : cfm.cfName);

        if (isMarkedForDelete())
            sb.append(" -").append(deletionInfo()).append("-");

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

    public static AbstractType<?> getComparatorFor(String table, String columnFamilyName, ByteBuffer superColumnName)
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

    public ColumnStats getColumnStats()
    {
        long maxTimestampSeen = deletionInfo().maxTimestamp();
        StreamingHistogram tombstones = new StreamingHistogram(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE);

        for (IColumn column : columns)
        {
            maxTimestampSeen = Math.max(maxTimestampSeen, column.maxTimestamp());
            int deletionTime = column.getLocalDeletionTime();
            if (deletionTime < Integer.MAX_VALUE)
                tombstones.update(deletionTime);
        }
        return new ColumnStats(getColumnCount(), maxTimestampSeen, tombstones);
    }
}
