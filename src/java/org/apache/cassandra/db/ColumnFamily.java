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
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.UUID;

import com.google.common.base.Function;
import com.google.common.base.Functions;

import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.utils.*;

import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.io.sstable.ColumnStats;

public class ColumnFamily implements IRowCacheEntry, Iterable<Column>
{
    /* The column serializer for this Column Family. Create based on config. */
    public static final ColumnFamilySerializer serializer = new ColumnFamilySerializer();
    private final CFMetaData cfm;
    protected final ISortedColumns columns;

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

    protected ColumnFamily(CFMetaData cfm, ISortedColumns columns)
    {
        this.columns = columns;
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

    public void addColumn(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        addColumn(name, value, timestamp, 0);
    }

    public void addColumn(ByteBuffer name, ByteBuffer value, long timestamp, int timeToLive)
    {
        assert !metadata().getDefaultValidator().isCommutative();
        Column column = Column.create(name, value, timestamp, timeToLive, metadata());
        addColumn(column);
    }

    public void addCounter(ByteBuffer name, long value)
    {
        addColumn(new CounterUpdateColumn(name, value, System.currentTimeMillis()));
    }

    public void addTombstone(ByteBuffer name, ByteBuffer localDeletionTime, long timestamp)
    {
        addColumn(new DeletedColumn(name, localDeletionTime, timestamp));
    }

    public void addTombstone(ByteBuffer name, int localDeletionTime, long timestamp)
    {
        addColumn(new DeletedColumn(name, localDeletionTime, timestamp));
    }

    public void addAtom(OnDiskAtom atom)
    {
        if (atom instanceof Column)
        {
            addColumn((Column)atom);
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

        // (don't need to worry about cfNew containing Columns that are shadowed by
        // the delete tombstone, since cfNew was generated by CF.resolve, which
        // takes care of those for us.)
        for (Column columnExternal : cfComposite)
        {
            ByteBuffer cName = columnExternal.name();
            Column columnInternal = this.columns.getColumn(cName);
            if (columnInternal == null)
            {
                cfDiff.addColumn(columnExternal);
            }
            else
            {
                Column columnDiff = columnInternal.diff(columnExternal);
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

    public long maxTimestamp()
    {
        long maxTimestamp = deletionInfo().maxTimestamp();
        for (Column column : columns)
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
        for (Column column : columns)
            column.updateDigest(digest);
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
        for (Column column : this)
        {
            column.validateFields(metadata);
        }
    }

    public ColumnStats getColumnStats()
    {
        long minTimestampSeen = deletionInfo() == DeletionInfo.LIVE ? Long.MAX_VALUE : deletionInfo().minTimestamp();
        long maxTimestampSeen = deletionInfo().maxTimestamp();
        StreamingHistogram tombstones = new StreamingHistogram(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE);
        int maxLocalDeletionTime = Integer.MIN_VALUE;

        for (Column column : columns)
        {
            minTimestampSeen = Math.min(minTimestampSeen, column.minTimestamp());
            maxTimestampSeen = Math.max(maxTimestampSeen, column.maxTimestamp());
            maxLocalDeletionTime = Math.max(maxLocalDeletionTime, column.getLocalDeletionTime());
            int deletionTime = column.getLocalDeletionTime();
            if (deletionTime < Integer.MAX_VALUE)
                tombstones.update(deletionTime);
        }
        return new ColumnStats(getColumnCount(), minTimestampSeen, maxTimestampSeen, maxLocalDeletionTime, tombstones);
    }

    public void delete(ColumnFamily cc2)
    {
        delete(cc2.columns.getDeletionInfo());
    }

    public void delete(DeletionInfo delInfo)
    {
        columns.delete(delInfo);
    }

    // Contrarily to delete(), this will use the provided info even if those
    // are older that the current ones. Used for SuperColumn in QueryFilter.
    // delete() is probably the right method in all other cases.
    public void setDeletionInfo(DeletionInfo delInfo)
    {
        columns.setDeletionInfo(delInfo);
    }

    public boolean isMarkedForDelete()
    {
        return !deletionInfo().isLive();
    }

    public DeletionInfo deletionInfo()
    {
        return columns.getDeletionInfo();
    }

    public AbstractType<?> getComparator()
    {
        return columns.getComparator();
    }

    /**
     * Drops expired row-level tombstones.  Normally, these are dropped once the row no longer exists, but
     * if new columns are inserted into the row post-deletion, they can keep the row tombstone alive indefinitely,
     * with non-intuitive results.  See https://issues.apache.org/jira/browse/CASSANDRA-2317
     */
    public void maybeResetDeletionTimes(int gcBefore)
    {
        columns.maybeResetDeletionTimes(gcBefore);
    }

    public long addAllWithSizeDelta(ColumnFamily cc, Allocator allocator, Function<Column, Column> transformation, SecondaryIndexManager.Updater indexer)
    {
        return columns.addAllWithSizeDelta(cc.columns, allocator, transformation, indexer);
    }

    public void addAll(ColumnFamily cc, Allocator allocator, Function<Column, Column> transformation)
    {
        columns.addAll(cc.columns, allocator, transformation);
    }

    public void addAll(ColumnFamily cc, Allocator allocator)
    {
        addAll(cc, allocator, Functions.<Column>identity());
    }

    public void addColumn(Column column)
    {
        addColumn(column, HeapAllocator.instance);
    }

    public void addColumn(Column column, Allocator allocator)
    {
        columns.addColumn(column, allocator);
    }

    public Column getColumn(ByteBuffer name)
    {
        return columns.getColumn(name);
    }

    public boolean replace(Column oldColumn, Column newColumn)
    {
        return columns.replace(oldColumn, newColumn);
    }

    /*
    * Note that for some of the implementation backing the container, the
    * return set may not have implementation for tailSet, headSet and subSet.
    * See ColumnNamesSet in ArrayBackedSortedColumns for more details.
    */
    public SortedSet<ByteBuffer> getColumnNames()
    {
        return columns.getColumnNames();
    }

    public Collection<Column> getSortedColumns()
    {
        return columns.getSortedColumns();
    }

    public Collection<Column> getReverseSortedColumns()
    {
        return columns.getReverseSortedColumns();
    }

    public void remove(ByteBuffer columnName)
    {
        columns.removeColumn(columnName);
    }

    public int getColumnCount()
    {
        return columns.size();
    }

    public boolean isEmpty()
    {
        return columns.isEmpty();
    }

    public boolean hasOnlyTombstones()
    {
        for (Column column : columns)
        {
            if (column.isLive())
                return false;
        }
        return true;
    }

    public Iterator<Column> iterator()
    {
        return columns.iterator();
    }

    public Iterator<Column> iterator(ColumnSlice[] slices)
    {
        return columns.iterator(slices);
    }

    public Iterator<Column> reverseIterator(ColumnSlice[] slices)
    {
        return columns.reverseIterator(slices);
    }

    public boolean hasIrrelevantData(int gcBefore)
    {
        // Do we have gcable deletion infos?
        if (!deletionInfo().purge(gcBefore).equals(deletionInfo()))
            return true;

        // Do we have colums that are either deleted by the container or gcable tombstone?
        for (Column column : columns)
            if (deletionInfo().isDeleted(column) || column.hasIrrelevantData(gcBefore))
                return true;

        return false;
    }
}
