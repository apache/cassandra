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
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.utils.FBUtilities;

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
        return create(DatabaseDescriptor.getCFMetaData(cfId));
    }

    public static ColumnFamily create(String tableName, String cfName)
    {
        return create(DatabaseDescriptor.getCFMetaData(tableName, cfName));
    }

    public static ColumnFamily create(CFMetaData cfm)
    {
        return new ColumnFamily(cfm);
    }

    public ColumnFamily(CFMetaData cfm)
    {
        this(cfm, new ConcurrentSkipListMap<ByteBuffer, IColumn>(cfm.comparator));
    }

    private ColumnFamily(CFMetaData cfm, ConcurrentSkipListMap<ByteBuffer, IColumn> map)
    {
        super(map);
        assert cfm != null;
        this.cfm = cfm;
    }
    
    public ColumnFamily cloneMeShallow()
    {
        ColumnFamily cf = new ColumnFamily(cfm);
        // since deletion info is immutable, aliasing it is fine
        cf.deletionInfo.set(deletionInfo.get());
        return cf;
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
        ColumnFamily cf = new ColumnFamily(cfm, columns.clone());
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
        ColumnFamily cfDiff = new ColumnFamily(cfm);
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

    int size()
    {
        int size = 0;
        for (IColumn column : columns.values())
        {
            size += column.size();
        }
        return size;
    }

    public long maxTimestamp()
    {
        long maxTimestamp = Long.MIN_VALUE;
        for (IColumn column : columns.values())
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
        for (IColumn column : columns.values())
            column.updateDigest(digest);
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
        for (IColumn column : columns.values())
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
        for (IColumn column : getSortedColumns())
        {
            column.validateFields(metadata);
        }
    }

    protected void putColumn(SuperColumn sc)
    {
        throw new UnsupportedOperationException("Unsupported operation for a column family");
    }
}
