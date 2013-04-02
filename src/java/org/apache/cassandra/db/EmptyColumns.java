package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import com.google.common.base.Function;

import com.sun.xml.internal.xsom.impl.scd.Iterators;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.utils.Allocator;

public class EmptyColumns extends AbstractThreadUnsafeSortedColumns
{
    public static final Factory<EmptyColumns> factory = new Factory<EmptyColumns>()
    {
        public EmptyColumns create(CFMetaData metadata, boolean insertReversed)
        {
            assert !insertReversed;
            return new EmptyColumns(metadata, DeletionInfo.LIVE);
        }
    };

    public EmptyColumns(CFMetaData metadata, DeletionInfo info)
    {
        super(metadata, info);
    }

    public ColumnFamily cloneMe()
    {
        return new EmptyColumns(metadata, deletionInfo);
    }

    public void clear() {
    }

    public Factory<EmptyColumns> getFactory()
    {
        return factory;
    }

    public void addColumn(Column column, Allocator allocator)
    {
        throw new UnsupportedOperationException();
    }

    public void addAll(ColumnFamily cm, Allocator allocator, Function<Column, Column> transformation)
    {
        throw new UnsupportedOperationException();
    }

    public boolean replace(Column oldColumn, Column newColumn)
    {
        throw new UnsupportedOperationException();
    }

    public Column getColumn(ByteBuffer name)
    {
        throw new UnsupportedOperationException();
    }

    public Iterable<ByteBuffer> getColumnNames()
    {
        return Collections.emptyList();
    }

    public Collection<Column> getSortedColumns()
    {
        return Collections.emptyList();
    }

    public Collection<Column> getReverseSortedColumns()
    {
        return Collections.emptyList();
    }

    public int getColumnCount()
    {
        return 0;
    }

    public Iterator<Column> iterator(ColumnSlice[] slices)
    {
        return Iterators.empty();
    }

    public Iterator<Column> reverseIterator(ColumnSlice[] slices)
    {
        return Iterators.empty();
    }

    public boolean isInsertReversed()
    {
        return false;
    }
}
