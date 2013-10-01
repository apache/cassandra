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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.utils.Allocator;

/**
 * A ColumnFamily that allows inserting in any order, even unsorted.
 *
 * Operations that require sorting (getSortedColumns) or that cannot be efficient without it
 * (replace, getColumn, etc.) are not supported.
 */
public class UnsortedColumns extends AbstractThreadUnsafeSortedColumns
{
    private final ArrayList<Column> columns;

    public static final Factory<UnsortedColumns> factory = new Factory<UnsortedColumns>()
    {
        public UnsortedColumns create(CFMetaData metadata, boolean insertReversed)
        {
            assert !insertReversed;
            return new UnsortedColumns(metadata);
        }
    };

    private UnsortedColumns(CFMetaData metadata)
    {
        this(metadata, new ArrayList<Column>());
    }

    private UnsortedColumns(CFMetaData metadata, ArrayList<Column> columns)
    {
        super(metadata);
        this.columns = columns;
    }

    public Factory getFactory()
    {
        return factory;
    }

    public ColumnFamily cloneMe()
    {
        return new UnsortedColumns(metadata, new ArrayList<Column>(columns));
    }

    public boolean isInsertReversed()
    {
        return false;
    }

    public void clear()
    {
        columns.clear();
    }

    public void addColumn(Column column, Allocator allocator)
    {
        columns.add(column);
    }

    public void addAll(ColumnFamily cm, Allocator allocator, Function<Column, Column> transformation)
    {
        delete(cm.deletionInfo());
        for (Column column : cm)
            addColumn(column);
    }

    public Iterator<Column> iterator()
    {
        return columns.iterator();
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
        return Iterables.transform(columns, new Function<Column, ByteBuffer>()
        {
            public ByteBuffer apply(Column column)
            {
                return column.name;
            }
        });
    }

    public Collection<Column> getSortedColumns()
    {
        throw new UnsupportedOperationException();
    }

    public Collection<Column> getReverseSortedColumns()
    {
        throw new UnsupportedOperationException();
    }

    public int getColumnCount()
    {
        return columns.size();
    }

    public Iterator<Column> iterator(ColumnSlice[] slices)
    {
        throw new UnsupportedOperationException();
    }

    public Iterator<Column> reverseIterator(ColumnSlice[] slices)
    {
        throw new UnsupportedOperationException();
    }
}
