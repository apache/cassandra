package org.apache.cassandra.db;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.utils.Allocator;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

public class EmptyColumns extends AbstractThreadUnsafeSortedColumns
{
    public static final Factory<EmptyColumns> factory = new Factory<EmptyColumns>()
    {
        public EmptyColumns create(CFMetaData metadata, boolean insertReversed)
        {
            assert !insertReversed;
            return new EmptyColumns(metadata, DeletionInfo.live());
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

    public void clear()
    {
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
        return Iterators.emptyIterator();
    }

    public Iterator<Column> reverseIterator(ColumnSlice[] slices)
    {
        return Iterators.emptyIterator();
    }

    public boolean isInsertReversed()
    {
        return false;
    }
}
