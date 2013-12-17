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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
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
    private final ArrayList<Cell> cells;

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
        this(metadata, new ArrayList<Cell>());
    }

    private UnsortedColumns(CFMetaData metadata, ArrayList<Cell> cells)
    {
        super(metadata);
        this.cells = cells;
    }

    public Factory getFactory()
    {
        return factory;
    }

    public ColumnFamily cloneMe()
    {
        return new UnsortedColumns(metadata, new ArrayList<Cell>(cells));
    }

    public boolean isInsertReversed()
    {
        return false;
    }

    public void clear()
    {
        cells.clear();
    }

    public void addColumn(Cell cell, Allocator allocator)
    {
        cells.add(cell);
    }

    public void addAll(ColumnFamily cm, Allocator allocator, Function<Cell, Cell> transformation)
    {
        delete(cm.deletionInfo());
        for (Cell cell : cm)
            addColumn(cell);
    }

    public Iterator<Cell> iterator()
    {
        return cells.iterator();
    }

    public boolean replace(Cell oldCell, Cell newCell)
    {
        throw new UnsupportedOperationException();
    }

    public Cell getColumn(CellName name)
    {
        throw new UnsupportedOperationException();
    }

    public Iterable<CellName> getColumnNames()
    {
        return Iterables.transform(cells, new Function<Cell, CellName>()
        {
            public CellName apply(Cell cell)
            {
                return cell.name;
            }
        });
    }

    public Collection<Cell> getSortedColumns()
    {
        throw new UnsupportedOperationException();
    }

    public Collection<Cell> getReverseSortedColumns()
    {
        throw new UnsupportedOperationException();
    }

    public int getColumnCount()
    {
        return cells.size();
    }

    public Iterator<Cell> iterator(ColumnSlice[] slices)
    {
        throw new UnsupportedOperationException();
    }

    public Iterator<Cell> reverseIterator(ColumnSlice[] slices)
    {
        throw new UnsupportedOperationException();
    }
}
