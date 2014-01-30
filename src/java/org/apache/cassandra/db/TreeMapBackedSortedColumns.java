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

import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import com.google.common.base.Function;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public class TreeMapBackedSortedColumns extends AbstractThreadUnsafeSortedColumns
{
    private final TreeMap<CellName, Cell> map;

    public static final ColumnFamily.Factory<TreeMapBackedSortedColumns> factory = new Factory<TreeMapBackedSortedColumns>()
    {
        public TreeMapBackedSortedColumns create(CFMetaData metadata, boolean insertReversed)
        {
            assert !insertReversed;
            return new TreeMapBackedSortedColumns(metadata);
        }
    };

    public CellNameType getComparator()
    {
        return (CellNameType)map.comparator();
    }

    private TreeMapBackedSortedColumns(CFMetaData metadata)
    {
        super(metadata);
        this.map = new TreeMap<>(metadata.comparator);
    }

    private TreeMapBackedSortedColumns(CFMetaData metadata, SortedMap<CellName, Cell> columns)
    {
        super(metadata);
        this.map = new TreeMap<>(columns);
    }

    public ColumnFamily.Factory getFactory()
    {
        return factory;
    }

    public ColumnFamily cloneMe()
    {
        return new TreeMapBackedSortedColumns(metadata, map);
    }

    public boolean isInsertReversed()
    {
        return false;
    }

    /*
     * If we find an old cell that has the same name
     * the ask it to resolve itself else add the new cell
    */
    public void addColumn(Cell cell, AbstractAllocator allocator)
    {
        CellName name = cell.name();
        // this is a slightly unusual way to structure this; a more natural way is shown in ThreadSafeSortedColumns,
        // but TreeMap lacks putAbsent.  Rather than split it into a "get, then put" check, we do it as follows,
        // which saves the extra "get" in the no-conflict case [for both normal and super columns],
        // in exchange for a re-put in the SuperColumn case.
        Cell oldCell = map.put(name, cell);
        if (oldCell == null)
            return;

        // calculate reconciled col from old (existing) col and new col
        map.put(name, cell.reconcile(oldCell, allocator));
    }

    /**
     * We need to go through each column in the column container and resolve it before adding
     */
    public void addAll(ColumnFamily cm, AbstractAllocator allocator, Function<Cell, Cell> transformation)
    {
        delete(cm.deletionInfo());
        for (Cell cell : cm)
            addColumn(transformation.apply(cell), allocator);
    }

    public boolean replace(Cell oldCell, Cell newCell)
    {
        if (!oldCell.name().equals(newCell.name()))
            throw new IllegalArgumentException();

        // We are not supposed to put the newCell is either there was not
        // column or the column was not equal to oldCell (to be coherent
        // with other implementation). We optimize for the common case where
        // oldCell do is present though.
        Cell previous = map.put(oldCell.name(), newCell);
        if (previous == null)
        {
            map.remove(oldCell.name());
            return false;
        }
        if (!previous.equals(oldCell))
        {
            map.put(oldCell.name(), previous);
            return false;
        }
        return true;
    }

    public Cell getColumn(CellName name)
    {
        return map.get(name);
    }

    public void clear()
    {
        setDeletionInfo(DeletionInfo.live());
        map.clear();
    }

    public int getColumnCount()
    {
        return map.size();
    }

    public Collection<Cell> getSortedColumns()
    {
        return map.values();
    }

    public Collection<Cell> getReverseSortedColumns()
    {
        return map.descendingMap().values();
    }

    public SortedSet<CellName> getColumnNames()
    {
        return map.navigableKeySet();
    }

    public Iterator<Cell> iterator()
    {
        return map.values().iterator();
    }

    public Iterator<Cell> iterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(map, slices);
    }

    public Iterator<Cell> reverseIterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(map.descendingMap(), slices);
    }

}
