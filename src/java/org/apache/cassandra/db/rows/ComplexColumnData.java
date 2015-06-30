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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * The data for a complex column, that is it's cells and potential complex
 * deletion time.
 */
public class ComplexColumnData implements ColumnData, Iterable<Cell>
{
    static final Cell[] NO_CELLS = new Cell[0];

    private static final long EMPTY_SIZE = ObjectSizes.measure(new ComplexColumnData(ColumnDefinition.regularDef("", "", "", SetType.getInstance(ByteType.instance, true)), NO_CELLS, new DeletionTime(0, 0)));

    private final ColumnDefinition column;

    // The cells for 'column' sorted by cell path.
    private final Cell[] cells;

    private final DeletionTime complexDeletion;

    // Only ArrayBackedRow should call this.
    ComplexColumnData(ColumnDefinition column, Cell[] cells, DeletionTime complexDeletion)
    {
        assert column.isComplex();
        assert cells.length > 0 || !complexDeletion.isLive();
        this.column = column;
        this.cells = cells;
        this.complexDeletion = complexDeletion;
    }

    public boolean hasCells()
    {
        return cellsCount() > 0;
    }

    public int cellsCount()
    {
        return cells.length;
    }

    public ColumnDefinition column()
    {
        return column;
    }

    public Cell getCell(CellPath path)
    {
        int idx = binarySearch(path);
        return idx < 0 ? null : cells[idx];
    }

    public Cell getCellByIndex(int i)
    {
        assert 0 <= i && i < cells.length;
        return cells[i];
    }

    /**
     * The complex deletion time of the complex column.
     * <p>
     * The returned "complex deletion" is a deletion of all the cells of the column. For instance,
     * for a collection, this correspond to a full collection deletion.
     * Please note that this deletion says nothing about the individual cells of the complex column:
     * there can be no complex deletion but some of the individual cells can be deleted.
     *
     * @return the complex deletion time for the column this is the data of or {@code DeletionTime.LIVE}
     * if the column is not deleted.
     */
    public DeletionTime complexDeletion()
    {
        return complexDeletion;
    }

    public Iterator<Cell> iterator()
    {
        return Iterators.forArray(cells);
    }

    public int dataSize()
    {
        int size = complexDeletion.dataSize();
        for (Cell cell : cells)
            size += cell.dataSize();
        return size;
    }

    public long unsharedHeapSizeExcludingData()
    {
        long heapSize = EMPTY_SIZE + ObjectSizes.sizeOfArray(cells);
        for (Cell cell : cells)
            heapSize += cell.unsharedHeapSizeExcludingData();
        return heapSize;
    }

    public void validate()
    {
        for (Cell cell : cells)
            cell.validate();
    }

    public ComplexColumnData filter(ColumnFilter filter, DeletionTime activeDeletion, CFMetaData.DroppedColumn dropped)
    {
        ColumnFilter.Tester cellTester = filter.newTester(column);
        if (cellTester == null && activeDeletion.isLive() && dropped == null)
            return this;

        DeletionTime newComplexDeletion = activeDeletion.supersedes(complexDeletion) ? DeletionTime.LIVE : complexDeletion;

        int newSize = 0;
        for (Cell cell : cells)
        {
            // The cell must be:
            //   - Included by the query
            //   - not shadowed by the active deletion
            //   - not being for a dropped column
            if ((cellTester == null || cellTester.includes(cell.path()))
                 && !activeDeletion.deletes(cell)
                 && (dropped == null || cell.timestamp() > dropped.droppedTime))
                ++newSize;
        }


        if (newSize == 0)
            return newComplexDeletion.isLive() ? null : new ComplexColumnData(column, NO_CELLS, newComplexDeletion);

        if (newSize == cells.length && newComplexDeletion == complexDeletion)
            return this;

        Cell[] newCells = new Cell[newSize];
        int j = 0;
        cellTester = filter.newTester(column); // we need to reste the tester
        for (Cell cell : cells)
        {
            if ((cellTester == null || cellTester.includes(cell.path()))
                && !activeDeletion.deletes(cell)
                && (dropped == null || cell.timestamp() > dropped.droppedTime))
                newCells[j++] = cell;
        }
        assert j == newSize;

        return new ComplexColumnData(column, newCells, newComplexDeletion);
    }

    public void digest(MessageDigest digest)
    {
        if (!complexDeletion.isLive())
            complexDeletion.digest(digest);

        for (Cell cell : cells)
            cell.digest(digest);
    }

    public ComplexColumnData markCounterLocalToBeCleared()
    {
        Cell[] newCells = null;
        for (int i = 0; i < cells.length; i++)
        {
            Cell cell = cells[i];
            Cell marked = cell.markCounterLocalToBeCleared();
            if (marked != cell)
            {
                if (newCells == null)
                    newCells = Arrays.copyOf(cells, cells.length);
                newCells[i] = marked;
            }
        }

        return newCells == null
             ? this
             : new ComplexColumnData(column, newCells, complexDeletion);
    }

    public ComplexColumnData purge(DeletionPurger purger, int nowInSec)
    {
        DeletionTime newDeletion = complexDeletion.isLive() || purger.shouldPurge(complexDeletion) ? DeletionTime.LIVE : complexDeletion;

        int newSize = 0;
        for (Cell cell : cells)
        {
            Cell purged = cell.purge(purger, nowInSec);
            if (purged != null)
                ++newSize;
        }

        if (newSize == 0)
            return newDeletion.isLive() ? null : new ComplexColumnData(column, NO_CELLS, newDeletion);

        if (newDeletion == complexDeletion && newSize == cells.length)
            return this;

        Cell[] newCells = new Cell[newSize];
        int j = 0;
        for (Cell cell : cells)
        {
            Cell purged = cell.purge(purger, nowInSec);
            if (purged != null)
                newCells[j++] = purged;
        }
        assert j == newSize;

        return new ComplexColumnData(column, newCells, newDeletion);
    }

    public ComplexColumnData updateAllTimestamp(long newTimestamp)
    {
        DeletionTime newDeletion = complexDeletion.isLive() ? complexDeletion : new DeletionTime(newTimestamp - 1, complexDeletion.localDeletionTime());
        Cell[] newCells = new Cell[cells.length];
        for (int i = 0; i < cells.length; i++)
            newCells[i] = (Cell)cells[i].updateAllTimestamp(newTimestamp);

        return new ComplexColumnData(column, newCells, newDeletion);
    }

    // This is the partner in crime of ArrayBackedRow.setValue. The exact warning apply. The short
    // version is: "don't use that method".
    void setValue(CellPath path, ByteBuffer value)
    {
        int idx = binarySearch(path);
        assert idx >= 0;
        cells[idx] = cells[idx].withUpdatedValue(value);
    }

    private int binarySearch(CellPath path)
    {
        return binarySearch(path, 0, cells.length);
    }

    /**
     * Simple binary search for a given cell (in the cells array).
     *
     * The return value has the exact same meaning that the one of Collections.binarySearch() but
     * we don't use the later because we're searching for a 'CellPath' in an array of 'Cell'.
     */
    private int binarySearch(CellPath path, int fromIndex, int toIndex)
    {
        int low = fromIndex;
        int mid = toIndex;
        int high = mid - 1;
        int result = -1;
        while (low <= high)
        {
            mid = (low + high) >> 1;
            if ((result = column.cellPathComparator().compare(path, cells[mid].path())) > 0)
                low = mid + 1;
            else if (result == 0)
                return mid;
            else
                high = mid - 1;
        }
        return -mid - (result < 0 ? 1 : 2);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;

        if(!(other instanceof ComplexColumnData))
            return false;

        ComplexColumnData that = (ComplexColumnData)other;
        return this.column().equals(that.column())
            && this.complexDeletion().equals(that.complexDeletion)
            && Arrays.equals(this.cells, that.cells);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column(), complexDeletion(), cells);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private ColumnDefinition column;
        private DeletionTime complexDeletion;
        public final List<Cell> cells = new ArrayList<>();

        public void newColumn(ColumnDefinition column)
        {
            this.column = column;
            this.complexDeletion = DeletionTime.LIVE; // default if writeComplexDeletion is not called
            this.cells.clear();
        }

        public void addComplexDeletion(DeletionTime complexDeletion)
        {
            this.complexDeletion = complexDeletion;
        }

        public void addCell(Cell cell)
        {
            assert cell.column().equals(column);
            assert cells.isEmpty() || cell.column().cellPathComparator().compare(cells.get(cells.size() - 1).path(), cell.path()) < 0;
            cells.add(cell);
        }

        public ComplexColumnData build()
        {
            if (complexDeletion.isLive() && cells.isEmpty())
                return null;

            return new ComplexColumnData(column, cells.toArray(new Cell[cells.size()]), complexDeletion);
        }
    }
}
