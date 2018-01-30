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
import java.util.Iterator;
import java.util.Objects;

import com.google.common.base.Function;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;

/**
 * The data for a complex column, that is it's cells and potential complex
 * deletion time.
 */
public class ComplexColumnData extends ColumnData implements Iterable<Cell>
{
    static final Cell[] NO_CELLS = new Cell[0];

    private static final long EMPTY_SIZE = ObjectSizes.measure(new ComplexColumnData(ColumnDefinition.regularDef("", "", "", SetType.getInstance(ByteType.instance, true)), NO_CELLS, new DeletionTime(0, 0)));

    // The cells for 'column' sorted by cell path.
    private final Object[] cells;

    private final DeletionTime complexDeletion;

    // Only ArrayBackedRow should call this.
    ComplexColumnData(ColumnDefinition column, Object[] cells, DeletionTime complexDeletion)
    {
        super(column);
        assert column.isComplex();
        assert cells.length > 0 || !complexDeletion.isLive();
        this.cells = cells;
        this.complexDeletion = complexDeletion;
    }

    public boolean hasCells()
    {
        return !BTree.isEmpty(cells);
    }

    public int cellsCount()
    {
        return BTree.size(cells);
    }

    public Cell getCell(CellPath path)
    {
        return (Cell) BTree.<Object>find(cells, column.asymmetricCellPathComparator(), path);
    }

    public Cell getCellByIndex(int idx)
    {
        return BTree.findByIndex(cells, idx);
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
        return BTree.iterator(cells);
    }

    public Iterator<Cell> reverseIterator()
    {
        return BTree.iterator(cells, BTree.Dir.DESC);
    }

    public int dataSize()
    {
        int size = complexDeletion.dataSize();
        for (Cell cell : this)
            size += cell.dataSize();
        return size;
    }

    public long unsharedHeapSizeExcludingData()
    {
        long heapSize = EMPTY_SIZE + ObjectSizes.sizeOfArray(cells);
        // TODO: this can be turned into a simple multiplication, at least while we have only one Cell implementation
        for (Cell cell : this)
            heapSize += cell.unsharedHeapSizeExcludingData();
        return heapSize;
    }

    public void validate()
    {
        for (Cell cell : this)
            cell.validate();
    }

    public void digest(MessageDigest digest)
    {
        if (!complexDeletion.isLive())
            complexDeletion.digest(digest);

        for (Cell cell : this)
            cell.digest(digest);
    }

    public ComplexColumnData markCounterLocalToBeCleared()
    {
        return transformAndFilter(complexDeletion, Cell::markCounterLocalToBeCleared);
    }

    public ComplexColumnData filter(ColumnFilter filter, DeletionTime activeDeletion, CFMetaData.DroppedColumn dropped, LivenessInfo rowLiveness)
    {
        ColumnFilter.Tester cellTester = filter.newTester(column);
        if (cellTester == null && activeDeletion.isLive() && dropped == null)
            return this;

        DeletionTime newDeletion = activeDeletion.supersedes(complexDeletion) ? DeletionTime.LIVE : complexDeletion;
        return transformAndFilter(newDeletion, (cell) ->
        {
            boolean isForDropped = dropped != null && cell.timestamp() <= dropped.droppedTime;
            boolean isShadowed = activeDeletion.deletes(cell);
            boolean isSkippable = cellTester != null && (!cellTester.fetches(cell.path())
                                                         || (!cellTester.fetchedCellIsQueried(cell.path()) && cell.timestamp() < rowLiveness.timestamp()));
            return isForDropped || isShadowed || isSkippable ? null : cell;
        });
    }

    public ComplexColumnData purge(DeletionPurger purger, int nowInSec)
    {
        DeletionTime newDeletion = complexDeletion.isLive() || purger.shouldPurge(complexDeletion) ? DeletionTime.LIVE : complexDeletion;
        return transformAndFilter(newDeletion, (cell) -> cell.purge(purger, nowInSec));
    }

    public ComplexColumnData withOnlyQueriedData(ColumnFilter filter)
    {
        return transformAndFilter(complexDeletion, (cell) -> filter.fetchedCellIsQueried(column, cell.path()) ? null : cell);
    }

    private ComplexColumnData transformAndFilter(DeletionTime newDeletion, Function<? super Cell, ? extends Cell> function)
    {
        Object[] transformed = BTree.transformAndFilter(cells, function);

        if (cells == transformed && newDeletion == complexDeletion)
            return this;

        if (newDeletion == DeletionTime.LIVE && BTree.isEmpty(transformed))
            return null;

        return new ComplexColumnData(column, transformed, newDeletion);
    }

    public ComplexColumnData updateAllTimestamp(long newTimestamp)
    {
        DeletionTime newDeletion = complexDeletion.isLive() ? complexDeletion : new DeletionTime(newTimestamp - 1, complexDeletion.localDeletionTime());
        return transformAndFilter(newDeletion, (cell) -> (Cell) cell.updateAllTimestamp(newTimestamp));
    }

    public long maxTimestamp()
    {
        long timestamp = complexDeletion.markedForDeleteAt();
        for (Cell cell : this)
            timestamp = Math.max(timestamp, cell.timestamp());
        return timestamp;
    }

    // This is the partner in crime of ArrayBackedRow.setValue. The exact warning apply. The short
    // version is: "don't use that method".
    void setValue(CellPath path, ByteBuffer value)
    {
        Cell current = (Cell) BTree.<Object>find(cells, column.asymmetricCellPathComparator(), path);
        BTree.replaceInSitu(cells, column.cellComparator(), current, current.withUpdatedValue(value));
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
            && BTree.equals(this.cells, that.cells);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column(), complexDeletion(), BTree.hashCode(cells));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private DeletionTime complexDeletion;
        private ColumnDefinition column;
        private BTree.Builder<Cell> builder;

        public void newColumn(ColumnDefinition column)
        {
            this.column = column;
            this.complexDeletion = DeletionTime.LIVE; // default if writeComplexDeletion is not called
            if (builder == null)
                builder = BTree.builder(column.cellComparator());
            else
                builder.reuse(column.cellComparator());
        }

        public void addComplexDeletion(DeletionTime complexDeletion)
        {
            this.complexDeletion = complexDeletion;
        }

        public void addCell(Cell cell)
        {
            builder.add(cell);
        }

        public ComplexColumnData build()
        {
            if (complexDeletion.isLive() && builder.isEmpty())
                return null;

            return new ComplexColumnData(column, builder.build(), complexDeletion);
        }
    }
}
