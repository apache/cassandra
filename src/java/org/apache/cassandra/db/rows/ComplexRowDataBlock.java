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
import java.util.*;

import com.google.common.collect.UnmodifiableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Holds cells data and complex deletions for the complex columns of one or more rows.
 * <p>
 * Contrarily to {@code SimpleRowDataBlock}, each complex column can have multiple cells and
 * we thus can't use a similar dense encoding. Instead, we still store the actual cell data
 * in a {@code CellData} object, but we add a level of indirection (the cellIdx array in
 * {@link ComplexCellBlock}) which for every column of every row stores 2 indexes: the index
 * in the {@code CellData} where the first cell for this column is, and the the index of the
 * last cell (or rather, the index to the first cell that does not belong to that column).
 * <p>
 * What makes this a little bit more complicated however is that in some cases (for
 * {@link PartitionUpdate} typically), we need to be able to swap rows inside a
 * {@code ComplexRowDataBlock} and the extra level of indirection makes that more complex.
 * So in practice, we have 2 separate sub-implementation of a {@code ComplexRowDataBlock}:
 *   - The first one, {@code SimpleComplexRowDataBlock} does not support swapping rows
 *     (and is thus only used when we don't need to) but it uses a single {@code CellData}
 *     for all the rows stored.
 *   - The second one, {@code SortableComplexRowDataBlock}, uses one separate {@code CellData}
 *     per row (in fact, a {@code ComplexCellBlock} which groups the cell data with the
 *     indexing array discussed above) and simply keeps those per-row block in a list. It
 *     is thus less compact in memory but make the swapping of rows trivial.
 */
public abstract class ComplexRowDataBlock
{
    private static final Logger logger = LoggerFactory.getLogger(ComplexRowDataBlock.class);

    private final Columns columns;

    // For each complex column, it's deletion time (if any): the nth complex column of row i
    // will have it's deletion time at complexDelTimes[(i * ccs) + n] where ccs it the number
    // of complex columns in 'columns'.
    final DeletionTimeArray complexDelTimes;

    protected ComplexRowDataBlock(Columns columns, int rows)
    {
        this.columns = columns;

        int columnCount = rows * columns.complexColumnCount();
        this.complexDelTimes = new DeletionTimeArray(columnCount);
    }

    public static ComplexRowDataBlock create(Columns columns, int rows, boolean sortable, boolean isCounter)
    {
        return sortable
             ? new SortableComplexRowDataBlock(columns, rows, isCounter)
             : new SimpleComplexRowDataBlock(columns, rows, isCounter);
    }

    public Columns columns()
    {
        return columns;
    }

    public CellData cellData(int row)
    {
        return cellBlock(row).data;
    }

    public int cellIdx(int row, ColumnDefinition c, CellPath path)
    {
        ComplexCellBlock block = cellBlock(row);
        if (block == null)
            return -1;

        int base = cellBlockBase(row);
        int i = base + 2 * columns.complexIdx(c, 0);

        int start = block.cellIdx[i];
        int end = block.cellIdx[i+1];

        if (i >= block.cellIdx.length || end <= start)
            return -1;

        return Arrays.binarySearch(block.complexPaths, start, end, path, c.cellPathComparator());
    }

    // The following methods abstract the fact that we have 2 sub-implementations: both
    // implementation will use a ComplexCellBlock to store a row, but one will use one
    // ComplexCellBlock per row, while the other will store all rows into the same block.

    // Returns the cell block for a given row. Can return null if the asked row has no data.
    protected abstract ComplexCellBlock cellBlock(int row);
    // Same as cellBlock(), but create the proper block if the row doesn't exists and return it.
    protected abstract ComplexCellBlock cellBlockForWritting(int row);
    // The index in the block returned by cellBlock()/cellBlockFroWriting() where the row starts.
    protected abstract int cellBlockBase(int row);

    protected abstract void swapCells(int i, int j);
    protected abstract void mergeCells(int i, int j, int nowInSec);
    protected abstract void moveCells(int i, int j);

    protected abstract long cellDataUnsharedHeapSizeExcludingData();
    protected abstract int dataCellSize();
    protected abstract void clearCellData();

    // Swap row i and j
    public void swap(int i, int j)
    {
        swapCells(i, j);

        int s = columns.complexColumnCount();
        for (int k = 0; k < s; k++)
            complexDelTimes.swap(i * s + k, j * s + k);
    }

    // Merge row i into j
    public void merge(int i, int j, int nowInSec)
    {
        assert i > j;

        mergeCells(i, j, nowInSec);

        int s = columns.complexColumnCount();
        if (i * s >= complexDelTimes.size())
            return;

        for (int k = 0; k < s; k++)
            if (complexDelTimes.supersedes(i * s + k, j * s + k))
                complexDelTimes.move(i * s + k, j * s + k);
    }

    // Move row i into j
    public void move(int i, int j)
    {
        moveCells(i, j);
        ensureDelTimesCapacity(Math.max(i, j));
        int s = columns.complexColumnCount();
        for (int k = 0; k < s; k++)
            complexDelTimes.move(i * s + k, j * s + k);
    }

    public long unsharedHeapSizeExcludingData()
    {
        return cellDataUnsharedHeapSizeExcludingData() + complexDelTimes.unsharedHeapSize();
    }

    public int dataSize()
    {
        return dataCellSize() + complexDelTimes.dataSize();
    }

    public CellWriter cellWriter(boolean inOrderCells)
    {
        return new CellWriter(inOrderCells);
    }

    public int complexDeletionIdx(int row, ColumnDefinition column)
    {
        int baseIdx = columns.complexIdx(column, 0);
        if (baseIdx < 0)
            return -1;

        int idx = (row * columns.complexColumnCount()) + baseIdx;
        return idx < complexDelTimes.size() ? idx : -1;
    }

    public boolean hasComplexDeletion(int row)
    {
        int base = row * columns.complexColumnCount();
        for (int i = base; i < base + columns.complexColumnCount(); i++)
            if (!complexDelTimes.isLive(i))
                return true;
        return false;
    }

    public ByteBuffer getValue(int row, ColumnDefinition column, CellPath path)
    {
        CellData data = cellData(row);
        assert data != null;
        int idx = cellIdx(row, column, path);
        return data.value(idx);
    }

    public void setValue(int row, ColumnDefinition column, CellPath path, ByteBuffer value)
    {
        CellData data = cellData(row);
        assert data != null;
        int idx = cellIdx(row, column, path);
        data.setValue(idx, value);
    }

    public static ReusableIterator reusableComplexCells()
    {
        return new ReusableIterator();
    }

    public static DeletionTimeArray.Cursor complexDeletionCursor()
    {
        return new DeletionTimeArray.Cursor();
    }

    public static ReusableIterator reusableIterator()
    {
        return new ReusableIterator();
    }

    public void clear()
    {
        clearCellData();
        complexDelTimes.clear();
    }

    private void ensureDelTimesCapacity(int rowToSet)
    {
        int originalCapacity = complexDelTimes.size() / columns.complexColumnCount();
        if (rowToSet < originalCapacity)
            return;

        int newCapacity = RowDataBlock.computeNewCapacity(originalCapacity, rowToSet);
        complexDelTimes.resize(newCapacity * columns.complexColumnCount());
    }

    /**
     * Simple sub-implementation that doesn't support swapping/sorting rows.
     * The cell data for every row is stored in the same contiguous {@code ComplexCellBloc}
     * object.
     */
    private static class SimpleComplexRowDataBlock extends ComplexRowDataBlock
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new SimpleComplexRowDataBlock(Columns.NONE, 0, false));

        private final ComplexCellBlock cells;

        private SimpleComplexRowDataBlock(Columns columns, int rows, boolean isCounter)
        {
            super(columns, rows);
            this.cells = new ComplexCellBlock(columns, rows, isCounter);
        }

        protected ComplexCellBlock cellBlock(int row)
        {
            return cells;
        }

        protected ComplexCellBlock cellBlockForWritting(int row)
        {
            cells.ensureCapacity(row);
            return cells;
        }

        protected int cellBlockBase(int row)
        {
            return 2 * row * columns().complexColumnCount();
        }

        // Swap cells from row i and j
        public void swapCells(int i, int j)
        {
            throw new UnsupportedOperationException();
        }

        // Merge cells from row i into j
        public void mergeCells(int i, int j, int nowInSec)
        {
            throw new UnsupportedOperationException();
        }

        // Move cells from row i into j
        public void moveCells(int i, int j)
        {
            throw new UnsupportedOperationException();
        }

        protected long cellDataUnsharedHeapSizeExcludingData()
        {
            return EMPTY_SIZE + cells.unsharedHeapSizeExcludingData();
        }

        protected int dataCellSize()
        {
            return cells.dataSize();
        }

        protected void clearCellData()
        {
            cells.clear();
        }
    }

    /**
     * Sub-implementation that support swapping/sorting rows.
     * The data for each row is stored in a different {@code ComplexCellBlock} object,
     * making swapping rows easy.
     */
    private static class SortableComplexRowDataBlock extends ComplexRowDataBlock
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new SortableComplexRowDataBlock(Columns.NONE, 0, false));

        // The cell data for each row.
        private final List<ComplexCellBlock> cells;
        private final boolean isCounter;

        private SortableComplexRowDataBlock(Columns columns, int rows, boolean isCounter)
        {
            super(columns, rows);
            this.cells = new ArrayList<>(rows);
            this.isCounter = isCounter;
        }

        protected ComplexCellBlock cellBlockForWritting(int row)
        {
            if (row < cells.size())
                return cells.get(row);

            // Make sure the list of size 'row-1' before the insertion, adding nulls if necessary,
            // so that we do are writing row 'row'
            ensureCapacity(row-1);

            assert row == cells.size();
            ComplexCellBlock block = new ComplexCellBlock(columns(), 1, isCounter);
            cells.add(block);
            return block;
        }

        private void ensureCapacity(int row)
        {
            while (row >= cells.size())
                cells.add(null);
        }

        protected ComplexCellBlock cellBlock(int row)
        {
            return row >= cells.size() ? null : cells.get(row);
        }

        protected int cellBlockBase(int row)
        {
            return 0;
        }

        // Swap row i and j
        protected void swapCells(int i, int j)
        {
            int max = Math.max(i, j);
            if (max >= cells.size())
                ensureCapacity(max);

            ComplexCellBlock block = cells.get(j);
            move(i, j);
            cells.set(i, block);
        }

        // Merge row i into j
        protected void mergeCells(int i, int j, int nowInSec)
        {
            assert i > j;
            if (i >= cells.size())
                return;

            ComplexCellBlock b1 = cells.get(i);
            if (b1 == null)
                return; // nothing to merge into j

            ComplexCellBlock b2 = cells.get(j);
            if (b2 == null)
            {
                cells.set(j, b1);
                return;
            }

            ComplexCellBlock merged = new ComplexCellBlock(columns(), 1, isCounter);

            int idxMerged = 0;
            int s = columns().complexColumnCount();
            for (int k = 0; k < s; k++)
            {
                ColumnDefinition column = columns().getComplex(k);
                Comparator<CellPath> comparator = column.cellPathComparator();

                merged.cellIdx[2 * k] = idxMerged;

                int idx1 = b1.cellIdx[2 * k];
                int end1 = b1.cellIdx[2 * k + 1];
                int idx2 = b2.cellIdx[2 * k];
                int end2 = b2.cellIdx[2 * k + 1];

                while (idx1 < end1 || idx2 < end2)
                {
                    int cmp = idx1 >= end1 ? 1
                            : (idx2 >= end2 ? -1
                            : comparator.compare(b1.complexPaths[idx1], b2.complexPaths[idx2]));

                    if (cmp == 0)
                        merge(b1, idx1++, b2, idx2++, merged, idxMerged++, nowInSec);
                    else if (cmp < 0)
                        copy(b1, idx1++, merged, idxMerged++);
                    else
                        copy(b2, idx2++, merged, idxMerged++);
                }

                merged.cellIdx[2 * k + 1] = idxMerged;
            }

            cells.set(j, merged);
        }

        private void copy(ComplexCellBlock fromBlock, int fromIdx, ComplexCellBlock toBlock, int toIdx)
        {
            fromBlock.data.moveCell(fromIdx, toBlock.data, toIdx);
            toBlock.ensureComplexPathsCapacity(toIdx);
            toBlock.complexPaths[toIdx] = fromBlock.complexPaths[fromIdx];
        }

        private void merge(ComplexCellBlock b1, int idx1, ComplexCellBlock b2, int idx2, ComplexCellBlock mergedBlock, int mergedIdx, int nowInSec)
        {
            if (isCounter)
                CellData.mergeCounterCell(b1.data, idx1, b2.data, idx2, mergedBlock.data, mergedIdx, nowInSec);
            else
                CellData.mergeRegularCell(b1.data, idx1, b2.data, idx2, mergedBlock.data, mergedIdx, nowInSec);
            mergedBlock.ensureComplexPathsCapacity(mergedIdx);
            mergedBlock.complexPaths[mergedIdx] = b1.complexPaths[idx1];
        }

        // Move row i into j
        protected void moveCells(int i, int j)
        {
            int max = Math.max(i, j);
            if (max >= cells.size())
                ensureCapacity(max);

            cells.set(j, cells.get(i));
        }

        protected long cellDataUnsharedHeapSizeExcludingData()
        {
            long size = EMPTY_SIZE;
            for (ComplexCellBlock block : cells)
                if (block != null)
                    size += block.unsharedHeapSizeExcludingData();
            return size;
        }

        protected int dataCellSize()
        {
            int size = 0;
            for (ComplexCellBlock block : cells)
                if (block != null)
                    size += block.dataSize();
            return size;
        }

        protected void clearCellData()
        {
            for (ComplexCellBlock block : cells)
                if (block != null)
                    block.clear();
        }
    }

    /**
     * Stores complex column cell data for one or more rows.
     * <p>
     * On top of a {@code CellData} object, this stores an index to where the cells
     * of a given column start and stop in that {@code CellData} object (cellIdx)
     * as well as the cell path for the cells (since {@code CellData} doesn't have those).
     */
    private static class ComplexCellBlock
    {
        private final Columns columns;

        /*
         * For a given complex column c, we have to store an unknown number of
         * cells. So for each column of each row, we keep pointers (in data)
         * to the start and end of the cells for this column (cells for a given
         * columns are thus stored contiguously).
         * For instance, if columns has 'c' complex columns, the x-th column of
         * row 'n' will have it's cells in data at indexes
         *    [cellIdx[2 * (n * c + x)], cellIdx[2 * (n * c + x) + 1])
         */
        private int[] cellIdx;

        private final CellData data;

        // The first free idx in data (for writing purposes).
        private int idx;

        // THe (complex) cells path. This is indexed exactly like the cells in data (so through cellIdx).
        private CellPath[] complexPaths;

        public ComplexCellBlock(Columns columns, int rows, boolean isCounter)
        {
            this.columns = columns;

            int columnCount = columns.complexColumnCount();
            this.cellIdx = new int[columnCount * 2 * rows];

            // We start with an estimated 4 cells per complex column. The arrays
            // will grow if needed so this is just a somewhat random estimation.
            int cellCount =  columnCount * 4;
            this.data = new CellData(cellCount, isCounter);
            this.complexPaths = new CellPath[cellCount];
        }

        public void addCell(int columnIdx, ByteBuffer value, LivenessInfo info, CellPath path, boolean isFirstCell)
        {
            if (isFirstCell)
                cellIdx[columnIdx] = idx;
            cellIdx[columnIdx + 1] = idx + 1;

            data.setCell(idx, value, info);
            ensureComplexPathsCapacity(idx);
            complexPaths[idx] = path;
            idx++;
        }

        public long unsharedHeapSizeExcludingData()
        {
            long size = ObjectSizes.sizeOfArray(cellIdx)
                      + data.unsharedHeapSizeExcludingData()
                      + ObjectSizes.sizeOfArray(complexPaths);

            for (int i = 0; i < complexPaths.length; i++)
                if (complexPaths[i] != null)
                    size += ((MemtableRowData.BufferCellPath)complexPaths[i]).unsharedHeapSizeExcludingData();
            return size;
        }

        public int dataSize()
        {
            int size = data.dataSize() + cellIdx.length * 4;

            for (int i = 0; i < complexPaths.length; i++)
                if (complexPaths[i] != null)
                    size += complexPaths[i].dataSize();

            return size;
        }

        private void ensureCapacity(int rowToSet)
        {
            int columnCount = columns.complexColumnCount();
            int originalCapacity = cellIdx.length / (2 * columnCount);
            if (rowToSet < originalCapacity)
                return;

            int newCapacity = RowDataBlock.computeNewCapacity(originalCapacity, rowToSet);
            cellIdx = Arrays.copyOf(cellIdx, newCapacity * 2 * columnCount);
        }

        private void ensureComplexPathsCapacity(int idxToSet)
        {
            int originalCapacity = complexPaths.length;
            if (idxToSet < originalCapacity)
                return;

            int newCapacity = RowDataBlock.computeNewCapacity(originalCapacity, idxToSet);
            complexPaths = Arrays.copyOf(complexPaths, newCapacity);
        }

        public void clear()
        {
            data.clear();
            Arrays.fill(cellIdx, 0);
            Arrays.fill(complexPaths, null);
            idx = 0;
        }
    }

    /**
     * Simple sublcassing of {@code CellData.ReusableCell} to include the cell path.
     */
    private static class ReusableCell extends CellData.ReusableCell
    {
        private ComplexCellBlock cellBlock;

        ReusableCell setTo(ComplexCellBlock cellBlock, ColumnDefinition column, int idx)
        {
            this.cellBlock = cellBlock;
            super.setTo(cellBlock.data, column, idx);
            return this;
        }

        @Override
        public CellPath path()
        {
            return cellBlock.complexPaths[idx];
        }
    }

    /**
     * An iterator over the complex cells of a given row.
     * This is used both to iterate over all the (complex) cells of the row, or only on the cells
     * of a given column within the row.
     */
    static class ReusableIterator extends UnmodifiableIterator<Cell>
    {
        private ComplexCellBlock cellBlock;
        private final ReusableCell cell = new ReusableCell();

        // The idx in 'cellBlock' of the row we're iterating over
        private int rowIdx;

        // columnIdx is the index in 'columns' of the current column we're iterating over.
        // 'endColumnIdx' is the value of 'columnIdx' at which we should stop iterating.
        private int columnIdx;
        private int endColumnIdx;

        // idx is the index in 'cellBlock.data' of the current cell this iterator is on. 'endIdx'
        // is the index in 'cellBlock.data' of the first cell that does not belong to the current
        // column we're iterating over (the one pointed by columnIdx).
        private int idx;
        private int endIdx;

        private ReusableIterator()
        {
        }

        // Sets the iterator for iterating over the cells of 'column' in 'row'
        public ReusableIterator setTo(ComplexRowDataBlock dataBlock, int row, ColumnDefinition column)
        {
            if (dataBlock == null)
            {
                this.cellBlock = null;
                return null;
            }

            this.cellBlock = dataBlock.cellBlock(row);
            if (cellBlock == null)
                return null;

            rowIdx = dataBlock.cellBlockBase(row);

            columnIdx = dataBlock.columns.complexIdx(column, 0);
            if (columnIdx < 0)
                return null;

            // We only want the cells of 'column', so stop as soon as we've reach the next column
            endColumnIdx = columnIdx + 1;

            resetCellIdx();

            return endIdx <= idx ? null : this;
        }

        // Sets the iterator for iterating over all the cells of 'row'
        public ReusableIterator setTo(ComplexRowDataBlock dataBlock, int row)
        {
            if (dataBlock == null)
            {
                this.cellBlock = null;
                return null;
            }

            this.cellBlock = dataBlock.cellBlock(row);
            if (cellBlock == null)
                return null;

            rowIdx = dataBlock.cellBlockBase(row);

            // We want to iterator over all columns
            columnIdx = 0;
            endColumnIdx = dataBlock.columns.complexColumnCount();

            // Not every column might have cells, so set thing up so we're on the
            // column having cells (with idx and endIdx sets properly for that column)
            findNextColumnWithCells();
            return columnIdx < endColumnIdx ? null : this;
        }

        private void findNextColumnWithCells()
        {
            while (columnIdx < endColumnIdx)
            {
                resetCellIdx();
                if (idx < endIdx)
                    return;
                ++columnIdx;
            }
        }

        // Provided that columnIdx and rowIdx are properly set, sets idx to the first
        // cells of the pointed column, and endIdx to the first cell not for said column
        private void resetCellIdx()
        {
            int i = rowIdx + 2 * columnIdx;
            if (i >= cellBlock.cellIdx.length)
            {
                idx = 0;
                endIdx = 0;
            }
            else
            {
                idx = cellBlock.cellIdx[i];
                endIdx = cellBlock.cellIdx[i + 1];
            }
        }

        public boolean hasNext()
        {
            if (cellBlock == null)
                return false;

            if (columnIdx >= endColumnIdx)
                return false;

            // checks if we have more cells for the current column
            if (idx < endIdx)
                return true;

            // otherwise, find the next column that has cells.
            ++columnIdx;
            findNextColumnWithCells();

            return columnIdx < endColumnIdx;
        }

        public Cell next()
        {
            return cell.setTo(cellBlock, cellBlock.columns.getComplex(columnIdx), idx++);
        }
    }

    public class CellWriter
    {
        private final boolean inOrderCells;

        private int base;
        private int row;
        private int lastColumnIdx;

        public CellWriter(boolean inOrderCells)
        {
            this.inOrderCells = inOrderCells;
        }

        public void addCell(ColumnDefinition column, ByteBuffer value, LivenessInfo info, CellPath path)
        {
            assert path != null;

            ComplexCellBlock cellBlock = cellBlockForWritting(row);

            lastColumnIdx = columns.complexIdx(column, inOrderCells ? lastColumnIdx : 0);
            assert lastColumnIdx >= 0 : "Cannot find column " + column.name + " in " + columns;

            int idx = cellBlockBase(row) + 2 * lastColumnIdx;

            int start = cellBlock.cellIdx[idx];
            int end = cellBlock.cellIdx[idx + 1];

            cellBlock.addCell(idx, value, info, path, end <= start);
        }

        public void setComplexDeletion(ColumnDefinition column, DeletionTime deletionTime)
        {
            int columnIdx = base + columns.complexIdx(column, 0);
            ensureDelTimesCapacity(row);
            complexDelTimes.set(columnIdx, deletionTime);
        }

        public void endOfRow()
        {
            base += columns.complexColumnCount();
            lastColumnIdx = 0;
            ++row;
        }

        public void reset()
        {
            base = 0;
            row = 0;
            lastColumnIdx = 0;
            clearCellData();
            complexDelTimes.clear();
        }
    }
}
