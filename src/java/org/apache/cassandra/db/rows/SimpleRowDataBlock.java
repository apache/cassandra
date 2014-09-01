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

import com.google.common.collect.UnmodifiableIterator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Holds cells data for the simple columns of one or more rows.
 * <p>
 * In practice, a {@code SimpleRowDataBlock} contains a single {@code CellData} "array" and
 * the (simple) columns for which the {@code SimplerowDataBlock} has data for. The cell for
 * a row i and a column c is stored in the {@code CellData} at index 'i * index(c)'.
 * <p>
 * This  does mean that we store cells in a "dense" way: if column doesn't have a cell for a
 * given row, the correspond index in the cell data array will simple have a {@code null} value.
 * We might want to switch to a more sparse encoding in the future but we keep it simple for
 * now (having a sparse encoding make things a tad more complex because we need to be able to
 * swap the cells for 2 given rows as seen in ComplexRowDataBlock).
 */
public class SimpleRowDataBlock
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new SimpleRowDataBlock(Columns.NONE, 0, false));

    final Columns columns;
    final CellData data;

    public SimpleRowDataBlock(Columns columns, int rows, boolean isCounter)
    {
        this.columns = columns;
        this.data = new CellData(rows * columns.simpleColumnCount(), isCounter);
    }

    public Columns columns()
    {
        return columns;
    }

    // Swap row i and j
    public void swap(int i, int j)
    {
        int s = columns.simpleColumnCount();
        for (int k = 0; k < s; k++)
            data.swapCell(i * s + k, j * s + k);
    }

    // Merge row i into j
    public void merge(int i, int j, int nowInSec)
    {
        int s = columns.simpleColumnCount();
        for (int k = 0; k < s; k++)
            data.mergeCell(i * s + k, j * s + k, nowInSec);
    }

    // Move row i into j
    public void move(int i, int j)
    {
        int s = columns.simpleColumnCount();
        for (int k = 0; k < s; k++)
            data.moveCell(i * s + k, j * s + k);
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE + data.unsharedHeapSizeExcludingData();
    }

    public int dataSize()
    {
        return data.dataSize();
    }

    public CellWriter cellWriter(boolean inOrderCells)
    {
        return new CellWriter(inOrderCells);
    }

    public static CellData.ReusableCell reusableCell()
    {
        return new CellData.ReusableCell();
    }

    public static ReusableIterator reusableIterator()
    {
        return new ReusableIterator();
    }

    public void clear()
    {
        data.clear();
    }

    static class ReusableIterator extends UnmodifiableIterator<Cell>
    {
        private SimpleRowDataBlock dataBlock;
        private final CellData.ReusableCell cell = new CellData.ReusableCell();

        private int base;
        private int column;

        private ReusableIterator()
        {
        }

        public ReusableIterator setTo(SimpleRowDataBlock dataBlock, int row)
        {
            this.dataBlock = dataBlock;
            this.base = dataBlock == null ? -1 : row * dataBlock.columns.simpleColumnCount();
            this.column = 0;
            return this;
        }

        public boolean hasNext()
        {
            if (dataBlock == null)
                return false;

            int columnCount = dataBlock.columns.simpleColumnCount();
            // iterate over column until we find one with data
            while (column < columnCount && !dataBlock.data.hasCell(base + column))
                ++column;

            return column < columnCount;
        }

        public Cell next()
        {
            cell.setTo(dataBlock.data, dataBlock.columns.getSimple(column), base + column);
            ++column;
            return cell;
        }
    }

    public class CellWriter
    {
        private final boolean inOrderCells;

        private int base;
        private int lastColumnIdx;

        public CellWriter(boolean inOrderCells)
        {
            this.inOrderCells = inOrderCells;
        }

        public void addCell(ColumnDefinition column, ByteBuffer value, LivenessInfo info)
        {
            int fromIdx = inOrderCells ? lastColumnIdx : 0;
            lastColumnIdx = columns.simpleIdx(column, fromIdx);
            assert lastColumnIdx >= 0 : "Cannot find column " + column.name + " in " + columns + " from " + fromIdx;
            int idx = base + lastColumnIdx;
            data.setCell(idx, value, info);
        }

        public void reset()
        {
            base = 0;
            lastColumnIdx = 0;
            data.clear();
        }

        public void endOfRow()
        {
            base += columns.simpleColumnCount();
            lastColumnIdx = 0;
        }
    }
}
