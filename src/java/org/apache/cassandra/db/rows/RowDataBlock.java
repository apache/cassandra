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
 * A {@code RowDataBlock} holds data for one or more row (of a given table). More precisely, it contains
 * cell data  and complex deletion data (for complex columns) and allow access to this data. Please note
 * however that {@code RowDataBlock} only holds the data inside the row, it does not hold the data
 * pertaining to the row itself: clustering, partition key liveness info and row deletion.
 * <p>
 * {@code RowDataBlock} is largely an implementation detail: it is only there to be reused by
 * {@link AbstractPartitionData} and every concrete row implementation.
 */
public class RowDataBlock
{
    private static final Logger logger = LoggerFactory.getLogger(RowDataBlock.class);

    private static final long EMPTY_SIZE = ObjectSizes.measure(new RowDataBlock(Columns.NONE, 0, false, false));

    // We distinguish 2 sub-objects: SimpleRowDataBlock that contains the data for the simple columns only,
    // and ComplexRowDataBlock that only contains data for complex columns. The reason for having 2 separate
    // objects is that simple columns are much easier to handle since we have only a single cell per-object
    // and thus having a more specialized object allow a simpler and more efficient handling.
    final SimpleRowDataBlock simpleData;
    final ComplexRowDataBlock complexData;

    public RowDataBlock(Columns columns, int rows, boolean sortable, boolean isCounter)
    {
        this.simpleData = columns.hasSimple() ? new SimpleRowDataBlock(columns, rows, isCounter) : null;
        this.complexData = columns.hasComplex() ? ComplexRowDataBlock.create(columns, rows, sortable, isCounter) : null;
    }

    public Columns columns()
    {
        if (simpleData != null)
            return simpleData.columns();
        if (complexData != null)
            return complexData.columns();
        return Columns.NONE;
    }

    /**
     * Return the cell value for a given column of a given row.
     *
     * @param row the row for which to return the cell value.
     * @param column the column for which to return the cell value.
     * @param path the cell path for which to return the cell value. Can be null for
     * simple columns.
     *
     * @return the value of the cell of path {@code path} for {@code column} in row {@code row}, or
     * {@code null} if their is no such cell.
     */
    public ByteBuffer getValue(int row, ColumnDefinition column, CellPath path)
    {
        if (column.isComplex())
        {
            return complexData.getValue(row, column, path);
        }
        else
        {
            int idx = columns().simpleIdx(column, 0);
            assert idx >= 0;
            return simpleData.data.value((row * columns().simpleColumnCount()) + idx);
        }
    }

    /**
     * Sets the cell value for a given simple column of a given row.
     *
     * @param row the row for which to set the cell value.
     * @param column the simple column for which to set the cell value.
     * @param path the cell path for which to return the cell value. Can be null for
     * simple columns.
     * @param value the value to set.
     */
    public void setValue(int row, ColumnDefinition column, CellPath path, ByteBuffer value)
    {
        if (column.isComplex())
        {
            complexData.setValue(row, column, path, value);
        }
        else
        {
            int idx = columns().simpleIdx(column, 0);
            assert idx >= 0;
            simpleData.data.setValue((row * columns().simpleColumnCount()) + idx, value);
        }
    }

    public static ReusableIterator reusableIterator()
    {
        return new ReusableIterator();
    }

    // Swap row i and j
    public void swap(int i, int j)
    {
        if (simpleData != null)
            simpleData.swap(i, j);
        if (complexData != null)
            complexData.swap(i, j);
    }

    // Merge row i into j
    public void merge(int i, int j, int nowInSec)
    {
        if (simpleData != null)
            simpleData.merge(i, j, nowInSec);
        if (complexData != null)
            complexData.merge(i, j, nowInSec);
    }

    // Move row i into j
    public void move(int i, int j)
    {
        if (simpleData != null)
            simpleData.move(i, j);
        if (complexData != null)
            complexData.move(i, j);
    }

    public boolean hasComplexDeletion(int row)
    {
        return complexData != null && complexData.hasComplexDeletion(row);
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE
             + (simpleData == null ? 0 : simpleData.unsharedHeapSizeExcludingData())
             + (complexData == null ? 0 : complexData.unsharedHeapSizeExcludingData());
    }

    public static int computeNewCapacity(int currentCapacity, int idxToSet)
    {
        int newCapacity = currentCapacity == 0 ? 4 : currentCapacity;
        while (idxToSet >= newCapacity)
            newCapacity = 1 + (newCapacity * 3) / 2;
        return newCapacity;
    }

    public int dataSize()
    {
        return (simpleData == null ? 0 : simpleData.dataSize())
             + (complexData == null ? 0 : complexData.dataSize());
    }

    public void clear()
    {
        if (simpleData != null)
            simpleData.clear();
        if (complexData != null)
            complexData.clear();
    }

    public abstract static class Writer implements Row.Writer
    {
        private final boolean inOrderCells;

        protected int row;

        protected SimpleRowDataBlock.CellWriter simpleWriter;
        protected ComplexRowDataBlock.CellWriter complexWriter;

        protected Writer(boolean inOrderCells)
        {
            this.inOrderCells = inOrderCells;
        }

        protected Writer(RowDataBlock data, boolean inOrderCells)
        {
            this(inOrderCells);
            updateWriter(data);
        }

        protected void updateWriter(RowDataBlock data)
        {
            this.simpleWriter = data.simpleData == null ? null : data.simpleData.cellWriter(inOrderCells);
            this.complexWriter = data.complexData == null ? null : data.complexData.cellWriter(inOrderCells);
        }

        public Writer reset()
        {
            row = 0;

            if (simpleWriter != null)
                simpleWriter.reset();
            if (complexWriter != null)
                complexWriter.reset();

            return this;
        }

        public void writeCell(ColumnDefinition column, boolean isCounter, ByteBuffer value, LivenessInfo info, CellPath path)
        {
            if (column.isComplex())
                complexWriter.addCell(column, value, info, path);
            else
                simpleWriter.addCell(column, value, info);
        }

        public void writeComplexDeletion(ColumnDefinition c, DeletionTime complexDeletion)
        {
            if (complexDeletion.isLive())
                return;

            complexWriter.setComplexDeletion(c, complexDeletion);
        }

        public void endOfRow()
        {
            ++row;
            if (simpleWriter != null)
                simpleWriter.endOfRow();
            if (complexWriter != null)
                complexWriter.endOfRow();
        }
    }

    static class ReusableIterator extends UnmodifiableIterator<Cell> implements Iterator<Cell>
    {
        private SimpleRowDataBlock.ReusableIterator simpleIterator;
        private ComplexRowDataBlock.ReusableIterator complexIterator;

        public ReusableIterator()
        {
            this.simpleIterator = SimpleRowDataBlock.reusableIterator();
            this.complexIterator = ComplexRowDataBlock.reusableIterator();
        }

        public ReusableIterator setTo(RowDataBlock dataBlock, int row)
        {
            simpleIterator.setTo(dataBlock.simpleData, row);
            complexIterator.setTo(dataBlock.complexData, row);
            return this;
        }

        public boolean hasNext()
        {
            return simpleIterator.hasNext() || complexIterator.hasNext();
        }

        public Cell next()
        {
            return simpleIterator.hasNext() ? simpleIterator.next() : complexIterator.next();
        }
    }
}
