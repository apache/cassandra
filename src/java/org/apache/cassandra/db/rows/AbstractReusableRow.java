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

import java.util.Iterator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.SearchIterator;

public abstract class AbstractReusableRow extends AbstractRow
{
    private CellData.ReusableCell simpleCell;
    private ComplexRowDataBlock.ReusableIterator complexCells;
    private DeletionTimeArray.Cursor complexDeletionCursor;
    private RowDataBlock.ReusableIterator iterator;

    public AbstractReusableRow()
    {
    }

    protected abstract int row();
    protected abstract RowDataBlock data();

    private CellData.ReusableCell simpleCell()
    {
        if (simpleCell == null)
            simpleCell = SimpleRowDataBlock.reusableCell();
        return simpleCell;
    }

    private ComplexRowDataBlock.ReusableIterator complexCells()
    {
        if (complexCells == null)
            complexCells = ComplexRowDataBlock.reusableComplexCells();
        return complexCells;
    }

    private DeletionTimeArray.Cursor complexDeletionCursor()
    {
        if (complexDeletionCursor == null)
            complexDeletionCursor = ComplexRowDataBlock.complexDeletionCursor();
        return complexDeletionCursor;
    }

    private RowDataBlock.ReusableIterator reusableIterator()
    {
        if (iterator == null)
            iterator = RowDataBlock.reusableIterator();
        return iterator;
    }

    public Columns columns()
    {
        return data().columns();
    }

    public Cell getCell(ColumnDefinition c)
    {
        assert !c.isComplex();
        if (data().simpleData == null)
            return null;

        int idx = columns().simpleIdx(c, 0);
        if (idx < 0)
            return null;

        return simpleCell().setTo(data().simpleData.data, c, (row() * columns().simpleColumnCount()) + idx);
    }

    public Cell getCell(ColumnDefinition c, CellPath path)
    {
        assert c.isComplex();

        ComplexRowDataBlock data = data().complexData;
        if (data == null)
            return null;

        int idx = data.cellIdx(row(), c, path);
        if (idx < 0)
            return null;

        return simpleCell().setTo(data.cellData(row()), c, idx);
    }

    public Iterator<Cell> getCells(ColumnDefinition c)
    {
        assert c.isComplex();
        return complexCells().setTo(data().complexData, row(), c);
    }

    public boolean hasComplexDeletion()
    {
        return data().hasComplexDeletion(row());
    }

    public DeletionTime getDeletion(ColumnDefinition c)
    {
        assert c.isComplex();
        if (data().complexData == null)
            return DeletionTime.LIVE;

        int idx = data().complexData.complexDeletionIdx(row(), c);
        return idx < 0
             ? DeletionTime.LIVE
             : complexDeletionCursor().setTo(data().complexData.complexDelTimes, idx);
    }

    public Iterator<Cell> iterator()
    {
        return reusableIterator().setTo(data(), row());
    }

    public SearchIterator<ColumnDefinition, ColumnData> searchIterator()
    {
        return new SearchIterator<ColumnDefinition, ColumnData>()
        {
            private int simpleIdx = 0;

            public boolean hasNext()
            {
                // TODO: we can do better, but we expect users to no rely on this anyway
                return true;
            }

            public ColumnData next(ColumnDefinition column)
            {
                if (column.isComplex())
                {
                    // TODO: this is sub-optimal

                    Iterator<Cell> cells = getCells(column);
                    return cells == null ? null : new ColumnData(column, null, cells, getDeletion(column));
                }
                else
                {
                    int idx = columns().simpleIdx(column, simpleIdx);
                    if (idx < 0)
                        return null;

                    Cell cell = simpleCell().setTo(data().simpleData.data, column, (row() * columns().simpleColumnCount()) + idx);
                    simpleIdx = idx + 1;
                    return cell == null ? null : new ColumnData(column, cell, null, null);
                }
            }
        };
    }

    public Row takeAlias()
    {
        final Clustering clustering = clustering().takeAlias();
        final LivenessInfo info = primaryKeyLivenessInfo().takeAlias();
        final DeletionTime deletion = deletion().takeAlias();

        final RowDataBlock data = data();
        final int row = row();

        return new AbstractReusableRow()
        {
            protected RowDataBlock data()
            {
                return data;
            }

            protected int row()
            {
                return row;
            }

            public Clustering clustering()
            {
                return clustering;
            }

            public LivenessInfo primaryKeyLivenessInfo()
            {
                return info;
            }

            public DeletionTime deletion()
            {
                return deletion;
            }

            @Override
            public Row takeAlias()
            {
                return this;
            }
        };
    }
}
