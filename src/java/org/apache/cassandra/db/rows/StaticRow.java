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
import java.util.Iterator;

import org.apache.cassandra.db.*;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.utils.SearchIterator;

public class StaticRow extends AbstractRow
{
    private final DeletionTime deletion;
    private final RowDataBlock data;

    private StaticRow(DeletionTime deletion, RowDataBlock data)
    {
        this.deletion = deletion.takeAlias();
        this.data = data;
    }

    public Columns columns()
    {
        return data.columns();
    }

    public Cell getCell(ColumnDefinition c)
    {
        assert !c.isComplex();
        if (data.simpleData == null)
            return null;

        int idx = columns().simpleIdx(c, 0);
        if (idx < 0)
            return null;

        return SimpleRowDataBlock.reusableCell().setTo(data.simpleData.data, c, idx);
    }

    public Cell getCell(ColumnDefinition c, CellPath path)
    {
        assert c.isComplex();

        ComplexRowDataBlock dataBlock = data.complexData;
        if (dataBlock == null)
            return null;

        int idx = dataBlock.cellIdx(0, c, path);
        if (idx < 0)
            return null;

        return SimpleRowDataBlock.reusableCell().setTo(dataBlock.cellData(0), c, idx);
    }

    public Iterator<Cell> getCells(ColumnDefinition c)
    {
        assert c.isComplex();
        return ComplexRowDataBlock.reusableComplexCells().setTo(data.complexData, 0, c);
    }

    public boolean hasComplexDeletion()
    {
        return data.hasComplexDeletion(0);
    }

    public DeletionTime getDeletion(ColumnDefinition c)
    {
        assert c.isComplex();
        if (data.complexData == null)
            return DeletionTime.LIVE;

        int idx = data.complexData.complexDeletionIdx(0, c);
        return idx < 0
             ? DeletionTime.LIVE
             : ComplexRowDataBlock.complexDeletionCursor().setTo(data.complexData.complexDelTimes, idx);
    }

    public Iterator<Cell> iterator()
    {
        return RowDataBlock.reusableIterator().setTo(data, 0);
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
                    simpleIdx = columns().simpleIdx(column, simpleIdx);
                    assert simpleIdx >= 0;

                    Cell cell = SimpleRowDataBlock.reusableCell().setTo(data.simpleData.data, column, simpleIdx);
                    ++simpleIdx;
                    return cell == null ? null : new ColumnData(column, cell, null, null);
                }
            }
        };
    }

    public Row takeAlias()
    {
        return this;
    }

    public Clustering clustering()
    {
        return Clustering.STATIC_CLUSTERING;
    }

    public LivenessInfo primaryKeyLivenessInfo()
    {
        return LivenessInfo.NONE;
    }

    public DeletionTime deletion()
    {
        return deletion;
    }

    public static Builder builder(Columns columns, boolean inOrderCells, boolean isCounter)
    {
        return new Builder(columns, inOrderCells, isCounter);
    }

    public static class Builder extends RowDataBlock.Writer
    {
        private final RowDataBlock data;
        private DeletionTime deletion = DeletionTime.LIVE;

        public Builder(Columns columns, boolean inOrderCells, boolean isCounter)
        {
            super(inOrderCells);
            this.data = new RowDataBlock(columns, 1, false, isCounter);
            updateWriter(data);
        }

        public void writeClusteringValue(ByteBuffer buffer)
        {
            throw new UnsupportedOperationException();
        }

        public void writePartitionKeyLivenessInfo(LivenessInfo info)
        {
            // Static rows are special and don't really have an existence unless they have live cells,
            // so we shouldn't have any partition key liveness info.
            assert info.equals(LivenessInfo.NONE);
        }

        public void writeRowDeletion(DeletionTime deletion)
        {
            this.deletion = deletion;
        }

        public StaticRow build()
        {
            return new StaticRow(deletion, data);
        }
    }
}
