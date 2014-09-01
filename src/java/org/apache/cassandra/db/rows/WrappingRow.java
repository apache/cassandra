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

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.collect.UnmodifiableIterator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.SearchIterator;

public abstract class WrappingRow extends AbstractRow
{
    protected Row wrapped;

    private final ReusableIterator cellIterator = new ReusableIterator();
    private final ReusableSearchIterator cellSearchIterator = new ReusableSearchIterator();

    /**
     * Apply some filtering/transformation on cells. This function
     * can return {@code null} in which case the cell will be ignored.
     */
    protected abstract Cell filterCell(Cell cell);

    protected DeletionTime filterDeletionTime(DeletionTime deletionTime)
    {
        return deletionTime;
    }

    protected ColumnData filterColumnData(ColumnData data)
    {
        if (data.column().isComplex())
        {
            Iterator<Cell> cells = cellIterator.setTo(data.cells());
            DeletionTime dt = filterDeletionTime(data.complexDeletion());
            return cells == null && dt.isLive()
                 ? null
                 : new ColumnData(data.column(), null, cells == null ? Collections.emptyIterator(): cells, dt);
        }
        else
        {
            Cell filtered = filterCell(data.cell());
            return filtered == null ? null : new ColumnData(data.column(), filtered, null, null);
        }
    }

    public WrappingRow setTo(Row row)
    {
        this.wrapped = row;
        return this;
    }

    public Unfiltered.Kind kind()
    {
        return Unfiltered.Kind.ROW;
    }

    public Clustering clustering()
    {
        return wrapped.clustering();
    }

    public Columns columns()
    {
        return wrapped.columns();
    }

    public LivenessInfo primaryKeyLivenessInfo()
    {
        return wrapped.primaryKeyLivenessInfo();
    }

    public DeletionTime deletion()
    {
        return wrapped.deletion();
    }

    public boolean hasComplexDeletion()
    {
        // Note that because cells can be filtered out/transformed through
        // filterCell(), we can't rely on wrapped.hasComplexDeletion().
        for (int i = 0; i < columns().complexColumnCount(); i++)
            if (!getDeletion(columns().getComplex(i)).isLive())
                return true;
        return false;
    }

    public Cell getCell(ColumnDefinition c)
    {
        Cell cell = wrapped.getCell(c);
        return cell == null ? null : filterCell(cell);
    }

    public Cell getCell(ColumnDefinition c, CellPath path)
    {
        Cell cell = wrapped.getCell(c, path);
        return cell == null ? null : filterCell(cell);
    }

    public Iterator<Cell> getCells(ColumnDefinition c)
    {
        Iterator<Cell> cells = wrapped.getCells(c);
        if (cells == null)
            return null;

        cellIterator.setTo(cells);
        return cellIterator.hasNext() ? cellIterator : null;
    }

    public DeletionTime getDeletion(ColumnDefinition c)
    {
        return filterDeletionTime(wrapped.getDeletion(c));
    }

    public Iterator<Cell> iterator()
    {
        return cellIterator.setTo(wrapped.iterator());
    }

    public SearchIterator<ColumnDefinition, ColumnData> searchIterator()
    {
        return cellSearchIterator.setTo(wrapped.searchIterator());
    }

    public Row takeAlias()
    {
        boolean isCounter = columns().hasCounters();
        if (isStatic())
        {
            StaticRow.Builder builder = StaticRow.builder(columns(), true, isCounter);
            copyTo(builder);
            return builder.build();
        }
        else
        {
            ReusableRow copy = new ReusableRow(clustering().size(), columns(), true, isCounter);
            copyTo(copy.writer());
            return copy;
        }
    }

    private class ReusableIterator extends UnmodifiableIterator<Cell>
    {
        private Iterator<Cell> iter;
        private Cell next;

        public ReusableIterator setTo(Iterator<Cell> iter)
        {
            this.iter = iter;
            this.next = null;
            return this;
        }

        public boolean hasNext()
        {
            while (next == null && iter.hasNext())
                next = filterCell(iter.next());
            return next != null;
        }

        public Cell next()
        {
            if (next == null && !hasNext())
                throw new NoSuchElementException();

            Cell result = next;
            next = null;
            return result;
        }
    };

    private class ReusableSearchIterator implements SearchIterator<ColumnDefinition, ColumnData>
    {
        private SearchIterator<ColumnDefinition, ColumnData> iter;

        public ReusableSearchIterator setTo(SearchIterator<ColumnDefinition, ColumnData> iter)
        {
            this.iter = iter;
            return this;
        }

        public boolean hasNext()
        {
            return iter.hasNext();
        }

        public ColumnData next(ColumnDefinition column)
        {
            ColumnData data = iter.next(column);
            if (data == null)
                return null;

            return filterColumnData(data);
        }
    };
}
