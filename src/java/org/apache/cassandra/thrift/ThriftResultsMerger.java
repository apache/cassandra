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
package org.apache.cassandra.thrift;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.partitions.*;

/**
 * Given an iterator on a partition of a compact table, this return an iterator that merges the
 * static row columns with the other results.
 *
 * Compact tables stores thrift column_metadata as static columns (see CompactTables for
 * details). When reading for thrift however, we want to merge those static values with other
 * results because:
 *   1) on thrift, all "columns" are sorted together, whether or not they are declared
 *      column_metadata.
 *   2) it's possible that a table add a value for a "dynamic" column, and later that column
 *      is statically defined. Merging "static" and "dynamic" columns make sure we don't miss
 *      a value prior to the column declaration.
 *
 * For example, if a thrift table declare 2 columns "c1" and "c5" and the results from a query
 * is:
 *    Partition: static: { c1: 3, c5: 4 }
 *                 "a" : { value : 2 }
 *                 "c3": { value : 8 }
 *                 "c7": { value : 1 }
 * then this class transform it into:
 *    Partition:   "a" : { value : 2 }
 *                 "c1": { value : 3 }
 *                 "c3": { value : 8 }
 *                 "c5": { value : 4 }
 *                 "c7": { value : 1 }
 */
public class ThriftResultsMerger extends WrappingUnfilteredPartitionIterator
{
    private final int nowInSec;

    private ThriftResultsMerger(UnfilteredPartitionIterator wrapped, int nowInSec)
    {
        super(wrapped);
        this.nowInSec = nowInSec;
    }

    public static UnfilteredPartitionIterator maybeWrap(UnfilteredPartitionIterator iterator, CFMetaData metadata, int nowInSec)
    {
        if (!metadata.isStaticCompactTable() && !metadata.isSuper())
            return iterator;

        return new ThriftResultsMerger(iterator, nowInSec);
    }

    public static UnfilteredRowIterator maybeWrap(UnfilteredRowIterator iterator, int nowInSec)
    {
        if (!iterator.metadata().isStaticCompactTable() && !iterator.metadata().isSuper())
            return iterator;

        return iterator.metadata().isSuper()
             ? new SuperColumnsPartitionMerger(iterator, nowInSec)
             : new PartitionMerger(iterator, nowInSec);
    }

    protected UnfilteredRowIterator computeNext(UnfilteredRowIterator iter)
    {
        return iter.metadata().isSuper()
             ? new SuperColumnsPartitionMerger(iter, nowInSec)
             : new PartitionMerger(iter, nowInSec);
    }

    private static class PartitionMerger extends WrappingUnfilteredRowIterator
    {
        private final int nowInSec;

        // We initialize lazily to avoid having this iterator fetch the wrapped iterator before it's actually asked for it.
        private boolean isInit;

        private Row staticRow;
        private int i; // the index of the next column of static row to return

        private ReusableRow nextToMerge;
        private Unfiltered nextFromWrapped;

        private PartitionMerger(UnfilteredRowIterator results, int nowInSec)
        {
            super(results);
            assert results.metadata().isStaticCompactTable();
            this.nowInSec = nowInSec;
        }

        private void init()
        {
            assert !isInit;
            this.staticRow = super.staticRow();
            assert staticRow.columns().complexColumnCount() == 0;

            this.nextToMerge = createReusableRow();
            updateNextToMerge();
            isInit = true;
        }

        @Override
        public Row staticRow()
        {
            return Rows.EMPTY_STATIC_ROW;
        }

        private ReusableRow createReusableRow()
        {
            return new ReusableRow(metadata().clusteringColumns().size(), metadata().partitionColumns().regulars, true, metadata().isCounter());
        }

        @Override
        public boolean hasNext()
        {
            if (!isInit)
                init();

            return nextFromWrapped != null || nextToMerge != null || super.hasNext();
        }

        @Override
        public Unfiltered next()
        {
            if (!isInit)
                init();

            if (nextFromWrapped == null && super.hasNext())
                nextFromWrapped = super.next();

            if (nextFromWrapped == null)
            {
                if (nextToMerge == null)
                    throw new NoSuchElementException();

                return consumeNextToMerge();
            }

            if (nextToMerge == null)
                return consumeNextWrapped();

            int cmp = metadata().comparator.compare(nextToMerge, nextFromWrapped);
            if (cmp < 0)
                return consumeNextToMerge();
            if (cmp > 0)
                return consumeNextWrapped();

            // Same row, but we know the row has only a single column so just pick the more recent
            assert nextFromWrapped instanceof Row;
            ReusableRow row = createReusableRow();
            Rows.merge((Row)consumeNextWrapped(), consumeNextToMerge(), columns().regulars, row.writer(), nowInSec);
            return row;
        }

        private Unfiltered consumeNextWrapped()
        {
            Unfiltered toReturn = nextFromWrapped;
            nextFromWrapped = null;
            return toReturn;
        }

        private Row consumeNextToMerge()
        {
            Row toReturn = nextToMerge;
            updateNextToMerge();
            return toReturn;
        }

        private void updateNextToMerge()
        {
            while (i < staticRow.columns().simpleColumnCount())
            {
                Cell cell = staticRow.getCell(staticRow.columns().getSimple(i++));
                if (cell != null)
                {
                    // Given a static cell, the equivalent row uses the column name as clustering and the
                    // value as unique cell value.
                    Row.Writer writer = nextToMerge.writer();
                    writer.writeClusteringValue(cell.column().name.bytes);
                    writer.writeCell(metadata().compactValueColumn(), cell.isCounterCell(), cell.value(), cell.livenessInfo(), cell.path());
                    writer.endOfRow();
                    return;
                }
            }
            // Nothing more to merge.
            nextToMerge = null;
        }
    }

    private static class SuperColumnsPartitionMerger extends WrappingUnfilteredRowIterator
    {
        private final int nowInSec;
        private final ReusableRow reusableRow;
        private final ColumnDefinition superColumnMapColumn;
        private final AbstractType<?> columnComparator;

        private SuperColumnsPartitionMerger(UnfilteredRowIterator results, int nowInSec)
        {
            super(results);
            assert results.metadata().isSuper();
            this.nowInSec = nowInSec;

            this.superColumnMapColumn = results.metadata().compactValueColumn();
            assert superColumnMapColumn != null && superColumnMapColumn.type instanceof MapType;

            this.reusableRow = new ReusableRow(results.metadata().clusteringColumns().size(),
                                               Columns.of(superColumnMapColumn),
                                               true,
                                               results.metadata().isCounter());
            this.columnComparator = ((MapType)superColumnMapColumn.type).nameComparator();
        }

        @Override
        public Unfiltered next()
        {
            Unfiltered next = super.next();
            if (next.kind() != Unfiltered.Kind.ROW)
                return next;

            Row row = (Row)next;
            Row.Writer writer = reusableRow.writer();
            row.clustering().writeTo(writer);

            PeekingIterator<Cell> staticCells = Iterators.peekingIterator(makeStaticCellIterator(row));
            if (!staticCells.hasNext())
                return row;

            Iterator<Cell> cells = row.getCells(superColumnMapColumn);
            PeekingIterator<Cell> dynamicCells = Iterators.peekingIterator(cells.hasNext() ? cells : Collections.<Cell>emptyIterator());

            while (staticCells.hasNext() && dynamicCells.hasNext())
            {
                Cell staticCell = staticCells.peek();
                Cell dynamicCell = dynamicCells.peek();
                int cmp = columnComparator.compare(staticCell.column().name.bytes, dynamicCell.path().get(0));
                if (cmp < 0)
                {
                    staticCell = staticCells.next();
                    writer.writeCell(superColumnMapColumn, staticCell.isCounterCell(), staticCell.value(), staticCell.livenessInfo(), CellPath.create(staticCell.column().name.bytes));
                }
                else if (cmp > 0)
                {
                    dynamicCells.next().writeTo(writer);
                }
                else
                {
                    staticCell = staticCells.next();
                    Cell toMerge = Cells.create(superColumnMapColumn,
                                                 staticCell.isCounterCell(),
                                                 staticCell.value(),
                                                 staticCell.livenessInfo(),
                                                 CellPath.create(staticCell.column().name.bytes));
                    Cells.reconcile(toMerge, dynamicCells.next(), nowInSec).writeTo(writer);
                }
            }

            while (staticCells.hasNext())
            {
                Cell staticCell = staticCells.next();
                writer.writeCell(superColumnMapColumn, staticCell.isCounterCell(), staticCell.value(), staticCell.livenessInfo(), CellPath.create(staticCell.column().name.bytes));
            }
            while (dynamicCells.hasNext())
            {
                dynamicCells.next().writeTo(writer);
            }

            writer.endOfRow();
            return reusableRow;
        }

        private static Iterator<Cell> makeStaticCellIterator(final Row row)
        {
            return new AbstractIterator<Cell>()
            {
                private int i;

                protected Cell computeNext()
                {
                    while (i < row.columns().simpleColumnCount())
                    {
                        Cell cell = row.getCell(row.columns().getSimple(i++));
                        if (cell != null)
                            return cell;
                    }
                    return endOfData();
                }
            };
        }
    }
}

