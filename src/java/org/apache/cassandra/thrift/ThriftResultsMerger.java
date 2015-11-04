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

import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.utils.AbstractIterator;
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
public class ThriftResultsMerger extends Transformation<UnfilteredRowIterator>
{
    private final int nowInSec;

    private ThriftResultsMerger(int nowInSec)
    {
        this.nowInSec = nowInSec;
    }

    public static UnfilteredPartitionIterator maybeWrap(UnfilteredPartitionIterator iterator, CFMetaData metadata, int nowInSec)
    {
        if (!metadata.isStaticCompactTable() && !metadata.isSuper())
            return iterator;

        return Transformation.apply(iterator, new ThriftResultsMerger(nowInSec));
    }

    public static UnfilteredRowIterator maybeWrap(UnfilteredRowIterator iterator, int nowInSec)
    {
        if (!iterator.metadata().isStaticCompactTable() && !iterator.metadata().isSuper())
            return iterator;

        return iterator.metadata().isSuper()
             ? Transformation.apply(iterator, new SuperColumnsPartitionMerger(iterator, nowInSec))
             : new PartitionMerger(iterator, nowInSec);
    }

    @Override
    public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator iter)
    {
        return iter.metadata().isSuper()
             ? Transformation.apply(iter, new SuperColumnsPartitionMerger(iter, nowInSec))
             : new PartitionMerger(iter, nowInSec);
    }

    private static class PartitionMerger extends WrappingUnfilteredRowIterator
    {
        private final int nowInSec;

        // We initialize lazily to avoid having this iterator fetch the wrapped iterator before it's actually asked for it.
        private boolean isInit;

        private Iterator<Cell> staticCells;

        private final Row.Builder builder;
        private Row nextToMerge;
        private Unfiltered nextFromWrapped;

        private PartitionMerger(UnfilteredRowIterator results, int nowInSec)
        {
            super(results);
            assert results.metadata().isStaticCompactTable();
            this.nowInSec = nowInSec;
            this.builder = BTreeRow.sortedBuilder();
        }

        private void init()
        {
            assert !isInit;
            Row staticRow = super.staticRow();
            assert !staticRow.hasComplex();

            staticCells = staticRow.cells().iterator();
            updateNextToMerge();
            isInit = true;
        }

        @Override
        public Row staticRow()
        {
            return Rows.EMPTY_STATIC_ROW;
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

            // Same row, so merge them
            assert nextFromWrapped instanceof Row;
            return Rows.merge((Row)consumeNextWrapped(), consumeNextToMerge(), nowInSec);
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
            if (!staticCells.hasNext())
            {
                // Nothing more to merge.
                nextToMerge = null;
                return;
            }

            Cell cell = staticCells.next();

            // Given a static cell, the equivalent row uses the column name as clustering and the value as unique cell value.
            builder.newRow(Clustering.make(cell.column().name.bytes));
            builder.addCell(new BufferCell(metadata().compactValueColumn(), cell.timestamp(), cell.ttl(), cell.localDeletionTime(), cell.value(), cell.path()));
            nextToMerge = builder.build();
        }
    }

    private static class SuperColumnsPartitionMerger extends Transformation
    {
        private final int nowInSec;
        private final Row.Builder builder;
        private final ColumnDefinition superColumnMapColumn;
        private final AbstractType<?> columnComparator;

        private SuperColumnsPartitionMerger(UnfilteredRowIterator applyTo, int nowInSec)
        {
            assert applyTo.metadata().isSuper();
            this.nowInSec = nowInSec;

            this.superColumnMapColumn = applyTo.metadata().compactValueColumn();
            assert superColumnMapColumn != null && superColumnMapColumn.type instanceof MapType;

            this.builder = BTreeRow.sortedBuilder();
            this.columnComparator = ((MapType)superColumnMapColumn.type).nameComparator();
        }

        @Override
        public Row applyToRow(Row row)
        {
            PeekingIterator<Cell> staticCells = Iterators.peekingIterator(simpleCellsIterator(row));
            if (!staticCells.hasNext())
                return row;

            builder.newRow(row.clustering());

            ComplexColumnData complexData = row.getComplexColumnData(superColumnMapColumn);
            
            PeekingIterator<Cell> dynamicCells;
            if (complexData == null)
            {
                dynamicCells = Iterators.peekingIterator(Collections.<Cell>emptyIterator());
            }
            else
            {
                dynamicCells = Iterators.peekingIterator(complexData.iterator());
                builder.addComplexDeletion(superColumnMapColumn, complexData.complexDeletion());
            }

            while (staticCells.hasNext() && dynamicCells.hasNext())
            {
                Cell staticCell = staticCells.peek();
                Cell dynamicCell = dynamicCells.peek();
                int cmp = columnComparator.compare(staticCell.column().name.bytes, dynamicCell.path().get(0));
                if (cmp < 0)
                    builder.addCell(makeDynamicCell(staticCells.next()));
                else if (cmp > 0)
                    builder.addCell(dynamicCells.next());
                else
                    builder.addCell(Cells.reconcile(makeDynamicCell(staticCells.next()), dynamicCells.next(), nowInSec));
            }

            while (staticCells.hasNext())
                builder.addCell(makeDynamicCell(staticCells.next()));
            while (dynamicCells.hasNext())
                builder.addCell(dynamicCells.next());

            return builder.build();
        }

        private Cell makeDynamicCell(Cell staticCell)
        {
            return new BufferCell(superColumnMapColumn, staticCell.timestamp(), staticCell.ttl(), staticCell.localDeletionTime(), staticCell.value(), CellPath.create(staticCell.column().name.bytes));
        }

        private Iterator<Cell> simpleCellsIterator(Row row)
        {
            final Iterator<Cell> cells = row.cells().iterator();
            return new AbstractIterator<Cell>()
            {
                protected Cell computeNext()
                {
                    if (cells.hasNext())
                    {
                        Cell cell = cells.next();
                        if (cell.column().isSimple())
                            return cell;
                    }
                    return endOfData();
                }
            };
        }
    }
}

