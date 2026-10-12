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

package org.apache.cassandra.db.compaction;

import java.util.List;

import org.apache.cassandra.db.marshal.AbstractTimeUUIDType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableSimpleScanner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.isState;

/**
 * Puts the sources of a merge group in the order the iterator path gives them to its reducers,
 * where that order decides which source's bytes reach the output.
 *
 * <p>Two values can compare equal and still differ in bytes: decimal 1.0 and 1.00, a varint with
 * extra leading zero bytes, boolean true as 0x01 and 0x02.  When several sources hold such equal
 * values, the iterator path writes the bytes of one source:
 * <ul>
 *   <li>a row: the clustering of the last row {@code Row.Merger} is given;</li>
 *   <li>a range tombstone marker: the bound of the last marker {@code RangeTombstoneMarker.Merger}
 *       is given;</li>
 *   <li>a complex cell: the path of the first cell, of the cells that tie on everything, that
 *       {@code Cells.reconcile} is given.</li>
 * </ul>
 * The order comes from four nested {@code MergeIterator}s: partitions over the scanners, rows over
 * the partition's sources in partition merge order, columns over the row's sources in partition
 * merge order, and the cells of a complex column over that column's sources in column merge
 * order.  This class runs one {@link IteratorMergeOrder} for each, beside the cursor sorts, which
 * still decide the groups.
 *
 * <p>A table whose clustering types and cell path types all compare equal only for equal bytes
 * gets no instance: every order writes the same bytes there.  See {@link #equalValuesHaveEqualBytes}.
 */
final class CursorMergeOrder
{
    /** One scanner of the compaction: the iterator path merges scanners, not sstables. */
    private static final class ScannerSource
    {
        /** The scanner's sstables, in the order the scanner reads them. */
        final StatefulCursor[] cursors;
        int current;

        ScannerSource(StatefulCursor[] cursors)
        {
            this.cursors = cursors;
        }

        StatefulCursor cursor()
        {
            return cursors[current];
        }
    }

    private final boolean orderRows;
    private final boolean orderCells;

    private final IteratorMergeOrder<ScannerSource> partitionOrder;
    private final ScannerSource[] scannerSources;
    private final ScannerSource[] partitionGroupSources;
    /** The sources of the current partition, in partition merge order. */
    private final StatefulCursor[] partitionGroup;
    private int partitionGroupSize;

    private final IteratorMergeOrder<StatefulCursor> rowOrder;
    private final StatefulCursor[] rowGroup;

    /** How many sources the current row has; the cell sorts order the first this many cursors. */
    private int rowGroupSize;

    private final IteratorMergeOrder<StatefulCursor> columnOrder;
    private final StatefulCursor[] columnGroup;
    private final StatefulCursor[] columnSources;
    private ColumnMetadata currentColumn;

    private final IteratorMergeOrder<StatefulCursor> cellOrder;
    private final StatefulCursor[] cellGroup;
    /** The complex column whose cells {@link #cellOrder} merges, or null if it needs no order. */
    private ColumnMetadata orderedCellColumn;

    private CursorMergeOrder(List<ISSTableScanner> scanners, StatefulCursor[] cursorsInScannerOrder, boolean orderRows, boolean orderCells)
    {
        this.orderRows = orderRows;
        this.orderCells = orderCells;
        int cursorCount = cursorsInScannerOrder.length;

        scannerSources = new ScannerSource[scanners.size()];
        int from = 0;
        for (int i = 0; i < scannerSources.length; i++)
        {
            ISSTableScanner scanner = scanners.get(i);
            int count = scanner instanceof SSTableSimpleScanner ? 1 : scanner.getBackingSSTables().size();
            StatefulCursor[] cursors = new StatefulCursor[count];
            System.arraycopy(cursorsInScannerOrder, from, cursors, 0, count);
            scannerSources[i] = new ScannerSource(cursors);
            from += count;
        }
        assert from == cursorCount : "scanners hold " + from + " sstables, but there are " + cursorCount + " cursors";

        partitionOrder = new IteratorMergeOrder<>(scannerSources.length, PARTITION_SOURCES);
        partitionOrder.reset(scannerSources, scannerSources.length);
        partitionGroupSources = new ScannerSource[scannerSources.length];
        partitionGroup = new StatefulCursor[cursorCount];

        rowOrder = new IteratorMergeOrder<>(cursorCount, BY_MERGE_RANK);
        rowGroup = new StatefulCursor[cursorCount];

        columnOrder = new IteratorMergeOrder<>(cursorCount, COLUMN_SOURCES);
        columnGroup = new StatefulCursor[cursorCount];
        columnSources = new StatefulCursor[cursorCount];
        cellOrder = new IteratorMergeOrder<>(cursorCount, new CellSources());
        cellGroup = new StatefulCursor[cursorCount];
    }

    /**
     * @param cursorsInScannerOrder one cursor per sstable, grouped by scanner, in scanner order
     * @return null if no order can change the output bytes of this compaction
     */
    static CursorMergeOrder create(TableMetadata metadata, List<ISSTableScanner> scanners,
                                   Iterable<SSTableReader> sstables, StatefulCursor[] cursorsInScannerOrder)
    {
        boolean orderRows = false;
        for (AbstractType<?> type : metadata.comparator.subtypes())
            orderRows |= !equalValuesHaveEqualBytes(type);

        boolean orderCells = false;
        for (ColumnMetadata column : metadata.regularAndStaticColumns())
            orderCells |= pathBytesMayDiffer(column.type);
        for (SSTableReader sstable : sstables)
        {
            for (AbstractType<?> type : sstable.header.clusteringTypes())
                orderRows |= !equalValuesHaveEqualBytes(type);
            for (ColumnMetadata column : sstable.header.columns())
                orderCells |= pathBytesMayDiffer(sstable.header.getType(column)) || pathBytesMayDiffer(column.type);
        }

        if (!orderRows && !orderCells)
            return null;
        return new CursorMergeOrder(scanners, cursorsInScannerOrder, orderRows, orderCells);
    }

    private static boolean pathBytesMayDiffer(AbstractType<?> type)
    {
        if (type == null || !type.isMultiCell())
            return false;
        AbstractType<?> pathType = ColumnMetadata.pathNameComparator(type);
        return pathType == null || !equalValuesHaveEqualBytes(pathType);
    }

    /**
     * True if two values of {@code type} that compare equal always have the same bytes.  A type
     * not listed here counts as false, which costs the order work but never changes the output.
     */
    static boolean equalValuesHaveEqualBytes(AbstractType<?> type)
    {
        if (type instanceof ReversedType)
            type = ((ReversedType<?>) type).baseType;
        if (type.isByteOrderComparable)
            return true;
        // fixed-length types whose compare reads every byte
        return type instanceof Int32Type
               || type instanceof LongType
               || type instanceof ShortType
               || type instanceof ByteType
               || type instanceof TimestampType
               || type instanceof UUIDType
               || type instanceof AbstractTimeUUIDType
               || type instanceof LexicalUUIDType;
    }

    /**
     * Steps the partition merge.  Call once per partition, after the cursor sort found the
     * partition's group.
     */
    void partitionGroup(int partitionMergeLimit)
    {
        int n = partitionOrder.next(partitionGroupSources);
        assert n == partitionMergeLimit : "partition merge order found " + n + " sources, the cursor sort " + partitionMergeLimit;
        for (int i = 0; i < n; i++)
        {
            StatefulCursor cursor = partitionGroupSources[i].cursor();
            cursor.partitionMergeIndex = i;
            partitionGroup[i] = cursor;
        }
        partitionGroupSize = n;
        if (orderRows && n > 1)
            rowOrder.reset(partitionGroup, n);
    }

    /**
     * Steps the row merge, and puts the source whose clustering the iterator path writes at the
     * front of the group.  Call once per row or marker of the partition.
     */
    void orderUnfilteredGroup(StatefulCursor[] sstableCursors, boolean[] equalsNext, int unfilteredMergeLimit)
    {
        if (!orderRows || partitionGroupSize < 2)
            return;
        // the row sort has ordered every source of the partition
        setMergeRanks(sstableCursors, equalsNext, partitionGroupSize);
        int n = rowOrder.next(rowGroup);
        assert n == unfilteredMergeLimit : "row merge order found " + n + " sources, the cursor sort " + unfilteredMergeLimit;
        // Row.Merger and RangeTombstoneMarker.Merger keep the clustering of the last one added
        moveToFront(sstableCursors, n, rowGroup[n - 1]);
    }

    /**
     * Starts the column merge of a row.  Row.Merger gives its column merge the row's sources in
     * partition merge order; a static row merge gives it every source of the partition.
     */
    void startRowCells(StatefulCursor[] sstableCursors, int rowMergeLimit, boolean isStatic)
    {
        currentColumn = null;
        orderedCellColumn = null;
        rowGroupSize = rowMergeLimit;
        if (!orderCells || partitionGroupSize < 2)
            return;
        if (isStatic)
        {
            columnOrder.reset(partitionGroup, partitionGroupSize);
            return;
        }
        for (int i = 0; i < partitionGroupSize; i++)
            columnSources[i] = null;
        for (int i = 0; i < rowMergeLimit; i++)
            columnSources[sstableCursors[i].partitionMergeIndex] = sstableCursors[i];
        int count = 0;
        for (int i = 0; i < partitionGroupSize; i++)
            if (columnSources[i] != null)
                columnSources[count++] = columnSources[i];
        columnOrder.reset(columnSources, count);
    }

    /**
     * Steps the column merge at each new column, and the cell merge at each cell of a complex
     * column that needs it, and puts the cell group in cell merge order.  Call once per cell
     * group, after the cursor sort, before the winner is picked.
     */
    void orderCellGroup(StatefulCursor[] sstableCursors, boolean[] equalsNext, int cellMergeLimit)
    {
        if (!orderCells || partitionGroupSize < 2)
            return;
        SSTableCursorReader.CellCursor lead = sstableCursors[0].cellCursor();
        if (!ColumnMetadata.sameName(currentColumn, lead.cellColumn))
        {
            currentColumn = lead.cellColumn;
            int n = columnOrder.next(columnGroup);
            assert n > 0 && ColumnMetadata.sameName(columnGroup[0].cellCursor().cellColumn, currentColumn)
                 : "column merge order is not at column " + currentColumn;
            orderedCellColumn = pathBytesMayDiffer(currentColumn.type) ? currentColumn : null;
            if (orderedCellColumn != null)
                cellOrder.reset(columnGroup, n);
        }
        // A position with no cell is a complex deletion alone; it has no path to pick
        if (orderedCellColumn == null || !lead.producedCell)
            return;
        // the cell sort has ordered every source of the row, by column and then path
        setMergeRanks(sstableCursors, equalsNext, rowGroupSize);
        int n = cellOrder.next(cellGroup);
        assert n == cellMergeLimit : "cell merge order found " + n + " sources, the cursor sort " + cellMergeLimit;
        // Cells.reconcile keeps the first of two cells that tie
        System.arraycopy(cellGroup, 0, sstableCursors, 0, n);
    }

    /**
     * Numbers the first {@code n} cursors by their place in the cursor sort, equal ones alike, so
     * the merge orders compare ints instead of values.  A value compare can allocate: decimal
     * builds a BigDecimal for each side.
     */
    private static void setMergeRanks(StatefulCursor[] sstableCursors, boolean[] equalsNext, int n)
    {
        int rank = 0;
        sstableCursors[0].mergeRank = 0;
        for (int i = 1; i < n; i++)
        {
            if (!equalsNext[i - 1])
                rank++;
            sstableCursors[i].mergeRank = rank;
        }
    }

    private static void moveToFront(StatefulCursor[] sstableCursors, int groupSize, StatefulCursor cursor)
    {
        for (int i = 0; i < groupSize; i++)
        {
            if (sstableCursors[i] == cursor)
            {
                sstableCursors[i] = sstableCursors[0];
                sstableCursors[0] = cursor;
                return;
            }
        }
        throw new IllegalStateException("source " + cursor + " is not in the merge group");
    }

    private static boolean atCell(StatefulCursor cursor)
    {
        return isState(cursor.state(), CELL_VALUE_START | CELL_END);
    }

    /** The scanner reads its sstables one after another; it is exhausted after its last one. */
    private static final IteratorMergeOrder.Sources<ScannerSource> PARTITION_SOURCES = new IteratorMergeOrder.Sources<>()
    {
        public int compare(ScannerSource a, ScannerSource b)
        {
            return a.cursor().currentKey().compareTo(b.cursor().currentKey());
        }

        public boolean advance(ScannerSource source)
        {
            while (source.current < source.cursors.length && source.cursors[source.current].state() == DONE)
                source.current++;
            if (source.current < source.cursors.length)
                return true;
            source.current = source.cursors.length - 1;
            return false;
        }
    };

    /** The rows of a partition, compared by their place in the row sort. */
    private static final IteratorMergeOrder.Sources<StatefulCursor> BY_MERGE_RANK = new IteratorMergeOrder.Sources<>()
    {
        public int compare(StatefulCursor a, StatefulCursor b)
        {
            return Integer.compare(a.mergeRank, b.mergeRank);
        }

        public boolean advance(StatefulCursor source)
        {
            return source.state() != PARTITION_END;
        }
    };

    private static final IteratorMergeOrder.Sources<StatefulCursor> COLUMN_SOURCES = new IteratorMergeOrder.Sources<>()
    {
        public int compare(StatefulCursor a, StatefulCursor b)
        {
            return a.cellCursor().cellColumn.compareTo(b.cellCursor().cellColumn);
        }

        public boolean advance(StatefulCursor source)
        {
            return atCell(source);
        }
    };

    private final class CellSources implements IteratorMergeOrder.Sources<StatefulCursor>
    {
        /** Within one column the cell sort orders by path. */
        public int compare(StatefulCursor a, StatefulCursor b)
        {
            return Integer.compare(a.mergeRank, b.mergeRank);
        }

        public boolean advance(StatefulCursor source)
        {
            SSTableCursorReader.CellCursor cell = source.cellCursor();
            return atCell(source) && cell.producedCell && ColumnMetadata.sameName(cell.cellColumn, orderedCellColumn);
        }
    }
}
