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
import java.util.function.Predicate;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Immutable implementation of a Row object.
 */
public class ArrayBackedRow extends AbstractRow
{
    private static final ColumnData[] NO_DATA = new ColumnData[0];

    private static final long EMPTY_SIZE = ObjectSizes.measure(new ArrayBackedRow(Clustering.EMPTY, Columns.NONE, LivenessInfo.EMPTY, DeletionTime.LIVE, 0, NO_DATA, Integer.MAX_VALUE));

    private final Clustering clustering;
    private final Columns columns;
    private final LivenessInfo primaryKeyLivenessInfo;
    private final DeletionTime deletion;

    // The data for each columns present in this row in column sorted order.
    private final int size;
    private final ColumnData[] data;

    // We need to filter the tombstones of a row on every read (twice in fact: first to remove purgeable tombstone, and then after reconciliation to remove
    // all tombstone since we don't return them to the client) as well as on compaction. But it's likely that many rows won't have any tombstone at all, so
    // we want to speed up that case by not having to iterate/copy the row in this case. We could keep a single boolean telling us if we have tombstones,
    // but that doesn't work for expiring columns. So instead we keep the deletion time for the first thing in the row to be deleted. This allow at any given
    // time to know if we have any deleted information or not. If we any "true" tombstone (i.e. not an expiring cell), this value will be forced to
    // Integer.MIN_VALUE, but if we don't and have expiring cells, this will the time at which the first expiring cell expires. If we have no tombstones and
    // no expiring cells, this will be Integer.MAX_VALUE;
    private final int minLocalDeletionTime;

    private ArrayBackedRow(Clustering clustering, Columns columns, LivenessInfo primaryKeyLivenessInfo, DeletionTime deletion, int size, ColumnData[] data, int minLocalDeletionTime)
    {
        this.clustering = clustering;
        this.columns = columns;
        this.primaryKeyLivenessInfo = primaryKeyLivenessInfo;
        this.deletion = deletion;
        this.size = size;
        this.data = data;
        this.minLocalDeletionTime = minLocalDeletionTime;
    }

    // Note that it's often easier/safer to use the sortedBuilder/unsortedBuilder or one of the static creation method below. Only directly useful in a small amount of cases.
    public static ArrayBackedRow create(Clustering clustering, Columns columns, LivenessInfo primaryKeyLivenessInfo, DeletionTime deletion, int size, ColumnData[] data)
    {
        int minDeletionTime = Math.min(minDeletionTime(primaryKeyLivenessInfo), minDeletionTime(deletion));
        if (minDeletionTime != Integer.MIN_VALUE)
        {
            for (int i = 0; i < size; i++)
                minDeletionTime = Math.min(minDeletionTime, minDeletionTime(data[i]));
        }

        return new ArrayBackedRow(clustering, columns, primaryKeyLivenessInfo, deletion, size, data, minDeletionTime);
    }

    public static ArrayBackedRow emptyRow(Clustering clustering)
    {
        return new ArrayBackedRow(clustering, Columns.NONE, LivenessInfo.EMPTY, DeletionTime.LIVE, 0, NO_DATA, Integer.MAX_VALUE);
    }

    public static ArrayBackedRow singleCellRow(Clustering clustering, Cell cell)
    {
        if (cell.column().isSimple())
            return new ArrayBackedRow(clustering, Columns.of(cell.column()), LivenessInfo.EMPTY, DeletionTime.LIVE, 1, new ColumnData[]{ cell }, minDeletionTime(cell));

        ComplexColumnData complexData = new ComplexColumnData(cell.column(), new Cell[]{ cell }, DeletionTime.LIVE);
        return new ArrayBackedRow(clustering, Columns.of(cell.column()), LivenessInfo.EMPTY, DeletionTime.LIVE, 1, new ColumnData[]{ complexData }, minDeletionTime(cell));
    }

    public static ArrayBackedRow emptyDeletedRow(Clustering clustering, DeletionTime deletion)
    {
        assert !deletion.isLive();
        return new ArrayBackedRow(clustering, Columns.NONE, LivenessInfo.EMPTY, deletion, 0, NO_DATA, Integer.MIN_VALUE);
    }

    public static ArrayBackedRow noCellLiveRow(Clustering clustering, LivenessInfo primaryKeyLivenessInfo)
    {
        assert !primaryKeyLivenessInfo.isEmpty();
        return new ArrayBackedRow(clustering, Columns.NONE, primaryKeyLivenessInfo, DeletionTime.LIVE, 0, NO_DATA, minDeletionTime(primaryKeyLivenessInfo));
    }

    private static int minDeletionTime(Cell cell)
    {
        return cell.isTombstone() ? Integer.MIN_VALUE : cell.localDeletionTime();
    }

    private static int minDeletionTime(LivenessInfo info)
    {
        return info.isExpiring() ? info.localExpirationTime() : Integer.MAX_VALUE;
    }

    private static int minDeletionTime(DeletionTime dt)
    {
        return dt.isLive() ? Integer.MAX_VALUE : Integer.MIN_VALUE;
    }

    private static int minDeletionTime(ComplexColumnData cd)
    {
        int min = minDeletionTime(cd.complexDeletion());
        for (Cell cell : cd)
            min = Math.min(min, minDeletionTime(cell));
        return min;
    }

    private static int minDeletionTime(ColumnData cd)
    {
        return cd.column().isSimple() ? minDeletionTime((Cell)cd) : minDeletionTime((ComplexColumnData)cd);
    }

    public Clustering clustering()
    {
        return clustering;
    }

    public Columns columns()
    {
        return columns;
    }

    public LivenessInfo primaryKeyLivenessInfo()
    {
        return primaryKeyLivenessInfo;
    }

    public DeletionTime deletion()
    {
        return deletion;
    }

    public Cell getCell(ColumnDefinition c)
    {
        assert !c.isComplex();
        int idx = binarySearch(c);
        return idx < 0 ? null : (Cell)data[idx];
    }

    public Cell getCell(ColumnDefinition c, CellPath path)
    {
        assert c.isComplex();
        int idx = binarySearch(c);
        if (idx < 0)
            return null;

        return ((ComplexColumnData)data[idx]).getCell(path);
    }

    public ComplexColumnData getComplexColumnData(ColumnDefinition c)
    {
        assert c.isComplex();
        int idx = binarySearch(c);
        return idx < 0 ? null : (ComplexColumnData)data[idx];
    }

    public Iterator<ColumnData> iterator()
    {
        return new ColumnDataIterator();
    }

    public Iterable<Cell> cells()
    {
        return CellIterator::new;
    }

    public SearchIterator<ColumnDefinition, ColumnData> searchIterator()
    {
        return new ColumnSearchIterator();
    }

    public Row filter(ColumnFilter filter, CFMetaData metadata)
    {
        return filter(filter, DeletionTime.LIVE, false, metadata);
    }

    public Row filter(ColumnFilter filter, DeletionTime activeDeletion, boolean setActiveDeletionToRow, CFMetaData metadata)
    {
        Map<ByteBuffer, CFMetaData.DroppedColumn> droppedColumns = metadata.getDroppedColumns();

        if (filter.includesAllColumns() && (activeDeletion.isLive() || deletion.supersedes(activeDeletion)) && droppedColumns.isEmpty())
            return this;

        boolean mayHaveShadowed = activeDeletion.supersedes(deletion);

        LivenessInfo newInfo = primaryKeyLivenessInfo;
        DeletionTime newDeletion = deletion;
        if (mayHaveShadowed)
        {
            if (activeDeletion.deletes(newInfo.timestamp()))
                newInfo = LivenessInfo.EMPTY;
            // note that mayHaveShadowed means the activeDeletion shadows the row deletion. So if don't have setActiveDeletionToRow,
            // the row deletion is shadowed and we shouldn't return it.
            newDeletion = setActiveDeletionToRow ? activeDeletion : DeletionTime.LIVE;
        }

        ColumnData[] newData = new ColumnData[size];
        int newMinDeletionTime = Math.min(minDeletionTime(newInfo), minDeletionTime(newDeletion));
        Columns columns = filter.fetchedColumns().columns(isStatic());
        Predicate<ColumnDefinition> inclusionTester = columns.inOrderInclusionTester();
        int newSize = 0;
        for (int i = 0; i < size; i++)
        {
            ColumnData cd = data[i];
            ColumnDefinition column = cd.column();
            if (!inclusionTester.test(column))
                continue;

            CFMetaData.DroppedColumn dropped = droppedColumns.get(column.name.bytes);
            if (column.isSimple())
            {
                Cell cell = (Cell)cd;
                if ((dropped == null || cell.timestamp() > dropped.droppedTime) && !(mayHaveShadowed && activeDeletion.deletes(cell)))
                {
                    newData[newSize++] = cell;
                    newMinDeletionTime = Math.min(newMinDeletionTime, minDeletionTime(cell));
                }
            }
            else
            {
                ColumnData newCd = ((ComplexColumnData)cd).filter(filter, mayHaveShadowed ? activeDeletion : DeletionTime.LIVE, dropped);
                if (newCd != null)
                {
                    newData[newSize++] = newCd;
                    newMinDeletionTime = Math.min(newMinDeletionTime, minDeletionTime(newCd));
                }
            }
        }

        if (newSize == 0 && newInfo.isEmpty() && newDeletion.isLive())
            return null;

        return new ArrayBackedRow(clustering, columns, newInfo, newDeletion, newSize, newData, newMinDeletionTime);
    }

    public boolean hasComplexDeletion()
    {
        // We start by the end cause we know complex columns sort before simple ones
        for (int i = size - 1; i >= 0; i--)
        {
            ColumnData cd = data[i];
            if (cd.column().isSimple())
                return false;

            if (!((ComplexColumnData)cd).complexDeletion().isLive())
                return true;
        }
        return false;
    }

    public Row markCounterLocalToBeCleared()
    {
        ColumnData[] newData = null;
        for (int i = 0; i < size; i++)
        {
            ColumnData cd = data[i];
            ColumnData newCd = cd.column().cellValueType().isCounter()
                             ? cd.markCounterLocalToBeCleared()
                             : cd;
            if (newCd != cd)
            {
                if (newData == null)
                    newData = Arrays.copyOf(data, size);
                newData[i] = newCd;
            }
        }

        return newData == null
             ? this
             : new ArrayBackedRow(clustering, columns, primaryKeyLivenessInfo, deletion, size, newData, minLocalDeletionTime);
    }

    public boolean hasDeletion(int nowInSec)
    {
        return nowInSec >= minLocalDeletionTime;
    }

    /**
     * Returns a copy of the row where all timestamps for live data have replaced by {@code newTimestamp} and
     * all deletion timestamp by {@code newTimestamp - 1}.
     *
     * This exists for the Paxos path, see {@link PartitionUpdate#updateAllTimestamp} for additional details.
     */
    public Row updateAllTimestamp(long newTimestamp)
    {
        LivenessInfo newInfo = primaryKeyLivenessInfo.isEmpty() ? primaryKeyLivenessInfo : primaryKeyLivenessInfo.withUpdatedTimestamp(newTimestamp);
        DeletionTime newDeletion = deletion.isLive() ? deletion : new DeletionTime(newTimestamp - 1, deletion.localDeletionTime());

        ColumnData[] newData = new ColumnData[size];
        for (int i = 0; i < size; i++)
            newData[i] = data[i].updateAllTimestamp(newTimestamp);

        return new ArrayBackedRow(clustering, columns, newInfo, newDeletion, size, newData, minLocalDeletionTime);
    }

    public Row purge(DeletionPurger purger, int nowInSec)
    {
        if (!hasDeletion(nowInSec))
            return this;

        LivenessInfo newInfo = purger.shouldPurge(primaryKeyLivenessInfo, nowInSec) ? LivenessInfo.EMPTY : primaryKeyLivenessInfo;
        DeletionTime newDeletion = purger.shouldPurge(deletion) ? DeletionTime.LIVE : deletion;

        int newMinDeletionTime = Math.min(minDeletionTime(newInfo), minDeletionTime(newDeletion));
        ColumnData[] newData = new ColumnData[size];
        int newSize = 0;
        for (int i = 0; i < size; i++)
        {
            ColumnData purged = data[i].purge(purger, nowInSec);
            if (purged != null)
            {
                newData[newSize++] = purged;
                newMinDeletionTime = Math.min(newMinDeletionTime, minDeletionTime(purged));
            }
        }

        if (newSize == 0 && newInfo.isEmpty() && newDeletion.isLive())
            return null;

        return new ArrayBackedRow(clustering, columns, newInfo, newDeletion, newSize, newData, newMinDeletionTime);
    }

    public int dataSize()
    {
        int dataSize = clustering.dataSize()
                     + primaryKeyLivenessInfo.dataSize()
                     + deletion.dataSize();

        for (int i = 0; i < size; i++)
            dataSize += data[i].dataSize();
        return dataSize;
    }

    public long unsharedHeapSizeExcludingData()
    {
        long heapSize = EMPTY_SIZE
                      + clustering.unsharedHeapSizeExcludingData()
                      + ObjectSizes.sizeOfArray(data);

        for (int i = 0; i < size; i++)
            heapSize += data[i].unsharedHeapSizeExcludingData();
        return heapSize;
    }

    public static Row.Builder sortedBuilder(Columns columns)
    {
        return new SortedBuilder(columns);
    }

    public static Row.Builder unsortedBuilder(Columns columns, int nowInSec)
    {
        return new UnsortedBuilder(columns, nowInSec);
    }

    // This is only used by PartitionUpdate.CounterMark but other uses should be avoided as much as possible as it breaks our general
    // assumption that Row objects are immutable. This method should go away post-#6506 in particular.
    // This method is in particular not exposed by the Row API on purpose.
    // This method also *assumes* that the cell we're setting already exists.
    public void setValue(ColumnDefinition column, CellPath path, ByteBuffer value)
    {
        int idx = binarySearch(column);
        assert idx >= 0;
        if (column.isSimple())
            data[idx] = ((Cell)data[idx]).withUpdatedValue(value);
        else
            ((ComplexColumnData)data[idx]).setValue(path, value);
    }

    private int binarySearch(ColumnDefinition column)
    {
        return binarySearch(column, 0, size);
    }

    /**
     * Simple binary search for a given column (in the data list).
     *
     * The return value has the exact same meaning that the one of Collections.binarySearch() but
     * we don't use the later because we're searching for a 'ColumnDefinition' in an array of 'ColumnData'.
     */
    private int binarySearch(ColumnDefinition column, int fromIndex, int toIndex)
    {
        int low = fromIndex;
        int mid = toIndex;
        int high = mid - 1;
        int result = -1;
        while (low <= high)
        {
            mid = (low + high) >> 1;
            if ((result = column.compareTo(data[mid].column())) > 0)
                low = mid + 1;
            else if (result == 0)
                return mid;
            else
                high = mid - 1;
        }
        return -mid - (result < 0 ? 1 : 2);
    }

    private class ColumnDataIterator extends AbstractIterator<ColumnData>
    {
        private int i;

        protected ColumnData computeNext()
        {
            return i < size ? data[i++] : endOfData();
        }
    }

    private class CellIterator extends AbstractIterator<Cell>
    {
        private int i;
        private Iterator<Cell> complexCells;

        protected Cell computeNext()
        {
            while (true)
            {
                if (complexCells != null)
                {
                    if (complexCells.hasNext())
                        return complexCells.next();

                    complexCells = null;
                }

                if (i >= size)
                    return endOfData();

                ColumnData cd = data[i++];
                if (cd.column().isComplex())
                    complexCells = ((ComplexColumnData)cd).iterator();
                else
                    return (Cell)cd;
            }
        }
    }

    private class ColumnSearchIterator implements SearchIterator<ColumnDefinition, ColumnData>
    {
        // The index at which the next call to "next" should start looking from
        private int searchFrom = 0;

        public boolean hasNext()
        {
            return searchFrom < size;
        }

        public ColumnData next(ColumnDefinition column)
        {
            int idx = binarySearch(column, searchFrom, size);
            if (idx < 0)
            {
                searchFrom = -idx - 1;
                return null;
            }
            else
            {
                // We've found it. We'll start after it next time.
                searchFrom = idx + 1;
                return data[idx];
            }
        }
    }

    private static abstract class AbstractBuilder implements Row.Builder
    {
        protected final Columns columns;

        protected Clustering clustering;
        protected LivenessInfo primaryKeyLivenessInfo;
        protected DeletionTime deletion;

        protected List<Cell> cells = new ArrayList<>();

        // For complex column at index i of 'columns', we store at complexDeletions[i] its complex deletion.
        protected DeletionTime[] complexDeletions;
        protected int columnsWithComplexDeletion;

        protected AbstractBuilder(Columns columns)
        {
            this.columns = columns;
            this.complexDeletions = new DeletionTime[columns.complexColumnCount()];
        }

        public void newRow(Clustering clustering)
        {
            assert this.clustering == null; // Ensures we've properly called build() if we've use this builder before
            this.clustering = clustering;
        }

        public Clustering clustering()
        {
            return clustering;
        }

        protected void reset()
        {
            this.clustering = null;
            this.primaryKeyLivenessInfo = LivenessInfo.EMPTY;
            this.deletion = DeletionTime.LIVE;
            this.cells.clear();
            Arrays.fill(this.complexDeletions, null);
            this.columnsWithComplexDeletion = 0;
        }

        public void addPrimaryKeyLivenessInfo(LivenessInfo info)
        {
            this.primaryKeyLivenessInfo = info;
        }

        public void addRowDeletion(DeletionTime deletion)
        {
            this.deletion = deletion;
        }

        public void addCell(Cell cell)
        {
            assert cell.column().isStatic() == (clustering == Clustering.STATIC_CLUSTERING) : "Column is " + cell.column() + ", clustering = " + clustering;
            cells.add(cell);
        }

        public Row build()
        {
            Row row = buildInternal();
            reset();
            return row;
        }

        protected abstract Row buildInternal();

        protected Row buildNoCells()
        {
            assert cells.isEmpty();
            int minDeletionTime = Math.min(minDeletionTime(primaryKeyLivenessInfo), minDeletionTime(deletion));
            if (columnsWithComplexDeletion == 0)
                return new ArrayBackedRow(clustering, columns, primaryKeyLivenessInfo, deletion, 0, NO_DATA, minDeletionTime);

            ColumnData[] data = new ColumnData[columnsWithComplexDeletion];
            int size = 0;
            for (int i = 0; i < complexDeletions.length; i++)
            {
                DeletionTime complexDeletion = complexDeletions[i];
                if (complexDeletion != null)
                {
                    assert !complexDeletion.isLive();
                    data[size++] = new ComplexColumnData(columns.getComplex(i), ComplexColumnData.NO_CELLS, complexDeletion);
                    minDeletionTime = Integer.MIN_VALUE;
                }
            }
            return new ArrayBackedRow(clustering, columns, primaryKeyLivenessInfo, deletion, size, data, minDeletionTime);
        }
    }

    public static class SortedBuilder extends AbstractBuilder
    {
        private int columnCount;

        private ColumnDefinition column;

        // The index of the last column for which we've called setColumn if complex.
        private int complexColumnIndex;

        // For complex column at index i of 'columns', we store at complexColumnCellsCount[i] its number of added cells.
        private final int[] complexColumnCellsCount;

        protected SortedBuilder(Columns columns)
        {
            super(columns);
            this.complexColumnCellsCount = new int[columns.complexColumnCount()];
            reset();
        }

        @Override
        protected void reset()
        {
            super.reset();
            this.column = null;
            this.columnCount = 0;
            this.complexColumnIndex = -1;
            Arrays.fill(this.complexColumnCellsCount, 0);
        }

        public boolean isSorted()
        {
            return true;
        }

        private void setColumn(ColumnDefinition column)
        {
            int cmp = this.column == null ? -1 : this.column.compareTo(column);
            assert cmp <= 0 : "current = " + this.column + ", new = " + column;
            if (cmp != 0)
            {
                this.column = column;
                ++columnCount;
                if (column.isComplex())
                    complexColumnIndex = columns.complexIdx(column, complexColumnIndex + 1);
            }
        }

        @Override
        public void addCell(Cell cell)
        {
            setColumn(cell.column());
            super.addCell(cell);
            if (column.isComplex())
                complexColumnCellsCount[complexColumnIndex] += 1;
        }

        @Override
        public void addComplexDeletion(ColumnDefinition column, DeletionTime complexDeletion)
        {
            if (complexDeletion.isLive())
                return;

            setColumn(column);
            assert complexDeletions[complexColumnIndex] == null;
            complexDeletions[complexColumnIndex] = complexDeletion;
            ++columnsWithComplexDeletion;
        }

        protected Row buildInternal()
        {
            if (cells.isEmpty())
                return buildNoCells();

            int minDeletionTime = Math.min(minDeletionTime(primaryKeyLivenessInfo), minDeletionTime(deletion));

            ColumnData[] data = new ColumnData[columnCount];
            int complexIdx = 0;
            int i = 0;
            int size = 0;
            while (i < cells.size())
            {
                Cell cell = cells.get(i);
                ColumnDefinition column = cell.column();
                if (column.isSimple())
                {
                    data[size++] = cell;
                    minDeletionTime = Math.min(minDeletionTime, minDeletionTime(cell));
                    ++i;
                }
                else
                {
                    while (columns.getComplex(complexIdx).compareTo(column) < 0)
                    {
                        if (complexDeletions[complexIdx] != null)
                        {
                            data[size++] = new ComplexColumnData(columns.getComplex(complexIdx), ComplexColumnData.NO_CELLS, complexDeletions[complexIdx]);
                            minDeletionTime = Integer.MIN_VALUE;
                        }
                        ++complexIdx;
                    }

                    DeletionTime complexDeletion = complexDeletions[complexIdx];
                    if (complexDeletion != null)
                        minDeletionTime = Integer.MIN_VALUE;
                    int cellCount = complexColumnCellsCount[complexIdx];
                    Cell[] complexCells = new Cell[cellCount];
                    for (int j = 0; j < cellCount; j++)
                    {
                        Cell complexCell = cells.get(i + j);
                        complexCells[j] = complexCell;
                        minDeletionTime = Math.min(minDeletionTime, minDeletionTime(complexCell));
                    }
                    i += cellCount;

                    data[size++] = new ComplexColumnData(column, complexCells, complexDeletion == null ? DeletionTime.LIVE : complexDeletion);
                    ++complexIdx;
                }
            }
            for (int j = complexIdx; j < complexDeletions.length; j++)
            {
                if (complexDeletions[j] != null)
                {
                    data[size++] = new ComplexColumnData(columns.getComplex(j), ComplexColumnData.NO_CELLS, complexDeletions[j]);
                    minDeletionTime = Integer.MIN_VALUE;
                }
            }
            assert size == data.length;
            return new ArrayBackedRow(clustering, columns, primaryKeyLivenessInfo, deletion, size, data, minDeletionTime);
        }
    }

    private static class UnsortedBuilder extends AbstractBuilder
    {
        private final int nowInSec;

        private UnsortedBuilder(Columns columns, int nowInSec)
        {
            super(columns);
            this.nowInSec = nowInSec;
            reset();
        }

        public boolean isSorted()
        {
            return false;
        }

        public void addComplexDeletion(ColumnDefinition column, DeletionTime complexDeletion)
        {
            assert column.isComplex();
            assert column.isStatic() == (clustering == Clustering.STATIC_CLUSTERING);

            if (complexDeletion.isLive())
                return;

            int complexColumnIndex = columns.complexIdx(column, 0);

            DeletionTime previous = complexDeletions[complexColumnIndex];
            if (previous == null || complexDeletion.supersedes(previous))
            {
                complexDeletions[complexColumnIndex] = complexDeletion;
                if (previous == null)
                    ++columnsWithComplexDeletion;
            }
        }

        protected Row buildInternal()
        {
            // First, the easy cases
            if (cells.isEmpty())
                return buildNoCells();

            // Cells have been added in an unsorted way, so sort them first
            Collections.sort(cells, Cell.comparator);

            // We now need to
            //  1) merge equal cells together
            //  2) group the cells for a given complex column together, and include their potential complex deletion time.
            //     And this without forgetting that some complex columns may have a complex deletion but not cells.

            int addedColumns = countAddedColumns();
            ColumnData[] data = new ColumnData[addedColumns];

            int nextComplexWithDeletion = findNextComplexWithDeletion(0);
            ColumnDefinition previousColumn = null;

            int minDeletionTime = Math.min(minDeletionTime(primaryKeyLivenessInfo), minDeletionTime(deletion));

            int i = 0;
            int size = 0;
            while (i < cells.size())
            {
                Cell cell = cells.get(i++);
                ColumnDefinition column = cell.column();
                if (column.isSimple())
                {
                    // Either it's a cell for the same column than our previous cell and we merge them together, or it's a new column
                    if (previousColumn != null && previousColumn.compareTo(column) == 0)
                        data[size - 1] = Cells.reconcile((Cell)data[size - 1], cell, nowInSec);
                    else
                        data[size++] = cell;
                }
                else
                {
                    // First, collect the complex deletion time for the column we got the first complex column of. We'll
                    // also find if there is columns that sorts before but had only a complex deletion and add them.
                    DeletionTime complexDeletion = DeletionTime.LIVE;
                    while (nextComplexWithDeletion >= 0)
                    {
                        int cmp = column.compareTo(columns.getComplex(nextComplexWithDeletion));
                        if (cmp < 0)
                        {
                            // This is after the column we're gonna add cell for. We'll deal with it later
                            break;
                        }
                        else if (cmp > 0)
                        {
                            // We have a column that only has a complex deletion and no column. Add its data first
                            data[size++] = new ComplexColumnData(columns.getComplex(nextComplexWithDeletion), ComplexColumnData.NO_CELLS, complexDeletions[nextComplexWithDeletion]);
                            minDeletionTime = Integer.MIN_VALUE;
                            nextComplexWithDeletion = findNextComplexWithDeletion(nextComplexWithDeletion + 1);
                        }
                        else // cmp == 0
                        {
                            // This is the column we'll about to add cell for. Record the deletion time and break to the cell addition
                            complexDeletion = complexDeletions[nextComplexWithDeletion];
                            minDeletionTime = Integer.MIN_VALUE;
                            nextComplexWithDeletion = findNextComplexWithDeletion(nextComplexWithDeletion + 1);
                            break;
                        }
                    }

                    // Find how many cells the complex column has (cellCount) and the index of the next cell that doesn't belong to it (nextColumnIdx).
                    int nextColumnIdx = i; // i is on cell following the current one
                    int cellCount = 1; // We have at least the current cell
                    Cell previousCell = cell;
                    while (nextColumnIdx < cells.size())
                    {
                        Cell newCell = cells.get(nextColumnIdx);
                        if (column.compareTo(newCell.column()) != 0)
                            break;

                        ++nextColumnIdx;
                        if (column.cellPathComparator().compare(previousCell.path(), newCell.path()) != 0)
                            ++cellCount;
                        previousCell = newCell;
                    }
                    Cell[] columnCells = new Cell[cellCount];
                    int complexSize = 0;
                    columnCells[complexSize++] = cell;
                    previousCell = cell;
                    for (int j = i; j < nextColumnIdx; j++)
                    {
                        Cell newCell = cells.get(j);
                        // Either it's a cell for the same path than our previous cell and we merge them together, or it's a new path
                        if (column.cellPathComparator().compare(previousCell.path(), newCell.path()) == 0)
                            columnCells[complexSize - 1] = Cells.reconcile(previousCell, newCell, nowInSec);
                        else
                            columnCells[complexSize++] = newCell;
                        previousCell = newCell;
                    }
                    i = nextColumnIdx;

                    data[size++] = new ComplexColumnData(column, columnCells, complexDeletion);
                }
                previousColumn = column;
            }
            // We may still have some complex columns with only a complex deletion
            while (nextComplexWithDeletion >= 0)
            {
                data[size++] = new ComplexColumnData(columns.getComplex(nextComplexWithDeletion), ComplexColumnData.NO_CELLS, complexDeletions[nextComplexWithDeletion]);
                nextComplexWithDeletion = findNextComplexWithDeletion(nextComplexWithDeletion + 1);
                minDeletionTime = Integer.MIN_VALUE;
            }
            assert size == addedColumns;

            // Reconciliation made it harder to compute minDeletionTime for cells in the loop above, so just do it now if we need to.
            if (minDeletionTime != Integer.MIN_VALUE)
            {
                for (ColumnData cd : data)
                    minDeletionTime = Math.min(minDeletionTime, minDeletionTime(cd));
            }

            return new ArrayBackedRow(clustering, columns, primaryKeyLivenessInfo, deletion, size, data, minDeletionTime);
        }

        private int findNextComplexWithDeletion(int from)
        {
            for (int i = from; i < complexDeletions.length; i++)
            {
                if (complexDeletions[i] != null)
                    return i;
            }
            return -1;
        }

        // Should only be called once the cells have been sorted
        private int countAddedColumns()
        {
            int columnCount = 0;
            int nextComplexWithDeletion = findNextComplexWithDeletion(0);
            ColumnDefinition previousColumn = null;
            for (Cell cell : cells)
            {
                if (previousColumn != null && previousColumn.compareTo(cell.column()) == 0)
                    continue;

                ++columnCount;
                previousColumn = cell.column();

                // We know that simple columns sort before the complex ones, so don't bother with the column having complex deletion
                // until we've reached the cells of complex columns.
                if (!previousColumn.isComplex())
                    continue;

                while (nextComplexWithDeletion >= 0)
                {
                    // Check how the column we just counted compared to the next with complex deletion
                    int cmp = previousColumn.compareTo(columns.getComplex(nextComplexWithDeletion));
                    if (cmp < 0)
                    {
                        // it's before, we'll handle nextColumnWithComplexDeletion later
                        break;
                    }
                    else if (cmp > 0)
                    {
                        // it's after. nextColumnWithComplexDeletion has no cell but we should count it
                        ++columnCount;
                        nextComplexWithDeletion = findNextComplexWithDeletion(nextComplexWithDeletion + 1);
                    }
                    else // cmp == 0
                    {
                        // it's the column we just counted. Ignore it and we know we're good with nextComplexWithDeletion for this loop
                        nextComplexWithDeletion = findNextComplexWithDeletion(nextComplexWithDeletion + 1);
                        break;
                    }
                }
            }
            // Anything remaining in complexDeletionColumns are complex columns with no cells but some complex deletion
            while (nextComplexWithDeletion >= 0)
            {
                ++columnCount;
                nextComplexWithDeletion = findNextComplexWithDeletion(nextComplexWithDeletion + 1);
            }
            return columnCount;
        }
    }
}
