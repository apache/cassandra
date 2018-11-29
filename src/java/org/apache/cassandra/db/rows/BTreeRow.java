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
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSearchIterator;
import org.apache.cassandra.utils.btree.UpdateFunction;

/**
 * Immutable implementation of a Row object.
 */
public class BTreeRow extends AbstractRow
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(emptyRow(Clustering.EMPTY));

    private final Clustering clustering;
    private final LivenessInfo primaryKeyLivenessInfo;
    private final Deletion deletion;

    // The data for each columns present in this row in column sorted order.
    private final Object[] btree;

    // We need to filter the tombstones of a row on every read (twice in fact: first to remove purgeable tombstone, and then after reconciliation to remove
    // all tombstone since we don't return them to the client) as well as on compaction. But it's likely that many rows won't have any tombstone at all, so
    // we want to speed up that case by not having to iterate/copy the row in this case. We could keep a single boolean telling us if we have tombstones,
    // but that doesn't work for expiring columns. So instead we keep the deletion time for the first thing in the row to be deleted. This allow at any given
    // time to know if we have any deleted information or not. If we any "true" tombstone (i.e. not an expiring cell), this value will be forced to
    // Integer.MIN_VALUE, but if we don't and have expiring cells, this will the time at which the first expiring cell expires. If we have no tombstones and
    // no expiring cells, this will be Integer.MAX_VALUE;
    private final int minLocalDeletionTime;

    private BTreeRow(Clustering clustering,
                     LivenessInfo primaryKeyLivenessInfo,
                     Deletion deletion,
                     Object[] btree,
                     int minLocalDeletionTime)
    {
        assert !deletion.isShadowedBy(primaryKeyLivenessInfo);
        this.clustering = clustering;
        this.primaryKeyLivenessInfo = primaryKeyLivenessInfo;
        this.deletion = deletion;
        this.btree = btree;
        this.minLocalDeletionTime = minLocalDeletionTime;
    }

    private BTreeRow(Clustering clustering, Object[] btree, int minLocalDeletionTime)
    {
        this(clustering, LivenessInfo.EMPTY, Deletion.LIVE, btree, minLocalDeletionTime);
    }

    // Note that it's often easier/safer to use the sortedBuilder/unsortedBuilder or one of the static creation method below. Only directly useful in a small amount of cases.
    public static BTreeRow create(Clustering clustering,
                                  LivenessInfo primaryKeyLivenessInfo,
                                  Deletion deletion,
                                  Object[] btree)
    {
        int minDeletionTime = Math.min(minDeletionTime(primaryKeyLivenessInfo), minDeletionTime(deletion.time()));
        if (minDeletionTime != Integer.MIN_VALUE)
        {
            for (ColumnData cd : BTree.<ColumnData>iterable(btree))
                minDeletionTime = Math.min(minDeletionTime, minDeletionTime(cd));
        }

        return create(clustering, primaryKeyLivenessInfo, deletion, btree, minDeletionTime);
    }

    public static BTreeRow create(Clustering clustering,
                                  LivenessInfo primaryKeyLivenessInfo,
                                  Deletion deletion,
                                  Object[] btree,
                                  int minDeletionTime)
    {
        return new BTreeRow(clustering, primaryKeyLivenessInfo, deletion, btree, minDeletionTime);
    }

    public static BTreeRow emptyRow(Clustering clustering)
    {
        return new BTreeRow(clustering, BTree.empty(), Integer.MAX_VALUE);
    }

    public static BTreeRow singleCellRow(Clustering clustering, Cell cell)
    {
        if (cell.column().isSimple())
            return new BTreeRow(clustering, BTree.singleton(cell), minDeletionTime(cell));

        ComplexColumnData complexData = new ComplexColumnData(cell.column(), new Cell[]{ cell }, DeletionTime.LIVE);
        return new BTreeRow(clustering, BTree.singleton(complexData), minDeletionTime(cell));
    }

    public static BTreeRow emptyDeletedRow(Clustering clustering, Deletion deletion)
    {
        assert !deletion.isLive();
        return new BTreeRow(clustering, LivenessInfo.EMPTY, deletion, BTree.empty(), Integer.MIN_VALUE);
    }

    public static BTreeRow noCellLiveRow(Clustering clustering, LivenessInfo primaryKeyLivenessInfo)
    {
        assert !primaryKeyLivenessInfo.isEmpty();
        return new BTreeRow(clustering,
                            primaryKeyLivenessInfo,
                            Deletion.LIVE,
                            BTree.empty(),
                            minDeletionTime(primaryKeyLivenessInfo));
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
        {
            min = Math.min(min, minDeletionTime(cell));
            if (min == Integer.MIN_VALUE)
                break;
        }
        return min;
    }

    private static int minDeletionTime(ColumnData cd)
    {
        return cd.column().isSimple() ? minDeletionTime((Cell) cd) : minDeletionTime((ComplexColumnData)cd);
    }

    public void apply(Consumer<ColumnData> function, boolean reversed)
    {
        BTree.apply(btree, function, reversed);
    }

    public void apply(Consumer<ColumnData> funtion, com.google.common.base.Predicate<ColumnData> stopCondition, boolean reversed)
    {
        BTree.apply(btree, funtion, stopCondition, reversed);
    }

    private static int minDeletionTime(Object[] btree, LivenessInfo info, DeletionTime rowDeletion)
    {
        //we have to wrap this for the lambda
        final WrappedInt min = new WrappedInt(Math.min(minDeletionTime(info), minDeletionTime(rowDeletion)));

        BTree.<ColumnData>apply(btree, cd -> min.set( Math.min(min.get(), minDeletionTime(cd)) ), cd -> min.get() == Integer.MIN_VALUE, false);
        return min.get();
    }

    public Clustering clustering()
    {
        return clustering;
    }

    public Collection<ColumnDefinition> columns()
    {
        return Collections2.transform(columnData(), ColumnData::column);
    }

    public int columnCount()
    {
        return BTree.size(btree);
    }

    public LivenessInfo primaryKeyLivenessInfo()
    {
        return primaryKeyLivenessInfo;
    }

    public boolean isEmpty()
    {
        return primaryKeyLivenessInfo().isEmpty()
               && deletion().isLive()
               && BTree.isEmpty(btree);
    }

    public Deletion deletion()
    {
        return deletion;
    }

    public Cell getCell(ColumnDefinition c)
    {
        assert !c.isComplex();
        return (Cell) BTree.<Object>find(btree, ColumnDefinition.asymmetricColumnDataComparator, c);
    }

    public Cell getCell(ColumnDefinition c, CellPath path)
    {
        assert c.isComplex();
        ComplexColumnData cd = getComplexColumnData(c);
        if (cd == null)
            return null;
        return cd.getCell(path);
    }

    public ComplexColumnData getComplexColumnData(ColumnDefinition c)
    {
        assert c.isComplex();
        return (ComplexColumnData) BTree.<Object>find(btree, ColumnDefinition.asymmetricColumnDataComparator, c);
    }

    @Override
    public Collection<ColumnData> columnData()
    {
        return new AbstractCollection<ColumnData>()
        {
            @Override public Iterator<ColumnData> iterator() { return BTreeRow.this.iterator(); }
            @Override public int size() { return BTree.size(btree); }
        };
    }

    public Iterator<ColumnData> iterator()
    {
        return searchIterator();
    }

    public Iterable<Cell> cells()
    {
        return CellIterator::new;
    }

    public BTreeSearchIterator<ColumnDefinition, ColumnData> searchIterator()
    {
        return BTree.slice(btree, ColumnDefinition.asymmetricColumnDataComparator, BTree.Dir.ASC);
    }

    public Row filter(ColumnFilter filter, CFMetaData metadata)
    {
        return filter(filter, DeletionTime.LIVE, false, metadata);
    }

    public Row filter(ColumnFilter filter, DeletionTime activeDeletion, boolean setActiveDeletionToRow, CFMetaData metadata)
    {
        Map<ByteBuffer, CFMetaData.DroppedColumn> droppedColumns = metadata.getDroppedColumns();

        boolean mayFilterColumns = !filter.fetchesAllColumns() || !filter.allFetchedColumnsAreQueried();
        boolean mayHaveShadowed = activeDeletion.supersedes(deletion.time());

        if (!mayFilterColumns && !mayHaveShadowed && droppedColumns.isEmpty())
            return this;


        LivenessInfo newInfo = primaryKeyLivenessInfo;
        Deletion newDeletion = deletion;
        if (mayHaveShadowed)
        {
            if (activeDeletion.deletes(newInfo.timestamp()))
                newInfo = LivenessInfo.EMPTY;
            // note that mayHaveShadowed means the activeDeletion shadows the row deletion. So if don't have setActiveDeletionToRow,
            // the row deletion is shadowed and we shouldn't return it.
            newDeletion = setActiveDeletionToRow ? Deletion.regular(activeDeletion) : Deletion.LIVE;
        }

        Columns columns = filter.fetchedColumns().columns(isStatic());
        Predicate<ColumnDefinition> inclusionTester = columns.inOrderInclusionTester();
        Predicate<ColumnDefinition> queriedByUserTester = filter.queriedColumns().columns(isStatic()).inOrderInclusionTester();
        final LivenessInfo rowLiveness = newInfo;
        return transformAndFilter(newInfo, newDeletion, (cd) -> {

            ColumnDefinition column = cd.column();
            if (!inclusionTester.test(column))
                return null;

            CFMetaData.DroppedColumn dropped = droppedColumns.get(column.name.bytes);
            if (column.isComplex())
                return ((ComplexColumnData) cd).filter(filter, mayHaveShadowed ? activeDeletion : DeletionTime.LIVE, dropped, rowLiveness);

            Cell cell = (Cell) cd;
            // We include the cell unless it is 1) shadowed, 2) for a dropped column or 3) skippable.
            // And a cell is skippable if it is for a column that is not queried by the user and its timestamp
            // is lower than the row timestamp (see #10657 or SerializationHelper.includes() for details).
            boolean isForDropped = dropped != null && cell.timestamp() <= dropped.droppedTime;
            boolean isShadowed = mayHaveShadowed && activeDeletion.deletes(cell);
            boolean isSkippable = !queriedByUserTester.test(column) && cell.timestamp() < rowLiveness.timestamp();
            return isForDropped || isShadowed || isSkippable ? null : cell;
        });
    }

    public Row withOnlyQueriedData(ColumnFilter filter)
    {
        if (filter.allFetchedColumnsAreQueried())
            return this;

        return transformAndFilter(primaryKeyLivenessInfo, deletion, (cd) -> {

            ColumnDefinition column = cd.column();
            if (column.isComplex())
                return ((ComplexColumnData)cd).withOnlyQueriedData(filter);

            return filter.fetchedColumnIsQueried(column) ? cd : null;
        });
    }

    public boolean hasComplex()
    {
        // We start by the end cause we know complex columns sort after the simple ones
        ColumnData cd = Iterables.getFirst(BTree.<ColumnData>iterable(btree, BTree.Dir.DESC), null);
        return cd != null && cd.column.isComplex();
    }

    public boolean hasComplexDeletion()
    {
        final WrappedBoolean result = new WrappedBoolean(false);

        // We start by the end cause we know complex columns sort before simple ones
        apply(c -> {}, cd -> {
            if (cd.column.isSimple())
            {
                result.set(false);
                return true;
            }

            if (!((ComplexColumnData) cd).complexDeletion().isLive())
            {
                result.set(true);
                return true;
            }

            return false;
        }, true);

        return result.get();
    }

    public Row markCounterLocalToBeCleared()
    {
        return transformAndFilter(primaryKeyLivenessInfo, deletion, (cd) -> cd.column().isCounterColumn()
                                                                            ? cd.markCounterLocalToBeCleared()
                                                                            : cd);
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
        // If the deletion is shadowable and the row has a timestamp, we'll forced the deletion timestamp to be less than the row one, so we
        // should get rid of said deletion.
        Deletion newDeletion = deletion.isLive() || (deletion.isShadowable() && !primaryKeyLivenessInfo.isEmpty())
                             ? Deletion.LIVE
                             : new Deletion(new DeletionTime(newTimestamp - 1, deletion.time().localDeletionTime()), deletion.isShadowable());

        return transformAndFilter(newInfo, newDeletion, (cd) -> cd.updateAllTimestamp(newTimestamp));
    }

    public Row withRowDeletion(DeletionTime newDeletion)
    {
        // Note that:
        //  - it is a contract with the caller that the new deletion shouldn't shadow anything in
        //    the row, and so in particular it can't shadow the row deletion. So if there is a
        //    already a row deletion we have nothing to do.
        //  - we set the minLocalDeletionTime to MIN_VALUE because we know the deletion is live
        return newDeletion.isLive() || !deletion.isLive()
             ? this
             : new BTreeRow(clustering, primaryKeyLivenessInfo, Deletion.regular(newDeletion), btree, Integer.MIN_VALUE);
    }

    public Row purge(DeletionPurger purger, int nowInSec, boolean enforceStrictLiveness)
    {
        if (!hasDeletion(nowInSec))
            return this;

        LivenessInfo newInfo = purger.shouldPurge(primaryKeyLivenessInfo, nowInSec) ? LivenessInfo.EMPTY : primaryKeyLivenessInfo;
        Deletion newDeletion = purger.shouldPurge(deletion.time()) ? Deletion.LIVE : deletion;

        // when enforceStrictLiveness is set, a row is considered dead when it's PK liveness info is not present
        if (enforceStrictLiveness && newDeletion.isLive() && newInfo.isEmpty())
            return null;

        return transformAndFilter(newInfo, newDeletion, (cd) -> cd.purge(purger, nowInSec));
    }

    private Row transformAndFilter(LivenessInfo info, Deletion deletion, Function<ColumnData, ColumnData> function)
    {
        Object[] transformed = BTree.transformAndFilter(btree, function);

        if (btree == transformed && info == this.primaryKeyLivenessInfo && deletion == this.deletion)
            return this;

        if (info.isEmpty() && deletion.isLive() && BTree.isEmpty(transformed))
            return null;

        int minDeletionTime = minDeletionTime(transformed, info, deletion.time());
        return BTreeRow.create(clustering, info, deletion, transformed, minDeletionTime);
    }

    public int dataSize()
    {
        int dataSize = clustering.dataSize()
                     + primaryKeyLivenessInfo.dataSize()
                     + deletion.dataSize();

        for (ColumnData cd : this)
            dataSize += cd.dataSize();
        return dataSize;
    }

    public long unsharedHeapSizeExcludingData()
    {
        long heapSize = EMPTY_SIZE
                      + clustering.unsharedHeapSizeExcludingData()
                      + BTree.sizeOfStructureOnHeap(btree);

        for (ColumnData cd : this)
            heapSize += cd.unsharedHeapSizeExcludingData();
        return heapSize;
    }

    public static Row.Builder sortedBuilder()
    {
        return new Builder(true);
    }

    public static Row.Builder unsortedBuilder(int nowInSec)
    {
        return new Builder(false, nowInSec);
    }

    // This is only used by PartitionUpdate.CounterMark but other uses should be avoided as much as possible as it breaks our general
    // assumption that Row objects are immutable. This method should go away post-#6506 in particular.
    // This method is in particular not exposed by the Row API on purpose.
    // This method also *assumes* that the cell we're setting already exists.
    public void setValue(ColumnDefinition column, CellPath path, ByteBuffer value)
    {
        ColumnData current = (ColumnData) BTree.<Object>find(btree, ColumnDefinition.asymmetricColumnDataComparator, column);
        if (column.isSimple())
            BTree.replaceInSitu(btree, ColumnData.comparator, current, ((Cell) current).withUpdatedValue(value));
        else
            ((ComplexColumnData) current).setValue(path, value);
    }

    public Iterable<Cell> cellsInLegacyOrder(CFMetaData metadata, boolean reversed)
    {
        return () -> new CellInLegacyOrderIterator(metadata, reversed);
    }

    private class CellIterator extends AbstractIterator<Cell>
    {
        private Iterator<ColumnData> columnData = iterator();
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

                if (!columnData.hasNext())
                    return endOfData();

                ColumnData cd = columnData.next();
                if (cd.column().isComplex())
                    complexCells = ((ComplexColumnData)cd).iterator();
                else
                    return (Cell)cd;
            }
        }
    }

    private class CellInLegacyOrderIterator extends AbstractIterator<Cell>
    {
        private final Comparator<ByteBuffer> comparator;
        private final boolean reversed;
        private final int firstComplexIdx;
        private int simpleIdx;
        private int complexIdx;
        private Iterator<Cell> complexCells;
        private final Object[] data;

        private CellInLegacyOrderIterator(CFMetaData metadata, boolean reversed)
        {
            AbstractType<?> nameComparator = metadata.getColumnDefinitionNameComparator(isStatic() ? ColumnDefinition.Kind.STATIC : ColumnDefinition.Kind.REGULAR);
            this.comparator = reversed ? Collections.reverseOrder(nameComparator) : nameComparator;
            this.reversed = reversed;

            // copy btree into array for simple separate iteration of simple and complex columns
            this.data = new Object[BTree.size(btree)];
            BTree.toArray(btree, data, 0);

            int idx = Iterators.indexOf(Iterators.forArray(data), cd -> cd instanceof ComplexColumnData);
            this.firstComplexIdx = idx < 0 ? data.length : idx;
            this.complexIdx = firstComplexIdx;
        }

        private int getSimpleIdx()
        {
            return reversed ? firstComplexIdx - simpleIdx - 1 : simpleIdx;
        }

        private int getSimpleIdxAndIncrement()
        {
            int idx = getSimpleIdx();
            ++simpleIdx;
            return idx;
        }

        private int getComplexIdx()
        {
            return reversed ? data.length + firstComplexIdx - complexIdx - 1 : complexIdx;
        }

        private int getComplexIdxAndIncrement()
        {
            int idx = getComplexIdx();
            ++complexIdx;
            return idx;
        }

        private Iterator<Cell> makeComplexIterator(Object complexData)
        {
            ComplexColumnData ccd = (ComplexColumnData)complexData;
            return reversed ? ccd.reverseIterator() : ccd.iterator();
        }

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

                if (simpleIdx >= firstComplexIdx)
                {
                    if (complexIdx >= data.length)
                        return endOfData();

                    complexCells = makeComplexIterator(data[getComplexIdxAndIncrement()]);
                }
                else
                {
                    if (complexIdx >= data.length)
                        return (Cell)data[getSimpleIdxAndIncrement()];

                    if (comparator.compare(((ColumnData) data[getSimpleIdx()]).column().name.bytes, ((ColumnData) data[getComplexIdx()]).column().name.bytes) < 0)
                        return (Cell)data[getSimpleIdxAndIncrement()];
                    else
                        complexCells = makeComplexIterator(data[getComplexIdxAndIncrement()]);
                }
            }
        }
    }

    public static class Builder implements Row.Builder
    {
        // a simple marker class that will sort to the beginning of a run of complex cells to store the deletion time
        private static class ComplexColumnDeletion extends BufferCell
        {
            public ComplexColumnDeletion(ColumnDefinition column, DeletionTime deletionTime)
            {
                super(column, deletionTime.markedForDeleteAt(), 0, deletionTime.localDeletionTime(), ByteBufferUtil.EMPTY_BYTE_BUFFER, CellPath.BOTTOM);
            }
        }

        // converts a run of Cell with equal column into a ColumnData
        private static class CellResolver implements BTree.Builder.Resolver
        {
            final int nowInSec;
            private CellResolver(int nowInSec)
            {
                this.nowInSec = nowInSec;
            }

            public ColumnData resolve(Object[] cells, int lb, int ub)
            {
                Cell cell = (Cell) cells[lb];
                ColumnDefinition column = cell.column;
                if (cell.column.isSimple())
                {
                    assert lb + 1 == ub || nowInSec != Integer.MIN_VALUE;
                    while (++lb < ub)
                        cell = Cells.reconcile(cell, (Cell) cells[lb], nowInSec);
                    return cell;
                }

                // TODO: relax this in the case our outer provider is sorted (want to delay until remaining changes are
                // bedded in, as less important; galloping makes it pretty cheap anyway)
                Arrays.sort(cells, lb, ub, (Comparator<Object>) column.cellComparator());
                DeletionTime deletion = DeletionTime.LIVE;
                // Deal with complex deletion (for which we've use "fake" ComplexColumnDeletion cells that we need to remove).
                // Note that in almost all cases we'll at most one of those fake cell, but the contract of {{Row.Builder.addComplexDeletion}}
                // does not forbid it being called twice (especially in the unsorted case) and this can actually happen when reading
                // legacy sstables (see #10743).
                while (lb < ub)
                {
                    cell = (Cell) cells[lb];
                    if (!(cell instanceof ComplexColumnDeletion))
                        break;

                    if (cell.timestamp() > deletion.markedForDeleteAt())
                        deletion = new DeletionTime(cell.timestamp(), cell.localDeletionTime());
                    lb++;
                }

                List<Object> buildFrom = new ArrayList<>(ub - lb);
                Cell previous = null;
                for (int i = lb; i < ub; i++)
                {
                    Cell c = (Cell) cells[i];

                    if (deletion == DeletionTime.LIVE || c.timestamp() >= deletion.markedForDeleteAt())
                    {
                        if (previous != null && column.cellComparator().compare(previous, c) == 0)
                        {
                            c = Cells.reconcile(previous, c, nowInSec);
                            buildFrom.set(buildFrom.size() - 1, c);
                        }
                        else
                        {
                            buildFrom.add(c);
                        }
                        previous = c;
                    }
                }

                Object[] btree = BTree.build(buildFrom, UpdateFunction.noOp());
                return new ComplexColumnData(column, btree, deletion);
            }

        }
        protected Clustering clustering;
        protected LivenessInfo primaryKeyLivenessInfo = LivenessInfo.EMPTY;
        protected Deletion deletion = Deletion.LIVE;

        private final boolean isSorted;
        private BTree.Builder<Cell> cells_;
        private final CellResolver resolver;
        private boolean hasComplex = false;

        // For complex column at index i of 'columns', we store at complexDeletions[i] its complex deletion.

        protected Builder(boolean isSorted)
        {
            this(isSorted, Integer.MIN_VALUE);
        }

        protected Builder(boolean isSorted, int nowInSecs)
        {
            cells_ = null;
            resolver = new CellResolver(nowInSecs);
            this.isSorted = isSorted;
        }

        private BTree.Builder<Cell> getCells()
        {
            if (cells_ == null)
            {
                cells_ = BTree.builder(ColumnData.comparator);
                cells_.auto(false);
            }
            return cells_;
        }

        protected Builder(Builder builder)
        {
            clustering = builder.clustering;
            primaryKeyLivenessInfo = builder.primaryKeyLivenessInfo;
            deletion = builder.deletion;
            cells_ = builder.cells_ == null ? null : builder.cells_.copy();
            resolver = builder.resolver;
            isSorted = builder.isSorted;
            hasComplex = builder.hasComplex;
        }

        @Override
        public Builder copy()
        {
            return new Builder(this);
        }

        public boolean isSorted()
        {
            return isSorted;
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
            this.deletion = Deletion.LIVE;
            this.cells_.reuse();
            this.hasComplex = false;
        }

        public void addPrimaryKeyLivenessInfo(LivenessInfo info)
        {
            // The check is only required for unsorted builders, but it's worth the extra safety to have it unconditional
            if (!deletion.deletes(info))
                this.primaryKeyLivenessInfo = info;
        }

        public void addRowDeletion(Deletion deletion)
        {
            this.deletion = deletion;
            // The check is only required for unsorted builders, but it's worth the extra safety to have it unconditional
            if (deletion.deletes(primaryKeyLivenessInfo))
                this.primaryKeyLivenessInfo = LivenessInfo.EMPTY;
        }

        public void addCell(Cell cell)
        {
            assert cell.column().isStatic() == (clustering == Clustering.STATIC_CLUSTERING) : "Column is " + cell.column() + ", clustering = " + clustering;

            // In practice, only unsorted builder have to deal with shadowed cells, but it doesn't cost us much to deal with it unconditionally in this case
            if (deletion.deletes(cell))
                return;

            getCells().add(cell);
            hasComplex |= cell.column.isComplex();
        }

        public void addComplexDeletion(ColumnDefinition column, DeletionTime complexDeletion)
        {
            getCells().add(new ComplexColumnDeletion(column, complexDeletion));
            hasComplex = true;
        }

        public Row build()
        {
            if (!isSorted)
                getCells().sort();
            // we can avoid resolving if we're sorted and have no complex values
            // (because we'll only have unique simple cells, which are already in their final condition)
            if (!isSorted | hasComplex)
                getCells().resolve(resolver);
            Object[] btree = getCells().build();

            if (deletion.isShadowedBy(primaryKeyLivenessInfo))
                deletion = Deletion.LIVE;

            int minDeletionTime = minDeletionTime(btree, primaryKeyLivenessInfo, deletion.time());
            Row row = BTreeRow.create(clustering, primaryKeyLivenessInfo, deletion, btree, minDeletionTime);
            reset();
            return row;
        }
    }
}
