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

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.DroppedColumn;

import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.BiLongAccumulator;
import org.apache.cassandra.utils.BulkIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.LongAccumulator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSearchIterator;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.memory.Cloner;

/**
 * Immutable implementation of a Row object.
 */
public class BTreeRow extends AbstractRow
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(emptyRow(Clustering.EMPTY));

    private final Clustering<?> clustering;
    private final LivenessInfo primaryKeyLivenessInfo;
    private final Deletion deletion;

    // The data for each columns present in this row in column sorted order.
    private final Object[] btree;

    private static final ColumnData FIRST_COMPLEX_STATIC = new ComplexColumnData(Columns.FIRST_COMPLEX_STATIC, new Object[0], DeletionTime.build(0, 0));
    private static final ColumnData FIRST_COMPLEX_REGULAR = new ComplexColumnData(Columns.FIRST_COMPLEX_REGULAR, new Object[0], DeletionTime.build(0, 0));
    private static final Comparator<ColumnData> COLUMN_COMPARATOR = (cd1, cd2) -> cd1.column.compareTo(cd2.column);


    // We need to filter the tombstones of a row on every read (twice in fact: first to remove purgeable tombstone, and then after reconciliation to remove
    // all tombstone since we don't return them to the client) as well as on compaction. But it's likely that many rows won't have any tombstone at all, so
    // we want to speed up that case by not having to iterate/copy the row in this case. We could keep a single boolean telling us if we have tombstones,
    // but that doesn't work for expiring columns. So instead we keep the deletion time for the first thing in the row to be deleted. This allow at any given
    // time to know if we have any deleted information or not. If we any "true" tombstone (i.e. not an expiring cell), this value will be forced to
    // Long.MIN_VALUE, but if we don't and have expiring cells, this will the time at which the first expiring cell expires. If we have no tombstones and
    // no expiring cells, this will be Cell.MAX_DELETION_TIME;
    private final long minLocalDeletionTime;

    private BTreeRow(Clustering clustering,
                     LivenessInfo primaryKeyLivenessInfo,
                     Deletion deletion,
                     Object[] btree,
                     long minLocalDeletionTime)
    {
        assert !deletion.isShadowedBy(primaryKeyLivenessInfo);
        this.clustering = clustering;
        this.primaryKeyLivenessInfo = primaryKeyLivenessInfo;
        this.deletion = deletion;
        this.btree = btree;
        this.minLocalDeletionTime = minLocalDeletionTime;
    }

    private BTreeRow(Clustering<?> clustering, Object[] btree, long minLocalDeletionTime)
    {
        this(clustering, LivenessInfo.EMPTY, Deletion.LIVE, btree, minLocalDeletionTime);
    }

    // Note that it's often easier/safer to use the sortedBuilder/unsortedBuilder or one of the static creation method below. Only directly useful in a small amount of cases.
    public static BTreeRow create(Clustering<?> clustering,
                                  LivenessInfo primaryKeyLivenessInfo,
                                  Deletion deletion,
                                  Object[] btree)
    {
        long minDeletionTime = Math.min(minDeletionTime(primaryKeyLivenessInfo), minDeletionTime(deletion.time()));
        if (minDeletionTime != Long.MIN_VALUE)
        {
            long result = BTree.<ColumnData>accumulate(btree, (cd, l) -> Math.min(l, minDeletionTime(cd)) , minDeletionTime);
            minDeletionTime = result;
        }

        return create(clustering, primaryKeyLivenessInfo, deletion, btree, minDeletionTime);
    }

    public static BTreeRow create(Clustering<?> clustering,
                                  LivenessInfo primaryKeyLivenessInfo,
                                  Deletion deletion,
                                  Object[] btree,
                                  long minDeletionTime)
    {
        return new BTreeRow(clustering, primaryKeyLivenessInfo, deletion, btree, minDeletionTime);
    }

    public static BTreeRow emptyRow(Clustering<?> clustering)
    {
        return new BTreeRow(clustering, BTree.empty(), Cell.MAX_DELETION_TIME);
    }

    public static BTreeRow singleCellRow(Clustering<?> clustering, Cell<?> cell)
    {
        if (cell.column().isSimple())
            return new BTreeRow(clustering, BTree.singleton(cell), minDeletionTime(cell));

        ComplexColumnData complexData = new ComplexColumnData(cell.column(), new Cell<?>[]{ cell }, DeletionTime.LIVE);
        return new BTreeRow(clustering, BTree.singleton(complexData), minDeletionTime(cell));
    }

    public static BTreeRow emptyDeletedRow(Clustering<?> clustering, Deletion deletion)
    {
        assert !deletion.isLive();
        return new BTreeRow(clustering, LivenessInfo.EMPTY, deletion, BTree.empty(), Long.MIN_VALUE);
    }

    public static BTreeRow noCellLiveRow(Clustering<?> clustering, LivenessInfo primaryKeyLivenessInfo)
    {
        assert !primaryKeyLivenessInfo.isEmpty();
        return new BTreeRow(clustering,
                            primaryKeyLivenessInfo,
                            Deletion.LIVE,
                            BTree.empty(),
                            minDeletionTime(primaryKeyLivenessInfo));
    }

    private static long minDeletionTime(Cell<?> cell)
    {
        return cell.isTombstone() ? Long.MIN_VALUE : cell.localDeletionTime();
    }

    private static long minDeletionTime(LivenessInfo info)
    {
        return info.isExpiring() ? info.localExpirationTime() : Cell.MAX_DELETION_TIME;
    }

    private static long minDeletionTime(DeletionTime dt)
    {
        return dt.isLive() ? Cell.MAX_DELETION_TIME : Long.MIN_VALUE;
    }

    private static long minDeletionTime(ComplexColumnData cd)
    {
        long min = minDeletionTime(cd.complexDeletion());
        for (Cell<?> cell : cd)
        {
            min = Math.min(min, minDeletionTime(cell));
            if (min == Long.MIN_VALUE)
                break;
        }
        return min;
    }

    private static long minDeletionTime(ColumnData cd)
    {
        return cd.column().isSimple() ? minDeletionTime((Cell<?>) cd) : minDeletionTime((ComplexColumnData)cd);
    }

    public void apply(Consumer<ColumnData> function)
    {
        BTree.apply(btree, function);
    }

    public <A> void apply(BiConsumer<A, ColumnData> function, A arg)
    {
        BTree.apply(btree, function, arg);
    }

    public long accumulate(LongAccumulator<ColumnData> accumulator, long initialValue)
    {
        return BTree.accumulate(btree, accumulator, initialValue);
    }

    public long accumulate(LongAccumulator<ColumnData> accumulator, Comparator<ColumnData> comparator, ColumnData from, long initialValue)
    {
        return BTree.accumulate(btree, accumulator, comparator, from, initialValue);
    }

    public <A> long accumulate(BiLongAccumulator<A, ColumnData> accumulator, A arg, long initialValue)
    {
        return BTree.accumulate(btree, accumulator, arg, initialValue);
    }

    public <A> long accumulate(BiLongAccumulator<A, ColumnData> accumulator, A arg, Comparator<ColumnData> comparator, ColumnData from, long initialValue)
    {
        return BTree.accumulate(btree, accumulator, arg, comparator, from, initialValue);
    }

    private static long minDeletionTime(Object[] btree, LivenessInfo info, DeletionTime rowDeletion)
    {
        long min = Math.min(minDeletionTime(info), minDeletionTime(rowDeletion));
        return BTree.<ColumnData>accumulate(btree, (cd, l) -> Math.min(l, minDeletionTime(cd)), min);
    }

    public Clustering<?> clustering()
    {
        return clustering;
    }

    public Collection<ColumnMetadata> columns()
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

    public Cell<?> getCell(ColumnMetadata c)
    {
        assert !c.isComplex();
        return (Cell<?>) BTree.<Object>find(btree, ColumnMetadata.asymmetricColumnDataComparator, c);
    }

    public Cell<?> getCell(ColumnMetadata c, CellPath path)
    {
        assert c.isComplex();
        ComplexColumnData cd = getComplexColumnData(c);
        if (cd == null)
            return null;
        return cd.getCell(path);
    }

    public ComplexColumnData getComplexColumnData(ColumnMetadata c)
    {
        assert c.isComplex();
        return (ComplexColumnData) getColumnData(c);
    }

    public ColumnData getColumnData(ColumnMetadata c)
    {
        return (ColumnData) BTree.<Object>find(btree, ColumnMetadata.asymmetricColumnDataComparator, c);
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

    public Iterable<Cell<?>> cells()
    {
        return CellIterator::new;
    }

    public BTreeSearchIterator<ColumnMetadata, ColumnData> searchIterator()
    {
        return BTree.slice(btree, ColumnMetadata.asymmetricColumnDataComparator, BTree.Dir.ASC);
    }

    public Row filter(ColumnFilter filter, TableMetadata metadata)
    {
        return filter(filter, DeletionTime.LIVE, false, metadata);
    }

    public Row filter(ColumnFilter filter, DeletionTime activeDeletion, boolean setActiveDeletionToRow, TableMetadata metadata)
    {
        Map<ByteBuffer, DroppedColumn> droppedColumns = metadata.droppedColumns;

        boolean mayFilterColumns = !filter.fetchesAllColumns(isStatic()) || !filter.allFetchedColumnsAreQueried();
        // When merging sstable data in Row.Merger#merge(), rowDeletion is removed if it doesn't supersede activeDeletion.
        boolean mayHaveShadowed = !activeDeletion.isLive() && !deletion.time().supersedes(activeDeletion);

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
        Predicate<ColumnMetadata> inclusionTester = columns.inOrderInclusionTester();
        Predicate<ColumnMetadata> queriedByUserTester = filter.queriedColumns().columns(isStatic()).inOrderInclusionTester();
        final LivenessInfo rowLiveness = newInfo;
        return transformAndFilter(newInfo, newDeletion, (cd) -> {

            ColumnMetadata column = cd.column();
            if (!inclusionTester.test(column))
                return null;

            DroppedColumn dropped = droppedColumns.get(column.name.bytes);
            if (column.isComplex())
                return ((ComplexColumnData) cd).filter(filter, mayHaveShadowed ? activeDeletion : DeletionTime.LIVE, dropped, rowLiveness);

            Cell<?> cell = (Cell<?>) cd;
            // We include the cell unless it is 1) shadowed, 2) for a dropped column or 3) skippable.
            // And a cell is skippable if it is for a column that is not queried by the user and its timestamp
            // is lower than the row timestamp (see #10657 or SerializationHelper.includes() for details).
            boolean isForDropped = dropped != null && cell.timestamp() <= dropped.droppedTime;
            boolean isShadowed = mayHaveShadowed && activeDeletion.deletes(cell);
            boolean isSkippable = !queriedByUserTester.test(column);

            if (isForDropped || isShadowed || (isSkippable && cell.timestamp() < rowLiveness.timestamp()))
                return null;

            // We should apply the same "optimization" as in Cell.deserialize to avoid discrepances
            // between sstables and memtables data, i.e resulting in a digest mismatch.
            return isSkippable ? cell.withSkippedValue() : cell;
        });
    }

    public Row withOnlyQueriedData(ColumnFilter filter)
    {
        if (filter.allFetchedColumnsAreQueried())
            return this;

        return transformAndFilter(primaryKeyLivenessInfo, deletion, (cd) -> {

            ColumnMetadata column = cd.column();
            if (column.isComplex())
                return ((ComplexColumnData)cd).withOnlyQueriedData(filter);

            return filter.fetchedColumnIsQueried(column) ? cd : null;
        });
    }

    public boolean hasComplex()
    {
        if (BTree.isEmpty(btree))
            return false;

        int size = BTree.size(btree);
        ColumnData last = BTree.findByIndex(btree, size - 1);
        return last.column.isComplex();
    }

    public boolean hasComplexDeletion()
    {
        long result = accumulate((cd, v) -> ((ComplexColumnData) cd).complexDeletion().isLive() ? 0 : Cell.MAX_DELETION_TIME,
                                 COLUMN_COMPARATOR, isStatic() ? FIRST_COMPLEX_STATIC : FIRST_COMPLEX_REGULAR, 0L);
        return result == Cell.MAX_DELETION_TIME;
    }

    public Row markCounterLocalToBeCleared()
    {
        return transform((cd) -> cd.column().isCounterColumn() ? cd.markCounterLocalToBeCleared()
                                                               : cd);
    }

    public boolean hasDeletion(long nowInSec)
    {
        return nowInSec >= minLocalDeletionTime;
    }

    public boolean hasInvalidDeletions()
    {
        if (primaryKeyLivenessInfo().isExpiring() && (primaryKeyLivenessInfo().ttl() < 0 || primaryKeyLivenessInfo().localExpirationTime() < 0))
            return true;
        if (!deletion().time().validate())
            return true;
        return accumulate((cd, v) -> cd.hasInvalidDeletions() ? Cell.MAX_DELETION_TIME : v, 0) != 0;
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
                             : new Deletion(DeletionTime.build(newTimestamp - 1, deletion.time().localDeletionTime()), deletion.isShadowable());

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
             : new BTreeRow(clustering, primaryKeyLivenessInfo, Deletion.regular(newDeletion), btree, Long.MIN_VALUE);
    }

    public Row purge(DeletionPurger purger, long nowInSec, boolean enforceStrictLiveness)
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

    public Row purgeDataOlderThan(long timestamp, boolean enforceStrictLiveness)
    {
        LivenessInfo newInfo = primaryKeyLivenessInfo.timestamp() < timestamp ? LivenessInfo.EMPTY : primaryKeyLivenessInfo;
        Deletion newDeletion = deletion.time().markedForDeleteAt() < timestamp ? Deletion.LIVE : deletion;

        // when enforceStrictLiveness is set, a row is considered dead when it's PK liveness info is not present
        if (enforceStrictLiveness && newDeletion.isLive() && newInfo.isEmpty())
            return null;

        return transformAndFilter(newInfo, newDeletion, cd -> cd.purgeDataOlderThan(timestamp));
    }

    @Override
    public Row transformAndFilter(LivenessInfo info, Deletion deletion, Function<ColumnData, ColumnData> function)
    {
        return update(info, deletion, BTree.transformAndFilter(btree, function));
    }

    private Row update(LivenessInfo info, Deletion deletion, Object[] newTree)
    {
        if (btree == newTree && info == this.primaryKeyLivenessInfo && deletion == this.deletion)
            return this;

        if (info.isEmpty() && deletion.isLive() && BTree.isEmpty(newTree))
            return null;

        long minDeletionTime = minDeletionTime(newTree, info, deletion.time());
        return BTreeRow.create(clustering, info, deletion, newTree, minDeletionTime);
    }

    @Override
    public Row transformAndFilter(Function<ColumnData, ColumnData> function)
    {
        return transformAndFilter(primaryKeyLivenessInfo, deletion, function);
    }

    public Row transform(Function<ColumnData, ColumnData> function)
    {
        return update(primaryKeyLivenessInfo, deletion, BTree.transform(btree, function));
    }

    @Override
    public Row clone(Cloner cloner)
    {
        Object[] tree = BTree.<ColumnData, ColumnData>transform(btree, c -> c.clone(cloner));
        return BTreeRow.create(cloner.clone(clustering), primaryKeyLivenessInfo, deletion, tree);
    }

    public int dataSize()
    {
        int dataSize = clustering.dataSize()
                     + primaryKeyLivenessInfo.dataSize()
                     + deletion.dataSize();

        return Ints.checkedCast(accumulate((cd, v) -> v + cd.dataSize(), dataSize));
    }

    @Override
    public long unsharedHeapSize()
    {
        long heapSize = EMPTY_SIZE
                        + clustering.unsharedHeapSize()
                        + primaryKeyLivenessInfo.unsharedHeapSize()
                        + deletion.unsharedHeapSize()
                        + BTree.sizeOfStructureOnHeap(btree);

        return accumulate((cd, v) -> v + cd.unsharedHeapSize(), heapSize);
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        long heapSize = EMPTY_SIZE
                        + clustering.unsharedHeapSizeExcludingData()
                        + primaryKeyLivenessInfo.unsharedHeapSize()
                        + deletion.unsharedHeapSize()
                        + BTree.sizeOfStructureOnHeap(btree);

        return accumulate((cd, v) -> v + cd.unsharedHeapSizeExcludingData(), heapSize);
    }

    public static Row.Builder sortedBuilder()
    {
        return new Builder(true);
    }

    public static Row.Builder unsortedBuilder()
    {
        return new Builder(false);
    }

    // This is only used by PartitionUpdate.CounterMark but other uses should be avoided as much as possible as it breaks our general
    // assumption that Row objects are immutable. This method should go away post-#6506 in particular.
    // This method is in particular not exposed by the Row API on purpose.
    // This method also *assumes* that the cell we're setting already exists.
    public void setValue(ColumnMetadata column, CellPath path, ByteBuffer value)
    {
        ColumnData current = (ColumnData) BTree.<Object>find(btree, ColumnMetadata.asymmetricColumnDataComparator, column);
        if (column.isSimple())
            BTree.replaceInSitu(btree, ColumnData.comparator, current, ((Cell<?>) current).withUpdatedValue(value));
        else
            ((ComplexColumnData) current).setValue(path, value);
    }

    public Iterable<Cell<?>> cellsInLegacyOrder(TableMetadata metadata, boolean reversed)
    {
        return () -> new CellInLegacyOrderIterator(metadata, reversed);
    }

    public static Row merge(BTreeRow existing,
                            BTreeRow update,
                            ColumnData.PostReconciliationFunction reconcileF)
    {
        Object[] existingBtree = existing.btree;
        Object[] updateBtree = update.btree;

        LivenessInfo existingInfo = existing.primaryKeyLivenessInfo();
        LivenessInfo updateInfo = update.primaryKeyLivenessInfo();
        LivenessInfo livenessInfo = existingInfo.supersedes(updateInfo) ? existingInfo : updateInfo;

        Row.Deletion rowDeletion = existing.deletion().supersedes(update.deletion()) ? existing.deletion() : update.deletion();

        if (rowDeletion.deletes(livenessInfo))
            livenessInfo = LivenessInfo.EMPTY;
        else if (rowDeletion.isShadowedBy(livenessInfo))
            rowDeletion = Row.Deletion.LIVE;

        DeletionTime deletion = rowDeletion.time();
        try (ColumnData.Reconciler reconciler = ColumnData.reconciler(reconcileF, deletion))
        {
            if (!rowDeletion.isLive())
            {
                if (rowDeletion == existing.deletion())
                {
                    updateBtree = BTree.transformAndFilter(updateBtree, reconciler::retain);
                }
                else
                {
                    existingBtree = BTree.transformAndFilter(existingBtree, reconciler::retain);
                }
            }
            Object[] tree = BTree.update(existingBtree, updateBtree, ColumnData.comparator, reconciler);
            return new BTreeRow(existing.clustering, livenessInfo, rowDeletion, tree, minDeletionTime(tree, livenessInfo, deletion));
        }
    }

    private class CellIterator extends AbstractIterator<Cell<?>>
    {
        private Iterator<ColumnData> columnData = iterator();
        private Iterator<Cell<?>> complexCells;

        protected Cell<?> computeNext()
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
                    return (Cell<?>)cd;
            }
        }
    }

    private class CellInLegacyOrderIterator extends AbstractIterator<Cell<?>>
    {
        private final Comparator<ByteBuffer> comparator;
        private final boolean reversed;
        private final int firstComplexIdx;
        private int simpleIdx;
        private int complexIdx;
        private Iterator<Cell<?>> complexCells;
        private final Object[] data;

        private CellInLegacyOrderIterator(TableMetadata metadata, boolean reversed)
        {
            AbstractType<?> nameComparator = UTF8Type.instance;
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

        private Iterator<Cell<?>> makeComplexIterator(Object complexData)
        {
            ComplexColumnData ccd = (ComplexColumnData)complexData;
            return reversed ? ccd.reverseIterator() : ccd.iterator();
        }

        protected Cell<?> computeNext()
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
                        return (Cell<?>)data[getSimpleIdxAndIncrement()];

                    if (comparator.compare(((ColumnData) data[getSimpleIdx()]).column().name.bytes, ((ColumnData) data[getComplexIdx()]).column().name.bytes) < 0)
                        return (Cell<?>)data[getSimpleIdxAndIncrement()];
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
            public ComplexColumnDeletion(ColumnMetadata column, DeletionTime deletionTime)
            {
                super(column, deletionTime.markedForDeleteAt(), 0, deletionTime.localDeletionTime(), ByteBufferUtil.EMPTY_BYTE_BUFFER, CellPath.BOTTOM);
            }
        }

        // converts a run of Cell with equal column into a ColumnData
        private static class CellResolver implements BTree.Builder.Resolver
        {
            static final CellResolver instance = new CellResolver();

            public ColumnData resolve(Object[] cells, int lb, int ub)
            {
                Cell<?> cell = (Cell<?>) cells[lb];
                ColumnMetadata column = cell.column;
                if (cell.column.isSimple())
                {
                    while (++lb < ub)
                        cell = Cells.reconcile(cell, (Cell<?>) cells[lb]);
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
                    cell = (Cell<?>) cells[lb];
                    if (!(cell instanceof ComplexColumnDeletion))
                        break;

                    if (cell.timestamp() > deletion.markedForDeleteAt())
                        deletion = DeletionTime.build(cell.timestamp(), cell.localDeletionTime());
                    lb++;
                }

                Object[] buildFrom = new Object[ub - lb];
                int buildFromCount = 0;
                Cell<?> previous = null;
                for (int i = lb; i < ub; i++)
                {
                    Cell<?> c = (Cell<?>) cells[i];

                    if (deletion == DeletionTime.LIVE || c.timestamp() >= deletion.markedForDeleteAt())
                    {
                        if (previous != null && column.cellComparator().compare(previous, c) == 0)
                        {
                            c = Cells.reconcile(previous, c);
                            buildFrom[buildFromCount - 1] = c;
                        }
                        else
                        {
                            buildFrom[buildFromCount++] = c;
                        }
                        previous = c;
                    }
                }

                try (BulkIterator<Cell> iterator = BulkIterator.of(buildFrom))
                {
                    Object[] btree = BTree.build(iterator, buildFromCount, UpdateFunction.noOp());
                    return new ComplexColumnData(column, btree, deletion);
                }
            }
        }

        protected Clustering<?> clustering;
        protected LivenessInfo primaryKeyLivenessInfo = LivenessInfo.EMPTY;
        protected Deletion deletion = Deletion.LIVE;

        private final boolean isSorted;
        private BTree.Builder<Cell<?>> cells_;
        private boolean hasComplex = false;

        // For complex column at index i of 'columns', we store at complexDeletions[i] its complex deletion.

        protected Builder(boolean isSorted)
        {
            cells_ = null;
            this.isSorted = isSorted;
        }

        private BTree.Builder<Cell<?>> getCells()
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

        public void newRow(Clustering<?> clustering)
        {
            assert this.clustering == null; // Ensures we've properly called build() if we've use this builder before
            this.clustering = clustering;
        }

        public Clustering<?> clustering()
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

        public void addCell(Cell<?> cell)
        {
            assert cell.column().isStatic() == (clustering == Clustering.STATIC_CLUSTERING) : "Column is " + cell.column() + ", clustering = " + clustering;

            // In practice, only unsorted builder have to deal with shadowed cells, but it doesn't cost us much to deal with it unconditionally in this case
            if (deletion.deletes(cell))
                return;

            getCells().add(cell);
            hasComplex |= cell.column.isComplex();
        }

        public void addComplexDeletion(ColumnMetadata column, DeletionTime complexDeletion)
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
                getCells().resolve(CellResolver.instance);
            Object[] btree = getCells().build();

            if (deletion.isShadowedBy(primaryKeyLivenessInfo))
                deletion = Deletion.LIVE;

            long minDeletionTime = minDeletionTime(btree, primaryKeyLivenessInfo, deletion.time());
            Row row = BTreeRow.create(clustering, primaryKeyLivenessInfo, deletion, btree, minDeletionTime);
            reset();
            return row;
        }
    }
}
