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

import java.util.*;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;

/**
 * Storage engine representation of a row.
 *
 * A row mainly contains the following informations:
 *   1) Its {@code Clustering}, which holds the values for the clustering columns identifying the row.
 *   2) Its row level informations: the primary key liveness infos and the row deletion (see
 *      {@link #primaryKeyLivenessInfo()} and {@link #deletion()} for more details).
 *   3) Data for the columns it contains, or in other words, it's a (sorted) collection of
 *      {@code ColumnData}.
 *
 * Also note that as for every other storage engine object, a {@code Row} object cannot shadow
 * it's own data. For instance, a {@code Row} cannot contains a cell that is deleted by its own
 * row deletion.
 */
public interface Row extends Unfiltered, Collection<ColumnData>
{
    /**
     * The clustering values for this row.
     */
    @Override
    public Clustering clustering();

    /**
     * An in-natural-order collection of the columns for which data (incl. simple tombstones)
     * is present in this row.
     */
    public Collection<ColumnDefinition> columns();

    /**
     * The row deletion.
     *
     * This correspond to the last row deletion done on this row.
     *
     * @return the row deletion.
     */
    public Deletion deletion();

    /**
     * Liveness information for the primary key columns of this row.
     * <p>
     * As a row is uniquely identified by its primary key, all its primary key columns
     * share the same {@code LivenessInfo}. This liveness information is what allows us
     * to distinguish between a dead row (it has no live cells and its primary key liveness
     * info is empty) and a live row but where all non PK columns are null (it has no
     * live cells, but its primary key liveness is not empty). Please note that the liveness
     * info (including it's eventually ttl/local deletion time) only apply to the primary key
     * columns and has no impact on the row content.
     * <p>
     * Note in particular that a row may have live cells but no PK liveness info, because the
     * primary key liveness informations are only set on {@code INSERT} (which makes sense
     * in itself, see #6782) but live cells can be added through {@code UPDATE} even if the row
     * wasn't pre-existing (which users are encouraged not to do, but we can't validate).
     */
    public LivenessInfo primaryKeyLivenessInfo();

    /**
     * Whether the row correspond to a static row or not.
     *
     * @return whether the row correspond to a static row or not.
     */
    public boolean isStatic();

    /**
     * Whether the row has no information whatsoever. This means no PK liveness info, no row
     * deletion, no cells and no complex deletion info.
     *
     * @return {@code true} if the row has no data, {@code false} otherwise.
     */
    public boolean isEmpty();

    /**
     * Whether the row has some live information (i.e. it's not just deletion informations).
     */
    public boolean hasLiveData(int nowInSec);

    /**
     * Returns a cell for a simple column.
     *
     * @param c the simple column for which to fetch the cell.
     * @return the corresponding cell or {@code null} if the row has no such cell.
     */
    public Cell getCell(ColumnDefinition c);

    /**
     * Return a cell for a given complex column and cell path.
     *
     * @param c the complex column for which to fetch the cell.
     * @param path the cell path for which to fetch the cell.
     * @return the corresponding cell or {@code null} if the row has no such cell.
     */
    public Cell getCell(ColumnDefinition c, CellPath path);

    /**
     * The data for a complex column.
     * <p>
     * The returned object groups all the cells for the column, as well as it's complex deletion (if relevant).
     *
     * @param c the complex column for which to return the complex data.
     * @return the data for {@code c} or {@code null} is the row has no data for this column.
     */
    public ComplexColumnData getComplexColumnData(ColumnDefinition c);

    /**
     * An iterable over the cells of this row.
     * <p>
     * The iterable guarantees that cells are returned in order of {@link Cell#comparator}.
     *
     * @return an iterable over the cells of this row.
     */
    public Iterable<Cell> cells();

    /**
     * An iterable over the cells of this row that return cells in "legacy order".
     * <p>
     * In 3.0+, columns are sorted so that all simple columns are before all complex columns. Previously
     * however, the cells where just sorted by the column name. This iterator return cells in that
     * legacy order. It's only ever meaningful for backward/thrift compatibility code.
     *
     * @param metadata the table this is a row of.
     * @param reversed if cells should returned in reverse order.
     * @return an iterable over the cells of this row in "legacy order".
     */
    public Iterable<Cell> cellsInLegacyOrder(CFMetaData metadata, boolean reversed);

    /**
     * Whether the row stores any (non-live) complex deletion for any complex column.
     */
    public boolean hasComplexDeletion();

    /**
     * Whether the row stores any (non-RT) data for any complex column.
     */
    boolean hasComplex();

    /**
     * Whether the row has any deletion info (row deletion, cell tombstone, expired cell or complex deletion).
     *
     * @param nowInSec the current time in seconds to decid if a cell is expired.
     */
    public boolean hasDeletion(int nowInSec);

    /**
     * An iterator to efficiently search data for a given column.
     *
     * @return a search iterator for the cells of this row.
     */
    public SearchIterator<ColumnDefinition, ColumnData> searchIterator();

    /**
     * Returns a copy of this row that:
     *   1) only includes the data for the column included by {@code filter}.
     *   2) doesn't include any data that belongs to a dropped column (recorded in {@code metadata}).
     */
    public Row filter(ColumnFilter filter, CFMetaData metadata);

    /**
     * Returns a copy of this row that:
     *   1) only includes the data for the column included by {@code filter}.
     *   2) doesn't include any data that belongs to a dropped column (recorded in {@code metadata}).
     *   3) doesn't include any data that is shadowed/deleted by {@code activeDeletion}.
     *   4) uses {@code activeDeletion} as row deletion iff {@code setActiveDeletionToRow} and {@code activeDeletion} supersedes the row deletion.
     */
    public Row filter(ColumnFilter filter, DeletionTime activeDeletion, boolean setActiveDeletionToRow, CFMetaData metadata);

    /**
     * Returns a copy of this row without any deletion info that should be purged according to {@code purger}.
     *
     * @param purger the {@code DeletionPurger} to use to decide what can be purged.
     * @param nowInSec the current time to decide what is deleted and what isn't (in the case of expired cells).
     * @return this row but without any deletion info purged by {@code purger}.
     */
    public Row purge(DeletionPurger purger, int nowInSec);

    /**
     * Returns a copy of this row which only include the data queried by {@code filter}, excluding anything _fetched_ for
     * internal reasons but not queried by the user (see {@link ColumnFilter} for details).
     *
     * @param filter the {@code ColumnFilter} to use when deciding what is user queried. This should be the filter
     * that was used when querying the row on which this method is called.
     * @return the row but with all data that wasn't queried by the user skipped.
     */
    public Row withOnlyQueriedData(ColumnFilter filter);

    /**
     * Returns a copy of this row where all counter cells have they "local" shard marked for clearing.
     */
    public Row markCounterLocalToBeCleared();

    /**
     * returns a copy of this row where all live timestamp have been replaced by {@code newTimestamp} and every deletion timestamp
     * by {@code newTimestamp - 1}. See {@link Commit} for why we need this.
     */
    public Row updateAllTimestamp(long newTimestamp);

    public int dataSize();

    public long unsharedHeapSizeExcludingData();

    public String toString(CFMetaData metadata, boolean fullDetails);

    /**
     * A row deletion/tombstone.
     * <p>
     * A row deletion mostly consists of the time of said deletion, but there is 2 variants: shadowable
     * and regular row deletion.
     * <p>
     * A shadowable row deletion only exists if the row timestamp ({@code primaryKeyLivenessInfo().timestamp()})
     * is lower than the deletion timestamp. That is, if a row has a shadowable deletion with timestamp A and an update is made
     * to that row with a timestamp B such that B > A, then the shadowable deletion is 'shadowed' by that update. A concrete
     * consequence is that if said update has cells with timestamp lower than A, then those cells are preserved
     * (since the deletion is removed), and this contrarily to a normal (regular) deletion where the deletion is preserved
     * and such cells are removed.
     * <p>
     * Currently, the only use of shadowable row deletions is Materialized Views, see CASSANDRA-10261.
     */
    public static class Deletion
    {
        public static final Deletion LIVE = new Deletion(DeletionTime.LIVE, false);

        private final DeletionTime time;
        private final boolean isShadowable;

        public Deletion(DeletionTime time, boolean isShadowable)
        {
            assert !time.isLive() || !isShadowable;
            this.time = time;
            this.isShadowable = isShadowable;
        }

        public static Deletion regular(DeletionTime time)
        {
            return time.isLive() ? LIVE : new Deletion(time, false);
        }

        public static Deletion shadowable(DeletionTime time)
        {
            return new Deletion(time, true);
        }

        /**
         * The time of the row deletion.
         *
         * @return the time of the row deletion.
         */
        public DeletionTime time()
        {
            return time;
        }

        /**
         * Whether the deletion is a shadowable one or not.
         *
         * @return whether the deletion is a shadowable one. Note that if {@code isLive()}, then this is
         * guarantee to return {@code false}.
         */
        public boolean isShadowable()
        {
            return isShadowable;
        }

        /**
         * Wether the deletion is live or not, that is if its an actual deletion or not.
         *
         * @return {@code true} if this represents no deletion of the row, {@code false} if that's an actual
         * deletion.
         */
        public boolean isLive()
        {
            return time().isLive();
        }

        public boolean supersedes(DeletionTime that)
        {
            return time.supersedes(that);
        }

        public boolean supersedes(Deletion that)
        {
            return time.supersedes(that.time);
        }

        public boolean isShadowedBy(LivenessInfo primaryKeyLivenessInfo)
        {
            return isShadowable && primaryKeyLivenessInfo.timestamp() > time.markedForDeleteAt();
        }

        public boolean deletes(LivenessInfo info)
        {
            return time.deletes(info);
        }

        public void digest(MessageDigest digest)
        {
            time.digest(digest);
            FBUtilities.updateWithBoolean(digest, isShadowable);
        }

        public int dataSize()
        {
            return time.dataSize() + 1;
        }

        @Override
        public boolean equals(Object o)
        {
            if(!(o instanceof Deletion))
                return false;
            Deletion that = (Deletion)o;
            return this.time.equals(that.time) && this.isShadowable == that.isShadowable;
        }

        @Override
        public final int hashCode()
        {
            return Objects.hash(time, isShadowable);
        }

        @Override
        public String toString()
        {
            return String.format("%s%s", time, isShadowable ? "(shadowable)" : "");
        }
    }

    /**
     * Interface for building rows.
     * <p>
     * The builder of a row should always abid to the following rules:
     *   1) {@link #newRow} is always called as the first thing for the row.
     *   2) {@link #addPrimaryKeyLivenessInfo} and {@link #addRowDeletion}, if called, are called before
     *      any {@link #addCell}/{@link #addComplexDeletion} call.
     *   3) {@link #build} is called to construct the new row. The builder can then be reused.
     *
     * There is 2 variants of a builder: sorted and unsorted ones. A sorted builder expects user to abid to the
     * following additional rules:
     *   4) Calls to {@link #addCell}/{@link #addComplexDeletion} are done in strictly increasing column order.
     *      In other words, all calls to these methods for a give column {@code c} are done after any call for
     *      any column before {@code c} and before any call for any column after {@code c}.
     *   5) Calls to {@link #addCell} are further done in strictly increasing cell order (the one defined by
     *      {@link Cell#comparator}. That is, for a give column, cells are passed in {@code CellPath} order.
     *
     * An unsorted builder will not expect those last rules however: {@link #addCell} and {@link #addComplexDeletion}
     * can be done in any order. And in particular unsorted builder allows multiple calls for the same column/cell. In
     * that latter case, the result will follow the usual reconciliation rules (so equal cells are reconciled with
     * {@link Cells#reconcile} and the "biggest" of multiple complex deletion for the same column wins).
     */
    public interface Builder
    {
        /**
         * Whether the builder is a sorted one or not.
         *
         * @return if the builder requires calls to be done in sorted order or not (see above).
         */
        public boolean isSorted();

        /**
         * Prepares the builder to build a new row of clustering {@code clustering}.
         * <p>
         * This should always be the first call for a given row.
         *
         * @param clustering the clustering for the new row.
         */
        public void newRow(Clustering clustering);

        /**
         * The clustering for the row that is currently being built.
         *
         * @return the clustering for the row that is currently being built, or {@code null} if {@link #newRow} hasn't
         * yet been called.
         */
        public Clustering clustering();

        /**
         * Adds the liveness information for the partition key columns of this row.
         *
         * This call is optional (skipping it is equivalent to calling {@code addPartitionKeyLivenessInfo(LivenessInfo.NONE)}).
         *
         * @param info the liveness information for the partition key columns of the built row.
         */
        public void addPrimaryKeyLivenessInfo(LivenessInfo info);

        /**
         * Adds the deletion information for this row.
         *
         * This call is optional and can be skipped if the row is not deleted.
         *
         * @param deletion the row deletion time, or {@code Deletion.LIVE} if the row isn't deleted.
         */
        public void addRowDeletion(Deletion deletion);

        /**
         * Adds a cell to this builder.
         *
         * @param cell the cell to add.
         */
        public void addCell(Cell cell);

        /**
         * Adds a complex deletion.
         *
         * @param column the column for which to add the {@code complexDeletion}.
         * @param complexDeletion the complex deletion time to add.
         */
        public void addComplexDeletion(ColumnDefinition column, DeletionTime complexDeletion);

        /**
         * Builds and return built row.
         *
         * @return the last row built by this builder.
         */
        public Row build();
    }

    /**
     * Utility class to help merging rows from multiple inputs (UnfilteredRowIterators).
     */
    public static class Merger
    {
        private final Row[] rows;
        private final List<Iterator<ColumnData>> columnDataIterators;

        private Clustering clustering;
        private int rowsToMerge;
        private int lastRowSet = -1;

        private final List<ColumnData> dataBuffer = new ArrayList<>();
        private final ColumnDataReducer columnDataReducer;

        public Merger(int size, int nowInSec, boolean hasComplex)
        {
            this.rows = new Row[size];
            this.columnDataIterators = new ArrayList<>(size);
            this.columnDataReducer = new ColumnDataReducer(size, nowInSec, hasComplex);
        }

        public void clear()
        {
            dataBuffer.clear();
            Arrays.fill(rows, null);
            columnDataIterators.clear();
            rowsToMerge = 0;
            lastRowSet = -1;
        }

        public void add(int i, Row row)
        {
            clustering = row.clustering();
            rows[i] = row;
            ++rowsToMerge;
            lastRowSet = i;
        }

        public Row merge(DeletionTime activeDeletion)
        {
            // If for this clustering we have only one row version and have no activeDeletion (i.e. nothing to filter out),
            // then we can just return that single row
            if (rowsToMerge == 1 && activeDeletion.isLive())
            {
                Row row = rows[lastRowSet];
                assert row != null;
                return row;
            }

            LivenessInfo rowInfo = LivenessInfo.EMPTY;
            Deletion rowDeletion = Deletion.LIVE;
            for (Row row : rows)
            {
                if (row == null)
                    continue;

                if (row.primaryKeyLivenessInfo().supersedes(rowInfo))
                    rowInfo = row.primaryKeyLivenessInfo();
                if (row.deletion().supersedes(rowDeletion))
                    rowDeletion = row.deletion();
            }

            if (rowDeletion.isShadowedBy(rowInfo))
                rowDeletion = Deletion.LIVE;

            if (rowDeletion.supersedes(activeDeletion))
                activeDeletion = rowDeletion.time();
            else
                rowDeletion = Deletion.LIVE;

            if (activeDeletion.deletes(rowInfo))
                rowInfo = LivenessInfo.EMPTY;

            for (Row row : rows)
                columnDataIterators.add(row == null ? Collections.emptyIterator() : row.iterator());

            columnDataReducer.setActiveDeletion(activeDeletion);
            Iterator<ColumnData> merged = MergeIterator.get(columnDataIterators, ColumnData.comparator, columnDataReducer);
            while (merged.hasNext())
            {
                ColumnData data = merged.next();
                if (data != null)
                    dataBuffer.add(data);
            }

            // Because some data might have been shadowed by the 'activeDeletion', we could have an empty row
            return rowInfo.isEmpty() && rowDeletion.isLive() && dataBuffer.isEmpty()
                 ? null
                 : BTreeRow.create(clustering, rowInfo, rowDeletion, BTree.build(dataBuffer, UpdateFunction.<ColumnData>noOp()));
        }

        public Clustering mergedClustering()
        {
            return clustering;
        }

        public Row[] mergedRows()
        {
            return rows;
        }

        private static class ColumnDataReducer extends MergeIterator.Reducer<ColumnData, ColumnData>
        {
            private final int nowInSec;

            private ColumnDefinition column;
            private final List<ColumnData> versions;

            private DeletionTime activeDeletion;

            private final ComplexColumnData.Builder complexBuilder;
            private final List<Iterator<Cell>> complexCells;
            private final CellReducer cellReducer;

            public ColumnDataReducer(int size, int nowInSec, boolean hasComplex)
            {
                this.nowInSec = nowInSec;
                this.versions = new ArrayList<>(size);
                this.complexBuilder = hasComplex ? ComplexColumnData.builder() : null;
                this.complexCells = hasComplex ? new ArrayList<>(size) : null;
                this.cellReducer = new CellReducer(nowInSec);
            }

            public void setActiveDeletion(DeletionTime activeDeletion)
            {
                this.activeDeletion = activeDeletion;
            }

            public void reduce(int idx, ColumnData data)
            {
                column = data.column();
                versions.add(data);
            }

            protected ColumnData getReduced()
            {
                if (column.isSimple())
                {
                    Cell merged = null;
                    for (ColumnData data : versions)
                    {
                        Cell cell = (Cell)data;
                        if (!activeDeletion.deletes(cell))
                            merged = merged == null ? cell : Cells.reconcile(merged, cell, nowInSec);
                    }
                    return merged;
                }
                else
                {
                    complexBuilder.newColumn(column);
                    complexCells.clear();
                    DeletionTime complexDeletion = DeletionTime.LIVE;
                    for (ColumnData data : versions)
                    {
                        ComplexColumnData cd = (ComplexColumnData)data;
                        if (cd.complexDeletion().supersedes(complexDeletion))
                            complexDeletion = cd.complexDeletion();
                        complexCells.add(cd.iterator());
                    }

                    if (complexDeletion.supersedes(activeDeletion))
                    {
                        cellReducer.setActiveDeletion(complexDeletion);
                        complexBuilder.addComplexDeletion(complexDeletion);
                    }
                    else
                    {
                        cellReducer.setActiveDeletion(activeDeletion);
                    }

                    Iterator<Cell> cells = MergeIterator.get(complexCells, Cell.comparator, cellReducer);
                    while (cells.hasNext())
                    {
                        Cell merged = cells.next();
                        if (merged != null)
                            complexBuilder.addCell(merged);
                    }
                    return complexBuilder.build();
                }
            }

            protected void onKeyChange()
            {
                versions.clear();
            }
        }

        private static class CellReducer extends MergeIterator.Reducer<Cell, Cell>
        {
            private final int nowInSec;

            private DeletionTime activeDeletion;
            private Cell merged;

            public CellReducer(int nowInSec)
            {
                this.nowInSec = nowInSec;
            }

            public void setActiveDeletion(DeletionTime activeDeletion)
            {
                this.activeDeletion = activeDeletion;
                onKeyChange();
            }

            public void reduce(int idx, Cell cell)
            {
                if (!activeDeletion.deletes(cell))
                    merged = merged == null ? cell : Cells.reconcile(merged, cell, nowInSec);
            }

            protected Cell getReduced()
            {
                return merged;
            }

            protected void onKeyChange()
            {
                merged = null;
            }
        }
    }
}
