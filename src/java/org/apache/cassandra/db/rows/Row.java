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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.BiLongAccumulator;
import org.apache.cassandra.utils.LongAccumulator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.memory.Cloner;

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
public interface Row extends Unfiltered, Iterable<ColumnData>, IMeasurableMemory
{
    /**
     * The clustering values for this row.
     */
    @Override
    public Clustering<?> clustering();

    /**
     * An in-natural-order collection of the columns for which data (incl. simple tombstones)
     * is present in this row.
     */
    public Collection<ColumnMetadata> columns();


    /**
     * The number of columns for which data (incl. simple tombstones) is present in this row.
     */
    public int columnCount();

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
     * 
     * @param nowInSec the current time to decide what is deleted and what isn't
     * @param enforceStrictLiveness whether the row should be purged if there is no PK liveness info,
     *                              normally retrieved from {@link TableMetadata#enforceStrictLiveness()}
     * @return true if there is some live information
     */
    public boolean hasLiveData(long nowInSec, boolean enforceStrictLiveness);

    /**
     * Returns a cell for a simple column.
     *
     * @param c the simple column for which to fetch the cell.
     * @return the corresponding cell or {@code null} if the row has no such cell.
     */
    public Cell<?> getCell(ColumnMetadata c);

    /**
     * Return a cell for a given complex column and cell path.
     *
     * @param c the complex column for which to fetch the cell.
     * @param path the cell path for which to fetch the cell.
     * @return the corresponding cell or {@code null} if the row has no such cell.
     */
    public Cell<?> getCell(ColumnMetadata c, CellPath path);

    /**
     * The data for a complex column.
     * <p>
     * The returned object groups all the cells for the column, as well as it's complex deletion (if relevant).
     *
     * @param c the complex column for which to return the complex data.
     * @return the data for {@code c} or {@code null} if the row has no data for this column.
     */
    public ComplexColumnData getComplexColumnData(ColumnMetadata c);

    /**
     * Returns the {@link ColumnData} for the specified column.
     *
     * @param c the column for which to fetch the data.
     * @return the data for the column or {@code null} if the row has no data for this column.
     */
    public ColumnData getColumnData(ColumnMetadata c);

    /**
     * An iterable over the cells of this row.
     * <p>
     * The iterable guarantees that cells are returned in order of {@link Cell#comparator}.
     *
     * @return an iterable over the cells of this row.
     */
    public Iterable<Cell<?>> cells();

    /**
     * A collection of the ColumnData representation of this row, for columns with some data (possibly not live) present
     * <p>
     * The data is returned in column order.
     *
     * @return a Collection of the non-empty ColumnData for this row.
     */
    public Collection<ColumnData> columnData();

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
    public Iterable<Cell<?>> cellsInLegacyOrder(TableMetadata metadata, boolean reversed);

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
    public boolean hasDeletion(long nowInSec);

    /**
     * An iterator to efficiently search data for a given column.
     *
     * @return a search iterator for the cells of this row.
     */
    public SearchIterator<ColumnMetadata, ColumnData> searchIterator();

    /**
     * Returns a copy of this row that:
     *   1) only includes the data for the column included by {@code filter}.
     *   2) doesn't include any data that belongs to a dropped column (recorded in {@code metadata}).
     */
    public Row filter(ColumnFilter filter, TableMetadata metadata);

    /**
     * Returns a copy of this row that:
     *   1) only includes the data for the column included by {@code filter}.
     *   2) doesn't include any data that belongs to a dropped column (recorded in {@code metadata}).
     *   3) doesn't include any data that is shadowed/deleted by {@code activeDeletion}.
     *   4) uses {@code activeDeletion} as row deletion iff {@code setActiveDeletionToRow} and {@code activeDeletion} supersedes the row deletion.
     */
    public Row filter(ColumnFilter filter, DeletionTime activeDeletion, boolean setActiveDeletionToRow, TableMetadata metadata);

    /**
     * Requires that {@code function} returns either {@code null} or {@code ColumnData} for the same column.
     *
     * Returns a copy of this row that:
     *   1) {@code function} has been applied to the members of
     *   2) doesn't include any {@code null} results of {@code function}
     *   3) has precisely the provided {@code LivenessInfo} and {@code Deletion}
     */
    public Row transformAndFilter(LivenessInfo info, Deletion deletion, Function<ColumnData, ColumnData> function);

    /**
     * Requires that {@code function} returns either {@code null} or {@code ColumnData} for the same column.
     *
     * Returns a copy of this row that:
     *   1) {@code function} has been applied to the members of
     *   2) doesn't include any {@code null} results of {@code function}
     */
    public Row transformAndFilter(Function<ColumnData, ColumnData> function);

    public Row clone(Cloner cloner);

    /**
     * Returns a copy of this row without any deletion info that should be purged according to {@code purger}.
     *
     * @param purger the {@code DeletionPurger} to use to decide what can be purged.
     * @param nowInSec the current time to decide what is deleted and what isn't (in the case of expired cells).
     * @param enforceStrictLiveness whether the row should be purged if there is no PK liveness info,
     *                              normally retrieved from {@link TableMetadata#enforceStrictLiveness()}
     *
     *        When enforceStrictLiveness is set, rows with empty PK liveness info
     *        and no row deletion are purged.
     *
     *        Currently this is only used by views with normal base column as PK column
     *        so updates to other base columns do not make the row live when the PK column
     *        is not live. See CASSANDRA-11500.
     *
     * @return this row but without any deletion info purged by {@code purger}. If the purged row is empty, returns
     *         {@code null}.
     */
    public Row purge(DeletionPurger purger, long nowInSec, boolean enforceStrictLiveness);

    /**
     * Returns a copy of this row which only include the data queried by {@code filter}, excluding anything _fetched_ for
     * internal reasons but not queried by the user (see {@link ColumnFilter} for details).
     *
     * @param filter the {@code ColumnFilter} to use when deciding what is user queried. This should be the filter
     * that was used when querying the row on which this method is called.
     * @return the row but with all data that wasn't queried by the user skipped.
     */
    public Row withOnlyQueriedData(ColumnFilter filter);

    /*
     * Returns a copy of this row without any data with a timestamp older than the one provided
     */
    public Row purgeDataOlderThan(long timestamp, boolean enforceStrictLiveness);

    /**
     * Returns a copy of this row where all counter cells have they "local" shard marked for clearing.
     */
    public Row markCounterLocalToBeCleared();

    /**
     * Returns a copy of this row where all live timestamp have been replaced by {@code newTimestamp} and every deletion
     * timestamp by {@code newTimestamp - 1}.
     *
     * @param newTimestamp the timestamp to use for all live data in the returned row.
     *
     * @see Commit for why we need this.
     */
    public Row updateAllTimestamp(long newTimestamp);

    /**
     * Returns a copy of this row with the new deletion as row deletion if it is more recent
     * than the current row deletion.
     * <p>
     * WARNING: this method <b>does not</b> check that nothing in the row is shadowed by the provided
     * deletion and if that is the case, the created row will be <b>invalid</b>. It is thus up to the
     * caller to verify that this is not the case and the only reasonable use case of this is probably
     * when the row and the deletion comes from the same {@code UnfilteredRowIterator} since that gives
     * use this guarantee.
     */
    public Row withRowDeletion(DeletionTime deletion);

    public int dataSize();

    public long unsharedHeapSizeExcludingData();

    public String toString(TableMetadata metadata, boolean fullDetails);
    public long unsharedHeapSize();

    /**
     * Apply a function to every column in a row
     */
    public void apply(Consumer<ColumnData> function);

    /**
     * Apply a function to every column in a row
     */
    public <A> void apply(BiConsumer<A, ColumnData> function, A arg);

    /**
     * Apply an accumulation funtion to every column in a row
     */

    public long accumulate(LongAccumulator<ColumnData> accumulator, long initialValue);

    public long accumulate(LongAccumulator<ColumnData> accumulator, Comparator<ColumnData> comparator, ColumnData from, long initialValue);

    public <A> long accumulate(BiLongAccumulator<A, ColumnData> accumulator, A arg, long initialValue);

    public <A> long accumulate(BiLongAccumulator<A, ColumnData> accumulator, A arg, Comparator<ColumnData> comparator, ColumnData from, long initialValue);

    /**
     * A row deletion/tombstone.
     * <p>
     * A row deletion mostly consists of the time of said deletion, but there is 2 variants: shadowable
     * and regular row deletion.
     * <p>
     * A shadowable row deletion only exists if the row has no timestamp. In other words, the deletion is only
     * valid as long as no newer insert is done (thus setting a row timestamp; note that if the row timestamp set
     * is lower than the deletion, it is shadowed (and thus ignored) as usual).
     * <p>
     * That is, if a row has a shadowable deletion with timestamp A and an update is made to that row with a
     * timestamp B such that {@code B > A} (and that update sets the row timestamp), then the shadowable deletion is 'shadowed'
     * by that update. A concrete consequence is that if said update has cells with timestamp lower than A, then those
     * cells are preserved(since the deletion is removed), and this is contrary to a normal (regular) deletion where the
     * deletion is preserved and such cells are removed.
     * <p>
     * Currently, the only use of shadowable row deletions is Materialized Views, see CASSANDRA-10261.
     */
    public static class Deletion
    {
        public static final Deletion LIVE = new Deletion(DeletionTime.LIVE, false);
        private static final long EMPTY_SIZE = ObjectSizes.measure(DeletionTime.build(0, 0));

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

        /** @deprecated See CAASSANDRA-10261 */
        @Deprecated(since = "4.0")
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

        public boolean deletes(Cell<?> cell)
        {
            return time.deletes(cell);
        }

        public void digest(Digest digest)
        {
            time.digest(digest);
            digest.updateWithBoolean(isShadowable);
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

        public long unsharedHeapSize()
        {
            if(this == LIVE)
                return 0;

            return EMPTY_SIZE + time().unsharedHeapSize();
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
     *   6) No shadowed data should be added. Concretely, this means that if a a row deletion is added, it doesn't
     *      deletes the row timestamp or any cell added later, and similarly no cell added is deleted by the complex
     *      deletion of the column this is a cell of.
     *
     * An unsorted builder will not expect those last rules however: {@link #addCell} and {@link #addComplexDeletion}
     * can be done in any order. And in particular unsorted builder allows multiple calls for the same column/cell. In
     * that latter case, the result will follow the usual reconciliation rules (so equal cells are reconciled with
     * {@link Cells#reconcile} and the "biggest" of multiple complex deletion for the same column wins).
     */
    public interface Builder
    {
        /**
         * Creates a copy of this {@code Builder}.
         * @return a copy of this {@code Builder}
         */
        public Builder copy();

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
        public void newRow(Clustering<?> clustering);

        /**
         * The clustering for the row that is currently being built.
         *
         * @return the clustering for the row that is currently being built, or {@code null} if {@link #newRow} hasn't
         * yet been called.
         */
        public Clustering<?> clustering();

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
        public void addCell(Cell<?> cell);

        /**
         * Adds a complex deletion.
         *
         * @param column the column for which to add the {@code complexDeletion}.
         * @param complexDeletion the complex deletion time to add.
         */
        public void addComplexDeletion(ColumnMetadata column, DeletionTime complexDeletion);

        /**
         * Builds and return built row.
         *
         * @return the last row built by this builder.
         */
        public Row build();
    }

    /**
     * Row builder interface geared towards human.
     * <p>
     * Where the {@link Builder} deals with building rows efficiently from internal objects ({@code Cell}, {@code
     * LivenessInfo}, ...), the {@code SimpleBuilder} is geared towards building rows from string column name and
     * 'native' values (string for text, ints for numbers, et...). In particular, it is meant to be convenient, not
     * efficient, and should be used only in place where performance is not of the utmost importance (it is used to
     * build schema mutation for instance).
     * <p>
     * Also note that contrarily to {@link Builder}, the {@code SimpleBuilder} API has no {@code newRow()} method: it is
     * expected that the clustering of the row built is provided by the constructor of the builder.
     */
    public interface SimpleBuilder
    {
        /**
         * Sets the timestamp to use for the following additions.
         * <p>
         * Note that the for non-compact tables, this method must be called before any column addition for this
         * timestamp to be used for the row {@code LivenessInfo}.
         *
         * @param timestamp the timestamp to use for following additions. If that timestamp hasn't been set, the current
         * time in microseconds will be used.
         * @return this builder.
         */
        public SimpleBuilder timestamp(long timestamp);

        /**
         * Sets the ttl to use for the following additions.
         * <p>
         * Note that the for non-compact tables, this method must be called before any column addition for this
         * ttl to be used for the row {@code LivenessInfo}.
         *
         * @param ttl the ttl to use for following additions. If that ttl hasn't been set, no ttl will be used.
         * @return this builder.
         */
        public SimpleBuilder ttl(int ttl);

        /**
         * Adds a value to a given column.
         *
         * @param columnName the name of the column for which to add a new value.
         * @param value the value to add, which must be of the proper type for {@code columnName}. This can be {@code
         * null} in which case the this is equivalent to {@code delete(columnName)}.
         * @return this builder.
         */
        public SimpleBuilder add(String columnName, Object value);

        /**
         * Appends new values to a given non-frozen collection column.
         * <p>
         * This method is similar to {@code add()} but the collection elements added through this method are "appended"
         * to any pre-exising elements. In other words, this is like {@code add()} except that it doesn't delete the
         * previous value of the collection. This can only be called on non-frozen collection columns.
         * <p>
         * Note that this method can be used in replacement of {@code add()} if you know that there can't be any
         * pre-existing value for that column, in which case this is slightly less expensive as it avoid the collection
         * tombstone inherent to {@code add()}.
         *
         * @param columnName the name of the column for which to add a new value, which must be a non-frozen collection.
         * @param value the value to add, which must be of the proper type for {@code columnName} (in other words, it
         * <b>must</b> be a collection).
         * @return this builder.
         *
         * @throws IllegalArgumentException if columnName is not a non-frozen collection column.
         */
        public SimpleBuilder appendAll(String columnName, Object value);

        /**
         * Deletes the whole row.
         * <p>
         * If called, this is generally the only method called on the builder (outside of {@code timestamp()}.
         *
         * @return this builder.
         */
        public SimpleBuilder delete();

        /**
         * Deletes the whole row with a timestamp that is just before the new data's timestamp, to make sure no expired
         * data remains on the row.
         *
         * @return this builder.
         */
        public SimpleBuilder deletePrevious();

        /**
         * Removes the value for a given column (creating a tombstone).
         *
         * @param columnName the name of the column to delete.
         * @return this builder.
         */
        public SimpleBuilder delete(String columnName);

        /**
         * Don't include any primary key {@code LivenessInfo} in the built row.
         *
         * @return this builder.
         */
        public SimpleBuilder noPrimaryKeyLivenessInfo();

        /**
         * Returns the built row.
         *
         * @return the built row.
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

        private Clustering<?> clustering;
        private int rowsToMerge;
        private int lastRowSet = -1;

        private final List<ColumnData> dataBuffer = new ArrayList<>();
        private final ColumnDataReducer columnDataReducer;

        public Merger(int size, boolean hasComplex)
        {
            this.rows = new Row[size];
            this.columnDataIterators = new ArrayList<>(size);
            this.columnDataReducer = new ColumnDataReducer(size, hasComplex);
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

        @SuppressWarnings("resource")
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
                 : BTreeRow.create(clustering, rowInfo, rowDeletion, BTree.build(dataBuffer));
        }

        public Clustering<?> mergedClustering()
        {
            return clustering;
        }

        public Row[] mergedRows()
        {
            return rows;
        }

        private static class ColumnDataReducer extends MergeIterator.Reducer<ColumnData, ColumnData>
        {
            private ColumnMetadata column;
            private final List<ColumnData> versions;

            private DeletionTime activeDeletion;

            private final ComplexColumnData.Builder complexBuilder;
            private final List<Iterator<Cell<?>>> complexCells;
            private final CellReducer cellReducer;

            public ColumnDataReducer(int size, boolean hasComplex)
            {
                this.versions = new ArrayList<>(size);
                this.complexBuilder = hasComplex ? ComplexColumnData.builder() : null;
                this.complexCells = hasComplex ? new ArrayList<>(size) : null;
                this.cellReducer = new CellReducer();
            }

            public void setActiveDeletion(DeletionTime activeDeletion)
            {
                this.activeDeletion = activeDeletion;
            }

            public void reduce(int idx, ColumnData data)
            {
                if (useColumnMetadata(data.column()))
                    column = data.column();

                versions.add(data);
            }

            /**
             * Determines it the {@code ColumnMetadata} is the one that should be used.
             * @param dataColumn the {@code ColumnMetadata} to use.
             * @return {@code true} if the {@code ColumnMetadata} is the one that should be used, {@code false} otherwise.
             */
            private boolean useColumnMetadata(ColumnMetadata dataColumn)
            {
                if (column == null)
                    return true;

                return ColumnMetadataVersionComparator.INSTANCE.compare(column, dataColumn) < 0;
            }

            @SuppressWarnings("resource")
            protected ColumnData getReduced()
            {
                if (column.isSimple())
                {
                    Cell<?> merged = null;
                    for (int i=0, isize=versions.size(); i<isize; i++)
                    {
                        Cell<?> cell = (Cell<?>) versions.get(i);
                        if (!activeDeletion.deletes(cell))
                            merged = merged == null ? cell : Cells.reconcile(merged, cell);
                    }
                    return merged;
                }
                else
                {
                    complexBuilder.newColumn(column);
                    complexCells.clear();
                    DeletionTime complexDeletion = DeletionTime.LIVE;
                    for (int i=0, isize=versions.size(); i<isize; i++)
                    {
                        ColumnData data = versions.get(i);
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

                    Iterator<Cell<?>> cells = MergeIterator.get(complexCells, Cell.comparator, cellReducer);
                    while (cells.hasNext())
                    {
                        Cell<?> merged = cells.next();
                        if (merged != null)
                            complexBuilder.addCell(merged);
                    }
                    return complexBuilder.build();
                }
            }

            protected void onKeyChange()
            {
                column = null;
                versions.clear();
            }
        }

        private static class CellReducer extends MergeIterator.Reducer<Cell<?>, Cell<?>>
        {
            private DeletionTime activeDeletion;
            private Cell<?> merged;

            public void setActiveDeletion(DeletionTime activeDeletion)
            {
                this.activeDeletion = activeDeletion;
                onKeyChange();
            }

            public void reduce(int idx, Cell<?> cell)
            {
                if (!activeDeletion.deletes(cell))
                    merged = merged == null ? cell : Cells.reconcile(merged, cell);
            }

            protected Cell<?> getReduced()
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
