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

import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.SearchIterator;

/**
 * Storage engine representation of a row.
 *
 * A row is identified by it's clustering column values (it's an Unfiltered),
 * has row level informations (deletion and partition key liveness infos (see below))
 * and contains data (Cells) regarding the columns it contains.
 *
 * A row implements {@code WithLivenessInfo} and has thus a timestamp, ttl and
 * local deletion time. Those information do not apply to the row content, they
 * apply to the partition key columns. In other words, the timestamp is the
 * timestamp for the partition key columns: it is what allows to distinguish
 * between a dead row, and a live row but for which only the partition key columns
 * are set. The ttl and local deletion time information are for the case where
 * a TTL is set on those partition key columns. Note however that a row can have
 * live cells but no partition key columns timestamp, because said timestamp (and
 * its corresponding ttl) is only set on INSERT (not UPDATE).
 */
public interface Row extends Unfiltered, Iterable<Cell>, Aliasable<Row>
{
    /**
     * The clustering values for this row.
     */
    @Override
    public Clustering clustering();

    /**
     * The columns this row contains.
     *
     * Note that this is actually a superset of the columns the row contains. The row
     * may not have values for each of those columns, but it can't have values for other
     * columns.
     *
     * @return a superset of the columns contained in this row.
     */
    public Columns columns();

    /**
     * The row deletion.
     *
     * This correspond to the last row deletion done on this row.
     *
     * @return the row deletion.
     */
    public DeletionTime deletion();

    /**
     * Liveness information for the primary key columns of this row.
     * <p>
     * As a row is uniquely identified by its primary key, all its primary key columns
     * share the same {@code LivenessInfo}. This liveness information is what allows us
     * to distinguish between a dead row (it has no live cells and its primary key liveness
     * info has no timestamp) and a live row but where all non PK columns are null (it has no
     * live cells, but its primary key liveness has a timestamp). Please note that the ttl
     * (and local deletion time) of the PK liveness information only apply to the
     * liveness info timestamp, and not to the content of the row. Also note that because
     * in practice there is not way to only delete the primary key columns (without deleting
     * the row itself), the returned {@code LivenessInfo} can only have a local deletion time
     * if it has a TTL.
     * <p>
     * Lastly, note that it is possible for a row to have live cells but no PK liveness
     * info timestamp, because said timestamp is only set on {@code INSERT} (which makes sense
     * in itself, see #6782) but live cells can be add through {@code UPDATE} even if the row
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
     * Whether the row has no information whatsoever. This means no row infos
     * (timestamp, ttl, deletion), no cells and no complex deletion info.
     *
     * @return {@code true} if the row has no data whatsoever, {@code false} otherwise.
     */
    public boolean isEmpty();

    /**
     * Whether the row has some live information (i.e. it's not just deletion informations).
     */
    public boolean hasLiveData(int nowInSec);

    /**
     * Whether or not this row contains any deletion for a complex column. That is if
     * there is at least one column for which {@code getDeletion} returns a non
     * live deletion time.
     */
    public boolean hasComplexDeletion();

    /**
     * Returns a cell for a simple column.
     *
     * Calls to this method are allowed to return the same Cell object, and hence the returned
     * object is only valid until the next getCell/getCells call on the same Row object. You will need
     * to copy the returned data if you plan on using a reference to the Cell object
     * longer than that.
     *
     * @param c the simple column for which to fetch the cell.
     * @return the corresponding cell or {@code null} if the row has no such cell.
     */
    public Cell getCell(ColumnDefinition c);

    /**
     * Return a cell for a given complex column and cell path.
     *
     * Calls to this method are allowed to return the same Cell object, and hence the returned
     * object is only valid until the next getCell/getCells call on the same Row object. You will need
     * to copy the returned data if you plan on using a reference to the Cell object
     * longer than that.
     *
     * @param c the complex column for which to fetch the cell.
     * @param path the cell path for which to fetch the cell.
     * @return the corresponding cell or {@code null} if the row has no such cell.
     */
    public Cell getCell(ColumnDefinition c, CellPath path);

    /**
     * Returns an iterator on the cells of a complex column c.
     *
     * Calls to this method are allowed to return the same iterator object, and
     * hence the returned object is only valid until the next getCell/getCells call
     * on the same Row object. You will need to copy the returned data if you
     * plan on using a reference to the Cell object longer than that.
     *
     * @param c the complex column for which to fetch the cells.
     * @return an iterator on the cells of complex column {@code c} or {@code null} if the row has no
     * cells for that column.
     */
    public Iterator<Cell> getCells(ColumnDefinition c);

    /**
     * Deletion informations for complex columns.
     *
     * @param c the complex column for which to fetch deletion info.
     * @return the deletion time for complex column {@code c} in this row.
     */
    public DeletionTime getDeletion(ColumnDefinition c);

    /**
     * An iterator over the cells of this row.
     *
     * The iterator guarantees that for 2 rows of the same partition, columns
     * are returned in a consistent order in the sense that if the cells for
     * column c1 is returned before the cells for column c2 by the first iterator,
     * it is also the case for the 2nd iterator.
     *
     * The object returned by a call to next() is only guaranteed to be valid until
     * the next call to hasNext() or next(). If a consumer wants to keep a
     * reference on the returned Cell objects for longer than the iteration, it must
     * make a copy of it explicitly.
     *
     * @return an iterator over the cells of this row.
     */
    public Iterator<Cell> iterator();

    /**
     * An iterator to efficiently search data for a given column.
     *
     * @return a search iterator for the cells of this row.
     */
    public SearchIterator<ColumnDefinition, ColumnData> searchIterator();

    /**
     * Copy this row to the provided writer.
     *
     * @param writer the row writer to write this row to.
     */
    public void copyTo(Row.Writer writer);

    public String toString(CFMetaData metadata, boolean fullDetails);

    /**
     * Interface for writing a row.
     * <p>
     * Clients of this interface should abid to the following assumptions:
     *   1) if the row has a non empty clustering (it's not a static one and it doesn't belong to a table without
     *      clustering columns), then that clustering should be the first thing written (through
     *      {@link ClusteringPrefix.Writer#writeClusteringValue})).
     *   2) for a given complex column, calls to {@link #writeCell} are performed consecutively (without
     *      any call to {@code writeCell} for another column intermingled) and in {@code CellPath} order.
     *   3) {@link #endOfRow} is always called to end the writing of a given row.
     */
    public interface Writer extends ClusteringPrefix.Writer
    {
        /**
         * Writes the livness information for the partition key columns of this row.
         *
         * This call is optional: skipping it is equivalent to calling {@code writePartitionKeyLivenessInfo(LivenessInfo.NONE)}.
         *
         * @param info the liveness information for the partition key columns of the written row.
         */
        public void writePartitionKeyLivenessInfo(LivenessInfo info);

        /**
         * Writes the deletion information for this row.
         *
         * This call is optional and can be skipped if the row is not deleted.
         *
         * @param deletion the row deletion time, or {@code DeletionTime.LIVE} if the row isn't deleted.
         */
        public void writeRowDeletion(DeletionTime deletion);

        /**
         * Writes a cell to the writer.
         *
         * As mentionned above, add cells for a given column should be added consecutively (and in {@code CellPath} order for complex columns).
         *
         * @param column the column for the written cell.
         * @param isCounter whether or not this is a counter cell.
         * @param value the value for the cell. For tombstones, which don't have values, this should be an empty buffer.
         * @param info the cell liveness information.
         * @param path the {@link CellPath} for complex cells and {@code null} for regular cells.
         */
        public void writeCell(ColumnDefinition column, boolean isCounter, ByteBuffer value, LivenessInfo info, CellPath path);

        /**
         * Writes a deletion for a complex column, that is one that apply to all cells of the complex column.
         *
         * @param column the (complex) column this is a deletion for.
         * @param complexDeletion the deletion time.
         */
        public void writeComplexDeletion(ColumnDefinition column, DeletionTime complexDeletion);

        /**
         * Should be called to indicates that the row has been fully written.
         */
        public void endOfRow();
    }

    /**
     * Utility class to help merging rows from multiple inputs (UnfilteredRowIterators).
     */
    public abstract static class Merger
    {
        private final CFMetaData metadata;
        private final int nowInSec;
        private final UnfilteredRowIterators.MergeListener listener;
        private final Columns columns;

        private Clustering clustering;
        private final Row[] rows;
        private int rowsToMerge;

        private LivenessInfo rowInfo = LivenessInfo.NONE;
        private DeletionTime rowDeletion = DeletionTime.LIVE;

        private final Cell[] cells;
        private final List<Iterator<Cell>> complexCells;
        private final ComplexColumnReducer complexReducer = new ComplexColumnReducer();

        // For the sake of the listener if there is one
        private final DeletionTime[] complexDelTimes;

        private boolean signaledListenerForRow;

        public static Merger createStatic(CFMetaData metadata, int size, int nowInSec, Columns columns, UnfilteredRowIterators.MergeListener listener)
        {
            return new StaticMerger(metadata, size, nowInSec, columns, listener);
        }

        public static Merger createRegular(CFMetaData metadata, int size, int nowInSec, Columns columns, UnfilteredRowIterators.MergeListener listener)
        {
            return new RegularMerger(metadata, size, nowInSec, columns, listener);
        }

        protected Merger(CFMetaData metadata, int size, int nowInSec, Columns columns, UnfilteredRowIterators.MergeListener listener)
        {
            this.metadata = metadata;
            this.nowInSec = nowInSec;
            this.listener = listener;
            this.columns = columns;
            this.rows = new Row[size];
            this.complexCells = new ArrayList<>(size);

            this.cells = new Cell[size];
            this.complexDelTimes = listener == null ? null : new DeletionTime[size];
        }

        public void clear()
        {
            Arrays.fill(rows, null);
            Arrays.fill(cells, null);
            if (complexDelTimes != null)
                Arrays.fill(complexDelTimes, null);
            complexCells.clear();
            rowsToMerge = 0;

            rowInfo = LivenessInfo.NONE;
            rowDeletion = DeletionTime.LIVE;

            signaledListenerForRow = false;
        }

        public void add(int i, Row row)
        {
            clustering = row.clustering();
            rows[i] = row;
            ++rowsToMerge;
        }

        protected abstract Row.Writer getWriter();
        protected abstract Row getRow();

        public Row merge(DeletionTime activeDeletion)
        {
            // If for this clustering we have only one row version and have no activeDeletion (i.e. nothing to filter out),
            // then we can just return that single row (we also should have no listener)
            if (rowsToMerge == 1 && activeDeletion.isLive() && listener == null)
            {
                for (int i = 0; i < rows.length; i++)
                    if (rows[i] != null)
                        return rows[i];
                throw new AssertionError();
            }

            Row.Writer writer = getWriter();
            Rows.writeClustering(clustering, writer);

            for (int i = 0; i < rows.length; i++)
            {
                if (rows[i] == null)
                    continue;

                rowInfo = rowInfo.mergeWith(rows[i].primaryKeyLivenessInfo());

                if (rows[i].deletion().supersedes(rowDeletion))
                    rowDeletion = rows[i].deletion();
            }

            if (rowDeletion.supersedes(activeDeletion))
                activeDeletion = rowDeletion;

            if (activeDeletion.deletes(rowInfo))
                rowInfo = LivenessInfo.NONE;

            writer.writePartitionKeyLivenessInfo(rowInfo);
            writer.writeRowDeletion(rowDeletion);

            for (int i = 0; i < columns.simpleColumnCount(); i++)
            {
                ColumnDefinition c = columns.getSimple(i);
                for (int j = 0; j < rows.length; j++)
                    cells[j] = rows[j] == null ? null : rows[j].getCell(c);

                reconcileCells(activeDeletion, writer);
            }

            complexReducer.activeDeletion = activeDeletion;
            complexReducer.writer = writer;
            for (int i = 0; i < columns.complexColumnCount(); i++)
            {
                ColumnDefinition c = columns.getComplex(i);

                DeletionTime maxComplexDeletion = DeletionTime.LIVE;
                for (int j = 0; j < rows.length; j++)
                {
                    if (rows[j] == null)
                        continue;

                    DeletionTime dt = rows[j].getDeletion(c);
                    if (complexDelTimes != null)
                        complexDelTimes[j] = dt;

                    if (dt.supersedes(maxComplexDeletion))
                        maxComplexDeletion = dt;
                }

                boolean overrideActive = maxComplexDeletion.supersedes(activeDeletion);
                maxComplexDeletion =  overrideActive ? maxComplexDeletion : DeletionTime.LIVE;
                writer.writeComplexDeletion(c, maxComplexDeletion);
                if (listener != null)
                    listener.onMergedComplexDeletion(c, maxComplexDeletion, complexDelTimes);

                mergeComplex(overrideActive ? maxComplexDeletion : activeDeletion, c);
            }
            writer.endOfRow();

            Row row = getRow();
            // Because shadowed cells are skipped, the row could be empty. In which case
            // we return null (we also don't want to signal anything in that case since that
            // means everything in the row was shadowed and the listener will have been signalled
            // for whatever shadows it).
            if (row.isEmpty())
                return null;

            maybeSignalEndOfRow();
            return row;
        }

        private void maybeSignalListenerForRow()
        {
            if (listener != null && !signaledListenerForRow)
            {
                listener.onMergingRows(clustering, rowInfo, rowDeletion, rows);
                signaledListenerForRow = true;
            }
        }

        private void maybeSignalListenerForCell(Cell merged, Cell[] versions)
        {
            if (listener != null)
            {
                maybeSignalListenerForRow();
                listener.onMergedCells(merged, versions);
            }
        }

        private void maybeSignalEndOfRow()
        {
            if (listener != null)
            {
                // If we haven't signaled the listener yet (we had no cells but some deletion info), do it now
                maybeSignalListenerForRow();
                listener.onRowDone();
            }
        }

        private void reconcileCells(DeletionTime activeDeletion, Row.Writer writer)
        {
            Cell reconciled = null;
            for (int j = 0; j < cells.length; j++)
            {
                Cell cell = cells[j];
                if (cell != null && !activeDeletion.deletes(cell.livenessInfo()))
                    reconciled = Cells.reconcile(reconciled, cell, nowInSec);
            }

            if (reconciled != null)
            {
                reconciled.writeTo(writer);
                maybeSignalListenerForCell(reconciled, cells);
            }
        }

        private void mergeComplex(DeletionTime activeDeletion, ColumnDefinition c)
        {
            complexCells.clear();
            for (int j = 0; j < rows.length; j++)
            {
                Row row = rows[j];
                Iterator<Cell> iter = row == null ? null : row.getCells(c);
                complexCells.add(iter == null ? Iterators.<Cell>emptyIterator() : iter);
            }

            complexReducer.column = c;
            complexReducer.activeDeletion = activeDeletion;

            // Note that we use the mergeIterator only to group cells to merge, but we
            // write the result to the writer directly in the reducer, so all we care
            // about is iterating over the result.
            Iterator<Void> iter = MergeIterator.get(complexCells, c.cellComparator(), complexReducer);
            while (iter.hasNext())
                iter.next();
        }

        private class ComplexColumnReducer extends MergeIterator.Reducer<Cell, Void>
        {
            private DeletionTime activeDeletion;
            private Row.Writer writer;
            private ColumnDefinition column;

            public void reduce(int idx, Cell current)
            {
                cells[idx] = current;
            }

            protected Void getReduced()
            {
                reconcileCells(activeDeletion, writer);
                return null;
            }

            protected void onKeyChange()
            {
                Arrays.fill(cells, null);
            }
        }

        private static class StaticMerger extends Merger
        {
            private final StaticRow.Builder builder;

            private StaticMerger(CFMetaData metadata, int size, int nowInSec, Columns columns, UnfilteredRowIterators.MergeListener listener)
            {
                super(metadata, size, nowInSec, columns, listener);
                this.builder = StaticRow.builder(columns, true, metadata.isCounter());
            }

            protected Row.Writer getWriter()
            {
                return builder;
            }

            protected Row getRow()
            {
                return builder.build();
            }
        }

        private static class RegularMerger extends Merger
        {
            private final ReusableRow row;

            private RegularMerger(CFMetaData metadata, int size, int nowInSec, Columns columns, UnfilteredRowIterators.MergeListener listener)
            {
                super(metadata, size, nowInSec, columns, listener);
                this.row = new ReusableRow(metadata.clusteringColumns().size(), columns, true, metadata.isCounter());
            }

            protected Row.Writer getWriter()
            {
                return row.writer();
            }

            protected Row getRow()
            {
                return row;
            }
        }
    }
}
