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
import java.util.Comparator;
import java.util.Iterator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Static methods to work on cells.
 */
public abstract class Cells
{
    private Cells() {}

    /**
     * Writes a tombstone cell to the provided writer.
     *
     * @param writer the {@code Row.Writer} to write the tombstone to.
     * @param column the column for the tombstone.
     * @param timestamp the timestamp for the tombstone.
     * @param localDeletionTime the local deletion time (in seconds) for the tombstone.
     */
    public static void writeTombstone(Row.Writer writer, ColumnDefinition column, long timestamp, int localDeletionTime)
    {
        writer.writeCell(column, false, ByteBufferUtil.EMPTY_BYTE_BUFFER, SimpleLivenessInfo.forDeletion(timestamp, localDeletionTime), null);
    }

    /**
     * Computes the difference between a cell and the result of merging this
     * cell to other cells.
     * <p>
     * This method is used when cells from multiple sources are merged and we want to
     * find for a given source if it was up to date for that cell, and if not, what
     * should be sent to the source to repair it.
     *
     * @param merged the cell that is the result of merging multiple source.
     * @param cell the cell from one of the source that has been merged to yied
     * {@code merged}.
     * @return {@code null} if the source having {@code cell} is up-to-date for that
     * cell, or a cell that applied to the source will "repair" said source otherwise.
     */
    public static Cell diff(Cell merged, Cell cell)
    {
        // Note that it's enough to check if merged is a counterCell. If it isn't and
        // cell is one, it means that merged is a tombstone with a greater timestamp
        // than cell, because that's the only case where reconciling a counter with
        // a tombstone don't yield a counter. If that's the case, the normal path will
        // return what it should.
        if (merged.isCounterCell())
        {
            if (merged.livenessInfo().supersedes(cell.livenessInfo()))
                return merged;

            // Reconciliation never returns something with a timestamp strictly lower than its operand. This
            // means we're in the case where merged.timestamp() == cell.timestamp(). As 1) tombstones
            // always win over counters (CASSANDRA-7346) and 2) merged is a counter, it follows that cell
            // can't be a tombstone or merged would be one too.
            assert !cell.isTombstone();

            CounterContext.Relationship rel = CounterContext.instance().diff(merged.value(), cell.value());
            return (rel == CounterContext.Relationship.GREATER_THAN || rel == CounterContext.Relationship.DISJOINT) ? merged : null;
        }
        return merged.livenessInfo().supersedes(cell.livenessInfo()) ? merged : null;
    }

    /**
     * Reconciles/merges two cells, one being an update to an existing cell,
     * yielding index updates if appropriate.
     * <p>
     * Note that this method assumes that the provided cells can meaningfully
     * be reconciled together, that is that those cells are for the same row and same
     * column (and same cell path if the column is complex).
     * <p>
     * Also note that which cell is provided as {@code existing} and which is
     * provided as {@code update} matters for index updates.
     *
     * @param clustering the clustering for the row the cells to merge originate from.
     * This is only used for index updates, so this can be {@code null} if
     * {@code indexUpdater == SecondaryIndexManager.nullUpdater}.
     * @param existing the pre-existing cell, the one that is updated. This can be
     * {@code null} if this reconciliation correspond to an insertion.
     * @param update the newly added cell, the update. This can be {@code null} out
     * of convenience, in which case this function simply copy {@code existing} to
     * {@code writer}.
     * @param deletion the deletion time that applies to the cells being considered.
     * This deletion time may delete both {@code existing} or {@code update}.
     * @param writer the row writer to which the result of the reconciliation is written.
     * @param nowInSec the current time in seconds (which plays a role during reconciliation
     * because deleted cells always have precedence on timestamp equality and deciding if a
     * cell is a live or not depends on the current time due to expiring cells).
     * @param indexUpdater an index updater to which the result of the reconciliation is
     * signaled (if relevant, that is if the update is not simply ignored by the reconciliation).
     * This cannot be {@code null} but {@code SecondaryIndexManager.nullUpdater} can be passed.
     *
     * @return the timestamp delta between existing and update, or {@code Long.MAX_VALUE} if one
     * of them is {@code null} or deleted by {@code deletion}).
     */
    public static long reconcile(Clustering clustering,
                                 Cell existing,
                                 Cell update,
                                 DeletionTime deletion,
                                 Row.Writer writer,
                                 int nowInSec,
                                 SecondaryIndexManager.Updater indexUpdater)
    {
        existing = existing == null || deletion.deletes(existing.livenessInfo()) ? null : existing;
        update = update == null || deletion.deletes(update.livenessInfo()) ? null : update;
        if (existing == null || update == null)
        {
            if (update != null)
            {
                // It's inefficient that we call maybeIndex (which is for primary key indexes) on every cell, but
                // we'll need to fix that damn 2ndary index API to avoid that.
                updatePKIndexes(clustering, update, nowInSec, indexUpdater);
                indexUpdater.insert(clustering, update);
                update.writeTo(writer);
            }
            else if (existing != null)
            {
                existing.writeTo(writer);
            }
            return Long.MAX_VALUE;
        }

        Cell reconciled = reconcile(existing, update, nowInSec);
        reconciled.writeTo(writer);

        // Note that this test rely on reconcile returning either 'existing' or 'update'. That's not true for counters but we don't index them
        if (reconciled == update)
        {
            updatePKIndexes(clustering, update, nowInSec, indexUpdater);
            indexUpdater.update(clustering, existing, reconciled);
        }
        return Math.abs(existing.livenessInfo().timestamp() - update.livenessInfo().timestamp());
    }

    private static void updatePKIndexes(Clustering clustering, Cell cell, int nowInSec, SecondaryIndexManager.Updater indexUpdater)
    {
        if (indexUpdater != SecondaryIndexManager.nullUpdater && cell.isLive(nowInSec))
            indexUpdater.maybeIndex(clustering, cell.livenessInfo().timestamp(), cell.livenessInfo().ttl(), DeletionTime.LIVE);
    }

    /**
     * Reconciles/merge two cells.
     * <p>
     * Note that this method assumes that the provided cells can meaningfully
     * be reconciled together, that is that cell are for the same row and same
     * column (and same cell path if the column is complex).
     * <p>
     * This method is commutative over it's cells arguments: {@code reconcile(a, b, n) == reconcile(b, a, n)}.
     *
     * @param c1 the first cell participating in the reconciliation.
     * @param c2 the second cell participating in the reconciliation.
     * @param nowInSec the current time in seconds (which plays a role during reconciliation
     * because deleted cells always have precedence on timestamp equality and deciding if a
     * cell is a live or not depends on the current time due to expiring cells).
     *
     * @return a cell corresponding to the reconciliation of {@code c1} and {@code c2}.
     * For non-counter cells, this will always be either {@code c1} or {@code c2}, but for
     * counter cells this can be a newly allocated cell.
     */
    public static Cell reconcile(Cell c1, Cell c2, int nowInSec)
    {
        if (c1 == null)
            return c2 == null ? null : c2;
        if (c2 == null)
            return c1;

        if (c1.isCounterCell() || c2.isCounterCell())
        {
            Conflicts.Resolution res = Conflicts.resolveCounter(c1.livenessInfo().timestamp(),
                                                                c1.isLive(nowInSec),
                                                                c1.value(),
                                                                c2.livenessInfo().timestamp(),
                                                                c2.isLive(nowInSec),
                                                                c2.value());

            switch (res)
            {
                case LEFT_WINS: return c1;
                case RIGHT_WINS: return c2;
                default:
                    ByteBuffer merged = Conflicts.mergeCounterValues(c1.value(), c2.value());
                    LivenessInfo mergedInfo = c1.livenessInfo().mergeWith(c2.livenessInfo());

                    // We save allocating a new cell object if it turns out that one cell was
                    // a complete superset of the other
                    if (merged == c1.value() && mergedInfo == c1.livenessInfo())
                        return c1;
                    else if (merged == c2.value() && mergedInfo == c2.livenessInfo())
                        return c2;
                    else // merge clocks and timestamps.
                        return create(c1.column(), true, merged, mergedInfo, null);
            }
        }

        Conflicts.Resolution res = Conflicts.resolveRegular(c1.livenessInfo().timestamp(),
                                                            c1.isLive(nowInSec),
                                                            c1.livenessInfo().localDeletionTime(),
                                                            c1.value(),
                                                            c2.livenessInfo().timestamp(),
                                                            c2.isLive(nowInSec),
                                                            c2.livenessInfo().localDeletionTime(),
                                                            c2.value());
        assert res != Conflicts.Resolution.MERGE;
        return res == Conflicts.Resolution.LEFT_WINS ? c1 : c2;
    }

    /**
     * Computes the reconciliation of a complex column given its pre-existing
     * cells and the ones it is updated with, and generating index update if
     * appropriate.
     * <p>
     * Note that this method assumes that the provided cells can meaningfully
     * be reconciled together, that is that the cells are for the same row and same
     * complex column.
     * <p>
     * Also note that which cells is provided as {@code existing} and which are
     * provided as {@code update} matters for index updates.
     *
     * @param clustering the clustering for the row the cells to merge originate from.
     * This is only used for index updates, so this can be {@code null} if
     * {@code indexUpdater == SecondaryIndexManager.nullUpdater}.
     * @param column the complex column the cells are for.
     * @param existing the pre-existing cells, the ones that are updated. This can be
     * {@code null} if this reconciliation correspond to an insertion.
     * @param update the newly added cells, the update. This can be {@code null} out
     * of convenience, in which case this function simply copy the cells from
     * {@code existing} to {@code writer}.
     * @param deletion the deletion time that applies to the cells being considered.
     * This deletion time may delete cells in both {@code existing} and {@code update}.
     * @param writer the row writer to which the result of the reconciliation is written.
     * @param nowInSec the current time in seconds (which plays a role during reconciliation
     * because deleted cells always have precedence on timestamp equality and deciding if a
     * cell is a live or not depends on the current time due to expiring cells).
     * @param indexUpdater an index updater to which the result of the reconciliation is
     * signaled (if relevant, that is if the updates are not simply ignored by the reconciliation).
     * This cannot be {@code null} but {@code SecondaryIndexManager.nullUpdater} can be passed.
     *
     * @return the smallest timestamp delta between corresponding cells from existing and update. A
     * timestamp delta being computed as the difference between a cell from {@code update} and the
     * cell in {@code existing} having the same cell path (if such cell exists). If the intersection
     * of cells from {@code existing} and {@code update} having the same cell path is empty, this
     * returns {@code Long.MAX_VALUE}.
     */
    public static long reconcileComplex(Clustering clustering,
                                        ColumnDefinition column,
                                        Iterator<Cell> existing,
                                        Iterator<Cell> update,
                                        DeletionTime deletion,
                                        Row.Writer writer,
                                        int nowInSec,
                                        SecondaryIndexManager.Updater indexUpdater)
    {
        Comparator<CellPath> comparator = column.cellPathComparator();
        Cell nextExisting = getNext(existing);
        Cell nextUpdate = getNext(update);
        long timeDelta = Long.MAX_VALUE;
        while (nextExisting != null || nextUpdate != null)
        {
            int cmp = nextExisting == null ? 1
                     : (nextUpdate == null ? -1
                     : comparator.compare(nextExisting.path(), nextUpdate.path()));
            if (cmp < 0)
            {
                reconcile(clustering, nextExisting, null, deletion, writer, nowInSec, indexUpdater);
                nextExisting = getNext(existing);
            }
            else if (cmp > 0)
            {
                reconcile(clustering, null, nextUpdate, deletion, writer, nowInSec, indexUpdater);
                nextUpdate = getNext(update);
            }
            else
            {
                timeDelta = Math.min(timeDelta, reconcile(clustering, nextExisting, nextUpdate, deletion, writer, nowInSec, indexUpdater));
                nextExisting = getNext(existing);
                nextUpdate = getNext(update);
            }
        }
        return timeDelta;
    }

    private static Cell getNext(Iterator<Cell> iterator)
    {
        return iterator == null || !iterator.hasNext() ? null : iterator.next();
    }

    /**
     * Creates a simple cell.
     * <p>
     * Note that in general cell objects are created by the container they are in and so this method should
     * only be used in a handful of cases when we know it's the right thing to do.
     *
     * @param column the column for the cell to create.
     * @param isCounter whether the create cell should be a counter one.
     * @param value the value for the cell.
     * @param info the liveness info for the cell.
     * @param path the cell path for the cell.
     * @return the newly allocated cell object.
     */
    public static Cell create(ColumnDefinition column, boolean isCounter, ByteBuffer value, LivenessInfo info, CellPath path)
    {
        return new SimpleCell(column, isCounter, value, info, path);
    }

    private static class SimpleCell extends AbstractCell
    {
        private final ColumnDefinition column;
        private final boolean isCounter;
        private final ByteBuffer value;
        private final LivenessInfo info;
        private final CellPath path;

        private SimpleCell(ColumnDefinition column, boolean isCounter, ByteBuffer value, LivenessInfo info, CellPath path)
        {
            this.column = column;
            this.isCounter = isCounter;
            this.value = value;
            this.info = info.takeAlias();
            this.path = path;
        }

        public ColumnDefinition column()
        {
            return column;
        }

        public boolean isCounterCell()
        {
            return isCounter;
        }

        public ByteBuffer value()
        {
            return value;
        }

        public LivenessInfo livenessInfo()
        {
            return info;
        }

        public CellPath path()
        {
            return path;
        }
    }
}
