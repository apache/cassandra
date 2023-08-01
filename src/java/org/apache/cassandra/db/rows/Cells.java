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

import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.partitions.PartitionStatisticsCollector;

/**
 * Static methods to work on cells.
 */
public abstract class Cells
{
    private Cells() {}

    /**
     * Collect statistics ont a given cell.
     *
     * @param cell the cell for which to collect stats.
     * @param collector the stats collector.
     */
    public static void collectStats(Cell<?> cell, PartitionStatisticsCollector collector)
    {
        collector.update(cell);

        if (cell.isCounterCell())
            collector.updateHasLegacyCounterShards(CounterCells.hasLegacyShards(cell));
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
     *
     * @return a cell corresponding to the reconciliation of {@code c1} and {@code c2}.
     * For non-counter cells, this will always be either {@code c1} or {@code c2}, but for
     * counter cells this can be a newly allocated cell.
     */
    public static Cell<?> reconcile(Cell<?> c1, Cell<?> c2)
    {
        if (c1 == null || c2 == null)
            return c2 == null ? c1 : c2;

        if (c1.isCounterCell() || c2.isCounterCell())
            return resolveCounter(c1, c2);

        return resolveRegular(c1, c2);
    }

    private static Cell<?> resolveRegular(Cell<?> left, Cell<?> right)
    {
        long leftTimestamp = left.timestamp();
        long rightTimestamp = right.timestamp();
        if (leftTimestamp != rightTimestamp)
            return leftTimestamp > rightTimestamp ? left : right;

        long leftLocalDeletionTime = left.localDeletionTime();
        long rightLocalDeletionTime = right.localDeletionTime();

        boolean leftIsExpiringOrTombstone = leftLocalDeletionTime != Cell.NO_DELETION_TIME;
        boolean rightIsExpiringOrTombstone = rightLocalDeletionTime != Cell.NO_DELETION_TIME;

        if (leftIsExpiringOrTombstone | rightIsExpiringOrTombstone)
        {
            // Tombstones always win reconciliation with live cells of the same timstamp
            // CASSANDRA-14592: for consistency of reconciliation, regardless of system clock at time of reconciliation
            // this requires us to treat expiring cells (which will become tombstones at some future date) the same wrt regular cells
            if (leftIsExpiringOrTombstone != rightIsExpiringOrTombstone)
                return leftIsExpiringOrTombstone ? left : right;

            // for most historical consistency, we still prefer tombstones over expiring cells.
            // While this leads to the an inconsistency over which is chosen
            // (i.e. before expiry, the pure tombstone; after expiry, whichever is more recent)
            // this inconsistency has no user-visible distinction, as at this point they are both logically tombstones
            // (the only possible difference is the time at which the cells become purgeable)
            boolean leftIsTombstone = !left.isExpiring(); // !isExpiring() == isTombstone(), but does not need to consider localDeletionTime()
            boolean rightIsTombstone = !right.isExpiring();
            if (leftIsTombstone != rightIsTombstone)
                return leftIsTombstone ? left : right;

            // ==> (leftIsExpiring && rightIsExpiring) or (leftIsTombstone && rightIsTombstone)
            // if both are expiring, we do not want to consult the value bytes if we can avoid it, as like with C-14592
            // the value bytes implicitly depend on the system time at reconciliation, as a
            // would otherwise always win (unless it had an empty value), until it expired and was translated to a tombstone
            if (leftLocalDeletionTime != rightLocalDeletionTime)
                return leftLocalDeletionTime > rightLocalDeletionTime ? left : right;
        }

        return compareValues(left, right) >= 0 ? left : right;
    }

    private static Cell<?> resolveCounter(Cell<?> left, Cell<?> right)
    {
        long leftTimestamp = left.timestamp();
        long rightTimestamp = right.timestamp();

        boolean leftIsTombstone = left.isTombstone();
        boolean rightIsTombstone = right.isTombstone();

        if (leftIsTombstone | rightIsTombstone)
        {
            // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
            assert leftIsTombstone != rightIsTombstone;
            return leftIsTombstone ? left : right;
        }

        ByteBuffer leftValue = left.buffer();
        ByteBuffer rightValue = right.buffer();

        // Handle empty values. Counters can't truly have empty values, but we can have a counter cell that temporarily
        // has one on read if the column for the cell is not queried by the user due to the optimization of #10657. We
        // thus need to handle this (see #11726 too).
        boolean leftIsEmpty = !leftValue.hasRemaining();
        boolean rightIsEmpty = !rightValue.hasRemaining();
        if (leftIsEmpty || rightIsEmpty)
        {
            if (leftIsEmpty != rightIsEmpty)
                return leftIsEmpty ? left : right;
            return leftTimestamp > rightTimestamp ? left : right;
        }

        ByteBuffer merged = CounterContext.instance().merge(leftValue, rightValue);
        long timestamp = Math.max(leftTimestamp, rightTimestamp);

        // We save allocating a new cell object if it turns out that one cell was
        // a complete superset of the other
        if (merged == leftValue && timestamp == leftTimestamp)
            return left;
        else if (merged == rightValue && timestamp == rightTimestamp)
            return right;
        else // merge clocks and timestamps.
            return new BufferCell(left.column(), timestamp, Cell.NO_TTL, Cell.NO_DELETION_TIME, merged, left.path());
    }

    /**
     * Adds to the builder a representation of the given existing cell that, when merged/reconciled with the given
     * update cell, produces the same result as merging the original with the update.
     * <p>
     * For simple cells that is either the original cell (if still live), or nothing (if shadowed).
     *
     * @param existing the pre-existing cell, the one that is updated.
     * @param update the newly added cell, the update. This can be {@code null} out
     * of convenience, in which case this function simply copy {@code existing} to
     * {@code writer}.
     * @param deletion the deletion time that applies to the cells being considered.
     * This deletion time may delete both {@code existing} or {@code update}.
     * @param builder the row builder to which the result of the filtering is written.
     */
    public static void addNonShadowed(Cell<?> existing,
                                      Cell<?> update,
                                      DeletionTime deletion,
                                      Row.Builder builder)
    {
        if (deletion.deletes(existing))
            return;

        Cell<?> reconciled = reconcile(existing, update);
        if (reconciled != update)
            builder.addCell(existing);
    }

    /**
     * Adds to the builder a representation of the given existing cell that, when merged/reconciled with the given
     * update cell, produces the same result as merging the original with the update.
     * <p>
     * For simple cells that is either the original cell (if still live), or nothing (if shadowed).
     *
     * @param column the complex column the cells are for.
     * @param existing the pre-existing cells, the ones that are updated.
     * @param update the newly added cells, the update. This can be {@code null} out
     * of convenience, in which case this function simply copy the cells from
     * {@code existing} to {@code writer}.
     * @param deletion the deletion time that applies to the cells being considered.
     * This deletion time may delete both {@code existing} or {@code update}.
     * @param builder the row builder to which the result of the filtering is written.
     */
    public static void addNonShadowedComplex(ColumnMetadata column,
                                             Iterator<Cell<?>> existing,
                                             Iterator<Cell<?>> update,
                                             DeletionTime deletion,
                                             Row.Builder builder)
    {
        Comparator<CellPath> comparator = column.cellPathComparator();
        Cell<?> nextExisting = getNext(existing);
        Cell<?> nextUpdate = getNext(update);
        while (nextExisting != null)
        {
            int cmp = nextUpdate == null ? -1 : comparator.compare(nextExisting.path(), nextUpdate.path());
            if (cmp < 0)
            {
                addNonShadowed(nextExisting, null, deletion, builder);
                nextExisting = getNext(existing);
            }
            else if (cmp == 0)
            {
                addNonShadowed(nextExisting, nextUpdate, deletion, builder);
                nextExisting = getNext(existing);
                nextUpdate = getNext(update);
            }
            else
            {
                nextUpdate = getNext(update);
            }
        }
    }

    private static Cell<?> getNext(Iterator<Cell<?>> iterator)
    {
        return iterator == null || !iterator.hasNext() ? null : iterator.next();
    }

    private static <L, R> int compareValues(Cell<L> left, Cell<R> right)
    {
        return ValueAccessor.compare(left.value(), left.accessor(), right.value(), right.accessor());
    }

    public static <L, R> boolean valueEqual(Cell<L> left, Cell<R> right)
    {
        return ValueAccessor.equals(left.value(), left.accessor(), right.value(), right.accessor());
    }

    public static <T, V> T composeValue(Cell<V> cell, AbstractType<T> type)
    {
        return type.compose(cell.value(), cell.accessor());
    }

    public static <V> String valueString(Cell<V> cell, AbstractType<?> type)
    {
        return type.getString(cell.value(), cell.accessor());
    }

    public static <V> String valueString(Cell<V> cell)
    {
        return valueString(cell, cell.column().type);
    }
}
