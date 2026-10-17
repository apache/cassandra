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

import java.io.DataInput;
import java.nio.ByteBuffer;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.TableMetadata;

@NotThreadSafe
public class DeserializationHelper
{
    /**
     * Flag affecting deserialization behavior (this only affect counters in practice).
     *  - LOCAL: for deserialization of local data (Expired columns are
     *      converted to tombstones (to gain disk space)).
     *  - FROM_REMOTE: for deserialization of data received from remote hosts
     *      (Expired columns are converted to tombstone and counters have
     *      their delta cleared)
     *  - PRESERVE_SIZE: used when no transformation must be performed, i.e,
     *      when we must ensure that deserializing and reserializing the
     *      result yield the exact same bytes. Streaming uses this.
     */
    public enum Flag
    {
        LOCAL, FROM_REMOTE, PRESERVE_SIZE
    }

    private final Flag flag;
    public final int version;

    private final ColumnFilter columnsToFetch;
    private ColumnFilter.Tester tester;

    private final boolean hasDroppedColumns;
    private final Map<ByteBuffer, DroppedColumn> droppedColumns;
    private DroppedColumn currentDroppedComplex;

    // reusable fields to avoid extra allocation during cells processing
    // within org.apache.cassandra.db.rows.UnfilteredSerializer.deserializeRowBody
    DataInputPlus in;
    SerializationHeader header;
    Row.Builder builder;
    LivenessInfo livenessInfo;
    boolean hasComplexDeletion;

    // Reusable per-partition tracker for row-size-bounded reads in SSTable deserialization.
    private TrackedDataInputPlus trackedInput;

    public DeserializationHelper(TableMetadata metadata, int version, Flag flag, ColumnFilter columnsToFetch)
    {
        this.flag = flag;
        this.version = version;
        this.columnsToFetch = columnsToFetch;
        this.droppedColumns = metadata.droppedColumns;
        this.hasDroppedColumns = droppedColumns.size() > 0;
    }

    public DeserializationHelper(TableMetadata metadata, int version, Flag flag)
    {
        this(metadata, version, flag, null);
    }

    public boolean includes(ColumnMetadata column)
    {
        return columnsToFetch == null || columnsToFetch.fetches(column);
    }

    public boolean includes(Cell<?> cell, LivenessInfo rowLiveness)
    {
        if (columnsToFetch == null)
            return true;

        // During queries, some columns are included even though they are not queried by the user because
        // we always need to distinguish between having a row (with potentially only null values) and not
        // having a row at all (see #CASSANDRA-7085 for background). In the case where the column is not
        // actually requested by the user however (canSkipValue), we can skip the full cell if the cell
        // timestamp is lower than the row one, because in that case, the row timestamp is enough proof
        // of the liveness of the row. Otherwise, we'll only be able to skip the values of those cells.
        ColumnMetadata column = cell.column();
        if (column.isComplex())
        {
            if (!includes(cell.path()))
                return false;

            return !canSkipValue(cell.path()) || cell.timestamp() >= rowLiveness.timestamp();
        }
        else
        {
            return columnsToFetch.fetchedColumnIsQueried(column) || cell.timestamp() >= rowLiveness.timestamp();
        }
    }

    public boolean includes(CellPath path)
    {
        return path == null || tester == null || tester.fetches(path);
    }

    public boolean canSkipValue(ColumnMetadata column)
    {
        return columnsToFetch != null && !columnsToFetch.fetchedColumnIsQueried(column);
    }

    public boolean canSkipValue(CellPath path)
    {
        return path != null && tester != null && !tester.fetchedCellIsQueried(path);
    }

    public void startOfComplexColumn(ColumnMetadata column)
    {
        this.tester = columnsToFetch == null ? null : columnsToFetch.newTester(column);
        this.currentDroppedComplex = droppedColumns.get(column.name.bytes);
    }

    public void endOfComplexColumn()
    {
        this.tester = null;
    }

    public boolean isDropped(Cell<?> cell, boolean isComplex)
    {
        return isDropped(cell.column(), cell.timestamp(), isComplex);
    }

    /**
     * The drop rule: discard anything written at or before the column's drop time.
     * Every other form here resolves a {@link DroppedColumn} and defers * to this,
     * except {@link #isDroppedAtHorizon}, which cannot because it holds only the time.
     *
     * @param dropped the cell's column's drop record, or null if it has none
     */
    private static boolean isDroppedAt(long timestamp, DroppedColumn dropped)
    {
        return dropped != null && timestamp <= dropped.droppedTime;
    }

    /**
     * Drop rule over the decoded cell header rather than a {@link Cell}: the body of
     * {@link #isDropped(Cell, boolean)}. Its simple-column branch looks the column up by name on a
     * table that has dropped columns, so a reader that decodes cell headers in bulk builds a
     * {@link #droppedTimeOrMin} array once per column set and tests that with
     * {@link #isDroppedAtHorizon} instead.
     *
     * @param column    the cell's column; ignored when {@code isComplex}
     * @param timestamp the cell's timestamp
     * @param isComplex reads the {@link #currentDroppedComplex} cache instead of looking the column
     *                  up by name. Only correct after {@link #startOfComplexColumn} has primed that
     *                  cache for this column; a caller that has not filters nothing.
     */
    public boolean isDropped(ColumnMetadata column, long timestamp, boolean isComplex)
    {
        if (!hasDroppedColumns)
            return false;

        return isDroppedAt(timestamp, isComplex ? currentDroppedComplex : droppedColumns.get(column.name.bytes));
    }

    public boolean hasDroppedColumns()
    {
        return hasDroppedColumns;
    }

    /**
     * The sentinel {@link #droppedTimeOrMin} returns for a column with no drop horizon. It is the
     * least possible {@code long}, so {@code timestamp <= NO_DROP_HORIZON} is NOT unconditionally
     * false — see {@link #isDroppedAtHorizon}, which is why that predicate exists rather than a
     * bare comparison at each call site.
     */
    public static final long NO_DROP_HORIZON = Long.MIN_VALUE;

    /**
     * The column's drop horizon, or {@link #NO_DROP_HORIZON} if it has none. For callers that build
     * a per-superset array of these once (e.g. {@code SSTableCursorReader.CellCursor}), so a per-cell
     * drop check becomes a plain array read plus {@link #isDroppedAtHorizon} instead of a map lookup.
     */
    public long droppedTimeOrMin(ColumnMetadata column)
    {
        if (!hasDroppedColumns)
            return NO_DROP_HORIZON;
        DroppedColumn dropped = droppedColumns.get(column.name.bytes);
        return dropped != null ? dropped.droppedTime : NO_DROP_HORIZON;
    }

    /**
     * The drop rule against a horizon read from a {@link #droppedTimeOrMin} array. The one form of
     * the rule that cannot defer to {@link #isDroppedAt}, because the array holds the drop time
     * without the {@link DroppedColumn} it came from. Equivalent to
     * {@code isDropped(column, timestamp, false)} for the column the horizon came from, except for
     * a column whose recorded {@code droppedTime} is {@code Long.MIN_VALUE}: the array cannot tell
     * that apart from "no drop record". No schema path records such a drop time
     * ({@code AlterTableStatement} uses the schema mutation timestamp, a CQL DROP uses
     * {@code Long.MAX_VALUE}), so the encoding is sound in practice, but it is lossy.
     *
     * The {@code NO_DROP_HORIZON} test is required. That sentinel is {@code Long.MIN_VALUE}, so
     * {@code timestamp <= dropHorizon} alone would discard a cell written at
     * {@code Long.MIN_VALUE} on a column that was never dropped.
     */
    public static boolean isDroppedAtHorizon(long timestamp, long dropHorizon)
    {
        return dropHorizon != NO_DROP_HORIZON && timestamp <= dropHorizon;
    }

    /** True if {@code column} has a drop horizon at all, whatever the timestamp. */
    public boolean isDroppedColumn(ColumnMetadata column)
    {
        return hasDroppedColumns && droppedColumns.containsKey(column.name.bytes);
    }

    /**
     * The drop rule applied to a complex column's deletion. Reads the {@link #currentDroppedComplex}
     * cache, so it is only correct after {@link #startOfComplexColumn} has primed it for this column.
     */
    public boolean isDroppedComplexDeletion(DeletionTime complexDeletion)
    {
        return isDroppedAt(complexDeletion.markedForDeleteAt(), currentDroppedComplex);
    }

    public <V> V maybeClearCounterValue(V value, ValueAccessor<V> accessor)
    {
        return flag == Flag.FROM_REMOTE || (flag == Flag.LOCAL && CounterContext.instance().shouldClearLocal(value, accessor))
               ? CounterContext.instance().clearAllLocal(value, accessor)
               : value;
    }

    /**
     * @param source the original source of {@link DataInput}
     * @param limit  the limit number of bytes to read
     * @return a reusable {@link TrackedDataInputPlus}. The instance is lazily created on
     * first use and reused for every row in the partition.
     */
    public TrackedDataInputPlus trackedDataInputPlus(DataInput source, long limit)
    {
        if (trackedInput == null)
            trackedInput = new TrackedDataInputPlus(source, limit);
        else
            trackedInput.reset(source, limit);
        return trackedInput;
    }
}
