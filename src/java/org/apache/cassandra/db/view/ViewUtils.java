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

package org.apache.cassandra.db.view;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import com.google.common.collect.Iterables;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.TableMetadata;

public final class ViewUtils
{
    private ViewUtils()
    {
    }

    /**
     * Calculate the natural endpoint for the view.
     *
     * The view natural endpoint is the endpoint which has the same cardinality as this node in the replication factor.
     * The cardinality is the number at which this node would store a piece of data, given the change in replication
     * factor. If the keyspace's replication strategy is a NetworkTopologyStrategy, we filter the ring to contain only
     * nodes in the local datacenter when calculating cardinality.
     *
     * For example, if we have the following ring:
     *   {@code A, T1 -> B, T2 -> C, T3 -> A}
     *
     * For the token T1, at RF=1, A would be included, so A's cardinality for T1 is 1. For the token T1, at RF=2, B would
     * be included, so B's cardinality for token T1 is 2. For token T3, at RF = 2, A would be included, so A's cardinality
     * for T3 is 2.
     *
     * For a view whose base token is T1 and whose view token is T3, the pairings between the nodes would be:
     *  A writes to C (A's cardinality is 1 for T1, and C's cardinality is 1 for T3)
     *  B writes to A (B's cardinality is 2 for T1, and A's cardinality is 2 for T3)
     *  C writes to B (C's cardinality is 3 for T1, and B's cardinality is 3 for T3)
     *
     * @return Optional.empty() if this method is called using a base token which does not belong to this replica
     */
    public static Optional<Replica> getViewNaturalEndpoint(AbstractReplicationStrategy replicationStrategy, Token baseToken, Token viewToken)
    {
        String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter();
        EndpointsForToken naturalBaseReplicas = replicationStrategy.getNaturalReplicasForToken(baseToken);
        EndpointsForToken naturalViewReplicas = replicationStrategy.getNaturalReplicasForToken(viewToken);

        Optional<Replica> localReplica = Iterables.tryFind(naturalViewReplicas, Replica::isSelf).toJavaUtil();
        if (localReplica.isPresent())
            return localReplica;

        // We only select replicas from our own DC
        // TODO: this is poor encapsulation, leaking implementation details of replication strategy
        Predicate<Replica> isLocalDC = r -> !(replicationStrategy instanceof NetworkTopologyStrategy)
                || DatabaseDescriptor.getEndpointSnitch().getDatacenter(r).equals(localDataCenter);

        // We have to remove any endpoint which is shared between the base and the view, as it will select itself
        // and throw off the counts otherwise.
        EndpointsForToken baseReplicas = naturalBaseReplicas.filter(
                r -> !naturalViewReplicas.endpoints().contains(r.endpoint()) && isLocalDC.test(r)
        );
        EndpointsForToken viewReplicas = naturalViewReplicas.filter(
                r -> !naturalBaseReplicas.endpoints().contains(r.endpoint()) && isLocalDC.test(r)
        );

        // The replication strategy will be the same for the base and the view, as they must belong to the same keyspace.
        // Since the same replication strategy is used, the same placement should be used and we should get the same
        // number of replicas for all of the tokens in the ring.
        assert baseReplicas.size() == viewReplicas.size() : "Replication strategy should have the same number of endpoints for the base and the view";

        int baseIdx = -1;
        for (int i=0; i<baseReplicas.size(); i++)
        {
            if (baseReplicas.get(i).isSelf())
            {
                baseIdx = i;
                break;
            }
        }

        if (baseIdx < 0)
            //This node is not a base replica of this key, so we return empty
            return Optional.empty();

        return Optional.of(viewReplicas.get(baseIdx));
    }

    /**
     * Computes the liveness info for a materialized view entry based on the base table row.
     * 
     * @param view the materialized view
     * @param baseRow the base table row
     * @param nowInSec current time in seconds
     * @return the computed liveness info for the view entry
     */
    public static LivenessInfo computeLivenessInfoForEntry(View view, Row baseRow, int nowInSec)
    {
        /**
         * There 3 cases:
         *  1. No extra primary key in view and all base columns are selected in MV. all base row's components(livenessInfo,
         *     deletion, cells) are same as view row. Simply map base components to view row.
         *  2. There is a base non-key column used in view pk. This base non-key column determines the liveness of view row. view's row level
         *     info should based on this column.
         *  3. Most tricky case is no extra primary key in view and some base columns are not selected in MV. We cannot use 1 livenessInfo or
         *     row deletion to represent the liveness of unselected column properly, see CASSANDRA-11500.
         *     We could make some simplification: the unselected columns will be used only when it affects view row liveness. eg. if view row
         *     already exists and not expiring, there is no need to use unselected columns.
         *     Note: if the view row is removed due to unselected column removal(ttl or cell tombstone), we will have problem keeping view
         *     row alive with a smaller or equal timestamp than the max unselected column timestamp.
         *
         */
        assert view.baseNonPKColumnsInViewPK.size() <= 1; // This may change, but is currently an enforced limitation

        LivenessInfo baseLiveness = baseRow.primaryKeyLivenessInfo();

        if (view.baseNonPKColumnsInViewPK.isEmpty())
        {
            if (view.getDefinition().includeAllColumns)
                return baseLiveness;

            long timestamp = baseLiveness.timestamp();
            boolean hasNonExpiringLiveCell = false;
            Cell<?> biggestExpirationCell = null;
            for (Cell<?> cell : baseRow.cells())
            {
                if (view.getViewColumn(cell.column()) != null)
                    continue;
                if (!isLive(cell, nowInSec))
                    continue;
                timestamp = Math.max(timestamp, cell.maxTimestamp());
                if (!cell.isExpiring())
                    hasNonExpiringLiveCell = true;
                else
                {
                    if (biggestExpirationCell == null)
                        biggestExpirationCell = cell;
                    else if (cell.localDeletionTime() > biggestExpirationCell.localDeletionTime())
                        biggestExpirationCell = cell;
                }
            }
            if (baseLiveness.isLive(nowInSec) && !baseLiveness.isExpiring())
                return LivenessInfo.create(timestamp, nowInSec);
            if (hasNonExpiringLiveCell)
                return LivenessInfo.create(timestamp, nowInSec);
            if (biggestExpirationCell == null)
                return baseLiveness;
            if (biggestExpirationCell.localDeletionTime() > baseLiveness.localExpirationTime()
                    || !baseLiveness.isLive(nowInSec))
                return LivenessInfo.withExpirationTime(timestamp,
                                                       biggestExpirationCell.ttl(),
                                                       biggestExpirationCell.localDeletionTime());
            return baseLiveness;
        }

        Cell<?> cell = baseRow.getCell(view.baseNonPKColumnsInViewPK.get(0));
        assert isLive(cell, nowInSec) : "We shouldn't have got there if the base row had no associated entry";

        return LivenessInfo.withExpirationTime(cell.timestamp(), cell.ttl(), cell.localDeletionTime());
    }

    /**
     * Checks if a cell is live at the given time.
     * 
     * @param cell the cell to check
     * @param nowInSec current time in seconds
     * @return true if the cell is live
     */
    public static boolean isLive(Cell<?> cell, int nowInSec)
    {
        return cell != null && cell.isLive(nowInSec);
    }

    /**
     * Gets the value for a primary key column from a base table row.
     * 
     * @param column the column metadata for the primary key column
     * @param row the base table row
     * @param basePartitionKey the base partition key components
     * @return the value for the column, or null if the column value is null
     */
    public static ByteBuffer getValueForPK(ColumnMetadata column, Row row, ByteBuffer[] basePartitionKey)
    {
        switch (column.kind)
        {
            case PARTITION_KEY:
                return basePartitionKey[column.position()];
            case CLUSTERING:
                return row.clustering().bufferAt(column.position());
            default:
                Cell<?> cell = row.getCell(column);
                return cell == null ? null : cell.buffer();
        }
    }

    /**
     * Extracts the key components from a decorated key.
     * 
     * @param partitionKey the decorated partition key
     * @param type the partition key type
     * @return array of key components
     */
    public static ByteBuffer[] extractKeyComponents(DecoratedKey partitionKey, AbstractType<?> type)
    {
        return type instanceof CompositeType
             ? ((CompositeType)type).split(partitionKey.getKey())
             : new ByteBuffer[]{ partitionKey.getKey() };
    }

    /**
     * Adds column data from a base table to a view row builder.
     * 
     * @param viewRowBuilder the view row builder
     * @param viewColumn the view column metadata
     * @param baseTableData the base table column data
     */
    public static void addColumnDataToBuilder(Row.Builder viewRowBuilder, ColumnMetadata viewColumn, ColumnData baseTableData)
    {
        assert viewColumn.isComplex() == baseTableData.column().isComplex();
        if (!viewColumn.isComplex())
        {
            addCellToBuilder(viewRowBuilder, viewColumn, (Cell<?>)baseTableData);
            return;
        }

        ComplexColumnData complexData = (ComplexColumnData)baseTableData;
        viewRowBuilder.addComplexDeletion(viewColumn, complexData.complexDeletion());
        for (Cell<?> cell : complexData)
            addCellToBuilder(viewRowBuilder, viewColumn, cell);
    }

    /**
     * Adds a cell from a base table to a view row builder.
     * 
     * @param viewRowBuilder the view row builder
     * @param viewColumn the view column metadata
     * @param baseTableCell the base table cell
     */
    public static void addCellToBuilder(Row.Builder viewRowBuilder, ColumnMetadata viewColumn, Cell<?> baseTableCell)
    {
        assert !viewColumn.isPrimaryKeyColumn();
        viewRowBuilder.addCell(baseTableCell.withUpdatedColumn(viewColumn));
    }

    public static DecoratedKey makeViewPartitionKey(TableMetadata viewMetadata, ByteBuffer[] currentViewEntryPartitionKey)
    {
        ByteBuffer rawKey = viewMetadata.partitionKeyColumns().size() == 1
                            ? currentViewEntryPartitionKey[0]
                            : CompositeType.build(ByteBufferAccessor.instance, currentViewEntryPartitionKey);

        return viewMetadata.partitioner.decorateKey(rawKey);
    }

    /**
     * Utility class for comparing base table rows with view rows.
     * Used for MV key rebuild diagnostics.
     */
    public static final class ViewRowComparison
    {
        private ViewRowComparison() {}

        /**
         * Comparison status for base vs view row.
         */
        public enum Status
        {
            /** Both rows match (or both correctly absent/filtered) */
            IDENTICAL,
            /** View row exists but base is NULL or tombstone - safe to delete view row */
            STALE_BASE_ABSENT,
            /** View row exists but base doesn't qualify for view (clustering/non-PK) - safe to delete view row */
            STALE_BASE_EXCLUDED,
            /** View row exists at old clustering but non-PK column value changed - safe to delete view row */
            STALE_VALUE_CHANGED,
            /** View row is missing but should exist - safe to regenerate from base */
            MISSING,
            /** Both exist with differences - check viewAhead flag for safety */
            MISMATCH,
            /** Base row filtered by clustering restriction - view correctly absent */
            CONSISTENT_FILTERED_CLUSTERING,
            /** Base row filtered because non-PK column in view PK is null or dead - view correctly absent */
            CONSISTENT_FILTERED_NONPK_COLUMN
        }

        /**
         * Result of comparing a base table row with the corresponding view row.
         */
        public static class Result
        {
            public final Status status;
            public final String summary;
            /** True if view has newer timestamp than expected - DANGEROUS, cannot fix from base alone! */
            public final boolean viewAhead;

            private Result(Status status, String summary, boolean viewAhead)
            {
                this.status = status;
                this.summary = summary;
                this.viewAhead = viewAhead;
            }

            public static Result of(Status status, String summary)
            {
                return new Result(status, summary, false);
            }

            public static Result of(Status status, String summary, boolean viewAhead)
            {
                return new Result(status, summary, viewAhead);
            }
        }

        /**
         * Compares a base table row with the actual view row and reports differences.
         * Optionally records metrics based on the comparison result.
         *
         * @param view the materialized view
         * @param baseRow the base table row (may be null)
         * @param actualViewRow the actual row read from the view table (may be null)
         * @param basePartitionKey the base table partition key
         * @param viewNonPKValueFromQuery the non-PK column value from view's clustering (if applicable)
         * @param nowInSec current time in seconds
         * @param viewMetrics the view table metrics to record (may be null to skip metrics recording)
         * @return comparison result with status and summary
         */
        public static Result compare(View view,
                                     Row baseRow,
                                     Row actualViewRow,
                                     DecoratedKey basePartitionKey,
                                     ByteBuffer viewNonPKValueFromQuery,
                                     int nowInSec,
                                     TableMetrics viewMetrics)
        {
            // Case 1: Base row is null or tombstone (no live data)
            if (baseRow == null || !baseRow.hasLiveData(nowInSec, false))
            {
                String baseState = baseRow == null ? "NULL" : "tombstone";
                if (actualViewRow == null)
                    return recordAndReturn(Result.of(Status.IDENTICAL, "base row is " + baseState + ", view row is NULL"), viewMetrics);
                else if (!actualViewRow.hasLiveData(nowInSec, view.enforceStrictLiveness()))
                    return recordAndReturn(Result.of(Status.IDENTICAL, "base row is " + baseState + ", view row is dead"), viewMetrics);
                else
                    return recordAndReturn(Result.of(Status.STALE_BASE_ABSENT, "base row is " + baseState + " but view row exists"), viewMetrics);
            }

            // Case 2: Base row is live - check if it matches view filter (includes WHERE clause)
            boolean matchesFilter = view.matchesViewFilter(basePartitionKey, baseRow, nowInSec);

            if (!matchesFilter)
            {
                Result filterResult = describeWhyFilterFailed(view, baseRow, basePartitionKey, nowInSec);
                // View row absent or dead is consistent with base being filtered out
                if (actualViewRow == null || !actualViewRow.hasLiveData(nowInSec, view.enforceStrictLiveness()))
                    return recordAndReturn(filterResult, viewMetrics);
                else
                    return recordAndReturn(Result.of(Status.STALE_BASE_EXCLUDED, "filter failed: " + filterResult.summary), viewMetrics);
            }

            // Case 3: Base matches filter but view row is absent or dead
            if (actualViewRow == null || !actualViewRow.hasLiveData(nowInSec, view.enforceStrictLiveness()))
            {
                String viewState = actualViewRow == null ? "not found" : "dead";
                return recordAndReturn(Result.of(Status.MISSING, "view row " + viewState), viewMetrics);
            }

            // Case 4: Check non-PK column value mismatch (base row has different value than view clustering)
            if (!view.hasSamePrimaryKeyColumnsAsBaseTable() && viewNonPKValueFromQuery != null)
            {
                ColumnMetadata col = view.baseNonPKColumnsInViewPK.get(0);
                Cell<?> baseCell = baseRow.getCell(col);
                if (isLive(baseCell, nowInSec))
                {
                    ByteBuffer actualValue = baseCell.buffer();
                    if (ByteBufferUtil.compareUnsigned(actualValue, viewNonPKValueFromQuery) != 0)
                    {
                        LivenessInfo viewLiveness = actualViewRow.primaryKeyLivenessInfo();
                        return recordAndReturn(Result.of(Status.STALE_VALUE_CHANGED, String.format(
                            "NonPkCol=%s stale record, base=%s (ts=%d, ttl=%d, delTime=%d), view=%s (rowTs=%d, rowTtl=%d, rowExpTime=%d)",
                            col.name, col.type.getString(actualValue),
                            baseCell.timestamp(), baseCell.ttl(), baseCell.localDeletionTime(),
                            col.type.getString(viewNonPKValueFromQuery),
                            viewLiveness.timestamp(), viewLiveness.ttl(), viewLiveness.localExpirationTime())), viewMetrics);
                    }
                } // !isLive cases already covered in filter check with primaryKeyColumnsNonNull
            }

            // Case 5: Both exist and aligned - compare details
            Row expectedViewRow = ViewRowTranslator.translateBaseRowToViewRow(view, baseRow, basePartitionKey, nowInSec);
            ComparisonDetails details = compareRowDetails(view, expectedViewRow, actualViewRow, nowInSec);

            if (details.diffs.isEmpty())
                return recordAndReturn(Result.of(Status.IDENTICAL, ""), viewMetrics);
            else
                return recordAndReturn(Result.of(Status.MISMATCH, String.join("; ", details.diffs), details.viewAhead), viewMetrics);
        }

        /**
         * Records the comparison result metric if viewMetrics is provided, then returns the result.
         */
        private static Result recordAndReturn(Result result, TableMetrics viewMetrics)
        {
            if (viewMetrics != null)
                recordMetric(viewMetrics, result.status);
            return result;
        }

        /**
         * Records the appropriate metric counter based on the comparison status.
         *
         * @param viewMetrics the view table metrics to record
         * @param status the comparison status
         */
        public static void recordMetric(TableMetrics viewMetrics, Status status)
        {
            switch (status)
            {
                case IDENTICAL:
                case CONSISTENT_FILTERED_NONPK_COLUMN:
                case CONSISTENT_FILTERED_CLUSTERING:
                    viewMetrics.viewRebuildConsistent.inc();
                    break;
                case MISMATCH:
                    viewMetrics.viewRebuildMismatch.inc();
                    break;
                case STALE_VALUE_CHANGED:
                case STALE_BASE_EXCLUDED:
                case STALE_BASE_ABSENT:
                    viewMetrics.viewRebuildStale.inc();
                    break;
                case MISSING:
                    viewMetrics.viewRebuildMissing.inc();
                    break;
                default:
                    throw new IllegalStateException("Unknown ViewRowComparison.Result.Status " + status);
            }
        }

        /** Internal class to hold comparison details including timestamp analysis */
        private static class ComparisonDetails
        {
            final List<String> diffs;
            final boolean viewAhead;  // true if view has newer timestamp than expected

            ComparisonDetails(List<String> diffs, boolean viewAhead)
            {
                this.diffs = diffs;
                this.viewAhead = viewAhead;
            }
        }

        /**
         * Describes why a base row failed to match the view filter.
         * matchesViewFilter() checks: clustering selection, non-PK column liveness (via IS NOT NULL in WHERE).
         * @return Result with specific CONSISTENT_FILTERED_* status and reason
         */
        private static Result describeWhyFilterFailed(View view, Row baseRow, DecoratedKey basePartitionKey, int nowInSec)
        {
            // Check clustering selection
            if (!view.getReadQuery().selectsClustering(basePartitionKey, baseRow.clustering()))
                return Result.of(Status.CONSISTENT_FILTERED_CLUSTERING, "clustering not selected by view query");

            // Check non-PK column in view PK (if view has one)
            // Note: MV WHERE clause only allows IS NOT NULL on columns in view PK, which is covered here
            if (!view.hasSamePrimaryKeyColumnsAsBaseTable())
            {
                ColumnMetadata col = view.baseNonPKColumnsInViewPK.get(0);
                Cell<?> cell = baseRow.getCell(col);
                if (cell == null)
                    return Result.of(Status.CONSISTENT_FILTERED_NONPK_COLUMN, String.format("NonPKCol=%s is null", col.name));
                else if (!cell.isLive(nowInSec))
                    return Result.of(Status.CONSISTENT_FILTERED_NONPK_COLUMN, String.format("NonPKCol=%s is dead", col.name));
            }

            // Shouldn't reach here if matchesViewFilter returned false
            throw new IllegalStateException("matchesViewFilter returned false but no filter condition matched for view " + view.name);
        }

        /**
         * Compare row details between expected (from base) and actual (from view) view rows.
         * Note: Clustering keys are NOT compared
         *
         * @return ComparisonDetails with diffs and whether view has newer timestamp (viewAhead)
         */
        private static ComparisonDetails compareRowDetails(View view, Row expected, Row actual, int nowInSec)
        {
            List<String> diffs = new ArrayList<>();
            TableMetadata viewMetadata = view.getDefinition().metadata;
            long maxExpectedTs = Long.MIN_VALUE;
            long maxActualTs = Long.MIN_VALUE;

            // Compare liveness info and track timestamps
            LivenessInfo expLiveness = expected.primaryKeyLivenessInfo();
            LivenessInfo actLiveness = actual.primaryKeyLivenessInfo();
            compareLivenessInfo(expLiveness, actLiveness, diffs);
            if (expLiveness != null && !expLiveness.isEmpty())
                maxExpectedTs = Math.max(maxExpectedTs, expLiveness.timestamp());
            if (actLiveness != null && !actLiveness.isEmpty())
                maxActualTs = Math.max(maxActualTs, actLiveness.timestamp());

            // Note: Row deletion time is not compared - this has no impact on user-visible differences
            //       v1 MV sync job might have inserted tombstone with different deletion time

            // Compare each regular/static column in the view
            for (ColumnMetadata col : viewMetadata.regularAndStaticColumns())
            {
                ColumnData expData = expected.getColumnData(col);
                ColumnData actData = actual.getColumnData(col);

                if (expData == null && actData == null)
                    continue;
                if (expData == null)
                {
                    // If actual is also dead/tombstone, treat as both absent
                    if (isColumnDataDead(actData, nowInSec))
                        continue;
                    diffs.add(String.format("%s: extra in view", col.name));
                    // Track timestamp from actual (extra in view)
                    maxActualTs = Math.max(maxActualTs, getMaxTimestamp(actData));
                    continue;
                }
                if (actData == null)
                {
                    // If expected is also dead/tombstone, treat as both absent
                    if (isColumnDataDead(expData, nowInSec))
                        continue;
                    diffs.add(String.format("%s: missing in view", col.name));
                    maxExpectedTs = Math.max(maxExpectedTs, getMaxTimestamp(expData));
                    continue;
                }
                compareColumnData(col, expData, actData, diffs);
                maxExpectedTs = Math.max(maxExpectedTs, getMaxTimestamp(expData));
                maxActualTs = Math.max(maxActualTs, getMaxTimestamp(actData));
            }

            // View is "ahead" if actual has strictly newer timestamp than expected
            boolean viewAhead = maxActualTs > maxExpectedTs;
            return new ComparisonDetails(diffs, viewAhead);
        }

        /** Check if column data is effectively dead (tombstone or expired) */
        private static boolean isColumnDataDead(ColumnData data, int nowInSec)
        {
            if (data == null)
                return true;
            if (data instanceof Cell<?>)
            {
                Cell<?> cell = (Cell<?>) data;
                return !cell.isLive(nowInSec);
            }
            if (data instanceof ComplexColumnData)
            {
                ComplexColumnData complex = (ComplexColumnData) data;
                // Complex column is dead if all cells are dead
                for (Cell<?> cell : complex)
                {
                    if (cell.isLive(nowInSec))
                        return false;
                }
                return true;
            }
            return false;
        }

        /** Get max timestamp from column data (handles simple and complex columns) */
        private static long getMaxTimestamp(ColumnData data)
        {
            if (data == null)
                return Long.MIN_VALUE;
            if (data instanceof Cell)
                return ((Cell<?>) data).timestamp();
            // Complex column - find max across all cells
            long max = Long.MIN_VALUE;
            for (Cell<?> cell : (ComplexColumnData) data)
                max = Math.max(max, cell.timestamp());
            return max;
        }

        /**
         * Compare liveness info between expected and actual view rows.
         *
         * Note: Write timestamp differences are NOT considered mismatches.
         *       What matters for user-visible consistency is whether the row expires at the same time
         */
        private static void compareLivenessInfo(LivenessInfo expected, LivenessInfo actual, List<String> diffs)
        {
            boolean expEmpty = expected == null || expected.isEmpty();
            boolean actEmpty = actual == null || actual.isEmpty();

            // Determine if each is expiring
            boolean expExpiring = !expEmpty && expected.isExpiring();
            boolean actExpiring = !actEmpty && actual.isExpiring();

            // If neither is expiring, no concern - both effectively live forever
            if (!expExpiring && !actExpiring)
                return;

            // If one is expiring and other is not, they'll diverge
            if (expExpiring != actExpiring)
            {
                diffs.add(String.format("liveness.expiring: expected=%s (expTime=%d) actual=%s (expTime=%d)",
                                        expExpiring, expExpiring ? expected.localExpirationTime() : -1,
                                        actExpiring, actExpiring ? actual.localExpirationTime() : -1));
                return;
            }

            // Both are expiring - compare expiration times
            if (expected.localExpirationTime() != actual.localExpirationTime())
                diffs.add(String.format("liveness.expirationTime: expected=%d actual=%d",
                                        expected.localExpirationTime(), actual.localExpirationTime()));
        }

        private static void compareColumnData(ColumnMetadata col, ColumnData expected, ColumnData actual, List<String> diffs)
        {
            if (col.isComplex())
                compareComplexColumnData(col, (ComplexColumnData) expected, (ComplexColumnData) actual, diffs);
            else
                compareCell(col, (Cell<?>) expected, (Cell<?>) actual, diffs);
        }

        private static void compareComplexColumnData(ColumnMetadata col, ComplexColumnData expected,
                                                     ComplexColumnData actual, List<String> diffs)
        {
            if (!expected.complexDeletion().equals(actual.complexDeletion()))
                diffs.add(String.format("%s.deletion: expected=%s actual=%s",
                                        col.name, expected.complexDeletion(), actual.complexDeletion()));

            for (Cell<?> expCell : expected)
            {
                Cell<?> actCell = actual.getCell(expCell.path());
                if (actCell == null)
                    diffs.add(String.format("%s[%s]: missing in view", col.name, expCell.path()));
                else
                    compareCell(col, expCell, actCell, diffs);
            }
            for (Cell<?> actCell : actual)
            {
                if (expected.getCell(actCell.path()) == null)
                    diffs.add(String.format("%s[%s]: extra in view", col.name, actCell.path()));
            }
        }

        private static void compareCell(ColumnMetadata col, Cell<?> expected, Cell<?> actual, List<String> diffs)
        {
            String name = col.name.toString();
            List<String> cellDiffs = new ArrayList<>();

            // Value comparison
            if (!Objects.equals(expected.buffer(), actual.buffer()))
                cellDiffs.add(String.format("val='%s'->'%s'", formatValue(col, expected.buffer()), formatValue(col, actual.buffer())));

            // Timestamp comparison
            if (expected.timestamp() != actual.timestamp())
                cellDiffs.add(String.format("ts=%d->%d", expected.timestamp(), actual.timestamp()));

            // TTL comparison
            if (expected.ttl() != actual.ttl())
                cellDiffs.add(String.format("ttl=%d->%d", expected.ttl(), actual.ttl()));

            // Deletion time comparison (for tombstones or expiring cells)
            if (expected.localDeletionTime() != actual.localDeletionTime())
                cellDiffs.add(String.format("delTime=%d->%d", expected.localDeletionTime(), actual.localDeletionTime()));

            // Tombstone status
            if (expected.isTombstone() != actual.isTombstone())
                cellDiffs.add(String.format("tombstone=%s->%s", expected.isTombstone(), actual.isTombstone()));

            if (!cellDiffs.isEmpty())
                diffs.add(String.format("%s: {%s}", name, String.join(" ", cellDiffs)));
        }

        private static String formatValue(ColumnMetadata col, ByteBuffer value)
        {
            return value == null ? "NULL" : col.type.getString(value);
        }
    }
}
