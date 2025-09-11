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
import java.util.Optional;
import java.util.function.Predicate;

import com.google.common.collect.Iterables;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
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
}
