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
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;

/**
 * Creates the updates to apply to a view given the existing rows in the base
 * table and the updates that we're applying to them (this handles updates
 * on a single partition only).
 *
 * This class is used by passing the updates made to the base table to
 * {@link #addBaseTableUpdate} and calling {@link #generateViewUpdates} once all updates have
 * been handled to get the resulting view mutations.
 */
public class ViewUpdateGenerator
{
    private final View view;
    private final long nowInSec;

    private final TableMetadata baseMetadata;
    private final DecoratedKey baseDecoratedKey;
    private final ByteBuffer[] basePartitionKey;

    private final TableMetadata viewMetadata;
    private final boolean baseEnforceStrictLiveness;

    private final Map<DecoratedKey, PartitionUpdate.Builder> updates = new HashMap<>();

    // Reused internally to build a new entry
    private final ByteBuffer[] currentViewEntryPartitionKey;
    private final Row.Builder currentViewEntryBuilder;

    /**
     * The type of type update action to perform to the view for a given base table
     * update.
     */
    private enum UpdateAction
    {
        NONE,            // There was no view entry and none should be added
        NEW_ENTRY,       // There was no entry but there is one post-update
        DELETE_OLD,      // There was an entry but there is nothing after update
        UPDATE_EXISTING, // There was an entry and the update modifies it
        SWITCH_ENTRY     // There was an entry and there is still one after update,
                         // but they are not the same one.
    }

    /**
     * Creates a new {@code ViewUpdateBuilder}.
     *
     * @param view the view for which this will be building updates for.
     * @param basePartitionKey the partition key for the base table partition for which
     * we'll handle updates for.
     * @param nowInSec the current time in seconds. Used to decide if data are live or not
     * and as base reference for new deletions.
     */
    public ViewUpdateGenerator(View view, DecoratedKey basePartitionKey, long nowInSec)
    {
        this.view = view;
        this.nowInSec = nowInSec;

        this.baseMetadata = view.getDefinition().baseTableMetadata();
        this.baseEnforceStrictLiveness = baseMetadata.enforceStrictLiveness();
        this.baseDecoratedKey = basePartitionKey;
        this.basePartitionKey = extractKeyComponents(basePartitionKey, baseMetadata.partitionKeyType);

        this.viewMetadata = Schema.instance.getTableMetadata(view.getDefinition().metadata.id);

        this.currentViewEntryPartitionKey = new ByteBuffer[viewMetadata.partitionKeyColumns().size()];
        this.currentViewEntryBuilder = BTreeRow.sortedBuilder();
    }

    private static ByteBuffer[] extractKeyComponents(DecoratedKey partitionKey, AbstractType<?> type)
    {
        return type instanceof CompositeType
             ? ((CompositeType)type).split(partitionKey.getKey())
             : new ByteBuffer[]{ partitionKey.getKey() };
    }

    /**
     * Adds to this generator the updates to be made to the view given a base table row
     * before and after an update.
     *
     * @param existingBaseRow the base table row as it is before an update.
     * @param mergedBaseRow the base table row after the update is applied (note that
     * this is not just the new update, but rather the resulting row).
     */
    public void addBaseTableUpdate(Row existingBaseRow, Row mergedBaseRow)
    {
        switch (updateAction(existingBaseRow, mergedBaseRow))
        {
            case NONE:
                return;
            case NEW_ENTRY:
                createEntry(mergedBaseRow);
                return;
            case DELETE_OLD:
                deleteOldEntry(existingBaseRow, mergedBaseRow);
                return;
            case UPDATE_EXISTING:
                updateEntry(existingBaseRow, mergedBaseRow);
                return;
            case SWITCH_ENTRY:
                createEntry(mergedBaseRow);
                deleteOldEntry(existingBaseRow, mergedBaseRow);
                return;
        }
    }

    /**
     * Returns the updates that needs to be done to the view given the base table updates
     * passed to {@link #addBaseTableUpdate}.
     *
     * @return the updates to do to the view.
     */
    public Collection<PartitionUpdate> generateViewUpdates()
    {
        return updates.values().stream().map(PartitionUpdate.Builder::build).collect(Collectors.toList());
    }

    /**
     * Clears the current state so that the generator may be reused.
     */
    public void clear()
    {
        updates.clear();
    }

    /**
     * Compute which type of action needs to be performed to the view for a base table row
     * before and after an update.
     */
    private UpdateAction updateAction(Row existingBaseRow, Row mergedBaseRow)
    {
        // Having existing empty is useful, it just means we'll insert a brand new entry for mergedBaseRow,
        // but if we have no update at all, we shouldn't get there.
        assert !mergedBaseRow.isEmpty();

        // Note that none of the base PK columns will differ since we're intrinsically dealing
        // with the same base row. So we have to check 2 things:
        //   1) if there is a column not part of the base PK in the view PK, whether it is changed by the update.
        //   2) whether mergedBaseRow actually match the view SELECT filter

        if (baseMetadata.isCompactTable())
        {
            Clustering clustering = mergedBaseRow.clustering();
            for (int i = 0; i < clustering.size(); i++)
            {
                if (clustering.get(i) == null)
                    return UpdateAction.NONE;
            }
        }

        assert view.baseNonPKColumnsInViewPK.size() <= 1 : "We currently only support one base non-PK column in the view PK";

        if (view.baseNonPKColumnsInViewPK.isEmpty())
        {
            // The view entry is necessarily the same pre and post update.

            // Note that we allow existingBaseRow to be null and treat it as empty (see MultiViewUpdateBuilder.generateViewsMutations).
            boolean existingHasLiveData = existingBaseRow != null && existingBaseRow.hasLiveData(nowInSec, baseEnforceStrictLiveness);
            boolean mergedHasLiveData = mergedBaseRow.hasLiveData(nowInSec, baseEnforceStrictLiveness);
            return existingHasLiveData
                 ? (mergedHasLiveData ? UpdateAction.UPDATE_EXISTING : UpdateAction.DELETE_OLD)
                 : (mergedHasLiveData ? UpdateAction.NEW_ENTRY : UpdateAction.NONE);
        }

        ColumnMetadata baseColumn = view.baseNonPKColumnsInViewPK.get(0);
        assert !baseColumn.isComplex() : "A complex column couldn't be part of the view PK";
        Cell<?> before = existingBaseRow == null ? null : existingBaseRow.getCell(baseColumn);
        Cell<?> after = mergedBaseRow.getCell(baseColumn);

        // If the update didn't modified this column, the cells will be the same object so it's worth checking
        if (before == after)
            return isLive(before) ? UpdateAction.UPDATE_EXISTING : UpdateAction.NONE;

        if (!isLive(before))
            return isLive(after) ? UpdateAction.NEW_ENTRY : UpdateAction.NONE;
        if (!isLive(after))
        {
            return UpdateAction.DELETE_OLD;
        }

        return baseColumn.cellValueType().compare(before.buffer(), after.buffer()) == 0
             ? UpdateAction.UPDATE_EXISTING
             : UpdateAction.SWITCH_ENTRY;
    }

    private boolean matchesViewFilter(Row baseRow)
    {
        return view.matchesViewFilter(baseDecoratedKey, baseRow, nowInSec);
    }

    private boolean isLive(Cell<?> cell)
    {
        return cell != null && cell.isLive(nowInSec);
    }

    /**
     * Creates a view entry corresponding to the provided base row.
     * <p>
     * This method checks that the base row does match the view filter before applying it.
     */
    private void createEntry(Row baseRow)
    {
        // Before create a new entry, make sure it matches the view filter
        if (!matchesViewFilter(baseRow))
            return;

        startNewUpdate(baseRow);
        currentViewEntryBuilder.addPrimaryKeyLivenessInfo(computeLivenessInfoForEntry(baseRow));
        currentViewEntryBuilder.addRowDeletion(baseRow.deletion());

        for (ColumnData data : baseRow)
        {
            ColumnMetadata viewColumn = view.getViewColumn(data.column());
            // If that base table column is not denormalized in the view, we had nothing to do.
            // Alose, if it's part of the view PK it's already been taken into account in the clustering.
            if (viewColumn == null || viewColumn.isPrimaryKeyColumn())
                continue;

            addColumnData(viewColumn, data);
        }

        submitUpdate();
    }

    /**
     * Creates the updates to apply to the existing view entry given the base table row before
     * and after the update, assuming that the update hasn't changed to which view entry the
     * row correspond (that is, we know the columns composing the view PK haven't changed).
     * <p>
     * This method checks that the base row (before and after) does match the view filter before
     * applying anything.
     */
    private void updateEntry(Row existingBaseRow, Row mergedBaseRow)
    {
        // While we know existingBaseRow and mergedBaseRow are corresponding to the same view entry,
        // they may not match the view filter.
        if (!matchesViewFilter(existingBaseRow))
        {
            createEntry(mergedBaseRow);
            return;
        }
        if (!matchesViewFilter(mergedBaseRow))
        {
            deleteOldEntryInternal(existingBaseRow, mergedBaseRow);
            return;
        }

        startNewUpdate(mergedBaseRow);

        // In theory, it may be the PK liveness and row deletion hasn't been change by the update
        // and we could condition the 2 additions below. In practice though, it's as fast (if not
        // faster) to compute those info than to check if they have changed so we keep it simple.
        currentViewEntryBuilder.addPrimaryKeyLivenessInfo(computeLivenessInfoForEntry(mergedBaseRow));
        currentViewEntryBuilder.addRowDeletion(mergedBaseRow.deletion());

        addDifferentCells(existingBaseRow, mergedBaseRow);
        submitUpdate();
    }

    private void addDifferentCells(Row existingBaseRow, Row mergedBaseRow)
    {
        // We only add to the view update the cells from mergedBaseRow that differs from
        // existingBaseRow. For that and for speed we can just cell pointer equality: if the update
        // hasn't touched a cell, we know it will be the same object in existingBaseRow and
        // mergedBaseRow (note that including more cells than we strictly should isn't a problem
        // for correction, so even if the code change and pointer equality don't work anymore, it'll
        // only a slightly inefficiency which we can fix then).
        // Note: we could alternatively use Rows.diff() for this, but because it is a bit more generic
        // than what we need here, it's also a bit less efficient (it allocates more in particular),
        // and this might be called a lot of time for view updates. So, given that this is not a whole
        // lot of code anyway, it's probably doing the diff manually.
        PeekingIterator<ColumnData> existingIter = Iterators.peekingIterator(existingBaseRow.iterator());
        for (ColumnData mergedData : mergedBaseRow)
        {
            ColumnMetadata baseColumn = mergedData.column();
            ColumnMetadata viewColumn = view.getViewColumn(baseColumn);
            // If that base table column is not denormalized in the view, we had nothing to do.
            // Alose, if it's part of the view PK it's already been taken into account in the clustering.
            if (viewColumn == null || viewColumn.isPrimaryKeyColumn())
                continue;

            ColumnData existingData = null;
            // Find if there is data for that column in the existing row
            while (existingIter.hasNext())
            {
                int cmp = baseColumn.compareTo(existingIter.peek().column());
                if (cmp < 0)
                    break;

                ColumnData next = existingIter.next();
                if (cmp == 0)
                {
                    existingData = next;
                    break;
                }
            }

            if (existingData == null)
            {
                addColumnData(viewColumn, mergedData);
                continue;
            }

            if (mergedData == existingData)
                continue;

            if (baseColumn.isComplex())
            {
                ComplexColumnData mergedComplexData = (ComplexColumnData)mergedData;
                ComplexColumnData existingComplexData = (ComplexColumnData)existingData;
                if (mergedComplexData.complexDeletion().supersedes(existingComplexData.complexDeletion()))
                    currentViewEntryBuilder.addComplexDeletion(viewColumn, mergedComplexData.complexDeletion());

                PeekingIterator<Cell<?>> existingCells = Iterators.peekingIterator(existingComplexData.iterator());
                for (Cell<?> mergedCell : mergedComplexData)
                {
                    Cell<?> existingCell = null;
                    // Find if there is corresponding cell in the existing row
                    while (existingCells.hasNext())
                    {
                        int cmp = baseColumn.cellPathComparator().compare(mergedCell.path(), existingCells.peek().path());
                        if (cmp > 0)
                            break;

                        Cell<?> next = existingCells.next();
                        if (cmp == 0)
                        {
                            existingCell = next;
                            break;
                        }
                    }

                    if (mergedCell != existingCell)
                        addCell(viewColumn, mergedCell);
                }
            }
            else
            {
                // Note that we've already eliminated the case where merged == existing
                addCell(viewColumn, (Cell<?>)mergedData);
            }
        }
    }

    /**
     * Deletes the view entry corresponding to the provided base row.
     * <p>
     * This method checks that the base row does match the view filter before bothering.
     */
    private void deleteOldEntry(Row existingBaseRow, Row mergedBaseRow)
    {
        // Before deleting an old entry, make sure it was matching the view filter (otherwise there is nothing to delete)
        if (!matchesViewFilter(existingBaseRow))
            return;

        deleteOldEntryInternal(existingBaseRow, mergedBaseRow);
    }

    private void deleteOldEntryInternal(Row existingBaseRow, Row mergedBaseRow)
    {
        startNewUpdate(existingBaseRow);
        long timestamp = computeTimestampForEntryDeletion(existingBaseRow, mergedBaseRow);
        long rowDeletion = mergedBaseRow.deletion().time().markedForDeleteAt();
        assert timestamp >= rowDeletion;
        
        // If computed deletion timestamp greater than row deletion, it must be coming from 
        //  1. non-pk base column used in view pk, or
        //  2. unselected base column
        //  any case, we need to use it as expired livenessInfo
        // If computed deletion timestamp is from row deletion, we only need row deletion itself
        if (timestamp > rowDeletion)
        {
            /*
             * We use an expired liveness instead of a row tombstone to allow a shadowed MV
             * entry to co-exist with a row tombstone, see ViewComplexTest#testCommutativeRowDeletion.
             *
             * TODO This is a dirty overload of LivenessInfo and we should modify
             * the storage engine to properly support this on CASSANDRA-13826.
             */
            LivenessInfo info = LivenessInfo.withExpirationTime(timestamp, LivenessInfo.EXPIRED_LIVENESS_TTL, nowInSec);
            currentViewEntryBuilder.addPrimaryKeyLivenessInfo(info);
        }
        currentViewEntryBuilder.addRowDeletion(mergedBaseRow.deletion());

        addDifferentCells(existingBaseRow, mergedBaseRow);
        submitUpdate();
    }

    /**
     * Computes the partition key and clustering for a new view entry, and setup the internal
     * row builder for the new row.
     *
     * This assumes that there is corresponding entry, i.e. no values for the partition key and
     * clustering are null (since we have eliminated that case through updateAction).
     */
    private void startNewUpdate(Row baseRow)
    {
        ByteBuffer[] clusteringValues = new ByteBuffer[viewMetadata.clusteringColumns().size()];
        for (ColumnMetadata viewColumn : viewMetadata.primaryKeyColumns())
        {
            ColumnMetadata baseColumn = view.getBaseColumn(viewColumn);
            ByteBuffer value = getValueForPK(baseColumn, baseRow);
            if (viewColumn.isPartitionKey())
                currentViewEntryPartitionKey[viewColumn.position()] = value;
            else
                clusteringValues[viewColumn.position()] = value;
        }

        currentViewEntryBuilder.newRow(Clustering.make(clusteringValues));
    }

    private LivenessInfo computeLivenessInfoForEntry(Row baseRow)
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
                if (!isLive(cell))
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
        assert isLive(cell) : "We shouldn't have got there if the base row had no associated entry";

        return LivenessInfo.withExpirationTime(cell.timestamp(), cell.ttl(), cell.localDeletionTime());
    }

    private long computeTimestampForEntryDeletion(Row existingBaseRow, Row mergedBaseRow)
    {
        DeletionTime deletion = mergedBaseRow.deletion().time();
        if (view.hasSamePrimaryKeyColumnsAsBaseTable())
        {
            long timestamp = Math.max(deletion.markedForDeleteAt(), existingBaseRow.primaryKeyLivenessInfo().timestamp());
            if (view.getDefinition().includeAllColumns)
                return timestamp;

            for (Cell<?> cell : existingBaseRow.cells())
            {
                // selected column should not contribute to view deletion, itself is already included in view row
                if (view.getViewColumn(cell.column()) != null)
                    continue;
                // unselected column is used regardless live or dead, because we don't know if it was used for liveness.
                timestamp = Math.max(timestamp, cell.maxTimestamp());
            }
            return timestamp;
        }
        // has base non-pk column in view pk
        Cell<?> before = existingBaseRow.getCell(view.baseNonPKColumnsInViewPK.get(0));
        assert isLive(before) : "We shouldn't have got there if the base row had no associated entry";
        return deletion.deletes(before) ? deletion.markedForDeleteAt() : before.timestamp();
    }

    private void addColumnData(ColumnMetadata viewColumn, ColumnData baseTableData)
    {
        assert viewColumn.isComplex() == baseTableData.column().isComplex();
        if (!viewColumn.isComplex())
        {
            addCell(viewColumn, (Cell<?>)baseTableData);
            return;
        }

        ComplexColumnData complexData = (ComplexColumnData)baseTableData;
        currentViewEntryBuilder.addComplexDeletion(viewColumn, complexData.complexDeletion());
        for (Cell<?> cell : complexData)
            addCell(viewColumn, cell);
    }

    private void addCell(ColumnMetadata viewColumn, Cell<?> baseTableCell)
    {
        assert !viewColumn.isPrimaryKeyColumn();
        currentViewEntryBuilder.addCell(baseTableCell.withUpdatedColumn(viewColumn));
    }

    /**
     * Finish building the currently updated view entry and add it to the other built
     * updates.
     */
    private void submitUpdate()
    {
        Row row = currentViewEntryBuilder.build();
        // I'm not sure we can reach there is there is nothing is updated, but adding an empty row breaks things
        // and it costs us nothing to be prudent here.
        if (row.isEmpty())
            return;

        DecoratedKey partitionKey = makeCurrentPartitionKey();
        // We can't really know which columns of the view will be updated nor how many row will be updated for this key
        // so we rely on hopefully sane defaults.
        PartitionUpdate.Builder update = updates.computeIfAbsent(partitionKey,
                                                                 k -> new PartitionUpdate.Builder(viewMetadata,
                                                                                                  partitionKey,
                                                                                                  viewMetadata.regularAndStaticColumns(),
                                                                                                  4));
        update.add(row);
    }

    private DecoratedKey makeCurrentPartitionKey()
    {
        ByteBuffer rawKey = viewMetadata.partitionKeyColumns().size() == 1
                          ? currentViewEntryPartitionKey[0]
                          : CompositeType.build(ByteBufferAccessor.instance, currentViewEntryPartitionKey);

        return viewMetadata.partitioner.decorateKey(rawKey);
    }

    private ByteBuffer getValueForPK(ColumnMetadata column, Row row)
    {
        switch (column.kind)
        {
            case PARTITION_KEY:
                return basePartitionKey[column.position()];
            case CLUSTERING:
                return row.clustering().bufferAt(column.position());
            default:
                // This shouldn't NPE as we shouldn't get there if the value can be null (or there is a bug in updateAction())
                return row.getCell(column).buffer();
        }
    }
}
