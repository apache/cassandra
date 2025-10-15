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

import javax.annotation.Nullable;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;

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
    protected static final Logger logger = LoggerFactory.getLogger(ViewUpdateGenerator.class);
    private final View view;
    private final int nowInSec;

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

    private enum ReadRebuildAction
    {
        REWRITE,                // Rewrite the base row entry
        DELETE_FROM_BASE_READ        // Delete the base row entry
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
    public ViewUpdateGenerator(View view, DecoratedKey basePartitionKey, int nowInSec)
    {
        this.view = view;
        this.nowInSec = nowInSec;

        this.baseMetadata = view.getDefinition().baseTableMetadata();
        this.baseEnforceStrictLiveness = baseMetadata.enforceStrictLiveness();
        this.baseDecoratedKey = basePartitionKey;
        this.basePartitionKey = ViewUtils.extractKeyComponents(basePartitionKey, baseMetadata.partitionKeyType);

        this.viewMetadata = Schema.instance.getTableMetadata(view.getDefinition().metadata.id);

        this.currentViewEntryPartitionKey = new ByteBuffer[viewMetadata.partitionKeyColumns().size()];
        this.currentViewEntryBuilder = BTreeRow.sortedBuilder();
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

    public void addBaseTableRowForReadRebuild(@Nullable Row baseRow,
                                              Clustering<?> baseClustering,
                                              long readTime,
                                              @Nullable ByteBuffer nonPKValue)
    {
        assert (nonPKValue == null && view.hasSamePrimaryKeyColumnsAsBaseTable())
               || (nonPKValue != null && !view.hasSamePrimaryKeyColumnsAsBaseTable()) : "nonPKValue should be null iff view has same PK columns as base table";

        switch (updateActionForReadRebuild(baseRow, nonPKValue))
        {
            case REWRITE:
                // updateActionForReadRebuild returns REWRITE only when baseRow is non-null
                logger.debug("REWRITE view entry for base row: {}", baseRow);
                createEntry(baseRow);
                return;
            case DELETE_FROM_BASE_READ:
                logger.debug("DELETE_FROM_BASE_READ view entry for base row: {}", baseRow);
                deleteEntryFromBaseRead(baseRow, baseClustering, readTime, nonPKValue);
                return;
        }
    }

    Row maybeAddDeletionFromReadTime(@Nullable Row baseRow,
                                     Clustering<?> baseClustering,
                                     long readTime)
    {
        // In case we read null from the base table, the tombstone is disacrded.
        // We fabricate a deleted row with deletion time the same as the read time.
        if (baseRow == null)
            return BTreeRow.emptyDeletedRow(baseClustering,
                                            Row.Deletion.regular(new DeletionTime(readTime, nowInSec)));

        // check if non-pk is null - if so we add a psedo deletion with the read time as deletion time to the cell
        if (!view.hasSamePrimaryKeyColumnsAsBaseTable())
        {
            ColumnMetadata nonPKCol = view.baseNonPKColumnsInViewPK.get(0);
            ColumnMetadata baseNonPkCol = view.getBaseColumn(nonPKCol);
            ColumnData baseNonPKColData = baseRow.getColumnData(baseNonPkCol);

            if (baseNonPKColData == null)
            {
                // clone the base row and add a pseudo deletion to the non-pk cell
                Row.Builder builder = BTreeRow.unsortedBuilder();
                builder.newRow(baseClustering);
                builder.addRowDeletion(baseRow.deletion());
                builder.addPrimaryKeyLivenessInfo(baseRow.primaryKeyLivenessInfo());
                for (ColumnData data : baseRow.columnData())
                {
                    if (data.column().isComplex())
                    {
                        ComplexColumnData complexData = (ComplexColumnData)data;
                        builder.addComplexDeletion(data.column(), complexData.complexDeletion());
                        for (Cell<?> cell : complexData)
                            builder.addCell(cell);
                    }
                    else
                        builder.addCell((Cell<?>)data);
                }

                // add a pseudo deletion to the cell
                builder.addCell(BufferCell.tombstone(baseNonPkCol, readTime, nowInSec));
                return builder.build();
            }
        }
        return baseRow;
    }

    private void deleteEntryFromBaseRead(@Nullable Row baseRow,
                                         Clustering<?> baseClustering,
                                         long readTime,
                                         @Nullable ByteBuffer nonPKValue)
    {
        Row targetRow = maybeAddDeletionFromReadTime(baseRow, baseClustering, readTime);
        // compute the view PK
        if (view.hasSamePrimaryKeyColumnsAsBaseTable())
            startNewUpdate(targetRow);
        else
        {
            // baseRow can have mismatched nonPKValue. The deletion should be issued on the targeted PK
            ByteBuffer[] clusteringValues = new ByteBuffer[viewMetadata.clusteringColumns().size()];
            for (ColumnMetadata viewColumn : viewMetadata.primaryKeyColumns())
            {
                ByteBuffer value = view.baseNonPKColumnsInViewPK.contains(view.getBaseColumn(viewColumn))
                                   ? nonPKValue
                                   : ViewUtils.getValueForPK(view.getBaseColumn(viewColumn), targetRow, basePartitionKey);
                if (viewColumn.isPartitionKey())
                    currentViewEntryPartitionKey[viewColumn.position()] = value;
                else
                    clusteringValues[viewColumn.position()] = value;
            }
            currentViewEntryBuilder.newRow(Clustering.make(clusteringValues));
        }

        // compute a deletion timestamp for the entry
        DeletionTime rowDeletion = targetRow.deletion().time();
        long timestamp = rowDeletion.markedForDeleteAt();
        if (view.hasSamePrimaryKeyColumnsAsBaseTable())
        {
            timestamp = Math.max(timestamp, targetRow.primaryKeyLivenessInfo().timestamp());
            if (!view.getDefinition().includeAllColumns)
            {
                for (Cell<?> cell : targetRow.cells())
                {
                    // At this point, we know baseRow.hasLiveData=false, which means
                    // 1. Primary key liveness info says dead (never had row level liveness, or it's already expired)
                    // 2. any(cell.isLive)=false

                    // Here we need to determine the timestamp for the expired TTL liveness info. (See ViewUtils.computeLivenessInfoForEntry)
                    // In deleteOldEntryInternal, one will only check the unselected cells (when PK is the same, and there are unselected columns)
                    // Here instead we'll have to check all cells - The intention here is to ensure that the deletion
                    // shadow any existing cell, so we need to find the max timestamp among all cells
                    timestamp = Math.max(timestamp, cell.maxTimestamp());
                }
            }
        }
        else
        {
            Cell<?> nonPKCell = targetRow.getCell(view.baseNonPKColumnsInViewPK.get(0));
            // targetRow can be an empty deleted row, in which case nonPKCell is null
            timestamp = (nonPKCell != null && !rowDeletion.deletes(nonPKCell))
                        ? nonPKCell.timestamp()
                        : rowDeletion.markedForDeleteAt();
        }

        // ensure that deletion can only at max readTime, can only happen if we read a timestamp in the future
        timestamp = Math.min(timestamp, readTime);
        addDeletion(targetRow, timestamp, rowDeletion.markedForDeleteAt());
        submitUpdate();
    }

    ReadRebuildAction updateActionForReadRebuild(@Nullable Row baseRow, @Nullable ByteBuffer nonPKValue)
    {
        // REWRITE: baseRow is alive
        // baseRow is alive from view's perspective, either:
        // 1. has non-pk base column in view pk, which is alive
        // 2. doesn't have the non-pk base column in view pk, baseRow has live data

        // DELETE: baseRow is dead
        // baseRow is dead from view's perspective, either:
        // 1. has non-pk base column in view pk, which is null/dead, or mismatch with the clustering key
        //    e.g., view pk (k,v), base pk (k), base non-pk column v. We want to fix (k=1,v=1) in view, however,
        //    we read (k=1,v=9) from base table, so we need to delete (k=1,v=1) in view
        // 2. doesn't have the non-pk base column in view pk, baseRow is null/dead

        // TODO: obervability on each action taken?

        if (baseRow == null)
            return ReadRebuildAction.DELETE_FROM_BASE_READ;

        if (view.hasSamePrimaryKeyColumnsAsBaseTable())
        {
            return baseRow.hasLiveData(nowInSec, baseEnforceStrictLiveness)
                   ? ReadRebuildAction.REWRITE
                   : ReadRebuildAction.DELETE_FROM_BASE_READ;
        }

        ColumnMetadata nonPKCol = view.baseNonPKColumnsInViewPK.get(0);
        ColumnMetadata baseNonPkCol = view.getBaseColumn(nonPKCol);
        Cell<?> cell = baseRow.getCell(baseNonPkCol);
        if (!ViewUtils.isLive(cell, nowInSec))
            return ReadRebuildAction.DELETE_FROM_BASE_READ;
        // if the non-pk cell matches
        return cell.buffer().equals(nonPKValue)
               ? ReadRebuildAction.REWRITE
               : ReadRebuildAction.DELETE_FROM_BASE_READ;
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
            return ViewUtils.isLive(before, nowInSec) ? UpdateAction.UPDATE_EXISTING : UpdateAction.NONE;

        if (!ViewUtils.isLive(before, nowInSec))
            return ViewUtils.isLive(after, nowInSec) ? UpdateAction.NEW_ENTRY : UpdateAction.NONE;
        if (!ViewUtils.isLive(after, nowInSec))
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
        currentViewEntryBuilder.addPrimaryKeyLivenessInfo(ViewUtils.computeLivenessInfoForEntry(view, baseRow, nowInSec));
        currentViewEntryBuilder.addRowDeletion(baseRow.deletion());

        for (ColumnData data : baseRow)
        {
            ColumnMetadata viewColumn = view.getViewColumn(data.column());
            // If that base table column is not denormalized in the view, we had nothing to do.
            // Alose, if it's part of the view PK it's already been taken into account in the clustering.
            if (viewColumn == null || viewColumn.isPrimaryKeyColumn())
                continue;

            ViewUtils.addColumnDataToBuilder(currentViewEntryBuilder, viewColumn, data);
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
        currentViewEntryBuilder.addPrimaryKeyLivenessInfo(ViewUtils.computeLivenessInfoForEntry(view, mergedBaseRow, nowInSec));
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
                ViewUtils.addColumnDataToBuilder(currentViewEntryBuilder, viewColumn, mergedData);
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
                        ViewUtils.addCellToBuilder(currentViewEntryBuilder, viewColumn, mergedCell);
                }
            }
            else
            {
                // Note that we've already eliminated the case where merged == existing
                ViewUtils.addCellToBuilder(currentViewEntryBuilder, viewColumn, (Cell<?>)mergedData);
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

        addDeletion(mergedBaseRow, timestamp, rowDeletion);
        addDifferentCells(existingBaseRow, mergedBaseRow);
        submitUpdate();
    }

    private void addDeletion(Row mergedBaseRow, long timestamp, long rowDeletion)
    {
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
            ByteBuffer value = ViewUtils.getValueForPK(baseColumn, baseRow, basePartitionKey);
            if (viewColumn.isPartitionKey())
                currentViewEntryPartitionKey[viewColumn.position()] = value;
            else
                clusteringValues[viewColumn.position()] = value;
        }

        currentViewEntryBuilder.newRow(Clustering.make(clusteringValues));
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
        assert ViewUtils.isLive(before, nowInSec) : "We shouldn't have got there if the base row had no associated entry";
        return deletion.deletes(before) ? deletion.markedForDeleteAt() : before.timestamp();
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

        DecoratedKey partitionKey = ViewUtils.makeViewPartitionKey(viewMetadata, currentViewEntryPartitionKey);
        // We can't really know which columns of the view will be updated nor how many row will be updated for this key
        // so we rely on hopefully sane defaults.
        PartitionUpdate.Builder update = updates.computeIfAbsent(partitionKey,
                                                                 k -> new PartitionUpdate.Builder(viewMetadata,
                                                                                                  partitionKey,
                                                                                                  viewMetadata.regularAndStaticColumns(),
                                                                                                  4));
        update.add(row);
    }

}
