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

package org.apache.cassandra.db.partitions;

import javax.annotation.Nullable;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.memtable.TrieCellData;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.memtable.TrieMemtable.PartitionData;

/// The function we provide to the trie utilities to perform any partition and row inserts and updates when a legacy
/// secondary index is in use. This builds on the plain [TriePartitionUpdater] and prepares legacy interpretations of
/// the changes for the indexing calls.
public final class TriePartitionUpdaterLegacyIndex extends TriePartitionUpdater
{
    private UpdateTransaction indexer;
    private TableMetadata metadata;
    /// When a tombstone boundary opens a range, we store the position here to report a range tombstone when it closes.
    ///
    /// Cleared to null when the range is closed. Since all ranges must be closed, this cannot retain a reference at the
    /// end of a mutation.
    private ClusteringBound<byte[]> rangeTombstoneOpenPosition;
    /// The depth at which we saw the root of the current partition. Used to obtain clustering keys of modified rows.
    private int currentPartitionDepth;
    /// The depth at which we saw the root of the current row. Used to obtain cell columns and paths.
    ///
    /// Set when we pass through a row marker ([LivenessInfo] or [TrieTombstoneMarker.LevelMarker.ROW]), which always
    /// happens before seeing any of the columns and cells of the update.
    private int currentRowDepth;
    /// The column set for the current row (differs for static rows)
    ///
    /// Set when we pass through a row marker ([LivenessInfo] or [TrieTombstoneMarker.LevelMarker.ROW]), which always
    /// happens before seeing any of the columns and cells of the update.
    private Columns currentColumns;

    public TriePartitionUpdaterLegacyIndex(TrieMemtable.MemtableShard owner,
                                           InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data,
                                           TableMetadata metadata)
    {
        super(owner, data);
        this.metadata = metadata;
    }

    /// Set the update transaction to use
    public void setIndexContext(UpdateTransaction indexer)
    {
        this.indexer = indexer;
        this.rangeTombstoneOpenPosition = null;
        assert indexer != UpdateTransaction.NO_OP;
    }

    @Override
    public TrieTombstoneMarker mergeMarkers(@Nullable TrieTombstoneMarker existing, TrieTombstoneMarker update)
    {
        TrieTombstoneMarker merged = super.mergeMarkers(existing, update);
        if (merged == null)
            return null; // Note: Row markers always survive deletions and this path will never be reached for them.

        if (merged.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW))
            processRowDeletionUpdate(existing != null ? existing.applicableToPointForward() : null, merged.applicableToPointForward());
        else if (update.isBoundary())
            // Report the deletions of the update (as done for legacy memtables). The merged marker may include
            // existing deletions which the indexer does not need to know about.
            processMarkerBoundary(update);

        return merged;
    }

    private void processRowDeletionUpdate(DeletionTime existing, DeletionTime updated)
    {
        Clustering<?> clustering = metadata.comparator.clusteringFromByteComparable(
            ByteBufferAccessor.instance,
            byteComparableForCurrentDeletionBranchKey());

        Row.Deletion updatedDeletion = updated != null ? Row.Deletion.regular(updated) : Row.Deletion.LIVE;
        if (existing == null)
            indexer.startRow(clustering, null, null, LivenessInfo.EMPTY, updatedDeletion);
        else
            indexer.startRow(clustering, LivenessInfo.EMPTY, Row.Deletion.regular(existing), LivenessInfo.EMPTY, updatedDeletion);

        currentRowDepth = mutator.getDeletionBranchDepth();
        currentColumns = metadata.regularAndStaticColumns().columns(clustering == Clustering.STATIC_CLUSTERING);
    }

    private void processMarkerBoundary(TrieTombstoneMarker update)
    {
        TrieTombstoneMarker.Covering leftSide = update.leftDeletion();
        TrieTombstoneMarker.Covering rightSide = update.rightDeletion();
        TrieTombstoneMarker.Kind leftKind = leftSide != null ? leftSide.deletionKind() : null;
        TrieTombstoneMarker.Kind rightKind = rightSide != null ? rightSide.deletionKind() : null;

        assert leftKind != TrieTombstoneMarker.Kind.ROW && rightKind != TrieTombstoneMarker.Kind.ROW
            : "Row deletion without row level marker: " + update;

        // We need to report column deletions. Do so by issuing it on the open side.
        // Indexer ignores existing deletions, so we don't need to report them here.
        if (rightKind == TrieTombstoneMarker.Kind.COLUMN)
        {
            byte[] cellPath = mutator.getDeletionBranchKeyBytes(currentRowDepth);
            ColumnMetadata column = TrieBackedRow.columnMetadataFromPath(cellPath, cellPath.length, currentColumns);
            indexer.onComplexColumnDeletion(column, rightSide);
            return;
        }
        // For range tombstones, we should only report when they start and stop. This means ignoring all switches
        // that include a lower-level change.
        if (leftKind == TrieTombstoneMarker.Kind.COLUMN)
            return;

        // We should also skip the sides that switch to or from the partition deletion.
        if (leftKind == TrieTombstoneMarker.Kind.PARTITION)
            leftSide = null;
        if (rightKind == TrieTombstoneMarker.Kind.PARTITION)
            rightSide = null;

        if (leftSide != null || rightSide != null)
            processRangeTombstoneMarker(leftSide, rightSide);
    }

    private void processRangeTombstoneMarker(TrieTombstoneMarker.Covering leftSide, TrieTombstoneMarker.Covering rightSide)
    {
        ByteComparable deletionBranchKey = byteComparableForCurrentDeletionBranchKey();
        if (rangeTombstoneOpenPosition != null)
        {
            // We have an active range. The incoming marker's left side must close it. Combine with the start
            // position to form the tombstone range we report to the indexer.
            assert leftSide != null; // open markers are always closed
            ClusteringBound<?> bound = metadata.comparator.boundFromByteComparable(
                ByteArrayAccessor.instance,
                deletionBranchKey,
                true);
            indexer.onRangeTombstone(new RangeTombstone(Slice.make(rangeTombstoneOpenPosition,
                                                                   bound),
                                                        leftSide));
        }
        else
            assert leftSide == null;

        if (rightSide != null)
        {
            // The right side of the marker tells us if this boundary opens a new deletion. If so, store the
            // position to report the range when it closes.
            // Note: we don't need to save the deletion time as the closing side will repeat it.
            rangeTombstoneOpenPosition = metadata.comparator.boundFromByteComparable(
                ByteArrayAccessor.instance,
                deletionBranchKey,
                false);
        }
        else
            rangeTombstoneOpenPosition = null;
    }

    @Override
    Object applyIncomingMarker(Object existingContent, TrieTombstoneMarker updateMarker)
    {
        // We override this to make sure we mark the row start when we start deleting from it, so that we can report
        // all removed cells. Note that if any cells are deleted this will always be called with `LevelMarker.ROW`,
        // possibly with null `applicableToPointForward`.
        if (existingContent instanceof LivenessInfo)
            return applyRowDeletionMarker((LivenessInfo) existingContent, updateMarker);
        else
            return super.applyIncomingMarker(existingContent, updateMarker);
    }

    @Override
    protected CellData<?, ?> applyCellDeletion(CellData<?, ?> existingContent, DeletionTime deletion)
    {
        CellData<?, ?> mergedCellData = super.applyCellDeletion(existingContent, deletion);
        if (mergedCellData == existingContent)
            return mergedCellData;

        byte[] cellPath = mutator.getCurrentKeyBytes(currentRowDepth);
        Cell<?> existingAsCell = TrieBackedRow.cellFromCellData(existingContent, cellPath, cellPath.length, currentColumns);
        assert mergedCellData == null;
        indexer.onCellUpdate(existingAsCell, null);
        return mergedCellData;
    }

    public LivenessInfo applyRowDeletionMarker(LivenessInfo existing, TrieTombstoneMarker marker)
    {
        DeletionTime deletion = marker.applicableToPointForward();
        LivenessInfo mergedInfo = deletion != null ? super.applyRowDeletion(existing, deletion) : existing;
        Clustering<?> clustering = clusteringForCurrentKey();
        // Note: Earlier memtable implementations do not apply range or partition deletions to rows.
        // To emulate this behaviour, we don't set the incoming deletion time to the row, resulting
        // in empty row for indexing, which is filtered out by the indexer.
        indexer.startRow(clustering, existing, Row.Deletion.LIVE, mergedInfo,
                         marker.leftDeletion() != marker.rightDeletion() ? Row.Deletion.regular(deletion)
                                                                         : Row.Deletion.LIVE);

        currentRowDepth = mutator.currentDepth();
        currentColumns = metadata.regularAndStaticColumns().columns(clustering == Clustering.STATIC_CLUSTERING);
        return mergedInfo;
    }

    @Override
    Object applyPartitionDeletion(PartitionData existing, DeletionTime deletion)
    {
        indexer.onPartitionDeletion(deletion);
        return super.applyPartitionDeletion(existing, deletion);
    }

    @Override
    protected PartitionData mergePartitionMarkers(@Nullable PartitionData existing)
    {
        currentPartitionDepth = mutator.currentDepth();
        return super.mergePartitionMarkers(existing);
    }

    @Override
    LivenessInfo applyIncomingRowMarker(@Nullable LivenessInfo existing, LivenessInfo insert)
    {
        Clustering<?> clustering = clusteringForCurrentKey();
        LivenessInfo mergedInfo = super.applyIncomingRowMarker(existing, insert);
        indexer.startRow(clustering, existing, Row.Deletion.LIVE, mergedInfo, Row.Deletion.LIVE);
        currentRowDepth = mutator.currentDepth();
        currentColumns = metadata.regularAndStaticColumns().columns(clustering == Clustering.STATIC_CLUSTERING);
        return mergedInfo;
    }

    @Override
    CellData<?, ?> applyCell(@Nullable TrieCellData existing, CellData<?, ?> update)
    {
        CellData<?, ?> mergedCellData = super.applyCell(existing, update);
        byte[] cellPath = mutator.getCurrentKeyBytes(currentRowDepth);
        Cell<?> existingAsCell = null;
        Cell<?> mergedAsCell = null;
        if (mergedCellData != null)
        {
            mergedAsCell = TrieBackedRow.cellFromCellData(mergedCellData, cellPath, cellPath.length, currentColumns);
            if (existing != null)
                existingAsCell = existing.toCell(mergedAsCell.column(), mergedAsCell.path());
        }
        else
        {
            assert existing != null;
            existingAsCell = TrieBackedRow.cellFromCellData(existing, cellPath, cellPath.length, currentColumns);
        }
        indexer.onCellUpdate(existingAsCell, mergedAsCell);

        return mergedCellData;
    }

    private ByteComparable byteComparableForCurrentDeletionBranchKey()
    {
        return ByteComparable.preencoded(mutator.byteComparableVersion(),
                                         mutator.getDeletionBranchKeyBytes());
    }

    /// Note: this uses [ByteBufferAccessor] to produce the [Clustering#STATIC_CLUSTERING] singleton for static rows.
    private Clustering<?> clusteringForCurrentKey()
    {
        return metadata.comparator.clusteringFromByteComparable(
            ByteBufferAccessor.instance,
            ByteComparable.preencoded(mutator.byteComparableVersion(),
                                      mutator.getCurrentKeyBytes(currentPartitionDepth)));
    }
}
