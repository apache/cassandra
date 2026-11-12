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

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.memtable.TrieCellData;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;

import static org.apache.cassandra.db.memtable.TrieMemtable.PartitionData;

/// The function we provide to the trie utilities to perform any partition and row inserts and updates.
/// This version is used when no secondary index is applied, which makes the process quite a bit simpler.
public class TriePartitionUpdater
{
    private final TrieMemtable.MemtableShard owner;
    protected final InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker> mutator;

    public long dataSize;
    public long colUpdateTimeDelta;
    public int partitionsAdded;

    /// Holds a reference to the current partition's statistics, used to update them when merging data.
    protected PartitionData currentPartition;

    public TriePartitionUpdater(TrieMemtable.MemtableShard owner,
                                InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        this.owner = owner;
        this.mutator = data.mutator(this::mergeData,
                                    this::mergeMarkers,
                                    this::applyIncomingMarker,
                                    this::applyExistingMarkerToIncomingRow,
                                    true,
                                    TrieMemtable.FORCE_COPY_PARTITION_BOUNDARY,
                                    x -> { throw new AssertionError("Force copy should already be in effect for all range tries"); },
                                    this::dropLevelMarker);
    }

    /// Merge the given update into the data trie.
    public void mergeUpdate(DeletionAwareTrie<Object, TrieTombstoneMarker> update) throws TrieSpaceExhaustedException
    {
        this.currentPartition = null;
        this.partitionsAdded = 0;
        this.dataSize = 0;
        this.colUpdateTimeDelta = Long.MAX_VALUE;

        try
        {
            mutator.apply(update);
        }
        finally
        {
            this.currentPartition = null; // clear the reference to avoid holding on to data
        }
    }

    /// Merge incoming live data (cell, liveness info or various level markers) with existing content.
    Object mergeData(@Nullable Object existing, Object update)
    {
        // Most common case first
        if (update instanceof CellData)
            return applyCell((TrieCellData) existing, (CellData<?, ?>) update);
        else if (update == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            return update;
        else if (update instanceof LivenessInfo)
            return applyIncomingRowMarker((LivenessInfo) existing, (LivenessInfo) update);
        else if (update == TrieBackedPartition.PARTITION_MARKER)
            return mergePartitionMarkers((PartitionData) existing);
        else
            throw new AssertionError("Unexpected update type: " + update.getClass());
    }

    long dataSizeOfMarker(TrieTombstoneMarker marker)
    {
        if (!marker.isBoundary())
            return 0;
        // We will only count one of the sides.
        TrieTombstoneMarker.Covering rightSide = marker.rightDeletion();
        if (rightSide == null)
            return 0;
        else
            return rightSide.dataSize();
    }

    /// Merge an incoming tombstone with existing deletions.
    /// This will be called for all boundary tombstones in the update, but also for all existing boundaries that are
    /// covered by an incoming range.
    TrieTombstoneMarker mergeMarkers(@Nullable TrieTombstoneMarker existing, TrieTombstoneMarker update)
    {
        if (existing == null)
        {
            if (TriePartitionUpdate.startsRowDeletion(update))
                currentPartition.markAddedTombstones(1);

            dataSize += dataSizeOfMarker(update);
            return update;
        }
        else
        {
            // We are adding a new tombstone. We are counting tombstones on the row level, so ones that introduce
            // or close column deletions should not count.
            // We will only count one of the sides as we want to increase the count by one for each pair.
            TrieTombstoneMarker merged = update.mergeWith(existing);
            int hadTombstone = existing.isBoundary() && TriePartitionUpdate.startsRowDeletion(existing) ? 1 : 0;
            int hasTombstone = merged != null && merged.isBoundary() && TriePartitionUpdate.startsRowDeletion(merged) ? 1 : 0;
            currentPartition.markAddedTombstones(hasTombstone - hadTombstone);
            dataSize -= dataSizeOfMarker(existing);
            dataSize += dataSizeOfMarker(update);
            return merged;
        }
    }

    /// Apply an incoming tombstone to existing data, possibly removing it from the trie.
    Object applyIncomingMarker(Object existingContent, TrieTombstoneMarker updateMarker)
    {
        DeletionTime deletion = updateMarker.applicableToPointForward();
        if (deletion == null)
            return existingContent;

        // Most common case first
        if (existingContent instanceof CellData)
            return applyCellDeletion((CellData<?, ?>) existingContent, deletion);
        else if (existingContent == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            return existingContent;
        else if (existingContent instanceof LivenessInfo)
            return applyRowDeletion((LivenessInfo) existingContent, deletion);
        else if (existingContent instanceof PartitionData)
            return applyPartitionDeletion((PartitionData) existingContent, deletion);
        else
            throw new AssertionError("Unexpected content in trie " + existingContent + " for deletion " + updateMarker);
    }

    protected CellData<?, ?> applyCellDeletion(CellData<?, ?> existingContent, DeletionTime deletion)
    {
        if (!deletion.deletes(existingContent))
            return existingContent;
        dataSize -= existingContent.dataSizeWithoutPath();
        return null;
    }

    Object applyPartitionDeletion(PartitionData existing, DeletionTime unused)
    {
        return existing;
    }

    LivenessInfo applyRowDeletion(LivenessInfo existing, DeletionTime deletion)
    {
        if (existing != LivenessInfo.EMPTY && deletion.deletes(existing))
        {
            dataSize -= existing.dataSize();
            return LivenessInfo.EMPTY;
            // TODO: Does strict row liveness apply here? How do we drop tail trie if it does?
        }
        return existing;
    }


    private void dropLevelMarker(Object o)
    {
        if (o == LivenessInfo.EMPTY)
            currentPartition.markInsertedRows(-1);
        if (o instanceof TrieTombstoneMarker && ((TrieTombstoneMarker) o).rightDeletion() != null)
        {
            currentPartition.markAddedTombstones(-1);
            dataSize -= dataSizeOfMarker((TrieTombstoneMarker) o);
        }
    }


    /// Apply an existing tombstone to incoming data before merging that data in the trie.
    Object applyExistingMarkerToIncomingRow(TrieTombstoneMarker marker, Object content)
    {
        DeletionTime rowDeletion = marker.applicableToPointForward();
        if (rowDeletion == null)
            return content; // there is no row deletion here

        // No size tracking is needed, because the result of this gets applied to the trie with applyRow.
        if (content instanceof Cell)
            return rowDeletion.deletes((Cell<?>) content) ? null : content;
        else if (content == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            return content;
        else if (content instanceof LivenessInfo)
        {
            if (!rowDeletion.deletes((LivenessInfo) content))
                return content;
            else
                return LivenessInfo.EMPTY;
        }
        else if (content instanceof PartitionData)
            return content;
        else
            throw new AssertionError("Unexpected content in trie " + content + " for deletion " + marker);
    }

    LivenessInfo applyIncomingRowMarker(@Nullable LivenessInfo existing, LivenessInfo insert)
    {
        if (existing == null)
        {
            this.dataSize += insert.dataSize();
            currentPartition.markInsertedRows(1);  // null pointer here means a problem in applyDeletion
            return insert;
        }
        else
        {
            LivenessInfo reconciled = LivenessInfo.merge(existing, insert);
            if (reconciled != existing)
                this.dataSize += reconciled.dataSize() - existing.dataSize();

            return reconciled;
        }
    }

    CellData<?, ?> applyCell(@Nullable TrieCellData existing, CellData<?, ?> update)
    {
        if (existing == null)
        {
            this.dataSize += update.dataSizeWithoutPath();
            return update;
        }
        else
        {
            CellData<?, ?> reconciled = Cells.<CellData>reconcile(existing, update);
            if (reconciled != existing)
            {
                long timeDelta = Math.abs(reconciled.timestamp() - existing.timestamp());
                if (timeDelta < colUpdateTimeDelta)
                    colUpdateTimeDelta = timeDelta;
                this.dataSize += reconciled.dataSizeWithoutPath() - existing.dataSizeWithoutPath();
            }
            return reconciled;
        }
    }

    /// Called at the partition boundary to merge the existing and new metadata associated with the partition. This needs
    /// to make sure that the statistics we track for the partition (dataSize) are updated for the changes caused by
    /// merging the update's rows.
    ///
    /// @param existing Any partition data already associated with the partition.
    /// @return the combined partition data, creating a new marker if one did not already exist.
    protected PartitionData mergePartitionMarkers(@Nullable PartitionData existing)
    {
        if (existing == null)
        {
            PartitionData newRef = new PartitionData(owner);
            ++this.partitionsAdded;
            return currentPartition = newRef;
        }

        assert owner == existing.owner;
        return currentPartition = existing;
    }
}
