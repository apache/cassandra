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

package org.apache.cassandra.service.reads.repair;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;
import java.util.function.Consumer;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import com.carrotsearch.hppc.ObjectIntHashMap;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowDiffListener;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.schema.ColumnMetadata;

public class RowIteratorMergeListener<E extends Endpoints<E>>
        implements UnfilteredRowIterators.MergeListener
{
    private final DecoratedKey partitionKey;
    private final RegularAndStaticColumns columns;
    private final boolean isReversed;
    private final ReadCommand command;

    private final BitSet writeBackTo;
    private final boolean buildFullDiff;
    /** the repairs we will send to each source, suffixed by a complete repair of all differences, if {@link #buildFullDiff} */
    private final PartitionUpdate.Builder[] repairs;
    private final Row.Builder[] currentRows;
    private final RowDiffListener diffListener;
    private final ReplicaPlan.ForRead<E, ?> readPlan;
    private final ReplicaPlan.ForWrite writePlan;

    // The partition level deletion for the merge row.
    private DeletionTime partitionLevelDeletion;
    // When merged has a currently open marker, its time. null otherwise.
    private DeletionTime mergedDeletionTime;
    // For each source, the time of the current deletion as known by the source.
    private final DeletionTime[] sourceDeletionTime;
    // For each source, record if there is an open range to send as repair, and from where.
    private final ClusteringBound<?>[] markerToRepair;

    private final ReadRepair readRepair;

    public RowIteratorMergeListener(DecoratedKey partitionKey, RegularAndStaticColumns columns, boolean isReversed, ReplicaPlan.ForRead<E, ?> readPlan, ReadCommand command, ReadRepair readRepair)
    {
        this.partitionKey = partitionKey;
        this.columns = columns;
        this.isReversed = isReversed;
        this.readPlan = readPlan;
        this.writePlan = ReplicaPlans.forReadRepair(partitionKey.getToken(), readPlan);

        int size = readPlan.contacts().size();
        this.writeBackTo = new BitSet(size);
        {
            int i = 0;
            for (Replica replica : readPlan.contacts())
            {
                if (writePlan.contacts().endpoints().contains(replica.endpoint()))
                    writeBackTo.set(i);
                ++i;
            }
        }
        // If we are contacting any nodes we didn't read from, we are likely handling a range movement.
        // In this case we need to send all differences to these nodes, as we do not (with present design) know which
        // node they bootstrapped from, and so which data we need to duplicate.
        // In reality, there will be situations where we are simply sending the same number of writes to different nodes
        // and in this case we could probably avoid building a full difference, and only ensure each write makes it to
        // some other node, but it is probably not worth special casing this scenario.
        this.buildFullDiff = Iterables.any(writePlan.contacts().endpoints(), e -> !readPlan.contacts().endpoints().contains(e));
        this.repairs = new PartitionUpdate.Builder[size + (buildFullDiff ? 1 : 0)];
        this.currentRows = new Row.Builder[size];
        this.sourceDeletionTime = new DeletionTime[size];
        this.markerToRepair = new ClusteringBound<?>[size];
        this.command = command;
        this.readRepair = readRepair;

        this.diffListener = new RowDiffListener()
        {
            public void onPrimaryKeyLivenessInfo(int i, Clustering<?> clustering, LivenessInfo merged, LivenessInfo original)
            {
                if (merged != null && !merged.equals(original))
                    currentRow(i, clustering).addPrimaryKeyLivenessInfo(merged);
            }

            public void onDeletion(int i, Clustering<?> clustering, Row.Deletion merged, Row.Deletion original)
            {
                if (merged != null && !merged.equals(original))
                    currentRow(i, clustering).addRowDeletion(merged);
            }

            public void onComplexDeletion(int i, Clustering<?> clustering, ColumnMetadata column, DeletionTime merged, DeletionTime original)
            {
                if (merged != null && !merged.equals(original))
                    currentRow(i, clustering).addComplexDeletion(column, merged);
            }

            public void onCell(int i, Clustering<?> clustering, Cell<?> merged, Cell<?> original)
            {
                if (merged != null && !merged.equals(original) && isQueried(merged))
                    currentRow(i, clustering).addCell(merged);
            }

            private boolean isQueried(Cell<?> cell)
            {
                // When we read, we may have some cell that have been fetched but are not selected by the user. Those cells may
                // have empty values as optimization (see CASSANDRA-10655) and hence they should not be included in the read-repair.
                // This is fine since those columns are not actually requested by the user and are only present for the sake of CQL
                // semantic (making sure we can always distinguish between a row that doesn't exist from one that do exist but has
                /// no value for the column requested by the user) and so it won't be unexpected by the user that those columns are
                // not repaired.
                ColumnMetadata column = cell.column();
                ColumnFilter filter = RowIteratorMergeListener.this.command.columnFilter();
                return column.isComplex() ? filter.fetchedCellIsQueried(column, cell.path()) : filter.fetchedColumnIsQueried(column);
            }
        };
    }

    /**
     * The partition level deletion with with which source {@code i} is currently repaired, or
     * {@code DeletionTime.LIVE} if the source is not repaired on the partition level deletion (meaning it was
     * up to date on it). The output* of this method is only valid after the call to
     * {@link #onMergedPartitionLevelDeletion}.
     */
    private DeletionTime partitionLevelRepairDeletion(int i)
    {
        return repairs[i] == null ? DeletionTime.LIVE : repairs[i].partitionLevelDeletion();
    }

    private Row.Builder currentRow(int i, Clustering<?> clustering)
    {
        if (currentRows[i] == null)
        {
            currentRows[i] = BTreeRow.sortedBuilder();
            currentRows[i].newRow(clustering);
        }
        return currentRows[i];
    }

    @Inline
    private void applyToPartition(int i, Consumer<PartitionUpdate.Builder> f)
    {
        if (writeBackTo.get(i))
        {
            if (repairs[i] == null)
                repairs[i] = new PartitionUpdate.Builder(command.metadata(), partitionKey, columns, 1);
            f.accept(repairs[i]);
        }
        if (buildFullDiff)
        {
            if (repairs[repairs.length - 1] == null)
                repairs[repairs.length - 1] = new PartitionUpdate.Builder(command.metadata(), partitionKey, columns, 1);
            f.accept(repairs[repairs.length - 1]);
        }
    }

    public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
    {
        this.partitionLevelDeletion = mergedDeletion;
        for (int i = 0; i < versions.length; i++)
        {
            if (mergedDeletion.supersedes(versions[i]))
                applyToPartition(i, p -> p.addPartitionDeletion(mergedDeletion));
        }
    }

    public Row onMergedRows(Row merged, Row[] versions)
    {
        // If a row was shadowed post merged, it must be by a partition level or range tombstone, and we handle
        // those case directly in their respective methods (in other words, it would be inefficient to send a row
        // deletion as repair when we know we've already send a partition level or range tombstone that covers it).
        if (merged.isEmpty())
            return merged;

        Rows.diff(diffListener, merged, versions);
        for (int i = 0; i < currentRows.length; i++)
        {
            if (currentRows[i] != null)
            {
                Row row = currentRows[i].build();
                applyToPartition(i, p -> p.add(row));
            }
        }
        Arrays.fill(currentRows, null);

        return merged;
    }

    private DeletionTime currentDeletion()
    {
        return mergedDeletionTime == null ? partitionLevelDeletion : mergedDeletionTime;
    }

    public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions)
    {
        // The current deletion as of dealing with this marker.
        DeletionTime currentDeletion = currentDeletion();

        for (int i = 0; i < versions.length; i++)
        {
            // we are not collecting a mutation for this version/source, skip;
            if (!writeBackTo.get(i))
                continue;
            RangeTombstoneMarker marker = versions[i];

            // Update what the source now thinks is the current deletion
            if (marker != null)
                sourceDeletionTime[i] = marker.isOpen(isReversed) ? marker.openDeletionTime(isReversed) : null;

            // If merged == null, some of the source is opening or closing a marker
            if (merged == null)
            {
                // but if it's not this source, move to the next one
                if (marker == null)
                    continue;

                // We have a close and/or open marker for a source, with nothing corresponding in merged.
                // Because merged is a superset, this implies that we have a current deletion (either due to an
                // early opening in merged or a partition level deletion) and that this deletion will still be
                // active after that point. Further whatever deletion was open or is open by this marker on the
                // source, that deletion cannot supersedes the current one.
                //
                // But while the marker deletion (before and/or after this point) cannot supersede the current
                // deletion, we want to know if it's equal to it (both before and after), because in that case
                // the source is up to date and we don't want to include it into repair.
                //
                // So in practice we have 2 possible cases:
                //  1) the source was up-to-date on deletion up to that point: then it won't be from that point
                //     on unless it's a boundary and the new opened deletion time is also equal to the current
                //     deletion (note that this implies the boundary has the same closing and opening deletion
                //     time, which should generally not happen, but can due to legacy reading code not avoiding
                //     this for a while, see CASSANDRA-13237).
                //  2) the source wasn't up-to-date on deletion up to that point and it may now be (if it isn't
                //     we just have nothing to do for that marker).
                assert !currentDeletion.isLive() : currentDeletion.toString();

                // Is the source up to date on deletion? It's up to date if it doesn't have an open RT repair
                // nor an "active" partition level deletion (where "active" means that it's greater or equal
                // to the current deletion: if the source has a repaired partition deletion lower than the
                // current deletion, this means the current deletion is due to a previously open range tombstone,
                // and if the source isn't currently repaired for that RT, then it means it's up to date on it).
                DeletionTime partitionRepairDeletion = partitionLevelRepairDeletion(i);

                if (markerToRepair[i] == null && currentDeletion.supersedes(partitionRepairDeletion))
                {
                    /*
                     * Since there is an ongoing merged deletion, the only two ways we don't have an open repair for
                     * this source are that:
                     *
                     * 1) it had a range open with the same deletion as current marker, and the marker is coming from
                     *    a short read protection response - repeating the open RT bound, or
                     * 2) it had a range open with the same deletion as current marker, and the marker is closing it.
                     */
                    if (!marker.isBoundary() && marker.isOpen(isReversed)) // (1)
                    {
                        assert currentDeletion.equals(marker.openDeletionTime(isReversed))
                        : String.format("currentDeletion=%s, marker=%s", currentDeletion, marker.toString(command.metadata()));
                    }
                    else // (2)
                    {
                        assert marker.isClose(isReversed) && currentDeletion.equals(marker.closeDeletionTime(isReversed))
                        : String.format("currentDeletion=%s, marker=%s", currentDeletion, marker.toString(command.metadata()));
                    }

                    // and so unless it's a boundary whose opening deletion time is still equal to the current
                    // deletion (see comment above for why this can actually happen), we have to repair the source
                    // from that point on.
                    if (!(marker.isOpen(isReversed) && currentDeletion.equals(marker.openDeletionTime(isReversed))))
                        markerToRepair[i] = marker.closeBound(isReversed).invert();
                }
                // In case 2) above, we only have something to do if the source is up-to-date after that point
                // (which, since the source isn't up-to-date before that point, means we're opening a new deletion
                // that is equal to the current one).
                else
                {
                    if (markerToRepair[i] == null)
                    {
                        // Only way we can have no open RT repair is that partition deletion that has the same timestamp
                        // as the deletion and same local deletion time. In such case, since partition deletion covers
                        // an entire partition, we do not include it into repair.
                        assert currentDeletion.localDeletionTime() == partitionRepairDeletion.localDeletionTime();
                    }
                    else if (marker.isOpen(isReversed) && currentDeletion.equals(marker.openDeletionTime(isReversed)))
                    {
                        closeOpenMarker(i, marker.openBound(isReversed).invert());
                    }
                }
            }
            else
            {
                // We have a change of current deletion in merged (potentially to/from no deletion at all).

                if (merged.isClose(isReversed))
                {
                    // We're closing the merged range. If we're recorded that this should be repaird for the
                    // source, close and add said range to the repair to send.
                    if (markerToRepair[i] != null)
                        closeOpenMarker(i, merged.closeBound(isReversed));

                }

                if (merged.isOpen(isReversed))
                {
                    // If we're opening a new merged range (or just switching deletion), then unless the source
                    // is up to date on that deletion (note that we've updated what the source deleteion is
                    // above), we'll have to sent the range to the source.
                    DeletionTime newDeletion = merged.openDeletionTime(isReversed);
                    DeletionTime sourceDeletion = sourceDeletionTime[i];
                    if (!newDeletion.equals(sourceDeletion))
                        markerToRepair[i] = merged.openBound(isReversed);
                }
            }
        }

        if (merged != null)
            mergedDeletionTime = merged.isOpen(isReversed) ? merged.openDeletionTime(isReversed) : null;
    }

    private void closeOpenMarker(int i, ClusteringBound<?> close)
    {
        ClusteringBound<?> open = markerToRepair[i];
        RangeTombstone rt = new RangeTombstone(Slice.make(isReversed ? close : open, isReversed ? open : close), currentDeletion());
        applyToPartition(i, p -> p.add(rt));
        markerToRepair[i] = null;
    }

    public void close()
    {
        boolean hasRepairs = false;
        for (int i = 0 ; !hasRepairs && i < repairs.length ; ++i)
            hasRepairs = repairs[i] != null;
        if (!hasRepairs)
            return;

        PartitionUpdate fullDiffRepair = null;
        if (buildFullDiff && repairs[repairs.length - 1] != null)
            fullDiffRepair = repairs[repairs.length - 1].build();

        Map<Replica, Mutation> mutations = Maps.newHashMapWithExpectedSize(writePlan.contacts().size());
        ObjectIntHashMap<InetAddressAndPort> sourceIds = new ObjectIntHashMap<>(((repairs.length + 1) * 4) / 3);
        for (int i = 0 ; i < readPlan.contacts().size() ; ++i)
            sourceIds.put(readPlan.contacts().get(i).endpoint(), 1 + i);

        for (Replica replica : writePlan.contacts())
        {
            PartitionUpdate update = null;
            int i = -1 + sourceIds.get(replica.endpoint());
            if (i < 0)
                update = fullDiffRepair;
            else if (repairs[i] != null)
                update = repairs[i].build();

            Mutation mutation = BlockingReadRepairs.createRepairMutation(update, readPlan.consistencyLevel(), replica.endpoint(), false);
            if (mutation == null)
                continue;

            mutations.put(replica, mutation);
        }

        readRepair.repairPartition(partitionKey, mutations, writePlan);
    }
}
