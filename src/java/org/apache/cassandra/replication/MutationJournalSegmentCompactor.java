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
package org.apache.cassandra.replication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.journal.SegmentCompactor;
import org.apache.cassandra.journal.StaticSegment;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.replication.migration.KeyspaceMigrationInfo;
import org.apache.cassandra.service.replication.migration.MutationTrackingMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Segment compactor: takes static segments, selects the ones that are safe to drop, and drops them
 */
class MutationJournalSegmentCompactor implements SegmentCompactor<ShortMutationId, Mutation>
{
    private static final Logger logger = LoggerFactory.getLogger(MutationJournalSegmentCompactor.class);

    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);

    private final MutationJournal journal;

    // for testing only
    private final Supplier<Log2OffsetsMap<?>> durablyReconciledOffsetsSupplier;

    @VisibleForTesting
    MutationJournalSegmentCompactor(MutationJournal journal, Supplier<Log2OffsetsMap<?>> durablyReconciledOffsetsSupplier)
    {
        this.journal = journal;
        this.durablyReconciledOffsetsSupplier = durablyReconciledOffsetsSupplier;
    }

    /**
     * Returns the list of segments that do not need to be replayed, and that are not referenced by a tracked
     * SSTable, and that are durably reconciled over the full {@link #durablyReconciledOffsets durably reconciled offsets}.
     *
     * @param candidates the list of potential candidate segments
     * @return a subset of {@code candidates} that are safe to drop
     */
    @Override
    public Collection<StaticSegment<ShortMutationId, Mutation>> select(Collection<StaticSegment<ShortMutationId, Mutation>> candidates)
    {
        Log2OffsetsMap<?> durablyReconciled = durablyReconciledOffsets();
        List<StaticSegment<ShortMutationId, Mutation>> selected = new ArrayList<>();
        for (StaticSegment<ShortMutationId, Mutation> segment : candidates)
        {
            if (!segment.metadata().needsReplay()
                && !journal.segmentReferenceTracker().isReferenced(segment.id())
                && ((MutationJournal.StaticOffsetRanges) segment.keyStats()).isFullyCovered(durablyReconciled))
                selected.add(segment);
        }
        return selected;
    }

    /**
     * We drop every segment, since already know that the selected segments are safe to be discarded
     */
    @Override
    public Collection<StaticSegment<ShortMutationId, Mutation>> compact(Collection<StaticSegment<ShortMutationId, Mutation>> segments)
    {
        return Collections.emptyList();
    }

    @Override
    public void onCompacted()
    {
        try
        {
            maybePromoteReconciledSSTables();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            noSpamLogger.warn("Failed to promote reconciled sstables to repaired; will retry on a later pass", t);
        }
    }

    Log2OffsetsMap<?> durablyReconciledOffsets()
    {
        if (durablyReconciledOffsetsSupplier == null) // durablyReconciledOffsetsSupplier is only used for testing
        {
            Log2OffsetsMap.Mutable durablyReconciled = new Log2OffsetsMap.Mutable();
            if (MutationTrackingService.isEnabled())
                MutationTrackingService.instance().collectDurablyReconciledOffsets(durablyReconciled);
            return durablyReconciled;
        }
        return durablyReconciledOffsetsSupplier.get();
    }

    /**
     * Out-of-band promotion of already durably-reconciled but still-unrepaired sstables to repaired, attempted at
     * the end of every compaction pass while the on-disk mutation journal is over the
     * {@code mutation_tracking.journal_promotion_threshold}.
     *
     * <p>Best-effort: an sstable we cannot make a decision about is skipped, and a failure to flip
     * one table's sstables is logged and retried on a later pass.
     */
    @VisibleForTesting
    void maybePromoteReconciledSSTables()
    {
        long threshold = DatabaseDescriptor.getMutationTrackingConfig().getJournalPromotionThresholdBytes();
        if (threshold <= 0)
            return;

        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata == null)
            return;

        Set<SSTableReader> trackedSSTables = journal.segmentReferenceTracker().trackedSSTables();
        if (trackedSSTables.isEmpty() || journal.getDiskSpaceUsed() <= threshold)
            return;

        MutationTrackingMigrationState mutationTrackingMigrationState = metadata.mutationTrackingMigrationState;
        long repairedAt = Clock.Global.currentTimeMillis();
        Map<ColumnFamilyStore, List<SSTableReader>> toPromoteByTable = new HashMap<>();
        for (SSTableReader sstable : trackedSSTables)
        {
            ColumnFamilyStore cfs;
            try
            {
                if (!isReconciliationPromotable(mutationTrackingMigrationState, sstable))
                    continue;

                cfs = ColumnFamilyStore.getIfExists(sstable.metadata().id);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.debug("Skipping sstable ({}) for reconciliation promotion; could not determine eligibility",
                             sstable.getId(), t);
                continue;
            }

            if (cfs == null)
                continue;

            toPromoteByTable.computeIfAbsent(cfs, ignore -> new ArrayList<>()).add(sstable);
        }

        if (toPromoteByTable.isEmpty())
            return;

        for (Map.Entry<ColumnFamilyStore, List<SSTableReader>> entry : toPromoteByTable.entrySet())
        {
            ColumnFamilyStore cfs = entry.getKey();
            List<SSTableReader> toPromote = entry.getValue();
            try
            {
                cfs.getCompactionStrategyManager().mutateRepaired(toPromote, repairedAt, ActiveRepairService.NO_PENDING_REPAIR);
                logger.debug("Promoted {} reconciled sstables of {}.{} to repaired to release journal segments",
                             toPromote.size(), cfs.getKeyspaceName(), cfs.getTableName());
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                noSpamLogger.warn("Failed to promote reconciled sstables of {}.{} to repaired; will retry",
                                  cfs.getKeyspaceName(), cfs.getTableName(), t);
            }
        }
    }

    /**
     * Whether the {@code sstable} is eligible for reconciliation-based promotion to repaired.
     *
     * <p>We replicate the guard in {@link org.apache.cassandra.io.sstable.format.SSTableWriter#finalizeMetadata()}
     * to exclude sstables that have a pending migration.
     *
     * <p>During {@code MIGRATE_TO} writes are tracked for all tokens while reads are not. So we exclude
     * sstables already owned by a pending repair session.
     *
     * @param state   the mutation tracking migration state
     * @param sstable the sstable to test
     * @return true if the sstable should be considered for promotion to repaired
     */
    private boolean isReconciliationPromotable(MutationTrackingMigrationState state, SSTableReader sstable)
    {
        StatsMetadata stats = sstable.getSSTableMetadata();
        if (stats.repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE || stats.pendingRepair != ActiveRepairService.NO_PENDING_REPAIR)
            return false;

        ReplicationType replicationType = sstable.metadata().replicationType();
        if (replicationType == null || !replicationType.isTracked())
            return false;

        KeyspaceMigrationInfo migrationInfo = state.getKeyspaceInfo(sstable.metadata().keyspace);
        boolean inMigrationPendingRange = migrationInfo != null && migrationInfo.isRangeInPendingMigration(sstable.metadata().id,
                                                                                                           sstable.getFirst().getToken(),
                                                                                                           sstable.getLast().getToken());
        if (inMigrationPendingRange)
            return false;

        return MutationTrackingService.instance().isDurablyReconciled(stats.coordinatorLogOffsets);
    }
}
