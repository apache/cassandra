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
package org.apache.cassandra.repair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.RepairMetrics;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationTrackingSyncCoordinator;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.replication.ValidationOffsets;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.DiagnosticSnapshotService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * Preview repair for fully migrated tracked keyspaces. Chains a
 * {@link MutationTrackingIncrementalRepairTask} to capture the offset every replica has
 * reconciled up to, then dispatches a {@link MutationTrackingValidationCoordinator} per
 * (range, table) to build and compare merkle trees over each participant's data at or
 * before that offset. Fails outright, with no partial results, if the cluster topology
 * changes between capturing the offset and dispatching validation.
 */
public class MutationTrackingPreviewRepairTask extends AbstractRepairTask
{
    public static final String ESTABLISHING_OFFSETS = "Establishing validation offsets";
    public static final String OFFSETS_ESTABLISHED = "Validation offsets established";
    public static final String DISPATCHING_VALIDATION = "Dispatching validation";
    public static final String VALIDATION_COMPLETE = "Validation complete";
    public static final String IN_SYNC_MESSAGE = "Repaired data is in sync";
    public static final String INCONSISTENT_MESSAGE = "Repaired data is inconsistent";

    private final TimeUUID parentSession;
    private final String[] tableNames;
    private volatile String successMessage = name() + " completed successfully";

    protected MutationTrackingPreviewRepairTask(RepairCoordinator coordinator,
                                                TimeUUID parentSession,
                                                RepairCoordinator.NeighborsAndRanges neighborsAndRanges,
                                                String[] tableNames)
    {
        super(coordinator, neighborsAndRanges);
        this.parentSession = parentSession;
        this.tableNames = tableNames;
    }

    @Override
    public String name()
    {
        return "MutationTrackingRepairedPreview";
    }

    @Override
    public String successMessage()
    {
        return successMessage;
    }

    @Override
    public Future<CoordinatedRepairResult> performUnsafe(ExecutorPlus executor, Scheduler validationScheduler)
    {
        Epoch epochAtStart = ClusterMetadata.current().epoch;

        coordinator.notifyProgress(ESTABLISHING_OFFSETS);

        MutationTrackingIncrementalRepairTask irTask =
            new MutationTrackingIncrementalRepairTask(coordinator, parentSession, neighborsAndRanges, tableNames);

        AsyncPromise<CoordinatedRepairResult> promise = new AsyncPromise<>();

        irTask.perform(executor, validationScheduler).addCallback(
            irResult -> {
                if (irResult.hasFailed())
                {
                    promise.tryFailure(new RuntimeException("unable to capture offset: incremental repair reported failure"));
                    return;
                }

                // The participant set and the offset itself are only meaningful for the topology they were computed 
                // against. Rather than dispatch against a stale participant list, fail and let the operator retry.
                if (epochChanged(epochAtStart))
                {
                    promise.tryFailure(new RuntimeException("topology changed during REPAIRED preview; retry"));
                    return;
                }

                coordinator.notifyProgress(OFFSETS_ESTABLISHED);
                coordinator.notifyProgress(DISPATCHING_VALIDATION);

                // Extract captured targets from every sync coordinator IR ran, merge into one
                // flat ValidationOffsets, and pass to the validation dispatch.
                List<Map<CoordinatorLogId, Offsets.Immutable>> targetsList = new ArrayList<>();
                for (MutationTrackingSyncCoordinator syncCoordinator : irTask.getSyncCoordinators())
                    targetsList.add(syncCoordinator.getCapturedTargets());
                ValidationOffsets validationOffsets = ValidationOffsets.flatten(targetsList);

                runValidation(promise, validationOffsets, executor, validationScheduler);
            },
            failure -> promise.tryFailure(new RuntimeException("unable to capture offset: " + failure.getMessage(), failure))
        );

        return promise;
    }

    boolean epochChanged(Epoch epochAtStart)
    {
        return !ClusterMetadata.current().epoch.equals(epochAtStart);
    }

    private void runValidation(AsyncPromise<CoordinatedRepairResult> promise, ValidationOffsets offset, ExecutorPlus executor, Scheduler validationScheduler)
    {
        List<CommonRange> allRanges = neighborsAndRanges.filterCommonRanges(keyspace, tableNames).commonRanges;
        if (allRanges.isEmpty())
        {
            emitInSyncAndComplete(promise, offset);
            return;
        }

        // Union of all full replica endpoints across common ranges, passed to the snapshot fan-out on mismatch.
        // Common range holds remote replicas only. The coordinator is implicit and is added only if it is a full
        // replica for at least one common range.
        Set<InetAddressAndPort> allParticipants = new HashSet<>();

        // Per-table tuple so per-table mismatching ranges are retained for metric emission.
        List<ValidationHandle> handles = new ArrayList<>();
        try
        {
            for (CommonRange commonRange : allRanges)
            {
                // Common range only holds remote replicas. Exclude witness replicas  and include the coordinator
                // only if it is a full replica for this range.
                Set<InetAddressAndPort> participants = new HashSet<>(Sets.difference(commonRange.endpoints, commonRange.transEndpoints));
                if (StorageService.instance.isLocalFullReplica(keyspace, commonRange.ranges))
                    participants.add(broadcastAddressAndPort);

                if (!participants.isEmpty())
                    allParticipants.addAll(participants);

                for (String tableName : tableNames)
                {
                    RepairJobDesc desc = new RepairJobDesc(parentSession, TimeUUID.Generator.nextTimeUUID(), keyspace, tableName, commonRange.ranges);
                    MutationTrackingValidationCoordinator validationCoordinator = new MutationTrackingValidationCoordinator(coordinator.ctx, desc, participants, offset);
                    Future<MutationTrackingValidationCoordinator.Result> future = validationScheduler.schedule(validationCoordinator::start, executor);
                    handles.add(new ValidationHandle(tableName, validationCoordinator, future));
                }
            }
        }
        catch (Throwable t)
        {
            for (ValidationHandle handle : handles)
                handle.coordinator.cancel();
            promise.tryFailure(new RuntimeException("Failed to dispatch tracked validation: " + t.getMessage(), t));
            return;
        }

        AtomicInteger remaining = new AtomicInteger(handles.size());
        Map<String, Set<Range<Token>>> mismatchesByTable = new HashMap<>();

        Preconditions.checkState(!allParticipants.isEmpty());
        Preconditions.checkState(!handles.isEmpty());

        for (ValidationHandle handle : handles)
        {
            handle.future.addCallback(
                result -> {
                    synchronized (mismatchesByTable)
                    {
                        mismatchesByTable.computeIfAbsent(handle.tableName, k -> new HashSet<>()).addAll(result.mismatchingRanges);
                    }
                    if (remaining.decrementAndGet() == 0)
                        emitValidationComplete(promise, mismatchesByTable, allParticipants, offset);
                },
                failure -> {
                    // Any validation failure fails the whole preview. Cancel siblings so they
                    // don't keep running to completion or their own timeout, holding merkle
                    // trees and registry entries for a result nobody will consume.
                    promise.tryFailure(new RuntimeException("Tracked validation failed: " + failure.getMessage(), failure));
                    for (ValidationHandle sibling : handles)
                        sibling.coordinator.cancel();
                }
            );
        }
    }

    /** Per-table tuple so per-table mismatching ranges are retained for metric emission. */
    private static final class ValidationHandle
    {
        final String tableName;
        final MutationTrackingValidationCoordinator coordinator;
        final Future<MutationTrackingValidationCoordinator.Result> future;

        ValidationHandle(String tableName, MutationTrackingValidationCoordinator coordinator, Future<MutationTrackingValidationCoordinator.Result> future)
        {
            this.tableName = tableName;
            this.coordinator = coordinator;
            this.future = future;
        }
    }

    private static final class MismatchSummary
    {
        final String table;
        final int rawRangeCount;
        final List<Range<Token>> normalizedRanges;
        final long bytes;

        MismatchSummary(String table, int rawRangeCount, List<Range<Token>> normalizedRanges, long bytes)
        {
            this.table = table;
            this.rawRangeCount = rawRangeCount;
            this.normalizedRanges = normalizedRanges;
            this.bytes = bytes;
        }

        @Override
        public String toString()
        {
            return table + " - " + rawRangeCount + " ranges, " + FBUtilities.prettyPrintMemory(bytes) + " bytes";
        }

        static String format(List<MismatchSummary> mismatches)
        {
            StringBuilder output = new StringBuilder();
            for (MismatchSummary mismatch : mismatches)
                output.append(mismatch).append('\n');
            return output.toString();
        }
    }

    private void emitInSyncAndComplete(AsyncPromise<CoordinatedRepairResult> promise, ValidationOffsets offset)
    {
        emitValidationComplete(promise, Collections.emptyMap(), Collections.emptySet(), offset);
    }

    private void emitValidationComplete(AsyncPromise<CoordinatedRepairResult> promise,
                                    Map<String, Set<Range<Token>>> mismatchesByTable,
                                    Set<InetAddressAndPort> allParticipants,
                                    ValidationOffsets offset)
    {
        coordinator.notifyProgress(VALIDATION_COMPLETE);

        List<MismatchSummary> mismatches = summarizeMismatches(mismatchesByTable, offset);
        String message = mismatches.isEmpty() ? IN_SYNC_MESSAGE : INCONSISTENT_MESSAGE + '\n' + MismatchSummary.format(mismatches);
        coordinator.notification(message);
        successMessage = name() + " completed successfully; " + message;

        if (!mismatches.isEmpty())
        {
            emitMismatchMetrics(mismatches);
            maybeSnapshotReplicas(mismatches, allParticipants);
        }

        promise.trySuccess(CoordinatedRepairResult.success(List.of()));
    }

    private List<MismatchSummary> summarizeMismatches(Map<String, Set<Range<Token>>> mismatchesByTable, ValidationOffsets offset)
    {
        List<MismatchSummary> mismatches = new ArrayList<>();
        for (Map.Entry<String, Set<Range<Token>>> entry : mismatchesByTable.entrySet())
        {
            Set<Range<Token>> rawRanges = entry.getValue();
            if (rawRanges.isEmpty())
                continue;
            List<Range<Token>> normalizedRanges = Range.normalize(rawRanges);
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, entry.getKey());
            long bytes = cfs == null ? 0 : estimatedDesynchronizedBytes(cfs, normalizedRanges, offset);
            mismatches.add(new MismatchSummary(entry.getKey(), rawRanges.size(), normalizedRanges, bytes));
        }

        // a multi-table mismatch notification should read the same way every time
        mismatches.sort(Comparator.comparing(m -> m.table));
        return mismatches;
    }

    private void emitMismatchMetrics(List<MismatchSummary> mismatches)
    {
        RepairMetrics.previewFailures.inc();

        for (MismatchSummary mismatch : mismatches)
        {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, mismatch.table);
            if (cfs == null)
                continue;
            cfs.metric.tokenRangesPreviewedDesynchronized.mark(mismatch.rawRangeCount);
            cfs.metric.bytesPreviewedDesynchronized.mark(mismatch.bytes);
        }
    }

    private long estimatedDesynchronizedBytes(ColumnFamilyStore cfs, List<Range<Token>> normalizedRanges, ValidationOffsets offset)
    {
        long bytes = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (!sstable.isRepaired() && !offset.covers(sstable.getCoordinatorLogOffsets(), normalizedRanges))
                continue;
            for (SSTableReader.PartitionPositionBounds bounds : sstable.getPositionsForRanges(normalizedRanges))
                bytes += bounds.upperPosition - bounds.lowerPosition;
        }
        return bytes;
    }

    private void maybeSnapshotReplicas(List<MismatchSummary> mismatches, Set<InetAddressAndPort> participants)
    {
        if (!DatabaseDescriptor.snapshotOnRepairedDataMismatch())
            return;

        String snapshotName = DiagnosticSnapshotService.getSnapshotName(DiagnosticSnapshotService.REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX);
        for (MismatchSummary mismatch : mismatches)
        {
            String table = mismatch.table;
            try
            {
                if (SnapshotManager.instance.exists(keyspace, table, snapshotName))
                {
                    logger.info("Not snapshotting {}.{} - snapshot {} exists", keyspace, table, snapshotName);
                    continue;
                }

                logger.info("Snapshotting {}.{} for REPAIRED preview mismatch for ranges {} with tag {} on instances {}",
                            keyspace, table, mismatch.normalizedRanges, snapshotName, participants);
                DiagnosticSnapshotService.repairedDataMismatch(Keyspace.open(keyspace).getColumnFamilyStore(table).metadata(), participants, mismatch.normalizedRanges);
            }
            catch (Exception e)
            {
                logger.error("Failed to trigger diagnostic snapshot for {}.{}", keyspace, table, e);
            }
        }
    }
}
