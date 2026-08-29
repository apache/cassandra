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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.DiagnosticSnapshotService;
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

                // The participant set and the offset itself are only meaningful for the topology
                // they were computed against. Rather than dispatch against a possibly-stale
                // participant list, fail outright and let the operator retry -- matching the
                // "no partial results on topology change" stance for this feature.
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

                runValidation(promise, validationOffsets);
            },
            failure -> promise.tryFailure(new RuntimeException("unable to capture offset: " + failure.getMessage(), failure))
        );

        return promise;
    }

    boolean epochChanged(Epoch epochAtStart)
    {
        return !ClusterMetadata.current().epoch.equals(epochAtStart);
    }

    private void runValidation(AsyncPromise<CoordinatedRepairResult> promise, ValidationOffsets offset)
    {
        List<CommonRange> allRanges = neighborsAndRanges.filterCommonRanges(keyspace, tableNames).commonRanges;
        if (allRanges.isEmpty())
        {
            emitInSyncAndComplete(promise, offset);
            return;
        }

        // Union of all endpoints across common ranges, passed to the snapshot fan-out on mismatch.
        // Common range only holds the remote replicas only. The coordinator is implicit and must
        // be added explicitly so it also receives a SNAPSHOT_REQ for its own local snapshot.
        Set<InetAddressAndPort> allParticipants = new HashSet<>();
        allParticipants.add(broadcastAddressAndPort);
        for (CommonRange commonRange : allRanges)
            allParticipants.addAll(commonRange.endpoints);

        // Per-table tuple so per-table mismatching ranges are retained for metric emission.
        List<ValidationHandle> handles = new ArrayList<>();
        for (CommonRange commonRange : allRanges)
        {
            // Common range only holds remote replicas. Include the coordinator
            // so its own tree is built and enters the pairwise diff. MessagingService handles
            // the self-send locally.
            Set<InetAddressAndPort> participants = new HashSet<>(commonRange.endpoints);
            participants.add(broadcastAddressAndPort);
            for (String tableName : tableNames)
            {
                RepairJobDesc desc = new RepairJobDesc(parentSession, TimeUUID.Generator.nextTimeUUID(), keyspace, tableName, commonRange.ranges);
                MutationTrackingValidationCoordinator validationCoordinator =
                    new MutationTrackingValidationCoordinator(coordinator.ctx, desc, participants, offset);
                handles.add(new ValidationHandle(tableName, validationCoordinator, validationCoordinator.start()));
            }
        }

        AtomicInteger remaining = new AtomicInteger(handles.size());
        Map<String, Set<Range<Token>>> mismatchesByTable = new HashMap<>();

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
                    promise.tryFailure(new RuntimeException("MT validation failed: " + failure.getMessage(), failure));
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

        boolean anyMismatch = mismatchesByTable.values().stream().anyMatch(s -> !s.isEmpty());
        String message = anyMismatch ? INCONSISTENT_MESSAGE : IN_SYNC_MESSAGE;
        coordinator.notification(message);
        successMessage = name() + " completed successfully; " + message;

        if (anyMismatch)
        {
            emitMismatchMetrics(mismatchesByTable, offset);
            maybeSnapshotReplicas(mismatchesByTable, allParticipants);
        }

        promise.trySuccess(CoordinatedRepairResult.success(List.of()));
    }

    private void emitMismatchMetrics(Map<String, Set<Range<Token>>> mismatchesByTable, ValidationOffsets offset)
    {
        RepairMetrics.previewFailures.inc();

        for (Map.Entry<String, Set<Range<Token>>> entry : mismatchesByTable.entrySet())
        {
            if (entry.getValue().isEmpty())
                continue;
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, entry.getKey());
            if (cfs == null)
                continue;
            cfs.metric.tokenRangesPreviewedDesynchronized.mark(entry.getValue().size());
            cfs.metric.bytesPreviewedDesynchronized.mark(estimatedDesynchronizedBytes(cfs, entry.getValue(), offset));
        }
    }

    private long estimatedDesynchronizedBytes(ColumnFamilyStore cfs, Set<Range<Token>> mismatchingRanges, ValidationOffsets offset)
    {
        List<Range<Token>> normalizedRanges = Range.normalize(mismatchingRanges);
        long bytes = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (!sstable.isRepaired() && !offset.containsAll(sstable.getCoordinatorLogOffsets()))
                continue;
            for (SSTableReader.PartitionPositionBounds bounds : sstable.getPositionsForRanges(normalizedRanges))
                bytes += bounds.upperPosition - bounds.lowerPosition;
        }
        return bytes;
    }

    private void maybeSnapshotReplicas(Map<String, Set<Range<Token>>> mismatchesByTable, Set<InetAddressAndPort> participants)
    {
        if (!DatabaseDescriptor.snapshotOnRepairedDataMismatch())
            return;

        String snapshotName = DiagnosticSnapshotService.getSnapshotName(DiagnosticSnapshotService.REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX);
        for (Map.Entry<String, Set<Range<Token>>> entry : mismatchesByTable.entrySet())
        {
            if (entry.getValue().isEmpty())
                continue;
            String table = entry.getKey();
            try
            {
                if (SnapshotManager.instance.exists(keyspace, table, snapshotName))
                {
                    logger.info("Not snapshotting {}.{} - snapshot {} exists", keyspace, table, snapshotName);
                    continue;
                }

                List<Range<Token>> normalizedRanges = Range.normalize(entry.getValue());
                logger.info("Snapshotting {}.{} for REPAIRED preview mismatch for ranges {} with tag {} on instances {}",
                            keyspace, table, normalizedRanges, snapshotName, participants);
                DiagnosticSnapshotService.repairedDataMismatch(Keyspace.open(keyspace).getColumnFamilyStore(table).metadata(), participants, normalizedRanges);
            }
            catch (Exception e)
            {
                logger.error("Failed to trigger diagnostic snapshot for {}.{}", keyspace, table, e);
            }
        }
    }
}
