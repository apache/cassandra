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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.replication.MutationTrackingSyncCoordinator;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import static com.google.common.base.Preconditions.checkState;

/** Repair task that syncs mutation tracking offsets across replicas */
public class MutationTrackingIncrementalRepairTask extends AbstractRepairTask
{

    private final TimeUUID parentSession;
    private final String[] cfnames;
    private final ClusterMetadata metadata;

    protected MutationTrackingIncrementalRepairTask(RepairCoordinator coordinator,
                                                    TimeUUID parentSession,
                                                    RepairCoordinator.NeighborsAndRanges neighborsAndRanges,
                                                    String[] cfnames)
    {
        super(coordinator, neighborsAndRanges);
        this.parentSession = parentSession;
        this.cfnames = cfnames;
        this.metadata = coordinator.metadata;
    }

    @Override
    public String name()
    {
        return "MutationTrackingRepair";
    }

    @Override
    public Future<CoordinatedRepairResult> performUnsafe(ExecutorPlus executor, Scheduler validationScheduler)
    {
        List<CommonRange> allRanges = neighborsAndRanges.filterCommonRanges(keyspace, cfnames).commonRanges;
        checkState(!allRanges.isEmpty(), "No ranges to repair");

        List<MutationTrackingSyncCoordinator> syncCoordinators = new ArrayList<>();
        List<Collection<Range<Token>>> rangeCollections = new ArrayList<>();

        for (CommonRange commonRange : allRanges)
        {
            for (Range<Token> range : commonRange.ranges)
            {
                RepairJobDesc desc = new RepairJobDesc(parentSession, TimeUUID.Generator.nextTimeUUID(),
                                                       keyspace, "Mutation Tracking Sync", List.of(range));
                MutationTrackingSyncCoordinator syncCoordinator =
                    new MutationTrackingSyncCoordinator(coordinator.ctx, desc, commonRange.endpoints, metadata);
                syncCoordinator.start();
                syncCoordinators.add(syncCoordinator);
                rangeCollections.add(List.of(range));

                logger.info("Started mutation tracking sync for range {}", range);
            }
        }

        coordinator.notifyProgress("Started mutation tracking sync for " + syncCoordinators.size() + " ranges");

        AsyncPromise<CoordinatedRepairResult> resultPromise = new AsyncPromise<>();

        executor.execute(() -> {
            try
            {
                waitForSyncCompletion(syncCoordinators, rangeCollections, resultPromise);
            }
            catch (InterruptedException e)
            {
                try
                {
                    resultPromise.tryFailure(new RuntimeException("Interrupted waiting for Mutation Tracking sync coordinators to finish", e));
                }
                finally
                {
                    Thread.currentThread().interrupt();
                }
            }
            catch (Exception e)
            {
                logger.error("Error during mutation tracking repair", e);
                resultPromise.tryFailure(e);
            }
        });

        return resultPromise;
    }

    private void waitForSyncCompletion(List<MutationTrackingSyncCoordinator> syncCoordinators,
                                       List<Collection<Range<Token>>> rangeCollections,
                                       AsyncPromise<CoordinatedRepairResult> resultPromise) throws Exception
    {
        long deadlineNanos = coordinator.ctx.clock().nanoTime() + TimeUnit.MILLISECONDS.toNanos(
            DatabaseDescriptor.getMutationTrackingSyncTimeout(TimeUnit.MILLISECONDS));
        coordinator.ctx.scheduledTasks().schedule(() -> {
            for (MutationTrackingSyncCoordinator syncCoordinator : syncCoordinators)
            {
                try
                {
                    syncCoordinator.timeout();
                }
                catch (Exception e)
                {
                    logger.error("Exception cancelling mutation tracking sync coordinator", e);
                }
            }
        }, deadlineNanos - coordinator.ctx.clock().nanoTime(), TimeUnit.NANOSECONDS);

        Exception error = null;
        for (MutationTrackingSyncCoordinator syncCoordinator : syncCoordinators)
        {
            try
            {
                syncCoordinator.awaitCompletion();
            }
            catch (InterruptedException e)
            {
                if (error != null)
                    e.addSuppressed(error);
                for (MutationTrackingSyncCoordinator c : syncCoordinators)
                {
                    try
                    {
                        c.cancel();
                    }
                    catch (Exception e2)
                    {
                        e.addSuppressed(e2);
                    }
                }
                throw e;
            }
            catch (Exception e)
            {
                error = Throwables.merge(error, e);
            }
        }

        if (error != null)
        {
            logger.warn("Mutation tracking sync failed for keyspace {}", keyspace, error);
            throw error;
        }

        coordinator.notifyProgress("Mutation tracking sync completed for all ranges");

        List<RepairSessionResult> results = new ArrayList<>();
        for (int i = 0; i < rangeCollections.size(); i++)
        {
            Collection<Range<Token>> ranges = rangeCollections.get(i);
            results.add(new RepairSessionResult(parentSession, keyspace, ranges, List.of(), false));
        }
        resultPromise.trySuccess(CoordinatedRepairResult.create(rangeCollections, results));
    }

    /**
     * Determines if this keyspace should use mutation tracking incremental repair.
     * Returns true if:
     * - Keyspace uses mutation tracking replication, OR
     * - Keyspace is currently migrating (either direction)
     *
     * @param metadata the snapshotted cluster metadata to evaluate against
     * @param keyspace the keyspace name to check
     */
    public static boolean shouldUseMutationTrackingRepair(ClusterMetadata metadata, String keyspace)
    {
        KeyspaceMetadata ksm = metadata.schema.maybeGetKeyspaceMetadata(keyspace).orElse(null);
        if (ksm == null)
            return false;

        // Check if keyspace uses mutation tracking
        if (ksm.useMutationTracking())
            return true;

        // For tracked→untracked migration (keyspace is currently untracked but migration is in progress),
        // use regular incremental repair instead of MT repair. The MT sync step can't complete for this
        // direction because streaming doesn't update mutation tracking offsets, and the keyspace is moving
        // away from tracking. Regular incremental repair will sync the data and the RepairJob callback
        // handler will still advance the migration state.
        // TODO (desired): This is an over simplification in that depending on which ranges are migrated we might be able to just run MT sync but running IR should also be fine
        return false;
    }

    /**
     * Determines if a mutation tracking migration is in progress for this keyspace.
     * Returns true during migration:
     * - Migrating TO mutation tracking: need traditional repair to sync pre-migration data
     * - Migrating FROM mutation tracking: need traditional repair for post-migration consistency
     *
     * @param metadata the snapshotted cluster metadata to evaluate against
     * @param keyspace the keyspace name to check
     */
    public static boolean isMutationTrackingMigrationInProgress(ClusterMetadata metadata, String keyspace)
    {
        return metadata.mutationTrackingMigrationState.isMigrating(keyspace);
    }
}
