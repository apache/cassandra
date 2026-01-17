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
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.replication.MutationTrackingSyncCoordinator;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.replication.migration.KeyspaceMigrationInfo;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

/** Incremental repair task for keyspaces using mutation tracking */
public class MutationTrackingIncrementalRepairTask extends AbstractRepairTask
{
    private static final long SYNC_TIMEOUT_MINUTES = 30;

    private final TimeUUID parentSession;
    private final RepairCoordinator.NeighborsAndRanges neighborsAndRanges;
    private final String[] cfnames;

    protected MutationTrackingIncrementalRepairTask(RepairCoordinator coordinator,
                                                    TimeUUID parentSession,
                                                    RepairCoordinator.NeighborsAndRanges neighborsAndRanges,
                                                    String[] cfnames)
    {
        super(coordinator);
        this.parentSession = parentSession;
        this.neighborsAndRanges = neighborsAndRanges;
        this.cfnames = cfnames;
    }

    @Override
    public String name()
    {
        return "MutationTrackingIncrementalRepair";
    }

    @Override
    public Future<CoordinatedRepairResult> performUnsafe(ExecutorPlus executor, Scheduler validationScheduler)
    {
        List<CommonRange> allRanges = neighborsAndRanges.filterCommonRanges(keyspace, cfnames);

        if (allRanges.isEmpty())
        {
            logger.info("No common ranges to repair for keyspace {}", keyspace);
            return new AsyncPromise<CoordinatedRepairResult>().setSuccess(CoordinatedRepairResult.create(List.of(), List.of()));
        }

        List<MutationTrackingSyncCoordinator> syncCoordinators = new ArrayList<>();
        List<Collection<Range<Token>>> rangeCollections = new ArrayList<>();

        for (CommonRange commonRange : allRanges)
        {
            for (Range<Token> range : commonRange.ranges)
            {
                MutationTrackingSyncCoordinator syncCoordinator = new MutationTrackingSyncCoordinator(keyspace, range);
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
                waitForSyncCompletion(syncCoordinators, executor, validationScheduler, allRanges, rangeCollections, resultPromise);
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
                                       ExecutorPlus executor,
                                       Scheduler validationScheduler,
                                       List<CommonRange> allRanges,
                                       List<Collection<Range<Token>>> rangeCollections,
                                       AsyncPromise<CoordinatedRepairResult> resultPromise) throws InterruptedException
    {
        boolean allSucceeded = true;
        for (MutationTrackingSyncCoordinator syncCoordinator : syncCoordinators)
        {
            boolean completed = syncCoordinator.awaitCompletion(SYNC_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            if (!completed)
            {
                logger.warn("Mutation tracking sync timed out for keyspace {} range {}",
                            keyspace, syncCoordinator.getRange());
                syncCoordinator.cancel();
                allSucceeded = false;
            }
        }

        if (!allSucceeded)
        {
            resultPromise.tryFailure(new RuntimeException("Mutation tracking sync timed out for some ranges"));
            return;
        }

        coordinator.notifyProgress("Mutation tracking sync completed for all ranges");

        if (requiresTraditionalRepair(keyspace))
        {
            runTraditionalRepairForMigration(executor, validationScheduler, allRanges, resultPromise);
        }
        else
        {
            // Pure mutation tracking - create successful result
            resultPromise.trySuccess(CoordinatedRepairResult.create(rangeCollections, List.of()));
        }
    }

    private void runTraditionalRepairForMigration(ExecutorPlus executor,
                                                   Scheduler validationScheduler,
                                                   List<CommonRange> allRanges,
                                                   AsyncPromise<CoordinatedRepairResult> resultPromise)
    {
        coordinator.notifyProgress("Running traditional repair for migration");

        // Use the inherited runRepair method from AbstractRepairTask
        Future<CoordinatedRepairResult> traditionalRepair = runRepair(parentSession, true, executor,
                                                                      validationScheduler, allRanges,
                                                                      neighborsAndRanges.shouldExcludeDeadParticipants,
                                                                      cfnames);

        traditionalRepair.addListener(f -> {
            try
            {
                CoordinatedRepairResult result = (CoordinatedRepairResult) f.get();
                resultPromise.setSuccess(result);
            }
            catch (Exception e)
            {
                resultPromise.setFailure(e);
            }
        });
    }

    /**
     * Determines if this keyspace should use mutation tracking incremental repair.
     * Returns true if:
     * - Keyspace uses mutation tracking replication, OR
     * - Keyspace is currently migrating (either direction)
     */
    public static boolean shouldUseMutationTrackingRepair(String keyspace)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        KeyspaceMetadata ksm = metadata.schema.maybeGetKeyspaceMetadata(keyspace).orElse(null);
        if (ksm == null)
            return false;

        // Check if keyspace uses mutation tracking
        if (ksm.useMutationTracking())
            return true;

        // Check if keyspace is in migration (either direction)
        KeyspaceMigrationInfo migrationInfo = metadata.mutationTrackingMigrationState.getKeyspaceInfo(keyspace);
        return migrationInfo != null;
    }

    /**
     * Determines if we also need to run traditional repair.
     * Returns true during migration:
     * - Migrating TO mutation tracking: need traditional repair to sync pre-migration data
     * - Migrating FROM mutation tracking: need traditional repair for post-migration consistency
     */
    public static boolean requiresTraditionalRepair(String keyspace)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        KeyspaceMigrationInfo migrationInfo = metadata.mutationTrackingMigrationState.getKeyspaceInfo(keyspace);
        return migrationInfo != null;
    }
}
