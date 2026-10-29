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

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.replication.MutationTrackingSyncCoordinator;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;

public class NormalRepairTask extends AbstractRepairTask
{
    private final TimeUUID parentSession;
    private final String[] cfnames;

    protected NormalRepairTask(RepairCoordinator coordinator,
                               TimeUUID parentSession,
                               RepairCoordinator.NeighborsAndRanges neighborsAndRanges,
                               String[] cfnames)
    {
        super(coordinator, neighborsAndRanges);
        this.parentSession = parentSession;
        this.cfnames = cfnames;
    }

    @Override
    public String name()
    {
        return "Repair";
    }

    @Override
    public Future<CoordinatedRepairResult> performUnsafe(ExecutorPlus executor, Scheduler validationScheduler)
    {
        ClusterMetadata metadata = coordinator.metadata != null ? coordinator.metadata : ClusterMetadata.current();
        boolean useMT = MutationTrackingIncrementalRepairTask.shouldUseMutationTrackingRepair(metadata, keyspace);
        if (useMT)
        {
            coordinator.notifyProgress("Started mutation tracking sync before full repair");
            return MutationTrackingSyncCoordinator.sync(coordinator.ctx, parentSession, keyspace, neighborsAndRanges.commonRanges, metadata, executor)
                                                  .flatMap(ignored -> {
                                                      coordinator.notifyProgress("Mutation tracking sync completed; running full repair");
                                                      return runRepair(parentSession, false, executor, validationScheduler, neighborsAndRanges.commonRanges, neighborsAndRanges.shouldExcludeDeadParticipants, cfnames);
                                                  }, executor);
        }
        return runRepair(parentSession, false, executor, validationScheduler, neighborsAndRanges.commonRanges, neighborsAndRanges.shouldExcludeDeadParticipants, cfnames);
    }
}
