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

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.consistent.CoordinatorSession;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;

public class IncrementalRepairTask extends AbstractRepairTask
{
    private final TimeUUID parentSession;
    private final RepairCoordinator.NeighborsAndRanges neighborsAndRanges;
    private final String[] cfnames;

    protected IncrementalRepairTask(RepairCoordinator coordinator,
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
        return "Repair";
    }

    @Override
    public Future<CoordinatedRepairResult> performUnsafe(ExecutorPlus executor) throws Exception
    {
        // the local node also needs to be included in the set of participants, since coordinator sessions aren't persisted
        Set<InetAddressAndPort> allParticipants = ImmutableSet.<InetAddressAndPort>builder()
                                                  .addAll(neighborsAndRanges.participants)
                                                  .add(broadcastAddressAndPort)
                                                  .build();
        // Not necessary to include self for filtering. The common ranges only contains neighbhor node endpoints.
        List<CommonRange> allRanges = neighborsAndRanges.filterCommonRanges(keyspace, cfnames);

        CoordinatorSession coordinatorSession = coordinator.ctx.repair().consistent.coordinated.registerSession(parentSession, allParticipants, neighborsAndRanges.shouldExcludeDeadParticipants);

        return coordinatorSession.execute(() -> runRepair(parentSession, true, executor, allRanges, cfnames));

    }
}
