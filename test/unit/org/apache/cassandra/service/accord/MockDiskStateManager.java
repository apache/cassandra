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

package org.apache.cassandra.service.accord;

import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.Invariants;

import java.util.Set;

public enum MockDiskStateManager implements AccordConfigurationService.DiskStateManager {
    instance;

    @Override
    public AccordKeyspace.EpochDiskState loadTopologies(AccordKeyspace.TopologyLoadConsumer consumer) {
        return AccordKeyspace.EpochDiskState.EMPTY;
    }

    @Override
    public AccordKeyspace.EpochDiskState setNotifyingLocalSync(long epoch, Set<Node.Id> pending, AccordKeyspace.EpochDiskState diskState) {
        return maybeUpdateMaxEpoch(diskState, epoch);
    }

    @Override
    public AccordKeyspace.EpochDiskState setCompletedLocalSync(long epoch, AccordKeyspace.EpochDiskState diskState) {
        return maybeUpdateMaxEpoch(diskState, epoch);
    }

    @Override
    public AccordKeyspace.EpochDiskState markLocalSyncAck(Node.Id id, long epoch, AccordKeyspace.EpochDiskState diskState) {
        return maybeUpdateMaxEpoch(diskState, epoch);
    }

    @Override
    public AccordKeyspace.EpochDiskState saveTopology(Topology topology, AccordKeyspace.EpochDiskState diskState) {
        return maybeUpdateMaxEpoch(diskState, topology.epoch());
    }

    @Override
    public AccordKeyspace.EpochDiskState markRemoteTopologySync(Node.Id node, long epoch, AccordKeyspace.EpochDiskState diskState) {
        return maybeUpdateMaxEpoch(diskState, epoch);
    }

    @Override
    public AccordKeyspace.EpochDiskState markClosed(Ranges ranges, long epoch, AccordKeyspace.EpochDiskState diskState) {
        return maybeUpdateMaxEpoch(diskState, epoch);
    }

    @Override
    public AccordKeyspace.EpochDiskState truncateTopologyUntil(long epoch, AccordKeyspace.EpochDiskState diskState) {
        return maybeUpdateMaxEpoch(diskState, epoch);
    }

    private static AccordKeyspace.EpochDiskState maybeUpdateMaxEpoch(AccordKeyspace.EpochDiskState diskState, long epoch) {
        if (diskState.isEmpty())
            return AccordKeyspace.EpochDiskState.create(epoch);
        Invariants.checkArgument(epoch >= diskState.minEpoch, "Epoch %d < %d (min)", epoch, diskState.minEpoch);
        if (epoch > diskState.maxEpoch)
            diskState = diskState.withNewMaxEpoch(epoch);
        return diskState;
    }
}
