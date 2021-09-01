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

package org.apache.cassandra.simulator.cluster;

import java.util.Arrays;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.Actions;

import static java.util.Arrays.stream;
import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.Modifiers.STRICT;
import static org.apache.cassandra.utils.LazyToString.lazy;

class OnClusterChangeRf extends OnClusterChangeTopology
{
    final KeyspaceActions actions;
    final long timestamp;
    final int on;
    final boolean increase;
    final int[] newRf;

    OnClusterChangeRf(KeyspaceActions actions, long timestamp, int on, int[] membersOfRing, int[] membersOfQuorumDcs, boolean increase, int[] newRf, int quorumRfBefore, int quorumRfAfter)
    {
        super(increase ? lazy(() -> "Increase replication factor to " + Arrays.toString(newRf)) : lazy(() -> "Decrease replication factor to " + Arrays.toString(newRf)),
              actions, STRICT, NONE, membersOfRing, membersOfQuorumDcs, quorumRfBefore, quorumRfAfter);
        this.actions = actions;
        this.timestamp = timestamp;
        this.on = on;
        this.increase = increase;
        this.newRf = newRf;
    }

    protected ActionList performInternal()
    {
        before(actions.cluster.get(on));

        StringBuilder command = new StringBuilder("ALTER KEYSPACE " + actions.keyspace + " WITH replication = {'class': 'NetworkTopologyStrategy'");
        for (int i = 0; i < newRf.length; ++i)
            command.append(", '").append(actions.snitch.nameOfDc(i)).append("': ").append(newRf[i]);
        command.append("};");

        return ActionList.of(
        new OnClusterUpdateGossip(actions, on, new ClusterReliableQueryAction("ALTER KEYSPACE " + description(), actions, on, command.toString(), timestamp, ConsistencyLevel.ALL)),
        new OnClusterFullRepair(actions, membersOfRing, membersOfQuorumDcs, stream(newRf).sum(), false),
        // TODO (future): cleanup should clear paxos state tables
        Actions.of("Flush and Cleanup", !increase ? () -> actions.flushAndCleanup(membersOfRing) : ActionList::empty)
        );
    }
}
