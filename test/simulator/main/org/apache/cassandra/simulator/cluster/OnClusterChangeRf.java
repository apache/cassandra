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

import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.Actions;

import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.Modifiers.STRICT;
import static org.apache.cassandra.simulator.cluster.ClusterReliableQueryAction.schemaChange;
import static org.apache.cassandra.utils.LazyToString.lazy;

class OnClusterChangeRf extends OnClusterChangeTopology
{
    final KeyspaceActions actions;
    final long timestamp;
    final int on;
    final boolean increase;

    OnClusterChangeRf(KeyspaceActions actions, long timestamp, int on, Topology before, Topology after, boolean increase)
    {
        super(increase ? lazy(() -> "Increase replication factor to " + Arrays.toString(after.rf))
                       : lazy(() -> "Decrease replication factor to " + Arrays.toString(after.rf)),
              actions, STRICT, NONE, before, after, before.primaryKeys);
        this.actions = actions;
        this.timestamp = timestamp;
        this.on = on;
        this.increase = increase;
    }

    protected ActionList performSimple()
    {
        before(actions.cluster.get(on));

        StringBuilder command = new StringBuilder("ALTER KEYSPACE " + actions.keyspace + " WITH replication = {'class': 'NetworkTopologyStrategy'");
        for (int i = 0; i < after.rf.length; ++i)
            command.append(", '").append(actions.snitch.nameOfDc(i)).append("': ").append(after.rf[i]);
        command.append("};");

        return ActionList.of(
            schemaChange("ALTER KEYSPACE " + description(), actions, on, command.toString()),
            new OnClusterSyncPendingRanges(actions),
            new OnClusterFullRepair(actions, after, true, false, false),
            // TODO: cleanup should clear paxos state tables
            Actions.of("Flush and Cleanup", !increase ? () -> actions.flushAndCleanup(after.membersOfRing) : ActionList::empty)
        );
    }
}
