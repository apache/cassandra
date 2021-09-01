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
import java.util.function.Consumer;

import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.cluster.ClusterActionListener.RepairValidator;

import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE_NO_TIMEOUTS;
import static org.apache.cassandra.simulator.Action.Modifiers.STRICT;
import static org.apache.cassandra.simulator.ActionListener.runAfterTransitiveClosure;
import static org.apache.cassandra.utils.LazyToString.lazy;

class OnClusterFullRepair extends Action implements Consumer<Action>
{
    final KeyspaceActions actions;
    final Topology topology;
    final boolean force;
    final RepairValidator validator;
    final boolean repairPaxos;
    final boolean repairOnlyPaxos;

    public OnClusterFullRepair(KeyspaceActions actions, Topology topology, boolean repairPaxos, boolean repairOnlyPaxos, boolean force)
    {
        super(lazy(() -> "Full Repair on " + Arrays.toString(topology.membersOfRing)), STRICT, RELIABLE_NO_TIMEOUTS);
        this.actions = actions;
        // STRICT to ensure repairs do not run simultaneously, as seems not to be permitted even for non-overlapping ranges?
        this.topology = topology;
        this.repairPaxos = repairPaxos;
        this.repairOnlyPaxos = repairOnlyPaxos;
        this.force = force;
        this.validator = actions.listener.newRepairValidator(this);
        register(runAfterTransitiveClosure(this));
    }

    protected ActionList performSimple()
    {
        actions.validateReplicasForKeys(actions.cluster.get(topology.membersOfQuorum[0]), actions.keyspace, actions.table, topology);
        validator.before(topology, repairPaxos, repairOnlyPaxos);
        return actions.on(i -> new OnInstanceRepair(actions, i, repairPaxos, repairOnlyPaxos, force), topology.membersOfRing);
    }

    public void accept(Action ignore)
    {
        validator.after();
    }
}
