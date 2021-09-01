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
import org.apache.cassandra.simulator.Actions.SimpleAction;
import org.apache.cassandra.simulator.cluster.ClusterActionListener.RepairValidator;

import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE_NO_TIMEOUTS;
import static org.apache.cassandra.simulator.Action.Modifiers.STRICT;
import static org.apache.cassandra.simulator.ActionListener.runAfterTransitiveClosure;
import static org.apache.cassandra.utils.LazyToString.lazy;

class OnClusterFullRepair extends SimpleAction implements Consumer<Action>
{
    final KeyspaceActions actions;
    final int[] membersOfRing;
    final int[] membersOfQuorumDcs;
    final int quorumRf;
    final boolean force;
    final RepairValidator validator;

    public OnClusterFullRepair(KeyspaceActions actions, int[] membersOfRing, int[] membersOfQuorumDcs, int quorumRf, boolean force)
    {
        super(lazy(() -> "Full Repair on " + Arrays.toString(membersOfRing)), STRICT, RELIABLE_NO_TIMEOUTS);
        this.actions = actions;
        // STRICT to ensure repairs do not run simultaneously, as seems not to be permitted even for non-overlapping ranges?
        this.membersOfRing = membersOfRing;
        this.membersOfQuorumDcs = membersOfQuorumDcs;
        this.quorumRf = quorumRf;
        this.force = force;
        this.validator = actions.listener.newRepairValidator(this);
        register(runAfterTransitiveClosure(this));
    }

    protected ActionList performInternal()
    {
        int[] primaryKeys = actions.primaryKeys;
        validator.before(primaryKeys, actions.replicasForKeys(actions.keyspace, actions.table, primaryKeys, membersOfQuorumDcs), quorumRf);
        return actions.on((i) -> new OnInstanceRepair(actions, i, force), membersOfRing);
    }

    public void accept(Action ignore)
    {
        validator.after();
    }
}
