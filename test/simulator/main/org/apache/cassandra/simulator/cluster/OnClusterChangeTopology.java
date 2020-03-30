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

import java.util.function.Consumer;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.cluster.ClusterActionListener.TopologyChangeValidator;
import org.apache.cassandra.simulator.systems.NonInterceptible;

import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE;
import static org.apache.cassandra.simulator.Action.Modifiers.STRICT;
import static org.apache.cassandra.simulator.ActionListener.runAfterTransitiveClosure;
import static org.apache.cassandra.simulator.systems.NonInterceptible.Permit.REQUIRED;

abstract class OnClusterChangeTopology extends Action implements Consumer<Action>
{
    final KeyspaceActions actions;

    final int[] participatingKeys;
    final Topology before;
    final Topology after;

    final TopologyChangeValidator validator;

    public OnClusterChangeTopology(Object description, KeyspaceActions actions, Topology before, Topology after, int[] participatingKeys)
    {
        this(description, actions, STRICT, RELIABLE, before, after, participatingKeys);
    }

    public OnClusterChangeTopology(Object description, KeyspaceActions actions, Modifiers self, Modifiers children, Topology before, Topology after, int[] participatingKeys)
    {
        super(description, self, children);
        this.actions = actions;
        this.participatingKeys = participatingKeys;
        this.before = before;
        this.after = after;
        this.validator = actions.listener.newTopologyChangeValidator(this);
        register(runAfterTransitiveClosure(this));
    }

    void before(IInvokableInstance instance)
    {
        NonInterceptible.execute(REQUIRED, () -> {
            actions.validateReplicasForKeys(instance, actions.keyspace, actions.table, before);
            validator.before(before, participatingKeys);
        });
    }

    public void accept(Action ignore)
    {
        validator.after(after);
    }

}
