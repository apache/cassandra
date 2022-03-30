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

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.simulator.ActionList;

import static org.apache.cassandra.utils.LazyToString.lazy;

class OnClusterJoin extends OnClusterChangeTopology
{
    final int joining;

    OnClusterJoin(KeyspaceActions actions, Topology before, Topology during, Topology after, int joining)
    {
        super(lazy(() -> String.format("node%d Joining", joining)), actions, before, after, during.pendingKeys());
        this.joining = joining;
    }

    public ActionList performSimple()
    {
        IInvokableInstance joinInstance = actions.cluster.get(joining);
        before(joinInstance);
        return ActionList.of(
            // setup the node's own gossip state for pending ownership, and return gossip actions to disseminate
            new OnClusterUpdateGossip(actions, joining, new OnInstanceSetBootstrapping(actions, joining)),
            new OnInstanceSyncSchemaForBootstrap(actions, joining),
            new OnInstanceTopologyChangePaxosRepair(actions, joining, "Join"),
            // stream/repair from a peer
            new OnInstanceBootstrap(actions, joinInstance),
            // setup the node's own gossip state for natural ownership, and return gossip actions to disseminate
            new OnClusterUpdateGossip(actions, joining, new OnInstanceSetNormal(actions, joining))
        );
    }
}
