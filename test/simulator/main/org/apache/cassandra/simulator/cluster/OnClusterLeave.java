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

class OnClusterLeave extends OnClusterChangeTopology
{
    final int leaving;

    OnClusterLeave(KeyspaceActions actions, int[] membersOfRing, int[] membersOfQuorum, int leaving, int quorumRf)
    {
        super(lazy(() -> String.format("node%d Leaving", leaving)), actions, membersOfRing, membersOfQuorum, quorumRf, quorumRf);
        this.leaving = leaving;
    }

    public ActionList performInternal()
    {
        IInvokableInstance leaveInstance = actions.cluster.get(leaving);
        before(leaveInstance);
        return ActionList.of(
            // setup the node's own gossip state for pending ownership, and return gossip actions to disseminate
            new OnClusterUpdateGossip(actions, leaving, new OnInstanceSetLeaving(actions, leaving)),
            new OnInstanceUnbootstrap(actions, leaving, leaveInstance),
            // setup the node's own gossip state for natural ownership, and return gossip actions to disseminate
            new OnClusterUpdateGossip(actions, leaving, new OnInstanceSetLeft(actions, leaving))
        );
    }
}
