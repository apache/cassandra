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
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.Actions.ReliableAction;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.systems.SimulatedAction;

import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE_NO_TIMEOUTS;
import static org.apache.cassandra.simulator.Action.Modifiers.STRICT;

public class OnClusterUpdateGossip extends ReliableAction
{
    OnClusterUpdateGossip(ClusterActions actions, int on, SimulatedAction updateLocalState)
    {
        this(updateLocalState.description() + " and Sync Gossip", actions, ActionList.of(updateLocalState),
             new OnInstanceGossipWithAll(actions, on));
    }

    OnClusterUpdateGossip(ClusterActions actions, ActionList updateLocalState, Action sendGossip)
    {
        this(updateLocalState.get(0).description() + " and Sync Gossip", actions, updateLocalState, sendGossip);
    }

    OnClusterUpdateGossip(Object id, ClusterActions actions, ActionList updateLocalState, Action sendGossip)
    {
        super(id, STRICT, RELIABLE_NO_TIMEOUTS, () -> updateLocalState.andThen(sendGossip).andThen(new OnClusterSyncPendingRanges(actions)));
    }
}
