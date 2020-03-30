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

import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.Actions.ReliableAction;

import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.Modifiers.STRICT;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;
import static org.apache.cassandra.utils.LazyToString.lazy;

class OnClusterReplace extends OnClusterChangeTopology
{
    final int leaving;
    final int joining;

    OnClusterReplace(KeyspaceActions actions, Topology before, Topology during, Topology after, int leaving, int joining)
    {
        super(lazy(() -> String.format("node%d Replacing node%d", joining, leaving)), actions, STRICT, NONE, before, after, during.pendingKeys());
        this.leaving = leaving;
        this.joining = joining;
    }

    public ActionList performSimple()
    {
        // need to mark it as DOWN, and perhaps shut it down
        Map<InetSocketAddress, IInvokableInstance> lookup = Cluster.getUniqueAddressLookup(actions.cluster);
        IInvokableInstance leaveInstance = actions.cluster.get(leaving);
        IInvokableInstance joinInstance = actions.cluster.get(joining);
        before(leaveInstance);
        UUID hostId = leaveInstance.unsafeCallOnThisThread(SystemKeyspace::getLocalHostId);
        String movingToken = leaveInstance.unsafeCallOnThisThread(() -> Utils.currentToken().toString());
        List<Map.Entry<String, String>> repairRanges = leaveInstance.unsafeApplyOnThisThread(
            (String keyspaceName) -> StorageService.instance.getLocalAndPendingRanges(keyspaceName).stream()
                                                            .map(OnClusterReplace::toStringEntry)
                                                            .collect(Collectors.toList()),
            actions.keyspace
        );

        int[] others = repairRanges.stream().mapToInt(
            repairRange -> lookup.get(leaveInstance.unsafeApplyOnThisThread(
                (String keyspaceName, String tk) -> Keyspace.open(keyspaceName).getReplicationStrategy().getNaturalReplicasForToken(Utils.parseToken(tk)).stream().map(Replica::endpoint)
                                                            .filter(i -> !i.equals(getBroadcastAddressAndPort()))
                                                            .findFirst()
                                                            .orElseThrow(IllegalStateException::new),
                actions.keyspace, repairRange.getValue())
            ).config().num()
        ).toArray();

        return ActionList.of(
        // first sync gossip so that newly joined nodes are known by all, so that when we markdown we do not throw UnavailableException
        ReliableAction.transitively("Sync Gossip", () -> actions.gossipWithAll(leaving)),

        // "shutdown" the leaving instance
        new OnClusterUpdateGossip(actions,
                                      ActionList.of(new OnInstanceMarkShutdown(actions, leaving),
                                                    new OnClusterMarkDown(actions, leaving)),
                                      new OnInstanceSendShutdownToAll(actions, leaving)),

        // TODO (safety): confirm repair does not include this node

        // note that in the case of node replacement, we must perform a paxos repair before AND mid-transition.
        // the first ensures the paxos state is flushed to the base table's sstables, so that the replacing node
        // must receive a copy of all earlier operations (since the old node is now "offline")

        new OnClusterRepairRanges(actions, others, true, false, repairRanges),

        // stream/repair from a peer
        new OnClusterUpdateGossip(actions, joining, new OnInstanceSetBootstrapReplacing(actions, joining, leaving, hostId, movingToken)),

        new OnInstanceSyncSchemaForBootstrap(actions, joining),
        new OnInstanceTopologyChangePaxosRepair(actions, joining, "Replace"),
        new OnInstanceBootstrap(actions, joinInstance, movingToken, true),

        // setup the node's own gossip state for natural ownership, and return gossip actions to disseminate
        new OnClusterUpdateGossip(actions, joining, new OnInstanceSetNormal(actions, joining, hostId, movingToken))
        );
    }

    static Map.Entry<String, String> toStringEntry(Range<Token> range)
    {
        return new AbstractMap.SimpleImmutableEntry<>(range.left.toString(), range.right.toString());
    }
}
