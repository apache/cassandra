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
import java.util.function.BiFunction;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.simulator.Action;

import static org.apache.cassandra.locator.InetAddressAndPort.getByAddress;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE_NO_TIMEOUTS;
import static org.apache.cassandra.simulator.Action.Modifiers.STRICT;

class OnInstanceSendLocalGossipState extends ClusterAction
{
    OnInstanceSendLocalGossipState(ClusterActions actions, int from, int to)
    {
        super("Send Local Gossip State from " + from + " to " + to, STRICT, RELIABLE_NO_TIMEOUTS, actions, from,
              invokableSendLocalGossipState(actions.cluster, to));
    }

    public static BiFunction<Integer, Integer, Action> factory(ClusterActions actions)
    {
        return (from, to) -> new OnInstanceSendLocalGossipState(actions, from, to);
    }

    static IIsolatedExecutor.SerializableRunnable invokableSendLocalGossipState(Cluster cluster, int to)
    {
        InetSocketAddress address = cluster.get(to).broadcastAddress();
        return () -> Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.unsafeSendLocalEndpointStateTo(getByAddress(address)));
    }
}
