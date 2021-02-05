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

package org.apache.cassandra.distributed.test.hostreplacement;

import java.net.InetSocketAddress;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.distributed.shared.ClusterUtils.stopAbrupt;

/**
 * If the operator attempts to assassinate the node before replacing it, this will cause the node to fail to start
 * as the status is non-normal.
 *
 * The node is removed abruptly before assassinate, leaving gossip without an empty entry.
 */
public class AssassinateAbruptDownedNodeTest extends BaseAssassinatedCase
{
    @Override
    void consume(Cluster cluster, IInvokableInstance nodeToRemove)
    {
        stopAbrupt(cluster, nodeToRemove);
    }

    @Override
    protected void afterNodeStatusIsLeft(Cluster cluster, IInvokableInstance removedNode)
    {
        // Check it is possible to alter keyspaces (see CASSANDRA-16422)

        // First, make sure the node is convicted so the gossiper considers it unreachable
        InetSocketAddress socketAddress = removedNode.config().broadcastAddress();
        InetAddressAndPort removedEndpoint = InetAddressAndPort.getByAddressOverrideDefaults(socketAddress.getAddress(),
                                                                                             socketAddress.getPort());
        cluster.get(BaseAssassinatedCase.SEED_NUM).runOnInstance(() -> Gossiper.instance.convict(removedEndpoint, 1.0));

        // Second, try and alter the keyspace.  Before the bug was fixed, this would fail as the check includes
        // unreachable nodes that could have LEFT status.
        cluster.schemaChangeIgnoringStoppedInstances(String.format("ALTER KEYSPACE %s WITH DURABLE_WRITES = false", KEYSPACE));
    }
}
