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

import static org.apache.cassandra.distributed.shared.ClusterUtils.assertGossipInfo;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopAll;

/**
 * If the operator attempts to assassinate the node before replacing it, this will cause the node to fail to start
 * as the status is non-normal.
 *
 * The cluster is put into the "empty" state for the node to remove.
 */
public class AssassinatedEmptyNodeTest extends BaseAssassinatedCase
{
    // empty state does not include the token metadata, so when assassinate happens it will fail to find the token
    @Override
    protected String expectedMessage(IInvokableInstance nodeToRemove)
    {
        return "Could not find tokens for " + nodeToRemove.config().broadcastAddress() + " to replace";
    }

    @Override
    void consume(Cluster cluster, IInvokableInstance nodeToRemove)
    {
        IInvokableInstance seed = cluster.get(SEED_NUM);
        IInvokableInstance peer = cluster.get(PEER_NUM);
        InetSocketAddress addressToReplace = nodeToRemove.broadcastAddress();

        // now stop all nodes
        stopAll(cluster);

        // with all nodes down, now start the seed (should be first node)
        seed.startup();
        peer.startup();

        // at this point node2 should be known in gossip, but with generation/version of 0
        assertGossipInfo(seed, addressToReplace, 0, -1);
        assertGossipInfo(peer, addressToReplace, 0, -1);
    }
}
