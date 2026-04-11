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

package org.apache.cassandra.distributed.test.gossip;

import java.net.InetSocketAddress;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;

public class GossipExpiryAfterAssassinateTest extends GossipExpiryTestBase
{
    @Override
    void doRemoval(Cluster cluster, IInvokableInstance toRemove)
    {
        // Shut down one peer, then have another assassinate it. The coordinating node will gossip a final LEFT status,
        // including the expiry time it calculated to the remaining members.
        IInvokableInstance coordinator = cluster.get(1);
        if (coordinator.equals(toRemove))
            throw new IllegalArgumentException("Node cannot assassinate itself");

        try
        {
            InetSocketAddress toAssassinate = toRemove.broadcastAddress();
            toRemove.shutdown().get();
            coordinator.nodetoolResult("assassinate", toAssassinate.getHostString()).asserts().success();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
