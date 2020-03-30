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

package org.apache.cassandra.distributed.test;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.Session;
import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.impl.INodeProvisionStrategy.Strategy.OneNetworkInterface;
import static org.awaitility.Awaitility.await;

public class NodeDecommissionTest extends TestBaseImpl
{

    @Test
    public void testDecomissionSucceedsForNodesOnTheSameInterface() throws Throwable
    {
        try (Cluster control = init(Cluster.build().withNodes(3).withNodeProvisionStrategy(OneNetworkInterface).withConfig(
        config -> {
            config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL);
        }).start()))
        {
            final com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
            Session session = cluster.connect();
            control.get(3).nodetool("disablebinary");
            control.get(3).nodetool("decommission", "-f");
            await().atMost(10, TimeUnit.SECONDS)
                   .untilAsserted(() -> Assert.assertEquals(2, cluster.getMetadata().getAllHosts().size()));
            session.close();
            cluster.close();
        }
    }
}

