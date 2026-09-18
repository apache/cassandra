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

import com.datastax.driver.core.Session;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ManagementTransportMultiNodeTest extends TestBaseImpl
{
    private static final int MANAGEMENT_PORT = 11211;

    @Test
    public void testMultiNodeManagementPort() throws Exception
    {
        int nodes = 3;
        try (Cluster cluster = init(Cluster.build(nodes)
                                           .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                                       .set("start_native_transport_management", true)
                                                                       .set("native_transport_management_port", MANAGEMENT_PORT))
                                           .start()))
        {
            for (int i = 1; i <= nodes; i++)
            {
                String host = cluster.get(i).config().broadcastAddress().getAddress().getHostAddress();
                try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder()
                                                                                          .addContactPoint(host)
                                                                                          .withPort(MANAGEMENT_PORT)
                                                                                          .build();
                     Session s = c.connect())
                {
                    assertFalse("Management port on node " + i + " should respond",
                                s.execute("SELECT * FROM system.local").all().isEmpty());
                }
            }

            String node1Host = cluster.get(1).config().broadcastAddress().getAddress().getHostAddress();
            try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder()
                                                                                      .addContactPoint(node1Host)
                                                                                      .withPort(MANAGEMENT_PORT)
                                                                                      .build();
                 Session s = c.connect())
            {
                int peerCount = s.execute("SELECT * FROM system.peers").all().size();
                assertEquals("Node 1 should see other peers via management port",
                             nodes - 1, peerCount);

                assertFalse("Keyspaces should be visible via management port",
                            s.execute("SELECT * FROM system_schema.keyspaces").all().isEmpty());
            }
        }
    }
}
