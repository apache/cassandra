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

import java.util.HashMap;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableRunnable;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.net.MessagingService;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.containsString;

public final class InternodeEncryptionEnforcementTest extends TestBaseImpl
{
    @Test
    public void testConnectionsAreRejectedWithInvalidConfig() throws Throwable
    {
        Cluster.Builder builder = builder()
            .withNodes(2)
            .withConfig(c ->
            {
                c.with(Feature.NETWORK);
                c.with(Feature.NATIVE_PROTOCOL);

                if (c.num() == 1)
                {
                    HashMap<String, Object> encryption = new HashMap<>();
                    encryption.put("keystore", "test/conf/cassandra_ssl_test.keystore");
                    encryption.put("keystore_password", "cassandra");
                    encryption.put("truststore", "test/conf/cassandra_ssl_test.truststore");
                    encryption.put("truststore_password", "cassandra");
                    encryption.put("internode_encryption", "dc");
                    c.set("server_encryption_options", encryption);
                }
            })
            .withNodeIdTopology(ImmutableMap.of(1, NetworkTopology.dcAndRack("dc1", "r1a"),
                                                2, NetworkTopology.dcAndRack("dc2", "r2a")));

        try (Cluster cluster = builder.start())
        {
            try
            {
                openConnections(cluster);
                fail("Instances should not be able to connect, much less complete a schema change.");
            }
            catch (RuntimeException ise)
            {
                assertThat(ise.getMessage(), containsString("agreement not reached"));
            }

            /*
             * instance (1) won't connect to (2), since (2) won't have a TLS listener;
             * instance (2) won't connect to (1), since inbound check will reject
             * the unencrypted connection attempt;
             *
             * without the patch, instance (2) *CAN* connect to (1), without encryption,
             * despite being in a different dc.
             */

            cluster.get(1).runOnInstance(() ->
            {
                List<MessagingService.SocketThread> threads = MessagingService.instance().getSocketThreads();
                assertEquals(2, threads.size());

                for (MessagingService.SocketThread thread : threads)
                {
                    assertEquals(0, thread.connections.size());
                }
            });

            cluster.get(2).runOnInstance(() ->
            {
                List<MessagingService.SocketThread> threads = MessagingService.instance().getSocketThreads();
                assertEquals(1, threads.size());
                assertTrue(getOnlyElement(threads).connections.isEmpty());
            });
        }
    }

    @Test
    public void testConnectionsAreAcceptedWithValidConfig() throws Throwable
    {
        Cluster.Builder builder = builder()
            .withNodes(2)
            .withConfig(c ->
            {
                c.with(Feature.NETWORK);
                c.with(Feature.NATIVE_PROTOCOL);

                HashMap<String, Object> encryption = new HashMap<>();
                encryption.put("keystore", "test/conf/cassandra_ssl_test.keystore");
                encryption.put("keystore_password", "cassandra");
                encryption.put("truststore", "test/conf/cassandra_ssl_test.truststore");
                encryption.put("truststore_password", "cassandra");
                encryption.put("internode_encryption", "dc");
                c.set("server_encryption_options", encryption);
            })
            .withNodeIdTopology(ImmutableMap.of(1, NetworkTopology.dcAndRack("dc1", "r1a"),
                                                2, NetworkTopology.dcAndRack("dc2", "r2a")));

        try (Cluster cluster = builder.start())
        {
            openConnections(cluster);

            /*
             * instance (1) should connect to instance (2) without any issues;
             * instance (2) should connect to instance (1) without any issues.
             */

            SerializableRunnable runnable = () ->
            {
                List<MessagingService.SocketThread> threads = MessagingService.instance().getSocketThreads();
                assertEquals(2, threads.size());

                MessagingService.SocketThread sslThread = threads.get(0);
                assertEquals(1, sslThread.connections.size());

                MessagingService.SocketThread plainThread = threads.get(1);
                assertEquals(0, plainThread.connections.size());
            };

            cluster.get(1).runOnInstance(runnable);
            cluster.get(2).runOnInstance(runnable);
        }
    }

    private void openConnections(Cluster cluster)
    {
        cluster.schemaChange("CREATE KEYSPACE test_connections_from_1 " +
                             "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};", false, cluster.get(1));

        cluster.schemaChange("CREATE KEYSPACE test_connections_from_2 " +
                             "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};", false, cluster.get(2));
    }
}
