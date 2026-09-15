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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ReadyMessage;
import org.apache.cassandra.transport.messages.RegisterMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.transport.messages.SupportedMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GracefulDisconnectTest
{

    @BeforeClass
    public static void setUp() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
    }

    public Cluster buildCluster(int nodeCount, boolean gracefulDisconnectEnabled) throws IOException
    {
        return Cluster
               .build(nodeCount)
               .withConfig(config ->
                           config
                           .with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP)
                           .set("graceful_disconnect_enabled", gracefulDisconnectEnabled))
               .start();
    }

    @Test
    public void testGracefulDisconnectAdvertisedWhenEnabled() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042).build())
            {
                client.connect(false);
                Message.Response response = client.execute(new OptionsMessage());
                SupportedMessage supported = (SupportedMessage) response;

                assertThat(supported.supported.containsKey(StartupMessage.GRACEFUL_DISCONNECT))
                .as("GRACEFUL_DISCONNECT should be advertised in SUPPORTED when enabled")
                .isTrue();
            }
        }
    }

    @Test
    public void testGracefulDisconnectDoesNotAdvertisedWhenNotEnabled() throws IOException
    {
        try (Cluster cluster = buildCluster(1, false))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042).build())
            {
                client.connect(false);
                SupportedMessage supported = (SupportedMessage) client.execute(new OptionsMessage());

                assertThat(supported.supported.containsKey(StartupMessage.GRACEFUL_DISCONNECT))
                .as("GRACEFUL_DISCONNECT should NOT be advertised in SUPPORTED when disabled")
                .isFalse();
            }
        }
    }

    @Test
    public void testSubscriptionViaREGISTER() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                   .protocolVersion(ProtocolVersion.V5)
                                                   .build())
            {
                client.connect(false);
                Message.Response response = client.execute(
                new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));

                assertThat(response).isInstanceOf(ReadyMessage.class);

                int subscribedCount = cluster.get(1).callOnInstance(() ->
                                                                    CassandraDaemon.getInstanceForTesting()
                                                                                   .nativeTransportService()
                                                                                   .getChannelsSubscribedToGracefulDisconnectCount());

                assertThat(subscribedCount)
                .as("One channel should be subscribed to GRACEFUL_DISCONNECT")
                .isEqualTo(1);
            }
        }
    }

    @Test
    public void testRegisterGracefulDisconnectAcceptedOnV4() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042).protocolVersion(ProtocolVersion.V4).build())
            {
                client.connect(false);
                client.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));
                int subscribedCount = cluster.get(1).callOnInstance(() ->
                                                                    CassandraDaemon.getInstanceForTesting()
                                                                                   .nativeTransportService()
                                                                                   .getChannelsSubscribedToGracefulDisconnectCount());
                assertThat(subscribedCount)
                .as("V4 client should be able to subscribe")
                .isEqualTo(1);
            }
        }
    }

    @Test
    public void testNonSubscribedClientDoesNotReceiveEvent() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                   .protocolVersion(ProtocolVersion.V4)
                                                   .build())
            {
                client.connect(false);
                int subscribedCount = cluster.get(1).callOnInstance(() ->
                                                                    CassandraDaemon.getInstanceForTesting()
                                                                                   .nativeTransportService()
                                                                                   .getChannelsSubscribedToGracefulDisconnectCount());
                assertThat(subscribedCount).isEqualTo(0);
            }
        }
    }

    @Test
    public void testServerStopsAcceptingNewConnectionsOnDrain() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042).build())
            {
                client.connect(false);
                cluster.get(1).nodetool("drain");

                assertThatThrownBy(() -> {
                    SimpleClient newClient = SimpleClient.builder(nativeAddr.getHostString(), 9042).build();
                    newClient.connect(false);
                }).as("Server should reject new connections after stopAcceptingNewConnections")
                  .isNotNull();
                cluster.get(1).shutdown();
            }
        }
    }

    @Test
    public void testNoEventEmittedWhenDisabled() throws IOException
    {
        try (Cluster cluster = buildCluster(1, false))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042).build())
            {
                client.connect(false);
                client.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));

                int subscribedCount = cluster.get(1).callOnInstance(() ->
                                                                    CassandraDaemon.getInstanceForTesting()
                                                                                   .nativeTransportService()
                                                                                   .getChannelsSubscribedToGracefulDisconnectCount());

                assertThat(subscribedCount).isEqualTo(0);

                boolean disabled = cluster.get(1).callOnInstance(() -> !DatabaseDescriptor.getGracefulDisconnectEnabled());
                assertThat(disabled).isTrue();
            }
        }
    }

    @Test
    public void testDrainProceedsImmediatelyWithNoSubscribedConnections() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                   .protocolVersion(ProtocolVersion.V4)
                                                   .build())
            {
                client.connect(false);
                cluster.get(1).nodetoolResult("drain").asserts().success();
                cluster.get(1).shutdown();
            }
        }
    }

    @Test
    public void testMultipleConnectionsCanSubscribe() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client1 = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                    .protocolVersion(ProtocolVersion.V5)
                                                    .build();
                 SimpleClient client2 = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                    .protocolVersion(ProtocolVersion.V5)
                                                    .build())
            {
                client1.connect(false);
                client2.connect(false);

                client1.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));
                client2.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));

                int subscribedCount = cluster.get(1).callOnInstance(() ->
                                                                    CassandraDaemon.getInstanceForTesting()
                                                                                   .nativeTransportService()
                                                                                   .getChannelsSubscribedToGracefulDisconnectCount());

                assertThat(subscribedCount).isEqualTo(2);
            }
        }
    }

    @Test
    public void testInFlightQueryCompletesDuringDrain() throws Exception
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE ks.tbl (id int PRIMARY KEY, val text)");
            cluster.get(1).executeInternal("INSERT INTO ks.tbl (id, val) VALUES (1, 'test_val')");

            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                   .protocolVersion(ProtocolVersion.V5)
                                                   .build())
            {
                client.connect(false);
                client.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));

                CompletableFuture<Void> drainFuture = CompletableFuture.runAsync(() -> {
                    cluster.get(1).nodetool("drain");
                });

                Message.Response response = client.execute(new org.apache.cassandra.transport.messages.QueryMessage(
                "SELECT val FROM ks.tbl WHERE id = 1",
                org.apache.cassandra.cql3.QueryOptions.DEFAULT
                ));

                assertThat(response)
                .as("Query must succeed during drain without connection drop or timeout")
                .isInstanceOf(org.apache.cassandra.transport.messages.ResultMessage.Rows.class);

                drainFuture.get(10, TimeUnit.SECONDS);

                assertThatThrownBy(() -> {
                    SimpleClient newClient = SimpleClient.builder(nativeAddr.getHostString(), 9042).build();
                    newClient.connect(false);
                }).isNotNull();
            }
        }
    }

    @Test
    public void testCooperativeClientDisconnectsWithinGracePeriod() throws Exception
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();

            SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                              .protocolVersion(ProtocolVersion.V5)
                                              .build();
            client.connect(false);
            client.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));

            cluster.get(1).nodetool("drain");

            long forcedCount = cluster.get(1).callOnInstance(() ->
                org.apache.cassandra.metrics.ClientMetrics.instance.forcedDisconnects.getCount()
            );
            assertThat(forcedCount).as("ForcedDisconnects metric should be 0 for a cooperative client").isEqualTo(0);
        }
    }


    @Test
    public void testGracefulDisconnectAllowsOtherNodesToProceed() throws Exception
    {
        try (Cluster cluster = buildCluster(3, true))
        {
            InetSocketAddress drainingAddr = cluster.get(1).config().broadcastAddress();
            InetSocketAddress otherAddr = cluster.get(2).config().broadcastAddress();

            try (SimpleClient drainingClient = SimpleClient.builder(drainingAddr.getHostString(), 9042)
                                                           .protocolVersion(ProtocolVersion.V5)
                                                           .ignoreGracefulDisconnect()
                                                           .build();
                 SimpleClient otherClient = SimpleClient.builder(otherAddr.getHostString(), 9042)
                                                        .protocolVersion(ProtocolVersion.V5)
                                                        .build())
            {
                drainingClient.connect(false);
                drainingClient.execute(new RegisterMessage(
                Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));

                otherClient.connect(false);

                CompletableFuture<Void> drainFuture = CompletableFuture.runAsync(() ->
                                                                                 cluster.get(1).nodetoolResult("drain").asserts().success()
                );

                Awaitility.await()
                          .atMost(15, TimeUnit.SECONDS)
                          .until(() -> !cluster.get(1).callOnInstance(() ->
                                                                      CassandraDaemon.getInstanceForTesting()
                                                                                     .nativeTransportService()
                                                                                     .isRunning()));

                assertThat(selectVal(otherClient))
                .as("other nodes must continue serving requests during drain")
                .isEqualTo("local");

                drainFuture.get(30, TimeUnit.SECONDS);

                assertThat(drainingClient.isConnected())
                .as("the draining connection must eventually be closed")
                .isFalse();

                assertThat(selectVal(otherClient))
                .as("other nodes must remain available after drain completes")
                .isEqualTo("local");
            }
        }
    }

    @Test
    public void testWithoutGracefulDisconnectQueryFails() throws Exception
    {
        try (Cluster cluster = buildCluster(3, false))
        {
            InetSocketAddress drainingAddr = cluster.get(1).config().broadcastAddress();
            InetSocketAddress otherAddr = cluster.get(2).config().broadcastAddress();

            try (SimpleClient drainingClient = SimpleClient.builder(drainingAddr.getHostString(), 9042)
                                                           .protocolVersion(ProtocolVersion.V5)
                                                           .ignoreGracefulDisconnect()
                                                           .build();
                 SimpleClient otherClient = SimpleClient.builder(otherAddr.getHostString(), 9042)
                                                        .protocolVersion(ProtocolVersion.V5)
                                                        .build())
            {
                drainingClient.connect(false);
                otherClient.connect(false);

                CompletableFuture<Void> drainFuture = CompletableFuture.runAsync(() ->
                                                                                 cluster.get(1).nodetoolResult("drain").asserts().success()
                );

                Awaitility.await()
                          .atMost(15, TimeUnit.SECONDS)
                          .until(() -> !cluster.get(1).callOnInstance(() ->
                                                                      CassandraDaemon.getInstanceForTesting().nativeTransportService().isRunning()));

                Thread.sleep(500);

                assertThatThrownBy(() -> selectVal(drainingClient))
                .as("without graceful disconnect, query should timeout")
                .hasMessage("timeout");

                assertThat(selectVal(otherClient)).isEqualTo("local");

                drainFuture.get(30, TimeUnit.SECONDS);

                assertThat(drainingClient.isConnected())
                .as("the connection is force-closed once the grace period runs out")
                .isFalse();

                assertThat(selectVal(otherClient)).isEqualTo("local");
            }
        }
    }

    @Test
    public void testUncooperativeClientIsCountedDrainingThenForceDisconnected() throws Exception
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(config ->
                                                  config.with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP)
                                                        .set("graceful_disconnect_enabled", true)
                                                        .set("graceful_disconnect_grace_period", "5s"))
                                      .start())
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();

            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                   .protocolVersion(ProtocolVersion.V5)
                                                   .ignoreGracefulDisconnect()
                                                   .build())
            {
                client.connect(false);

                Message.Response response = client.execute(
                new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT))
                );

                assertThat(response)
                .as("Registration must return ReadyMessage")
                .isInstanceOf(ReadyMessage.class);

                CompletableFuture<Void> drainFuture = CompletableFuture.runAsync(() ->
                                                                                 cluster.get(1).nodetoolResult("drain").asserts().success()
                );

                Awaitility.await()
                          .atMost(10, TimeUnit.SECONDS)
                          .pollInterval(100, TimeUnit.MILLISECONDS)
                          .until(() -> cluster.get(1).callOnInstance(() ->
                                                                     ClientMetrics.instance.connectionsDraining.get()) == 1);

                assertThat(cluster.get(1).callOnInstance(() ->
                                                         ClientMetrics.instance.connectionsDraining.get()))
                .isEqualTo(1);

                drainFuture.get(10, TimeUnit.SECONDS);

                Awaitility.await()
                          .atMost(10, TimeUnit.SECONDS)
                          .until(() -> cluster.get(1).callOnInstance(() ->
                                                                     ClientMetrics.instance.connectionsDraining.get()) == 0);
            }
        }
    }
    private static String selectVal(SimpleClient client)
    {
        Message.Response response = client.execute(new QueryMessage(
        "SELECT * FROM system.local",
        QueryOptions.DEFAULT));
        ResultMessage.Rows rows = (ResultMessage.Rows) response;
        return new String(rows.result.rows.get(0).get(0), StandardCharsets.UTF_8);
    }
}
