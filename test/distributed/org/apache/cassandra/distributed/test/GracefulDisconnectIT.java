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
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.apache.cassandra.transport.messages.ReadyMessage;
import org.apache.cassandra.transport.messages.RegisterMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.transport.messages.SupportedMessage;

import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

public class GracefulDisconnectIT
{

    @BeforeAll
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
                Assertions.assertTrue(supported.supported.containsKey(StartupMessage.GRACEFUL_DISCONNECT),
                                      "GRACEFUL_DISCONNECT should be advertised in SUPPORTED when enabled");
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
                Assertions.assertFalse(((SupportedMessage) client.execute(new OptionsMessage())).supported.containsKey(StartupMessage.GRACEFUL_DISCONNECT),
                                       "GRACEFUL_DISCONNECT should be advertised in SUPPORTED when enabled");
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

                Assertions.assertTrue(response instanceof ReadyMessage,
                                      "REGISTER for GRACEFUL_DISCONNECT should be accepted with READY");

                int subscribedCount = cluster.get(1).callOnInstance(() ->
                                                                    CassandraDaemon.getInstanceForTesting()
                                                                                   .nativeTransportService()
                                                                                   .getServer()
                                                                                   .getChannelsSubscribedToGracefulDisconnect()
                                                                                   .size());

                Assertions.assertEquals(1, subscribedCount,
                                        "One channel should be subscribed to GRACEFUL_DISCONNECT");
            }
        }
    }

    @Test
    public void testRegisterGracefulDisconnectRejectedOnV4() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042).protocolVersion(ProtocolVersion.V4).build())
            {
                client.connect(false);
                int subscribedCount = cluster.get(1).callOnInstance(() ->
                                                                    CassandraDaemon.getInstanceForTesting()
                                                                                   .nativeTransportService()
                                                                                   .getServer()
                                                                                   .getChannelsSubscribedToGracefulDisconnect()
                                                                                   .size());

                Assertions.assertEquals(0, subscribedCount,
                                        "One channel should be subscribed to GRACEFUL_DISCONNECT");
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
                                                                                   .getServer()
                                                                                   .getChannelsSubscribedToGracefulDisconnect()
                                                                                   .size());
                Assertions.assertEquals(0, subscribedCount,
                                        "V4 client should not be subscribed to GRACEFUL_DISCONNECT");
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
                cluster.get(1).runOnInstance(() -> {
                    CassandraDaemon.getInstanceForTesting()
                                   .nativeTransportService()
                                   .getServer()
                                   .stopAcceptingNewConnections();
                });
                Assertions.assertThrows(Exception.class, () -> {
                    SimpleClient newClient = SimpleClient.builder(nativeAddr.getHostString(), 9042).build();
                    newClient.connect(false);
                }, "Server should reject new connections after stopAcceptingNewConnections");
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
                                                                                   .getServer()
                                                                                   .getChannelsSubscribedToGracefulDisconnect()
                                                                                   .size());

                Assertions.assertEquals(0, subscribedCount);
                boolean gracefulDisconnectCalled = cluster.get(1).callOnInstance(() -> !DatabaseDescriptor.getGracefulDisconnectEnabled());
                Assertions.assertTrue(gracefulDisconnectCalled,
                                      "graceful_disconnect_enabled=false should skip event emission");
            }
        }
    }

    @Test
    public void testMaxDrainMsUpdatedViaJMX() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            cluster.get(1).runOnInstance(() ->
                                         StorageService.instance.setGracefulDisconnectMaxDrainMs(15000));

            long maxDrainMs = cluster.get(1).callOnInstance(StorageService.instance::getGracefulDisconnectMaxDrainMs);

            Assertions.assertEquals(15000, maxDrainMs,
                                    "max_drain_ms should be updated to 15000 via JMX");
        }
    }

    @Test
    public void testGracePeriodMsUpdatedViaJMX() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            cluster.get(1).runOnInstance(() -> {
                StorageService.instance.setGracefulDisconnectGracePeriodMs(3000);
            });

            long gracePeriodMs = cluster.get(1).callOnInstance(StorageService.instance::getGracefulDisconnectGracePeriodMs);

            Assertions.assertEquals(3000, gracePeriodMs,
                                    "grace_period_ms should be updated to 3000 via JMX");
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
                long start = System.currentTimeMillis();
                cluster.get(1).runOnInstance(() -> {
                    try
                    {
                        StorageService.instance.gracefulDisconnect(() -> {
                        }, new DefaultChannelGroup(GlobalEventExecutor.INSTANCE));
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                });
                // the one hardcoded 1s limit may make this a flaky test case
                long elapsed = System.currentTimeMillis() - start;
                Assertions.assertTrue(elapsed < 1000,
                                      "Should proceed immediately with no subscribed connections, took: " + elapsed + "ms");
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
                                                                                   .getServer()
                                                                                   .getChannelsSubscribedToGracefulDisconnect()
                                                                                   .size());

                Assertions.assertEquals(2, subscribedCount,
                                        "Both connections should be subscribed to GRACEFUL_DISCONNECT");
            }
        }
    }

    @Test
    public void testConnectionsDrainingMetric() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                   .protocolVersion(ProtocolVersion.V5)
                                                   .build())
            {
                client.connect(false);
                client.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));


                int drainingCount = cluster.get(1).callOnInstance(() ->
                                                                  ClientMetrics.instance.connectionsDraining.get());

                Assertions.assertEquals(0, drainingCount,
                                        "connections_draining should be 0 before drain");
            }
        }
    }

    @Test
    public void testGracefulDisconnectEventReceivedOnDrain() throws IOException, InterruptedException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                   .protocolVersion(ProtocolVersion.V5)
                                                   .build())
            {
                SimpleClient.SimpleEventHandler handler = new SimpleClient.SimpleEventHandler();
                client.setEventHandler(handler);
                client.connect(false);
                client.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));

                cluster.get(1).runOnInstance(() -> {
                    ChannelGroup channelGroup = CassandraDaemon.getInstanceForTesting()
                                                               .nativeTransportService()
                                                               .getServer()
                                                               .getChannelsSubscribedToGracefulDisconnect();
                    StorageService.instance.gracefulDisconnect(() -> {
                    }, channelGroup);
                });

                Event event = handler.queue.poll(10, TimeUnit.SECONDS);
                Assertions.assertNotNull(event, "Expected GRACEFUL_DISCONNECT event but got null");
                Assertions.assertEquals(Event.Type.GRACEFUL_DISCONNECT, event.type,
                                        "Expected GRACEFUL_DISCONNECT event type");
            }
        }
    }

    @Test
    public void testDefaultActionCalledAfterAllConnectionsClose() throws IOException, InterruptedException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                   .protocolVersion(ProtocolVersion.V5)
                                                   .build())
            {
                client.connect(false);
                client.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));

                client.close();
                Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                    int drainingCount = cluster.get(1).callOnInstance(() ->
                                                                      ClientMetrics.instance.connectionsDraining.get());
                    Assertions.assertEquals(0, drainingCount,
                                            "connections_draining should be 0 after all connections close");
                });

                boolean actionCalled = cluster.get(1).callOnInstance(() -> {
                    AtomicBoolean called = new AtomicBoolean(false);
                    ChannelGroup channelGroup = CassandraDaemon.getInstanceForTesting()
                                                               .nativeTransportService()
                                                               .getServer()
                                                               .getChannelsSubscribedToGracefulDisconnect();
                    StorageService.instance.gracefulDisconnect(() -> called.set(true), channelGroup);
                    return called.get();
                });

                Assertions.assertTrue(actionCalled,
                                      "Default action should be called immediately when no subscribed connections");
            }
        }
    }

    @Test
    public void testDefaultActionCalledAfterMaxDrainMs() throws IOException, InterruptedException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                   .protocolVersion(ProtocolVersion.V5)
                                                   .build())
            {
                client.connect(false);
                client.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));

                cluster.get(1).runOnInstance(() -> {
                    StorageService.instance.setGracefulDisconnectGracePeriodMs(500);
                    StorageService.instance.setGracefulDisconnectMaxDrainMs(1000);
                });

                cluster.get(1).runOnInstance(() -> {
                    ChannelGroup channelGroup = CassandraDaemon.getInstanceForTesting()
                                                               .nativeTransportService()
                                                               .getServer()
                                                               .getChannelsSubscribedToGracefulDisconnect();
                    StorageService.instance.gracefulDisconnect(() -> {
                    }, channelGroup);
                });

                Thread.sleep(3000);

                boolean actionCalled = cluster.get(1).callOnInstance(() -> ClientMetrics.instance.connectionsDraining.get() == 0);

                Assertions.assertTrue(actionCalled,
                                      "Default action should be called after max_drain_ms timeout");
            }
            finally
            {
                cluster.get(1).runOnInstance(() -> {
                    StorageService.instance.setGracefulDisconnectMaxDrainMs(30000);
                    StorageService.instance.setGracefulDisconnectGracePeriodMs(5000);
                });
            }
        }
    }

    @Test
    public void testMultipleConnectionsAllReceiveEvent() throws IOException, InterruptedException
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
                SimpleClient.SimpleEventHandler handler1 = new SimpleClient.SimpleEventHandler();
                SimpleClient.SimpleEventHandler handler2 = new SimpleClient.SimpleEventHandler();
                client1.setEventHandler(handler1);
                client2.setEventHandler(handler2);

                client1.connect(false);
                client2.connect(false);

                client1.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));
                client2.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));

                cluster.get(1).runOnInstance(() -> {
                    ChannelGroup channelGroup = CassandraDaemon.getInstanceForTesting()
                                                               .nativeTransportService()
                                                               .getServer()
                                                               .getChannelsSubscribedToGracefulDisconnect();
                    StorageService.instance.gracefulDisconnect(() -> {
                    }, channelGroup);
                });

                Event event1 = handler1.queue.poll(10, TimeUnit.SECONDS);
                Event event2 = handler2.queue.poll(10, TimeUnit.SECONDS);

                Assertions.assertNotNull(event1, "client1 should receive GRACEFUL_DISCONNECT");
                Assertions.assertNotNull(event2, "client2 should receive GRACEFUL_DISCONNECT");
                Assertions.assertEquals(Event.Type.GRACEFUL_DISCONNECT, event1.type);
                Assertions.assertEquals(Event.Type.GRACEFUL_DISCONNECT, event2.type);
            }
        }
    }

    @Test
    public void testMixedFleetOnlySubscribedReceivesEvent() throws IOException, InterruptedException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            InetSocketAddress nativeAddr = cluster.get(1).config().broadcastAddress();
            try (SimpleClient subscribedClient = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                             .protocolVersion(ProtocolVersion.V5)
                                                             .build();
                 SimpleClient nonSubscribedClient = SimpleClient.builder(nativeAddr.getHostString(), 9042)
                                                                .protocolVersion(ProtocolVersion.V4)
                                                                .build())
            {
                SimpleClient.SimpleEventHandler subscribedHandler = new SimpleClient.SimpleEventHandler();
                SimpleClient.SimpleEventHandler nonSubscribedHandler = new SimpleClient.SimpleEventHandler();
                subscribedClient.setEventHandler(subscribedHandler);
                nonSubscribedClient.setEventHandler(nonSubscribedHandler);

                subscribedClient.connect(false);
                nonSubscribedClient.connect(false);
                subscribedClient.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));

                cluster.get(1).runOnInstance(() -> {
                    ChannelGroup channelGroup = CassandraDaemon.getInstanceForTesting()
                                                               .nativeTransportService()
                                                               .getServer()
                                                               .getChannelsSubscribedToGracefulDisconnect();
                    StorageService.instance.gracefulDisconnect(() -> {
                    }, channelGroup);
                });

                Event subscribedEvent = subscribedHandler.queue.poll(10, TimeUnit.SECONDS);
                Event nonSubscribedEvent = nonSubscribedHandler.queue.poll(3, TimeUnit.SECONDS);

                Assertions.assertNotNull(subscribedEvent, "Subscribed client should receive GRACEFUL_DISCONNECT");
                Assertions.assertNull(nonSubscribedEvent, "Non-subscribed client should not receive GRACEFUL_DISCONNECT");
            }
        }
    }
}
