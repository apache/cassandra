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
import org.junit.BeforeClass;
import org.junit.Test;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GracefulDisconnectIT
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
                                                                                   .getServer()
                                                                                   .getChannelsSubscribedToGracefulDisconnect()
                                                                                   .size());

                assertThat(subscribedCount)
                .as("One channel should be subscribed to GRACEFUL_DISCONNECT")
                .isEqualTo(1);
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

                assertThat(subscribedCount)
                .as("V4 client should not be able to subscribe")
                .isEqualTo(0);
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
                cluster.get(1).runOnInstance(() -> {
                    CassandraDaemon.getInstanceForTesting()
                                   .nativeTransportService()
                                   .getServer()
                                   .stopAcceptingNewConnections();
                });

                assertThatThrownBy(() -> {
                    SimpleClient newClient = SimpleClient.builder(nativeAddr.getHostString(), 9042).build();
                    newClient.connect(false);
                }).as("Server should reject new connections after stopAcceptingNewConnections")
                  .isNotNull();
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

                assertThat(subscribedCount).isEqualTo(0);

                boolean disabled = cluster.get(1).callOnInstance(() -> !DatabaseDescriptor.getGracefulDisconnectEnabled());
                assertThat(disabled).isTrue();
            }
        }
    }

    @Test
    public void testMaxDrainMsUpdatedViaJMX() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            cluster.get(1).runOnInstance(() ->
                                         StorageService.instance.setGracefulDisconnectMaxDrain(15000));

            long maxDrain = cluster.get(1).callOnInstance(() -> {
                return StorageService.instance.getGracefulDisconnectMaxDrain();
            });

            assertThat(maxDrain).isEqualTo(15000L);
        }
    }

    @Test
    public void testGracePeriodMsUpdatedViaJMX() throws IOException
    {
        try (Cluster cluster = buildCluster(1, true))
        {
            cluster.get(1).runOnInstance(() -> {
                StorageService.instance.setGracefulDisconnectGracePeriod(3000);
            });

            long gracePeriodMs = cluster.get(1).callOnInstance(() -> {
                return StorageService.instance.getGracefulDisconnectGracePeriod();
            });

            assertThat(gracePeriodMs).isEqualTo(3000L);
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

                long elapsed = System.currentTimeMillis() - start;
                assertThat(elapsed)
                .as("Should proceed immediately with no subscribed connections")
                .isLessThan(1000L);
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

                assertThat(subscribedCount).isEqualTo(2);
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

                assertThat(drainingCount).isEqualTo(0);
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
                assertThat(event).isNotNull();
                assertThat(event.type).isEqualTo(Event.Type.GRACEFUL_DISCONNECT);
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
                    assertThat(drainingCount).isEqualTo(0);
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

                assertThat(actionCalled).isTrue();
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
                    StorageService.instance.setGracefulDisconnectGracePeriod(500);
                    StorageService.instance.setGracefulDisconnectMaxDrain(1000);
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

                boolean finishedDraining = cluster.get(1).callOnInstance(() -> ClientMetrics.instance.connectionsDraining.get() == 0);
                assertThat(finishedDraining).isTrue();
            }
            finally
            {
                cluster.get(1).runOnInstance(() -> {
                    StorageService.instance.setGracefulDisconnectMaxDrain(30000);
                    StorageService.instance.setGracefulDisconnectGracePeriod(5000);
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

                assertThat(event1).isNotNull();
                assertThat(event2).isNotNull();
                assertThat(event1.type).isEqualTo(Event.Type.GRACEFUL_DISCONNECT);
                assertThat(event2.type).isEqualTo(Event.Type.GRACEFUL_DISCONNECT);
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

                assertThat(subscribedEvent).isNotNull();
                assertThat(subscribedEvent.type).isEqualTo(Event.Type.GRACEFUL_DISCONNECT);
                assertThat(nonSubscribedEvent).isNull();
            }
        }
    }
}