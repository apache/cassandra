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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.apache.cassandra.transport.messages.ReadyMessage;
import org.apache.cassandra.transport.messages.RegisterMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.transport.messages.SupportedMessage;

// TODO: Expand integration tests once Java driver GRACEFUL_DISCONNECT support is complete.
// Test cases to cover:
// 5. Non-subscribed clients do NOT receive the event
// 6. In-flight requests complete successfully after GRACEFUL_DISCONNECT is received
// 7. No new requests are sent by driver after receiving event
// 8. Driver closes connection after all in-flight requests complete
// 9. Server proceeds with drain() after all subscribed connections close
// 10. Server force-closes connections that don't close within max_drain_ms
// 11. drain() proceeds after max_drain_ms even if connections are still open
// 12. Mixed fleet — some connections subscribed, some not — drain proceeds correctly
// 13. Non-subscribed connections continue serving requests during drain window
// 14. Server stops accepting new connections after emitting GRACEFUL_DISCONNECT
// 15. connections_draining increments correctly when event is emitted
// 16. connections_draining decrements when connection closes cleanly
// 17. connections_draining decrements when connection is force-closed
// 18. forced_disconnects increments when max_drain_ms is exceeded
// 19. forced_disconnects does NOT increment on clean drain
// 20. max_drain_ms updated via JMX — new value respected during drain
// 21. grace_period_ms updated via JMX — reflected correctly
// 22. No subscribed connections — drain proceeds immediately
// 23. All connections already closed before drain — drain proceeds immediately
// 24. Single connection drains cleanly
// 25. Multiple connections all drain cleanly
// 26. Multiple connections — some clean, some force-closed
// 26. Connection closes mid-drain (before event is sent)
// 27. Node restart after drain — new connections can subscribe again
// 28. Rapid consecutive drains — no state leakage between drains
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
                Throwable exception = Assertions.assertThrows(RuntimeException.class, () -> client.connect(false));
                Assertions.assertTrue(exception.getCause() instanceof ProtocolException, "Expected cause to be ProtocolException but was " + exception.getCause().getClass().getName());
                Assertions.assertTrue(exception.getMessage().contains("GRACEFUL_DISCONNECT not valid"), "Error message did not contain expected text. Found: " + exception.getMessage());
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
}
