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

package org.apache.cassandra.distributed.test.auth;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.handler.ssl.SslHandler;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.TlsTestUtils;
import org.apache.cassandra.transport.messages.ReadyMessage;
import org.apache.cassandra.transport.messages.StartupMessage;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class NativeTransportAuthenticationTimeoutTlsTest extends TestBaseImpl
{
    private static final long AUTH_TIMEOUT_MS = 2000;
    private static final String TIMEOUT_LOG_MSG = "Closing unauthenticated client connection";

    @BeforeClass
    public static void setupClient()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void connectionWhichNeverStartsTlsHandshakeTimesOut() throws Exception
    {
        try (Cluster cluster = startCluster())
        {
            InetSocketHost host = hostAndPort(cluster);
            long mark = cluster.get(1).logs().mark();
            try (SimpleClient client = new SimpleClient(host.address, host.port))
            {
                client.establishConnection();
                assertThat(client.isOpen())
                    .as("Connection should initially be open")
                    .isTrue();

                assertConnectionEventuallyCloses(client);
            }
            assertThat(cluster.get(1).logs().watchFor(mark, TIMEOUT_LOG_MSG).getResult())
            .as("Server should log the timeout warning exactly once")
            .hasSize(1);
        }
    }

    @Test
    public void connectionWhichStopsAfterTlsHandshakeTimesOut() throws Exception
    {
        try (Cluster cluster = startCluster())
        {
            InetSocketHost host = hostAndPort(cluster);
            long mark = cluster.get(1).logs().mark();
            try (SimpleClient client = newTlsClient(host))
            {
                client.establishConnection();
                awaitTlsHandshake(client);
                assertThat(client.isOpen())
                    .as("Connection should remain open after TLS handshake")
                    .isTrue();
                assertConnectionEventuallyCloses(client);
            }
            assertThat(cluster.get(1).logs().watchFor(mark, TIMEOUT_LOG_MSG).getResult())
            .as("Server should log the timeout warning exactly once")
            .hasSize(1);
        }
    }

    @Test
    public void successfulConnectionOverTlsDoesNotTimeout() throws Exception
    {
        try (Cluster cluster = startCluster())
        {
            InetSocketHost host = hostAndPort(cluster);
            try (SimpleClient client = newTlsClient(host))
            {
                client.establishConnection();
                awaitTlsHandshake(client);

                StartupMessage startup = new StartupMessage(
                    Collections.singletonMap(StartupMessage.CQL_VERSION, QueryProcessor.CQL_VERSION.toString()));
                assertThat(client.execute(startup))
                    .as("Expected READY response")
                    .isInstanceOf(ReadyMessage.class);

                assertThat(client.isOpen())
                    .as("Connection should initially be open")
                    .isTrue();
                await().pollInterval(100, TimeUnit.MILLISECONDS)
                       .during(Duration.ofMillis(AUTH_TIMEOUT_MS + 500))
                       .until(client::isOpen);

                assertThat(client.isOpen())
                    .as("Connection should remain open after reaching READY")
                    .isTrue();
                client.execute("SELECT * FROM system.local", ConsistencyLevel.ONE);
            }
            assertThat(cluster.get(1).logs().grep(TIMEOUT_LOG_MSG).getResult())
                .as("Timeout log should not appear for successful connections")
                .isEmpty();
        }
    }

    private static SimpleClient newTlsClient(InetSocketHost host)
    {
        return SimpleClient.builder(host.address, host.port)
                           .encryption(TlsTestUtils.getClientEncryptionOptions())
                           .build();
    }

    private static void awaitTlsHandshake(SimpleClient client)
    {
        SslHandler sslHandler = client.channelPipeline().get(SslHandler.class);
        assertThat(sslHandler).as("SslHandler should be present in pipeline").isNotNull();

        await().atMost(10, TimeUnit.SECONDS)
               .pollInterval(25, TimeUnit.MILLISECONDS)
               .until(() -> sslHandler.handshakeFuture().isDone());

        assertThat(sslHandler.handshakeFuture().isSuccess())
            .as("TLS handshake should succeed")
            .isTrue();
    }

    private static void assertConnectionEventuallyCloses(SimpleClient client)
    {
        await().atMost(10, TimeUnit.SECONDS)
               .pollInterval(25, TimeUnit.MILLISECONDS)
               .until(() -> !client.isOpen());

        assertThat(client.isOpen())
            .as("Connection should close when authentication or handshake is not completed")
            .isFalse();
    }

    private static Cluster startCluster() throws IOException
    {
        Map<String, Object> encryptionOptions = new HashMap<>();
        encryptionOptions.put("enabled", true);
        encryptionOptions.put("optional", false);
        encryptionOptions.put("keystore", TlsTestUtils.SERVER_KEYSTORE_PATH);
        encryptionOptions.put("keystore_password", TlsTestUtils.SERVER_KEYSTORE_PASSWORD);
        encryptionOptions.put("truststore", TlsTestUtils.SERVER_TRUSTSTORE_PATH);
        encryptionOptions.put("truststore_password", TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD);

        return init(Cluster.build().withNodes(1)
                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                       .set("client_encryption_options", encryptionOptions)
                                                       .set("native_transport_auth_timeout",
                                                            AUTH_TIMEOUT_MS + "ms"))
                           .start());
    }

    private static InetSocketHost hostAndPort(Cluster cluster)
    {
        String address = cluster.get(1).config()
                                .broadcastAddress()
                                .getAddress()
                                .getHostAddress();
        int port = cluster.get(1).callOnInstance(DatabaseDescriptor::getNativeTransportPort);
        return new InetSocketHost(address, port);
    }

    private static class InetSocketHost
    {
        final String address;
        final int port;

        InetSocketHost(String address, int port)
        {
            this.address = address;
            this.port = port;
        }
    }
}