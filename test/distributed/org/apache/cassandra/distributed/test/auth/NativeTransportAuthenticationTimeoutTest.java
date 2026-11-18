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
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.AuthResponse;
import org.apache.cassandra.transport.messages.AuthSuccess;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.utils.AssertionUtils.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class NativeTransportAuthenticationTimeoutTest extends TestBaseImpl
{
    private static final long AUTH_TIMEOUT_MS = 2000;
    private static final String TIMEOUT_LOG_MSG = "Closing unauthenticated client connection";

    @BeforeClass
    public static void setupClient()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void unauthenticatedConnectionShouldTimeout() throws Exception
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

            cluster.get(1).logs().watchFor(mark, TIMEOUT_LOG_MSG);
        }
    }

    @Test
    public void incompleteAuthenticationShouldTimeout() throws Exception
    {
        try (Cluster cluster = startCluster())
        {
            InetSocketHost host = hostAndPort(cluster);
            long mark = cluster.get(1).logs().mark();

            try (SimpleClient client = new SimpleClient(host.address, host.port))
            {
                client.connect(false);
                assertThat(client.isOpen())
                .as("Connection should initially be open")
                .isTrue();

                assertConnectionEventuallyCloses(client);
            }

            cluster.get(1).logs().watchFor(mark, TIMEOUT_LOG_MSG);
        }
    }

    @Test
    public void successfulAuthenticationShouldNotTimeout() throws Exception
    {
        try (Cluster cluster = startCluster())
        {
            InetSocketHost host = hostAndPort(cluster);
            try (SimpleClient client = new SimpleClient(host.address, host.port))
            {
                client.connect(false);
                AuthSuccess response = (AuthSuccess) client.execute(new AuthResponse(
                AuthTestUtils.getToken("cassandra", "cassandra")));
                assertThat(response)
                .as("Expected successful authentication")
                .isNotNull();
                assertThat(client.isOpen())
                .as("Authenticated connection should remain open")
                .isTrue();

                Awaitility.await()
                          .pollInterval(100, TimeUnit.MILLISECONDS)
                          .during(Duration.ofMillis(AUTH_TIMEOUT_MS + 500))
                          .until(client::isOpen);

                assertThat(client.isOpen())
                .as("Authenticated connection must not be closed by authentication timeout")
                .isTrue();

                client.execute("SELECT * FROM system.local", ConsistencyLevel.ONE);
            }

            assertThat(cluster.get(1).logs().grep(TIMEOUT_LOG_MSG).getResult())
                                           .as("Timeout log should not appear for successfully authenticated connections")
                                           .hasSize(0);
        }
    }

    @Test
    public void failedAuthenticationShouldEventuallyTimeout() throws Exception
    {
        try (Cluster cluster = startCluster())
        {
            InetSocketHost host = hostAndPort(cluster);
            long mark = cluster.get(1).logs().mark();

            try (SimpleClient client = new SimpleClient(host.address, host.port))
            {
                client.connect(false);
                assertThatThrownBy(() -> client.execute(new AuthResponse(
                AuthTestUtils.getToken("cassandra", "wrong_password"))))
                .hasRootCauseInstanceOf(AuthenticationException.class);
                assertConnectionEventuallyCloses(client);
            }
            cluster.get(1).logs().watchFor(mark, TIMEOUT_LOG_MSG);
        }
    }

    @Test
    public void earlyClientDisconnectShouldCancelTimeoutWithoutLoggingWarning() throws Exception
    {
        try (Cluster cluster = startCluster())
        {
            InetSocketHost host = hostAndPort(cluster);
            long mark = cluster.get(1).logs().mark();

            // Connect and close immediately
            try (SimpleClient client = new SimpleClient(host.address, host.port))
            {
                client.establishConnection();
                assertThat(client.isOpen()).isTrue();
            } // client closes here via try-with-resources

            Awaitility.await()
                      .pollInterval(100, TimeUnit.MILLISECONDS)
                      .during(Duration.ofMillis(AUTH_TIMEOUT_MS + 500))
                      .until(() -> cluster.get(1).logs().grep(mark, TIMEOUT_LOG_MSG).getResult().isEmpty());

            assertThat(cluster.get(1).logs().grep(mark, TIMEOUT_LOG_MSG).getResult())
            .as("Timeout log should not appear if client disconnected voluntarily")
            .hasSize(0);
        }
    }

    @Test
    public void unauthenticatedConnectionShouldNotTimeoutWhenDisabled() throws Exception
    {
        try (Cluster cluster = init(Cluster.build().withNodes(1)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                                       .set("authenticator", "PasswordAuthenticator")
                                                                       .set("native_transport_auth_timeout", "0ms"))
                                           .start()))
        {
            InetSocketHost host = hostAndPort(cluster);
            try (SimpleClient client = new SimpleClient(host.address, host.port))
            {
                client.establishConnection();
                assertThat(client.isOpen()).isTrue();

                Awaitility.await()
                          .pollInterval(100, TimeUnit.MILLISECONDS)
                          .during(Duration.ofMillis(AUTH_TIMEOUT_MS + 500))
                          .until(client::isOpen);

                assertThat(client.isOpen())
                .as("Connection should remain open when auth timeout is disabled (0ms)")
                .isTrue();
            }

            org.assertj.core.api.Assertions.assertThat(cluster.get(1).logs().grep(TIMEOUT_LOG_MSG).getResult())
                                           .as("Timeout log should not appear when feature is disabled")
                                           .hasSize(0);
        }
    }

    private static Cluster startCluster() throws IOException
    {
        return init(Cluster.build().withNodes(1)
                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                       .set("authenticator", "PasswordAuthenticator")
                                                       .set("native_transport_auth_timeout",
                                                            AUTH_TIMEOUT_MS + "ms"))
                           .start());
    }

    private static void assertConnectionEventuallyCloses(SimpleClient client)
    {
        Awaitility.await()
                  .atMost(10, TimeUnit.SECONDS)
                  .pollInterval(25, TimeUnit.MILLISECONDS)
                  .until(() -> !client.isOpen());

        assertThat(client.isOpen())
        .as("Connection should close when authentication is not completed")
        .isFalse();
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
