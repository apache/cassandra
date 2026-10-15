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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.transport.ConnectedClient;
import org.apache.cassandra.transport.Message.Response;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.AuthResponse;
import org.apache.cassandra.transport.messages.AuthSuccess;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.auth.AuthTestUtils.getToken;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.util.Auth.waitForExistingRoles;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConnectedClientVisibilityTest extends TestBaseImpl
{
    private static final String CLIENTS_QUERY =
    "SELECT address, hostname, client_options, connection_stage, driver_name, driver_version, " +
    "protocol_version, request_count, ssl_cipher_suite, ssl_enabled, ssl_protocol, username, " +
    "keyspace_name, authentication_mode, authentication_metadata " +
    "FROM system_views.clients";

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void unauthenticatedClientIsVisibleWithoutExposingClientMetadata() throws IOException
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                        .set("authenticator", "PasswordAuthenticator"))
                                      .start())
        {
            IInvokableInstance node = cluster.get(1);

            try (SimpleClient client = SimpleClient.builder("127.0.0.1", 9042).build())
            {
                client.connect(false);

                int[] serverSide = node.callOnInstance(() -> {
                    long deadline = Clock.Global.nanoTime() + TimeUnit.SECONDS.toNanos(30);

                    while (ClientMetrics.instance.allConnectedClients().isEmpty()
                           && Clock.Global.nanoTime() < deadline)
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));

                    int total = ClientMetrics.instance.allConnectedClients().size();
                    int loggedIn = 0;

                    for (ConnectedClient c : ClientMetrics.instance.allConnectedClients())
                    {
                        if (c.loggedIn())
                            loggedIn++;
                    }

                    return new int[]{ total, loggedIn };
                });

                assertTrue("expected the connection to be registered server-side", serverSide[0] >= 1);
                assertEquals("no connected client should be authenticated yet", 0, serverSide[1]);

                Object[][] rows = waitForClientRows(node);
                assertEquals("unauthenticated client should be listed", 1, rows.length);

                Object[] row = rows[0];

                assertNotNull("remote address should be visible", row[0]);
                assertNotNull("hostname should be visible", row[1]);
                assertNotNull("connection stage should be visible", row[3]);
                assertNotNull("protocol version should be visible", row[6]);
                assertNotNull("request count should be visible", row[7]);

                assertEquals("SSL should be disabled for this test connection", Boolean.FALSE, row[9]);
                assertNull("SSL cipher should be null without TLS", row[8]);
                assertNull("SSL protocol should be null without TLS", row[10]);

                // Client-supplied and authentication-dependent metadata is hidden.
                assertNull("client options must not be exposed before authentication", row[2]);
                assertNull("driver name must not be exposed before authentication", row[4]);
                assertNull("driver version must not be exposed before authentication", row[5]);
                assertNull("username must not be exposed before authentication", row[11]);
                assertNull("keyspace must not be exposed before authentication", row[12]);
                assertNull("authentication mode must not be exposed before authentication", row[13]);
                assertNull("authentication metadata must not be exposed before authentication", row[14]);

                boolean[] mapFieldsBeforeAuthentication = node.callOnInstance(() -> {
                    for (ConnectedClient c : ClientMetrics.instance.allConnectedClients())
                    {
                        if (!c.loggedIn())
                        {
                            Map<String, String> map = c.asMap();

                            return new boolean[]{
                            map.containsKey(ConnectedClient.ADDRESS),
                            map.containsKey(ConnectedClient.VERSION),
                            map.containsKey(ConnectedClient.REQUESTS),
                            map.containsKey(ConnectedClient.SSL),
                            map.containsKey(ConnectedClient.CIPHER),
                            map.containsKey(ConnectedClient.PROTOCOL),
                            map.containsKey(ConnectedClient.CLIENT_OPTIONS),
                            map.containsKey(ConnectedClient.USER),
                            map.containsKey(ConnectedClient.DRIVER_NAME),
                            map.containsKey(ConnectedClient.DRIVER_VERSION),
                            map.containsKey(ConnectedClient.KEYSPACE),
                            map.containsKey(ConnectedClient.AUTHENTICATION_MODE),
                            map.containsKey(ConnectedClient.AUTHENTICATION_METADATA)
                            };
                        }
                    }

                    return new boolean[0];
                });

                assertEquals("expected to find the unauthenticated client's map", 13,
                             mapFieldsBeforeAuthentication.length);

                // Basic connection metadata is present.
                for (int i = 0; i < 6; i++)
                    assertTrue("basic connection field at index " + i + " should be present",
                               mapFieldsBeforeAuthentication[i]);

                // Authenticated metadata is absent.
                for (int i = 6; i < mapFieldsBeforeAuthentication.length; i++)
                    assertFalse("authenticated metadata field at index " + i + " must be absent",
                                mapFieldsBeforeAuthentication[i]);

                // Complete authentication on the same connection.
                waitForExistingRoles(node);

                Response authResult = client.execute(
                new AuthResponse(getToken("cassandra", "cassandra"))
                );

                assertTrue("expected AUTH_SUCCESS, got: " + authResult,
                           authResult instanceof AuthSuccess);

                rows = waitForAuthenticatedClient(node);

                assertEquals("authenticated client should remain listed", 1, rows.length);

                row = rows[0];

                // Basic connection metadata remains visible.
                assertNotNull("remote address should remain visible", row[0]);
                assertNotNull("hostname should remain visible", row[1]);
                assertNotNull("connection stage should remain visible", row[3]);
                assertNotNull("protocol version should remain visible", row[6]);

                // Authenticated metadata becomes visible.
                assertNotNull("client options should be visible after authentication", row[2]);
                assertEquals("cassandra", row[11]);
                assertNotNull("authentication mode should be visible after authentication", row[13]);
                assertNotNull("authentication metadata should be visible after authentication", row[14]);

                @SuppressWarnings("unchecked")
                Map<String, String> options = (Map<String, String>) row[2];

                assertTrue("STARTUP CQL_VERSION should be present after authentication",
                           options.containsKey("CQL_VERSION"));

                boolean[] mapFieldsAfterAuthentication = node.callOnInstance(() -> {
                    for (ConnectedClient c : ClientMetrics.instance.allConnectedClients())
                    {
                        if (c.loggedIn())
                        {
                            Map<String, String> map = c.asMap();

                            return new boolean[]{
                            map.containsKey(ConnectedClient.ADDRESS),
                            map.containsKey(ConnectedClient.VERSION),
                            map.containsKey(ConnectedClient.REQUESTS),
                            map.containsKey(ConnectedClient.SSL),
                            map.containsKey(ConnectedClient.CIPHER),
                            map.containsKey(ConnectedClient.PROTOCOL),
                            map.containsKey(ConnectedClient.CLIENT_OPTIONS),
                            map.containsKey(ConnectedClient.USER),
                            map.containsKey(ConnectedClient.DRIVER_NAME),
                            map.containsKey(ConnectedClient.DRIVER_VERSION),
                            map.containsKey(ConnectedClient.KEYSPACE),
                            map.containsKey(ConnectedClient.AUTHENTICATION_MODE),
                            map.containsKey(ConnectedClient.AUTHENTICATION_METADATA)
                            };
                        }
                    }

                    return new boolean[0];
                });

                assertEquals("expected to find the authenticated client's map", 13,
                             mapFieldsAfterAuthentication.length);

                for (int i = 0; i < mapFieldsAfterAuthentication.length; i++)
                    assertTrue("expected field at index " + i + " to be present",
                               mapFieldsAfterAuthentication[i]);
            }
        }
    }

    private static Object[][] waitForClientRows(IInvokableInstance node)
    {
        long deadline = Clock.Global.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        Object[][] rows;

        do
        {
            rows = node.executeInternal(CLIENTS_QUERY);

            if (rows.length > 0)
                return rows;

            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
        }
        while (Clock.Global.nanoTime() < deadline);

        return rows;
    }

    private static Object[][] waitForAuthenticatedClient(IInvokableInstance node)
    {
        long deadline = Clock.Global.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        Object[][] rows;

        do
        {
            rows = node.executeInternal(CLIENTS_QUERY);

            if (rows.length == 1 && "cassandra".equals(rows[0][11]))
                return rows;

            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
        }
        while (Clock.Global.nanoTime() < deadline);

        return rows;
    }
}
