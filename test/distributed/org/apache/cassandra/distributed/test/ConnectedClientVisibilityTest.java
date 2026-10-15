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
import static org.junit.Assert.assertTrue;

public class ConnectedClientVisibilityTest extends TestBaseImpl
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /**
     * A client that has sent STARTUP but not finished authenticating must not appear in system_views.clients (its
     * options are attacker-controlled before authentication, CASSANDRA-21755) even though it is tracked server-side.
     * Once it completes authentication on the same connection, it becomes visible.
     */
    @Test
    public void clientIsListedOnlyAfterAuthentication() throws IOException
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                        .set("authenticator", "PasswordAuthenticator"))
                                      .start())
        {
            IInvokableInstance node = cluster.get(1);

            try (SimpleClient client = SimpleClient.builder("127.0.0.1", 9042).build())
            {
                // sends STARTUP only; with PasswordAuthenticator the server replies AUTHENTICATE and the connection
                // stays open with no logged-in user
                client.connect(false);

                // the connection is registered server-side, and nothing is authenticated yet
                int[] serverSide = node.callOnInstance(() -> {
                    long deadline = Clock.Global.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    while (ClientMetrics.instance.allConnectedClients().isEmpty() && Clock.Global.nanoTime() < deadline)
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));

                    int total = ClientMetrics.instance.allConnectedClients().size();
                    int loggedIn = 0;
                    for (ConnectedClient c : ClientMetrics.instance.allConnectedClients())
                        if (c.loggedIn())
                            loggedIn++;
                    return new int[]{ total, loggedIn };
                });

                assertTrue("expected the connection to be registered server-side", serverSide[0] >= 1);
                assertEquals("no connected client should be authenticated yet", 0, serverSide[1]);

                // before authentication it must not surface in the operator-facing view
                assertEquals("unauthenticated client must not be listed in system_views.clients",
                                    0, node.executeInternal("SELECT address FROM system_views.clients").length);

                // finish authenticating on the same connection with the default superuser
                waitForExistingRoles(node);
                Response authResult = client.execute(new AuthResponse(getToken("cassandra", "cassandra")));
                assertTrue("expected AUTH_SUCCESS, got: " + authResult, authResult instanceof AuthSuccess);

                // ... and now the same connection is visible
                long deadline = Clock.Global.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (node.executeInternal("SELECT address FROM system_views.clients").length == 0
                       && Clock.Global.nanoTime() < deadline)
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                assertEquals("authenticated client must be listed in system_views.clients",
                                    1, node.executeInternal("SELECT address FROM system_views.clients").length);
            }
        }
    }
}
