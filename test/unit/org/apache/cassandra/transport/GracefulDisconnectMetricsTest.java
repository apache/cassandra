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

package org.apache.cassandra.transport;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.AllowAllAuthorizer;
import org.apache.cassandra.auth.AllowAllNetworkAuthorizer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.transport.messages.RegisterMessage;

import static org.assertj.core.api.Assertions.assertThat;

public class GracefulDisconnectMetricsTest
{

    private InetAddress address;
    private int port;

    @Before
    public void setup() throws Exception
    {
        DatabaseDescriptor.toolInitialization();
        DatabaseDescriptor.setAuthenticator(new AllowAllAuthenticator());
        DatabaseDescriptor.setAuthorizer(new AllowAllAuthorizer());
        DatabaseDescriptor.setNetworkAuthorizer(new AllowAllNetworkAuthorizer());

        DatabaseDescriptor.getRawConfig().graceful_disconnect_enabled = true;
        DatabaseDescriptor.setGracefulDisconnectGracePeriod(1000);

        address = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0))
        {
            port = serverSocket.getLocalPort();
        }
        Thread.sleep(250);
    }

    private Server startServer()
    {
        Server server = new Server.Builder().withHost(address).withPort(port).build();
        server.start();
        ClientMetrics.instance.init(server);
        return server;
    }

    private SimpleClient registeredClient(boolean cooperative) throws Exception
    {
        SimpleClient.Builder builder = SimpleClient.builder(address.getHostAddress(), port)
                                                   .protocolVersion(ProtocolVersion.V5);
        if (!cooperative)
            builder.ignoreGracefulDisconnect();

        SimpleClient client = builder.build();
        client.connect(false);
        client.execute(new RegisterMessage(Collections.singletonList(Event.Type.GRACEFUL_DISCONNECT)));
        return client;
    }

    @Test
    public void cooperativeClientDrainsAndMetricsReturnToBaseline() throws Exception
    {
        Server server = startServer();
        long baselineForced = ClientMetrics.instance.forcedDisconnects.getCount();
        int baselineDraining = ClientMetrics.instance.connectionsDraining.get();

        try (SimpleClient client = registeredClient(true))
        {
            server.stop();

            assertThat(ClientMetrics.instance.connectionsDraining.get())
            .as("connectionsDraining should settle back to its pre-drain baseline")
            .isEqualTo(baselineDraining);
            assertThat(ClientMetrics.instance.forcedDisconnects.getCount())
            .as("a cooperative disconnect must not be counted as forced")
            .isEqualTo(baselineForced);
        }
    }

    @Test
    public void uncooperativeClientIsCountedDrainingThenForceDisconnected() throws Exception
    {
        Server server = startServer();
        long baselineForced = ClientMetrics.instance.forcedDisconnects.getCount();
        int baselineDraining = ClientMetrics.instance.connectionsDraining.get();

        try (SimpleClient ignored = registeredClient(false))
        {
            Thread stopper = new Thread(server::stop);
            stopper.start();

            Awaitility.await()
                      .atMost(2, TimeUnit.SECONDS)
                      .until(() -> ClientMetrics.instance.connectionsDraining.get() == baselineDraining + 1);

            stopper.join(10_000);

            assertThat(ClientMetrics.instance.connectionsDraining.get())
            .as("connectionsDraining should settle back to baseline once the channel is force-closed")
            .isEqualTo(baselineDraining);
            assertThat(ClientMetrics.instance.forcedDisconnects.getCount())
            .as("the uncooperative client should be counted as a forced disconnect")
            .isEqualTo(baselineForced + 1);
        }
    }
}
