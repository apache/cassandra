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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Server.ConnectionTracker;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class ConnectionTrackerTest
{

    private static final Predicate<ServerConnection> PREDICATE_TRUE = (__) -> true;

    @BeforeClass
    public static void beforeClass()
    {
        // perform daemon initialization to intialize core components (like authenticator) that ConnectionTracker
        // depends on.
        DatabaseDescriptor.daemonInitialization();
    }

    private void registerConnections(ConnectionTracker tracker, int count)
    {
        registerConnections(tracker, count, (addr) -> null);
    }

    private void registerConnections(ConnectionTracker tracker, int count, Function<InetSocketAddress, AuthenticatedUser> authenticatedUserGenerator)
    {
        for (int i = 0; i < count; i++)
        {
            InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 9040 + i);

            Channel c = new EmbeddedChannelWithSocketAddress(socketAddress);
            AuthenticatedUser user = authenticatedUserGenerator.apply(socketAddress);
            ServerConnection connection = createAuthenticatedConnection(c, user);
            c.attr(Connection.attributeKey).set(connection);

            tracker.addConnection(c, connection);
        }
    }

    @Test
    public void testCountConnectedClients()
    {
        final String ODD_USER = "odd";
        final String EVEN_USER = "even";
        ConnectionTracker connectionTracker = new ConnectionTracker(() -> true);
        registerConnections(connectionTracker, 6, (addr) -> {
            // if port divisible by 10, return 0, this will be treated as an anonymous user.
            if (addr.getPort() % 10 == 0)
            {
                return null;
            }
            return new AuthenticatedUser(addr.getPort() % 2 == 0 ? EVEN_USER : ODD_USER);
        });

        // Validate that countConnectedClients by predicate returns the proper count.

        // Should be 6 total connections when all matching this predicate
        assertEquals(6, connectionTracker.countConnectedClients(PREDICATE_TRUE));

        // Three connections should have a port > 9042.
        assertEquals(3, connectionTracker.countConnectedClients((conn) -> {
            int port = conn.getClientState().getRemoteAddress().getPort();
            return port > 9042;
        }));

        // Zero connections using ssl.
        assertEquals(0, connectionTracker.countConnectedClients(ServerConnection::isSSL));

        // Verify countConnectedClientsByUser appropriately counts by user.

        // Expect 2 users authenticated as 'even' {2, 4}, 3 as 'odd' {1, 3, 5}, one anonymous {0}.
        assertEquals(ImmutableMap.of(EVEN_USER, 2, ODD_USER, 3, "anonymous", 1), connectionTracker.countConnectedClientsByUser());
    }

    @Test
    public void testShouldNotCountChannelsMissingConnectionAttribute()
    {
        ConnectionTracker connectionTracker = new ConnectionTracker(() -> true);
        registerConnections(connectionTracker, 10);

        // Given a ServerConnection registered that lacks a 'Connection.attributeKey' attribute, it should
        // be skipped over when counting client connections.
        InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042);
        Channel c = new EmbeddedChannelWithSocketAddress(socketAddress);
        Connection connection = new ServerConnection(c, ProtocolVersion.V5, NO_OP_TRACKER);
        // intentionally omit setting Connection attribute.
        connectionTracker.addConnection(c, connection);

        // Expect the 10 existing connections, omitting the one added without a connection attribute.
        assertEquals(10, connectionTracker.countConnectedClients(PREDICATE_TRUE));
        assertEquals(Collections.singletonMap("anonymous", 10), connectionTracker.countConnectedClientsByUser());
    }

    @Test
    public void testShouldNotCountNonServerConnection()
    {
        ConnectionTracker connectionTracker = new ConnectionTracker(() -> true);
        registerConnections(connectionTracker, 10);

        // Given a Connection registered that is not a 'ServerConnection' it should be skipped over when counting client
        // connections.
        InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042);
        Channel c = new EmbeddedChannelWithSocketAddress(socketAddress);
        Connection connection = new Connection(c, ProtocolVersion.V5, NO_OP_TRACKER);
        c.attr(Connection.attributeKey).set(connection);
        connectionTracker.addConnection(c, connection);

        // Expect the 10 existing connections, omitting the one that isn't a ServerConnection.
        assertEquals(10, connectionTracker.countConnectedClients(PREDICATE_TRUE));
        assertEquals(Collections.singletonMap("anonymous", 10), connectionTracker.countConnectedClientsByUser());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShouldThrowThrowable()
    {
        ConnectionTracker connectionTracker = new ConnectionTracker(() -> true);
        registerConnections(connectionTracker, 10);

        // Mock a ServerConnection such that when you attempt to access its connection attribute, an unhandled throwable
        // is raised.
        InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042);
        Channel c = new EmbeddedChannelWithSocketAddress(socketAddress);
        Channel spyChannel = Mockito.spy(c);
        Connection connection = new ServerConnection(c, ProtocolVersion.V5, NO_OP_TRACKER);
        c.attr(Connection.attributeKey).set(connection);
        Mockito.when(spyChannel.attr(Connection.attributeKey)).thenThrow(new IllegalArgumentException("Unexpected behavior"));
        connectionTracker.addConnection(spyChannel, connection);

        // When calling countConnectedClients, we expect that expection to be propagated as we can't get a correct calculation.
        connectionTracker.countConnectedClients(PREDICATE_TRUE);
    }

    /**
     * @return a {@link ServerConnection} whose {@link ClientState} has the given {@link AuthenticatedUser}
     */
    private ServerConnection createAuthenticatedConnection(Channel channel, AuthenticatedUser user)
    {
        ServerConnection connection = new ServerConnection(channel, ProtocolVersion.V5, new Connection.Tracker()
        {
            @Override
            public void addConnection(Channel ch, Connection connection)
            {

            }

            @Override
            public boolean isRunning()
            {
                return false;
            }
        });

        if (user == null)
        {
            return connection;
        }

        ClientState state = connection.getClientState();

        ClientState spyState = Mockito.spy(state);
        Mockito.when(spyState.getUser()).thenReturn(user);

        ServerConnection spyConnection = Mockito.spy(connection);
        Mockito.when(spyConnection.getClientState()).thenReturn(spyState);

        return spyConnection;
    }

    /**
     * An embedded channel that uses the given {@link SocketAddress}'s string as its channel id, and {@link #remoteAddress()}
     * returns the provided address.
     *
     * This is needed because {@link ServerConnection} expects a {@link Channel} with a {@link SocketAddress}
     */
    private static class EmbeddedChannelWithSocketAddress extends EmbeddedChannel
    {
        private final SocketAddress socketAddress;

        EmbeddedChannelWithSocketAddress(SocketAddress socketAddress)
        {
            super(createChannelId(socketAddress));
            this.socketAddress = socketAddress;
        }

        private static ChannelId createChannelId(SocketAddress socketAddress)
        {
            return new ChannelId()
            {
                @Override
                public String asShortText()
                {
                    return socketAddress.toString();
                }

                @Override
                public String asLongText()
                {
                    return asShortText();
                }

                @Override
                public int compareTo(ChannelId o)
                {
                    return this.asShortText().compareTo(this.asShortText());
                }
            };
        }

        @Override
        public SocketAddress remoteAddress()
        {
            return this.socketAddress;
        }
    }

    private final Connection.Tracker NO_OP_TRACKER =  new Connection.Tracker()
    {
        @Override
        public void addConnection(Channel ch, Connection connection)
        {
        }

        @Override
        public boolean isRunning()
        {
            return true;
        }
    };
}
