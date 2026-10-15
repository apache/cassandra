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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ConnectedClientTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void loggedInReflectsAuthenticationState()
    {
        ServerConnection connection = mock(ServerConnection.class);
        ClientState state = mock(ClientState.class);
        when(connection.getClientState()).thenReturn(state);
        ConnectedClient client = new ConnectedClient(connection);
        when(state.getUser()).thenReturn(null);
        Assert.assertFalse(client.loggedIn());
        when(state.getUser()).thenReturn(new AuthenticatedUser("test"));
        Assert.assertTrue(client.loggedIn());
    }

    @Test
    public void asMapEscapesOptions()
    {
        ClientState state = mock(ClientState.class);
        ServerConnection connection = serverConnection(state);
        when(state.getRemoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9042));
        when(state.getClientOptions()).thenReturn(Optional.of(Map.of("k,1", "v=1")));
        when(state.getUser()).thenReturn(new AuthenticatedUser("test"));

        Map<String, String> map = new ConnectedClient(connection).asMap();

        Assert.assertEquals("k\\,1=v\\=1", map.get(ConnectedClient.CLIENT_OPTIONS));
    }

    @Test
    public void asMapKeepsKeySetWhileWithholdingClientMetadata()
    {
        ClientState state = mock(ClientState.class);
        ServerConnection connection = serverConnection(state);
        when(state.getRemoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9042));
        when(state.getClientOptions()).thenReturn(Optional.of(Map.of("CQL_VERSION", "3.0.0", "DRIVER_NAME", "evil")));
        when(state.getDriverName()).thenReturn(Optional.of("evil"));
        when(state.getDriverVersion()).thenReturn(Optional.of("1.0"));
        when(state.getUser()).thenReturn(null);

        Map<String, String> map = new ConnectedClient(connection).asMap();

        Assert.assertEquals(Set.of(ConnectedClient.ADDRESS, ConnectedClient.USER, ConnectedClient.VERSION,
                                   ConnectedClient.CLIENT_OPTIONS, ConnectedClient.DRIVER_NAME, ConnectedClient.DRIVER_VERSION,
                                   ConnectedClient.REQUESTS, ConnectedClient.KEYSPACE, ConnectedClient.SSL,
                                   ConnectedClient.CIPHER, ConnectedClient.PROTOCOL),
                            map.keySet());
        Assert.assertEquals("", map.get(ConnectedClient.CLIENT_OPTIONS));
        Assert.assertEquals("undefined", map.get(ConnectedClient.DRIVER_NAME));
        Assert.assertEquals("undefined", map.get(ConnectedClient.DRIVER_VERSION));
        Assert.assertEquals("undefined", map.get(ConnectedClient.USER));
    }

    @Test
    public void testEscapeOptionString()
    {
        // Test backslash escaping first
        Assert.assertEquals("foo\\\\bar", ConnectedClient.escapeOptionString("foo\\bar"));

        // Test comma and equals escaping
        Assert.assertEquals("foo\\,bar\\=baz", ConnectedClient.escapeOptionString("foo,bar=baz"));

        // Test combination of backslash, comma, and equals
        Assert.assertEquals("a\\\\b\\,c\\=d", ConnectedClient.escapeOptionString("a\\b,c=d"));

        // Test null safety
        Assert.assertEquals("", ConnectedClient.escapeOptionString(null));
    }

    /**
     * A real connection rather than a mock, as {@link ConnectedClient#requestCount()} reads the request counter
     * straight from a field of the connection.
     */
    private static ServerConnection serverConnection(ClientState state)
    {
        EmbeddedChannel channel = new EmbeddedChannel()
        {
            @Override
            protected SocketAddress remoteAddress0()
            {
                return new InetSocketAddress("127.0.0.1", 9042);
            }
        };

        ServerConnection connection = spy(new ServerConnection(channel, ProtocolVersion.V5, new NoOpTracker()));
        doReturn(state).when(connection).getClientState();
        return connection;
    }

    private static class NoOpTracker implements Connection.Tracker
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
    }
}
