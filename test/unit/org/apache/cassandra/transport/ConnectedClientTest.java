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
import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator.AuthenticationMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;

import static org.mockito.Mockito.mock;
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
    public void asMapEscapesOptionsAndAuthenticationMetadataAlike()
    {
        ServerConnection connection = mock(ServerConnection.class);
        Channel channel = mock(Channel.class);
        ClientState state = mock(ClientState.class);
        when(connection.getClientState()).thenReturn(state);
        when(connection.channel()).thenReturn(channel);
        when(connection.getVersion()).thenReturn(ProtocolVersion.V5);
        when(channel.pipeline()).thenReturn(mock(ChannelPipeline.class));
        when(state.getRemoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9042));
        when(state.getClientOptions()).thenReturn(Optional.of(Map.of("k,1", "v=1")));
        when(state.getUser()).thenReturn(new AuthenticatedUser("test",
                                                               AuthenticationMode.MTLS,
                                                               Map.<String, Object>of("identity", "spiffe://a,b=c\\d")));

        Map<String, String> map = new ConnectedClient(connection).asMap();

        Assert.assertEquals("k\\,1=v\\=1", map.get(ConnectedClient.CLIENT_OPTIONS));
        Assert.assertEquals("identity=spiffe://a\\,b\\=c\\\\d", map.get(ConnectedClient.AUTHENTICATION_METADATA));
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
}
