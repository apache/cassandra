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

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.transport.messages.AuthResponse;
import org.apache.cassandra.transport.messages.AuthSuccess;
import org.apache.cassandra.transport.messages.AuthenticateMessage;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.transport.messages.SupportedMessage;

import io.netty.channel.Channel;

import static org.apache.cassandra.auth.AuthTestUtils.getToken;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AuthenticationTimeoutHandlerTest extends CQLTester
{
    private static final long AUTH_TIMEOUT_MS = 2000L;
    private static long originalAuthTimeout;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        requireAuthentication();
        requireNetwork();
    }

    @Before
    public void setUp()
    {
        originalAuthTimeout = DatabaseDescriptor.nativeTransportAuthTimeout();
        DatabaseDescriptor.setNativeTransportAuthTimeout(AUTH_TIMEOUT_MS);
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setNativeTransportAuthTimeout(originalAuthTimeout);
    }

    @Test
    public void testUnauthenticatedConnectionTimesOut() throws Exception
    {
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            client.establishConnection();
            assertTrue(client.channel.isOpen());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (client.channel.isOpen() && System.nanoTime() < deadline)
                Thread.sleep(25);
            assertFalse("Unauthenticated connection should be closed after the timeout",
                        client.channel.isOpen());
        }
    }

    @Test
    public void testAuthenticationTimeout() throws Exception
    {
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            client.establishConnection();
            StartupMessage startup = new StartupMessage(Collections.singletonMap(StartupMessage.CQL_VERSION, QueryProcessor.CQL_VERSION.toString()));
            Message.Response response = client.execute(startup);
            assertTrue("Expected AUTHENTICATE response", response instanceof AuthenticateMessage);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (client.channel.isOpen() && System.nanoTime() < deadline)
                Thread.sleep(25);
            assertFalse("Connection should close when authentication is not completed", client.channel.isOpen());
        }
    }

    @Test
    public void testSuccessfulAuthenticationDoesNotTimeout() throws Exception
    {
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            client.establishConnection();
            StartupMessage startup = new StartupMessage(Collections.singletonMap(StartupMessage.CQL_VERSION, QueryProcessor.CQL_VERSION.toString()));
            Message.Response startupResponse = client.execute(startup);
            assertTrue("Expected AUTHENTICATE response", startupResponse instanceof AuthenticateMessage);
            Message.Response authResponse = client.execute(new AuthResponse(getToken("cassandra", "cassandra")));
            assertTrue("Expected AUTH_SUCCESS response", authResponse instanceof AuthSuccess);
            Thread.sleep(AUTH_TIMEOUT_MS + 500);
            assertTrue("Authenticated connection must remain open after the deadline", client.channel.isOpen());
            client.execute("SELECT * FROM system.local", ConsistencyLevel.ONE);
            assertAuthenticationTimeoutHandlerRemoved(client);
        }
    }


    @Test
    public void testAuthenticationTimeoutDisabled() throws Exception
    {
        DatabaseDescriptor.setNativeTransportAuthTimeout(0);
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            client.establishConnection();
            assertTrue("Connection should initially be open", client.channel.isOpen());
            Thread.sleep(AUTH_TIMEOUT_MS + 500);
            assertTrue("Connection should remain open when authentication timeout is disabled", client.channel.isOpen());
        }
    }

    @Test
    public void testUnauthenticatedActivityDoesNotResetTimeout() throws Exception
    {
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            client.establishConnection();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            long nextAttempt = System.nanoTime();
            while (client.channel.isOpen() && System.nanoTime() < deadline)
            {
                if (System.nanoTime() >= nextAttempt)
                {
                    Message.Response response = client.execute(new OptionsMessage());
                    assertTrue("Expected SUPPORTED response to OPTIONS", response instanceof SupportedMessage);
                    assertTrue("Connection should remain open before the timeout", client.channel.isOpen());
                    nextAttempt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(250);
                }
                if (client.channel.isOpen())
                    Thread.sleep(25);
            }
            assertFalse("Unauthenticated activity must not prevent the timeout", client.channel.isOpen());
        }
    }

    public static void assertAuthenticationTimeoutHandlerRemoved(SimpleClient client)
    {
        Channel serverChannel = null;
        for (Channel channel : allChannels())
        {
            Connection connection = channel.attr(Connection.attributeKey).get();
            if (connection instanceof ServerConnection && client.channel.localAddress().equals(channel.remoteAddress()))
            {
                serverChannel = channel;
                break;
            }
        }

        assertNotNull("Could not find the server-side channel for the client", serverChannel);
        assertNull("Authentication timeout handler should be removed after authentication", serverChannel.pipeline().get(AuthenticationTimeoutHandler.class));
    }
}