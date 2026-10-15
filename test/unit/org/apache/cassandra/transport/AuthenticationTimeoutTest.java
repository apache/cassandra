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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.transport.messages.AuthResponse;
import org.apache.cassandra.transport.messages.AuthSuccess;
import org.apache.cassandra.transport.messages.AuthenticateMessage;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.StartupMessage;

import static org.apache.cassandra.auth.AuthTestUtils.getToken;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AuthenticationTimeoutTest extends CQLTester
{
    private static final long AUTH_TIMEOUT_MS = 2000L;
    private static long originalAuthTimeout;

    @BeforeClass
    public static void setUp()
    {
        originalAuthTimeout = DatabaseDescriptor.nativeTransportAuthTimeout();
        DatabaseDescriptor.setNativeTransportAuthTimeout(AUTH_TIMEOUT_MS);
        requireAuthentication();
        requireNetwork();
    }

    @AfterClass
    public static void tearDown()
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
            assertTrue(client.channel.isOpen());

            client.channel.writeAndFlush(new StartupMessage(
            java.util.Collections.singletonMap(StartupMessage.CQL_VERSION, "3.0.0")));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (client.channel.isOpen() && System.nanoTime() < deadline)
                Thread.sleep(25);

            assertFalse("Connection should close when authentication is not completed",
                        client.channel.isOpen());
        }
    }

    @Test
    public void testSuccessfulAuthenticationDoesNotTimeout() throws Exception
    {
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            client.establishConnection();

            StartupMessage startup = new StartupMessage(
            Collections.singletonMap(StartupMessage.CQL_VERSION,
                                     QueryProcessor.CQL_VERSION.toString()));

            Message.Response startupResponse = client.execute(startup);

            assertTrue("Expected AUTHENTICATE response",
                       startupResponse instanceof AuthenticateMessage);

            Message.Response authResponse =
            client.execute(new AuthResponse(getToken("cassandra", "cassandra")));

            assertTrue("Expected AUTH_SUCCESS response",
                       authResponse instanceof AuthSuccess);

            Thread.sleep(AUTH_TIMEOUT_MS + 500);

            assertTrue("Authenticated connection must remain open after the deadline",
                       client.channel.isOpen());

            client.execute("SELECT * FROM system.local",
                           org.apache.cassandra.db.ConsistencyLevel.ONE);
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

            assertTrue("Connection should remain open when authentication timeout is disabled",
                       client.channel.isOpen());
        }
        finally
        {
            DatabaseDescriptor.setNativeTransportAuthTimeout(AUTH_TIMEOUT_MS);
        }
    }

    @Test
    public void testUnauthenticatedActivityDoesNotResetTimeout() throws Exception
    {
        try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostAddress(), nativePort).build())
        {
            client.establishConnection();

            StartupMessage startup = new StartupMessage(
            Collections.singletonMap(StartupMessage.CQL_VERSION,
                                     QueryProcessor.CQL_VERSION.toString()));

            Message.Response startupResponse = client.execute(startup);

            assertTrue("Expected AUTHENTICATE response",
                       startupResponse instanceof AuthenticateMessage);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AUTH_TIMEOUT_MS);
            long nextAttempt = System.nanoTime();

            while (client.channel.isOpen() && System.nanoTime() < deadline)
            {
                if (System.nanoTime() >= nextAttempt)
                {
                    Message.Response response =
                    client.execute(new AuthResponse(getToken("cassandra", "badpw")), false);

                    assertTrue("Expected authentication failure",
                               response instanceof ErrorMessage);

                    nextAttempt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(250);
                }

                if (client.channel.isOpen())
                    Thread.sleep(25);
            }

            assertFalse("Authentication attempts must not keep the connection alive indefinitely",
                        client.channel.isOpen());
        }
    }
}
