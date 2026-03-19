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

import java.io.IOException;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.transport.messages.AuthResponse;
import org.apache.cassandra.transport.messages.AuthSuccess;
import org.apache.cassandra.transport.messages.AuthenticateMessage;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.transport.messages.SupportedMessage;

import static org.apache.cassandra.auth.AuthTestUtils.getToken;
import static org.apache.cassandra.transport.messages.StartupMessage.AUTHENTICATORS;
import static org.apache.cassandra.transport.messages.StartupMessage.CQL_VERSION;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests authentication protocol flow when server does NOT support negotiation.
 * Covers scenario 3 from CEP-50: Negotiating client + Non-negotiating server.
 */
public class NonNegotiatedAuthenticationTest extends CQLTester
{
    @BeforeClass
    public static void setup()
    {
        requireNetwork();
        requireAuthentication();
    }

    // Scenario 1: Negotiating client + Non-negotiating server
    // Client sends OPTIONS, server SUPPORTED lacks AUTHENTICATORS, client falls back
    @Test
    public void testNegotiatingClientWithNonNegotiatingServer()
    {
        try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostAddress(), nativePort).build())
        {
            // Negotiating client: send OPTIONS first
            client.establishConnection();
            OptionsMessage options = new OptionsMessage();
            Message.Response optionsResponse = client.execute(options);

            assertTrue("Server should respond with SUPPORTED", optionsResponse instanceof SupportedMessage);

            SupportedMessage supported = (SupportedMessage) optionsResponse;
            assertFalse("Non-negotiating server should not include AUTHENTICATORS in SUPPORTED",
                       supported.supported.containsKey(AUTHENTICATORS));

            // Client detects no negotiation support, sends STARTUP without AUTHENTICATORS
            StartupMessage startup = new StartupMessage(Map.of(CQL_VERSION, QueryProcessor.CQL_VERSION.toString()));
            Message.Response startupResponse = client.execute(startup);

            assertTrue("Server should respond with AUTHENTICATE", startupResponse instanceof AuthenticateMessage);

            // Complete authentication
            AuthResponse authResponse = new AuthResponse(getToken("cassandra", "cassandra"));
            Message.Response authResult = client.execute(authResponse);

            assertTrue("Authentication should succeed", authResult instanceof AuthSuccess);
        }
        catch (IOException e)
        {
            fail("Error establishing connection: " + e.getMessage());
        }
    }


    // Scenario 2: Stubborn negotiating client + Non-negotiating server.
    // Client sends OPTIONS, server SUPPORTED lacks AUTHENTICATORS, client sends authenticators with STARTUP message
    // anyway, server ignores and executes non-negotiating auth flow.
    @Test
    public void testStubbornNegotiatingClientWithNonNegotiatingServer()
    {
        try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostAddress(), nativePort).build())
        {
            // Negotiating client: send OPTIONS first
            client.establishConnection();
            OptionsMessage options = new OptionsMessage();
            Message.Response optionsResponse = client.execute(options);

            assertTrue("Server should respond with SUPPORTED", optionsResponse instanceof SupportedMessage);

            SupportedMessage supported = (SupportedMessage) optionsResponse;
            assertFalse("Non-negotiating server should not include AUTHENTICATORS in SUPPORTED",
                        supported.supported.containsKey(AUTHENTICATORS));

            // Client ignores signal that server lacks negotiation support, sends STARTUP with AUTHENTICATORS
            StartupMessage startup = new StartupMessage(Map.of(CQL_VERSION, QueryProcessor.CQL_VERSION.toString(),
                                                               AUTHENTICATORS, "Password,MutualTls,Unauthenticated"));
            Message.Response startupResponse = client.execute(startup);

            assertTrue("Server should respond with AUTHENTICATE", startupResponse instanceof AuthenticateMessage);

            // Complete authentication
            AuthResponse authResponse = new AuthResponse(getToken("cassandra", "cassandra"));
            Message.Response authResult = client.execute(authResponse);

            assertTrue("Authentication should succeed", authResult instanceof AuthSuccess);
        }
        catch (IOException e)
        {
            fail("Error establishing connection: " + e.getMessage());
        }
    }
}
