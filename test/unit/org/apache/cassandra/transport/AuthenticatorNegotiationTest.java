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

import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.transport.messages.AuthResponse;
import org.apache.cassandra.transport.messages.AuthSuccess;
import org.apache.cassandra.transport.messages.AuthenticateMessage;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.transport.messages.SupportedMessage;

import static org.apache.cassandra.auth.AuthTestUtils.getToken;
import static org.apache.cassandra.transport.messages.StartupMessage.AUTHENTICATORS;
import static org.apache.cassandra.transport.messages.StartupMessage.CQL_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests the protocol flow for a server that is configured to support authenticator negotiation, including
 * compatibility scenarios defined in CEP-50. Covers the compatibility matrix for negotiating and non-negotiating
 * clients.
 */
public class AuthenticatorNegotiationTest extends CQLTester
{
    @BeforeClass
    public static void setup()
    {
        requireNetwork();
        requireAuthenticatorNegotiation();
    }

    // Scenario 1: Negotiating client + Negotiating server - OPTIONS/SUPPORTED handshake
    @Test
    public void testFullNegotiationHandshake()
    {
        try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostAddress(), nativePort).build())
        {
            // Negotiating client: send OPTIONS
            client.establishConnection();
            OptionsMessage options = new OptionsMessage();
            Message.Response optionsResponse = client.execute(options);

            assertTrue("Server should respond with SUPPORTED", optionsResponse instanceof SupportedMessage);

            SupportedMessage supported = (SupportedMessage) optionsResponse;
            assertTrue("Negotiating server should include AUTHENTICATORS in SUPPORTED",
                       supported.supported.containsKey(AUTHENTICATORS));
            assertNotNull("AUTHENTICATORS value should not be null",
                          supported.supported.get(AUTHENTICATORS));

            // Client sends STARTUP with AUTHENTICATORS
            StartupMessage startup = new StartupMessage(Map.of(CQL_VERSION, QueryProcessor.CQL_VERSION.toString(),
                                                               AUTHENTICATORS, "Password,MutualTls"));
            Message.Response startupResponse = client.execute(startup);

            assertTrue("Server should respond with AUTHENTICATE after negotiation",
                       startupResponse instanceof AuthenticateMessage);
            assertEquals(((AuthenticateMessage) startupResponse).authenticator,
                         AuthTestUtils.LocalPasswordAuthenticator.class.getName());

            // Complete authentication with server's preferred authenticator (Password)
            AuthResponse authResponse = new AuthResponse(getToken("cassandra", "cassandra"));
            Message.Response authResult = client.execute(authResponse);

            assertTrue("Authentication should succeed with default authenticator",
                       authResult instanceof AuthSuccess);
        }
        catch (IOException e)
        {
            fail("Error establishing connection: " + e.getMessage());
        }
    }

    // Scenario 2: "Short" negotiation - client skips sending Options message and immediately sends STARTUP with
    // a list of authenticators it can use.
    @Test
    public void testShortNegotiationHandshake()
    {
        try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostAddress(), nativePort).build())
        {
            client.establishConnection();
            StartupMessage startup = new StartupMessage(Map.of(CQL_VERSION, QueryProcessor.CQL_VERSION.toString(),
                                                               AUTHENTICATORS, "Password,MutualTls,Unauthenticated"));
            Message.Response startupResponse = client.execute(startup);

            assertTrue("Server should respond with AUTHENTICATE", startupResponse instanceof AuthenticateMessage);
            assertEquals(((AuthenticateMessage) startupResponse).authenticator,
                         AuthTestUtils.LocalPasswordAuthenticator.class.getName());

            AuthResponse authResponse = new AuthResponse(getToken("cassandra", "cassandra"));
            Message.Response authResult = client.execute(authResponse);

            assertTrue("Authentication should succeed with correct credentials",
                       authResult instanceof AuthSuccess);
        }
        catch (IOException e)
        {
            fail("Error establishing connection: " + e.getMessage());
        }
    }

    // Scenario 3: Non-negotiating client + Negotiating server
    // Client sends STARTUP without AUTHENTICATORS option, server falls back to default authenticator
    @Test
    public void testNonNegotiatingClientWithNegotiatingServer()
    {
        try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostAddress(), nativePort).build())
        {
            // Non-negotiating client: STARTUP without AUTHENTICATORS
            client.establishConnection();
            StartupMessage startup = new StartupMessage(Map.of(CQL_VERSION, QueryProcessor.CQL_VERSION.toString()));
            Message.Response startupResponse = client.execute(startup);

            assertTrue("Server should respond with AUTHENTICATE for non-negotiating client",
                       startupResponse instanceof AuthenticateMessage);
            assertEquals(((AuthenticateMessage) startupResponse).authenticator,
                         AuthTestUtils.LocalDefaultPasswordAuthenticator.class.getName());

            // Complete authentication with default authenticator (Password)
            AuthResponse authResponse = new AuthResponse(getToken("cassandra", "cassandra"));
            Message.Response authResult = client.execute(authResponse);

            assertTrue("Authentication should succeed with default authenticator",
                       authResult instanceof AuthSuccess);
        }
        catch (IOException e)
        {
            fail("Error establishing connection: " + e.getMessage());
        }
    }

    // Scenario 4: Negotating client + Negotiating server
    // Full negotiation but no matching authenticators: falls back to default authenticator
    @Test
    public void testFullNegotiationNoMatch()
    {
        try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostAddress(), nativePort).build())
        {
            // Client offers authenticators the server doesn't support
            client.establishConnection();
            StartupMessage startup = new StartupMessage(Map.of(CQL_VERSION, QueryProcessor.CQL_VERSION.toString(),
                                                               AUTHENTICATORS, "Kerberos,OAuth,JWT"));
            Message.Response startupResponse = client.execute(startup, false);

            assertTrue("Server should respond with AUTHENTICATE using default authenticator when no match",
                      startupResponse instanceof AuthenticateMessage);
            assertEquals(((AuthenticateMessage) startupResponse).authenticator,
                         AuthTestUtils.LocalDefaultPasswordAuthenticator.class.getName());

            // Complete authentication with default authenticator (Password)
            AuthResponse authResponse = new AuthResponse(getToken("cassandra", "cassandra"));
            Message.Response authResult = client.execute(authResponse);

            assertTrue("Authentication should succeed with default authenticator",
                       authResult instanceof AuthSuccess);
        }
        catch (IOException e)
        {
            fail("Error establishing connection: " + e.getMessage());
        }
    }

    // Scenario 5: Negotating client + Negotiating server
    // Successful negotiation but failed authentication should result in ERROR to client
    @Test
    public void testNegotiatedAuthenticationFailure()
    {
        try (SimpleClient client = SimpleClient.builder(nativeAddr.getHostAddress(), nativePort).build())
        {
            client.establishConnection();
            StartupMessage startup = new StartupMessage(Map.of(CQL_VERSION, QueryProcessor.CQL_VERSION.toString(),
                                                               AUTHENTICATORS, "Password,MutualTls,Unauthenticated"));
            Message.Response startupResponse = client.execute(startup);

            assertTrue("Server should respond with AUTHENTICATE", startupResponse instanceof AuthenticateMessage);
            assertEquals(((AuthenticateMessage) startupResponse).authenticator,
                         AuthTestUtils.LocalPasswordAuthenticator.class.getName());

            AuthResponse authResponse = new AuthResponse(getToken("cassandra", "badpassword"));
            Message.Response response = client.execute(authResponse, false);

            if (response instanceof ErrorMessage)
            {
                ErrorMessage errorMessage = (ErrorMessage) response;
                assertTrue("Expected AuthenticationException, got: " + errorMessage.error,
                          errorMessage.error instanceof AuthenticationException);
            }
            else
            {
                fail("Expected ErrorMessage but got: " + response);
            }
        }
        catch (IOException e)
        {
            fail("Error establishing connection: " + e.getMessage());
        }
    }
}
