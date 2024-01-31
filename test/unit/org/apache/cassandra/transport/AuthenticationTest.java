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
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.AuthResponse;
import org.apache.cassandra.transport.messages.AuthSuccess;
import org.apache.cassandra.transport.messages.AuthenticateMessage;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.StartupMessage;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.cassandra.auth.AuthTestUtils.getToken;
import static org.apache.cassandra.transport.messages.StartupMessage.CQL_VERSION;
import static org.junit.Assert.fail;
import static org.psjava.util.AssertStatus.assertTrue;

/**
 * Verifies that {@link StartupMessage#execute(QueryState, long)} will prompt a client for authentication if an
 * authenticator is configured.  For this test {@link org.apache.cassandra.auth.PasswordAuthenticator} is used.
 */
public class AuthenticationTest extends CQLTester
{

    @BeforeClass
    public static void setup()
    {
        requireNetwork();
        requireAuthentication();
    }

    @Before
    public void initNetwork()
    {
        reinitializeNetwork();
    }

    @Test
    public void testSuccessfulAuth()
    {
        testAuthResponse((client) -> {
            AuthResponse authResponse = new AuthResponse(getToken("cassandra", "cassandra"));
            Message.Response authResponseResponse = client.execute(authResponse);

            if (!(authResponseResponse instanceof AuthSuccess))
            {
                fail("Expected an AUTH_SUCCESS in response to a AUTH_RESPONSE with default credentials, got: " + authResponseResponse);
            }
        });
    }

    @Test
    public void testUnsuccessfulAuth()
    {
        testAuthResponse((client) -> {
            AuthResponse authResponse = new AuthResponse(getToken("cassandra", "badpw"));
            Message.Response response = client.execute(authResponse, false);

            if (response instanceof ErrorMessage)
            {
                ErrorMessage errorMessage = (ErrorMessage) response;
                assertTrue(errorMessage.error instanceof AuthenticationException, "Expected an AuthenticationException, got: " + errorMessage.error);
            }
            else
            {
                fail("Expected an ErrorMessage but got: " + response);
            }
        });
    }

    public void testAuthResponse(Consumer<SimpleClient> testFn)
    {
        SimpleClient.Builder builder = SimpleClient.builder(nativeAddr.getHostAddress(), nativePort);
        try (SimpleClient client = builder.build())
        {
            client.establishConnection();

            // Send a StartupMessage
            StartupMessage startup = new StartupMessage(ImmutableMap.of(CQL_VERSION, QueryProcessor.CQL_VERSION.toString()));
            Message.Response startupResponse = client.execute(startup);

            if (!(startupResponse instanceof AuthenticateMessage))
            {
                fail("Expected an AUTHENTICATE in response to a STARTUP, got: " + startupResponse);
            }

            testFn.accept(client);
        }
        catch (IOException e)
        {
            fail("Error establishing connection");
        }
    }
}
