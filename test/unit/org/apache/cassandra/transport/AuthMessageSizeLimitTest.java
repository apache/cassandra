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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Authenticator;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.PlainTextAuthProvider;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.transport.messages.AuthResponse;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.assertj.core.api.Assertions;

public class AuthMessageSizeLimitTest extends NativeProtocolLimitsTestBase
{
    private static final int TOO_BIG_MULTI_FRAME_AUTH_MESSAGE_SIZE = 2 * FrameEncoder.Payload.MAX_SIZE;

    // set MAX_CQL_MESSAGE_SIZE bigger than TOO_BIG_MULTI_FRAME_AUTH_MESSAGE_SIZE to ensure what the auth message size check is more restrictive
    private static final int MAX_CQL_MESSAGE_SIZE = TOO_BIG_MULTI_FRAME_AUTH_MESSAGE_SIZE * 2;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.setNativeTransportReceiveQueueCapacityInBytes(1);
        DatabaseDescriptor.setNativeTransportMaxRequestDataInFlightPerIpInBytes(MAX_CQL_MESSAGE_SIZE);
        DatabaseDescriptor.setNativeTransportConcurrentRequestDataInFlightInBytes(MAX_CQL_MESSAGE_SIZE);
        DatabaseDescriptor.setNativeTransportMaxMessageSizeInBytes(MAX_CQL_MESSAGE_SIZE);
        requireNetwork();
        requireAuthentication();
    }

    @Before
    public void setLimits()
    {
        ClientResourceLimits.setGlobalLimit(MAX_CQL_MESSAGE_SIZE);
        ClientResourceLimits.setEndpointLimit(MAX_CQL_MESSAGE_SIZE);
    }

    @Test
    public void sendSmallAuthMessage()
    {
        doTest((client) ->
               {
                   AuthResponse authResponse = createAuthMessage("cassandra", "cassandra");
                   client.execute(authResponse);
                   createTable(client);

                   int valueLessThanMessageMaxSize = MAX_CQL_MESSAGE_SIZE - 500;
                   QueryMessage queryMessage = queryMessage(valueLessThanMessageMaxSize);
                   client.execute(queryMessage);
               }
        );
    }

    @Test
    public void sendTooBigAuthMultiFrameMessage()
    {
        doTest((client) ->
               {
                   AuthResponse authResponse = createAuthMessage("cassandra", createIncorrectLongPassword(TOO_BIG_MULTI_FRAME_AUTH_MESSAGE_SIZE));
                   Assertions.assertThatThrownBy(() -> client.execute(authResponse))
                             .hasCauseInstanceOf(ProtocolException.class)
                             .hasMessageContaining(CQLMessageHandler.MULTI_FRAME_AUTH_ERROR_MESSAGE_PREFIX);
                   Util.spinAssertEquals(false, () -> client.connection.channel().isOpen(), 10);
               }
        );
    }

    private static String createIncorrectLongPassword(int length)
    {
        StringBuilder password = new StringBuilder(length);
        for (int i = 0; i < length; i++)
            password.append('a');
        return password.toString();
    }

    private AuthResponse createAuthMessage(String username, String password)
    {
        PlainTextAuthProvider authProvider = new PlainTextAuthProvider(username, password);
        Authenticator authenticator = authProvider.newAuthenticator((EndPoint) null, null);
        return new AuthResponse(authenticator.initialResponse());
    }
}
