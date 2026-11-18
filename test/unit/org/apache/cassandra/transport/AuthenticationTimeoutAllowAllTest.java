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
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.transport.messages.ReadyMessage;
import org.apache.cassandra.transport.messages.StartupMessage;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertEquals;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the authentication timeout with {@link org.apache.cassandra.auth.AllowAllAuthenticator}.
 * This path requires separate test because connections reach READY without
 * an authentication exchange, so authentication-completion handling differs.
 */
public class AuthenticationTimeoutAllowAllTest extends CQLTester
{
    // generous, the server has to answer STARTUP within this time even on a slow machine
    private static final long AUTH_TIMEOUT_MS = 30_000L;
    private static final long AWAIT_SECONDS = 30;
    private static long originalAuthTimeout;

    @BeforeClass
    public static void setUp()
    {
        originalAuthTimeout = DatabaseDescriptor.nativeTransportAuthTimeout();
        DatabaseDescriptor.setNativeTransportAuthTimeout(AUTH_TIMEOUT_MS);
        requireNetwork();
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setNativeTransportAuthTimeout(originalAuthTimeout);
    }

    @Test
    public void testHandlerIsRemovedWhenStartupTransitionsDirectlyToReady() throws Exception
    {
        long timeoutsBefore = ClientMetrics.instance.getAuthTimeoutCount();

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            client.establishConnection();
            StartupMessage startup = new StartupMessage(Collections.singletonMap(StartupMessage.CQL_VERSION, QueryProcessor.CQL_VERSION.toString()));
            Message.Response response = client.execute(startup);
            assertTrue("Expected READY response", response instanceof ReadyMessage);

            // without the handler there is no timeout task left which could close the connection
            awaitAuthenticationTimeoutHandlerRemoved(client);

            assertTrue("Connection should remain open after STARTUP transitions directly to READY",
                       client.channel.isOpen());
        }

        assertEquals("AuthTimeout metric should not increment for AllowAllAuthenticator",
                     timeoutsBefore, ClientMetrics.instance.getAuthTimeoutCount());
    }

    /**
     * The handler is removed on the event loop of the server after the response which completes authentication was
     * sent, so the client may see that response before the handler is gone.
     */
    private static void awaitAuthenticationTimeoutHandlerRemoved(SimpleClient client)
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

        ChannelPipeline pipeline = serverChannel.pipeline();
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
               .untilAsserted(() -> assertNull("Authentication timeout handler should be removed after authentication",
                                               pipeline.get(AuthenticationTimeoutHandler.class)));
    }
}
