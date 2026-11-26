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
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.AuthResponse;
import org.apache.cassandra.transport.messages.AuthSuccess;
import org.apache.cassandra.transport.messages.AuthenticateMessage;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.transport.messages.SupportedMessage;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;

import static org.apache.cassandra.auth.AuthTestUtils.getToken;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The behaviour of the handler in time is tested on an {@link EmbeddedChannel}, whose clock is advanced by the test,
 * so these tests do not sleep and do not depend on how fast the machine is. The tests which connect to a real server
 * cover only the wiring, they wait for an event to happen and never for a period of time to elapse.
 */
public class AuthenticationTimeoutHandlerTest extends CQLTester
{
    private static final long AUTH_TIMEOUT_MS = 2000L;
    // generous, the server has to answer STARTUP and authenticate within this time even on a slow machine
    private static final long SERVER_AUTH_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
    private static final long AWAIT_SECONDS = 30;
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
        DatabaseDescriptor.setNativeTransportAuthTimeout(SERVER_AUTH_TIMEOUT_MS);
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setNativeTransportAuthTimeout(originalAuthTimeout);
    }

    @Test
    public void testConnectionIsClosedAtTheDeadlineAndNotBefore()
    {
        EmbeddedChannel channel = newChannelWithHandler();
        long timeoutsBefore = ClientMetrics.instance.getAuthTimeoutCount();

        advanceTimeBy(channel, AUTH_TIMEOUT_MS - 1);
        assertTrue("Connection should remain open before the timeout", channel.isOpen());
        assertEquals(timeoutsBefore, ClientMetrics.instance.getAuthTimeoutCount());

        advanceTimeBy(channel, 1);
        assertFalse("Unauthenticated connection should be closed after the timeout", channel.isOpen());
        assertEquals(timeoutsBefore + 1, ClientMetrics.instance.getAuthTimeoutCount());
    }

    @Test
    public void testConnectionWhichIsStillAuthenticatingIsClosed()
    {
        EmbeddedChannel channel = newChannelWithHandler();
        setConnectionInStage(channel, ConnectionStage.AUTHENTICATING);

        advanceTimeBy(channel, AUTH_TIMEOUT_MS);

        assertFalse("Connection which is still authenticating should be closed by the timeout", channel.isOpen());
    }

    /**
     * The stage becomes READY on a request thread and the handler is removed later on the event loop. A timeout
     * firing in between must not close a connection which has authenticated.
     */
    @Test
    public void testTimeoutKeepsConnectionWhichIsReadyButHandlerIsNotRemovedYet()
    {
        EmbeddedChannel channel = newChannelWithHandler();
        setConnectionInStage(channel, ConnectionStage.READY);
        long timeoutsBefore = ClientMetrics.instance.getAuthTimeoutCount();

        advanceTimeBy(channel, AUTH_TIMEOUT_MS);

        assertTrue("Connection which is READY should not be closed by the timeout", channel.isOpen());
        assertEquals(timeoutsBefore, ClientMetrics.instance.getAuthTimeoutCount());
        channel.close();
    }

    @Test
    public void testInboundActivityDoesNotResetTimeout()
    {
        EmbeddedChannel channel = newChannelWithHandler();

        // activity until right before the deadline
        for (int i = 0; i < 10 && channel.isOpen(); i++)
        {
            advanceTimeBy(channel, (AUTH_TIMEOUT_MS - 1) / 10);
            assertTrue("Connection should remain open before the timeout", channel.isOpen());
            channel.writeInbound("activity");
        }

        advanceTimeBy(channel, AUTH_TIMEOUT_MS);
        assertFalse("Activity of an unauthenticated connection must not prevent the timeout", channel.isOpen());
    }

    @Test
    public void testCompletedAuthenticationCancelsTimeout()
    {
        AuthenticationTimeoutHandler handler = new AuthenticationTimeoutHandler(AUTH_TIMEOUT_MS);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        handler.markAuthenticationComplete(channel.pipeline().context(handler));

        assertNull("Handler should be removed after authentication", channel.pipeline().get(AuthenticationTimeoutHandler.class));
        channel.advanceTimeBy(AUTH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertEquals("The timeout task should be cancelled", -1, channel.runScheduledPendingTasks());
        assertTrue("Authenticated connection must remain open after the deadline", channel.isOpen());
        channel.close();
    }

    @Test
    public void testClosedConnectionCancelsTimeout()
    {
        EmbeddedChannel channel = newChannelWithHandler();
        long timeoutsBefore = ClientMetrics.instance.getAuthTimeoutCount();

        channel.close();

        channel.advanceTimeBy(AUTH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertEquals("The timeout task should be cancelled", -1, channel.runScheduledPendingTasks());
        assertEquals(timeoutsBefore, ClientMetrics.instance.getAuthTimeoutCount());
    }

    @Test
    public void testHandlerIsInstalledOnlyWhenTimeoutIsEnabled()
    {
        PipelineConfigurator configurator = new PipelineConfigurator(false, false, false, EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED);
        // a connection is created on STARTUP only, which is never sent here
        Connection.Factory connectionFactory = (channel, version) -> { throw new AssertionError(); };

        DatabaseDescriptor.setNativeTransportAuthTimeout(AUTH_TIMEOUT_MS);
        EmbeddedChannel enabled = new EmbeddedChannel();
        configurator.configureInitialPipeline(enabled, connectionFactory);
        assertNotNull(enabled.pipeline().get(AuthenticationTimeoutHandler.class));
        enabled.close();

        DatabaseDescriptor.setNativeTransportAuthTimeout(0);
        EmbeddedChannel disabled = new EmbeddedChannel();
        configurator.configureInitialPipeline(disabled, connectionFactory);
        assertNull(disabled.pipeline().get(AuthenticationTimeoutHandler.class));
        disabled.close();
    }

    @Test
    public void testUnauthenticatedConnectionTimesOut() throws Exception
    {
        DatabaseDescriptor.setNativeTransportAuthTimeout(100);
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            long timeoutsBefore = ClientMetrics.instance.getAuthTimeoutCount();
            client.establishConnection();
            awaitClosed(client, "Unauthenticated connection should be closed after the timeout");
            assertEquals(timeoutsBefore + 1, ClientMetrics.instance.getAuthTimeoutCount());
        }
    }

    @Test
    public void testAuthenticationTimeout() throws Exception
    {
        // long enough for the server to answer STARTUP, the connection is then left in the middle of authentication
        DatabaseDescriptor.setNativeTransportAuthTimeout(AUTH_TIMEOUT_MS);
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            client.establishConnection();
            StartupMessage startup = new StartupMessage(Collections.singletonMap(StartupMessage.CQL_VERSION, QueryProcessor.CQL_VERSION.toString()));
            Message.Response response = client.execute(startup);
            assertTrue("Expected AUTHENTICATE response", response instanceof AuthenticateMessage);
            awaitClosed(client, "Connection should close when authentication is not completed");
        }
    }

    @Test
    public void testSuccessfulAuthenticationRemovesHandler() throws Exception
    {
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            client.establishConnection();
            StartupMessage startup = new StartupMessage(Collections.singletonMap(StartupMessage.CQL_VERSION, QueryProcessor.CQL_VERSION.toString()));
            Message.Response startupResponse = client.execute(startup);
            assertTrue("Expected AUTHENTICATE response", startupResponse instanceof AuthenticateMessage);
            Message.Response authResponse = client.execute(new AuthResponse(getToken("cassandra", "cassandra")));
            assertTrue("Expected AUTH_SUCCESS response", authResponse instanceof AuthSuccess);
            // without the handler there is no timeout task left which could close the connection
            awaitAuthenticationTimeoutHandlerRemoved(client);
            client.execute("SELECT * FROM system.local", ConsistencyLevel.ONE);
        }
    }

    @Test
    public void testTimeoutChangedAtRuntimeAppliesToNewConnectionsOnly() throws Exception
    {
        StorageService.instance.setNativeTransportAuthTimeoutMillis(0);
        assertEquals(0, StorageService.instance.getNativeTransportAuthTimeoutMillis());

        try (SimpleClient establishedBefore = new SimpleClient(nativeAddr.getHostAddress(), nativePort);
             SimpleClient establishedAfter = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            establishedBefore.establishConnection();
            // The server reads the timeout when it initializes the channel, which may happen after the client sees
            // the connection as established. A response proves that the channel was initialized.
            assertTrue(establishedBefore.execute(new OptionsMessage()) instanceof SupportedMessage);

            StorageService.instance.setNativeTransportAuthTimeoutMillis(100);
            assertEquals(100, StorageService.instance.getNativeTransportAuthTimeoutMillis());

            establishedAfter.establishConnection();
            awaitClosed(establishedAfter, "Connection established after the timeout was enabled should be closed");
            assertTrue("Connection established while the timeout was disabled should remain open", establishedBefore.channel.isOpen());
        }
    }

    private static EmbeddedChannel newChannelWithHandler()
    {
        // The clock of an embedded channel follows the real time unless it is frozen, advanceTimeBy only adds to it.
        // It is frozen before the handler sees the channel becoming active, which is when it schedules the timeout,
        // so that the deadline is reached only by the time the test advances and never by the time the test takes
        // to run.
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.freezeTime();
        channel.pipeline().addLast(new AuthenticationTimeoutHandler(AUTH_TIMEOUT_MS));
        channel.pipeline().fireChannelActive();
        return channel;
    }

    private static void advanceTimeBy(EmbeddedChannel channel, long millis)
    {
        channel.advanceTimeBy(millis, TimeUnit.MILLISECONDS);
        channel.runScheduledPendingTasks();
    }

    private static void setConnectionInStage(Channel channel, ConnectionStage stage)
    {
        ServerConnection connection = mock(ServerConnection.class);
        when(connection.stage()).thenReturn(stage);
        channel.attr(Connection.attributeKey).set(connection);
    }

    private static void awaitClosed(SimpleClient client, String message) throws InterruptedException
    {
        assertTrue(message, client.channel.closeFuture().await(AWAIT_SECONDS, TimeUnit.SECONDS));
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
