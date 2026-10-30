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
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.transport.messages.ReadyMessage;
import org.apache.cassandra.transport.messages.StartupMessage;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the authentication timeout on a server which requires TLS. The timeout starts when the TCP connection is
 * established, so it covers the TLS handshake as well as what follows it.
 */
public class AuthenticationTimeoutTlsTest extends CQLTester
{
    private static final long AWAIT_SECONDS = 30;
    private static final String KEYSTORE_PATH = "test/conf/cassandra_ssl_test.keystore";
    private static final String TRUSTSTORE_PATH = "test/conf/cassandra_ssl_test.truststore";
    private static final String STORE_PASSWORD = "cassandra";

    private long originalAuthTimeout;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        DatabaseDescriptor.updateNativeProtocolEncryptionOptions(AuthenticationTimeoutTlsTest::withTestStores);
        requireNetwork(server -> server.withTlsEncryptionPolicy(EncryptionOptions.TlsEncryptionPolicy.ENCRYPTED), cluster -> {});
    }

    @Before
    public void setUp()
    {
        originalAuthTimeout = DatabaseDescriptor.nativeTransportAuthTimeout();
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setNativeTransportAuthTimeout(originalAuthTimeout);
    }

    @Test
    public void testConnectionWhichNeverStartsTlsHandshakeTimesOut() throws Exception
    {
        DatabaseDescriptor.setNativeTransportAuthTimeout(100);
        // a client without encryption, it connects and then never sends a byte
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            long timeoutsBefore = ClientMetrics.instance.getAuthTimeoutCount();
            client.establishConnection();
            awaitClosed(client, "Connection which did not start the TLS handshake should be closed after the timeout");
            assertEquals(timeoutsBefore + 1, ClientMetrics.instance.getAuthTimeoutCount());
        }
    }

    @Test
    public void testConnectionWhichStopsAfterTlsHandshakeTimesOut() throws Exception
    {
        // long enough for the handshake to finish, the connection then never sends STARTUP
        DatabaseDescriptor.setNativeTransportAuthTimeout(2000);
        try (SimpleClient client = newTlsClient())
        {
            client.establishConnection();
            awaitTlsHandshake(client);
            awaitClosed(client, "Connection which finished the TLS handshake but did not send STARTUP should be closed after the timeout");
        }
    }

    @Test
    public void testHandlerIsRemovedWhenConnectionBecomesReadyOverTls() throws Exception
    {
        // generous, the handshake and STARTUP have to finish within this time even on a slow machine
        DatabaseDescriptor.setNativeTransportAuthTimeout(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
        try (SimpleClient client = newTlsClient())
        {
            client.establishConnection();
            awaitTlsHandshake(client);
            StartupMessage startup = new StartupMessage(Collections.singletonMap(StartupMessage.CQL_VERSION, QueryProcessor.CQL_VERSION.toString()));
            Message.Response response = client.execute(startup);
            assertTrue("Expected READY response", response instanceof ReadyMessage);
            // without the handler there is no timeout task left which could close the connection
            awaitAuthenticationTimeoutHandlerRemoved(client);
            assertTrue("Connection should remain open after it became READY", client.channel.isOpen());
        }
    }

    private static SimpleClient newTlsClient()
    {
        // a client uses the truststore to verify the certificate of a server only when require_client_auth is set
        return new SimpleClient(nativeAddr.getHostAddress(), nativePort, withTestStores(new EncryptionOptions()).withRequireClientAuth(true));
    }

    private static EncryptionOptions withTestStores(EncryptionOptions options)
    {
        return options.withEnabled(true)
                      .withKeyStore(KEYSTORE_PATH)
                      .withKeyStorePassword(STORE_PASSWORD)
                      .withTrustStore(TRUSTSTORE_PATH)
                      .withTrustStorePassword(STORE_PASSWORD)
                      .withRequireEndpointVerification(false)
                      .withRequireClientAuth(false);
    }

    private static void awaitTlsHandshake(SimpleClient client) throws InterruptedException
    {
        SslHandler sslHandler = client.channel.pipeline().get(SslHandler.class);
        assertTrue("TLS handshake should succeed", sslHandler.handshakeFuture().await(AWAIT_SECONDS, TimeUnit.SECONDS));
        assertTrue("TLS handshake should succeed", sslHandler.handshakeFuture().isSuccess());
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
