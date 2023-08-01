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

package org.apache.cassandra.net;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.net.InetAddresses;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result.MessagingSuccess;
import org.apache.cassandra.security.DefaultSslContextFactory;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;

import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.MessagingService.minimum_version;
import static org.apache.cassandra.net.ConnectionType.SMALL_MESSAGES;
import static org.apache.cassandra.net.OutboundConnectionInitiator.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

// TODO: test failure due to exception, timeout, etc
public class HandshakeTest
{
    private static final SocketFactory factory = new SocketFactory();
    static final InetAddressAndPort TO_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.2"), 7012);
    static final InetAddressAndPort FROM_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.1"), 7012);
    private volatile Throwable handshakeEx;

    @BeforeClass
    public static void startup()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @AfterClass
    public static void cleanup() throws InterruptedException
    {
        factory.shutdownNow();
    }

    @Before
    public void setup()
    {
        handshakeEx = null;
    }

    private Result handshake(int outMin, int outMax) throws ExecutionException, InterruptedException
    {
        return handshake(new AcceptVersions(outMin, outMax), null);
    }
    private Result handshake(int outMin, int outMax, int inMin, int inMax) throws ExecutionException, InterruptedException
    {
        return handshake(new AcceptVersions(outMin, outMax), new AcceptVersions(inMin, inMax));
    }
    private Result handshake(AcceptVersions acceptOutbound, AcceptVersions acceptInbound) throws ExecutionException, InterruptedException
    {
        InboundSockets inbound = new InboundSockets(new InboundConnectionSettings().withAcceptMessaging(acceptInbound));
        try
        {
            inbound.open();
            InetAddressAndPort endpoint = inbound.sockets().stream().map(s -> s.settings.bindAddress).findFirst().get();
            EventLoop eventLoop = factory.defaultGroup().next();
            Future<Result<MessagingSuccess>> future =
            initiateMessaging(eventLoop,
                              SMALL_MESSAGES,
                              SslFallbackConnectionType.SERVER_CONFIG,
                              new OutboundConnectionSettings(endpoint)
                                                    .withAcceptVersions(acceptOutbound)
                                                    .withDefaults(ConnectionCategory.MESSAGING),
                              AsyncPromise.withExecutor(eventLoop));
            return future.get();
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }


    @Test
    public void testBothCurrentVersion() throws InterruptedException, ExecutionException
    {
        Result result = handshake(minimum_version, current_version);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        result.success().channel.close();
    }

    @Test
    public void testSendCompatibleOldVersion() throws InterruptedException, ExecutionException
    {
        Result result = handshake(current_version, current_version + 1, current_version +1, current_version + 2);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        Assert.assertEquals(current_version + 1, result.success().messagingVersion);
        result.success().channel.close();
    }

    @Test
    public void testSendCompatibleFutureVersion() throws InterruptedException, ExecutionException
    {
        Result result = handshake(MessagingService.minimum_version, current_version + 1);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        Assert.assertEquals(current_version, result.success().messagingVersion);
        result.success().channel.close();
    }

    @Test
    public void testSendIncompatibleFutureVersion() throws InterruptedException, ExecutionException
    {
        Result result = handshake(current_version + 1, current_version + 1);
        Assert.assertEquals(Result.Outcome.INCOMPATIBLE, result.outcome);
        Assert.assertEquals(current_version, result.incompatible().closestSupportedVersion);
        Assert.assertEquals(current_version, result.incompatible().maxMessagingVersion);
    }

    @Test
    public void testSendIncompatibleOldVersion() throws InterruptedException, ExecutionException
    {
        Result result = handshake(current_version + 1, current_version + 1, current_version + 2, current_version + 3);
        Assert.assertEquals(Result.Outcome.INCOMPATIBLE, result.outcome);
        Assert.assertEquals(current_version + 2, result.incompatible().closestSupportedVersion);
        Assert.assertEquals(current_version + 3, result.incompatible().maxMessagingVersion);
    }

    @Test
    public void testSendAllSupported() throws InterruptedException, ExecutionException
    {
        List<MessagingService.Version> supportedVersions = MessagingService.Version.supportedVersions();
        for (MessagingService.Version outMin : supportedVersions)
            for (MessagingService.Version outMax : supportedVersions)
                if (outMin.value <= outMax.value)
                    for (MessagingService.Version inMin : supportedVersions)
                        for (MessagingService.Version inMax : supportedVersions)
                            if (inMin.value <= inMax.value)
                            {
                                Result result = handshake(outMin.value, outMax.value, inMin.value, inMax.value);
                                // expect success if out and in have a version in common
                                boolean expectSuccess = outMin.value <= inMax.value && inMin.value <= outMax.value;

                                Assert.assertEquals(String.format("wrong result outcome for outMin %s outMax %s inMin %s inMax %s", outMin.value, outMax.value, inMin.value, inMax.value),
                                                expectSuccess ? Result.Outcome.SUCCESS : Result.Outcome.INCOMPATIBLE, result.outcome);

                                if (expectSuccess)
                                {
                                    Assert.assertEquals(String.format("wrong agreed messagingVersion for outMin %s outMax %s inMin %s inMax %s", outMin.value, outMax.value, inMin.value, inMax.value),
                                                    Math.min(outMax.value, inMax.value), result.success().messagingVersion);
                                    result.success().channel.close();
                                }
                            }
        }

    @Test
    public void testOutboundConnectionfFallbackDuringUpgrades() throws ClosedChannelException, InterruptedException
    {
        // Upgrade from Non-SSL -> Optional SSL
        // Outbound connection from Optional SSL(new node) -> Non-SSL (old node)
        testOutboundFallbackOnSSLHandshakeFailure(SslFallbackConnectionType.SSL, true, SslFallbackConnectionType.NO_SSL, false);

        // Upgrade from Optional SSL -> Strict SSL
        // Outbound connection from Strict SSL(new node) -> Optional SSL (old node)
        testOutboundFallbackOnSSLHandshakeFailure(SslFallbackConnectionType.SSL, false, SslFallbackConnectionType.SSL, true);

        // Upgrade from Optional SSL -> Strict MTLS
        // Outbound connection from Strict MTLS(new node) -> Optional SSL (old node)
        testOutboundFallbackOnSSLHandshakeFailure(SslFallbackConnectionType.MTLS, false, SslFallbackConnectionType.SSL, true);

        // Upgrade from Strict SSL -> Optional MTLS
        // Outbound connection from Optional MTLS(new node) -> Strict SSL (old node)
        testOutboundFallbackOnSSLHandshakeFailure(SslFallbackConnectionType.MTLS, true, SslFallbackConnectionType.SSL, false);

        // Upgrade from Strict Optional MTLS -> Strict MTLS
        // Outbound connection from Strict TLS(new node) -> Optional TLS (old node)
        testOutboundFallbackOnSSLHandshakeFailure(SslFallbackConnectionType.MTLS, false, SslFallbackConnectionType.MTLS, true);
    }

    @Test
    public void testOutboundConnectionfFallbackDuringDowngrades() throws ClosedChannelException, InterruptedException
    {
        // From Strict MTLS -> Optional MTLS
        // Outbound connection from Optional TLS(new node) -> Strict MTLS (old node)
        testOutboundFallbackOnSSLHandshakeFailure(SslFallbackConnectionType.MTLS, true, SslFallbackConnectionType.MTLS, false);

        // From Optional MTLS -> Strict SSL
        // Outbound connection from Strict SSL(new node) -> Optional MTLS (old node)
        testOutboundFallbackOnSSLHandshakeFailure(SslFallbackConnectionType.SSL, false, SslFallbackConnectionType.MTLS, true);

        // From Strict MTLS -> Optional SSL
        // Outbound connection from Optional SSL(new node) -> Strict MTLS (old node)
        testOutboundFallbackOnSSLHandshakeFailure(SslFallbackConnectionType.SSL, true, SslFallbackConnectionType.MTLS, false);

        // From Strict SSL -> Optional SSL
        // Outbound connection from Optional SSL(new node) -> Strict SSL (old node)
        testOutboundFallbackOnSSLHandshakeFailure(SslFallbackConnectionType.SSL, true, SslFallbackConnectionType.SSL, false);

        // From Optional SSL -> Non-SSL
        // Outbound connection from Non-SSL(new node) -> Optional SSL (old node)
        testOutboundFallbackOnSSLHandshakeFailure(SslFallbackConnectionType.NO_SSL, false, SslFallbackConnectionType.SSL, true);
    }

    @Test
    public void testOutboundConnectionDoesntFallbackWhenErrorIsNotSSLRelated() throws ClosedChannelException, InterruptedException
    {
        // Configuring nodes in Optional SSL mode
        // when optional mode is enabled, if the connection error is SSL related, fallback to another SSL strategy should happen,
        // otherwise it should use same SSL strategy and retry
        ServerEncryptionOptions serverEncryptionOptions = getServerEncryptionOptions(SslFallbackConnectionType.SSL, true);
        InboundSockets inbound = getInboundSocket(serverEncryptionOptions);
        try
        {
            InetAddressAndPort endpoint = inbound.sockets().stream().map(s -> s.settings.bindAddress).findFirst().get();

            // Open outbound connections before server starts listening
            // The connection should be accepted after opening inbound connections, with the same SSL context without fallback
            OutboundConnection outboundConnection = initiateOutbound(endpoint, SslFallbackConnectionType.SSL, true);

            // Let the outbound connection be tried for 4 times atleast
            while (outboundConnection.connectionAttempts() < SslFallbackConnectionType.values().length)
            {
                Thread.sleep(1000);
            }
            assertFalse(outboundConnection.isConnected());
            inbound.open();
            // As soon as the node accepts inbound connections, the connection must be established with right SSL context
            waitForConnection(outboundConnection);
            assertTrue(outboundConnection.isConnected());
        }
        finally
        {
            inbound.close().await(10L, TimeUnit.SECONDS);
        }
    }

    private ServerEncryptionOptions getServerEncryptionOptions(SslFallbackConnectionType sslConnectionType, boolean optional)
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions().withOptional(optional)
                                                                                       .withKeyStore("test/conf/cassandra_ssl_test.keystore")
                                                                                       .withKeyStorePassword("cassandra")
                                                                                       .withOutboundKeystore("test/conf/cassandra_ssl_test_outbound.keystore")
                                                                                       .withOutboundKeystorePassword("cassandra")
                                                                                       .withTrustStore("test/conf/cassandra_ssl_test.truststore")
                                                                                       .withTrustStorePassword("cassandra")
                                                                                       .withSslContextFactory((new ParameterizedClass(DefaultSslContextFactory.class.getName(),
                                                                                                                                      new HashMap<>())));
        if (sslConnectionType == SslFallbackConnectionType.MTLS)
        {
            serverEncryptionOptions = serverEncryptionOptions.withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all)
                                                             .withRequireClientAuth(true);
        }
        else if (sslConnectionType == SslFallbackConnectionType.SSL)
        {
            serverEncryptionOptions = serverEncryptionOptions.withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all)
                                                             .withRequireClientAuth(false);
        }
        return serverEncryptionOptions;
    }

    private InboundSockets getInboundSocket(ServerEncryptionOptions serverEncryptionOptions)
    {
        InboundConnectionSettings settings = new InboundConnectionSettings().withAcceptMessaging(new AcceptVersions(minimum_version, current_version))
                                                                            .withEncryption(serverEncryptionOptions)
                                                                            .withBindAddress(TO_ADDR);
        List<InboundConnectionSettings> settingsList =  new ArrayList<>();
        settingsList.add(settings);
        return new InboundSockets(settingsList);
    }

    private OutboundConnection initiateOutbound(InetAddressAndPort endpoint, SslFallbackConnectionType connectionType, boolean optional) throws ClosedChannelException
    {
        final OutboundConnectionSettings settings = new OutboundConnectionSettings(endpoint)
        .withAcceptVersions(new AcceptVersions(minimum_version, current_version))
        .withDefaults(ConnectionCategory.MESSAGING)
        .withEncryption(getServerEncryptionOptions(connectionType, optional))
        .withDebugCallbacks(new HandshakeAcknowledgeChecker(t -> handshakeEx = t))
        .withFrom(FROM_ADDR);
        OutboundConnections outboundConnections = OutboundConnections.tryRegister(new ConcurrentHashMap<>(), TO_ADDR, settings);
        GossipDigestSyn syn = new GossipDigestSyn("cluster", "partitioner", new ArrayList<>(0));
        Message<GossipDigestSyn> message = Message.out(Verb.GOSSIP_DIGEST_SYN, syn);
        OutboundConnection outboundConnection = outboundConnections.connectionFor(message);
        outboundConnection.enqueue(message);
        return outboundConnection;
    }

    private void testOutboundFallbackOnSSLHandshakeFailure(SslFallbackConnectionType fromConnectionType, boolean fromOptional,
                                                           SslFallbackConnectionType toConnectionType, boolean toOptional) throws ClosedChannelException, InterruptedException
    {
        // Configures inbound connections to be optional mTLS
        InboundSockets inbound = getInboundSocket(getServerEncryptionOptions(toConnectionType, toOptional));
        try
        {
            InetAddressAndPort endpoint = inbound.sockets().stream().map(s -> s.settings.bindAddress).findFirst().get();
            inbound.open();

            // Open outbound connections, and wait until connection is established
            OutboundConnection outboundConnection = initiateOutbound(endpoint, fromConnectionType, fromOptional);
            waitForConnection(outboundConnection);
            assertTrue(outboundConnection.isConnected());
            assertNull(handshakeEx);
        }
        finally
        {
            inbound.close().await(10L, TimeUnit.SECONDS);
        }
    }

    private void waitForConnection(OutboundConnection outboundConnection) throws InterruptedException
    {
        long startTime = System.currentTimeMillis();
        while (!outboundConnection.isConnected() && System.currentTimeMillis() - startTime < 60000)
        {
            Thread.sleep(1000);
        }
    }

    private static class HandshakeAcknowledgeChecker implements OutboundDebugCallbacks
    {
        private final AtomicInteger acks = new AtomicInteger(0);
        private final Consumer<Throwable> fail;

        private HandshakeAcknowledgeChecker(Consumer<Throwable> fail)
        {
            this.fail = fail;
        }

        @Override
        public void onSendSmallFrame(int messageCount, int payloadSizeInBytes)
        {
        }

        @Override
        public void onSentSmallFrame(int messageCount, int payloadSizeInBytes)
        {
        }

        @Override
        public void onFailedSmallFrame(int messageCount, int payloadSizeInBytes)
        {
        }

        @Override
        public void onConnect(int messagingVersion, OutboundConnectionSettings settings)
        {
            if (acks.incrementAndGet() > 1)
                fail.accept(new AssertionError("Handshake was acknowledged more than once"));
        }
    }
}
