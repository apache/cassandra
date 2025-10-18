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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.net.InetAddresses;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.security.DefaultSslContextFactory;
import static org.apache.cassandra.net.OutboundConnectionInitiator.Result;
import static org.apache.cassandra.net.OutboundConnectionInitiator.SslFallbackConnectionType;
import static org.apache.cassandra.net.OutboundConnectionInitiator.initiateStreaming;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.MessagingService.minimum_version;

public class StreamingTest
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

    private Future<Result<Result.StreamingSuccess>> streamingConnect(AcceptVersions acceptOutbound, AcceptVersions acceptInbound) throws ExecutionException, InterruptedException
    {
        InboundSockets inbound = new InboundSockets(new InboundConnectionSettings().withAcceptMessaging(acceptInbound));
        try
        {
            inbound.open();
            InetAddressAndPort endpoint = inbound.sockets().stream().map(s -> s.settings.bindAddress).findFirst().get();
            EventLoop eventLoop = factory.defaultGroup().next();
            Future<Result<Result.StreamingSuccess>> result = initiateStreaming(eventLoop,
                              new OutboundConnectionSettings(endpoint)
                                                    .withAcceptVersions(acceptOutbound)
                                                    .withDefaults(ConnectionCategory.STREAMING),
                              SslFallbackConnectionType.SERVER_CONFIG
                              );
            result.awaitUninterruptibly();
            return result;
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testIncompatibleVersion() throws InterruptedException, ExecutionException
    {
        Future<Result<Result.StreamingSuccess>> result = streamingConnect(new AcceptVersions(current_version + 1, current_version + 1), new AcceptVersions(minimum_version + 2, current_version + 3));
        Channel channel = null;
        if (result.isSuccess()) {
            Result<Result.StreamingSuccess> nowResult = result.getNow();
            channel = nowResult.success().channel;
        }
        Assert.assertNotNull(true);
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
