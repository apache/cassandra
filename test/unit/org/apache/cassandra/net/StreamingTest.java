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

import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.MessagingService.minimum_version;
//import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.OutboundConnectionInitiator.initiateStreaming;
import static org.apache.cassandra.net.OutboundConnectionInitiator.Result;
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

    private Result<Result.StreamingSuccess> streamingConnect(AcceptVersions acceptOutbound, AcceptVersions acceptInbound) throws ExecutionException, InterruptedException
    {
        InboundSockets inbound = new InboundSockets(new InboundConnectionSettings().withAcceptMessaging(acceptInbound));
        try
        {
            inbound.open();
            EventLoop eventLoop = MessagingService.instance().socketFactory.outboundStreamingGroup().next();

            InetAddressAndPort endpoint = inbound.sockets().stream().map(s -> s.settings.bindAddress).findFirst().get();
            OutboundConnectionSettings settings = new OutboundConnectionSettings(endpoint).withAcceptVersions(acceptOutbound)
                                                                                          .withDefaults(ConnectionCategory.STREAMING);
            Future<Result<Result.StreamingSuccess>> result = initiateStreaming(eventLoop,
                                                                               settings, acceptOutbound.min);
            result.awaitUninterruptibly();
            Assert.assertTrue(result.isSuccess());

            return result.getNow();
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testIncompatibleVersion() throws InterruptedException, ExecutionException
    {
        Result<Result.StreamingSuccess> nowResult = streamingConnect(new AcceptVersions(current_version + 1, current_version + 1), new AcceptVersions(minimum_version + 2, current_version + 3));
        Assert.assertNull(nowResult.success());
        System.out.println(nowResult.outcome);
        Assert.assertEquals(Result.Outcome.INCOMPATIBLE, nowResult.outcome);
        Assert.assertEquals(current_version, nowResult.incompatible().closestSupportedVersion);
        Assert.assertEquals(current_version, nowResult.incompatible().maxMessagingVersion);
    }

    @Test
    public void testCompatibleVersion() throws InterruptedException, ExecutionException
    {
        Result<Result.StreamingSuccess> nowResult = streamingConnect(new AcceptVersions(MessagingService.minimum_version, current_version + 1), new AcceptVersions(minimum_version + 2, current_version + 3));
        //new AcceptVersions(VERSION_40, VERSION_40), new AcceptVersions(VERSION_40, VERSION_40));
        Assert.assertNotNull(nowResult.success());
        System.out.println(nowResult.outcome);
        Assert.assertNotNull(nowResult.success().channel);
        Assert.assertEquals(Result.Outcome.SUCCESS, nowResult.outcome);
        Assert.assertEquals(current_version, nowResult.success().messagingVersion);
    }




    private OutboundConnection initiateOutbound(InetAddressAndPort endpoint, boolean optional) throws ClosedChannelException
    {
        final OutboundConnectionSettings settings = new OutboundConnectionSettings(endpoint)
                                                    .withAcceptVersions(new AcceptVersions(minimum_version, current_version))
                                                    .withDefaults(ConnectionCategory.MESSAGING)
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