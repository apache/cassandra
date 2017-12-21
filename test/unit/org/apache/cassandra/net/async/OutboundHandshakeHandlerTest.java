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

package org.apache.cassandra.net.async;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import io.netty.handler.codec.compression.Lz4FrameEncoder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.HandshakeProtocol.SecondHandshakeMessage;
import org.apache.cassandra.net.async.OutboundHandshakeHandler.HandshakeResult;

import static org.apache.cassandra.net.async.OutboundHandshakeHandler.HandshakeResult.UNKNOWN_PROTOCOL_VERSION;

public class OutboundHandshakeHandlerTest
{
    private static final int MESSAGING_VERSION = MessagingService.current_version;
    private static final InetSocketAddress localAddr = new InetSocketAddress("127.0.0.1", 0);
    private static final InetSocketAddress remoteAddr = new InetSocketAddress("127.0.0.2", 0);
    private static final String HANDLER_NAME = "clientHandshakeHandler";

    private EmbeddedChannel channel;
    private OutboundHandshakeHandler handler;
    private OutboundConnectionIdentifier connectionId;
    private OutboundConnectionParams params;
    private CallbackHandler callbackHandler;
    private ByteBuf buf;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter());
        connectionId = OutboundConnectionIdentifier.small(localAddr, remoteAddr);
        callbackHandler = new CallbackHandler();
        params = OutboundConnectionParams.builder()
                                         .connectionId(connectionId)
                                         .callback(handshakeResult -> callbackHandler.receive(handshakeResult))
                                         .mode(NettyFactory.Mode.MESSAGING)
                                         .protocolVersion(MessagingService.current_version)
                                         .coalescingStrategy(Optional.empty())
                                         .build();
        handler = new OutboundHandshakeHandler(params);
        channel.pipeline().addFirst(HANDLER_NAME, handler);
    }

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release();
        Assert.assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    public void decode_SmallInput() throws Exception
    {
        buf = Unpooled.buffer(2, 2);
        List<Object> out = new LinkedList<>();
        handler.decode(channel.pipeline().firstContext(), buf, out);
        Assert.assertEquals(0, buf.readerIndex());
        Assert.assertTrue(out.isEmpty());
    }

    @Test
    public void decode_HappyPath() throws Exception
    {
        buf = new SecondHandshakeMessage(MESSAGING_VERSION).encode(PooledByteBufAllocator.DEFAULT);
        channel.writeInbound(buf);
        Assert.assertEquals(1, channel.outboundMessages().size());
        Assert.assertTrue(channel.isOpen());
        Assert.assertTrue(channel.releaseOutbound()); // throw away any responses from decode()

        Assert.assertEquals(MESSAGING_VERSION, callbackHandler.result.negotiatedMessagingVersion);
        Assert.assertEquals(HandshakeResult.Outcome.SUCCESS, callbackHandler.result.outcome);
    }

    @Test
    public void decode_HappyPathThrowsException() throws Exception
    {
        callbackHandler.failOnCallback = true;
        buf = new SecondHandshakeMessage(MESSAGING_VERSION).encode(PooledByteBufAllocator.DEFAULT);
        channel.writeInbound(buf);
        Assert.assertFalse(channel.isOpen());
        Assert.assertEquals(1, channel.outboundMessages().size());
        Assert.assertTrue(channel.releaseOutbound()); // throw away any responses from decode()

        Assert.assertEquals(UNKNOWN_PROTOCOL_VERSION, callbackHandler.result.negotiatedMessagingVersion);
        Assert.assertEquals(HandshakeResult.Outcome.NEGOTIATION_FAILURE, callbackHandler.result.outcome);
    }

    @Test
    public void decode_ReceivedLowerMsgVersion() throws Exception
    {
        int msgVersion = MESSAGING_VERSION - 1;
        buf = new SecondHandshakeMessage(msgVersion).encode(PooledByteBufAllocator.DEFAULT);
        channel.writeInbound(buf);
        Assert.assertTrue(channel.inboundMessages().isEmpty());

        Assert.assertEquals(msgVersion, callbackHandler.result.negotiatedMessagingVersion);
        Assert.assertEquals(HandshakeResult.Outcome.DISCONNECT, callbackHandler.result.outcome);
        Assert.assertFalse(channel.isOpen());
        Assert.assertTrue(channel.outboundMessages().isEmpty());
    }

    @Test
    public void decode_ReceivedHigherMsgVersion() throws Exception
    {
        int msgVersion = MESSAGING_VERSION - 1;
        channel.pipeline().remove(HANDLER_NAME);
        params = OutboundConnectionParams.builder()
                                         .connectionId(connectionId)
                                         .callback(handshakeResult -> callbackHandler.receive(handshakeResult))
                                         .mode(NettyFactory.Mode.MESSAGING)
                                         .protocolVersion(msgVersion)
                                         .coalescingStrategy(Optional.empty())
                                         .build();
        handler = new OutboundHandshakeHandler(params);
        channel.pipeline().addFirst(HANDLER_NAME, handler);
        buf = new SecondHandshakeMessage(MESSAGING_VERSION).encode(PooledByteBufAllocator.DEFAULT);
        channel.writeInbound(buf);

        Assert.assertEquals(MESSAGING_VERSION, callbackHandler.result.negotiatedMessagingVersion);
        Assert.assertEquals(HandshakeResult.Outcome.DISCONNECT, callbackHandler.result.outcome);
    }

    @Test
    public void setupPipeline_WithCompression()
    {
        EmbeddedChannel chan = new EmbeddedChannel(new ChannelOutboundHandlerAdapter());
        ChannelPipeline pipeline =  chan.pipeline();
        params = OutboundConnectionParams.builder(params).compress(true).protocolVersion(MessagingService.current_version).build();
        handler = new OutboundHandshakeHandler(params);
        pipeline.addFirst(handler);
        handler.setupPipeline(chan, MESSAGING_VERSION);
        Assert.assertNotNull(pipeline.get(Lz4FrameEncoder.class));
        Assert.assertNull(pipeline.get(Lz4FrameDecoder.class));
        Assert.assertNotNull(pipeline.get(MessageOutHandler.class));
    }

    @Test
    public void setupPipeline_NoCompression()
    {
        EmbeddedChannel chan = new EmbeddedChannel(new ChannelOutboundHandlerAdapter());
        ChannelPipeline pipeline =  chan.pipeline();
        params = OutboundConnectionParams.builder(params).compress(false).protocolVersion(MessagingService.current_version).build();
        handler = new OutboundHandshakeHandler(params);
        pipeline.addFirst(handler);
        handler.setupPipeline(chan, MESSAGING_VERSION);
        Assert.assertNull(pipeline.get(Lz4FrameEncoder.class));
        Assert.assertNull(pipeline.get(Lz4FrameDecoder.class));
        Assert.assertNotNull(pipeline.get(MessageOutHandler.class));
    }

    private static class CallbackHandler
    {
        boolean failOnCallback;
        HandshakeResult result;

        Void receive(HandshakeResult handshakeResult)
        {
            if (failOnCallback)
            {
                // only fail the first callback
                failOnCallback = false;
                throw new RuntimeException("this exception is expected in the test - DON'T PANIC");
            }
            result = handshakeResult;
            return null;
        }
    }
}
