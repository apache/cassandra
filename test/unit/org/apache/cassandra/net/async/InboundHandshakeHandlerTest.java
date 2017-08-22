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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import io.netty.handler.codec.compression.Lz4FrameEncoder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.HandshakeProtocol.FirstHandshakeMessage;
import org.apache.cassandra.net.async.HandshakeProtocol.ThirdHandshakeMessage;
import org.apache.cassandra.net.async.InboundHandshakeHandler.State;

import static org.apache.cassandra.net.async.NettyFactory.Mode.MESSAGING;

public class InboundHandshakeHandlerTest
{
    private static final InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 0);
    private static final int MESSAGING_VERSION = MessagingService.current_version;
    private static final int VERSION_30 = MessagingService.VERSION_30;

    private InboundHandshakeHandler handler;
    private EmbeddedChannel channel;
    private ByteBuf buf;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        TestAuthenticator authenticator = new TestAuthenticator(false);
        handler = new InboundHandshakeHandler(authenticator);
        channel = new EmbeddedChannel(handler);
    }

    @After
    public void tearDown()
    {
        if (buf != null)
            buf.release();
        channel.finishAndReleaseAll();
    }

    @Test
    public void handleAuthenticate_Good()
    {
        handler = new InboundHandshakeHandler(new TestAuthenticator(true));
        channel = new EmbeddedChannel(handler);
        boolean result = handler.handleAuthenticate(addr, channel.pipeline().firstContext());
        Assert.assertTrue(result);
        Assert.assertTrue(channel.isOpen());
    }

    @Test
    public void handleAuthenticate_Bad()
    {
        boolean result = handler.handleAuthenticate(addr, channel.pipeline().firstContext());
        Assert.assertFalse(result);
        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(channel.isActive());
    }

    @Test
    public void handleAuthenticate_BadSocketAddr()
    {
        boolean result = handler.handleAuthenticate(new FakeSocketAddress(), channel.pipeline().firstContext());
        Assert.assertFalse(result);
        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(channel.isActive());
    }

    private static class FakeSocketAddress extends SocketAddress
    {   }

    @Test
    public void decode_AlreadyFailed()
    {
        handler.setState(State.HANDSHAKE_FAIL);
        buf = new FirstHandshakeMessage(MESSAGING_VERSION, MESSAGING, true).encode(PooledByteBufAllocator.DEFAULT);
        handler.decode(channel.pipeline().firstContext(), buf, new ArrayList<>());
        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(channel.isActive());
        Assert.assertSame(State.HANDSHAKE_FAIL, handler.getState());
    }

    @Test
    public void handleStart_NotEnoughInputBytes() throws IOException
    {
        ByteBuf buf = Unpooled.EMPTY_BUFFER;
        State state = handler.handleStart(channel.pipeline().firstContext(), buf);
        Assert.assertEquals(State.START, state);
        Assert.assertTrue(channel.isOpen());
        Assert.assertTrue(channel.isActive());
    }

    @Test (expected = IOException.class)
    public void handleStart_BadMagic() throws IOException
    {
        InboundHandshakeHandler handler = new InboundHandshakeHandler(new TestAuthenticator(false));
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        buf = Unpooled.buffer(32, 32);

        FirstHandshakeMessage first = new FirstHandshakeMessage(MESSAGING_VERSION,
                                                                MESSAGING,
                                                                true);

        buf.writeInt(MessagingService.PROTOCOL_MAGIC << 2);
        buf.writeInt(first.encodeFlags());
        handler.handleStart(channel.pipeline().firstContext(), buf);
    }

    @Test
    public void handleStart_VersionTooHigh() throws IOException
    {
        channel.eventLoop();
        buf = new FirstHandshakeMessage(MESSAGING_VERSION + 1, MESSAGING, true).encode(PooledByteBufAllocator.DEFAULT);
        State state = handler.handleStart(channel.pipeline().firstContext(), buf);
        Assert.assertEquals(State.HANDSHAKE_FAIL, state);
        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(channel.isActive());
    }

    @Test
    public void handleStart_VersionLessThan3_0() throws IOException
    {
        buf = new FirstHandshakeMessage(VERSION_30 - 1, MESSAGING, true).encode(PooledByteBufAllocator.DEFAULT);
        State state = handler.handleStart(channel.pipeline().firstContext(), buf);
        Assert.assertEquals(State.HANDSHAKE_FAIL, state);

        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(channel.isActive());
    }

    @Test
    public void handleStart_HappyPath_Messaging() throws IOException
    {
        buf = new FirstHandshakeMessage(MESSAGING_VERSION, MESSAGING, true).encode(PooledByteBufAllocator.DEFAULT);
        State state = handler.handleStart(channel.pipeline().firstContext(), buf);
        Assert.assertEquals(State.AWAIT_MESSAGING_START_RESPONSE, state);
        if (buf.refCnt() > 0)
            buf.release();

        buf = new ThirdHandshakeMessage(MESSAGING_VERSION, addr.getAddress()).encode(PooledByteBufAllocator.DEFAULT);
        state = handler.handleMessagingStartResponse(channel.pipeline().firstContext(), buf);

        Assert.assertEquals(State.MESSAGING_HANDSHAKE_COMPLETE, state);
        Assert.assertTrue(channel.isOpen());
        Assert.assertTrue(channel.isActive());
        Assert.assertFalse(channel.outboundMessages().isEmpty());
        channel.releaseOutbound();
    }

    @Test
    public void handleMessagingStartResponse_NotEnoughInputBytes() throws IOException
    {
        ByteBuf buf = Unpooled.EMPTY_BUFFER;
        State state = handler.handleMessagingStartResponse(channel.pipeline().firstContext(), buf);
        Assert.assertEquals(State.AWAIT_MESSAGING_START_RESPONSE, state);
        Assert.assertTrue(channel.isOpen());
        Assert.assertTrue(channel.isActive());
    }

    @Test
    public void handleMessagingStartResponse_BadMaxVersion() throws IOException
    {
        buf = Unpooled.buffer(32, 32);
        buf.writeInt(MESSAGING_VERSION + 1);
        CompactEndpointSerializationHelper.serialize(addr.getAddress(), new ByteBufOutputStream(buf));
        State state = handler.handleMessagingStartResponse(channel.pipeline().firstContext(), buf);
        Assert.assertEquals(State.HANDSHAKE_FAIL, state);
        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(channel.isActive());
    }

    @Test
    public void handleMessagingStartResponse_HappyPath() throws IOException
    {
        buf = Unpooled.buffer(32, 32);
        buf.writeInt(MESSAGING_VERSION);
        CompactEndpointSerializationHelper.serialize(addr.getAddress(), new ByteBufOutputStream(buf));
        State state = handler.handleMessagingStartResponse(channel.pipeline().firstContext(), buf);
        Assert.assertEquals(State.MESSAGING_HANDSHAKE_COMPLETE, state);
        Assert.assertTrue(channel.isOpen());
        Assert.assertTrue(channel.isActive());
    }

    @Test
    public void setupPipeline_NoCompression()
    {
        ChannelPipeline pipeline = channel.pipeline();
        Assert.assertNotNull(pipeline.get(InboundHandshakeHandler.class));

        handler.setupMessagingPipeline(pipeline, addr.getAddress(), false, MESSAGING_VERSION);
        Assert.assertNotNull(pipeline.get(MessageInHandler.class));
        Assert.assertNull(pipeline.get(Lz4FrameDecoder.class));
        Assert.assertNull(pipeline.get(Lz4FrameEncoder.class));
        Assert.assertNull(pipeline.get(InboundHandshakeHandler.class));
    }

    @Test
    public void setupPipeline_WithCompression()
    {
        ChannelPipeline pipeline = channel.pipeline();
        Assert.assertNotNull(pipeline.get(InboundHandshakeHandler.class));

        handler.setupMessagingPipeline(pipeline, addr.getAddress(), true, MESSAGING_VERSION);
        Assert.assertNotNull(pipeline.get(MessageInHandler.class));
        Assert.assertNotNull(pipeline.get(Lz4FrameDecoder.class));
        Assert.assertNull(pipeline.get(Lz4FrameEncoder.class));
        Assert.assertNull(pipeline.get(InboundHandshakeHandler.class));
    }

    @Test
    public void failHandshake()
    {
        ChannelPromise future = channel.newPromise();
        handler.setHandshakeTimeout(future);
        Assert.assertFalse(future.isCancelled());
        Assert.assertTrue(channel.isOpen());
        handler.failHandshake(channel.pipeline().firstContext());
        Assert.assertSame(State.HANDSHAKE_FAIL, handler.getState());
        Assert.assertTrue(future.isCancelled());
        Assert.assertFalse(channel.isOpen());
    }

    @Test
    public void failHandshake_AlreadyConnected()
    {
        ChannelPromise future = channel.newPromise();
        handler.setHandshakeTimeout(future);
        Assert.assertFalse(future.isCancelled());
        Assert.assertTrue(channel.isOpen());
        handler.setState(State.MESSAGING_HANDSHAKE_COMPLETE);
        handler.failHandshake(channel.pipeline().firstContext());
        Assert.assertSame(State.MESSAGING_HANDSHAKE_COMPLETE, handler.getState());
        Assert.assertTrue(channel.isOpen());
    }

    @Test
    public void failHandshake_TaskIsCancelled()
    {
        ChannelPromise future = channel.newPromise();
        future.cancel(false);
        handler.setHandshakeTimeout(future);
        handler.setState(State.AWAIT_MESSAGING_START_RESPONSE);
        Assert.assertTrue(channel.isOpen());
        handler.failHandshake(channel.pipeline().firstContext());
        Assert.assertSame(State.AWAIT_MESSAGING_START_RESPONSE, handler.getState());
        Assert.assertTrue(channel.isOpen());
    }
}
