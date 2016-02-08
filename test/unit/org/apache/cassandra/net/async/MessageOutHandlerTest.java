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
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.org.apache.bcel.internal.generic.DDIV;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.UUIDGen;

public class MessageOutHandlerTest
{
    private static final int MESSAGING_VERSION = MessagingService.current_version;

    private ChannelWriter channelWriter;
    private EmbeddedChannel channel;
    private MessageOutHandler handler;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.createAllDirectories();
    }

    @Before
    public void setup()
    {
        setup(MessageOutHandler.AUTO_FLUSH_THRESHOLD);
    }

    private void setup(int flushThreshold)
    {
        OutboundConnectionIdentifier connectionId = OutboundConnectionIdentifier.small(new InetSocketAddress("127.0.0.1", 0),
                                                                                       new InetSocketAddress("127.0.0.2", 0));
        OutboundMessagingConnection omc = new NonSendingOutboundMessagingConnection(connectionId, null, Optional.empty());
        channel = new EmbeddedChannel();
        channelWriter = ChannelWriter.create(channel, omc::handleMessageResult, Optional.empty());
        handler = new MessageOutHandler(connectionId, MESSAGING_VERSION, channelWriter, () -> null, flushThreshold);
        channel.pipeline().addLast(handler);
    }

    @Test
    public void write_NoFlush() throws ExecutionException, InterruptedException, TimeoutException
    {
        MessageOut message = new MessageOut(MessagingService.Verb.ECHO);
        ChannelFuture future = channel.write(new QueuedMessage(message, 42));
        Assert.assertTrue(!future.isDone());
        Assert.assertFalse(channel.releaseOutbound());
    }

    @Test
    public void write_WithFlush() throws ExecutionException, InterruptedException, TimeoutException
    {
        setup(1);
        MessageOut message = new MessageOut(MessagingService.Verb.ECHO);
        ChannelFuture future = channel.write(new QueuedMessage(message, 42));
        Assert.assertTrue(future.isSuccess());
        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void serializeMessage() throws IOException
    {
        channelWriter.pendingMessageCount.set(1);
        QueuedMessage msg = new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), 1);
        ChannelFuture future = channel.writeAndFlush(msg);

        Assert.assertTrue(future.isSuccess());
        Assert.assertTrue(1 <= channel.outboundMessages().size());
        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void wrongMessageType()
    {
        ChannelPromise promise = new DefaultChannelPromise(channel);
        Assert.assertFalse(handler.isMessageValid("this is the wrong message type", promise));

        Assert.assertFalse(promise.isSuccess());
        Assert.assertNotNull(promise.cause());
        Assert.assertSame(UnsupportedMessageTypeException.class, promise.cause().getClass());
    }

    @Test
    public void unexpiredMessage()
    {
        QueuedMessage msg = new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), 1);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        Assert.assertTrue(handler.isMessageValid(msg, promise));

        // we won't know if it was successful yet, but we'll know if it's a failure because cause will be set
        Assert.assertNull(promise.cause());
    }

    @Test
    public void expiredMessage()
    {
        QueuedMessage msg = new QueuedMessage(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), 1, 0, true, true);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        Assert.assertFalse(handler.isMessageValid(msg, promise));

        Assert.assertFalse(promise.isSuccess());
        Assert.assertNotNull(promise.cause());
        Assert.assertSame(ExpiredException.class, promise.cause().getClass());
        Assert.assertTrue(channel.outboundMessages().isEmpty());
    }

    @Test
    public void write_MessageTooLarge()
    {
        write_BadMessageSize(Integer.MAX_VALUE + 1);
    }

    @Test
    public void write_MessageSizeIsBananas()
    {
        write_BadMessageSize(Integer.MIN_VALUE + 10000);
    }

    private void write_BadMessageSize(long size)
    {
        IVersionedSerializer<Object> serializer = new IVersionedSerializer<Object>()
        {
            public void serialize(Object o, DataOutputPlus out, int version)
            {   }

            public Object deserialize(DataInputPlus in, int version)
            {
                return null;
            }

            public long serializedSize(Object o, int version)
            {
                return size;
            }
        };
        MessageOut message = new MessageOut(MessagingService.Verb.UNUSED_5, "payload", serializer);
        ChannelFuture future = channel.write(new QueuedMessage(message, 42));
        Throwable t = future.cause();
        Assert.assertNotNull(t);
        Assert.assertSame(IllegalStateException.class, t.getClass());
        Assert.assertTrue(channel.isOpen());
        Assert.assertFalse(channel.releaseOutbound());
    }

    @Test
    public void writeForceExceptionPath()
    {
        IVersionedSerializer<Object> serializer = new IVersionedSerializer<Object>()
        {
            public void serialize(Object o, DataOutputPlus out, int version)
            {
                throw new RuntimeException("this exception is part of the test - DON'T PANIC");
            }

            public Object deserialize(DataInputPlus in, int version)
            {
                return null;
            }

            public long serializedSize(Object o, int version)
            {
                return 42;
            }
        };
        MessageOut message = new MessageOut(MessagingService.Verb.UNUSED_5, "payload", serializer);
        ChannelFuture future = channel.write(new QueuedMessage(message, 42));
        Throwable t = future.cause();
        Assert.assertNotNull(t);
        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(channel.releaseOutbound());
    }

    @Test
    public void captureTracingInfo_ForceException()
    {
        MessageOut message = new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE)
                             .withParameter(Tracing.TRACE_HEADER, new byte[9]);
        handler.captureTracingInfo(new QueuedMessage(message, 42));
    }

    @Test
    public void captureTracingInfo_UnknownSession()
    {
        UUID uuid = UUID.randomUUID();
        MessageOut message = new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE)
                             .withParameter(Tracing.TRACE_HEADER, UUIDGen.decompose(uuid));
        handler.captureTracingInfo(new QueuedMessage(message, 42));
    }

    @Test
    public void captureTracingInfo_KnownSession()
    {
        Tracing.instance.newSession(new HashMap<>());
        MessageOut message = new MessageOut(MessagingService.Verb.REQUEST_RESPONSE);
        handler.captureTracingInfo(new QueuedMessage(message, 42));
    }

    @Test
    public void userEventTriggered_RandomObject()
    {
        Assert.assertTrue(channel.isOpen());
        ChannelUserEventSender sender = new ChannelUserEventSender();
        channel.pipeline().addFirst(sender);
        sender.sendEvent("ThisIsAFakeEvent");
        Assert.assertTrue(channel.isOpen());
    }

    @Test
    public void userEventTriggered_Idle_NoPendingBytes()
    {
        Assert.assertTrue(channel.isOpen());
        ChannelUserEventSender sender = new ChannelUserEventSender();
        channel.pipeline().addFirst(sender);
        sender.sendEvent(IdleStateEvent.WRITER_IDLE_STATE_EVENT);
        Assert.assertTrue(channel.isOpen());
    }

    @Test
    public void userEventTriggered_Idle_WithPendingBytes()
    {
        Assert.assertTrue(channel.isOpen());
        ChannelUserEventSender sender = new ChannelUserEventSender();
        channel.pipeline().addFirst(sender);

        MessageOut message = new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE);
        channel.writeOutbound(new QueuedMessage(message, 42));
        sender.sendEvent(IdleStateEvent.WRITER_IDLE_STATE_EVENT);
        Assert.assertFalse(channel.isOpen());
    }

    private static class ChannelUserEventSender extends ChannelOutboundHandlerAdapter
    {
        private ChannelHandlerContext ctx;

        @Override
        public void handlerAdded(final ChannelHandlerContext ctx) throws Exception
        {
            this.ctx = ctx;
        }

        private void sendEvent(Object event)
        {
            ctx.fireUserEventTriggered(event);
        }
    }
}
