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

import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.cassandra.locator.InetAddressAndPort;

public class MessageOutHandlerTest
{
    private EmbeddedChannel channel;

    @Before
    public void setUp() throws UnknownHostException
    {
        OutboundConnectionIdentifier connectionId = OutboundConnectionIdentifier.small(InetAddressAndPort.getByNameOverrideDefaults("127.0.0.1", 0),
                                                                                       InetAddressAndPort.getByNameOverrideDefaults("127.0.0.2", 0));
        channel = new EmbeddedChannel(new MessageOutHandler(connectionId));

    }

    @Test
    public void userEventTriggered_RandomObject()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        Assert.assertTrue(channel.isOpen());
        ChannelUserEventSender sender = new ChannelUserEventSender();
        channel.pipeline().addFirst(sender);
        sender.sendEvent("ThisIsAFakeEvent");
        Assert.assertTrue(channel.isOpen());
    }

    @Test
    public void userEventTriggered_Idle_NoPendingBytes()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
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
        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.buffer(8);
        buf.writeLong(42);

        // EmbeddedChnnel.writeOutbound() calls flush() for each message, so we prevent that with the handler we add to the pipeline
        ChannelUserEventSender sender = new ChannelUserEventSender();
        channel.pipeline().addFirst(sender);

        channel.writeOutbound(buf);
        sender.sendEvent(IdleStateEvent.WRITER_IDLE_STATE_EVENT);
        Assert.assertFalse(channel.isOpen());
    }

    private static class ChannelUserEventSender extends ChannelOutboundHandlerAdapter
    {
        private ChannelHandlerContext ctx;

        @Override
        public void handlerAdded(final ChannelHandlerContext ctx)
        {
            this.ctx = ctx;
        }

        private void sendEvent(Object event)
        {
            ctx.fireUserEventTriggered(event);
        }

        public void flush(ChannelHandlerContext ctx)
        {
            // explicitly catch the flush() to avoid moving the buytes out of the pipeline
        }
    }
}
