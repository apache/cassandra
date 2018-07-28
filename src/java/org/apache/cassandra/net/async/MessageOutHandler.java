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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * Note: this class derives from {@link ChannelDuplexHandler} so we can intercept calls to
 * {@link #userEventTriggered(ChannelHandlerContext, Object)} and {@link #channelWritabilityChanged(ChannelHandlerContext)}.
 */
class MessageOutHandler extends ChannelDuplexHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MessageOutHandler.class);

    private final OutboundConnectionIdentifier connectionId;

    MessageOutHandler(OutboundConnectionIdentifier connectionId)
    {
        this.connectionId = connectionId;
    }

    /**
     * {@inheritDoc}
     *
     * If we get an {@link IdleStateEvent} for the write path, we want to close the channel as we can't make progress.
     * That assumes, of course, that there's any outstanding bytes in the channel to write. We don't necesarrily care
     * about idleness (for example, gossip channels will be idle most of the time), but instead our concern is
     * the ability to make progress when there's work to be done.
     * <p>
     * Note: this is invoked on the netty event loop.
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
    {
        if (evt instanceof IdleStateEvent && ((IdleStateEvent)evt).state() == IdleState.WRITER_IDLE)
        {
            ChannelOutboundBuffer cob = ctx.channel().unsafe().outboundBuffer();
            if (cob != null && cob.totalPendingWriteBytes() > 0)
            {
                ctx.channel().attr(OutboundMessagingConnection.PURGE_MESSAGES_CHANNEL_ATTR)
                   .compareAndSet(Boolean.FALSE, Boolean.TRUE);
                ctx.close();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        if (cause instanceof IOException)
            logger.trace("{} io error", connectionId, cause);
        else
            logger.warn("{} error", connectionId, cause);

        ctx.close();
    }
}
