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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Closes native transport connections that do not complete authentication within the configured timeout.
 * <p>
 * The timeout starts when the channel becomes active and is cancelled when authentication completes
 * or the channel is closed.
 */
final class AuthenticationTimeoutHandler extends ChannelInboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(AuthenticationTimeoutHandler.class);

    private final long timeoutMillis;
    private ScheduledFuture<?> timeoutFuture;

    AuthenticationTimeoutHandler(long timeoutMillis)
    {
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        timeoutFuture = ctx.executor().schedule(() -> {
            logger.warn("Closing unauthenticated client connection {} because it did not complete "
                        + "authentication within {}ms (native_transport_auth_timeout)",
                        ctx.channel().remoteAddress(), timeoutMillis);
            ctx.close();
        }, timeoutMillis, TimeUnit.MILLISECONDS);

        super.channelActive(ctx);
    }

    void markAuthenticationComplete(ChannelHandlerContext ctx)
    {
        ctx.pipeline().remove(this);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception
    {
        cancelTimeout();
        super.handlerRemoved(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        cancelTimeout();
        super.channelInactive(ctx);
    }

    private void cancelTimeout()
    {
        if (timeoutFuture != null)
        {
            timeoutFuture.cancel(false);
            timeoutFuture = null;
        }
    }
}
