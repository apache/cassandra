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
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

final class AuthenticationTimeoutHandler extends ChannelInboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(AuthenticationTimeoutHandler.class);

    private final long timeoutMillis;
    private final AtomicBoolean completed = new AtomicBoolean();
    private volatile ScheduledFuture<?> timeoutFuture;

    AuthenticationTimeoutHandler(long timeoutMillis)
    {
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        ScheduledFuture<?> future = ctx.executor().schedule(() -> {
            if (completed.compareAndSet(false, true))
            {
                logger.info("Closing unauthenticated client connection {} after timeout of {}ms",
                            ctx.channel().remoteAddress(), timeoutMillis);
                ctx.close();
            }
        }, timeoutMillis, TimeUnit.MILLISECONDS);

        timeoutFuture = future;

        if (completed.get())
            future.cancel(false);

        super.channelActive(ctx);
    }

    void authenticationComplete()
    {
        if (completed.compareAndSet(false, true))
        {
            ScheduledFuture<?> future = timeoutFuture;
            if (future != null)
                future.cancel(false);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        completed.set(true);

        ScheduledFuture<?> future = timeoutFuture;
        if (future != null)
            future.cancel(false);

        super.channelInactive(ctx);
    }
}