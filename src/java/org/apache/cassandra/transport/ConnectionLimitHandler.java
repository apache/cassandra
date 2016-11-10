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


import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * {@link ChannelInboundHandlerAdapter} implementation which allows to limit the number of concurrent
 * connections to the Server. Be aware this <strong>MUST</strong> be shared between all child channels.
 */
@ChannelHandler.Sharable
final class ConnectionLimitHandler extends ChannelInboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectionLimitHandler.class);
    private final ConcurrentMap<InetAddress, AtomicLong> connectionsPerClient = new ConcurrentHashMap<>();
    private final AtomicLong counter = new AtomicLong(0);

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        final long count = counter.incrementAndGet();
        long limit = DatabaseDescriptor.getNativeTransportMaxConcurrentConnections();
        // Setting the limit to -1 disables it.
        if(limit < 0)
        {
            limit = Long.MAX_VALUE;
        }
        if (count > limit)
        {
            // The decrement will be done in channelClosed(...)
            logger.warn("Exceeded maximum native connection limit of {} by using {} connections", limit, count);
            ctx.close();
        }
        else
        {
            long perIpLimit = DatabaseDescriptor.getNativeTransportMaxConcurrentConnectionsPerIp();
            if (perIpLimit > 0)
            {
                InetAddress address = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress();

                AtomicLong perIpCount = connectionsPerClient.get(address);
                if (perIpCount == null)
                {
                    perIpCount = new AtomicLong(0);

                    AtomicLong old = connectionsPerClient.putIfAbsent(address, perIpCount);
                    if (old != null)
                    {
                        perIpCount = old;
                    }
                }
                if (perIpCount.incrementAndGet() > perIpLimit)
                {
                    // The decrement will be done in channelClosed(...)
                    logger.warn("Exceeded maximum native connection limit per ip of {} by using {} connections", perIpLimit, perIpCount);
                    ctx.close();
                    return;
                }
            }
            ctx.fireChannelActive();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        counter.decrementAndGet();
        InetAddress address = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress();

        AtomicLong count = connectionsPerClient.get(address);
        if (count != null)
        {
            if (count.decrementAndGet() <= 0)
            {
                connectionsPerClient.remove(address);
            }
        }
        ctx.fireChannelInactive();
    }
}
