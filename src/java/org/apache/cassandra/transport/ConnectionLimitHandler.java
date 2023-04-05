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


import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.NoSpamLogger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * {@link ChannelInboundHandlerAdapter} implementation which allows to limit the number of concurrent
 * connections to the Server. Be aware this <strong>MUST</strong> be shared between all child channels.
 */
@ChannelHandler.Sharable
final class ConnectionLimitHandler extends ChannelInboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectionLimitHandler.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);
    private static final AttributeKey<InetAddress> addressAttributeKey = AttributeKey.valueOf(ConnectionLimitHandler.class, "address");

    private final ConcurrentMap<InetAddress, AtomicLong> connectionsPerClient = new ConcurrentHashMap<>();
    private final AtomicLong counter = new AtomicLong(0);

    // Keep the remote address as a channel attribute.  The channel inactive callback needs
    // to know the entry into the connetionsPerClient map and depending on the state of the remote
    // an exception may be thrown trying to retrieve the address. Make sure the same address used
    // to increment is used for decrement.
    private static InetAddress setRemoteAddressAttribute(Channel channel)
    {
        Attribute<InetAddress> addressAttribute = channel.attr(addressAttributeKey);
        SocketAddress remoteAddress = channel.remoteAddress();
        if (remoteAddress instanceof InetSocketAddress)
        {
            addressAttribute.setIfAbsent(((InetSocketAddress) remoteAddress).getAddress());
        }
        else
        {
            noSpamLogger.warn("Remote address of unknown type: {}, skipping per-IP connection limits",
                              remoteAddress.getClass());
        }
        return addressAttribute.get();
    }

    private static InetAddress getRemoteAddressAttribute(Channel channel)
    {
        Attribute<InetAddress> addressAttribute = channel.attr(addressAttributeKey);
        return addressAttribute.get();
    }


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
            noSpamLogger.error("Exceeded maximum native connection limit of {} by using {} connections (see native_transport_max_concurrent_connections in cassandra.yaml)", limit, count);
            ctx.close();
        }
        else
        {
            long perIpLimit = DatabaseDescriptor.getNativeTransportMaxConcurrentConnectionsPerIp();
            if (perIpLimit > 0)
            {
                InetAddress address = setRemoteAddressAttribute(ctx.channel());
                if (address == null)
                {
                    ctx.close();
                    return;
                }
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
                    noSpamLogger.error("Exceeded maximum native connection limit per ip of {} by using {} connections (see native_transport_max_concurrent_connections_per_ip)", perIpLimit, perIpCount);
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
        InetAddress address = getRemoteAddressAttribute(ctx.channel());

        AtomicLong count = address == null ? null : connectionsPerClient.get(address);
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
