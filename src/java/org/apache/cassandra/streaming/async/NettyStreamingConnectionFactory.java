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

package org.apache.cassandra.streaming.async;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future; // checkstyle: permit this import
import org.apache.cassandra.net.ConnectionCategory;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result.StreamingSuccess;
import org.apache.cassandra.net.OutboundConnectionSettings;
import org.apache.cassandra.streaming.StreamingChannel;

import static org.apache.cassandra.locator.InetAddressAndPort.getByAddress;
import static org.apache.cassandra.net.InternodeConnectionUtils.isSSLError;
import static org.apache.cassandra.net.OutboundConnectionInitiator.initiateStreaming;
import static org.apache.cassandra.net.OutboundConnectionInitiator.SslFallbackConnectionType;
import static org.apache.cassandra.net.OutboundConnectionInitiator.SslFallbackConnectionType.SERVER_CONFIG;

public class NettyStreamingConnectionFactory implements StreamingChannel.Factory
{
    @VisibleForTesting
    public static int MAX_CONNECT_ATTEMPTS = 3;

    public static NettyStreamingChannel connect(OutboundConnectionSettings template, int messagingVersion, StreamingChannel.Kind kind) throws IOException
    {
        EventLoop eventLoop = MessagingService.instance().socketFactory.outboundStreamingGroup().next();
        OutboundConnectionSettings settings = template.withDefaults(ConnectionCategory.STREAMING);
        List<SslFallbackConnectionType> sslFallbacks = settings.withEncryption() && settings.encryption.getOptional()
                                                       ? Arrays.asList(SslFallbackConnectionType.values())
                                                       : Collections.singletonList(SERVER_CONFIG);

        Throwable cause = null;
        for (final SslFallbackConnectionType sslFallbackConnectionType : sslFallbacks)
        {
            for (int i = 0; i < MAX_CONNECT_ATTEMPTS; i++)
            {
                Future<Result<StreamingSuccess>> result = initiateStreaming(eventLoop, settings, sslFallbackConnectionType);
                result.awaitUninterruptibly(); // initiate has its own timeout, so this is "guaranteed" to return relatively promptly
                if (result.isSuccess())
                {
                    Channel channel = result.getNow().success().channel;
                    NettyStreamingChannel streamingChannel = new NettyStreamingChannel(channel, kind);
                    if (kind == StreamingChannel.Kind.CONTROL)
                    {
                        ChannelPipeline pipeline = channel.pipeline();
                        pipeline.addLast("stream", streamingChannel);
                    }
                    return streamingChannel;
                }
                cause = result.cause();
            }
            if (!isSSLError(cause))
            {
                // Fallback only when the error is SSL related, otherwise retries are exhausted, so fail
                break;
            }
        }
        throw new IOException("failed to connect to " + template.to + " for streaming data", cause);
    }

    @Override
    public StreamingChannel create(InetSocketAddress to, int messagingVersion, StreamingChannel.Kind kind) throws IOException
    {
        return connect(new OutboundConnectionSettings(getByAddress(to)), messagingVersion, kind);
    }

    @Override
    public StreamingChannel create(InetSocketAddress to,
                                   InetSocketAddress preferred,
                                   int messagingVersion,
                                   StreamingChannel.Kind kind) throws IOException
    {
        return connect(new OutboundConnectionSettings(getByAddress(to), getByAddress(preferred)), messagingVersion, kind);
    }
}
