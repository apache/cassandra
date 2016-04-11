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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.WriteBufferWaterMark;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier;
import org.apache.cassandra.net.async.OutboundConnectionParams;

public class DefaultConnectionFactory implements StreamConnectionFactory
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultConnectionFactory.class);

    private static final int DEFAULT_CHANNEL_BUFFER_SIZE = 1 << 22;

    private static final long MAX_WAIT_TIME_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final int MAX_CONNECT_ATTEMPTS = 3;

    @Override
    public Channel createConnection(OutboundConnectionIdentifier connectionId, int protocolVersion) throws IOException
    {
        ServerEncryptionOptions encryptionOptions = DatabaseDescriptor.getServerEncryptionOptions();

        if (encryptionOptions.internode_encryption == ServerEncryptionOptions.InternodeEncryption.none)
            encryptionOptions = null;

        return createConnection(connectionId, protocolVersion, encryptionOptions);
    }

    protected Channel createConnection(OutboundConnectionIdentifier connectionId, int protocolVersion, @Nullable ServerEncryptionOptions encryptionOptions) throws IOException
    {
        // this is the amount of data to allow in memory before netty sets the channel writablility flag to false
        int channelBufferSize = DEFAULT_CHANNEL_BUFFER_SIZE;
        WriteBufferWaterMark waterMark = new WriteBufferWaterMark(channelBufferSize >> 2, channelBufferSize);

        int sendBufferSize = DatabaseDescriptor.getInternodeSendBufferSize() > 0
                             ? DatabaseDescriptor.getInternodeSendBufferSize()
                             : OutboundConnectionParams.DEFAULT_SEND_BUFFER_SIZE;

        OutboundConnectionParams params = OutboundConnectionParams.builder()
                                                                  .connectionId(connectionId)
                                                                  .encryptionOptions(encryptionOptions)
                                                                  .mode(NettyFactory.Mode.STREAMING)
                                                                  .protocolVersion(protocolVersion)
                                                                  .sendBufferSize(sendBufferSize)
                                                                  .waterMark(waterMark)
                                                                  .build();

        Bootstrap bootstrap = NettyFactory.instance.createOutboundBootstrap(params);

        int connectionAttemptCount = 0;
        long now = System.nanoTime();
        final long end = now + MAX_WAIT_TIME_NANOS;
        final Channel channel;
        while (true)
        {
            ChannelFuture channelFuture = bootstrap.connect();
            channelFuture.awaitUninterruptibly(end - now, TimeUnit.MILLISECONDS);
            if (channelFuture.isSuccess())
            {
                channel = channelFuture.channel();
                break;
            }

            connectionAttemptCount++;
            now = System.nanoTime();
            if (connectionAttemptCount == MAX_CONNECT_ATTEMPTS || end - now <= 0)
                throw new IOException("failed to connect to " + connectionId + " for streaming data", channelFuture.cause());

            long waitms = DatabaseDescriptor.getRpcTimeout() * (long)Math.pow(2, connectionAttemptCount);
            logger.warn("Failed attempt {} to connect to {}. Retrying in {} ms.", connectionAttemptCount, connectionId, waitms);
            Uninterruptibles.sleepUninterruptibly(waitms, TimeUnit.MILLISECONDS);
        }

        return channel;
    }
}
