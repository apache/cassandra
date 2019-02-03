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

import java.util.function.Consumer;

import com.google.common.base.Preconditions;

import io.netty.channel.EventLoop;
import io.netty.channel.WriteBufferWaterMark;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.net.async.OutboundHandshakeHandler.HandshakeResult;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;

/**
 * A collection of data points to be passed around for outbound connections.
 */
public class OutboundConnectionParams
{
    final OutboundConnectionIdentifier connectionId;
    final Consumer<HandshakeResult> callback;
    final ServerEncryptionOptions encryptionOptions;
    final NettyFactory.Mode mode;
    final boolean compress;
    final CoalescingStrategy coalescingStrategy;
    final int sendBufferSize;
    final boolean tcpNoDelay;
    final WriteBufferWaterMark waterMark;
    final int protocolVersion;
    final int tcpConnectTimeoutInMS;
    final int tcpUserTimeoutInMS;
    final EventLoop eventLoop;

    private OutboundConnectionParams(OutboundConnectionIdentifier connectionId,
                                     Consumer<HandshakeResult> callback,
                                     ServerEncryptionOptions encryptionOptions,
                                     NettyFactory.Mode mode,
                                     boolean compress,
                                     CoalescingStrategy coalescingStrategy,
                                     int sendBufferSize,
                                     boolean tcpNoDelay,
                                     WriteBufferWaterMark waterMark,
                                     int protocolVersion,
                                     int tcpConnectTimeoutInMS,
                                     int tcpUserTimeoutInMS,
                                     EventLoop eventLoop)
    {
        this.connectionId = connectionId;
        this.callback = callback;
        this.encryptionOptions = encryptionOptions;
        this.mode = mode;
        this.compress = compress;
        this.coalescingStrategy = coalescingStrategy;
        this.sendBufferSize = sendBufferSize;
        this.tcpNoDelay = tcpNoDelay;
        this.waterMark = waterMark;
        this.protocolVersion = protocolVersion;
        this.tcpConnectTimeoutInMS = tcpConnectTimeoutInMS;
        this.tcpUserTimeoutInMS = tcpUserTimeoutInMS;
        this.eventLoop = eventLoop;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(OutboundConnectionParams params)
    {
        return new Builder(params);
    }
    
    public static class Builder
    {
        private OutboundConnectionIdentifier connectionId;
        private Consumer<HandshakeResult> callback;
        private ServerEncryptionOptions encryptionOptions;
        private NettyFactory.Mode mode;
        private boolean compress;
        private CoalescingStrategy coalescingStrategy;
        private int sendBufferSize = 0;
        private boolean tcpNoDelay;
        private WriteBufferWaterMark waterMark = WriteBufferWaterMark.DEFAULT;
        private int protocolVersion;
        private int tcpConnectTimeoutInMS;
        private int tcpUserTimeoutInMS;
        private EventLoop eventLoop;

        private Builder()
        {
            this.tcpConnectTimeoutInMS = DatabaseDescriptor.getInternodeTcpConnectTimeoutInMS();
            this.tcpUserTimeoutInMS = DatabaseDescriptor.getInternodeTcpUserTimeoutInMS();
        }

        private Builder(OutboundConnectionParams params)
        {
            this.connectionId = params.connectionId;
            this.callback = params.callback;
            this.encryptionOptions = params.encryptionOptions;
            this.mode = params.mode;
            this.compress = params.compress;
            this.coalescingStrategy = params.coalescingStrategy;
            this.sendBufferSize = params.sendBufferSize;
            this.tcpNoDelay = params.tcpNoDelay;
            this.tcpConnectTimeoutInMS = params.tcpConnectTimeoutInMS;
            this.tcpUserTimeoutInMS = params.tcpUserTimeoutInMS;
            this.waterMark = params.waterMark;
            this.protocolVersion = params.protocolVersion;
            this.eventLoop = params.eventLoop;
        }

        public Builder connectionId(OutboundConnectionIdentifier connectionId)
        {
            this.connectionId = connectionId;
            return this;
        }

        public Builder callback(Consumer<HandshakeResult> callback)
        {
            this.callback = callback;
            return this;
        }

        public Builder encryptionOptions(ServerEncryptionOptions encryptionOptions)
        {
            this.encryptionOptions = encryptionOptions;
            return this;
        }

        public Builder mode(NettyFactory.Mode mode)
        {
            this.mode = mode;
            return this;
        }

        public Builder compress(boolean compress)
        {
            this.compress = compress;
            return this;
        }

        public Builder coalescingStrategy(CoalescingStrategy coalescingStrategy)
        {
            this.coalescingStrategy = coalescingStrategy;
            return this;
        }

        public Builder sendBufferSize(int sendBufferSize)
        {
            this.sendBufferSize = sendBufferSize;
            return this;
        }

        public Builder tcpNoDelay(boolean tcpNoDelay)
        {
            this.tcpNoDelay = tcpNoDelay;
            return this;
        }

        public Builder waterMark(WriteBufferWaterMark waterMark)
        {
            this.waterMark = waterMark;
            return this;
        }

        public Builder protocolVersion(int protocolVersion)
        {
            this.protocolVersion = protocolVersion;
            return this;
        }

        public Builder tcpConnectTimeoutInMS(int tcpConnectTimeoutInMS)
        {
            this.tcpConnectTimeoutInMS = tcpConnectTimeoutInMS;
            return this;
        }

        public Builder tcpUserTimeoutInMS(int tcpUserTimeoutInMS)
        {
            this.tcpUserTimeoutInMS = tcpUserTimeoutInMS;
            return this;
        }

        public Builder eventLoop(EventLoop eventLoop)
        {
            this.eventLoop = eventLoop;
            return this;
        }

        public OutboundConnectionParams build()
        {
            Preconditions.checkArgument(protocolVersion > 0, "illegal protocol version: " + protocolVersion);
            Preconditions.checkArgument(sendBufferSize > 0 && sendBufferSize < 1 << 20, "illegal send buffer size: " + sendBufferSize);
            Preconditions.checkArgument(tcpUserTimeoutInMS >= 0, "tcp user timeout must be non negative: " + tcpUserTimeoutInMS);
            Preconditions.checkArgument(tcpConnectTimeoutInMS > 0, "tcp connect timeout must be positive: " + tcpConnectTimeoutInMS);

            return new OutboundConnectionParams(connectionId, callback, encryptionOptions, mode, compress, coalescingStrategy, sendBufferSize,
                                                tcpNoDelay, waterMark, protocolVersion, tcpConnectTimeoutInMS, tcpUserTimeoutInMS, eventLoop);
        }
    }
}
