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

import java.net.InetSocketAddress;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.metrics.ConnectionMetrics;
import org.apache.cassandra.net.BackPressureState;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;

/**
 * Groups a set of outbound connections to a given peer, and routes outgoing messages to the appropriate connection
 * (based upon message's type or size). Contains a {@link OutboundMessagingConnection} for each of the
 * {@link ConnectionType} type.
 */
public class OutboundMessagingPool
{
    @VisibleForTesting
    static final long LARGE_MESSAGE_THRESHOLD = Long.getLong(Config.PROPERTY_PREFIX + "otcp_large_message_threshold", 1024 * 64);

    private final ConnectionMetrics metrics;
    private final BackPressureState backPressureState;

    public OutboundMessagingConnection gossipChannel;
    public OutboundMessagingConnection largeMessageChannel;
    public OutboundMessagingConnection smallMessageChannel;

    /**
     * An override address on which to communicate with the peer. Typically used for something like EC2 public IP addresses
     * which need to be used for communication between EC2 regions.
     */
    private InetSocketAddress preferredRemoteAddr;

    public OutboundMessagingPool(InetSocketAddress remoteAddr, InetSocketAddress localAddr, ServerEncryptionOptions encryptionOptions,
                                 BackPressureState backPressureState, IInternodeAuthenticator authenticator)
    {
        preferredRemoteAddr = remoteAddr;
        this.backPressureState = backPressureState;
        metrics = new ConnectionMetrics(localAddr.getAddress(), this);


        smallMessageChannel = new OutboundMessagingConnection(OutboundConnectionIdentifier.small(localAddr, preferredRemoteAddr),
                                                              encryptionOptions, coalescingStrategy(remoteAddr), authenticator);
        largeMessageChannel = new OutboundMessagingConnection(OutboundConnectionIdentifier.large(localAddr, preferredRemoteAddr),
                                                              encryptionOptions, coalescingStrategy(remoteAddr), authenticator);

        // don't attempt coalesce the gossip messages, just ship them out asap (let's not anger the FD on any peer node by any artificial delays)
        gossipChannel = new OutboundMessagingConnection(OutboundConnectionIdentifier.gossip(localAddr, preferredRemoteAddr),
                                                        encryptionOptions, Optional.empty(), authenticator);
    }

    private static Optional<CoalescingStrategy> coalescingStrategy(InetSocketAddress remoteAddr)
    {
        String strategyName = DatabaseDescriptor.getOtcCoalescingStrategy();
        String displayName = remoteAddr.getAddress().getHostAddress();
        return CoalescingStrategies.newCoalescingStrategy(strategyName,
                                                          DatabaseDescriptor.getOtcCoalescingWindow(),
                                                          OutboundMessagingConnection.logger,
                                                          displayName);

    }

    public BackPressureState getBackPressureState()
    {
        return backPressureState;
    }

    public void sendMessage(MessageOut msg, int id)
    {
        getConnection(msg).sendMessage(msg, id);
    }

    @VisibleForTesting
    public OutboundMessagingConnection getConnection(MessageOut msg)
    {
        // optimize for the common path (the small message channel)
        if (Stage.GOSSIP != msg.getStage())
        {
            return msg.serializedSize(smallMessageChannel.getTargetVersion()) < LARGE_MESSAGE_THRESHOLD
            ? smallMessageChannel
            : largeMessageChannel;
        }
        return gossipChannel;
    }

    /**
     * Reconnect to the peer using the given {@code addr}. Outstanding messages in each channel will be sent on the
     * current channel. Typically this function is used for something like EC2 public IP addresses which need to be used
     * for communication between EC2 regions.
     *
     * @param addr IP Address to use (and prefer) going forward for connecting to the peer
     */
    public void reconnectWithNewIp(InetSocketAddress addr)
    {
        preferredRemoteAddr = addr;
        gossipChannel.reconnectWithNewIp(addr);
        largeMessageChannel.reconnectWithNewIp(addr);
        smallMessageChannel.reconnectWithNewIp(addr);
    }

    /**
     * Close each netty channel and it's socket.
     *
     * @param softClose {@code true} if existing messages in the queue should be sent before closing.
     */
    public void close(boolean softClose)
    {
        gossipChannel.close(softClose);
        largeMessageChannel.close(softClose);
        smallMessageChannel.close(softClose);
    }

    /**
     * For testing purposes only.
     */
    @VisibleForTesting
    OutboundMessagingConnection getConnection(ConnectionType connectionType)
    {
        switch (connectionType)
        {
            case GOSSIP:
                return gossipChannel;
            case LARGE_MESSAGE:
                return largeMessageChannel;
            case SMALL_MESSAGE:
                return smallMessageChannel;
            default:
                throw new IllegalArgumentException("unsupported connection type: " + connectionType);
        }
    }

    public void incrementTimeout()
    {
        metrics.timeouts.mark();
    }

    public long getTimeouts()
    {
        return metrics.timeouts.getCount();
    }

    public InetSocketAddress getPreferredRemoteAddr()
    {
        return preferredRemoteAddr;
    }
}
