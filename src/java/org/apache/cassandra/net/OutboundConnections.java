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
package org.apache.cassandra.net;

import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.concurrent.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectObjectHashMap;
import io.netty.util.concurrent.Future; //checkstyle: permit this import
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.InternodeOutboundMetrics;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.lang.Math.max;
import static org.apache.cassandra.config.CassandraRelevantProperties.OTCP_LARGE_MESSAGE_THRESHOLD;
import static org.apache.cassandra.gms.Gossiper.instance;
import static org.apache.cassandra.net.FrameEncoderCrc.HEADER_AND_TRAILER_LENGTH;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.ConnectionType.URGENT_MESSAGES;
import static org.apache.cassandra.net.ConnectionType.LARGE_MESSAGES;
import static org.apache.cassandra.net.ConnectionType.SMALL_MESSAGES;
import static org.apache.cassandra.net.ResourceLimits.*;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

/**
 * Groups a set of outbound connections to a given peer, and routes outgoing messages to the appropriate connection
 * (based upon message's type or size). Contains a {@link OutboundConnection} for each of the
 * {@link ConnectionType} types.
 */
public class OutboundConnections
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundConnections.class);

    private static final int HEADER_LENGTH = 8  // magic number
                                            + 1  // token
                                            + 4  // compressed length
                                            + 4  // uncompressed length
                                            + 4; // checksum

    @VisibleForTesting
    public static final int LARGE_MESSAGE_THRESHOLD = OTCP_LARGE_MESSAGE_THRESHOLD.getInt()
    - max(max(HEADER_LENGTH, HEADER_AND_TRAILER_LENGTH), FrameEncoderLZ4.HEADER_AND_TRAILER_LENGTH);

    private final Condition metricsReady = newOneTimeCondition();
    private volatile InternodeOutboundMetrics metrics;
    private final Limit reserveCapacity;

    private OutboundConnectionSettings template;
    public final OutboundConnection small;
    public final OutboundConnection large;
    public final OutboundConnection urgent;

    private OutboundConnections(OutboundConnectionSettings template)
    {
        this.template = template = template.withDefaultReserveLimits();
        reserveCapacity = new Concurrent(template.applicationSendQueueReserveEndpointCapacityInBytes);
        EndpointAndGlobal reserveCapacityInBytes = new EndpointAndGlobal(reserveCapacity, template.applicationSendQueueReserveGlobalCapacityInBytes);
        this.small = new OutboundConnection(SMALL_MESSAGES, template, reserveCapacityInBytes);
        this.large = new OutboundConnection(LARGE_MESSAGES, template, reserveCapacityInBytes);
        this.urgent = new OutboundConnection(URGENT_MESSAGES, template, reserveCapacityInBytes);
    }

    /**
     * Select the appropriate connection for the provided message and use it to send the message.
     */
    public void enqueue(Message msg, ConnectionType type) throws ClosedChannelException
    {
        connectionFor(msg, type).enqueue(msg);
    }

    static <K> OutboundConnections tryRegister(ConcurrentMap<K, OutboundConnections> in, K key, OutboundConnectionSettings settings)
    {
        OutboundConnections connections = in.get(key);
        if (connections == null)
        {
            connections = new OutboundConnections(settings);
            OutboundConnections existing = in.putIfAbsent(key, connections);

            if (existing == null)
            {
                connections.metrics = new InternodeOutboundMetrics(settings.to, connections);
                connections.metricsReady.signalAll();
            }
            else
            {
                connections.metricsReady.signalAll();
                connections.close(false);
                connections = existing;
            }
        }
        return connections;
    }

    /**
     * Reconnect to the peer using the given {@code addr}. Outstanding messages in each channel will be sent on the
     * current channel. Typically this function is used for something like EC2 public IP addresses which need to be used
     * for communication between EC2 regions.
     *
     * @param addr IP Address to use (and prefer) going forward for connecting to the peer
     */
    synchronized Future<Void> reconnectWithNewIp(InetAddressAndPort addr)
    {
        template = template.withConnectTo(addr);
        return FutureCombiner.nettySuccessListener(
            apply(c -> c.reconnectWith(template))
        );
    }

    /**
     * Close the connections permanently
     *
     * @param flushQueues {@code true} if existing messages in the queue should be sent before closing.
     */
    synchronized Future<Void> scheduleClose(long time, TimeUnit unit, boolean flushQueues)
    {
        // immediately release our metrics, so that if we need to re-open immediately we can safely register a new one
        releaseMetrics();
        return FutureCombiner.nettySuccessListener(
            apply(c -> c.scheduleClose(time, unit, flushQueues))
        );
    }

    /**
     * Close the connections permanently
     *
     * @param flushQueues {@code true} if existing messages in the queue should be sent before closing.
     */
    synchronized Future<Void> close(boolean flushQueues)
    {
        // immediately release our metrics, so that if we need to re-open immediately we can safely register a new one
        releaseMetrics();
        return FutureCombiner.nettySuccessListener(
            apply(c -> c.close(flushQueues))
        );
    }

    private void releaseMetrics()
    {
        try
        {
            metricsReady.await();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }

        if (metrics != null)
            metrics.release();
    }

    /**
     * Close each netty channel and its socket
     */
    void interrupt()
    {
        // must return a non-null value for ImmutableList.of()
        apply(OutboundConnection::interrupt);
    }

    /**
     * Apply the given function to each of the connections we are pooling, returning the results as a list
     */
    private <V> List<V> apply(Function<OutboundConnection, V> f)
    {
        return ImmutableList.of(
            f.apply(urgent), f.apply(small), f.apply(large)
        );
    }

    @VisibleForTesting
    OutboundConnection connectionFor(Message<?> message)
    {
        return connectionFor(message, null);
    }

    private OutboundConnection connectionFor(Message msg, ConnectionType forceConnection)
    {
        return connectionFor(connectionTypeFor(msg, forceConnection));
    }

    private static ConnectionType connectionTypeFor(Message<?> msg, ConnectionType specifyConnection)
    {
        if (specifyConnection != null)
            return specifyConnection;

        if (msg.serializedSize(current_version) > LARGE_MESSAGE_THRESHOLD)
        {
            if (msg.verb().priority == Verb.Priority.P0)
            {
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES,
                                 "Enqueued URGENT message which exceeds large message threshold");

                if (logger.isTraceEnabled())
                    logger.trace("{} message with size {} exceeded large message threshold {}",
                                 msg.verb(),
                                 msg.serializedSize(current_version),
                                 LARGE_MESSAGE_THRESHOLD);
            }

            return LARGE_MESSAGES;
        }

        return msg.verb().priority == Verb.Priority.P0
               ? URGENT_MESSAGES
               : SMALL_MESSAGES;
    }

    @VisibleForTesting
    final OutboundConnection connectionFor(ConnectionType type)
    {
        switch (type)
        {
            case SMALL_MESSAGES:
                return small;
            case LARGE_MESSAGES:
                return large;
            case URGENT_MESSAGES:
                return urgent;
            default:
                throw new IllegalArgumentException("unsupported connection type: " + type);
        }
    }

    public long usingReserveBytes()
    {
        return reserveCapacity.using();
    }

    long expiredCallbacks()
    {
        return metrics.expiredCallbacks.getCount();
    }

    void incrementExpiredCallbackCount()
    {
        metrics.expiredCallbacks.mark();
    }

    OutboundConnectionSettings template()
    {
        return template;
    }

    private static class UnusedConnectionMonitor
    {
        UnusedConnectionMonitor(MessagingService messagingService)
        {
            this.messagingService = messagingService;
        }

        static class Counts
        {
            final long small, large, urgent;
            Counts(long small, long large, long urgent)
            {
                this.small = small;
                this.large = large;
                this.urgent = urgent;
            }
        }

        final MessagingService messagingService;
        ObjectObjectHashMap<InetAddressAndPort, Counts> prevEndpointToCounts = new ObjectObjectHashMap<>();

        private void closeUnusedSinceLastRun()
        {
            ObjectObjectHashMap<InetAddressAndPort, Counts> curEndpointToCounts = new ObjectObjectHashMap<>();
            for (OutboundConnections connections : messagingService.channelManagers.values())
            {
                Counts cur = new Counts(
                    connections.small.submittedCount(),
                    connections.large.submittedCount(),
                    connections.urgent.submittedCount()
                );
                curEndpointToCounts.put(connections.template.to, cur);

                Counts prev = prevEndpointToCounts.get(connections.template.to);
                if (prev == null)
                    continue;

                if (cur.small != prev.small && cur.large != prev.large && cur.urgent != prev.urgent)
                    continue;

                if (cur.small == prev.small && cur.large == prev.large && cur.urgent == prev.urgent
                    && !instance.isKnownEndpoint(connections.template.to))
                {
                    logger.info("Closing outbound connections to {}, as inactive and not known by Gossiper",
                                connections.template.to);
                    // close entirely if no traffic and the endpoint is unknown
                    messagingService.closeOutboundNow(connections);
                    continue;
                }

                if (cur.small == prev.small)
                    connections.small.interrupt();

                if (cur.large == prev.large)
                    connections.large.interrupt();

                if (cur.urgent == prev.urgent)
                    connections.urgent.interrupt();
            }

            prevEndpointToCounts = curEndpointToCounts;
        }
    }

    static void scheduleUnusedConnectionMonitoring(MessagingService messagingService, ScheduledExecutorService executor, long delay, TimeUnit units)
    {
        executor.scheduleWithFixedDelay(new UnusedConnectionMonitor(messagingService)::closeUnusedSinceLastRun, 0L, delay, units);
    }

    @VisibleForTesting
    static OutboundConnections unsafeCreate(OutboundConnectionSettings template)
    {
        OutboundConnections connections = new OutboundConnections(template);
        connections.metricsReady.signalAll();
        return connections;
    }

}
