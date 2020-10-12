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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.Future;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.Collections.synchronizedList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.concurrent.Stage.MUTATION;
import static org.apache.cassandra.utils.Throwables.maybeFail;

/**
 * MessagingService implements all internode communication - with the exception of SSTable streaming (for now).
 *
 * Specifically, it's responsible for dispatch of outbound messages to other nodes and routing of inbound messages
 * to their appropriate {@link IVerbHandler}.
 *
 * <h2>Using MessagingService: sending requests and responses</h2>
 *
 * The are two ways to send a {@link Message}, and you should pick one depending on the desired behaviour:
 *  1. To send a request that expects a response back, use
 *     {@link #sendWithCallback(Message, InetAddressAndPort, RequestCallback)} method. Once a response
 *     message is received, {@link RequestCallback#onResponse(Message)} method will be invoked on the
 *     provided callback - in case of a success response. In case of a failure response (see {@link Verb#FAILURE_RSP}),
 *     or if a response doesn't arrive within verb's configured expiry time,
 *     {@link RequestCallback#onFailure(InetAddressAndPort, RequestFailureReason)} will be invoked instead.
 *  2. To send a response back, or a message that expects no response, use {@link #send(Message, InetAddressAndPort)}
 *     method.
 *
 * See also: {@link Message#out(Verb, Object)}, {@link Message#responseWith(Object)},
 * and {@link Message#failureResponse(RequestFailureReason)}.
 *
 * <h2>Using MessagingService: handling a request</h2>
 *
 * As described in the previous section, to handle responses you only need to implement {@link RequestCallback}
 * interface - so long as your response verb handler is the default {@link ResponseVerbHandler}.
 *
 * There are two steps you need to perform to implement request handling:
 *  1. Create a {@link IVerbHandler} to process incoming requests and responses for the new type (if applicable).
 *  2. Add a new {@link Verb} to the enum for the new request type, and, if applicable, one for the response message.
 *
 * MessagingService will now automatically invoke your handler whenever a {@link Message} with this verb arrives.
 *
 * <h1>Architecture of MessagingService</h1>
 *
 * <h2>QOS</h2>
 *
 * Since our messaging protocol is TCP-based, and also doesn't yet support interleaving messages with each other,
 * we need a way to prevent head-of-line blocking adversely affecting all messages - in particular, large messages
 * being in the way of smaller ones. To achive that (somewhat), we maintain three messaging connections to and
 * from each peer:
 * - one for large messages - defined as being larger than {@link OutboundConnections#LARGE_MESSAGE_THRESHOLD}
 *   (65KiB by default)
 * - one for small messages - defined as smaller than that threshold
 * - and finally, a connection for urgent messages - usually small and/or that are important to arrive
 *   promptly, e.g. gossip-related ones
 *
 * <h2>Wire format and framing</h2>
 *
 * Small messages are grouped together into frames, and large messages are split over multiple frames.
 * Framing provides application-level integrity protection to otherwise raw streams of data - we use
 * CRC24 for frame headers and CRC32 for the entire payload. LZ4 is optionally used for compression.
 *
 * You can find the on-wire format description of individual messages in the comments for
 * {@link Message.Serializer}, alongside with format evolution notes.
 * For the list and descriptions of available frame decoders see {@link FrameDecoder} comments. You can
 * find wire format documented in the javadoc of {@link FrameDecoder} implementations:
 * see {@link FrameDecoderCrc} and {@link FrameDecoderLZ4} in particular.
 *
 * <h2>Architecture of outbound messaging</h2>
 *
 * {@link OutboundConnection} is the core class implementing outbound connection logic, with
 * {@link OutboundConnection#enqueue(Message)} being its main entry point. The connections are initiated
 * by {@link OutboundConnectionInitiator}.
 *
 * Netty pipeline for outbound messaging connections generally consists of the following handlers:
 *
 * [(optional) SslHandler] <- [FrameEncoder]
 *
 * {@link OutboundConnection} handles the entire lifetime of a connection: from the very first handshake
 * to any necessary reconnects if necessary.
 *
 * Message-delivery flow varies depending on the connection type.
 *
 * For {@link ConnectionType#SMALL_MESSAGES} and {@link ConnectionType#URGENT_MESSAGES},
 * {@link Message} serialization and delivery occurs directly on the event loop.
 * See {@link OutboundConnection.EventLoopDelivery} for details.
 *
 * For {@link ConnectionType#LARGE_MESSAGES}, to ensure that servicing large messages doesn't block
 * timely service of other requests, message serialization is offloaded to a companion thread pool
 * ({@link SocketFactory#synchronousWorkExecutor}). Most of the work will be performed by
 * {@link AsyncChannelOutputPlus}. Please see {@link OutboundConnection.LargeMessageDelivery}
 * for details.
 *
 * To prevent fast clients, or slow nodes on the other end of the connection from overwhelming
 * a host with enqueued, unsent messages on heap, we impose strict limits on how much memory enqueued,
 * undelivered messages can claim.
 *
 * Every individual connection gets an exclusive permit quota to use - 4MiB by default; every endpoint
 * (group of large, small, and urgent connection) is capped at, by default, at 128MiB of undelivered messages,
 * and a global limit of 512MiB is imposed on all endpoints combined.
 *
 * On an attempt to {@link OutboundConnection#enqueue(Message)}, the connection will attempt to allocate
 * permits for message-size number of bytes from its exclusive quota; if successful, it will add the
 * message to the queue; if unsuccessful, it will need to allocate remainder from both endpoint and lobal
 * reserves, and if it fails to do so, the message will be rejected, and its callbacks, if any,
 * immediately expired.
 *
 * For a more detailed description please see the docs and comments of {@link OutboundConnection}.
 *
 * <h2>Architecture of inbound messaging</h2>
 *
 * {@link InboundMessageHandler} is the core class implementing inbound connection logic, paired
 * with {@link FrameDecoder}. Inbound connections are initiated by {@link InboundConnectionInitiator}.
 * The primary entry points to these classes are {@link FrameDecoder#channelRead(ShareableBytes)}
 * and {@link InboundMessageHandler#process(FrameDecoder.Frame)}.
 *
 * Netty pipeline for inbound messaging connections generally consists of the following handlers:
 *
 * [(optional) SslHandler] -> [FrameDecoder] -> [InboundMessageHandler]
 *
 * {@link FrameDecoder} is responsible for decoding incoming frames and work stashing; {@link InboundMessageHandler}
 * then takes decoded frames from the decoder and processes the messages contained in them.
 *
 * The flow differs between small and large messages. Small ones are deserialized immediately, and only
 * then scheduled on the right thread pool for the {@link Verb} for execution. Large messages, OTOH,
 * aren't deserialized until they are just about to be executed on the appropriate {@link Stage}.
 *
 * Similarly to outbound handling, inbound messaging imposes strict memory utilisation limits on individual
 * endpoints and on global aggregate consumption, and implements simple flow control, to prevent a single
 * fast endpoint from overwhelming a host.
 *
 * Every individual connection gets an exclusive permit quota to use - 4MiB by default; every endpoint
 * (group of large, small, and urgent connection) is capped at, by default, at 128MiB of unprocessed messages,
 * and a global limit of 512MiB is imposed on all endpoints combined.
 *
 * On arrival of a message header, the handler will attempt to allocate permits for message-size number
 * of bytes from its exclusive quota; if successful, it will proceed to deserializing and processing the message.
 * If unsuccessful, the handler will attempt to allocate the remainder from its endpoint and global reserve;
 * if either allocation is unsuccessful, the handler will cease any further frame processing, and tell
 * {@link FrameDecoder} to stop reading from the network; subsequently, it will put itself on a special
 * {@link org.apache.cassandra.net.InboundMessageHandler.WaitQueue}, to be reactivated once more permits
 * become available.
 *
 * For a more detailed description please see the docs and comments of {@link InboundMessageHandler} and
 * {@link FrameDecoder}.
 *
 * <h2>Observability</h2>
 *
 * MessagingService exposes diagnostic counters for both outbound and inbound directions - received and sent
 * bytes and message counts, overload bytes and message count, error bytes and error counts, and many more.
 *
 * See {@link org.apache.cassandra.metrics.InternodeInboundMetrics} and
 * {@link org.apache.cassandra.metrics.InternodeOutboundMetrics} for JMX-exposed counters.
 *
 * We also provide {@code system_views.internode_inbound} and {@code system_views.internode_outbound} virtual tables -
 * implemented in {@link org.apache.cassandra.db.virtual.InternodeInboundTable} and
 * {@link org.apache.cassandra.db.virtual.InternodeOutboundTable} respectively.
 */
public final class MessagingService extends MessagingServiceMBeanImpl
{
    private static final Logger logger = LoggerFactory.getLogger(MessagingService.class);

    // 8 bits version, so don't waste versions
    public static final int VERSION_30 = 10;
    public static final int VERSION_3014 = 11;
    public static final int VERSION_40 = 12;
    public static final int minimum_version = VERSION_30;
    public static final int current_version = VERSION_40;
    static AcceptVersions accept_messaging = new AcceptVersions(minimum_version, current_version);
    static AcceptVersions accept_streaming = new AcceptVersions(current_version, current_version);

    public enum Version
    {
        VERSION_30(10),
        VERSION_3014(11),
        VERSION_40(12);

        public final int value;

        Version(int value)
        {
            this.value = value;
        }
    }

    private static class MSHandle
    {
        public static final MessagingService instance = new MessagingService(false);
    }

    public static MessagingService instance()
    {
        return MSHandle.instance;
    }

    public final SocketFactory socketFactory = new SocketFactory();
    public final LatencySubscribers latencySubscribers = new LatencySubscribers();
    public final RequestCallbacks callbacks = new RequestCallbacks(this);

    // a public hook for filtering messages intended for delivery to this node
    public final InboundSink inboundSink = new InboundSink(this);

    // the inbound global reserve limits and associated wait queue
    private final InboundMessageHandlers.GlobalResourceLimits inboundGlobalReserveLimits = new InboundMessageHandlers.GlobalResourceLimits(
        new ResourceLimits.Concurrent(DatabaseDescriptor.getInternodeApplicationReceiveQueueReserveGlobalCapacityInBytes()));

    // the socket bindings we accept incoming connections on
    private final InboundSockets inboundSockets = new InboundSockets(new InboundConnectionSettings()
                                                                     .withHandlers(this::getInbound)
                                                                     .withSocketFactory(socketFactory));

    // a public hook for filtering messages intended for delivery to another node
    public final OutboundSink outboundSink = new OutboundSink(this::doSend);

    final ResourceLimits.Limit outboundGlobalReserveLimit =
        new ResourceLimits.Concurrent(DatabaseDescriptor.getInternodeApplicationSendQueueReserveGlobalCapacityInBytes());

    private volatile boolean isShuttingDown;

    @VisibleForTesting
    MessagingService(boolean testOnly)
    {
        super(testOnly);
        OutboundConnections.scheduleUnusedConnectionMonitoring(this, ScheduledExecutors.scheduledTasks, 1L, TimeUnit.HOURS);
    }

    /**
     * Send a non-mutation message to a given endpoint. This method specifies a callback
     * which is invoked with the actual response.
     *
     * @param message message to be sent.
     * @param to      endpoint to which the message needs to be sent
     * @param cb      callback interface which is used to pass the responses or
     *                suggest that a timeout occurred to the invoker of the send().
     */
    public void sendWithCallback(Message message, InetAddressAndPort to, RequestCallback cb)
    {
        sendWithCallback(message, to, cb, null);
    }

    public void sendWithCallback(Message message, InetAddressAndPort to, RequestCallback cb, ConnectionType specifyConnection)
    {
        callbacks.addWithExpiration(cb, message, to);
        if (cb.invokeOnFailure() && !message.callBackOnFailure())
            message = message.withCallBackOnFailure();
        send(message, to, specifyConnection);
    }

    /**
     * Send a mutation message or a Paxos Commit to a given endpoint. This method specifies a callback
     * which is invoked with the actual response.
     * Also holds the message (only mutation messages) to determine if it
     * needs to trigger a hint (uses StorageProxy for that).
     *
     * @param message message to be sent.
     * @param to      endpoint to which the message needs to be sent
     * @param handler callback interface which is used to pass the responses or
     *                suggest that a timeout occurred to the invoker of the send().
     */
    public void sendWriteWithCallback(Message message, Replica to, AbstractWriteResponseHandler<?> handler, boolean allowHints)
    {
        assert message.callBackOnFailure();
        callbacks.addWithExpiration(handler, message, to, handler.consistencyLevel(), allowHints);
        send(message, to.endpoint(), null);
    }

    /**
     * Send a message to a given endpoint. This method adheres to the fire and forget
     * style messaging.
     *
     * @param message messages to be sent.
     * @param to      endpoint to which the message needs to be sent
     */
    public void send(Message message, InetAddressAndPort to)
    {
        send(message, to, null);
    }

    public void send(Message message, InetAddressAndPort to, ConnectionType specifyConnection)
    {
        if (logger.isTraceEnabled())
        {
            logger.trace("{} sending {} to {}@{}", FBUtilities.getBroadcastAddressAndPort(), message.verb(), message.id(), to);

            if (to.equals(FBUtilities.getBroadcastAddressAndPort()))
                logger.trace("Message-to-self {} going over MessagingService", message);
        }

        outboundSink.accept(message, to, specifyConnection);
    }

    private void doSend(Message message, InetAddressAndPort to, ConnectionType specifyConnection)
    {
        // expire the callback if the message failed to enqueue (failed to establish a connection or exceeded queue capacity)
        while (true)
        {
            OutboundConnections connections = getOutbound(to);
            try
            {
                connections.enqueue(message, specifyConnection);
                return;
            }
            catch (ClosedChannelException e)
            {
                if (isShuttingDown)
                    return; // just drop the message, and let others clean up

                // remove the connection and try again
                channelManagers.remove(to, connections);
            }
        }
    }

    void markExpiredCallback(InetAddressAndPort addr)
    {
        OutboundConnections conn = channelManagers.get(addr);
        if (conn != null)
            conn.incrementExpiredCallbackCount();
    }

    /**
     * Only to be invoked once we believe the endpoint will never be contacted again.
     *
     * We close the connection after a five minute delay, to give asynchronous operations a chance to terminate
     */
    public void closeOutbound(InetAddressAndPort to)
    {
        OutboundConnections pool = channelManagers.get(to);
        if (pool != null)
            pool.scheduleClose(5L, MINUTES, true)
                .addListener(future -> channelManagers.remove(to, pool));
    }

    /**
     * Only to be invoked once we believe the connections will never be used again.
     */
    void closeOutboundNow(OutboundConnections connections)
    {
        connections.close(true).addListener(
            future -> channelManagers.remove(connections.template().to, connections)
        );
    }

    /**
     * Only to be invoked once we believe the connections will never be used again.
     */
    public void removeInbound(InetAddressAndPort from)
    {
        InboundMessageHandlers handlers = messageHandlers.remove(from);
        if (null != handlers)
            handlers.releaseMetrics();
    }

    /**
     * Closes any current open channel/connection to the endpoint, but does not cause any message loss, and we will
     * try to re-establish connections immediately
     */
    public void interruptOutbound(InetAddressAndPort to)
    {
        OutboundConnections pool = channelManagers.get(to);
        if (pool != null)
            pool.interrupt();
    }

    /**
     * Reconnect to the peer using the given {@code addr}. Outstanding messages in each channel will be sent on the
     * current channel. Typically this function is used for something like EC2 public IP addresses which need to be used
     * for communication between EC2 regions.
     *
     * @param address IP Address to identify the peer
     * @param preferredAddress IP Address to use (and prefer) going forward for connecting to the peer
     */
    @SuppressWarnings("UnusedReturnValue")
    public Future<Void> maybeReconnectWithNewIp(InetAddressAndPort address, InetAddressAndPort preferredAddress)
    {
        if (!SystemKeyspace.updatePreferredIP(address, preferredAddress))
            return null;

        OutboundConnections messagingPool = channelManagers.get(address);
        if (messagingPool != null)
            return messagingPool.reconnectWithNewIp(preferredAddress);

        return null;
    }

    /**
     * Wait for callbacks and don't allow any more to be created (since they could require writing hints)
     */
    public void shutdown()
    {
        shutdown(1L, MINUTES, true, true);
    }

    public void shutdown(long timeout, TimeUnit units, boolean shutdownGracefully, boolean shutdownExecutors)
    {
        isShuttingDown = true;
        logger.info("Waiting for messaging service to quiesce");
        // We may need to schedule hints on the mutation stage, so it's erroneous to shut down the mutation stage first
        assert !MUTATION.executor().isShutdown();

        if (shutdownGracefully)
        {
            callbacks.shutdownGracefully();
            List<Future<Void>> closing = new ArrayList<>();
            for (OutboundConnections pool : channelManagers.values())
                closing.add(pool.close(true));

            long deadline = System.nanoTime() + units.toNanos(timeout);
            maybeFail(() -> new FutureCombiner(closing).get(timeout, units),
                      () -> {
                          List<ExecutorService> inboundExecutors = new ArrayList<>();
                          inboundSockets.close(synchronizedList(inboundExecutors)::add).get();
                          ExecutorUtils.awaitTermination(1L, TimeUnit.MINUTES, inboundExecutors);
                      },
                      () -> {
                          if (shutdownExecutors)
                              shutdownExecutors(deadline);
                      },
                      () -> callbacks.awaitTerminationUntil(deadline),
                      inboundSink::clear,
                      outboundSink::clear);
        }
        else
        {
            callbacks.shutdownNow(false);
            List<Future<Void>> closing = new ArrayList<>();
            List<ExecutorService> inboundExecutors = synchronizedList(new ArrayList<ExecutorService>());
            closing.add(inboundSockets.close(inboundExecutors::add));
            for (OutboundConnections pool : channelManagers.values())
                closing.add(pool.close(false));

            long deadline = System.nanoTime() + units.toNanos(timeout);
            maybeFail(() -> new FutureCombiner(closing).get(timeout, units),
                      () -> {
                          if (shutdownExecutors)
                              shutdownExecutors(deadline);
                      },
                      () -> ExecutorUtils.awaitTermination(timeout, units, inboundExecutors),
                      () -> callbacks.awaitTerminationUntil(deadline),
                      inboundSink::clear,
                      outboundSink::clear);
        }
    }

    private void shutdownExecutors(long deadlineNanos) throws TimeoutException, InterruptedException
    {
        socketFactory.shutdownNow();
        socketFactory.awaitTerminationUntil(deadlineNanos);
    }

    private OutboundConnections getOutbound(InetAddressAndPort to)
    {
        OutboundConnections connections = channelManagers.get(to);
        if (connections == null)
            connections = OutboundConnections.tryRegister(channelManagers, to, new OutboundConnectionSettings(to).withDefaults(ConnectionCategory.MESSAGING));
        return connections;
    }

    InboundMessageHandlers getInbound(InetAddressAndPort from)
    {
        InboundMessageHandlers handlers = messageHandlers.get(from);
        if (null != handlers)
            return handlers;

        return messageHandlers.computeIfAbsent(from, addr ->
            new InboundMessageHandlers(FBUtilities.getLocalAddressAndPort(),
                                       addr,
                                       DatabaseDescriptor.getInternodeApplicationReceiveQueueCapacityInBytes(),
                                       DatabaseDescriptor.getInternodeApplicationReceiveQueueReserveEndpointCapacityInBytes(),
                                       inboundGlobalReserveLimits, metrics, inboundSink)
        );
    }

    @VisibleForTesting
    boolean isConnected(InetAddressAndPort address, Message<?> messageOut)
    {
        OutboundConnections pool = channelManagers.get(address);
        if (pool == null)
            return false;
        return pool.connectionFor(messageOut).isConnected();
    }

    public void listen()
    {
        inboundSockets.open();
    }

    public void waitUntilListening() throws InterruptedException
    {
        inboundSockets.open().await();
    }
}
