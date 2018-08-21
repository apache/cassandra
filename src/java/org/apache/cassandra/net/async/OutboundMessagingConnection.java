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

import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.NettyFactory.Mode;
import org.apache.cassandra.net.async.OutboundHandshakeHandler.HandshakeResult;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Represents a connection type to a peer, and handles the state transistions on the connection and the netty {@link Channel}.
 * The underlying socket is not opened until explicitly requested (by sending a message).
 *
 * Aside from a few administrative methods, the main entry point to sending a message is, aptly named, {@link #sendMessage(MessageOut, int)}.
 * As a producer, any thread may send a message (which enqueues the message to {@link #backlog}; but then there is only
 * one consuming thread, which consumes from the {@link #backlog}. The method {@link #maybeStartDequeuing()} is the entry point
 * for the task that consumes the backlog (and executes on the event loop).
 *
 * For all of the netty and channel related behaviors, we get an explicit netty event loop (which can be thought of
 * as a glorified thread) in the constructor, and stash that for the instance lifetime in {@link #eventLoop}. That event loop
 * is then used for all channels that will be created during the lifetime of this instance, as well as handling connection
 * timeouts and callbacks. This way all the activity happens on a single thread, and there's less multi-threaded concurrency
 * to have to reason about. However, one must be able to reason about the ordering and states of the callbacks/methods that are
 * executed on that event loop, which can be tricky.
 *
 * The state of the class is guided an an instance of {@link State}; refer to it's javadoc to understand the full
 * state machine semantics. In brief, the state machine helps to signal between the producers and consumer task, as well
 * as ensuring only one consume task is executing (and only one connect operation is happening, as well).
 * Ignoring the connecting state, the basic state flow is:
 * IDLE -> ENQUEUED -> RUNNING -> IDLE.
 *
 * The state begins at IDLE, then is transitioned either by a producer or the consumer to ENQUEUED, at which point
 * the consumer task is scheduled (or is already running). The consumer task then moves the state from ENQUEUED to RUNNING.
 * When the consumer is done writing to the netty channel, it moves the state back to IDLE. There is some special handling,
 * primarily in {@link #maybeEnqueueConsumerTask()}, to catch for races between producers and consumer wrt the {@link #backlog}
 * and changing the state.
 *
 * When reading this code, it is important to know what thread a given method will be invoked on. To that end,
 * the important methods have javadoc comments that state which thread they will/should execute on.
 * To that end, the {@link #backlog} is treated like a MPSC queue (Multi-producer, single-consumer), where any thread
 * can write to it, but only the {@link #eventLoop} should consume from it.
 */
public class OutboundMessagingConnection
{
    static final Logger logger = LoggerFactory.getLogger(OutboundMessagingConnection.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 30L, TimeUnit.SECONDS);

    private static final String INTRADC_TCP_NODELAY_PROPERTY = Config.PROPERTY_PREFIX + "otc_intradc_tcp_nodelay";

    /**
     * Enabled/disable TCP_NODELAY for intradc connections. Defaults to enabled.
     */
    private static final boolean INTRADC_TCP_NODELAY = Boolean.parseBoolean(System.getProperty(INTRADC_TCP_NODELAY_PROPERTY, "true"));

    /**
     * Number of milliseconds between connection createRetry attempts.
     */
    private static final int OPEN_RETRY_DELAY_MS = 200;

    /**
     * A minimum number of milliseconds to wait for a connection (TCP socket connect + handshake)
     */
    private static final int MINIMUM_CONNECT_TIMEOUT_MS = 2000;

    /**
     * Some (artificial) max number of time to attempt to connect at a given time.
     */
    private static final int MAX_RECONNECT_ATTEMPTS = 10;

    private static final String BACKLOG_PURGE_SIZE_PROPERTY = Config.PROPERTY_PREFIX + "otc_backlog_purge_size";
    private static final int BACKLOG_PURGE_SIZE = Integer.getInteger(BACKLOG_PURGE_SIZE_PROPERTY, 1024);

    /**
     * A naively high threshold for the number of of messages to be allowed in the {@link #backlog}.
     */
    private static final int DEFAULT_MAX_BACKLOG_DEPTH = 16 * 1024;

    /**
     * An upper bound on the number of messages to dequeue, in order to cooperatively make use of the {@link #eventLoop}
     * (and not starve out other tasks on the same event loop).
     */
    private static final int MAX_DEQUEUE_COUNT = 32;

    /**
     * A netty channel {@link Attribute} to indicate, when a channel is closed, any backlogged messages should be purged,
     * as well. See the class-level documentation for more information.
     */
    static final AttributeKey<Boolean> PURGE_MESSAGES_CHANNEL_ATTR = AttributeKey.newInstance("purgeMessages");

    private final IInternodeAuthenticator authenticator;

    /**
     * Backlog to hold messages passed by producer threads while a consumer task  sned them one by one to
     * the Netty {@link Channel}/{@link #channelWriter}.
     */
    private final Queue<QueuedMessage> backlog;

    /**
     * A count of the items in {@link #backlog}, as we are not using a data structure with a constant-time
     * {@link Queue#size()} method.
     *
     * NOTE: this field should only be used for expiring messages, where a slight inaccurcy wrt {@link Queue#size()}
     * is acceptable.
     */
    @VisibleForTesting
    final AtomicInteger backlogSize;

    /**
     * The size threshold at which to start trying to expiry messages from the {@link #backlog}.
     */
    private final int backlogPurgeSize;

    /**
     * An upper bound for the number of elements in {@link #backlog}. Anything more than this and we should drop messages
     * in order to avoid getting into a bad place wrt memory.
     */
    private final int maxQueueDepth;

    /**
     * The minimum time (in system nanos) after which to try expiring messages from the {@link #backlog}.
     */
    @VisibleForTesting
    volatile long backlogNextExpirationTime;

    /**
     * A flag to indicate if we are either scheduled to or are actively expiring messages from the {@link #backlog}.
     */
    @VisibleForTesting
    final AtomicBoolean backlogExpirationActive;

    private final AtomicLong droppedMessageCount;
    private final AtomicLong completedMessageCount;

    private volatile OutboundConnectionIdentifier connectionId;

    private final ServerEncryptionOptions encryptionOptions;

    /**
     * A future for retrying connections.
     */
    private ScheduledFuture<?> connectionRetryFuture;

    /**
     * A future for notifying when the timeout for creating the connection and negotiating the handshake has elapsed.
     * It will be cancelled when the channel is established correctly. This future executes in the netty event loop.
     */
    private ScheduledFuture<?> connectionTimeoutFuture;

    private final CoalescingStrategy coalescingStrategy;

    /**
     * An explicit thread from a netty {@link io.netty.channel.EventLoopGroup} on which to perform all connection creation
     * and channel sending related activities.
     */
    private final EventLoop eventLoop;

    /**
     * A running count of the number of times we've tried to create a connection. Is reset upon a successful connection creation.
     *
     * As it is only referenced on the event loop, this field does not need to be volatile.
     */
    private int connectAttemptCount;

    /**
     * The netty channel, once a socket connection is established; it won't be in it's normal working state until the handshake is complete.
     */
    private volatile ChannelWriter channelWriter;

    /**
     * the target protocol version to communicate to the peer with, discovered/negotiated via handshaking
     */
    private int targetVersion;

    private final State state;

    OutboundMessagingConnection(OutboundConnectionIdentifier connectionId,
                                ServerEncryptionOptions encryptionOptions,
                                CoalescingStrategy coalescingStrategy,
                                IInternodeAuthenticator authenticator)
    {
        this (connectionId, encryptionOptions, coalescingStrategy, authenticator, BACKLOG_PURGE_SIZE, DEFAULT_MAX_BACKLOG_DEPTH);
    }

    OutboundMessagingConnection(OutboundConnectionIdentifier connectionId,
                                ServerEncryptionOptions encryptionOptions,
                                CoalescingStrategy coalescingStrategy,
                                IInternodeAuthenticator authenticator,
                                int backlogPurgeSize,
                                int maxQueueDepth)
    {
        this.connectionId = connectionId;
        this.encryptionOptions = encryptionOptions;
        this.authenticator = authenticator;
        this.backlogPurgeSize = backlogPurgeSize;
        this.coalescingStrategy = coalescingStrategy;
        this.maxQueueDepth = maxQueueDepth;

        backlog = new ConcurrentLinkedQueue<>();
        backlogSize = new AtomicInteger(0);

        backlogExpirationActive = new AtomicBoolean(false);
        droppedMessageCount = new AtomicLong(0);
        completedMessageCount = new AtomicLong(0);
        state = new State();

        eventLoop = NettyFactory.instance.outboundGroup.next();

        // We want to use the most precise protocol version we know because while there is version detection on connect(),
        // the target version might be accessed by the pool (in getConnection()) before we actually connect (as we
        // only connect when the first message is submitted). Note however that the only case where we'll connect
        // without knowing the true version of a node is if that node is a seed (otherwise, we can't know a node
        // unless it has been gossiped to us or it has connected to us, and in both cases that will set the version).
        // In that case we won't rely on that targetVersion before we're actually connected and so the version
        // detection in connect() will do its job.
        targetVersion = MessagingService.instance().getVersion(connectionId.remote());
    }

    private String loggingTag()
    {
        ChannelWriter writer = channelWriter;
        String channelId = writer != null && !writer.isClosed()
                           ? writer.channel.id().asShortText()
                           : "[no-channel]";
        return connectionId.remote() + "-" + connectionId.type() + "-" + channelId;
    }

    boolean sendMessage(MessageOut msg, int id)
    {
        return sendMessage(new QueuedMessage(msg, id));
    }

    /**
     * This is the main entry point for enqueuing a message to be sent to the remote peer.
     *
     * Can be be called from any thread.
     */
    boolean sendMessage(QueuedMessage queuedMessage)
    {
        if (isClosed())
            return false;

        maybeScheduleMessageExpiry(System.nanoTime());

        if (queuedMessage.droppable && backlogSize.get() > maxQueueDepth)
        {
            noSpamLogger.warn("{} backlog to send messages is getting critically long, dropping outbound messages", loggingTag());
            droppedMessageCount.incrementAndGet();
            return false;
        }

        backlog.add(queuedMessage);
        backlogSize.incrementAndGet();

        if (state.set(State.STATE_IDLE, State.STATE_CONSUME_ENQUEUED))
            eventLoop.submit(this::maybeStartDequeuing);

        return true;
    }

    boolean maybeScheduleMessageExpiry(long timestampNanos)
    {
        if (backlogSize.get() < backlogPurgeSize)
            return false;

        if (backlogNextExpirationTime - timestampNanos > 0)
            return false; // Expiration is not due.

        if (backlogExpirationActive.compareAndSet(false, true))
        {
            eventLoop.submit(this::expireMessages);
            return true;
        }

        return false;
    }

    /**
     * Expire elements from the queue if the queue is pretty full and expiration is not already in progress.
     * This method will only remove droppable expired entries. If no such element exists, nothing is removed from the queue.
     *
     * Note: executes on the netty event loop, primarily to preserve a MPSC relationship on the {@link #backlog}.
     */
    int expireMessages()
    {
        int expiredMessageCount = 0;

        // if this instance is closed, don't bother unsetting the backlogExpirationActive flag, as we never want to run again
        if (isClosed())
            return expiredMessageCount;

        long timestampNanos = System.nanoTime();
        try
        {

            Iterator<QueuedMessage> iter = backlog.iterator();
            while (iter.hasNext())
            {
                QueuedMessage qm = iter.next();
                if (!qm.droppable)
                    continue;
                if (!qm.isTimedOut(timestampNanos))
                    continue;
                iter.remove();
                expiredMessageCount++;
            }

            backlogSize.addAndGet(-expiredMessageCount);
            droppedMessageCount.addAndGet(expiredMessageCount);

            if (logger.isTraceEnabled())
            {
                long duration = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - timestampNanos);
                logger.trace("{} Expiration took {}μs, and expired {} messages", loggingTag(), duration, expiredMessageCount);
            }
        }
        finally
        {
            long backlogExpirationIntervalNanos = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getOtcBacklogExpirationInterval());
            backlogNextExpirationTime = timestampNanos + backlogExpirationIntervalNanos;
            backlogExpirationActive.set(false);
        }

        return expiredMessageCount;
    }

    /**
     * Peform checks on the state of this instance, before invoking {@link #dequeueMessages()}.
     *
     * Note: executes on the netty event loop.
     */
    private boolean maybeStartDequeuing()
    {
        if (isClosed())
            return false;

        if (backlog.isEmpty())
        {
            state.set(State.STATE_IDLE);
            return false;
        }

        if (channelWriter == null || channelWriter.isClosed())
        {
            if (state.set(State.STATE_IDLE))
                reconnect();
            return false;
        }

        if (!state.set(State.STATE_CONSUME_ENQUEUED, State.STATE_CONSUME_RUNNING))
            return false;

        return dequeueMessages();
    }

    /**
     * Attempts to reschedule the consumer task if there are messages remaining. The key
     */
    private void maybeEnqueueConsumerTask()
    {
        if (!backlog.isEmpty() && state.set(State.STATE_IDLE, State.STATE_CONSUME_ENQUEUED))
            eventLoop.submit(this::maybeStartDequeuing);
    }

    /**
     * Pull messages off the {@link #backlog} and write them to the netty channel until we can flush (or hit an upper bound).
     * Assumes the {@link #channelWriter} and channel are properly established, and that we are in the RUNNING state.
     *
     * Note: executes on the netty event loop.
     */
    private boolean dequeueMessages()
    {
        int sentMessages = 0;
        long timestampNanos = System.nanoTime();

        // we loop until we know a flush has been scheduled else we'll never get handleMessageResult() invoked
        while (true)
        {
            if (!channelWriter.channel.isWritable())
            {
                if (sentMessages > 0)
                    channelWriter.channel.flush();
                break;
            }

            QueuedMessage next = backlog.poll();
            if (next == null)
            {
                // if we've already dequeued at least one message (on a previous loop iteration), we do not need to
                // explicitly call flush in this case as the last loop would have seen the backlog size at zero
                // and would have already called flush.
                break;
            }

            backlogSize.decrementAndGet();

            if (!next.isTimedOut(timestampNanos))
            {
                channelWriter.write(next).addListener(future -> handleMessageResult(next, future));
                sentMessages++;

                boolean flushed = channelWriter.flush(!backlog.isEmpty());

                if (flushed)
                    break;
                else if (sentMessages == MAX_DEQUEUE_COUNT)
                {
                    channelWriter.channel.flush();
                    break;
                }
            }
            else
            {
                droppedMessageCount.incrementAndGet();
            }
        }

        // note: we have to set the state back to IDLE if we did not send a message to channelWriter,
        // because there's no other component that will do it. handleMessageResult() sets the state correctly
        // when a message *is* written to the channleWriter
        if (sentMessages == 0)
            state.set(State.STATE_IDLE);

        maybeEnqueueConsumerTask();
        return sentMessages > 0;
    }

    /**
     * Initiate all the actions required to establish a working, valid connection. This includes
     * opening the socket, negotiating the internode messaging handshake, and setting up the working
     * Netty {@link Channel}. However, this method will not block for all those actions: it will only
     * kick off the connection attempt as everything is asynchronous.
     * <p>
     * Note: this should only be invoked on the event loop.
     */
    boolean connect(boolean cancelTimeout)
    {
        // clean up any lingering connection attempts
        cleanupConnectArtifacts(cancelTimeout);

        if (isClosed())
            return false;

        logger.debug("{} connection attempt {}", loggingTag(), connectAttemptCount);

        InetAddressAndPort remote = connectionId.remote();
        if (!authenticator.authenticate(remote.address, remote.port))
        {
            logger.warn("{} Internode auth failed", loggingTag());
            //Remove the connection pool and other thread so messages aren't queued
            MessagingService.instance().destroyConnectionPool(remote);

            // don't update the state field as destroyConnectionPool() *should* call OMC.close()
            // on all the connections in the OMP for the remoteAddress
            return false;
        }

        boolean compress = shouldCompressConnection(connectionId.local(), connectionId.remote());
        maybeUpdateConnectionId();
        Bootstrap bootstrap = buildBootstrap(compress);
        ChannelFuture connectFuture = bootstrap.connect();
        connectFuture.addListener(this::connectCallback);

        long timeout = Math.max(MINIMUM_CONNECT_TIMEOUT_MS, DatabaseDescriptor.getRpcTimeout());

        if (connectionTimeoutFuture == null || connectionTimeoutFuture.isDone())
            connectionTimeoutFuture = eventLoop.schedule(() -> connectionTimeout(connectFuture), timeout, TimeUnit.MILLISECONDS);
        return true;
    }

    @VisibleForTesting
    static boolean shouldCompressConnection(InetAddressAndPort localHost, InetAddressAndPort remoteHost)
    {
        return (DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.all)
               || ((DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.dc) && !isLocalDC(localHost, remoteHost));
    }

    /**
     * After a bounce we won't necessarily know the peer's version, so we assume the peer is at least 4.0
     * and thus using a single port for secure and non-secure communication. However, during a rolling upgrade from
     * 3.0.x/3.x to 4.0, the not-yet upgraded peer is still listening on separate ports, but we don't know the peer's
     * version until we can successfully connect. Fortunately, the peer can connect to this node, at which point
     * we'll grab it's version. We then use that knowledge to use the {@link Config#ssl_storage_port} to connect on,
     * and to do that we need to update some member fields in this instance.
     *
     * Note: can be removed at 5.0
     */
    void maybeUpdateConnectionId()
    {
        if (encryptionOptions != null)
        {
            int version = MessagingService.instance().getVersion(connectionId.remote());
            if (version < targetVersion)
            {
                targetVersion = version;
                int port = MessagingService.instance().portFor(connectionId.remote());
                connectionId = connectionId.withNewConnectionPort(port);
                logger.debug("{} changing connectionId to {}, with a different port for secure communication, because peer version is {}",
                             loggingTag(), connectionId, version);
            }
        }
    }

    private Bootstrap buildBootstrap(boolean compress)
    {
        boolean tcpNoDelay = isLocalDC(connectionId.local(), connectionId.remote()) ? INTRADC_TCP_NODELAY : DatabaseDescriptor.getInterDCTcpNoDelay();
        int sendBufferSize = DatabaseDescriptor.getInternodeSendBufferSize() > 0
                             ? DatabaseDescriptor.getInternodeSendBufferSize()
                             : OutboundConnectionParams.DEFAULT_SEND_BUFFER_SIZE;

        OutboundConnectionParams params = OutboundConnectionParams.builder()
                                                                  .connectionId(connectionId)
                                                                  .callback(this::finishHandshake)
                                                                  .encryptionOptions(encryptionOptions)
                                                                  .mode(Mode.MESSAGING)
                                                                  .compress(compress)
                                                                  .coalescingStrategy(coalescingStrategy)
                                                                  .sendBufferSize(sendBufferSize)
                                                                  .tcpNoDelay(tcpNoDelay)
                                                                  .protocolVersion(targetVersion)
                                                                  .eventLoop(eventLoop)
                                                                  .build();

        return NettyFactory.instance.createOutboundBootstrap(params);
    }

    static boolean isLocalDC(InetAddressAndPort localHost, InetAddressAndPort remoteHost)
    {
        String remoteDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(remoteHost);
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(localHost);
        return remoteDC != null && remoteDC.equals(localDC);
    }

    /**
     * Handles the callback of the TCP connection attempt (not including the handshake negotiation!), and really all
     * we're handling here is the TCP connection failures. On failure, we close the channel (which should disconnect
     * the socket, if connected). If there was an {@link IOException} while trying to connect, the connection will be
     * retried after a short delay.
     * <p>
     * This method does not alter the {@link #state} as it's only evaluating the TCP connect, not TCP connect and handshake.
     * Thus, {@link #finishHandshake(HandshakeResult)} will handle any necessary state updates.
     * <p>
     * Note: this method is called from the event loop.
     *
     * @return true iff the TCP connection was established and the {@link #state} is not closed; else false.
     */
    @VisibleForTesting
    boolean connectCallback(Future<? super Void> future)
    {
        ChannelFuture channelFuture = (ChannelFuture)future;

        // make sure this instance is not (terminally) closed
        if (isClosed())
        {
            // we don't have a ChannelWriter just yet, so we have to go directly to the future's channel to close it
            channelFuture.channel().close();
            return false;
        }

        // this is the success state
        final Throwable cause = future.cause();
        if (cause == null)
            return true;

        if (cause instanceof IOException)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} unable to connect on attempt {}", loggingTag(), connectAttemptCount, cause);

            if (connectAttemptCount < MAX_RECONNECT_ATTEMPTS)
            {
                connectAttemptCount++;
                connectionRetryFuture = eventLoop.schedule(() -> connect(false), OPEN_RETRY_DELAY_MS * connectAttemptCount, TimeUnit.MILLISECONDS);
            }
            else
            {
                state.set(State.STATE_IDLE);
            }
        }
        else
        {
            JVMStabilityInspector.inspectThrowable(cause);
            logger.error("{} non-IO error attempting to connect", loggingTag(), cause);
            cleanupConnectArtifacts(true);
            state.set(State.STATE_IDLE);
        }
        return false;
    }

    /**
     * A callback for handling timeouts when creating a connection/negotiating the handshake.
     *
     * Note: this method is invoked from the netty event loop.
     *
     * @return true if there was a timeout on the connect/handshake; else false.
     */
    boolean connectionTimeout(ChannelFuture channelFuture)
    {
        cleanupConnectArtifacts(true);
        if (isClosed())
            return false;

        if (logger.isDebugEnabled())
            logger.debug("{} timed out while trying to connect", loggingTag());

        state.set(State.STATE_IDLE);
        channelFuture.channel().close();
        purgeBacklog();
        return true;
    }

    /**
     * Process the results of the handshake negotiation. This should only be invoked after {@link #connectCallback(Future)}
     * has successfully be invoked (as we need the TCP connection to be properly established before any
     * application messages can be sent).
     * <p>
     * Note: this method will be invoked from the netty event loop.
     */
    void finishHandshake(HandshakeResult result)
    {
        // clean up the connector instances before changing the state
        cleanupConnectArtifacts(true);

        if (result.negotiatedMessagingVersion != HandshakeResult.UNKNOWN_PROTOCOL_VERSION)
        {
            targetVersion = result.negotiatedMessagingVersion;
            MessagingService.instance().setVersion(connectionId.remote(), targetVersion);
        }

        switch (result.outcome)
        {
            case SUCCESS:
                assert result.channelWriter != null;

                if (isClosed() || result.channelWriter.isClosed())
                {
                    result.channelWriter.close();
                    state.set(State.STATE_IDLE);
                    purgeBacklog();
                    break;
                }

                if (logger.isDebugEnabled())
                {
                    logger.debug("{} successfully connected, compress = {}, coalescing = {}", loggingTag(),
                                 shouldCompressConnection(connectionId.local(), connectionId.remote()),
                                 coalescingStrategy != null ? coalescingStrategy : CoalescingStrategies.Strategy.DISABLED);
                }
                result.channelWriter.channel.attr(PURGE_MESSAGES_CHANNEL_ATTR).set(false);

                ChannelWriter previousChannelWriter = channelWriter;
                channelWriter = result.channelWriter;
                if (previousChannelWriter != null)
                    previousChannelWriter.close();

                if (state.set(State.STATE_CONSUME_ENQUEUED))
                    maybeStartDequeuing();
                break;
            case DISCONNECT:
                state.set(State.STATE_IDLE);
                reconnect();
                break;
            case NEGOTIATION_FAILURE:
                state.set(State.STATE_IDLE);
                purgeBacklog();
                break;
            default:
                throw new IllegalArgumentException("unhandled result type: " + result.outcome);
        }
    }

    /**
     * Cleanup member fields used when connecting to a peer.
     *
     * Note: This will execute on the event loop
     */
    private void cleanupConnectArtifacts(boolean resetConnectTimeout)
    {
        if (resetConnectTimeout)
        {
            if (connectionTimeoutFuture != null)
            {
                connectionTimeoutFuture.cancel(false);
                connectionTimeoutFuture = null;
            }

            connectAttemptCount = 0;
        }

        if (connectionRetryFuture != null)
        {
            connectionRetryFuture.cancel(false);
            connectionRetryFuture = null;
        }
    }

    int getTargetVersion()
    {
        return targetVersion;
    }

    /**
     * Handles the result of sending each message. This function will be invoked when all of the bytes of the message have
     * been TCP ack'ed, not when any other application-level response has occurred.
     *
     * Note: this function is expected to be invoked on the netty event loop.
     */
    void handleMessageResult(QueuedMessage message, Future<? super Void> future)
    {
        completedMessageCount.incrementAndGet();

        if (isClosed())
            return;

        // checking the cause() is an optimized way to tell if the operation was successful (as the cause will be null)
        Throwable cause = future.cause();
        if (cause == null)
        {
            if (!dequeueMessages())
                maybeEnqueueConsumerTask();
            return;
        }

        JVMStabilityInspector.inspectThrowable(cause);

        if (cause instanceof IOException || cause.getCause() instanceof IOException)
        {
            boolean purge = shouldPurgeBacklog(channelWriter.channel);
            if (purge)
                purgeBacklog();

            state.set(State.STATE_IDLE);
            channelWriter.close();

            // maybe reenqueue the victim message, unless we're just purging all messages
            if (!purge && message != null && message.shouldRetry())
                sendMessage(message.createRetry());
        }
        else if (future.isCancelled())
        {
            // Someone cancelled the future, which we assume meant it doesn't want the message to be sent if it hasn't
            // yet. Just ignore.
        }
        else
        {
            // Non IO exceptions are likely a programming error so let's not silence them
            logger.error("{} Unexpected error writing", loggingTag(), cause);
        }

        maybeEnqueueConsumerTask();
    }


    private boolean shouldPurgeBacklog(Channel channel)
    {
        if (!channel.attr(PURGE_MESSAGES_CHANNEL_ATTR).get())
            return false;

        channel.attr(PURGE_MESSAGES_CHANNEL_ATTR).set(false);
        return true;
    }

    /**
     * Change the IP address on which we connect to the peer. We will attempt to connect to the new address if there
     * was a previous connection, and new incoming messages as well as existing {@link #backlog} messages will be sent there.
     * Any outstanding messages in the existing channel will still be sent to the previous address (we won't/can't move them from
     * one channel to another).
     *
     * Note: this will not be invoked on the event loop.
     */
    void reconnectWithNewIp(InetAddressAndPort newAddr)
    {
        // if we're closed, ignore the request
        if (isClosed())
            return;

        // capture a reference to the current channel, in case it gets swapped out before we can call close() on it
        ChannelWriter currentChannel = channelWriter;
        connectionId = connectionId.withNewConnectionAddress(newAddr);
        reconnect();

        // lastly, push through anything remaining in the existing channel.
        if (currentChannel != null)
            currentChannel.softClose();
    }

    /**
     * Schedule {@link #connect(boolean)} to attempt to reconnect.
     */
    void reconnect()
    {
        if (state.set(State.STATE_IDLE, State.STATE_CREATING_CONNECTION))
            eventLoop.submit(() -> connect(true));
    }

    private void purgeBacklog()
    {
        backlog.clear();
        int droppedCount = backlogSize.getAndSet(0);
        droppedMessageCount.addAndGet(droppedCount);
    }

    /**
     * Permanently close this instance.
     *
     * Note: should not be invoked on the event loop, but id the {@link #backlog} needs to be purged,
     * it should be scheduled on the event loop.
     */
    public void close(boolean softClose)
    {
        if (isClosed())
            return;

        if (!softClose)
            state.close();

        cleanupConnectArtifacts(true);

        // drain the backlog
        if (channelWriter != null)
        {
            if (softClose)
            {
                channelWriter.softClose();
            }
            else
            {
                eventLoop.submit(this::purgeBacklog).awaitUninterruptibly(5, TimeUnit.SECONDS);
                channelWriter.close();
            }
        }
    }

    @Override
    public String toString()
    {
        return connectionId.toString();
    }

    public Integer getPendingMessages()
    {
        return backlog.size();
    }

    public Long getCompletedMessages()
    {
        return completedMessageCount.get();
    }

    public Long getDroppedMessages()
    {
        return droppedMessageCount.get();
    }

    /*
        methods specific to testing follow
     */

    @VisibleForTesting
    void addToBacklog(QueuedMessage msg)
    {
        backlog.add(msg);
        backlogSize.incrementAndGet();
    }

    @VisibleForTesting
    void setChannelWriter(ChannelWriter channelWriter)
    {
        this.channelWriter = channelWriter;
    }

    @VisibleForTesting
    ChannelWriter getChannelWriter()
    {
        return channelWriter;
    }

    @VisibleForTesting
    void setTargetVersion(int targetVersion)
    {
        this.targetVersion = targetVersion;
    }

    @VisibleForTesting
    OutboundConnectionIdentifier getConnectionId()
    {
        return connectionId;
    }

    @VisibleForTesting
    void setConnectionTimeoutFuture(ScheduledFuture<?> connectionTimeoutFuture)
    {
        this.connectionTimeoutFuture = connectionTimeoutFuture;
    }

    @VisibleForTesting
    ScheduledFuture<?> getConnectionTimeoutFuture()
    {
        return connectionTimeoutFuture;
    }

    public boolean isConnected()
    {
        return channelWriter != null;
    }

    public boolean isClosed()
    {
        return state.state == State.STATE_CLOSED;
    }

    // For testing only!!
    void setState(int nextState)
    {
        State.stateUpdater.set(state, nextState);
    }

    int getState()
    {
        return state.state;
    }

    static class State
    {
        /**
         * An initial/idle state; the {@link OutboundMessagingConnection#channelWriter} may or may not be established yet.
         * May transition to:
         * - {@link #STATE_CREATING_CONNECTION}
         * - {@link #STATE_CONSUME_ENQUEUED}
         * - {@link #STATE_CLOSED}
         */
        static final int STATE_IDLE = 0;

        /**
         * State to indicate that a new connection/channel is being created
         * May transition to:
         * - {@link #STATE_IDLE}
         * - {@link #STATE_CONSUME_ENQUEUED}
         * - {@link #STATE_CLOSED}
         */
        static final int STATE_CREATING_CONNECTION = 1;

        /**
         * State to indicate that the consume task has been enqueued
         * May transition to:
         * - {@link #STATE_IDLE}
         * - {@link #STATE_CONSUME_RUNNING}
         * - {@link #STATE_CLOSED}
         */
        static final int STATE_CONSUME_ENQUEUED = 2;

        /**
         * State to indicate that the consume task is executing.
         * May transition to:
         * - {@link #STATE_IDLE}
         * - {@link #STATE_CLOSED}
         */
        static final int STATE_CONSUME_RUNNING = 3;

        /**
         * State to indicate this instance is closed (not creatng new connections, should not accept more messages).
         * This is a final state and cannot be transitioned away from.
         */
        static final int STATE_CLOSED = 4;

        volatile int state;
        private static final AtomicIntegerFieldUpdater<State> stateUpdater = AtomicIntegerFieldUpdater.newUpdater(State.class, "state");

        private State()
        {
            this(STATE_IDLE);
        }

        State(int state)
        {
            this.state = state;
        }

        boolean set(int nextState)
        {
            int currentState = state;
            return isValidTransition(currentState, nextState) && stateUpdater.compareAndSet(this, currentState, nextState);
        }

        /**
         * Ensure state transitions are valid. A state is allowed to transition to itself.
         */
        static boolean isValidTransition(int initial, int next)
        {
            if (initial == next)
                return true;

            switch (initial)
            {
                case STATE_CLOSED:
                    return false;
                case STATE_IDLE:
                    return next != STATE_CONSUME_RUNNING;
                case STATE_CONSUME_ENQUEUED:
                    return next != STATE_CREATING_CONNECTION;
                case STATE_CONSUME_RUNNING:
                    return next == STATE_IDLE || next == STATE_CLOSED;
                case STATE_CREATING_CONNECTION:
                    return next != STATE_CONSUME_RUNNING;
                default:
                    throw new IllegalArgumentException("unknown starting state: " + initial);
            }
        }

        boolean set(int expectedState, int nextState)
        {
            return isValidTransition(expectedState, nextState) && stateUpdater.compareAndSet(this, expectedState, nextState);
        }

        void close()
        {
            stateUpdater.set(this, STATE_CLOSED);
        }

        @Override
        public String toString()
        {
            return Integer.toString(state);
        }
    }
}