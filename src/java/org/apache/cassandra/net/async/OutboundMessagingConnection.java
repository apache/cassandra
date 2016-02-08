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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.NettyFactory.Mode;
import org.apache.cassandra.net.async.OutboundHandshakeHandler.HandshakeResult;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Represents one connection to a peer, and handles the state transistions on the connection and the netty {@link Channel}
 * The underlying socket is not opened until explicitly requested (by sending a message).
 *
 * The basic setup for the channel is like this: a message is requested to be sent via {@link #sendMessage(MessageOut, int)}.
 * If the channel is not established, then we need to create it (obviously). To prevent multiple threads from creating
 * independent connections, they attempt to update the {@link #state}; one thread will win the race and create the connection.
 * Upon sucessfully setting up the connection/channel, the {@link #state} will be updated again (to {@link State#READY},
 * which indicates to other threads that the channel is ready for business and can be written to.
 *
 */
public class OutboundMessagingConnection
{
    static final Logger logger = LoggerFactory.getLogger(OutboundMessagingConnection.class);
    private static final NoSpamLogger errorLogger = NoSpamLogger.getLogger(logger, 10, TimeUnit.SECONDS);

    private static final String INTRADC_TCP_NODELAY_PROPERTY = Config.PROPERTY_PREFIX + "otc_intradc_tcp_nodelay";

    /**
     * Enabled/disable TCP_NODELAY for intradc connections. Defaults to enabled.
     */
    private static final boolean INTRADC_TCP_NODELAY = Boolean.parseBoolean(System.getProperty(INTRADC_TCP_NODELAY_PROPERTY, "true"));

    /**
     * Number of milliseconds between connection createRetry attempts.
     */
    private static final int OPEN_RETRY_DELAY_MS = 100;

    /**
     * A minimum number of milliseconds to wait for a connection (TCP socket connect + handshake)
     */
    private static final int MINIMUM_CONNECT_TIMEOUT_MS = 2000;
    private final IInternodeAuthenticator authenticator;

    /**
     * Describes this instance's ability to send messages into it's Netty {@link Channel}.
     */
    enum State
    {
        /** waiting to create the connection */
        NOT_READY,
        /** we've started to create the connection/channel */
        CREATING_CHANNEL,
        /** channel is established and we can send messages */
        READY,
        /** a dead state which should not be transitioned away from */
        CLOSED
    }

    /**
     * Backlog to hold messages passed by upstream threads while the Netty {@link Channel} is being set up or recreated.
     */
    private final Queue<QueuedMessage> backlog;

    /**
     * Reference to a {@link ScheduledExecutorService} rther than directly depending on something like {@link ScheduledExecutors}.
     */
    private final ScheduledExecutorService scheduledExecutor;

    final AtomicLong droppedMessageCount;
    final AtomicLong completedMessageCount;

    private volatile OutboundConnectionIdentifier connectionId;

    private final ServerEncryptionOptions encryptionOptions;

    /**
     * A future for retrying connections. Bear in mind that this future does not execute in the
     * netty event event loop, so there's some races to be careful of.
     */
    private volatile ScheduledFuture<?> connectionRetryFuture;

    /**
     * A future for notifying when the timeout for creating the connection and negotiating the handshake has elapsed.
     * It will be cancelled when the channel is established correctly. Bear in mind that this future does not execute in the
     * netty event event loop, so there's some races to be careful of.
     */
    private volatile ScheduledFuture<?> connectionTimeoutFuture;

    private final AtomicReference<State> state;

    private final Optional<CoalescingStrategy> coalescingStrategy;

    /**
     * A running count of the number of times we've tried to create a connection.
     */
    private volatile int connectAttemptCount;

    /**
     * The netty channel, once a socket connection is established; it won't be in it's normal working state until the handshake is complete.
     */
    private volatile ChannelWriter channelWriter;

    /**
     * the target protocol version to communicate to the peer with, discovered/negotiated via handshaking
     */
    private int targetVersion;

    OutboundMessagingConnection(OutboundConnectionIdentifier connectionId,
                                ServerEncryptionOptions encryptionOptions,
                                Optional<CoalescingStrategy> coalescingStrategy,
                                IInternodeAuthenticator authenticator)
    {
        this(connectionId, encryptionOptions, coalescingStrategy, authenticator, ScheduledExecutors.scheduledFastTasks);
    }

    @VisibleForTesting
    OutboundMessagingConnection(OutboundConnectionIdentifier connectionId,
                                ServerEncryptionOptions encryptionOptions,
                                Optional<CoalescingStrategy> coalescingStrategy,
                                IInternodeAuthenticator authenticator,
                                ScheduledExecutorService sceduledExecutor)
    {
        this.connectionId = connectionId;
        this.encryptionOptions = encryptionOptions;
        this.authenticator = authenticator;
        backlog = new ConcurrentLinkedQueue<>();
        droppedMessageCount = new AtomicLong(0);
        completedMessageCount = new AtomicLong(0);
        state = new AtomicReference<>(State.NOT_READY);
        this.scheduledExecutor = sceduledExecutor;
        this.coalescingStrategy = coalescingStrategy;

        // We want to use the most precise protocol version we know because while there is version detection on connect(),
        // the target version might be accessed by the pool (in getConnection()) before we actually connect (as we
        // only connect when the first message is submitted). Note however that the only case where we'll connect
        // without knowing the true version of a node is if that node is a seed (otherwise, we can't know a node
        // unless it has been gossiped to us or it has connected to us, and in both cases that will set the version).
        // In that case we won't rely on that targetVersion before we're actually connected and so the version
        // detection in connect() will do its job.
        targetVersion = MessagingService.instance().getVersion(connectionId.remote());
    }

    /**
     * If the connection is set up and ready to use (the normal case), simply send the message to it and return.
     * Otherwise, one lucky thread is selected to create the Channel, while other threads just add the {@code msg} to
     * the backlog queue.
     *
     * @return true if the message was accepted by the {@link #channelWriter}; else false if it was not accepted
     * and added to the backlog or the channel is {@link State#CLOSED}. See documentation in {@link ChannelWriter} and
     * {@link MessageOutHandler} how the backlogged messages get consumed.
     */
    boolean sendMessage(MessageOut msg, int id)
    {
        return sendMessage(new QueuedMessage(msg, id));
    }

    boolean sendMessage(QueuedMessage queuedMessage)
    {
        State state = this.state.get();
        if (state == State.READY)
        {
            if (channelWriter.write(queuedMessage, false))
                return true;

            backlog.add(queuedMessage);
            return false;
        }
        else if (state == State.CLOSED)
        {
            errorLogger.warn("trying to write message to a closed connection");
            return false;
        }
        else
        {
            backlog.add(queuedMessage);
            connect();
            return true;
        }
    }

    /**
     * Initiate all the actions required to establish a working, valid connection. This includes
     * opening the socket, negotiating the internode messaging handshake, and setting up the working
     * Netty {@link Channel}. However, this method will not block for all those actions: it will only
     * kick off the connection attempt as everything is asynchronous.
     * <p>
     * Threads compete to update the {@link #state} field to {@link State#CREATING_CHANNEL} to ensure only one
     * connection is attempted at a time.
     *
     * @return true if kicking off the connection attempt was started by this thread; else, false.
     */
    public boolean connect()
    {
        // try to be the winning thread to create the channel
        if (!state.compareAndSet(State.NOT_READY, State.CREATING_CHANNEL))
            return false;

        // clean up any lingering connection attempts
        if (connectionTimeoutFuture != null)
        {
            connectionTimeoutFuture.cancel(false);
            connectionTimeoutFuture = null;
        }

        return tryConnect();
    }

    private boolean tryConnect()
    {
        if (state.get() != State.CREATING_CHANNEL)
                return false;

        logger.debug("connection attempt {} to {}", connectAttemptCount, connectionId);


        InetSocketAddress remote = connectionId.remoteAddress();
        if (!authenticator.authenticate(remote.getAddress(), remote.getPort()))
        {
            logger.warn("Internode auth failed connecting to {}", connectionId);
            //Remove the connection pool and other thread so messages aren't queued
            MessagingService.instance().destroyConnectionPool(remote.getAddress());

            // don't update the state field as destroyConnectionPool() *should* call OMC.close()
            // on all the connections in the OMP for the remoteAddress
            return false;
        }

        boolean compress = shouldCompressConnection(connectionId.local(), connectionId.remote());
        Bootstrap bootstrap = buildBootstrap(compress);

        ChannelFuture connectFuture = bootstrap.connect();
        connectFuture.addListener(this::connectCallback);

        long timeout = Math.max(MINIMUM_CONNECT_TIMEOUT_MS, DatabaseDescriptor.getRpcTimeout());
        if (connectionTimeoutFuture == null || connectionTimeoutFuture.isDone())
            connectionTimeoutFuture = scheduledExecutor.schedule(() -> connectionTimeout(connectFuture), timeout, TimeUnit.MILLISECONDS);
        return true;
    }

    @VisibleForTesting
    static boolean shouldCompressConnection(InetAddress localHost, InetAddress remoteHost)
    {
        return (DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.all)
               || ((DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.dc) && !isLocalDC(localHost, remoteHost));
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
                                                                  .backlogSupplier(() -> nextBackloggedMessage())
                                                                  .messageResultConsumer(this::handleMessageResult)
                                                                  .protocolVersion(targetVersion)
                                                                  .build();

        return NettyFactory.instance.createOutboundBootstrap(params);
    }

    private QueuedMessage nextBackloggedMessage()
    {
        QueuedMessage msg = backlog.poll();
        if (msg == null)
            return null;

        if (!msg.isTimedOut())
            return msg;

        if (msg.shouldRetry())
            return msg.createRetry();

        droppedMessageCount.incrementAndGet();
        return null;
    }

    static boolean isLocalDC(InetAddress localHost, InetAddress remoteHost)
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
     * Note: this method is called from the event loop, so be careful wrt thread visibility
     *
     * @return true iff the TCP connection was established and the {@link #state} is not {@link State#CLOSED}; else false.
     */
    @VisibleForTesting
    boolean connectCallback(Future<? super Void> future)
    {
        ChannelFuture channelFuture = (ChannelFuture)future;

        // make sure this instance is not (terminally) closed
        if (state.get() == State.CLOSED)
        {
            channelFuture.channel().close();
            return false;
        }

        // this is the success state
        final Throwable cause = future.cause();
        if (cause == null)
        {
            connectAttemptCount = 0;
            return true;
        }

        setStateIfNotClosed(state, State.NOT_READY);
        if (cause instanceof IOException)
        {
            logger.trace("unable to connect on attempt {} to {}", connectAttemptCount, connectionId, cause);
            connectAttemptCount++;
            connectionRetryFuture = scheduledExecutor.schedule(this::connect, OPEN_RETRY_DELAY_MS * connectAttemptCount, TimeUnit.MILLISECONDS);
        }
        else
        {
            JVMStabilityInspector.inspectThrowable(cause);
            logger.error("non-IO error attempting to connect to {}", connectionId, cause);
        }
        return false;
    }

    /**
     * A callback for handling timeouts when creating a connection/negotiating the handshake.
     * <p>
     * Note: this method is *not* invoked from the netty event loop,
     * so there's an inherent race with {@link #finishHandshake(HandshakeResult)},
     * as well as any possible connect() reattempts (a seemingly remote race condition, however).
     * Therefore, this function tries to lose any races, as much as possible.
     *
     * @return true if there was a timeout on the connect/handshake; else false.
     */
    boolean connectionTimeout(ChannelFuture channelFuture)
    {
        if (connectionRetryFuture != null)
        {
            connectionRetryFuture.cancel(false);
            connectionRetryFuture = null;
        }
        connectAttemptCount = 0;
        State initialState = state.get();
        if (initialState == State.CLOSED)
            return true;

        if (initialState != State.READY)
        {
            logger.debug("timed out while trying to connect to {}", connectionId);

            channelFuture.channel().close();
            // a last-ditch attempt to let finishHandshake() win the race
            if (state.compareAndSet(initialState, State.NOT_READY))
            {
                backlog.clear();
                return true;
            }
        }
        return false;
    }

    /**
     * Process the results of the handshake negotiation.
     * <p>
     * Note: this method will be invoked from the netty event loop,
     * so there's an inherent race with {@link #connectionTimeout(ChannelFuture)}.
     */
    void finishHandshake(HandshakeResult result)
    {
        // clean up the connector instances before changing the state
        if (connectionTimeoutFuture != null)
        {
            connectionTimeoutFuture.cancel(false);
            connectionTimeoutFuture = null;
        }
        if (connectionRetryFuture != null)
        {
            connectionRetryFuture.cancel(false);
            connectionRetryFuture = null;
        }
        connectAttemptCount = 0;

        if (result.negotiatedMessagingVersion != HandshakeResult.UNKNOWN_PROTOCOL_VERSION)
        {
            targetVersion = result.negotiatedMessagingVersion;
            MessagingService.instance().setVersion(connectionId.remote(), targetVersion);
        }

        switch (result.outcome)
        {
            case SUCCESS:
                assert result.channelWriter != null;
                logger.debug("successfully connected to {}, conmpress = {}, coalescing = {}", connectionId,
                             shouldCompressConnection(connectionId.local(), connectionId.remote()),
                             coalescingStrategy.isPresent() ? coalescingStrategy.get() : CoalescingStrategies.Strategy.DISABLED);
                if (state.get() == State.CLOSED)
                {
                    result.channelWriter.close();
                    backlog.clear();
                    break;
                }
                channelWriter = result.channelWriter;
                // drain the backlog to the channel
                channelWriter.writeBacklog(backlog, true);
                // change the state so newly incoming messages can be sent to the channel (without adding to the backlog)
                setStateIfNotClosed(state, State.READY);
                // ship out any stragglers that got added to the backlog
                channelWriter.writeBacklog(backlog, true);
                break;
            case DISCONNECT:
                reconnect();
                break;
            case NEGOTIATION_FAILURE:
                setStateIfNotClosed(state, State.NOT_READY);
                backlog.clear();
                break;
            default:
                throw new IllegalArgumentException("unhandled result type: " + result.outcome);
        }
    }

    @VisibleForTesting
    static boolean setStateIfNotClosed(AtomicReference<State> state, State newState)
    {
        State s = state.get();
        if (s == State.CLOSED)
            return false;
        state.set(newState);
        return true;
    }

    int getTargetVersion()
    {
        return targetVersion;
    }

    /**
     * Handles the result of each message sent.
     *
     * Note: this function is expected to be invoked on the netty event loop. Also, do not retain any state from
     * the input {@code messageResult}.
     */
    void handleMessageResult(MessageResult messageResult)
    {
        completedMessageCount.incrementAndGet();

        // checking the cause() is an optimized way to tell if the operation was successful (as the cause will be null)
        // Note that ExpiredException is just a marker for timeout-ed message we're dropping, but as we already
        // incremented the dropped message count in MessageOutHandler, we have nothing to do.
        Throwable cause = messageResult.future.cause();
        if (cause == null)
            return;

        if (cause instanceof ExpiredException)
        {
            droppedMessageCount.incrementAndGet();
            return;
        }

        JVMStabilityInspector.inspectThrowable(cause);

        if (cause instanceof IOException || cause.getCause() instanceof IOException)
        {
            ChannelWriter writer = messageResult.writer;
            if (writer.shouldPurgeBacklog())
                purgeBacklog();

            // This writer needs to be closed and we need to trigger a reconnection. We really only want to do that
            // once for this channel however (and again, no race because we're on the netty event loop).
            if (!writer.isClosed() && messageResult.allowReconnect)
            {
                reconnect();
                writer.close();
            }

            QueuedMessage msg = messageResult.msg;
            if (msg != null && msg.shouldRetry())
            {
                sendMessage(msg.createRetry());
            }
        }
        else if (messageResult.future.isCancelled())
        {
            // Someone cancelled the future, which we assume meant it doesn't want the message to be sent if it hasn't
            // yet. Just ignore.
        }
        else
        {
            // Non IO exceptions are likely a programming error so let's not silence them
            logger.error("Unexpected error writing on " + connectionId, cause);
        }
    }

    /**
     * Change the IP address on which we connect to the peer. We will attempt to connect to the new address if there
     * was a previous connection, and new incoming messages as well as existing {@link #backlog} messages will be sent there.
     * Any outstanding messages in the existing channel will still be sent to the previous address (we won't/can't move them from
     * one channel to another).
     */
    void reconnectWithNewIp(InetSocketAddress newAddr)
    {
        State currentState = state.get();

        // if we're closed, ignore the request
        if (currentState == State.CLOSED)
            return;

        // capture a reference to the current channel, in case it gets swapped out before we can call close() on it
        ChannelWriter currentChannel = channelWriter;
        connectionId = connectionId.withNewConnectionAddress(newAddr);

        if (currentState != State.NOT_READY)
            reconnect();

        // lastly, push through anything remaining in the existing channel.
        if (currentChannel != null)
            currentChannel.softClose();
    }

    /**
     * Sets the state properly so {@link #connect()} can attempt to reconnect.
     */
    void reconnect()
    {
        if (setStateIfNotClosed(state, State.NOT_READY))
            connect();
    }

    void purgeBacklog()
    {
        backlog.clear();
    }

    public void close(boolean softClose)
    {
        state.set(State.CLOSED);

        if (connectionTimeoutFuture != null)
        {
            connectionTimeoutFuture.cancel(false);
            connectionTimeoutFuture = null;
        }

        // drain the backlog
        if (channelWriter != null)
        {
            if (softClose)
            {
                channelWriter.writeBacklog(backlog, false);
                channelWriter.softClose();
            }
            else
            {
                backlog.clear();
                channelWriter.close();
            }

            channelWriter = null;
        }
    }

    @Override
    public String toString()
    {
        return connectionId.toString();
    }

    public Integer getPendingMessages()
    {
        int pending = backlog.size();
        ChannelWriter chan = channelWriter;
        if (chan != null)
            pending += (int)chan.pendingMessageCount();
        return pending;
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
    int backlogSize()
    {
        return backlog.size();
    }

    @VisibleForTesting
    void addToBacklog(QueuedMessage msg)
    {
        backlog.add(msg);
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
    void setState(State state)
    {
        this.state.set(state);
    }

    @VisibleForTesting
    State getState()
    {
        return state.get();
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
        return state.get() == State.READY;
    }
}