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
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;

import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.NettyFactory.Mode;
import org.apache.cassandra.net.async.OutboundHandshakeHandler.HandshakeResult;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.jctools.queues.MpscLinkedQueue;

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
 * When reading this code, it is important to know what thread a given method will be invoked on. To that end,
 * the important methods have javadoc comments that state which thread they will/should execute on.
 * The {@link #backlog} is treated like a MPSC queue (Multi-producer, single-consumer), where any thread
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
    static final int MAX_RECONNECT_ATTEMPTS = 10;

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
    static final int MAX_DEQUEUE_COUNT = 64;

    /**
     * A netty channel {@link Attribute} to indicate, when a channel is closed, any backlogged messages should be purged,
     * as well. See the class-level documentation for more information.
     */
    static final AttributeKey<Boolean> PURGE_MESSAGES_CHANNEL_ATTR = AttributeKey.newInstance("purgeMessages");

    private final IInternodeAuthenticator authenticator;

    /**
     * Backlog to hold messages passed by producer threads while a consumer task sends the Netty {@link #channel}.
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
    @VisibleForTesting
    ScheduledFuture<?> connectionRetryFuture;

    /**
     * A future for notifying when the timeout for creating the connection and negotiating the handshake has elapsed.
     * It will be cancelled when the channel is established correctly. This future executes in the netty event loop.
     */
    @VisibleForTesting
    ScheduledFuture<?> connectionTimeoutFuture;

    private final CoalescingStrategy coalescingStrategy;

    /**
     * An explicit thread from a netty {@link io.netty.channel.EventLoopGroup} on which to perform all connection creation
     * and channel sending related activities.
     */
    private final EventLoop eventLoop;

    private final ScheduledExecutorService consumerTaskThread;

    /**
     * A running count of the number of times we've tried to create a connection. Is reset upon a successful connection creation.
     *
     * As it is only referenced on the event loop, this field does not need to be volatile.
     */
    @VisibleForTesting
    int connectAttemptCount;

    /**
     * The netty channel, once a socket connection is established; it won't be in it's normal working state until the
     * handshake is complete. Should only be referenced on the {@link #consumerTaskThread}.
     */
    private Channel channel;

    /**
     * Responsible for pulling messages off the {@link #backlog}. Should only be referenced on the {@link #consumerTaskThread}.
     */
    private MessageDequeuer messageDequeuer;

    /**
     * the target protocol version to communicate to the peer with, discovered/negotiated via handshaking
     */
    private int targetVersion;

    private volatile boolean closed;

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

        backlog = MpscLinkedQueue.newMpscLinkedQueue();
        backlogSize = new AtomicInteger(0);

        backlogExpirationActive = new AtomicBoolean(false);
        droppedMessageCount = new AtomicLong(0);
        completedMessageCount = new AtomicLong(0);

        eventLoop = NettyFactory.instance.outboundGroup.next();

        if (connectionId.type() == OutboundConnectionIdentifier.ConnectionType.LARGE_MESSAGE)
        {
            String threadName = "MessagingService-NettyOutbound-" + connectionId.remote().toString() + "-LargeMessages";
            ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory(threadName));
            scheduledThreadPoolExecutor.setRejectedExecutionHandler((r, executor) -> {});
            consumerTaskThread = scheduledThreadPoolExecutor;

        }
        else
        {
            consumerTaskThread = eventLoop;
        }

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
        Channel channel = this.channel;
        String channelId = channel != null && channel.isOpen()
                           ? channel.id().asShortText()
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
        if (closed)
            return false;

        maybeScheduleMessageExpiry(System.nanoTime());

        if (queuedMessage.droppable && backlogSize.get() > maxQueueDepth)
        {
            noSpamLogger.warn("{} backlog to send messages is getting critically long, dropping outbound messages", loggingTag());
            droppedMessageCount.incrementAndGet();
            return false;
        }

        backlog.offer(queuedMessage);

        // only schedule from a producer thread if the backlog was zero before this thread incremented it
        if (backlogSize.getAndIncrement() == 0)
            consumerTaskThread.submit(this::maybeStartDequeuing);

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
            consumerTaskThread.submit(this::expireMessages);
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
        if (closed)
            return expiredMessageCount;

        long timestampNanos = System.nanoTime();
        try
        {
            // jctools queues do not support iterators, so iterate until we can't remove a message :(
            // for reference as to why jctools has no iterator support, https://github.com/JCTools/JCTools/issues/124
            QueuedMessage qm;
            while ((qm = backlog.peek()) != null)
            {
                if (!qm.droppable || !qm.isTimedOut(timestampNanos))
                    break;

                backlog.remove();
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
        catch (Throwable t)
        {
            // this really shouldn't fail, but don't allow the state to get all messed up; so just log any errors
            logger.warn("{} problem while trying to expire backlogged messages; ignoring", loggingTag());
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
     * Peform checks on the state of this instance, before invoking {@link MessageDequeuer#dequeueMessages()}.
     *
     * Note: executes on the netty event loop.
     */
    private boolean maybeStartDequeuing()
    {
        if (closed || backlog.isEmpty())
            return false;

        if (channel == null || !channel.isOpen())
        {
            connect(true);
            return false;
        }

        return messageDequeuer.dequeueMessages();
    }

    abstract class MessageDequeuer
    {
        /**
         * Pull messages off the {@link #backlog} and write them to the netty channel until we can flush (or hit an upper bound).
         * Assumes the channel is properly established.
         *
         * Note: executes on the netty event loop.
         */
        abstract boolean dequeueMessages();

        abstract void close();

        void updateCountersAfterDequeue(int dequeuedMessages, int sendMessageCount)
        {
            // only update the Atomic fields if we actually did anything
            if (dequeuedMessages == 0)
                return;

            backlogSize.addAndGet(-dequeuedMessages);

            if (sendMessageCount > 0)
                completedMessageCount.addAndGet(sendMessageCount);
            int droppedMessages = dequeuedMessages - sendMessageCount;
            if (droppedMessages > 0)
                droppedMessageCount.addAndGet(droppedMessages);
        }
    }

    class SimpleMessageDequeuer extends MessageDequeuer
    {
        /**
         * A reusable array to capture messages to send after they are dequeued.
         * Only used on the consumer thread.
         */
        private final QueuedMessage[] messagesToSend = new QueuedMessage[MAX_DEQUEUE_COUNT];

        /**
         * {@inheritDoc}
         *
         * This method *always* needs to exit with some resumable state for the consumer task:
         *  - backlog is empty (so producers can reschedule)
         *  - we've sent messages, and there's more in the backlog: handleMessageResult will be invoked when the future (promise)
         *      is fulfilled, which will call dequeueMessages again
         *  - the consumer task is rescheduled because we didn't send any messages (they were dequeued and all expired,
         *      or channel was not writable) but there are more in the queue
         */
        public boolean dequeueMessages()
        {
            final long timestampNanos = System.nanoTime();
            int sendMessageCount = 0;
            int dequeuedMessages = 0;
            int byteCountToSend = 0;
            boolean errorOccurred = false;
            try
            {
                final long remainingChannelBytes = channel.bytesBeforeUnwritable();

                // loop to figure out which messages we should send (and also calculate the end buffer size)
                while (remainingChannelBytes - byteCountToSend > 0 && dequeuedMessages < MAX_DEQUEUE_COUNT)
                {
                    QueuedMessage next = backlog.poll();
                    if (next == null)
                        break;

                    dequeuedMessages++;

                    if (!next.isTimedOut(timestampNanos))
                    {
                        messagesToSend[sendMessageCount++] = next;
                        byteCountToSend += next.message.serializedSize(targetVersion);
                    }
                }

                if (sendMessageCount > 0)
                    sendDequeuedMessages(sendMessageCount, byteCountToSend);
            }
            catch (Throwable e)
            {
                logger.error("{} an error occurred while trying dequeue and process messages", loggingTag(), e);
                errorOccurred = true;
            }
            finally
            {
                for (int i = 0; i < sendMessageCount; i++)
                    messagesToSend[i] = null;

                if (errorOccurred)
                    sendMessageCount = 0;

                updateCountersAfterDequeue(dequeuedMessages, sendMessageCount);
            }

            if (!backlog.isEmpty())
                consumerTaskThread.submit(OutboundMessagingConnection.this::maybeStartDequeuing);

            return sendMessageCount > 0;
        }

        /**
         * Allocate a single ByteBuf, serialize the messages into it, and write/flush the buffer to the channel.
         */
        private void sendDequeuedMessages(int sendMessageCount, int byteCountToSend)
        {
            int bufSize = Ints.checkedCast(byteCountToSend + (MessageOut.MESSAGE_PREFIX_SIZE * sendMessageCount));
            ByteBuf buf = channel.alloc().directBuffer(bufSize);

            try (ByteBufDataOutputPlus outputPlus = new ByteBufDataOutputPlus(buf))
            {
                for (int i = 0; i < sendMessageCount; i++)
                {
                    QueuedMessage msg = messagesToSend[i];
                    msg.message.serialize(outputPlus, targetVersion, connectionId, msg.id, msg.timestampNanos);
                }

                // next few lines are for debugging ... massively helpful!!
                // if we allocated too much buffer for this message, we'll log here.
                // if we allocated to little buffer space, we would have hit an exception when trying to write more bytes to it
                if (buf.isWritable())
                    noSpamLogger.warn("{} reported messages size {}, actual messages size {}",
                                      loggingTag(), buf.capacity(), buf.writerIndex());

                channel.writeAndFlush(buf)
                       .addListener(OutboundMessagingConnection.this::handleMessageResult);
            }
            catch (Throwable e)
            {
                if (buf != null)
                    buf.release();
            }
        }

        public void close()
        {  /* nop */  }
    }

    class LargeMessageDequeuer extends MessageDequeuer
    {
        private static final int DEFAULT_BUFFER_SIZE = 32 * 1024;

        /**
         * Reatin a local reference to the channel to avoid any visibilty issues with the parent class's {@link #channel}.
         */
        private final Channel channel;
        private final DataOutputStreamPlus output;

        private LargeMessagePromiseAggregator promiseAggregator;

        LargeMessageDequeuer(Channel channel)
        {
            this.channel = channel;

            output = ByteBufDataOutputStreamPlus.create(channel, DEFAULT_BUFFER_SIZE, this::handleError,
                                                        30, TimeUnit.SECONDS, this::handleFuture);


            // change this to keep the channel's COB from squwking all the damn time. plus, we are using the watermark
            // & channel writability flags anyway!
            // also, set this *after* creating the ByteBufDataOutputStreamPlus, as it's ctor looks at the channel's high water mark
            channel.config().setWriteBufferHighWaterMark(1 << 24);
        }

        /**
         * remember - called on the consumer task thread - not the netty event loop!
         */
        public boolean dequeueMessages()
        {
            final long timestampNanos = System.nanoTime();
            int dequeuedMessages = 0;
            int sendMessageCount = 0;

            QueuedMessage next;
            while (!closed && (next = backlog.poll()) != null)
            {
                dequeuedMessages++;
                if (!next.isTimedOut(timestampNanos))
                {
                    sendDequeuedMessage(next);
                    sendMessageCount++;
                }
            }

            updateCountersAfterDequeue(dequeuedMessages, sendMessageCount);

            // TODO:JEB do I need to reschedule here? like SmallMessageDequeuer

            return sendMessageCount > 0;
        }

        void sendDequeuedMessage(QueuedMessage message)
        {
            ChannelPromise finalPromise = channel.newPromise();
            // even though the promises for each of the chunks (via ByteBufDataOutputStreamPlus) will be fulfilled on the event loop,
            // when a failure occurs on *this* thread, we must ensure the handleMessageResult will also be executed on the event loop.
            // hence, we schedule it to the appropriate thread
            finalPromise.addListener(future -> eventLoop.submit(() -> handleMessageResult(future)));
            promiseAggregator = new LargeMessagePromiseAggregator(finalPromise);

            try
            {
                message.message.serialize(output, targetVersion, connectionId, message.id, message.timestampNanos);
                output.flush();
                promiseAggregator.finish();
            }
            catch (Throwable t)
            {
                finalPromise.tryFailure(new IOException(t));
            }
            finally
            {
                promiseAggregator = null;
            }
        }

        /**
         * Callback from {@link ByteBufDataOutputStreamPlus} when it sends a buffer to the netty channel.
         *
         * Note: called on the same thread as {@link #sendDequeuedMessage(QueuedMessage)}.
         */
        void handleFuture(ChannelPromise promise)
        {
            promiseAggregator.add(promise);
        }

        /**
         * Callback from {@link ByteBufDataOutputStreamPlus} when an error occurs trying to writing *within* the pipeline,
         * but not due to the inability to send a buffer. Think of this method as an express mechanism to know when
         * the pipeline fails, such that we don't need to have {@link #sendDequeuedMessage(QueuedMessage)} wait for
         * all the futures to be created (they might never be created).
         *
         * Note: invoked on the netty event loop.
         */
        void handleError(Throwable t)
        {
            logger.info("{} an error occurred while trying to send a large message", t);
            close();
            channel.close();
        }

        public void close()
        {
            try
            {
                consumerTaskThread.shutdown();
                output.close();
            }
            catch (IOException e)
            {
                if (logger.isDebugEnabled())
                    logger.debug("failed to close an output stream for sending large messages, ignoring as we are closing", e);
            }
        }

        // TODO:JEB doc me - basically a thread safe version (given the use case here) of netty's PromiseAggregator
        private class LargeMessagePromiseAggregator
        {
            // will be fulfilled on the event loop
            private final ChannelPromise aggregatePromise;

            // only set on the OMC.consumerTaskThread (a/k/a the large messages thread)
            private volatile boolean doneAdding;

            // only incrememnted on the OMC.consumerTaskThread (a/k/a the large messages thread)
            private volatile int expectedCount;

            // only incremented on event loop
            private volatile int completedCount;

            // only set on the event loop
            private volatile Throwable cause;

            private LargeMessagePromiseAggregator(ChannelPromise aggregatePromise)
            {
                this.aggregatePromise = aggregatePromise;
            }

            void add(ChannelPromise promise)
            {
                expectedCount++;
                promise.addListener(this::promiseComplete);
            }

            private void promiseComplete(Future<?> future)
            {
                completedCount++;

                if (!future.isSuccess())
                    cause = future.cause();

                if (completedCount == expectedCount && doneAdding)
                    tryPromise();
            }

            void finishFail(Throwable t)
            {
                doneAdding = true;
                aggregatePromise.tryFailure(t);
            }

            void finish()
            {
                doneAdding = true;
                if (expectedCount == completedCount)
                    tryPromise();
            }

            private void tryPromise()
            {
                if (cause == null)
                    aggregatePromise.trySuccess(null);
                else
                    aggregatePromise.tryFailure(cause);
            }
        }
    }
    /**
     * Handles the result of sending each message. This function will be invoked when all of the bytes of the message have
     * been TCP ack'ed, not when any other application-level response has occurred.
     *
     * Note: this function is expected to be invoked on the netty event loop.
     */
    void handleMessageResult(Future<? super Void> future)
    {
        if (closed)
            return;

        // checking the cause() is an optimized way to tell if the operation was successful (as the cause will be null)
        Throwable cause = future.cause();
        if (cause == null)
        {
            // explicitly check if the backlog queue is empty as we haven't necessarily decremented the backlogSize
            // by the time this handler is invoked.
            if (!backlog.isEmpty())
            {
                // not calling dequeueMessages explicitly here (and instead scheduling the task) as we can get here directly in-line
                // from dequeueMessages if the buffers were able to be flushed immediately to the socket. the problem we
                // would run into is that if the backlog is constantly > 0, we would creating a huge stack size due to the recursively
                // calling dequeueMessages, and b) we starve other consumers waiting to execute.
                consumerTaskThread.submit(this::maybeStartDequeuing);
            }
            return;
        }

        JVMStabilityInspector.inspectThrowable(cause);

        if (cause instanceof IOException || (cause.getCause() != null && cause.getCause() instanceof IOException))
        {
            if (shouldPurgeBacklog(channel))
                purgeBacklog();

            messageDequeuer.close();
            channel.close();
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

        consumerTaskThread.submit(this::maybeStartDequeuing);
    }

    private boolean shouldPurgeBacklog(Channel channel)
    {
        if (!channel.attr(PURGE_MESSAGES_CHANNEL_ATTR).get())
            return false;

        channel.attr(PURGE_MESSAGES_CHANNEL_ATTR).set(false);
        return true;
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

        if (closed)
            return false;

        if (logger.isTraceEnabled())
            logger.trace("{} connection attempt {}", loggingTag(), connectAttemptCount);

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
        int tcpConnectTimeout = DatabaseDescriptor.getInternodeTcpConnectTimeoutInMS();
        int tcpUserTimeout = DatabaseDescriptor.getInternodeTcpUserTimeoutInMS();

        OutboundConnectionParams.Builder builder = OutboundConnectionParams.builder()
                                                                  .connectionId(connectionId)
                                                                  .callback(this::finishHandshake)
                                                                  .encryptionOptions(encryptionOptions)
                                                                  .mode(Mode.MESSAGING)
                                                                  .compress(compress)
                                                                  .coalescingStrategy(coalescingStrategy)
                                                                  .tcpNoDelay(tcpNoDelay)
                                                                  .tcpConnectTimeoutInMS(tcpConnectTimeout)
                                                                  .tcpUserTimeoutInMS(tcpUserTimeout)
                                                                  .protocolVersion(targetVersion)
                                                                  .eventLoop(eventLoop);

          if (DatabaseDescriptor.getInternodeSendBufferSize() > 0)
              builder.sendBufferSize(DatabaseDescriptor.getInternodeSendBufferSize());

        return NettyFactory.instance.createOutboundBootstrap(builder.build());
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
     * This method does not alter ant state as it's only evaluating the TCP connect, not TCP connect and handshake.
     * Thus, {@link #finishHandshake(HandshakeResult)} will handle any necessary state updates.
     * <p>
     * Note: this method is called from the event loop.
     *
     * @return true iff the TCP connection was established and this instance is not closed; else false.
     */
    @VisibleForTesting
    boolean connectCallback(Future<? super Void> future)
    {
        ChannelFuture channelFuture = (ChannelFuture)future;

        // make sure this instance is not (terminally) closed
        if (closed)
        {
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
            // if we've met the MAX_RECONNECT_ATTEMPTS, don't bother to schedule anything as the connectionTimeout will kick in
            // and change enough state to allow future reconnect attempts
        }
        else
        {
            JVMStabilityInspector.inspectThrowable(cause);
            logger.error("{} non-IO error attempting to connect", loggingTag(), cause);
            cleanupConnectArtifacts(true);
            maybeReconnect();
        }
        return false;
    }

    void maybeReconnect()
    {
        expireMessages();
        if (backlogSize.get() > 0)
            connect(true);
    }

    /**
     * A callback for handling timeouts when creating a connection/negotiating the handshake.
     *
     * Note: this method is invoked from the netty event loop.
     */
    void connectionTimeout(ChannelFuture channelFuture)
    {
        if (logger.isDebugEnabled())
            logger.debug("{} timed out while trying to connect", loggingTag());

        cleanupConnectArtifacts(true);
        channelFuture.channel().close();
        purgeBacklog();
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

        if (closed)
        {
            if (result.channel != null)
                result.channel.close();
            purgeBacklog(); // probably purged when the instance was closed, but this won't hurt
            return;
        }

        switch (result.outcome)
        {
            case SUCCESS:
                if (messageDequeuer != null)
                    messageDequeuer.close();
                if (channel != null)
                    channel.close();
                channel = result.channel;
                channel.attr(PURGE_MESSAGES_CHANNEL_ATTR).set(false);

                if (logger.isTraceEnabled())
                    logger.trace("{} successfully connected, compress = {}, coalescing = {}", loggingTag(),
                                 shouldCompressConnection(connectionId.local(), connectionId.remote()),
                                 coalescingStrategy != null ? coalescingStrategy : CoalescingStrategies.Strategy.DISABLED);

                messageDequeuer = deriveMessageDequeuer(connectionId, channel);
                consumerTaskThread.submit(this::maybeStartDequeuing);
                break;
            case DISCONNECT:
                maybeReconnect();
                break;
            case NEGOTIATION_FAILURE:
                purgeBacklog();
                break;
            default:
                throw new AssertionError("unhandled result type: " + result.outcome);
        }
    }

    private MessageDequeuer deriveMessageDequeuer(OutboundConnectionIdentifier connectionId, Channel channel)
    {
        return connectionId.type() == OutboundConnectionIdentifier.ConnectionType.LARGE_MESSAGE
               ? new LargeMessageDequeuer(channel)
               : new SimpleMessageDequeuer();
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
     * Change the IP address on which we connect to the peer. We will attempt to connect to the new address if there
     * was a previous connection, and new incoming messages as well as existing {@link #backlog} messages will be sent there.
     * Any outstanding messages in the existing channel will still be sent to the previous address (we won't/can't move them from
     * one channel to another).
     *
     * Note: this will not be invoked on the event loop.
     */
    java.util.concurrent.Future<?> reconnectWithNewIp(InetAddressAndPort newAddr)
    {
        // if we're closed, ignore the request
        if (closed)
            return null;

        connectionId = connectionId.withNewConnectionAddress(newAddr);

        return consumerTaskThread.submit(() -> {
            if (channel != null)
                softClose();
        });
    }

    private void softClose()
    {
        if (closed)
            return;

        closed = true;
        channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }

    private void purgeBacklog()
    {
        int droppedCount = backlogSize.getAndSet(0);
        backlog.clear();
        droppedMessageCount.addAndGet(droppedCount);
    }

    /**
     * Permanently close this instance.
     *
     * Note: can be invoked outside the event loop, but certain cleanup activities are required to
     * execute on the event loop.
     */
    public void close(boolean softClose)
    {
        if (closed)
            return;

        if (!softClose)
            closed = true;

        try
        {
            consumerTaskThread.submit(() -> {
                cleanupConnectArtifacts(true);

                // drain the backlog
                if (channel != null)
                {
                    if (softClose)
                    {
                        softClose();
                    }
                    else
                    {
                        purgeBacklog();
                        channel.close();
                    }

                    messageDequeuer.close();
                }

            }).get(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            // ignore any errors
        }
    }

    @Override
    public String toString()
    {
        return connectionId.toString();
    }

    public Integer getPendingMessages()
    {
        return backlogSize.get();
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
    void setChannel(Channel channel)
    {
        this.channel = channel;
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

    public boolean isConnected()
    {
        return channel != null && channel.isOpen();
    }

    boolean isClosed()
    {
        return closed;
    }

    // For testing only!!
    void unsafeSetClosed(boolean closed)
    {
        this.closed = closed;
    }

    @VisibleForTesting
    void setMessageDequeuer(MessageDequeuer dequeuer)
    {
        this.messageDequeuer = dequeuer;
    }
}