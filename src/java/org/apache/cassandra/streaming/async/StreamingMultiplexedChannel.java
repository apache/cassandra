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

import java.io.IOError;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedByInterruptException;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future; // checkstyle: permit this import
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.StreamDeserializingTask;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.IncomingStreamMessage;
import org.apache.cassandra.streaming.messages.KeepAliveMessage;
import org.apache.cassandra.streaming.messages.OutgoingStreamMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.Semaphore;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static com.google.common.base.Throwables.getRootCause;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.*;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.STREAMING_SESSION_PARALLELTRANSFERS;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.streaming.StreamSession.createLogTag;
import static org.apache.cassandra.streaming.messages.StreamMessage.serialize;
import static org.apache.cassandra.streaming.messages.StreamMessage.serializedSize;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.FBUtilities.getAvailableProcessors;
import static org.apache.cassandra.utils.JVMStabilityInspector.inspectThrowable;
import static org.apache.cassandra.utils.concurrent.BlockingQueues.newBlockingQueue;
import static org.apache.cassandra.utils.concurrent.Semaphore.newFairSemaphore;

/**
 * Responsible for sending {@link StreamMessage}s to a given peer. We manage an array of netty {@link Channel}s
 * for sending {@link OutgoingStreamMessage} instances; all other {@link StreamMessage} types are sent via
 * a special control channel. The reason for this is to treat those messages carefully and not let them get stuck
 * behind a stream transfer.
 *
 * One of the challenges when sending streams is we might need to delay shipping the stream if:
 *
 * - we've exceeded our network I/O use due to rate limiting (at the cassandra level)
 * - the receiver isn't keeping up, which causes the local TCP socket buffer to not empty, which causes epoll writes to not
 * move any bytes to the socket, which causes buffers to stick around in user-land (a/k/a cassandra) memory.
 *
 * When those conditions occur, it's easy enough to reschedule processing the stream once the resources pick up
 * (we acquire the permits from the rate limiter, or the socket drains). However, we need to ensure that
 * no other messages are submitted to the same channel while the current stream is still being processed.
 */
public class StreamingMultiplexedChannel
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingMultiplexedChannel.class);

    private static final int DEFAULT_MAX_PARALLEL_TRANSFERS = getAvailableProcessors();
    private static final int MAX_PARALLEL_TRANSFERS = STREAMING_SESSION_PARALLELTRANSFERS.getInt(DEFAULT_MAX_PARALLEL_TRANSFERS);

    // a simple mechansim for allowing a degree of fairness across multiple sessions
    private static final Semaphore fileTransferSemaphore = newFairSemaphore(DEFAULT_MAX_PARALLEL_TRANSFERS);

    private final StreamingChannel.Factory factory;
    private final InetAddressAndPort to;
    private final StreamSession session;
    private final int messagingVersion;

    private volatile boolean closed;

    /**
     * A special {@link Channel} for sending non-stream streaming messages, basically anything that isn't an
     * {@link OutgoingStreamMessage} (or an {@link IncomingStreamMessage}, but a node doesn't send that, it's only received).
     */
    private volatile StreamingChannel controlChannel;

    // note: this really doesn't need to be a LBQ, just something that's thread safe
    private final Collection<ScheduledFuture<?>> channelKeepAlives = newBlockingQueue();

    private final ExecutorPlus fileTransferExecutor;

    /**
     * A mapping of each {@link #fileTransferExecutor} thread to a channel that can be written to (on that thread).
     */
    private final ConcurrentMap<Thread, StreamingChannel> threadToChannelMap = new ConcurrentHashMap<>();

    public StreamingMultiplexedChannel(StreamSession session, StreamingChannel.Factory factory, InetAddressAndPort to, @Nullable StreamingChannel controlChannel, int messagingVersion)
    {
        this.session = session;
        this.factory = factory;
        this.to = to;
        assert messagingVersion >= VERSION_40;
        this.messagingVersion = messagingVersion;
        this.controlChannel = controlChannel;

        String name = session.peer.toString().replace(':', '.');
        fileTransferExecutor = executorFactory()
                .configurePooled("NettyStreaming-Outbound-" + name, MAX_PARALLEL_TRANSFERS)
                .withKeepAlive(1L, SECONDS).build();
    }



    public InetAddressAndPort peer()
    {
        return to;
    }

    public InetSocketAddress connectedTo()
    {
        return controlChannel == null ? to : controlChannel.connectedTo();
    }

    /**
     * Used by initiator to setup control message channel connecting to follower
     */
    private void setupControlMessageChannel() throws IOException
    {
        if (controlChannel == null)
        {
            /*
             * Inbound handlers are needed:
             *  a) for initiator's control channel(the first outbound channel) to receive follower's message.
             *  b) for streaming receiver (note: both initiator and follower can receive streaming files) to reveive files,
             *     in {@link Handler#setupStreamingPipeline}
             */
            controlChannel = createControlChannel();
        }
    }

    private StreamingChannel createControlChannel() throws IOException
    {
        logger.debug("Creating stream session to {} as {}", to, session.isFollower() ? "follower" : "initiator");

        StreamingChannel channel = factory.create(to, messagingVersion, StreamingChannel.Kind.CONTROL);
        executorFactory().startThread(String.format("Stream-Deserializer-%s-%s", to.toString(), channel.id()),
                                      new StreamDeserializingTask(session, channel, messagingVersion));

        session.attachInbound(channel);
        session.attachOutbound(channel);

        scheduleKeepAliveTask(channel);

        logger.debug("Creating control {}", channel.description());
        return channel;
    }
    
    private StreamingChannel createFileChannel(InetAddressAndPort connectTo) throws IOException
    {
        logger.debug("Creating stream session to {} as {}", to, session.isFollower() ? "follower" : "initiator");

        StreamingChannel channel = factory.create(to, connectTo, messagingVersion, StreamingChannel.Kind.FILE);
        session.attachOutbound(channel);

        logger.debug("Creating file {}", channel.description());
        return channel;
    }

    public Future<?> sendControlMessage(StreamMessage message)
    {
        try
        {
            setupControlMessageChannel();
            return sendMessage(controlChannel, message);
        }
        catch (Exception e)
        {
            close();
            session.onError(e);
            return ImmediateFuture.failure(e);
        }

    }
    public Future<?> sendMessage(StreamingChannel channel, StreamMessage message)
    {
        if (closed)
            throw new RuntimeException("stream has been closed, cannot send " + message);

        if (message instanceof OutgoingStreamMessage)
        {
            if (session.isPreview())
                throw new RuntimeException("Cannot send stream data messages for preview streaming sessions");
            if (logger.isDebugEnabled())
                logger.debug("{} Sending {}", createLogTag(session), message);

            InetAddressAndPort connectTo = factory.supportsPreferredIp() ? SystemKeyspace.getPreferredIP(to) : to;
            return fileTransferExecutor.submit(new FileStreamTask((OutgoingStreamMessage) message, connectTo));
        }

        try
        {
            Future<?> promise = channel.send(outSupplier -> {
                // we anticipate that the control messages are rather small, so allocating a ByteBuf shouldn't  blow out of memory.
                long messageSize = serializedSize(message, messagingVersion);
                if (messageSize > 1 << 30)
                {
                    throw new IllegalStateException(format("%s something is seriously wrong with the calculated stream control message's size: %d bytes, type is %s",
                                                           createLogTag(session, controlChannel.id()), messageSize, message.type));
                }
                try (StreamingDataOutputPlus out = outSupplier.apply((int) messageSize))
                {
                    StreamMessage.serialize(message, out, messagingVersion, session);
                }
            });
            promise.addListener(future -> onMessageComplete(future, message));
            return promise;
        }
        catch (Exception e)
        {
            close();
            session.onError(e);
            return ImmediateFuture.failure(e);
        }
    }

    /**
     * Decides what to do after a {@link StreamMessage} is processed.
     *
     * Note: this is called from the netty event loop.
     *
     * @return null if the message was processed successfully; else, a {@link java.util.concurrent.Future} to indicate
     * the status of aborting any remaining tasks in the session.
     */
    Future<?> onMessageComplete(Future<?> future, StreamMessage msg)
    {
        Throwable cause = future.cause();
        if (cause == null)
            return null;

        Channel channel = future instanceof ChannelFuture ? ((ChannelFuture)future).channel() : null;
        logger.error("{} failed to send a stream message/data to peer {}: msg = {}",
                     createLogTag(session, channel), to, msg, future.cause());

        // StreamSession will invoke close(), but we have to mark this sender as closed so the session doesn't try
        // to send any failure messages
        return session.onError(cause);
    }

    class FileStreamTask implements Runnable
    {
        /**
         * Time interval, in minutes, to wait between logging a message indicating that we're waiting on a semaphore
         * permit to become available.
         */
        private static final int SEMAPHORE_UNAVAILABLE_LOG_INTERVAL = 3;

        /**
         * Even though we expect only an {@link OutgoingStreamMessage} at runtime, the type here is {@link StreamMessage}
         * to facilitate simpler testing.
         */
        private final StreamMessage msg;

        private final InetAddressAndPort connectTo;

        FileStreamTask(OutgoingStreamMessage ofm, InetAddressAndPort connectTo)
        {
            this.msg = ofm;
            this.connectTo = connectTo;
        }

        /**
         * For testing purposes
         */
        FileStreamTask(StreamMessage msg)
        {
            this.msg = msg;
            this.connectTo = null;
        }

        @Override
        public void run()
        {
            if (!acquirePermit(SEMAPHORE_UNAVAILABLE_LOG_INTERVAL))
                return;

            StreamingChannel channel = null;
            try
            {
                channel = getOrCreateFileChannel(connectTo);

                // close the DataOutputStreamPlus as we're done with it - but don't close the channel
                try (StreamingDataOutputPlus out = channel.acquireOut())
                {
                    serialize(msg, out, messagingVersion, session);
                }
            }
            catch (Exception e)
            {
                session.onError(e);
            }
            catch (Throwable t)
            {
                if (closed && getRootCause(t) instanceof ClosedByInterruptException && fileTransferExecutor.isShutdown())
                {
                    logger.debug("{} Streaming channel was closed due to the executor pool being shutdown", createLogTag(session, channel));
                }
                else
                {
                    inspectThrowable(t);
                    if (!session.state().isFinalState())
                        session.onError(t);
                }
            }
            finally
            {
                fileTransferSemaphore.release(1);
            }
        }

        boolean acquirePermit(int logInterval)
        {
            long logIntervalNanos = MINUTES.toNanos(logInterval);
            long timeOfLastLogging = nanoTime();
            while (true)
            {
                if (closed)
                    return false;
                try
                {
                    if (fileTransferSemaphore.tryAcquire(1, 1, SECONDS))
                        return true;

                    // log a helpful message to operators in case they are wondering why a given session might not be making progress.
                    long now = nanoTime();
                    if (now - timeOfLastLogging > logIntervalNanos)
                    {
                        timeOfLastLogging = now;
                        OutgoingStreamMessage ofm = (OutgoingStreamMessage)msg;

                        if (logger.isInfoEnabled())
                            logger.info("{} waiting to acquire a permit to begin streaming {}. This message logs every {} minutes",
                                        createLogTag(session), ofm.getName(), logInterval);
                    }
                }
                catch (InterruptedException e)
                {
                    throw new UncheckedInterruptedException(e);
                }
            }
        }

        private StreamingChannel getOrCreateFileChannel(InetAddressAndPort connectTo)
        {
            Thread currentThread = currentThread();
            try
            {
                StreamingChannel channel = threadToChannelMap.get(currentThread);
                if (channel != null)
                    return channel;

                channel = createFileChannel(connectTo);
                threadToChannelMap.put(currentThread, channel);
                return channel;
            }
            catch (Exception e)
            {
                throw new IOError(e);
            }
        }

        /**
         * For testing purposes
         */
        void injectChannel(StreamingChannel channel)
        {
            Thread currentThread = currentThread();
            if (threadToChannelMap.get(currentThread) != null)
                throw new IllegalStateException("previous channel already set");

            threadToChannelMap.put(currentThread, channel);
        }

        /**
         * For testing purposes
         */
        void unsetChannel()
        {
            threadToChannelMap.remove(currentThread());
        }
    }

    /**
     * Periodically sends the {@link KeepAliveMessage}.
     * <p>
     * NOTE: this task, and the callback function are executed in the netty event loop.
     */
    class KeepAliveTask implements Runnable
    {
        private final StreamingChannel channel;

        /**
         * A reference to the scheduled task for this instance so that it may be cancelled.
         */
        ScheduledFuture<?> future;

        KeepAliveTask(StreamingChannel channel)
        {
            this.channel = channel;
        }

        @Override
        public void run()
        {
            // if the channel has been closed, cancel the scheduled task and return
            if (!channel.connected() || closed)
            {
                if (null != future)
                    future.cancel(false);
                return;
            }

            if (logger.isTraceEnabled())
                logger.trace("{} Sending keep-alive to {}.", createLogTag(session, channel), session.peer);

            sendControlMessage(new KeepAliveMessage()).addListener(f ->
            {
                if (f.isSuccess() || f.isCancelled())
                    return;

                if (logger.isDebugEnabled())
                    logger.debug("{} Could not send keep-alive message (perhaps stream session is finished?).",
                                 createLogTag(session, channel), f.cause());
            });
        }
    }

    private void scheduleKeepAliveTask(StreamingChannel channel)
    {
        if (!(channel instanceof NettyStreamingChannel))
            return;

        int keepAlivePeriod = DatabaseDescriptor.getStreamingKeepAlivePeriod();
        if (keepAlivePeriod <= 0)
            return;

        if (logger.isDebugEnabled())
            logger.debug("{} Scheduling keep-alive task with {}s period.", createLogTag(session, channel), keepAlivePeriod);

        KeepAliveTask task = new KeepAliveTask(channel);
        ScheduledFuture<?> scheduledFuture =
            ((NettyStreamingChannel)channel).channel
                                            .eventLoop()
                                            .scheduleAtFixedRate(task, keepAlivePeriod, keepAlivePeriod, TimeUnit.SECONDS);
        task.future = scheduledFuture;
        channelKeepAlives.add(scheduledFuture);
    }

    /**
     * For testing purposes only.
     */
    public void setClosed()
    {
        closed = true;
    }

    void setControlChannel(NettyStreamingChannel channel)
    {
        controlChannel = channel;
    }

    int semaphoreAvailablePermits()
    {
        return fileTransferSemaphore.permits();
    }

    public boolean connected()
    {
        return !closed && (controlChannel == null || controlChannel.connected());
    }

    public void close()
    {
        if (closed)
            return;

        closed = true;
        if (logger.isDebugEnabled())
            logger.debug("{} Closing stream connection channels on {}", createLogTag(session), to);
        for (ScheduledFuture<?> future : channelKeepAlives)
            future.cancel(false);
        channelKeepAlives.clear();

        threadToChannelMap.values().forEach(StreamingChannel::close);
        threadToChannelMap.clear();
        fileTransferExecutor.shutdownNow();
    }

    @VisibleForTesting // For testing only -- close the control handle for testing streaming exception handling.
    public void unsafeCloseControlChannel()
    {
        logger.warn("Unsafe close of control channel");
        controlChannel.close().awaitUninterruptibly();
    }
}
