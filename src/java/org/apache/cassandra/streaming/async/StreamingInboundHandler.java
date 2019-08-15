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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.AsyncStreamingInputPlus;
import org.apache.cassandra.net.AsyncStreamingInputPlus.InputTimeoutException;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamReceiveException;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.IncomingStreamMessage;
import org.apache.cassandra.streaming.messages.KeepAliveMessage;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.streaming.async.NettyStreamingMessageSender.createLogTag;

/**
 * Handles the inbound side of streaming messages and stream data. From the incoming data, we derserialize the message
 * including the actual stream data itself. Because the reading and deserialization of streams is a blocking affair,
 * we can't block the netty event loop. Thus we have a background thread perform all the blocking deserialization.
 */
public class StreamingInboundHandler extends ChannelInboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingInboundHandler.class);
    private static final Function<SessionIdentifier, StreamSession> DEFAULT_SESSION_PROVIDER = sid -> StreamManager.instance.findSession(sid.from, sid.planId, sid.sessionIndex);
    private static volatile boolean trackInboundHandlers = false;
    private static Collection<StreamingInboundHandler> inboundHandlers;
    private final InetAddressAndPort remoteAddress;
    private final int protocolVersion;

    private final StreamSession session;

    /**
     * A collection of {@link ByteBuf}s that are yet to be processed. Incoming buffers are first dropped into this
     * structure, and then consumed.
     * <p>
     * For thread safety, this structure's resources are released on the consuming thread
     * (via {@link AsyncStreamingInputPlus#close()},
     * but the producing side calls {@link AsyncStreamingInputPlus#requestClosure()} to notify the input that is should close.
     */
    private AsyncStreamingInputPlus buffers;

    private volatile boolean closed;

    public StreamingInboundHandler(InetAddressAndPort remoteAddress, int protocolVersion, @Nullable StreamSession session)
    {
        this.remoteAddress = remoteAddress;
        this.protocolVersion = protocolVersion;
        this.session = session;
        if (trackInboundHandlers)
            inboundHandlers.add(this);
    }

    @Override
    @SuppressWarnings("resource")
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        buffers = new AsyncStreamingInputPlus(ctx.channel());
        Thread blockingIOThread = new FastThreadLocalThread(new StreamDeserializingTask(DEFAULT_SESSION_PROVIDER, session, ctx.channel()),
                                                            String.format("Stream-Deserializer-%s-%s", remoteAddress.toString(), ctx.channel().id()));
        blockingIOThread.setDaemon(true);
        blockingIOThread.start();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message)
    {
        if (closed || !(message instanceof ByteBuf) || !buffers.append((ByteBuf) message))
            ReferenceCountUtil.release(message);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        close();
        ctx.fireChannelInactive();
    }

    void close()
    {
        closed = true;
        buffers.requestClosure();
        if (trackInboundHandlers)
            inboundHandlers.remove(this);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        if (cause instanceof IOException)
            logger.trace("connection problem while streaming", cause);
        else
            logger.warn("exception occurred while in processing streaming data", cause);
        close();
    }

    /**
     * For testing only!!
     */
    void setPendingBuffers(AsyncStreamingInputPlus bufChannel)
    {
        this.buffers = bufChannel;
    }

    /**
     * The task that performs the actual deserialization.
     */
    class StreamDeserializingTask implements Runnable
    {
        private final Function<SessionIdentifier, StreamSession> sessionProvider;
        private final Channel channel;

        @VisibleForTesting
        StreamSession session;

        StreamDeserializingTask(Function<SessionIdentifier, StreamSession> sessionProvider, StreamSession session, Channel channel)
        {
            this.sessionProvider = sessionProvider;
            this.session = session;
            this.channel = channel;
        }

        @Override
        public void run()
        {
            try
            {
                while (true)
                {
                    buffers.maybeIssueRead();

                    // do a check of available bytes and possibly sleep some amount of time (then continue).
                    // this way we can break out of run() sanely or we end up blocking indefintely in StreamMessage.deserialize()
                    while (buffers.isEmpty())
                    {
                        if (closed)
                            return;

                        Uninterruptibles.sleepUninterruptibly(400, TimeUnit.MILLISECONDS);
                    }

                    StreamMessage message = StreamMessage.deserialize(buffers, protocolVersion, null);

                    // keep-alives don't necessarily need to be tied to a session (they could be arrive before or after
                    // wrt session lifecycle, due to races), just log that we received the message and carry on
                    if (message instanceof KeepAliveMessage)
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("{} Received {}", createLogTag(session, channel), message);
                        continue;
                    }

                    if (session == null)
                        session = deriveSession(message);

                    if (logger.isDebugEnabled())
                        logger.debug("{} Received {}", createLogTag(session, channel), message);

                    session.messageReceived(message);
                }
            }
            catch (InputTimeoutException | EOFException e)
            {
                // ignore
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                if (session != null)
                {
                    session.onError(t);
                }
                else if (t instanceof StreamReceiveException)
                {
                    ((StreamReceiveException)t).session.onError(t);
                }
                else
                {
                    logger.error("{} stream operation from {} failed", createLogTag(session, channel), remoteAddress, t);
                }
            }
            finally
            {
                channel.close();
                closed = true;

                if (buffers != null)
                {
                    // request closure again as the original request could have raced with receiving a
                    // message and been consumed in the message receive loop above.  Otherweise
                    // buffers could hang indefinitely on the queue.poll.
                    buffers.requestClosure();
                    buffers.close();
                }
            }
        }

        StreamSession deriveSession(StreamMessage message)
        {
            StreamSession streamSession = null;
            // StreamInitMessage starts a new channel, and IncomingStreamMessage potentially, as well.
            // IncomingStreamMessage needs a session to be established a priori, though
            if (message instanceof StreamInitMessage)
            {
                assert session == null : "initiator of stream session received a StreamInitMessage";
                StreamInitMessage init = (StreamInitMessage) message;
                StreamResultFuture.initReceivingSide(init.sessionIndex, init.planId, init.streamOperation, init.from, channel, init.pendingRepair, init.previewKind);
                streamSession = sessionProvider.apply(new SessionIdentifier(init.from, init.planId, init.sessionIndex));
            }
            else if (message instanceof IncomingStreamMessage)
            {
                // TODO: it'd be great to check if the session actually exists before slurping in the entire stream,
                // but that's a refactoring for another day
                StreamMessageHeader header = ((IncomingStreamMessage) message).header;
                streamSession = sessionProvider.apply(new SessionIdentifier(header.sender, header.planId, header.sessionIndex));
            }

            if (streamSession == null)
                throw new IllegalStateException(createLogTag(null, channel) + " no session found for message " + message);

            streamSession.attach(channel);
            return streamSession;
        }
    }

    /**
     * A simple struct to wrap the data points required to lookup a {@link StreamSession}
     */
    static class SessionIdentifier
    {
        final InetAddressAndPort from;
        final UUID planId;
        final int sessionIndex;

        SessionIdentifier(InetAddressAndPort from, UUID planId, int sessionIndex)
        {
            this.from = from;
            this.planId = planId;
            this.sessionIndex = sessionIndex;
        }
    }

    /** Shutdown for in-JVM tests. For any other usage, tracking of active inbound streaming handlers
     *  should be revisted first and in-JVM shutdown refactored with it.
     *  This does not prevent new inbound handlers being added after shutdown, nor is not thread-safe
     *  around new inbound handlers being opened during shutdown.
      */
    @VisibleForTesting
    public static void shutdown()
    {
        assert trackInboundHandlers == true : "in-JVM tests required tracking of inbound streaming handlers";

        inboundHandlers.forEach(StreamingInboundHandler::close);
        inboundHandlers.clear();
    }

    public static void trackInboundHandlers()
    {
        inboundHandlers = Collections.newSetFromMap(new ConcurrentHashMap<>());
        trackInboundHandlers = true;
    }
}
