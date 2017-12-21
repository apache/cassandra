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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.net.async.RebufferingByteBufDataInputPlus;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamReceiveException;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.streaming.messages.IncomingFileMessage;
import org.apache.cassandra.streaming.messages.KeepAliveMessage;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.streaming.async.NettyStreamingMessageSender.createLogTag;

/**
 * Handles the inbound side of streaming messages and sstable data. From the incoming data, we derserialize the message
 * and potentially reify partitions and rows and write those out to new sstable files. Because deserialization is a blocking affair,
 * we can't block the netty event loop. Thus we have a background thread perform all the blocking deserialization.
 */
public class StreamingInboundHandler extends ChannelInboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingInboundHandler.class);
    static final Function<SessionIdentifier, StreamSession> DEFAULT_SESSION_PROVIDER = sid -> StreamManager.instance.findSession(sid.from, sid.planId, sid.sessionIndex);

    private static final int AUTO_READ_LOW_WATER_MARK = 1 << 15;
    private static final int AUTO_READ_HIGH_WATER_MARK = 1 << 16;

    private final InetSocketAddress remoteAddress;
    private final int protocolVersion;

    private final StreamSession session;

    /**
     * A collection of {@link ByteBuf}s that are yet to be processed. Incoming buffers are first dropped into this
     * structure, and then consumed.
     * <p>
     * For thread safety, this structure's resources are released on the consuming thread
     * (via {@link RebufferingByteBufDataInputPlus#close()},
     * but the producing side calls {@link RebufferingByteBufDataInputPlus#markClose()} to notify the input that is should close.
     */
    private RebufferingByteBufDataInputPlus buffers;

    private volatile boolean closed;

    public StreamingInboundHandler(InetSocketAddress remoteAddress, int protocolVersion, @Nullable StreamSession session)
    {
        this.remoteAddress = remoteAddress;
        this.protocolVersion = protocolVersion;
        this.session = session;
    }

    @Override
    @SuppressWarnings("resource")
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        buffers = new RebufferingByteBufDataInputPlus(AUTO_READ_LOW_WATER_MARK, AUTO_READ_HIGH_WATER_MARK, ctx.channel().config());
        Thread blockingIOThread = new FastThreadLocalThread(new StreamDeserializingTask(DEFAULT_SESSION_PROVIDER, session, ctx.channel()),
                                                            String.format("Stream-Deserializer-%s-%s", remoteAddress.toString(), ctx.channel().id()));
        blockingIOThread.setDaemon(true);
        blockingIOThread.start();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message)
    {
        if (!closed && message instanceof ByteBuf)
            buffers.append((ByteBuf) message);
        else
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
        buffers.markClose();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        if (cause instanceof IOException)
            logger.trace("connection problem while streaming", cause);
        else
            logger.warn("exception occurred while in processing streaming file", cause);
        close();
    }

    /**
     * For testing only!!
     */
    void setPendingBuffers(RebufferingByteBufDataInputPlus bufChannel)
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
                    // do a check of available bytes and possibly sleep some amount of time (then continue).
                    // this way we can break out of run() sanely or we end up blocking indefintely in StreamMessage.deserialize()
                    while (buffers.available() == 0)
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
                        logger.debug("{} Received {}", createLogTag(session, channel), message);
                        continue;
                    }

                    if (session == null)
                        session = deriveSession(message);
                    logger.debug("{} Received {}", createLogTag(session, channel), message);
                    session.messageReceived(message);
                }
            }
            catch (EOFException eof)
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
                    buffers.close();
            }
        }

        StreamSession deriveSession(StreamMessage message) throws IOException
        {
            StreamSession streamSession = null;
            // StreamInitMessage starts a new channel, and IncomingFileMessage potentially, as well.
            // IncomingFileMessage needs a session to be established a priori, though
            if (message instanceof StreamInitMessage)
            {
                assert session == null : "initiator of stream session received a StreamInitMessage";
                StreamInitMessage init = (StreamInitMessage) message;
                StreamResultFuture.initReceivingSide(init.sessionIndex, init.planId, init.streamOperation, init.from, channel, init.keepSSTableLevel, init.pendingRepair, init.previewKind);
                streamSession = sessionProvider.apply(new SessionIdentifier(init.from, init.planId, init.sessionIndex));
            }
            else if (message instanceof IncomingFileMessage)
            {
                // TODO: it'd be great to check if the session actually exists before slurping in the entire sstable,
                // but that's a refactoring for another day
                FileMessageHeader header = ((IncomingFileMessage) message).header;
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
        final InetAddress from;
        final UUID planId;
        final int sessionIndex;

        SessionIdentifier(InetAddress from, UUID planId, int sessionIndex)
        {
            this.from = from;
            this.planId = planId;
            this.sessionIndex = sessionIndex;
        }
    }
}
