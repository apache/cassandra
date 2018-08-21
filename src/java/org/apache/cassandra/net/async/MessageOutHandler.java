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
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.config.Config.PROPERTY_PREFIX;

/**
 * A Netty {@link ChannelHandler} for serializing outbound messages.
 * <p>
 * Note: this class derives from {@link ChannelDuplexHandler} so we can intercept calls to
 * {@link #userEventTriggered(ChannelHandlerContext, Object)}.
 */
class MessageOutHandler extends ChannelDuplexHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MessageOutHandler.class);
    private static final NoSpamLogger errorLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.SECONDS);

    /**
     * The default size threshold for deciding when to auto-flush the channel.
     */
    private static final int DEFAULT_AUTO_FLUSH_THRESHOLD = 1 << 16;

    // reatining the pre 4.0 property name for backward compatibility.
    private static final String AUTO_FLUSH_PROPERTY = PROPERTY_PREFIX + "otc_buffer_size";
    static final int AUTO_FLUSH_THRESHOLD = Integer.getInteger(AUTO_FLUSH_PROPERTY, DEFAULT_AUTO_FLUSH_THRESHOLD);

    /**
     * The amount of prefix data, in bytes, before the serialized message.
     */
    private static final int MESSAGE_PREFIX_SIZE = 12;

    private final OutboundConnectionIdentifier connectionId;

    /**
     * The version of the messaging protocol we're communicating at.
     */
    private final int targetMessagingVersion;

    /**
     * The minumum size at which we'll automatically flush the channel.
     */
    private final int flushSizeThreshold;

    private String loggingTag;

    MessageOutHandler(OutboundConnectionIdentifier connectionId, int targetMessagingVersion)
    {
        this (connectionId, targetMessagingVersion, AUTO_FLUSH_THRESHOLD);
    }

    MessageOutHandler(OutboundConnectionIdentifier connectionId, int targetMessagingVersion, int flushThreshold)
    {
        this.connectionId = connectionId;
        this.targetMessagingVersion = targetMessagingVersion;
        this.flushSizeThreshold = flushThreshold;
    }

    @Override
    public void handlerAdded(final ChannelHandlerContext ctx)
    {
        loggingTag = connectionId.remote() + "-" + connectionId.type() + "-" + ctx.channel().id();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object o, ChannelPromise promise)
    {
        // this was a temporary fix until https://github.com/netty/netty/pull/6867 was released (probably netty 4.1.13).
        // TL;DR a closed channel can still process messages in the pipeline that were queued before the close.
        // the channel handlers are removed from the channel potentially async from the close operation.
        if (!ctx.channel().isOpen())
        {
            logger.debug("{} attempting to process a message in the pipeline, but channel is closed", loggingTag);
            promise.tryFailure(new ClosedChannelException());
            return;
        }

        ByteBuf out = null;
        try
        {
            if (!(o instanceof QueuedMessage))
            {
                ReferenceCountUtil.release(o);
                promise.tryFailure(new UnsupportedMessageTypeException(connectionId + " msg must be an instance of " +
                                                                       QueuedMessage.class.getSimpleName()));
                return;
            }

            QueuedMessage msg = (QueuedMessage) o;

            // frame size includes the magic and and other values *before* the actual serialized message.
            // note: don't even bother to check the compressed size (if compression is enabled for the channel),
            // cuz if it's this large already, we're probably screwed anyway
            long currentFrameSize = MESSAGE_PREFIX_SIZE + msg.message.serializedSize(targetMessagingVersion);
            if (currentFrameSize > Integer.MAX_VALUE || currentFrameSize < 0)
            {
                promise.tryFailure(new IllegalStateException(String.format("%s illegal frame size: %d, ignoring message", loggingTag, currentFrameSize)));
                return;
            }

            out = ctx.alloc().ioBuffer((int)currentFrameSize);

            captureTracingInfo(msg);
            serializeMessage(msg, out);
            ctx.write(out, promise);

            // check to see if we should flush based on buffered size
            ChannelOutboundBuffer outboundBuffer = ctx.channel().unsafe().outboundBuffer();
            if (outboundBuffer != null && outboundBuffer.totalPendingWriteBytes() >= flushSizeThreshold)
                ctx.flush();
        }
        catch(Exception e)
        {
            if (out != null && out.refCnt() > 0)
                out.release(out.refCnt());
            exceptionCaught(ctx, e);
            promise.tryFailure(e);
        }
    }

    /**
     * Record any tracing data, if enabled on this message.
     */
    @VisibleForTesting
    void captureTracingInfo(QueuedMessage msg)
    {
        try
        {
            UUID sessionId =  (UUID)msg.message.getParameter(ParameterType.TRACE_SESSION);
            if (sessionId != null)
            {
                TraceState state = Tracing.instance.get(sessionId);
                String message = String.format("Sending %s message to %s, size = %d bytes",
                                               msg.message.verb, connectionId.connectionAddress(),
                                               msg.message.serializedSize(targetMessagingVersion) + MESSAGE_PREFIX_SIZE);
                // session may have already finished; see CASSANDRA-5668
                if (state == null)
                {
                    Tracing.TraceType traceType = (Tracing.TraceType)msg.message.getParameter(ParameterType.TRACE_TYPE);
                    traceType = traceType == null ? Tracing.TraceType.QUERY : traceType;
                    Tracing.instance.trace(ByteBuffer.wrap(UUIDGen.decompose(sessionId)), message, traceType.getTTL());
                }
                else
                {
                    state.trace(message);
                    if (msg.message.verb == MessagingService.Verb.REQUEST_RESPONSE)
                        Tracing.instance.doneWithNonLocalSession(state);
                }
            }
        }
        catch (Exception e)
        {
            logger.warn("{} failed to capture the tracing info for an outbound message, ignoring", loggingTag, e);
        }
    }

    private void serializeMessage(QueuedMessage msg, ByteBuf out) throws IOException
    {
        out.writeInt(MessagingService.PROTOCOL_MAGIC);
        out.writeInt(msg.id);

        // int cast cuts off the high-order half of the timestamp, which we can assume remains
        // the same between now and when the recipient reconstructs it.
        out.writeInt((int) NanoTimeToCurrentTimeMillis.convert(msg.timestampNanos));
        @SuppressWarnings("resource")
        DataOutputPlus outStream = new ByteBufDataOutputPlus(out);
        msg.message.serialize(outStream, targetMessagingVersion);

        // next few lines are for debugging ... massively helpful!!
        // if we allocated too much buffer for this message, we'll log here.
        // if we allocated to little buffer space, we would have hit an exception when trying to write more bytes to it
        if (out.isWritable())
            errorLogger.error("{} reported message size {}, actual message size {}, msg {}",
                         loggingTag, out.capacity(), out.writerIndex(), msg.message);
    }

    /**
     * {@inheritDoc}
     *
     * If we get an {@link IdleStateEvent} for the write path, we want to close the channel as we can't make progress.
     * That assumes, of course, that there's any outstanding bytes in the channel to write. We don't necesarrily care
     * about idleness (for example, gossip channels will be idle most of the time), but instead our concern is
     * the ability to make progress when there's work to be done.
     * <p>
     * Note: this is invoked on the netty event loop.
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
    {
        if (evt instanceof IdleStateEvent && ((IdleStateEvent)evt).state() == IdleState.WRITER_IDLE)
        {
            ChannelOutboundBuffer cob = ctx.channel().unsafe().outboundBuffer();
            if (cob != null && cob.totalPendingWriteBytes() > 0)
            {
                ctx.channel().attr(OutboundMessagingConnection.PURGE_MESSAGES_CHANNEL_ATTR)
                   .compareAndSet(Boolean.FALSE, Boolean.TRUE);
                ctx.close();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        if (cause instanceof IOException)
            logger.trace("{} io error", loggingTag, cause);
        else
            logger.warn("{} error", loggingTag, cause);

        ctx.close();
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise)
    {
        ctx.flush();
        ctx.close(promise);
    }
}
