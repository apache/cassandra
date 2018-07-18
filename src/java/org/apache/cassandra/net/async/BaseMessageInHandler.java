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

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;

/**
 * Parses out individual messages from the incoming buffers. Each message, both header and payload, is incrementally built up
 * from the available input data, then passed to the {@link #messageConsumer}.
 *
 * Note: this class derives from {@link ByteToMessageDecoder} to take advantage of the {@link ByteToMessageDecoder.Cumulator}
 * behavior across {@link #decode(ChannelHandlerContext, ByteBuf, List)} invocations. That way we don't have to maintain
 * the not-fully consumed {@link ByteBuf}s.
 */
public abstract class BaseMessageInHandler extends ByteToMessageDecoder
{
    public static final Logger logger = LoggerFactory.getLogger(BaseMessageInHandler.class);

    enum State
    {
        READ_FIRST_CHUNK,
        READ_IP_ADDRESS,
        READ_VERB,
        READ_PARAMETERS_SIZE,
        READ_PARAMETERS_DATA,
        READ_PAYLOAD_SIZE,
        READ_PAYLOAD,
        CLOSED
    }

    /**
     * The byte count for magic, msg id, timestamp values.
     */
    @VisibleForTesting
    static final int FIRST_SECTION_BYTE_COUNT = 12;

    static final int VERB_LENGTH = Integer.BYTES;

    /**
     * The default target for consuming deserialized {@link MessageIn}.
     */
    static final BiConsumer<MessageIn, Integer> MESSAGING_SERVICE_CONSUMER = (messageIn, id) -> MessagingService.instance().receive(messageIn, id);

    /**
     * Abstracts out depending directly on {@link MessagingService#receive(MessageIn, int)}; this makes tests more sane
     * as they don't require nor trigger the entire message processing circus.
     */
    final BiConsumer<MessageIn, Integer> messageConsumer;

    final InetAddressAndPort peer;
    final int messagingVersion;

    protected State state;

    public BaseMessageInHandler(InetAddressAndPort peer, int messagingVersion, BiConsumer<MessageIn, Integer> messageConsumer)
    {
        this.peer = peer;
        this.messagingVersion = messagingVersion;
        this.messageConsumer = messageConsumer;
    }

    // redeclared here to make the method public (for testing)
    @VisibleForTesting
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
    {
        if (state == State.CLOSED)
        {
            in.skipBytes(in.readableBytes());
            return;
        }

        try
        {
            handleDecode(ctx, in, out);
        }
        catch (Exception e)
        {
            // prevent any future attempts at reading messages from any inbound buffers, as we're already in a bad state
            state = State.CLOSED;

            // force the buffer to appear to be consumed, thereby exiting the ByteToMessageDecoder.callDecode() loop,
            // and other paths in that class, more efficiently
            in.skipBytes(in.readableBytes());

            // throwing the exception up causes the ByteToMessageDecoder.callDecode() loop to exit. if we don't do that,
            // we'll keep trying to process data out of the last received buffer (and it'll be really, really wrong)
            throw e;
        }
    }

    public abstract void handleDecode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception;

    MessageHeader readFirstChunk(ByteBuf in) throws IOException
    {
        if (in.readableBytes() < FIRST_SECTION_BYTE_COUNT)
            return null;
        MessagingService.validateMagic(in.readInt());
        MessageHeader messageHeader = new MessageInHandler.MessageHeader();
        messageHeader.messageId = in.readInt();
        int messageTimestamp = in.readInt(); // make sure to read the sent timestamp, even if DatabaseDescriptor.hasCrossNodeTimeout() is not enabled
        messageHeader.constructionTime = MessageIn.deriveConstructionTime(peer, messageTimestamp, ApproximateTime.currentTimeMillis());

        return messageHeader;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        if (cause instanceof EOFException)
            logger.trace("eof reading from socket; closing", cause);
        else if (cause instanceof UnknownTableException)
            logger.warn(" Got message from unknown table while reading from socket {}[{}]; closing",
                        ctx.channel().remoteAddress(), ctx.channel().id(), cause);
        else if (cause instanceof IOException)
            logger.trace("IOException reading from socket; closing", cause);
        else
            logger.warn("Unexpected exception caught in inbound channel pipeline from {}[{}]",
                        ctx.channel().remoteAddress(), ctx.channel().id(), cause);

        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        logger.trace("received channel closed message for peer {} on local addr {}", ctx.channel().remoteAddress(), ctx.channel().localAddress());
        state = State.CLOSED;
        ctx.fireChannelInactive();
    }

    // should ony be used for testing!!!
    @VisibleForTesting
    abstract MessageHeader getMessageHeader();

    /**
     * A simple struct to hold the message header data as it is being built up.
     */
    static class MessageHeader
    {
        int messageId;
        long constructionTime;
        InetAddressAndPort from;
        MessagingService.Verb verb;
        int payloadSize;

        Map<ParameterType, Object> parameters = Collections.emptyMap();

        /**
         * Length of the parameter data. If the message's version is {@link MessagingService#VERSION_40} or higher,
         * this value is the total number of header bytes; else, for legacy messaging, this is the number of
         * key/value entries in the header.
         */
        int parameterLength;
    }

    // for testing purposes only!!!
    @VisibleForTesting
    public State getState()
    {
        return state;
    }
}
