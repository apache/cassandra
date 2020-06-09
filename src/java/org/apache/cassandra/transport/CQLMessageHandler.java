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

package org.apache.cassandra.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.net.AbstractMessageHandler;
import org.apache.cassandra.net.Crc;
import org.apache.cassandra.net.FrameDecoder;
import org.apache.cassandra.net.FrameDecoder.IntactFrame;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.net.ResourceLimits;
import org.apache.cassandra.net.ResourceLimits.Limit;
import org.apache.cassandra.net.ShareableBytes;
import org.apache.cassandra.transport.Flusher.FlushItem.Framed;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.utils.MonotonicClock.approxTime;

public class CQLMessageHandler extends AbstractMessageHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CQLMessageHandler.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.SECONDS);

    private final org.apache.cassandra.transport.Frame.Decoder cqlFrameDecoder;
    private final Message.ProtocolDecoder messageDecoder;
    private final Message.ProtocolEncoder messageEncoder;
    private final FrameEncoder.PayloadAllocator payloadAllocator;
    private final MessageConsumer dispatcher;
    private final ErrorHandler errorHandler;
    private final boolean throwOnOverload;

    long channelPayloadBytesInFlight;

    interface MessageConsumer
    {
        void accept(Channel channel, Message message, Dispatcher.FlushItemConverter toFlushItem);
    }

    interface ErrorHandler
    {
        void accept(Throwable error);
    }

    CQLMessageHandler(Channel channel,
                      FrameDecoder decoder,
                      Frame.Decoder cqlFrameDecoder,
                      Message.ProtocolDecoder messageDecoder,
                      Message.ProtocolEncoder messageEncoder,
                      MessageConsumer dispatcher,
                      FrameEncoder.PayloadAllocator payloadAllocator,
                      int queueCapacity,
                      Limit endpointReserve,
                      Limit globalReserve,
                      WaitQueue endpointWaitQueue,
                      WaitQueue globalWaitQueue,
                      OnHandlerClosed onClosed,
                      ErrorHandler errorHandler,
                      boolean throwOnOverload)
    {
        super(decoder,
              channel,
              FrameEncoder.Payload.MAX_SIZE - 1,
              queueCapacity,
              endpointReserve,
              globalReserve,
              endpointWaitQueue,
              globalWaitQueue,
              onClosed);
        this.cqlFrameDecoder    = cqlFrameDecoder;
        this.messageDecoder     = messageDecoder;
        this.messageEncoder     = messageEncoder;
        this.payloadAllocator   = payloadAllocator;
        this.dispatcher         = dispatcher;
        this.errorHandler       = errorHandler;
        this.throwOnOverload    = throwOnOverload;
    }

    protected boolean processOneContainedMessage(ShareableBytes bytes, Limit endpointReserve, Limit globalReserve) throws IOException
    {
        Frame frame = toCqlFrame(bytes);
        if (frame == null)
            return false;

        // max (CQL) frame size defaults to 256mb, so should be safe to downcast
        int frameSize = Ints.checkedCast(frame.header.bodySizeInBytes);

        if (throwOnOverload)
        {
            if (!acquireCapacity(frame.header, endpointReserve, globalReserve))
            {
                // discard the request and throw an exception
                ClientMetrics.instance.markRequestDiscarded();
                logger.trace("Discarded request of size: {}. InflightChannelRequestPayload: {}, InflightEndpointRequestPayload: {}, InflightOverallRequestPayload: {}, Header: {}",
                             frameSize,
                             channelPayloadBytesInFlight,
                             endpointReserve.using(),
                             globalReserve.using(),
                             frame.header);

                RuntimeException wrapped =
                    ErrorMessage.wrap(
                        new OverloadedException("Server is in overloaded state. Cannot accept more requests at this point"),
                                                frame.header.streamId);
                errorHandler.accept(wrapped);
                // Don't stop processing incoming frames, rely on the client to apply
                // backpressure when it receives OverloadedException
                return true;
            }
        }
        else
        {
            if (!acquireCapacityAndQueueOnFailure(frame.header, endpointReserve, globalReserve))
            {
                // force overallocation so we can process this one message, then return false
                // so that we stop processing further frames from the decoder
                forceOverAllocation(endpointReserve, globalReserve, frameSize);
                ClientMetrics.instance.pauseConnection();
                channelPayloadBytesInFlight += frameSize;

                // we're over allocated here, but process the message
                // anyway because we didn't throw any exception
                receivedCount++;
                receivedBytes += frameSize;
                processFrame(frameSize, frame);
                return false;
            }
        }

        channelPayloadBytesInFlight += frameSize;
        receivedCount++;
        receivedBytes += frameSize;
        processFrame(frameSize, frame);
        return true;
    }

    private void processFrame(int frameSize, Frame frame)
    {
        if (frameSize <= FrameEncoder.Payload.MAX_SIZE)
            processCqlFrame(frame);
        else
            processLargeMessage(frame);
    }

    // for various reasons, it's possible for a large message to be contained in a single frame
    private void processLargeMessage(Frame frame)
    {
//        new LargeMessage(header, bytes.sliceAndConsume(size).share()).schedule();
    }

    private Frame toCqlFrame(ShareableBytes bytes)
    {
        ByteBuffer buf = bytes.get();
        final int begin = buf.position();
        ByteBuf buffer = Unpooled.wrappedBuffer(buf);

        try
        {
            Frame f = cqlFrameDecoder.decodeFrame(buffer);
            buf.position(begin + buffer.readerIndex());
            return f;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t, false);
            logger.error("{} unexpected exception caught while deserializing a message", id(), t);
        }
        return null;
    }

    private void processCqlFrame(Frame frame)
    {
        Message message = messageDecoder.decodeMessage(channel, frame);
        // TODO throw ProtocolException if not a Message.Request
        dispatcher.accept(channel, message, this::toFlushItem);
    }

    // Acts as a Dispatcher.FlushItemConverter
    private Framed toFlushItem(Channel channel, Message.Request request, Message.Response response)
    {
        // Returns a FlushItem.Framed instance which wraps a Consumer<FlushItem> that performs
        // the work of returning the capacity allocated for processing the request.
        // The Dispatcher will call this to obtain the FlushItem to enqueue with its Flusher once
        // a dispatched request has been processed.
        return new Framed(channel,
                          messageEncoder.encodeMessage(response, request.getSourceFrame().header.version),
                          request.getSourceFrame(),
                          payloadAllocator,
                          this::releaseAfterFlush);
    }

    private void releaseAfterFlush(Flusher.FlushItem<Frame> flushItem)
    {
        releaseCapacity(Ints.checkedCast(flushItem.sourceFrame.header.bodySizeInBytes));
        channelPayloadBytesInFlight -= flushItem.sourceFrame.header.bodySizeInBytes;
        flushItem.sourceFrame.body.release();
    }

    /*
     * Handling of multi-frame large messages
     */
    protected boolean processFirstFrameOfLargeMessage(IntactFrame frame, Limit endpointReserve, Limit globalReserve) throws IOException
    {
        ShareableBytes bytes = frame.contents;
        ByteBuffer buf = bytes.get();
        try
        {
            Frame.Header header = cqlFrameDecoder.decodeHeader(Unpooled.wrappedBuffer(buf));

            if (!acquireCapacity(header, endpointReserve, globalReserve))
            {
                receivedCount++;
                receivedBytes += frame.frameSize;
                return false;
            }

            receivedCount++;
            receivedBytes += buf.remaining();
            largeMessage = new LargeMessage(header);
            largeMessage.supply(frame);
            return true;
        }
        catch (Exception e)
        {
            throw new IOException("Error decoding CQL frame", e);
        }
    }

    protected String id()
    {
        return channel.id().asShortText();
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean acquireCapacityAndQueueOnFailure(Frame.Header header, Limit endpointReserve, Limit globalReserve)
    {
        int frameSize = Ints.checkedCast(header.bodySizeInBytes);
        long currentTimeNanos = approxTime.now();
        return acquireCapacity(endpointReserve, globalReserve, frameSize, currentTimeNanos, Long.MAX_VALUE);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean acquireCapacity(Frame.Header header, Limit endpointReserve, Limit globalReserve)
    {
        int frameSize = Ints.checkedCast(header.bodySizeInBytes);
        return acquireCapacity(endpointReserve, globalReserve, frameSize) == ResourceLimits.Outcome.SUCCESS;
    }

    /*
     * Although it would be possible to recover when certain types of corrupt frame are encountered,
     * this could cause problems for clients as the payload may contain CQL messages from multiple
     * streams. Simply dropping the corrupt frame or returning an error response would not give the
     * client enough information to map back to inflight requests, leading to timeouts.
     * Instead, we need to fail fast, possibly dropping the connection whenever a corrupt frame is
     * encountered. Consequently, we throw whenever a corrupt frame is encountered, regardless of its
     * type.
     */
    protected void processCorruptFrame(FrameDecoder.CorruptFrame frame) throws Crc.InvalidCrc
    {
        corruptFramesUnrecovered++;
        if (!frame.isRecoverable())
        {
            noSpamLogger.error("{} invalid, unrecoverable CRC mismatch detected in frame header", id());
            logger.error("{} invalid, unrecoverable CRC mismatch detected in frame header", id());
            throw new Crc.InvalidCrc(frame.readCRC, frame.computedCRC);
        }
        else
        {
            receivedBytes += frame.frameSize;
            noSpamLogger.error("{} invalid, unrecoverable CRC mismatch detected in frame body", id());
            logger.error("{} invalid, unrecoverable CRC mismatch detected in frame header", id());
            throw new Crc.InvalidCrc(frame.readCRC, frame.computedCRC);
        }
    }

    protected void fatalExceptionCaught(Throwable cause)
    {
        decoder.discard();
        logger.warn("Unrecoverable exception caught in CQL message processing pipeline, closing the connection", cause);
        channel.close();
    }

    static int frameSize(Frame.Header header)
    {
        return Frame.Header.LENGTH + Ints.checkedCast(header.bodySizeInBytes);
    }

    private class LargeMessage extends AbstractMessageHandler.LargeMessage<Frame.Header>
    {
        private static final long EXPIRES_AT = Long.MAX_VALUE;

        private LargeMessage(Frame.Header header)
        {
            super(frameSize(header), header, EXPIRES_AT, false);
        }

        private LargeMessage(Frame.Header header, ShareableBytes bytes)
        {
            super(frameSize(header), header, EXPIRES_AT, bytes);
        }

        private Frame assembleFrame() throws Exception
        {
            // TODO we already have the frame header, so we could skip HEADER_LENGTH
            // and go straight to message decoding
            ByteBuf concat = Unpooled.wrappedBuffer(buffers.stream()
                                                           .map(ShareableBytes::get)
                                                           .toArray(ByteBuffer[]::new));
            return cqlFrameDecoder.decodeFrame(concat);
        }

        protected void onComplete()
        {
            if (!isCorrupt)
            {
                try
                {
                    processCqlFrame(assembleFrame());
                }
                catch (Exception e)
                {
                    logger.warn("Error decoding CQL frame", e);
                }
                finally
                {
                    releaseBuffers();
                }
            }
        }

        protected void abort()
        {
            if (!isCorrupt)
                releaseBuffersAndCapacity(); // release resources if in normal state when abort() is invoked
        }
    }
}
