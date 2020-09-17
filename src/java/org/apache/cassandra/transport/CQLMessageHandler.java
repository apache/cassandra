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

public class CQLMessageHandler<M extends Message> extends AbstractMessageHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CQLMessageHandler.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.SECONDS);

    public static final int LARGE_MESSAGE_THRESHOLD = FrameEncoder.Payload.MAX_SIZE - 1;
    private static final int UNKNOWN_STREAM_ID = 0;

    private final org.apache.cassandra.transport.Frame.Decoder cqlFrameDecoder;
    private final Message.Decoder<M> messageDecoder;
    private final FrameEncoder.PayloadAllocator payloadAllocator;
    private final MessageConsumer<M> dispatcher;
    private final ErrorHandler errorHandler;
    private final boolean throwOnOverload;

    long channelPayloadBytesInFlight;

    interface MessageConsumer<M extends Message>
    {
        void accept(Channel channel, M message, Dispatcher.FlushItemConverter toFlushItem);
    }

    interface ErrorHandler
    {
        void accept(Throwable error);
    }

    CQLMessageHandler(Channel channel,
                      FrameDecoder decoder,
                      Frame.Decoder cqlFrameDecoder,
                      Message.Decoder<M> messageDecoder,
                      MessageConsumer<M> dispatcher,
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
              LARGE_MESSAGE_THRESHOLD,
              queueCapacity,
              endpointReserve,
              globalReserve,
              endpointWaitQueue,
              globalWaitQueue,
              onClosed);
        this.cqlFrameDecoder    = cqlFrameDecoder;
        this.messageDecoder     = messageDecoder;
        this.payloadAllocator   = payloadAllocator;
        this.dispatcher         = dispatcher;
        this.errorHandler       = errorHandler;
        this.throwOnOverload    = throwOnOverload;
    }

    protected boolean processOneContainedMessage(ShareableBytes bytes, Limit endpointReserve, Limit globalReserve)
    {
        Frame frame = toCqlFrame(bytes);
        // null indicates a failure to extract the CQL frame. This will trigger a protocol exception
        // and closing the connection.
        if (null == frame)
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

                handleError(new OverloadedException("Server is in overloaded state. " +
                                                    "Cannot accept more requests at this point"),
                            frame.header);

                frame.release();
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
        new LargeMessage(frame.header, ShareableBytes.wrap(frame.body.nioBuffer())).onComplete();
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
            // Ideally we would recover from this as we could simply drop the messaging frame.
            // However, we have no good way to communicate this back to the client as we don't
            // yet know the stream id. So this will trigger an Error response and the connection
            // to be closed.
            handleError(new ProtocolException("Deserialization error processing CQL payload"));
            return null;
        }
    }

    private void processCqlFrame(Frame frame)
    {
        try
        {
            M message = messageDecoder.decode(channel, frame);
            dispatcher.accept(channel, message, this::toFlushItem);
        }
        catch (Exception e)
        {
            handleErrorAndRelease(e, frame.header);
        }
    }

    /**
     * For "expected" errors this ensures we pass a WrappedException,
     * which contains a streamId, to the error handler. This makes
     * sure that whereever possible, the streamId is propagated back
     * to the client.
     * This also releases the capacity acquired for processing as
     * indicated by supplied header.
     * @param t
     * @param header
     */
    private void handleErrorAndRelease(Throwable t, Frame.Header header)
    {
        release(header);
        handleError(t, header);
    }

    /**
     * For "expected" errors this ensures we pass a WrappedException,
     * which contains a streamId, to the error handler. This makes
     * sure that whereever possible, the streamId is propagated back
     * to the client.
     * This variant doesn't call release as it is intended for use
     * when an error occurs without any capacity being acquired.
     * Typically, this would be the result of an acquisition failure
     * if the THROW_ON_OVERLOAD option has been specified by the client.
     * @param t
     * @param header
     */
    private void handleError(Throwable t, Frame.Header header)
    {
        errorHandler.accept(ErrorMessage.wrap(t, header.streamId));
    }

    /**
     * For use in the case where the error can't be mapped to a specific stream id,
     * such as a corrupted messaging frame, or when extracing a CQL frame from the
     * messaging frame's payload fails. This does not attempt to release any
     * resources, as these errors should only occur before any capacity acquisition
     * is attempted (e.g. on receipt of a corrupt messaging frame, or failure to
     * extract a CQL frame from the message body).
     *
     * @param t
     */
    private void handleError(Throwable t)
    {
        errorHandler.accept(t);
    }

    // Acts as a Dispatcher.FlushItemConverter
    private Framed toFlushItem(Channel channel, Message.Request request, Message.Response response)
    {
        // Returns a FlushItem.Framed instance which wraps a Consumer<FlushItem> that performs
        // the work of returning the capacity allocated for processing the request.
        // The Dispatcher will call this to obtain the FlushItem to enqueue with its Flusher once
        // a dispatched request has been processed.
        return new Framed(channel,
                          response.encode(request.getSourceFrame().header.version),
                          request.getSourceFrame(),
                          payloadAllocator,
                          this::release);
    }

    private void release(Flusher.FlushItem<Frame> flushItem)
    {
        release(flushItem.sourceFrame.header);
        flushItem.sourceFrame.release();
    }

    private void release(Frame.Header header)
    {
        releaseCapacity(Ints.checkedCast(header.bodySizeInBytes));
        channelPayloadBytesInFlight -= header.bodySizeInBytes;
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
            // max (CQL) frame size defaults to 256mb, so should be safe to downcast
            int frameSize = Ints.checkedCast(header.bodySizeInBytes);
            receivedCount++;
            receivedBytes += buf.remaining();

            if (throwOnOverload)
            {
                LargeMessage largeMessage = new LargeMessage(header);
                if (!acquireCapacity(header, endpointReserve, globalReserve))
                {
                    // discard the request and throw an exception
                    ClientMetrics.instance.markRequestDiscarded();
                    logger.trace("Discarded request of size: {}. InflightChannelRequestPayload: {}, InflightEndpointRequestPayload: {}, InflightOverallRequestPayload: {}, Header: {}",
                                 frameSize,
                                 channelPayloadBytesInFlight,
                                 endpointReserve.using(),
                                 globalReserve.using(),
                                 header);

                    // mark as overloaded so that we consume
                    // subsequent frames and then discard the message
                    largeMessage.markOverloaded();
                }
                this.largeMessage = largeMessage;
                largeMessage.supply(frame);
                // Don't stop processing incoming frames, rely on the client to apply
                // backpressure when it receives OverloadedException
                return true;
            }
            else
            {
                if (!acquireCapacityAndQueueOnFailure(header, endpointReserve, globalReserve))
                {
                    receivedCount++;
                    receivedBytes += frame.frameSize;
                    return false;
                }
            }

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
     * encountered. Consequently, we terminate the connection (via a ProtocolException) whenever a
     * corrupt frame is encountered, regardless of its type.
     */
    protected void processCorruptFrame(FrameDecoder.CorruptFrame frame)
    {
        corruptFramesUnrecovered++;
        String error = String.format("%s invalid, unrecoverable CRC mismatch detected in frame %s. Read %d, Computed %d",
                                     id(), frame.isRecoverable() ? "body" : "header", frame.readCRC, frame.computedCRC);

        noSpamLogger.error(error);

        // If this is part of a multi-frame message, process it before passing control to the error handler.
        // This is so we can take care of any housekeeping associated with large messages.
        if (!frame.isSelfContained)
        {
            if (null == largeMessage) // first frame of a large message
                receivedBytes += frame.frameSize;
            else // subsequent frame of a large message
                processSubsequentFrameOfLargeMessage(frame);
        }

        handleError(new ProtocolException(error));
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

        private boolean overloaded = false;

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

        /**
         * Used to indicate that a message should be dropped and not processed.
         * We do this on receipt of the first frame of a large message if sufficient capacity
         * cannot be acquired to process it and throwOnOverload is set for the connection.
         * In this case, the client has elected to shed load rather than apply backpressure
         * so we must ensure that subsequent frames are consumed from the channel. At that
         * point an error response is returned to the client, rather than processing the message.
         */
        private void markOverloaded()
        {
            overloaded = true;
        }

        protected void onComplete()
        {
            if (overloaded)
            {
                handleError(new OverloadedException("Server is in overloaded state. " +
                                                              "Cannot accept more requests at this point"),
                            header);
            }
            else if (!isCorrupt)
            {
                try
                {
                    processCqlFrame(assembleFrame());
                }
                catch (Exception e)
                {
                    handleErrorAndRelease(e, header);
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
