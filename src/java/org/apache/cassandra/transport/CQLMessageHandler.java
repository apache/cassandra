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
import org.apache.cassandra.metrics.ClientMessageSizeMetrics;
import org.apache.cassandra.net.AbstractMessageHandler;
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

/**
 * Implementation of {@link AbstractMessageHandler} for processing CQL messages which comprise a {@link Message} wrapped
 * in an {@link Envelope}. This class is parameterized by a {@link Message} subtype, expected to be either
 * {@link Message.Request} or {@link Message.Response}. Most commonly, an instance for handling {@link Message.Request}
 * is created for each inbound CQL client connection.
 *
 * # Small vs large messages
 * Small messages are deserialized in place, and then handed off to a consumer for processing.
 * Large messages accumulate frames until all bytes for the envelope are received, then concatenate and deserialize the
 * frames on the event loop thread and pass them on to the same consumer.
 *
 * # Flow control (backpressure)
 * The size of an incoming message is explicit in the {@link Envelope.Header}.
 *
 * By default, every connection has 1MiB of exlusive permits available before needing to access the per-endpoint
 * and global reserves. By default, those reserves are sized proportionally to the heap - 2.5% of heap per-endpoint
 * and a 10% for the global reserve.
 *
 * Permits are held while CQL messages are processed and released after the response has been encoded into the
 * buffers of the response frame.
 *
 * A connection level option (THROW_ON_OVERLOAD) allows clients to choose the backpressure strategy when a connection
 * has exceeded the maximum number of allowed permits. The choices are to either pause reads from the incoming socket
 * and allow TCP backpressure to do the work, or to throw an explict exception and rely on the client to back off.
 */
public class CQLMessageHandler<M extends Message> extends AbstractMessageHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CQLMessageHandler.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.SECONDS);

    public static final int LARGE_MESSAGE_THRESHOLD = FrameEncoder.Payload.MAX_SIZE - 1;

    private final Envelope.Decoder envelopeDecoder;
    private final Message.Decoder<M> messageDecoder;
    private final FrameEncoder.PayloadAllocator payloadAllocator;
    private final MessageConsumer<M> dispatcher;
    private final ErrorHandler errorHandler;
    private final boolean throwOnOverload;
    private final ProtocolVersion version;

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
                      ProtocolVersion version,
                      FrameDecoder decoder,
                      Envelope.Decoder envelopeDecoder,
                      Message.Decoder<M> messageDecoder,
                      MessageConsumer<M> dispatcher,
                      FrameEncoder.PayloadAllocator payloadAllocator,
                      int queueCapacity,
                      ClientResourceLimits.ResourceProvider resources,
                      OnHandlerClosed onClosed,
                      ErrorHandler errorHandler,
                      boolean throwOnOverload)
    {
        super(decoder,
              channel,
              LARGE_MESSAGE_THRESHOLD,
              queueCapacity,
              resources.endpointLimit(),
              resources.globalLimit(),
              resources.endpointWaitQueue(),
              resources.globalWaitQueue(),
              onClosed);
        this.envelopeDecoder    = envelopeDecoder;
        this.messageDecoder     = messageDecoder;
        this.payloadAllocator   = payloadAllocator;
        this.dispatcher         = dispatcher;
        this.errorHandler       = errorHandler;
        this.throwOnOverload    = throwOnOverload;
        this.version            = version;
    }

    protected boolean processOneContainedMessage(ShareableBytes bytes, Limit endpointReserve, Limit globalReserve)
    {
        Envelope.Header header = extractHeader(bytes.get());
        // null indicates a failure to extract the CQL message header.
        // This will trigger a protocol exception and closing of the connection.
        if (null == header)
            return false;

        // max CQL message size defaults to 256mb, so should be safe to downcast
        int messageSize = Ints.checkedCast(header.bodySizeInBytes);

        if (throwOnOverload)
        {
            if (!acquireCapacity(header, endpointReserve, globalReserve))
            {
                // discard the request and throw an exception
                ClientMetrics.instance.markRequestDiscarded();
                logger.trace("Discarded request of size: {}. InflightChannelRequestPayload: {}, " +
                             "InflightEndpointRequestPayload: {}, InflightOverallRequestPayload: {}, Header: {}",
                             messageSize,
                             channelPayloadBytesInFlight,
                             endpointReserve.using(),
                             globalReserve.using(),
                             header);

                handleError(new OverloadedException("Server is in overloaded state. " +
                                                    "Cannot accept more requests at this point"), header);

                // Don't stop processing incoming messages, rely on the client to apply
                // backpressure when it receives OverloadedException
                // but discard this message as we're responding with the overloaded error
                incrementReceivedMessageMetrics(messageSize);
                ByteBuffer buf = bytes.get();
                buf.position(buf.position() + Envelope.Header.LENGTH + messageSize);
                return true;
            }
        }
        else if (!acquireCapacityAndQueueOnFailure(header, endpointReserve, globalReserve))
        {
            // set backpressure on the channel, queuing the request until we have capacity
            ClientMetrics.instance.pauseConnection();
            return false;
        }

        channelPayloadBytesInFlight += messageSize;
        incrementReceivedMessageMetrics(messageSize);
        processRequest(composeRequest(header, bytes));
        return true;
    }

    private void incrementReceivedMessageMetrics(int messageSize)
    {
        receivedCount++;
        receivedBytes += messageSize + Envelope.Header.LENGTH;
        ClientMessageSizeMetrics.bytesReceived.inc(messageSize + Envelope.Header.LENGTH);
        ClientMessageSizeMetrics.bytesReceivedPerRequest.update(messageSize + Envelope.Header.LENGTH);
    }

    private Envelope.Header extractHeader(ByteBuffer buf)
    {
        try
        {
            Envelope.Header header = envelopeDecoder.extractHeader(buf);
            if (header.version != version)
                handleError(new ProtocolException(String.format("Invalid message version. Got %s but previous" +
                                                                "messages on this connection had version %s",
                                                                header.version, version)));

            return header;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("{} unexpected exception caught while deserializing a message header", id(), t);
            // Ideally we would recover from this as we could simply drop the entire frame.
            // However, we have no good way to communicate this back to the client as we don't
            // yet know the stream id. So this will trigger an Error response and the connection
            // to be closed.
            handleError(new ProtocolException("Deserialization error processing CQL payload"));
            return null;
        }
    }

    private Envelope composeRequest(Envelope.Header header, ShareableBytes bytes)
    {
        // extract body
        ByteBuffer buf = bytes.get();
        int idx = buf.position() + Envelope.Header.LENGTH;
        final int end = idx + Ints.checkedCast(header.bodySizeInBytes);
        ByteBuf body = Unpooled.wrappedBuffer(buf.slice());
        body.readerIndex(Envelope.Header.LENGTH);
        body.retain();
        buf.position(end);
        return new Envelope(header, body);
    }

    protected void processRequest(Envelope request)
    {
        M message = null;
        try
        {
            message = messageDecoder.decode(channel, request);
            dispatcher.accept(channel, message, this::toFlushItem);
        }
        catch (Exception e)
        {
            if (message != null)
                request.release();
            handleErrorAndRelease(e, request.header);
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
    private void handleErrorAndRelease(Throwable t, Envelope.Header header)
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
    private void handleError(Throwable t, Envelope.Header header)
    {
        errorHandler.accept(ErrorMessage.wrap(t, header.streamId));
    }

    /**
     * For use in the case where the error can't be mapped to a specific stream id,
     * such as a corrupted frame, or when extracting a CQL message from the frame's
     * payload fails. This does not attempt to release any resources, as these errors
     * should only occur before any capacity acquisition is attempted (e.g. on receipt
     * of a corrupt frame, or failure to extract a CQL message from the envelope).
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

        Envelope responseFrame = response.encode(request.getSource().header.version);
        int responseSize = envelopeSize(responseFrame.header);
        ClientMessageSizeMetrics.bytesSent.inc(responseSize);
        ClientMessageSizeMetrics.bytesSentPerResponse.update(responseSize);

        return new Framed(channel,
                          responseFrame,
                          request.getSource(),
                          payloadAllocator,
                          this::release);
    }

    private void release(Flusher.FlushItem<Envelope> flushItem)
    {
        release(flushItem.request.header);
        flushItem.request.release();
        flushItem.response.release();
    }

    private void release(Envelope.Header header)
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
            Envelope.Header header = extractHeader(buf);
            // max CQL message size defaults to 256mb, so should be safe to downcast
            int messageSize = Ints.checkedCast(header.bodySizeInBytes);
            receivedBytes += buf.remaining();

            LargeMessage largeMessage = new LargeMessage(header);
            if (!acquireCapacity(header, endpointReserve, globalReserve))
            {
                // In the case of large messages, never stop processing incoming frames
                // as this will halt the client meaning no further frames will be sent,
                // leading to starvation.
                // If the throwOnOverload option is set, don't process the message once
                // read, return an error response to notify the client that resource
                // limits have been exceeded. If the option isn't set, the only thing we
                // can do is to consume the subsequent frames and process the message.
                // Large and small messages are never interleaved for a single client, so
                // we know that this client will finish sending the large message before
                // anything else. Other clients sending small messages concurrently will
                // be backpressured by the global resource limits. The server is still
                // vulnerable to overload by multiple clients sending large messages
                // concurrently.
                if (throwOnOverload)
                {
                    // discard the request and throw an exception
                    ClientMetrics.instance.markRequestDiscarded();
                    logger.trace("Discarded request of size: {}. InflightChannelRequestPayload: {}, " +
                                 "InflightEndpointRequestPayload: {}, InflightOverallRequestPayload: {}, Header: {}",
                                 messageSize,
                                 channelPayloadBytesInFlight,
                                 endpointReserve.using(),
                                 globalReserve.using(),
                                 header);

                    // mark as overloaded so that discard the message
                    // after consuming any subsequent frames
                    largeMessage.markOverloaded();
                }
            }
            this.largeMessage = largeMessage;
            largeMessage.supply(frame);
            return true;
        }
        catch (Exception e)
        {
            throw new IOException("Error decoding CQL Message", e);
        }
    }

    protected String id()
    {
        return channel.id().asShortText();
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean acquireCapacityAndQueueOnFailure(Envelope.Header header, Limit endpointReserve, Limit globalReserve)
    {
        int bytesRequired = Ints.checkedCast(header.bodySizeInBytes);
        long currentTimeNanos = approxTime.now();
        return acquireCapacity(endpointReserve, globalReserve, bytesRequired, currentTimeNanos, Long.MAX_VALUE);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean acquireCapacity(Envelope.Header header, Limit endpointReserve, Limit globalReserve)
    {
        int bytesRequired = Ints.checkedCast(header.bodySizeInBytes);
        return acquireCapacity(endpointReserve, globalReserve, bytesRequired) == ResourceLimits.Outcome.SUCCESS;
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

    static int envelopeSize(Envelope.Header header)
    {
        return Envelope.Header.LENGTH + Ints.checkedCast(header.bodySizeInBytes);
    }

    private class LargeMessage extends AbstractMessageHandler.LargeMessage<Envelope.Header>
    {
        private static final long EXPIRES_AT = Long.MAX_VALUE;

        private boolean overloaded = false;

        private LargeMessage(Envelope.Header header)
        {
            super(envelopeSize(header), header, EXPIRES_AT, false);
        }

        private Envelope assembleFrame()
        {
            ByteBuf body = Unpooled.wrappedBuffer(buffers.stream()
                                                          .map(ShareableBytes::get)
                                                          .toArray(ByteBuffer[]::new));

            body.readerIndex(Envelope.Header.LENGTH);
            body.retain();
            return new Envelope(header, body);
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
                handleErrorAndRelease(new OverloadedException("Server is in overloaded state. " +
                                                              "Cannot accept more requests at this point"), header);
            else if (!isCorrupt)
                processRequest(assembleFrame());

        }

        protected void abort()
        {
            if (!isCorrupt)
                releaseBuffersAndCapacity(); // release resources if in normal state when abort() is invoked
        }
    }
}
