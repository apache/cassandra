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
import org.apache.cassandra.transport.ClientResourceLimits.Overload;
import org.apache.cassandra.utils.concurrent.NonBlockingRateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.apache.cassandra.config.DatabaseDescriptor;
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
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

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
    public static final TimeUnit RATE_LIMITER_DELAY_UNIT = TimeUnit.NANOSECONDS;

    private final Envelope.Decoder envelopeDecoder;
    private final Message.Decoder<M> messageDecoder;
    private final FrameEncoder.PayloadAllocator payloadAllocator;
    private final MessageConsumer<M> dispatcher;
    private final ErrorHandler errorHandler;
    private final boolean throwOnOverload;
    private final ProtocolVersion version;
    private final NonBlockingRateLimiter requestRateLimiter;

    long channelPayloadBytesInFlight;
    private int consecutiveMessageErrors = 0;

    interface MessageConsumer<M extends Message>
    {
        void accept(Channel channel, M message, Dispatcher.FlushItemConverter toFlushItem, Overload backpressure);
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
        this.requestRateLimiter = resources.requestRateLimiter();
    }

    @Override
    public boolean process(FrameDecoder.Frame frame) throws IOException
    {
        // new frame, clean slate for processing errors
        consecutiveMessageErrors = 0;
        return super.process(frame);
    }

    /**
     * Checks limits on bytes in flight and the request rate limiter (if enabled), then takes one of three actions:
     * 
     * 1.) If no limits are breached, process the request.
     * 2.) If a limit is breached, and the connection is configured to throw on overload, throw {@link OverloadedException}.
     * 3.) If a limit is breached, and the connection is not configurd to throw, process the request, and return false
     *     to let the {@link FrameDecoder} know it should stop processing frames.
     *     
     * If the connection is configured to throw {@link OverloadedException}, requests that breach the rate limit are
     * not counted against that limit.
     * 
     * @return true if the {@link FrameDecoder} should continue to process incoming frames, and false if it should stop
     *         processing them, effectively applying backpressure to clients
     * 
     * @throws ErrorMessage.WrappedException with an {@link OverloadedException} if overload occurs and the 
     *         connection is configured to throw on overload
     */
    protected boolean processOneContainedMessage(ShareableBytes bytes, Limit endpointReserve, Limit globalReserve)
    {
        ByteBuffer buf = bytes.get();
        Envelope.Decoder.HeaderExtractionResult extracted = envelopeDecoder.extractHeader(buf);
        if (!extracted.isSuccess())
            return handleProtocolException(extracted.error(), buf, extracted.streamId(), extracted.bodyLength());

        Envelope.Header header = extracted.header();
        if (header.version != version)
        {
            ProtocolException error = new ProtocolException(String.format("Invalid message version. Got %s but previous" +
                                                                          "messages on this connection had version %s",
                                                                          header.version, version));
            return handleProtocolException(error, buf, header.streamId, header.bodySizeInBytes);
        }

        // max CQL message size defaults to 256mb, so should be safe to downcast
        int messageSize = Ints.checkedCast(header.bodySizeInBytes);
        
        if (throwOnOverload)
        {
            if (!acquireCapacity(header, endpointReserve, globalReserve))
            {
                discardAndThrow(endpointReserve, globalReserve, buf, header, messageSize, Overload.BYTES_IN_FLIGHT);
                return true;
            }

            if (DatabaseDescriptor.getNativeTransportRateLimitingEnabled() && !requestRateLimiter.tryReserve())
            {
                // We've already allocated against the bytes-in-flight limits, so release those resources.
                release(header);
                discardAndThrow(endpointReserve, globalReserve, buf, header, messageSize, Overload.REQUESTS);
                return true;
            }
        }
        else
        {
            Overload backpressure = Overload.NONE;

            if (!acquireCapacityAndQueueOnFailure(header, endpointReserve, globalReserve))
            {
                if (processRequestAndUpdateMetrics(bytes, header, messageSize, Overload.BYTES_IN_FLIGHT))
                {
                    if (decoder.isActive())
                        ClientMetrics.instance.pauseConnection();
                }

                backpressure = Overload.BYTES_IN_FLIGHT;
            }
            
            if (DatabaseDescriptor.getNativeTransportRateLimitingEnabled())
            {
                // Reserve a permit even if we've already triggered backpressure on bytes in flight.
                long delay = requestRateLimiter.reserveAndGetDelay(RATE_LIMITER_DELAY_UNIT);
                
                if (backpressure == Overload.NONE && delay > 0)
                {
                    if (processRequestAndUpdateMetrics(bytes, header, messageSize, Overload.REQUESTS))
                    {
                        if (decoder.isActive())
                            ClientMetrics.instance.pauseConnection();

                        // Schedule a wakup here if we process successfully. The connection should be closing otherwise.  
                        scheduleConnectionWakeupTask(delay, RATE_LIMITER_DELAY_UNIT);
                    }
                    
                    backpressure = Overload.REQUESTS;
                }
            }
            
            // If we triggered backpressure, make sure the caller stops processing frames after the request completes.
            if (backpressure != Overload.NONE)
                return false;
        }

        return processRequestAndUpdateMetrics(bytes, header, messageSize, Overload.NONE);
    }

    private boolean processRequestAndUpdateMetrics(ShareableBytes bytes, Envelope.Header header, int messageSize, Overload backpressure)
    {
        channelPayloadBytesInFlight += messageSize;
        incrementReceivedMessageMetrics(messageSize);
        return processRequest(composeRequest(header, bytes), backpressure);
    }

    private void discardAndThrow(Limit endpointReserve, Limit globalReserve, 
                                 ByteBuffer buf, Envelope.Header header, int messageSize,
                                 Overload overload)
    {
        ClientMetrics.instance.markRequestDiscarded();
        logOverload(endpointReserve, globalReserve, header, messageSize);

        OverloadedException exception = buildOverloadedException(endpointReserve, globalReserve, overload);
        handleError(exception, header);

        // Don't stop processing incoming messages, as we rely on the client to apply
        // backpressure when it receives OverloadedException, but discard this message 
        // as we're responding with the overloaded error.
        incrementReceivedMessageMetrics(messageSize);
        buf.position(buf.position() + Envelope.Header.LENGTH + messageSize);
    }

    private OverloadedException buildOverloadedException(Limit endpointReserve, Limit globalReserve, Overload overload) {
        return overload == Overload.REQUESTS
                ? new OverloadedException(String.format("Request breached global limit of %d requests/second. Server is " +
                                                        "currently in an overloaded state and cannot accept more requests.", 
                                                        requestRateLimiter.getRate()))
                : new OverloadedException(String.format("Request breached limit on bytes in flight. (Endpoint: %d/%d bytes, Global: %d/%d bytes.) " +
                                                        "Server is currently in an overloaded state and cannot accept more requests.",
                                                        endpointReserve.using(), endpointReserve.limit(), globalReserve.using(), globalReserve.limit()));
    }

    private void logOverload(Limit endpointReserve, Limit globalReserve, Envelope.Header header, int messageSize)
    {
        logger.trace("Discarded request of size {} with {} bytes in flight on channel. " + 
                     "Using {}/{} bytes of endpoint limit and {}/{} bytes of global limit. " + 
                     "Global rate limiter: {} Header: {}",
                     messageSize, channelPayloadBytesInFlight,
                     endpointReserve.using(), endpointReserve.limit(), globalReserve.using(), globalReserve.limit(),
                     requestRateLimiter, header);
    }

    private boolean handleProtocolException(ProtocolException exception,
                                            ByteBuffer buf,
                                            int streamId,
                                            long expectedMessageLength)
    {
        // hard fail if either :
        //  * the expectedMessageLength is < 0 as we're unable to  skip the remainder
        //    of the Envelope and attempt to read the next one
        //  * we hit a run of errors in the same frame. Some errors are recoverable
        //    as they have no effect on subsequent Envelopes, in which case we attempt
        //    to continue processing. If we start seeing consecutive errors we assume
        //    that this is not the case and that the entire remaining frame is garbage.
        //    It's possible here that we fail hard when we could potentially not do
        //    (e.g. every Envelope has an invalid opcode, but is otherwise semantically
        //    intact), but this is a trade off.
        if (expectedMessageLength < 0 || ++consecutiveMessageErrors > DatabaseDescriptor.getConsecutiveMessageErrorsThreshold())
        {
            // transform the exception to a fatal one so the exception handler closes the channel
            if (!exception.isFatal())
                exception = ProtocolException.toFatalException(exception);
            handleError(exception, streamId);
            return false;
        }
        else
        {
            // exception should not be a fatal error or the exception handler will close the channel
            handleError(exception, streamId);
            // skip body
            buf.position(Math.min(buf.limit(), buf.position() + Envelope.Header.LENGTH + Ints.checkedCast(expectedMessageLength)));
            // continue processing frame
            return true;
        }
    }

    private void incrementReceivedMessageMetrics(int messageSize)
    {
        receivedCount++;
        receivedBytes += messageSize + Envelope.Header.LENGTH;
        ClientMessageSizeMetrics.bytesReceived.inc(messageSize + Envelope.Header.LENGTH);
        ClientMessageSizeMetrics.bytesReceivedPerRequest.update(messageSize + Envelope.Header.LENGTH);
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

    protected boolean processRequest(Envelope request)
    {
        return processRequest(request, Overload.NONE);
    }
    
    protected boolean processRequest(Envelope request, Overload backpressure)
    {
        M message = null;
        try
        {
            message = messageDecoder.decode(channel, request);
            dispatcher.accept(channel, message, this::toFlushItem, backpressure);
            
            // sucessfully delivered a CQL message to the execution
            // stage, so reset the counter of consecutive errors
            consecutiveMessageErrors = 0;
            return true;
        }
        catch (Exception e)
        {
            if (message != null)
                request.release();

            boolean continueProcessing = true;

            // Indicate that an error was encountered. Initially, we can continue to
            // process the current frame, but if we keep catching errors, we assume that
            // the whole frame payload is no good, stop processing and close the connection.
            if(++consecutiveMessageErrors > DatabaseDescriptor.getConsecutiveMessageErrorsThreshold())
            {
                if (!(e instanceof ProtocolException))
                {
                    logger.debug("Error decoding CQL message", e);
                    e = new ProtocolException("Error encountered decoding CQL message: " + e.getMessage());
                }
                e = ProtocolException.toFatalException((ProtocolException) e);
                continueProcessing = false;
            }
            handleErrorAndRelease(e, request.header);
            return continueProcessing;
        }
    }

    /**
     * For "expected" errors this ensures we pass a WrappedException,
     * which contains a streamId, to the error handler. This makes
     * sure that whereever possible, the streamId is propagated back
     * to the client.
     * This also releases the capacity acquired for processing as
     * indicated by supplied header.
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
     */
    private void handleError(Throwable t, Envelope.Header header)
    {
        handleError(t, header.streamId);
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
     */
    private void handleError(Throwable t, int streamId)
    {
        errorHandler.accept(ErrorMessage.wrap(t, streamId));
    }

    /**
     * For use in the case where the error can't be mapped to a specific stream id,
     * such as a corrupted frame, or when extracting a CQL message from the frame's
     * payload fails. This does not attempt to release any resources, as these errors
     * should only occur before any capacity acquisition is attempted (e.g. on receipt
     * of a corrupt frame, or failure to extract a CQL message from the envelope).
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
            Envelope.Decoder.HeaderExtractionResult extracted = envelopeDecoder.extractHeader(buf);
            if (!extracted.isSuccess())
            {
                // Hard fail on any decoding error as we can't trust the subsequent frames of
                // the large message
                handleError(ProtocolException.toFatalException(extracted.error()));
                return false;
            }

            Envelope.Header header = extracted.header();
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
                    // Mark as overloaded so that discard the message after consuming any subsequent frames.
                    ClientMetrics.instance.markRequestDiscarded();
                    logOverload(endpointReserve, globalReserve, header, messageSize);
                    largeMessage.markOverloaded(Overload.BYTES_IN_FLIGHT);
                }
            }
            else if (DatabaseDescriptor.getNativeTransportRateLimitingEnabled())
            {
                if (throwOnOverload)
                {
                    if (!requestRateLimiter.tryReserve())
                    {
                        ClientMetrics.instance.markRequestDiscarded();
                        logOverload(endpointReserve, globalReserve, header, messageSize);
                        
                        // Mark as overloaded so that we discard the message after consuming any subsequent frames.
                        // (i.e. Request resources we may already have acquired above will be released.)
                        largeMessage.markOverloaded(Overload.REQUESTS);
                        
                        this.largeMessage = largeMessage;
                        largeMessage.supply(frame);
                        return true;
                    }
                }
                else
                {
                    long delay = requestRateLimiter.reserveAndGetDelay(RATE_LIMITER_DELAY_UNIT);

                    if (delay > 0)
                    {
                        this.largeMessage = largeMessage;
                        largeMessage.markBackpressure(Overload.REQUESTS);
                        largeMessage.supply(frame);

                        if (decoder.isActive())
                            ClientMetrics.instance.pauseConnection();

                        scheduleConnectionWakeupTask(delay, RATE_LIMITER_DELAY_UNIT);
                        return false;
                    }
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

    private void scheduleConnectionWakeupTask(long waitLength, TimeUnit unit)
    {
        channel.eventLoop().schedule(() ->
                                     {
                                         try
                                         {
                                             // We might have already reactivated via another wake task.
                                             if (!decoder.isActive())
                                             {
                                                 decoder.reactivate();

                                                 // Only update the relevant metric if we've actually activated.
                                                 if (decoder.isActive())
                                                     ClientMetrics.instance.unpauseConnection();
                                             }
                                         }
                                         catch (Throwable t)
                                         {
                                             fatalExceptionCaught(t);
                                         }
                                     },
                                     waitLength,
                                     unit);
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

        handleError(ProtocolException.toFatalException(new ProtocolException(error)));
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

        private Overload overload = Overload.NONE;
        private Overload backpressure = Overload.NONE;

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
        private void markOverloaded(Overload overload)
        {
            this.overload = overload;
        }

        private void markBackpressure(Overload backpressure)
        {
            this.backpressure = backpressure;
        }

        protected void onComplete()
        {
            if (overload != Overload.NONE)
                handleErrorAndRelease(buildOverloadedException(endpointReserveCapacity, globalReserveCapacity, overload), header);
            else if (!isCorrupt)
                processRequest(assembleFrame(), backpressure);
        }

        protected void abort()
        {
            if (!isCorrupt)
                releaseBuffersAndCapacity(); // release resources if in normal state when abort() is invoked
        }
    }
}
