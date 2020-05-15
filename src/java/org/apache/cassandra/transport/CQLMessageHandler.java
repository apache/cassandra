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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.net.Crc;
import org.apache.cassandra.net.FrameDecoder;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.net.ResourceLimits;
import org.apache.cassandra.net.ShareableBytes;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

public class CQLMessageHandler extends ChannelInboundHandlerAdapter implements FrameDecoder.FrameProcessor
{
    private static final Logger logger = LoggerFactory.getLogger(CQLMessageHandler.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.SECONDS);
    private final FrameDecoder decoder;
    private final Channel channel;
    private final org.apache.cassandra.transport.Frame.Decoder cqlFrameDecoder;
    private final Message.ProtocolDecoder messageDecoder;
    private final ResourceLimits.EndpointAndGlobal limits;
    private final Consumer<Message> dispatcher;
    private final boolean throwOnOverload;

    private LargeMessage largeMessage;

    long corruptFramesRecovered, corruptFramesUnrecovered;
    long receivedCount, receivedBytes;

    private long channelPayloadBytesInFlight;
    private boolean paused;

    CQLMessageHandler(Channel channel,
                      FrameDecoder decoder,
                      org.apache.cassandra.transport.Frame.Decoder cqlFrameDecoder,
                      Message.ProtocolDecoder messageDecoder,
                      Consumer<Message> dispatcher,
                      ResourceLimits.EndpointAndGlobal limits,
                      boolean throwOnOverload)
    {
        this.decoder            = decoder;
        this.channel            = channel;
        this.cqlFrameDecoder    = cqlFrameDecoder;
        this.messageDecoder     = messageDecoder;
        this.dispatcher         = dispatcher;
        this.limits             = limits;
        this.throwOnOverload    = throwOnOverload;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        /*
         * InboundMessageHandler works in tandem with FrameDecoder to implement flow control
         * and work stashing optimally. We rely on FrameDecoder to invoke the provided
         * FrameProcessor rather than on the pipeline and invocations of channelRead().
         * process(Frame) is the primary entry point for this class.
         */
        throw new IllegalStateException("InboundMessageHandler doesn't expect channelRead() to be invoked");
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        decoder.activate(this); // the frame decoder starts inactive until explicitly activated by the added inbound message handler
    }

    @Override
    public boolean process(FrameDecoder.Frame frame) throws IOException
    {
        if (frame instanceof FrameDecoder.IntactFrame)
            return processIntactFrame((FrameDecoder.IntactFrame) frame, limits);

        processCorruptFrame((FrameDecoder.CorruptFrame) frame);
        return true;
    }

    private boolean processIntactFrame(FrameDecoder.IntactFrame frame, ResourceLimits.EndpointAndGlobal limits) throws IOException
    {
        if (frame.isSelfContained)
            return processFrameOfContainedMessages(frame.contents, limits);
        else if (null == largeMessage)
            return processFirstFrameOfLargeMessage(frame, limits);
        else
            return processSubsequentFrameOfLargeMessage(frame);
    }

    /*
     * Handle contained messages (not crossing boundaries of the frame) - both small and large, for the inbound
     * definition of large (breaching the size threshold for what we are willing to process on event-loop vs.
     * off event-loop).
     */
    private boolean processFrameOfContainedMessages(ShareableBytes bytes, ResourceLimits.EndpointAndGlobal limits) throws IOException
    {
        while (bytes.hasRemaining())
            if (!processOneContainedMessage(bytes, limits))
                return false;
        return true;
    }

    private boolean processOneContainedMessage(ShareableBytes bytes, ResourceLimits.EndpointAndGlobal limits) throws IOException
    {
        Frame frame = toCqlFrame(bytes);
        if (frame == null)
            return false;

        // TODO old max frame size defaults to 256mb, so should be safe to downcast
        int size = Ints.checkedCast(frame.header.bodySizeInBytes);

        // TODO rename
        if (!shouldHandleRequest(frame.header, limits))
        {
            // we're over allocated here, but process the message
            // anyway because we didn't throw any exception
            receivedCount++;
            receivedBytes += size;
            processCqlFrame(frame);
            return false;
        }

        receivedCount++;
        receivedBytes += size;

        if (size <= FrameEncoder.Payload.MAX_SIZE)
            processCqlFrame(frame);
        else
            processLargeMessage(frame);

        return true;
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

        // get o.a.c.transport.Frame.Header from buf
        // deserialize o.a.c.transport.Message from buf
        // create task to process Header + Message
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
        dispatcher.accept(message);
    }

    /*
     * Handling of multi-frame large messages
     */

    private boolean processFirstFrameOfLargeMessage(FrameDecoder.IntactFrame frame,
                                                    ResourceLimits.EndpointAndGlobal limits) throws IOException
    {
        ShareableBytes bytes = frame.contents;
        ByteBuffer buf = bytes.get();
        try
        {
            Frame.Header header = cqlFrameDecoder.decodeHeader(Unpooled.wrappedBuffer(buf));

            if (!shouldHandleRequest(header, limits))
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

    private boolean processSubsequentFrameOfLargeMessage(FrameDecoder.Frame frame)
    {
        receivedBytes += frame.frameSize;
        if (largeMessage.supply(frame))
        {
            receivedCount++;
            largeMessage = null;
        }
        return true;
    }

    private String id()
    {
        // TODO
        return channel.id().asShortText();
    }

    private void releaseCapacity(int bytes)
    {
        limits.release(bytes);
        // TODO
    }

    private boolean shouldHandleRequest(Frame.Header header,
                                        ResourceLimits.EndpointAndGlobal limits)
    {
        long frameSize = header.bodySizeInBytes;

        // check for overloaded state by trying to allocate framesize to inflight payload trackers
        if (limits.tryAllocate(frameSize) != ResourceLimits.Outcome.SUCCESS)
        {
            if (throwOnOverload)
            {
                // discard the request and throw an exception
                ClientMetrics.instance.markRequestDiscarded();
                logger.trace("Discarded request of size: {}. InflightChannelRequestPayload: {}, InflightEndpointRequestPayload: {}, InflightOverallRequestPayload: {}, Header: {}",
                             frameSize,
                             channelPayloadBytesInFlight,
                             limits.endpoint().using(),
                             limits.global().using(),
                             header);
                decoder.discard();
                throw ErrorMessage.wrap(new OverloadedException("Server is in overloaded state. Cannot accept more requests at this point"),
                                        header.streamId);
            }
            else
            {
                // force overallocation so we can process this one message, then return false
                // so that we stop processing further frames from the decoder
                limits.allocate(frameSize);
                ClientMetrics.instance.pauseConnection();
                paused = true;
                channelPayloadBytesInFlight += frameSize;
                return false;
            }
        }
        channelPayloadBytesInFlight += frameSize;
        return true;
    }

    /*
     * We can handle some corrupt frames gracefully without dropping the connection and losing all the
     * queued up messages, but not others.
     *
     * Corrupt frames that *ARE NOT* safe to skip gracefully and require the connection to be dropped:
     *  - any frame with corrupt header (!frame.isRecoverable())
     *  - first corrupt-payload frame of a large message (impossible to infer message size, and without it
     *    impossible to skip the message safely
     *
     * Corrupt frames that *ARE* safe to skip gracefully, without reconnecting:
     *  - any self-contained frame with a corrupt payload (but not header): we lose all the messages in the
     *    frame, but that has no effect on subsequent ones
     *  - any non-first payload-corrupt frame of a large message: we know the size of the large message in
     *    flight, so we just skip frames until we've seen all its bytes; we only lose the large message
     */
    private void processCorruptFrame(FrameDecoder.CorruptFrame frame) throws Crc.InvalidCrc
    {
        if (!frame.isRecoverable())
        {
            corruptFramesUnrecovered++;
            throw new Crc.InvalidCrc(frame.readCRC, frame.computedCRC);
        }
        else if (frame.isSelfContained)
        {
            receivedBytes += frame.frameSize;
            corruptFramesRecovered++;
            noSpamLogger.warn("{} invalid, recoverable CRC mismatch detected while reading messages (corrupted self-contained frame)", id());
        }
        else if (null == largeMessage) // first frame of a large message
        {
            receivedBytes += frame.frameSize;
            corruptFramesUnrecovered++;
            noSpamLogger.error("{} invalid, unrecoverable CRC mismatch detected while reading messages (corrupted first frame of a large message)", id());
            throw new Crc.InvalidCrc(frame.readCRC, frame.computedCRC);
        }
        else // subsequent frame of a large message
        {
            processSubsequentFrameOfLargeMessage(frame);
            corruptFramesRecovered++;
            noSpamLogger.warn("{} invalid, recoverable CRC mismatch detected while reading a large message", id());
        }
    }

    private class LargeMessage
    {
        private final int size;
        private final Frame.Header header;

        private final List<ShareableBytes> buffers = new ArrayList<>();
        private long received;

        private boolean isCorrupt;

        private LargeMessage(Frame.Header header)
        {
            this.size = Frame.Header.LENGTH + Ints.checkedCast(header.bodySizeInBytes);
            this.header = header;
        }

        private LargeMessage(Frame.Header header, ShareableBytes bytes)
        {
            this(header);
            buffers.add(bytes);
        }

        private void schedule()
        {
            processCqlFrame(assembleFrame());
            releaseBuffers();
        }

        private Frame assembleFrame()
        {
            // TODO we already have the frame header, so we could skip HEADER_LENGTH
            // and go straight to message decoding
            ByteBuf concat = Unpooled.wrappedBuffer(buffers.stream()
                                                           .map(ShareableBytes::get)
                                                           .toArray(ByteBuffer[]::new));
            try
            {
                return cqlFrameDecoder.decodeFrame(concat);
            } catch (Exception e)
            {
                buffers.forEach(ShareableBytes::release);
                throw new RuntimeException("Error deserializing CQL frame", e);
            }
            //TODO lifecycle
        }

        /**
         * Return true if this was the last frame of the large message.
         */
        private boolean supply(FrameDecoder.Frame frame)
        {
            if (frame instanceof FrameDecoder.IntactFrame)
                onIntactFrame((FrameDecoder.IntactFrame) frame);
            else
                onCorruptFrame();

            received += frame.frameSize;
            if (size == received)
                onComplete();
            return size == received;
        }

        private void onIntactFrame(FrameDecoder.IntactFrame frame)
        {
            if (!isCorrupt)
            {
                buffers.add(frame.contents.sliceAndConsume(frame.frameSize).share());
                return;
            }
            frame.consume();
        }

        private void onCorruptFrame()
        {
            if (!isCorrupt)
                releaseBuffersAndCapacity(); // release resources once we transition from normal state to corrupt
            isCorrupt = true;
        }

        private void onComplete()
        {
            if (!isCorrupt)
                schedule();
        }

        private void abort()
        {
            if (!isCorrupt)
                releaseBuffersAndCapacity(); // release resources if in normal state when abort() is invoked
        }

        private void releaseBuffers()
        {
            buffers.forEach(ShareableBytes::release); buffers.clear();
        }

        private void releaseBuffersAndCapacity()
        {
            releaseBuffers();
            releaseCapacity(size);
        }
    }
}
