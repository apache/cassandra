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
import java.util.function.BiConsumer;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.net.AbstractMessageHandler;
import org.apache.cassandra.net.FrameDecoder;
import org.apache.cassandra.net.FrameDecoder.IntactFrame;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.net.ResourceLimits.Limit;
import org.apache.cassandra.net.ShareableBytes;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.utils.MonotonicClock.approxTime;

public class CQLMessageHandler extends AbstractMessageHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CQLMessageHandler.class);
    private final org.apache.cassandra.transport.Frame.Decoder cqlFrameDecoder;
    private final Message.ProtocolDecoder messageDecoder;
    private final BiConsumer<Channel, Message> dispatcher;
    private final boolean throwOnOverload;

    long receivedCount, receivedBytes;
    long channelPayloadBytesInFlight;

    CQLMessageHandler(Channel channel,
                      FrameDecoder decoder,
                      Frame.Decoder cqlFrameDecoder,
                      Message.ProtocolDecoder messageDecoder,
                      BiConsumer<Channel, Message> dispatcher,
                      Limit endpointReserve,
                      Limit globalReserve,
                      WaitQueue endpointWaitQueue,
                      WaitQueue globalWaitQueue,
                      OnHandlerClosed onClosed,
                      boolean throwOnOverload)
    {
        super(decoder,
              channel,
              FrameEncoder.Payload.MAX_SIZE - 1,
              1 << 22,                           // TODO check default and add to DD
              endpointReserve,
              globalReserve,
              endpointWaitQueue,
              globalWaitQueue,
              onClosed);
        this.cqlFrameDecoder    = cqlFrameDecoder;
        this.messageDecoder     = messageDecoder;
        this.dispatcher         = dispatcher;
        this.throwOnOverload    = throwOnOverload;
    }

    protected boolean processOneContainedMessage(ShareableBytes bytes, Limit endpointReserve, Limit globalReserve) throws IOException
    {
        Frame frame = toCqlFrame(bytes);
        if (frame == null)
            return false;

        // max (CQL) frame size defaults to 256mb, so should be safe to downcast
        int size = Ints.checkedCast(frame.header.bodySizeInBytes);

        if (!acquireCapacity(frame.header, endpointReserve, globalReserve))
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
        dispatcher.accept(channel, message);
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
        // TODO
        return channel.id().asShortText();
    }

    private boolean acquireCapacity(Frame.Header header, Limit endpointReserve, Limit globalReserve)
    {
        int frameSize = Ints.checkedCast(header.bodySizeInBytes);
        long currentTimeNanos = approxTime.now();
        boolean acquired = acquireCapacity(endpointReserve, globalReserve, frameSize, currentTimeNanos, Long.MAX_VALUE);

        // check for overloaded state by trying to allocate framesize to inflight payload trackers
        if (!acquired)
        {
            if (throwOnOverload)
            {
                // discard the request and throw an exception
                ClientMetrics.instance.markRequestDiscarded();
                logger.trace("Discarded request of size: {}. InflightChannelRequestPayload: {}, InflightEndpointRequestPayload: {}, InflightOverallRequestPayload: {}, Header: {}",
                             frameSize,
                             channelPayloadBytesInFlight,
                             endpointReserve.using(),
                             globalReserve.using(),
                             header);
                decoder.discard();
                throw ErrorMessage.wrap(new OverloadedException("Server is in overloaded state. Cannot accept more requests at this point"),
                                        header.streamId);
            }
            else
            {
                // force overallocation so we can process this one message, then return false
                // so that we stop processing further frames from the decoder
                endpointReserve.allocate(frameSize);
                ClientMetrics.instance.pauseConnection();
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
//    private void processCorruptFrame(FrameDecoder.CorruptFrame frame) throws Crc.InvalidCrc
//    {
//        if (!frame.isRecoverable())
//        {
//            corruptFramesUnrecovered++;
//            throw new Crc.InvalidCrc(frame.readCRC, frame.computedCRC);
//        }
//        else if (frame.isSelfContained)
//        {
//            receivedBytes += frame.frameSize;
//            corruptFramesRecovered++;
//            noSpamLogger.warn("{} invalid, recoverable CRC mismatch detected while reading messages (corrupted self-contained frame)", id());
//        }
//        else if (null == largeMessage) // first frame of a large message
//        {
//            receivedBytes += frame.frameSize;
//            corruptFramesUnrecovered++;
//            noSpamLogger.error("{} invalid, unrecoverable CRC mismatch detected while reading messages (corrupted first frame of a large message)", id());
//            throw new Crc.InvalidCrc(frame.readCRC, frame.computedCRC);
//        }
//        else // subsequent frame of a large message
//        {
//            processSubsequentFrameOfLargeMessage(frame);
//            corruptFramesRecovered++;
//            noSpamLogger.warn("{} invalid, recoverable CRC mismatch detected while reading a large message", id());
//        }
//    }

    private static int frameSize(Frame.Header header)
    {
        return Frame.Header.LENGTH + Ints.checkedCast(header.bodySizeInBytes);
    }

    private class LargeMessage extends AbstractMessageHandler.LargeMessage<Frame.Header>
    {
        private static final long EXPIRES_AT = Long.MAX_VALUE;

        private LargeMessage(Frame.Header header)
        {
            super(frameSize(header), header, Long.MAX_VALUE, false);
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
