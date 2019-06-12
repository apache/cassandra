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
package org.apache.cassandra.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.exceptions.IncompatibleSchemaException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message.Header;
import org.apache.cassandra.net.FrameDecoder.Frame;
import org.apache.cassandra.net.FrameDecoder.FrameProcessor;
import org.apache.cassandra.net.FrameDecoder.IntactFrame;
import org.apache.cassandra.net.FrameDecoder.CorruptFrame;
import org.apache.cassandra.net.ResourceLimits.Limit;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.net.Crc.*;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;

/**
 * Core logic for handling inbound message deserialization and execution (in tandem with {@link FrameDecoder}).
 *
 * Handles small and large messages, corruption, flow control, dispatch of message processing onto an appropriate
 * thread pool.
 *
 * # Interaction with {@link FrameDecoder}
 *
 * {@link InboundMessageHandler} sits on top of a {@link FrameDecoder} in the Netty pipeline, and is tightly
 * coupled with it.
 *
 * {@link FrameDecoder} decodes inbound frames and relies on a supplied {@link FrameProcessor} to act on them.
 * {@link InboundMessageHandler} provides two implementations of that interface:
 *  - {@link #process(Frame)} is the default, primary processor, and the primary entry point to this class
 *  - {@link UpToOneMessageFrameProcessor}, supplied to the decoder when the handler is reactivated after being
 *    put in waiting mode due to lack of acquirable reserve memory capacity permits
 *
 * Return value of {@link FrameProcessor#process(Frame)} determines whether the decoder should keep processing
 * frames (if {@code true} is returned) or stop until explicitly reactivated (if {@code false} is). To reactivate
 * the decoder (once notified of available resource permits), {@link FrameDecoder#reactivate()} is invoked.
 *
 * # Frames
 *
 * {@link InboundMessageHandler} operates on frames of messages, and there are several kinds of them:
 *  1. {@link IntactFrame} that are contained. As names suggest, these contain one or multiple fully contained
 *     messages believed to be uncorrupted. Guaranteed to not contain an part of an incomplete message.
 *     See {@link #processFrameOfContainedMessages(ShareableBytes, Limit, Limit)}.
 *  2. {@link IntactFrame} that are NOT contained. These are uncorrupted parts of a large message split over multiple
 *     parts due to their size. Can represent first or subsequent frame of a large message.
 *     See {@link #processFirstFrameOfLargeMessage(IntactFrame, Limit, Limit)} and
 *     {@link #processSubsequentFrameOfLargeMessage(Frame)}.
 *  3. {@link CorruptFrame} with corrupt header. These are unrecoverable, and force a connection to be dropped.
 *  4. {@link CorruptFrame} with a valid header, but corrupt payload. These can be either contained or uncontained.
 *     - contained frames with corrupt payload can be gracefully dropped without dropping the connection
 *     - uncontained frames with corrupt payload can be gracefully dropped unless they represent the first
 *       frame of a new large message, as in that case we don't know how many bytes to skip
 *     See {@link #processCorruptFrame(CorruptFrame)}.
 *
 *  Fundamental frame invariants:
 *  1. A contained frame can only have fully-encapsulated messages - 1 to n, that don't cross frame boundaries
 *  2. An uncontained frame can hold a part of one message only. It can NOT, say, contain end of one large message
 *     and a beginning of another one. All the bytes in an uncontained frame always belong to a single message.
 *
 * # Small vs large messages
 *
 * A single handler is equipped to process both small and large messages, potentially interleaved, but the logic
 * differs depending on size. Small messages are deserialized in place, and then handed off to an appropriate
 * thread pool for processing. Large messages accumulate frames until completion of a message, then hand off
 * the untouched frames to the correct thread pool for the verb to be deserialized there and immediately processed.
 *
 * See {@link LargeMessage} for details of the large-message accumulating state-machine, and {@link ProcessMessage}
 * and its inheritors for the differences in execution.
 *
 * # Flow control (backpressure)
 *
 * To prevent nodes from overwhelming and bringing each other to the knees with more inbound messages that
 * can be processed in a timely manner, {@link InboundMessageHandler} implements a strict flow control policy.
 *
 * Before we attempt to process a message fully, we first infer its size from the stream. Then we attempt to
 * acquire memory permits for a message of that size. If we succeed, then we move on actually process the message.
 * If we fail, the frame decoder deactivates until sufficient permits are released for the message to be processed
 * and the handler is activated again. Permits are released back once the message has been fully processed -
 * after the verb handler has been invoked - on the {@link Stage} for the {@link Verb} of the message.
 *
 * Every connection has an exclusive number of permits allocated to it (by default 4MiB). In addition to it,
 * there is a per-endpoint reserve capacity and a global reserve capacity {@link Limit}, shared between all
 * connections from the same host and all connections, respectively. So long as long as the handler stays within
 * its exclusive limit, it doesn't need to tap into reserve capacity.
 *
 * If tapping into reserve capacity is necessary, but the handler fails to acquire capacity from either
 * endpoint of global reserve (and it needs to acquire from both), the handler and its frame decoder become
 * inactive and register with a {@link WaitQueue} of the appropriate type, depending on which of the reserves
 * couldn't be tapped into. Once enough messages have finished processing and had their permits released back
 * to the reserves, {@link WaitQueue} will reactivate the sleeping handlers and they'll resume processing frames.
 *
 * The reason we 'split' reserve capacity into two limits - endpoing and global - is to guarantee liveness, and
 * prevent single endpoint's connections from taking over the whole reserve, starving other connections.
 *
 * One permit per byte of serialized message gets acquired. When inflated on-heap, each message will occupy more
 * than that, necessarily, but despite wide variance, it's a good enough proxy that correlates with on-heap footprint.
 */
public class InboundMessageHandler extends ChannelInboundHandlerAdapter implements FrameProcessor
{
    private static final Logger logger = LoggerFactory.getLogger(InboundMessageHandler.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.SECONDS);

    private static final Message.Serializer serializer = Message.serializer;

    private final FrameDecoder decoder;

    private final ConnectionType type;
    private final Channel channel;
    private final InetAddressAndPort self;
    private final InetAddressAndPort peer;
    private final int version;

    private final int largeThreshold;
    private LargeMessage largeMessage;

    private final long queueCapacity;
    volatile long queueSize = 0L;
    private static final AtomicLongFieldUpdater<InboundMessageHandler> queueSizeUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandler.class, "queueSize");

    private final Limit endpointReserveCapacity;
    private final WaitQueue endpointWaitQueue;

    private final Limit globalReserveCapacity;
    private final WaitQueue globalWaitQueue;

    private final OnHandlerClosed onClosed;
    private final InboundMessageCallbacks callbacks;
    private final Consumer<Message<?>> consumer;

    // wait queue handle, non-null if we overrun endpoint or global capacity and request to be resumed once it's released
    private WaitQueue.Ticket ticket = null;

    long corruptFramesRecovered, corruptFramesUnrecovered;
    long receivedCount, receivedBytes;
    long throttledCount, throttledNanos;

    private boolean isClosed;

    InboundMessageHandler(FrameDecoder decoder,

                          ConnectionType type,
                          Channel channel,
                          InetAddressAndPort self,
                          InetAddressAndPort peer,
                          int version,
                          int largeThreshold,

                          long queueCapacity,
                          Limit endpointReserveCapacity,
                          Limit globalReserveCapacity,
                          WaitQueue endpointWaitQueue,
                          WaitQueue globalWaitQueue,

                          OnHandlerClosed onClosed,
                          InboundMessageCallbacks callbacks,
                          Consumer<Message<?>> consumer)
    {
        this.decoder = decoder;

        this.type = type;
        this.channel = channel;
        this.self = self;
        this.peer = peer;
        this.version = version;
        this.largeThreshold = largeThreshold;

        this.queueCapacity = queueCapacity;
        this.endpointReserveCapacity = endpointReserveCapacity;
        this.endpointWaitQueue = endpointWaitQueue;
        this.globalReserveCapacity = globalReserveCapacity;
        this.globalWaitQueue = globalWaitQueue;

        this.onClosed = onClosed;
        this.callbacks = callbacks;
        this.consumer = consumer;
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
    public boolean process(Frame frame) throws IOException
    {
        if (frame instanceof IntactFrame)
            return processIntactFrame((IntactFrame) frame, endpointReserveCapacity, globalReserveCapacity);

        processCorruptFrame((CorruptFrame) frame);
        return true;
    }

    private boolean processIntactFrame(IntactFrame frame, Limit endpointReserve, Limit globalReserve) throws IOException
    {
        if (frame.isSelfContained)
            return processFrameOfContainedMessages(frame.contents, endpointReserve, globalReserve);
        else if (null == largeMessage)
            return processFirstFrameOfLargeMessage(frame, endpointReserve, globalReserve);
        else
            return processSubsequentFrameOfLargeMessage(frame);
    }

    /*
     * Handle contained messages (not crossing boundaries of the frame) - both small and large, for the inbound
     * definition of large (breaching the size threshold for what we are willing to process on event-loop vs.
     * off event-loop).
     */
    private boolean processFrameOfContainedMessages(ShareableBytes bytes, Limit endpointReserve, Limit globalReserve) throws IOException
    {
        while (bytes.hasRemaining())
            if (!processOneContainedMessage(bytes, endpointReserve, globalReserve))
                return false;
        return true;
    }

    private boolean processOneContainedMessage(ShareableBytes bytes, Limit endpointReserve, Limit globalReserve) throws IOException
    {
        ByteBuffer buf = bytes.get();

        long currentTimeNanos = approxTime.now();
        Header header = serializer.extractHeader(buf, peer, currentTimeNanos, version);
        long timeElapsed = currentTimeNanos - header.createdAtNanos;
        int size = serializer.inferMessageSize(buf, buf.position(), buf.limit(), version);

        if (approxTime.isAfter(currentTimeNanos, header.expiresAtNanos))
        {
            callbacks.onHeaderArrived(size, header, timeElapsed, NANOSECONDS);
            callbacks.onArrivedExpired(size, header, false, timeElapsed, NANOSECONDS);
            receivedCount++;
            receivedBytes += size;
            bytes.skipBytes(size);
            return true;
        }

        if (!acquireCapacity(endpointReserve, globalReserve, size, currentTimeNanos, header.expiresAtNanos))
            return false;

        callbacks.onHeaderArrived(size, header, timeElapsed, NANOSECONDS);
        callbacks.onArrived(size, header, timeElapsed, NANOSECONDS);
        receivedCount++;
        receivedBytes += size;

        if (size <= largeThreshold)
            processSmallMessage(bytes, size, header);
        else
            processLargeMessage(bytes, size, header);

        return true;
    }

    private void processSmallMessage(ShareableBytes bytes, int size, Header header)
    {
        ByteBuffer buf = bytes.get();
        final int begin = buf.position();
        final int end = buf.limit();
        buf.limit(begin + size); // cap to expected message size

        Message<?> message = null;
        try (DataInputBuffer in = new DataInputBuffer(buf, false))
        {
            Message<?> m = serializer.deserialize(in, header, version);
            if (in.available() > 0) // bytes remaining after deser: deserializer is busted
                throw new InvalidSerializedSizeException(header.verb, size, size - in.available());
            message = m;
        }
        catch (IncompatibleSchemaException e)
        {
            callbacks.onFailedDeserialize(size, header, e);
            noSpamLogger.info("{} incompatible schema encountered while deserializing a message", id(), e);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t, false);
            callbacks.onFailedDeserialize(size, header, t);
            logger.error("{} unexpected exception caught while deserializing a message", id(), t);
        }
        finally
        {
            if (null == message)
                releaseCapacity(size);

            // no matter what, set position to the beginning of the next message and restore limit, so that
            // we can always keep on decoding the frame even on failure to deserialize previous message
            buf.position(begin + size);
            buf.limit(end);
        }

        if (null != message)
            dispatch(new ProcessSmallMessage(message, size));
    }

    // for various reasons, it's possible for a large message to be contained in a single frame
    private void processLargeMessage(ShareableBytes bytes, int size, Header header)
    {
        new LargeMessage(size, header, bytes.sliceAndConsume(size).share()).schedule();
    }

    /*
     * Handling of multi-frame large messages
     */

    private boolean processFirstFrameOfLargeMessage(IntactFrame frame, Limit endpointReserve, Limit globalReserve) throws IOException
    {
        ShareableBytes bytes = frame.contents;
        ByteBuffer buf = bytes.get();

        long currentTimeNanos = approxTime.now();
        Header header = serializer.extractHeader(buf, peer, currentTimeNanos, version);
        int size = serializer.inferMessageSize(buf, buf.position(), buf.limit(), version);

        boolean expired = approxTime.isAfter(currentTimeNanos, header.expiresAtNanos);
        if (!expired && !acquireCapacity(endpointReserve, globalReserve, size, currentTimeNanos, header.expiresAtNanos))
            return false;

        callbacks.onHeaderArrived(size, header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);
        receivedBytes += buf.remaining();
        largeMessage = new LargeMessage(size, header, expired);
        largeMessage.supply(frame);
        return true;
    }

    private boolean processSubsequentFrameOfLargeMessage(Frame frame)
    {
        receivedBytes += frame.frameSize;
        if (largeMessage.supply(frame))
        {
            receivedCount++;
            largeMessage = null;
        }
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
    private void processCorruptFrame(CorruptFrame frame) throws InvalidCrc
    {
        if (!frame.isRecoverable())
        {
            corruptFramesUnrecovered++;
            throw new InvalidCrc(frame.readCRC, frame.computedCRC);
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
            throw new InvalidCrc(frame.readCRC, frame.computedCRC);
        }
        else // subsequent frame of a large message
        {
            processSubsequentFrameOfLargeMessage(frame);
            corruptFramesRecovered++;
            noSpamLogger.warn("{} invalid, recoverable CRC mismatch detected while reading a large message", id());
        }
    }

    private void onEndpointReserveCapacityRegained(Limit endpointReserve, long elapsedNanos)
    {
        onReserveCapacityRegained(endpointReserve, globalReserveCapacity, elapsedNanos);
    }

    private void onGlobalReserveCapacityRegained(Limit globalReserve, long elapsedNanos)
    {
        onReserveCapacityRegained(endpointReserveCapacity, globalReserve, elapsedNanos);
    }

    private void onReserveCapacityRegained(Limit endpointReserve, Limit globalReserve, long elapsedNanos)
    {
        if (isClosed)
            return;

        assert channel.eventLoop().inEventLoop();

        ticket = null;
        throttledNanos += elapsedNanos;

        try
        {
            /*
             * Process up to one message using supplied overriden reserves - one of them pre-allocated,
             * and guaranteed to be enough for one message - then, if no obstacles enountered, reactivate
             * the frame decoder using normal reserve capacities.
             */
            if (processUpToOneMessage(endpointReserve, globalReserve))
                decoder.reactivate();
        }
        catch (Throwable t)
        {
            exceptionCaught(t);
        }
    }

    // return true if the handler should be reactivated - if no new hurdles were encountered,
    // like running out of the other kind of reserve capacity
    private boolean processUpToOneMessage(Limit endpointReserve, Limit globalReserve) throws IOException
    {
        UpToOneMessageFrameProcessor processor = new UpToOneMessageFrameProcessor(endpointReserve, globalReserve);
        decoder.processBacklog(processor);
        return processor.isActive;
    }

    /*
     * Process at most one message. Won't always be an entire one (if the message in the head of line
     * is a large one, and there aren't sufficient frames to decode it entirely), but will never be more than one.
     */
    private class UpToOneMessageFrameProcessor implements FrameProcessor
    {
        private final Limit endpointReserve;
        private final Limit globalReserve;

        boolean isActive = true;
        boolean firstFrame = true;

        private UpToOneMessageFrameProcessor(Limit endpointReserve, Limit globalReserve)
        {
            this.endpointReserve = endpointReserve;
            this.globalReserve = globalReserve;
        }

        @Override
        public boolean process(Frame frame) throws IOException
        {
            if (firstFrame)
            {
                if (!(frame instanceof IntactFrame))
                    throw new IllegalStateException("First backlog frame must be intact");
                firstFrame = false;
                return processFirstFrame((IntactFrame) frame);
            }

            return processSubsequentFrame(frame);
        }

        private boolean processFirstFrame(IntactFrame frame) throws IOException
        {
            if (frame.isSelfContained)
            {
                isActive = processOneContainedMessage(frame.contents, endpointReserve, globalReserve);
                return false; // stop after one message
            }
            else
            {
                isActive = processFirstFrameOfLargeMessage(frame, endpointReserve, globalReserve);
                return isActive; // continue unless fallen behind coprocessor or ran out of reserve capacity again
            }
        }

        private boolean processSubsequentFrame(Frame frame) throws IOException
        {
            if (frame instanceof IntactFrame)
                processSubsequentFrameOfLargeMessage(frame);
            else
                processCorruptFrame((CorruptFrame) frame);

            return largeMessage != null; // continue until done with the large message
        }
    }

    /**
     * Try to acquire permits for the inbound message. In case of failure, register with the right wait queue to be
     * reactivated once permit capacity is regained.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean acquireCapacity(Limit endpointReserve, Limit globalReserve, int bytes, long currentTimeNanos, long expiresAtNanos)
    {
        ResourceLimits.Outcome outcome = acquireCapacity(endpointReserve, globalReserve, bytes);

        if (outcome == ResourceLimits.Outcome.INSUFFICIENT_ENDPOINT)
            ticket = endpointWaitQueue.register(this, bytes, currentTimeNanos, expiresAtNanos);
        else if (outcome == ResourceLimits.Outcome.INSUFFICIENT_GLOBAL)
            ticket = globalWaitQueue.register(this, bytes, currentTimeNanos, expiresAtNanos);

        if (outcome != ResourceLimits.Outcome.SUCCESS)
            throttledCount++;

        return outcome == ResourceLimits.Outcome.SUCCESS;
    }

    private ResourceLimits.Outcome acquireCapacity(Limit endpointReserve, Limit globalReserve, int bytes)
    {
        long currentQueueSize = queueSize;

        /*
         * acquireCapacity() is only ever called on the event loop, and as such queueSize is only ever increased
         * on the event loop. If there is enough capacity, we can safely addAndGet() and immediately return.
         */
        if (currentQueueSize + bytes <= queueCapacity)
        {
            queueSizeUpdater.addAndGet(this, bytes);
            return ResourceLimits.Outcome.SUCCESS;
        }

        // we know we don't have enough local queue capacity for the entire message, so we need to borrow some from reserve capacity
        long allocatedExcess = min(currentQueueSize + bytes - queueCapacity, bytes);

        if (!globalReserve.tryAllocate(allocatedExcess))
            return ResourceLimits.Outcome.INSUFFICIENT_GLOBAL;

        if (!endpointReserve.tryAllocate(allocatedExcess))
        {
            globalReserve.release(allocatedExcess);
            globalWaitQueue.signal();
            return ResourceLimits.Outcome.INSUFFICIENT_GLOBAL;
        }

        long newQueueSize = queueSizeUpdater.addAndGet(this, bytes);
        long actualExcess = max(0, min(newQueueSize - queueCapacity, bytes));

        /*
         * It's possible that some permits were released at some point after we loaded current queueSize,
         * and we can satisfy more of the permits using our exclusive per-connection capacity, needing
         * less than previously estimated from the reserves. If that's the case, release the now unneeded
         * permit excess back to endpoint/global reserves.
         */
        if (actualExcess != allocatedExcess) // actualExcess < allocatedExcess
        {
            long excess = allocatedExcess - actualExcess;

            endpointReserve.release(excess);
            globalReserve.release(excess);

            endpointWaitQueue.signal();
            globalWaitQueue.signal();
        }

        return ResourceLimits.Outcome.SUCCESS;
    }

    private void releaseCapacity(int bytes)
    {
        long oldQueueSize = queueSizeUpdater.getAndAdd(this, -bytes);
        if (oldQueueSize > queueCapacity)
        {
            long excess = min(oldQueueSize - queueCapacity, bytes);

            endpointReserveCapacity.release(excess);
            globalReserveCapacity.release(excess);

            endpointWaitQueue.signal();
            globalWaitQueue.signal();
        }
    }

    /**
     * Invoked to release capacity for a message that has been fully, successfully processed.
     *
     * Normally no different from invoking {@link #releaseCapacity(int)}, but is necessary for the verifier
     * to be able to delay capacity release for backpressure testing.
     */
    @VisibleForTesting
    protected void releaseProcessedCapacity(int size, Header header)
    {
        releaseCapacity(size);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        try
        {
            exceptionCaught(cause);
        }
        catch (Throwable t)
        {
            logger.error("Unexpected exception in {}.exceptionCaught", this.getClass().getSimpleName(), t);
        }
    }

    private void exceptionCaught(Throwable cause)
    {
        decoder.discard();

        JVMStabilityInspector.inspectThrowable(cause, false);

        if (cause instanceof Message.InvalidLegacyProtocolMagic)
            logger.error("{} invalid, unrecoverable CRC mismatch detected while reading messages - closing the connection", id());
        else
            logger.error("{} unexpected exception caught while processing inbound messages; terminating connection", id(), cause);

        channel.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        isClosed = true;

        if (null != largeMessage)
            largeMessage.abort();

        if (null != ticket)
            ticket.invalidate();

        onClosed.call(this);
    }

    private EventLoop eventLoop()
    {
        return channel.eventLoop();
    }

    String id(boolean includeReal)
    {
        if (!includeReal)
            return id();

        return SocketFactory.channelId(peer, (InetSocketAddress) channel.remoteAddress(),
                                       self, (InetSocketAddress) channel.localAddress(),
                                       type, channel.id().asShortText());
    }

    String id()
    {
        return SocketFactory.channelId(peer, self, type, channel.id().asShortText());
    }

    /*
     * A large-message frame-accumulating state machine.
     *
     * Collects intact frames until it's has all the bytes necessary to deserialize the large message,
     * at which point it schedules a task on the appropriate {@link Stage},
     * a task that deserializes the message and immediately invokes the verb handler.
     *
     * Also handles corrupt frames and potential expiry of the large message during accumulation:
     * if it's taking the frames too long to arrive, there is no point in holding on to the
     * accumulated frames, or in gathering more - so we release the ones we already have, and
     * skip any remaining ones, alongside with returning memory permits early.
     */
    private class LargeMessage
    {
        private final int size;
        private final Header header;

        private final List<ShareableBytes> buffers = new ArrayList<>();
        private int received;

        private boolean isExpired;
        private boolean isCorrupt;

        private LargeMessage(int size, Header header, boolean isExpired)
        {
            this.size = size;
            this.header = header;
            this.isExpired = isExpired;
        }

        private LargeMessage(int size, Header header, ShareableBytes bytes)
        {
            this(size, header, false);
            buffers.add(bytes);
        }

        private void schedule()
        {
            dispatch(new ProcessLargeMessage(this));
        }

        /**
         * Return true if this was the last frame of the large message.
         */
        private boolean supply(Frame frame)
        {
            if (frame instanceof IntactFrame)
                onIntactFrame((IntactFrame) frame);
            else
                onCorruptFrame();

            received += frame.frameSize;
            if (size == received)
                onComplete();
            return size == received;
        }

        private void onIntactFrame(IntactFrame frame)
        {
            boolean expires = approxTime.isAfter(header.expiresAtNanos);
            if (!isExpired && !isCorrupt)
            {
                if (!expires)
                {
                    buffers.add(frame.contents.sliceAndConsume(frame.frameSize).share());
                    return;
                }
                releaseBuffersAndCapacity(); // release resources once we transition from normal state to expired
            }
            frame.consume();
            isExpired |= expires;
        }

        private void onCorruptFrame()
        {
            if (!isExpired && !isCorrupt)
                releaseBuffersAndCapacity(); // release resources once we transition from normal state to corrupt
            isCorrupt = true;
            isExpired |= approxTime.isAfter(header.expiresAtNanos);
        }

        private void onComplete()
        {
            long timeElapsed = approxTime.now() - header.createdAtNanos;

            if (!isExpired && !isCorrupt)
            {
                callbacks.onArrived(size, header, timeElapsed, NANOSECONDS);
                schedule();
            }
            else if (isExpired)
            {
                callbacks.onArrivedExpired(size, header, isCorrupt, timeElapsed, NANOSECONDS);
            }
            else
            {
                callbacks.onArrivedCorrupt(size, header, timeElapsed, NANOSECONDS);
            }
        }

        private void abort()
        {
            if (!isExpired && !isCorrupt)
                releaseBuffersAndCapacity(); // release resources if in normal state when abort() is invoked
            callbacks.onClosedBeforeArrival(size, header, received, isCorrupt, isExpired);
        }

        private void releaseBuffers()
        {
            buffers.forEach(ShareableBytes::release); buffers.clear();
        }

        private void releaseBuffersAndCapacity()
        {
            releaseBuffers(); releaseCapacity(size);
        }

        private Message deserialize()
        {
            try (ChunkedInputPlus input = ChunkedInputPlus.of(buffers))
            {
                Message<?> m = serializer.deserialize(input, header, version);
                int remainder = input.remainder();
                if (remainder > 0)
                    throw new InvalidSerializedSizeException(header.verb, size, size - remainder);
                return m;
            }
            catch (IncompatibleSchemaException e)
            {
                callbacks.onFailedDeserialize(size, header, e);
                noSpamLogger.info("{} incompatible schema encountered while deserializing a message", id(), e);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t, false);
                callbacks.onFailedDeserialize(size, header, t);
                logger.error("{} unexpected exception caught while deserializing a message", id(), t);
            }
            finally
            {
                buffers.clear(); // closing the input will have ensured that the buffers were released no matter what
            }

            return null;
        }
    }

    /**
     * Submit a {@link ProcessMessage} task to the appropriate {@link Stage} for the {@link Verb}.
     */
    private void dispatch(ProcessMessage task)
    {
        Header header = task.header();

        TraceState state = Tracing.instance.initializeFromMessage(header);
        if (state != null) state.trace("{} message received from {}", header.verb, header.from);

        callbacks.onDispatched(task.size(), header);
        StageManager.getStage(header.verb.stage).execute(task, ExecutorLocals.create(state));
    }

    private abstract class ProcessMessage implements Runnable
    {
        /**
         * Actually handle the message. Runs on the appropriate {@link Stage} for the {@link Verb}.
         *
         * Small messages will come pre-deserialized. Large messages will be deserialized on the stage,
         * just in time, and only then processed.
         */
        public void run()
        {
            Header header = header();
            long currentTimeNanos = approxTime.now();
            boolean expired = approxTime.isAfter(currentTimeNanos, header.expiresAtNanos);

            boolean processed = false;
            try
            {
                callbacks.onExecuting(size(), header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);

                if (expired)
                {
                    callbacks.onExpired(size(), header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);
                    return;
                }

                Message message = provideMessage();
                if (null != message)
                {
                    consumer.accept(message);
                    processed = true;
                    callbacks.onProcessed(size(), header);
                }
            }
            finally
            {
                if (processed)
                    releaseProcessedCapacity(size(), header);
                else
                    releaseCapacity(size());

                releaseResources();

                callbacks.onExecuted(size(), header, approxTime.now() - currentTimeNanos, NANOSECONDS);
            }
        }

        abstract int size();
        abstract Header header();
        abstract Message provideMessage();
        void releaseResources() {}
    }

    private class ProcessSmallMessage extends ProcessMessage
    {
        private final int size;
        private final Message message;

        ProcessSmallMessage(Message message, int size)
        {
            this.size = size;
            this.message = message;
        }

        int size()
        {
            return size;
        }

        Header header()
        {
            return message.header;
        }

        Message provideMessage()
        {
            return message;
        }
    }

    private class ProcessLargeMessage extends ProcessMessage
    {
        private final LargeMessage message;

        ProcessLargeMessage(LargeMessage message)
        {
            this.message = message;
        }

        int size()
        {
            return message.size;
        }

        Header header()
        {
            return message.header;
        }

        Message provideMessage()
        {
            return message.deserialize();
        }

        @Override
        void releaseResources()
        {
            message.releaseBuffers(); // releases buffers if they haven't been yet (by deserialize() call)
        }
    }

    /**
     * A special-purpose wait queue to park inbound message handlers that failed to allocate
     * reserve capacity for a message in. Upon such failure a handler registers itself with
     * a {@link WaitQueue} of the appropriate kind (either ENDPOINT or GLOBAL - if failed
     * to allocate endpoint or global reserve capacity, respectively), stops processing any
     * accumulated frames or receiving new ones, and waits - until reactivated.
     *
     * Every time permits are returned to an endpoint or global {@link Limit}, the respective
     * queue gets signalled, and if there are any handlers registered in it, we will attempt
     * to reactivate as many waiting handlers as current available reserve capacity allows
     * us to - immediately, on the {@link #signal()}-calling thread. At most one such attempt
     * will be in progress at any given time.
     *
     * Handlers that can be reactivated will be grouped by their {@link EventLoop} and a single
     * {@link ReactivateHandlers} task will be scheduled per event loop, on the corresponding
     * event loops.
     *
     * When run, the {@link ReactivateHandlers} task will ask each handler in its group to first
     * process one message - using preallocated reserve capacity - and if no obstacles were met -
     * reactivate the handlers, this time using their regular reserves.
     *
     * See {@link WaitQueue#schedule()}, {@link ReactivateHandlers#run()}, {@link Ticket#reactivateHandler(Limit)}.
     */
    public static final class WaitQueue
    {
        enum Kind { ENDPOINT, GLOBAL }

        private static final int NOT_RUNNING = 0;
        @SuppressWarnings("unused")
        private static final int RUNNING     = 1;
        private static final int RUN_AGAIN   = 2;

        private volatile int scheduled;
        private static final AtomicIntegerFieldUpdater<WaitQueue> scheduledUpdater =
            AtomicIntegerFieldUpdater.newUpdater(WaitQueue.class, "scheduled");

        private final Kind kind;
        private final Limit reserveCapacity;

        private final ManyToOneConcurrentLinkedQueue<Ticket> queue = new ManyToOneConcurrentLinkedQueue<>();

        private WaitQueue(Kind kind, Limit reserveCapacity)
        {
            this.kind = kind;
            this.reserveCapacity = reserveCapacity;
        }

        public static WaitQueue endpoint(Limit endpointReserveCapacity)
        {
            return new WaitQueue(Kind.ENDPOINT, endpointReserveCapacity);
        }

        public static WaitQueue global(Limit globalReserveCapacity)
        {
            return new WaitQueue(Kind.GLOBAL, globalReserveCapacity);
        }

        private Ticket register(InboundMessageHandler handler, int bytesRequested, long registeredAtNanos, long expiresAtNanos)
        {
            Ticket ticket = new Ticket(this, handler, bytesRequested, registeredAtNanos, expiresAtNanos);
            Ticket previous = queue.relaxedPeekLastAndOffer(ticket);
            if (null == previous || !previous.isWaiting())
                signal(); // only signal the queue if this handler is first to register
            return ticket;
        }

        private void signal()
        {
            if (queue.relaxedIsEmpty())
                return; // we can return early if no handlers have registered with the wait queue

            if (NOT_RUNNING == scheduledUpdater.getAndUpdate(this, i -> min(RUN_AGAIN, i + 1)))
            {
                do
                {
                    schedule();
                }
                while (RUN_AGAIN == scheduledUpdater.getAndDecrement(this));
            }
        }

        private void schedule()
        {
            Map<EventLoop, ReactivateHandlers> tasks = null;

            long currentTimeNanos = approxTime.now();

            Ticket t;
            while ((t = queue.peek()) != null)
            {
                if (!t.call()) // invalidated
                {
                    queue.remove();
                    continue;
                }

                boolean isLive = t.isLive(currentTimeNanos);
                if (isLive && !reserveCapacity.tryAllocate(t.bytesRequested))
                {
                    if (!t.reset()) // the ticket was invalidated after being called but before now
                    {
                        queue.remove();
                        continue;
                    }
                    break; // TODO: traverse the entire queue to unblock handlers that have expired or invalidated tickets
                }

                if (null == tasks)
                    tasks = new IdentityHashMap<>();

                queue.remove();
                tasks.computeIfAbsent(t.handler.eventLoop(), e -> new ReactivateHandlers()).add(t, isLive);
            }

            if (null != tasks)
                tasks.forEach(EventLoop::execute);
        }

        private class ReactivateHandlers implements Runnable
        {
            List<Ticket> tickets = new ArrayList<>();
            long capacity = 0L;

            private void add(Ticket ticket, boolean isLive)
            {
                tickets.add(ticket);
                if (isLive) capacity += ticket.bytesRequested;
            }

            public void run()
            {
                Limit limit = new ResourceLimits.Basic(capacity);
                try
                {
                    for (Ticket ticket : tickets)
                        ticket.reactivateHandler(limit);
                }
                finally
                {
                    /*
                     * Free up any unused capacity, if any. Will be non-zero if one or more handlers were closed
                     * when we attempted to run their callback, or used more of their other reserve; or if the first
                     * message in the unprocessed stream has expired in the narrow time window.
                     */
                    long remaining = limit.remaining();
                    if (remaining > 0)
                    {
                        reserveCapacity.release(remaining);
                        signal();
                    }
                }
            }
        }

        private static final class Ticket
        {
            private static final int WAITING     = 0;
            private static final int CALLED      = 1;
            private static final int INVALIDATED = 2; // invalidated by a handler that got closed

            private volatile int state;
            private static final AtomicIntegerFieldUpdater<Ticket> stateUpdater =
                AtomicIntegerFieldUpdater.newUpdater(Ticket.class, "state");

            private final WaitQueue waitQueue;
            private final InboundMessageHandler handler;
            private final int bytesRequested;
            private final long reigsteredAtNanos;
            private final long expiresAtNanos;

            private Ticket(WaitQueue waitQueue, InboundMessageHandler handler, int bytesRequested, long registeredAtNanos, long expiresAtNanos)
            {
                this.waitQueue = waitQueue;
                this.handler = handler;
                this.bytesRequested = bytesRequested;
                this.reigsteredAtNanos = registeredAtNanos;
                this.expiresAtNanos = expiresAtNanos;
            }

            private void reactivateHandler(Limit capacity)
            {
                long elapsedNanos = approxTime.now() - reigsteredAtNanos;
                try
                {
                    if (waitQueue.kind == Kind.ENDPOINT)
                        handler.onEndpointReserveCapacityRegained(capacity, elapsedNanos);
                    else
                        handler.onGlobalReserveCapacityRegained(capacity, elapsedNanos);
                }
                catch (Throwable t)
                {
                    logger.error("{} exception caught while reactivating a handler", handler.id(), t);
                }
            }

            private boolean isWaiting()
            {
                return state == WAITING;
            }

            private boolean isLive(long currentTimeNanos)
            {
                return !approxTime.isAfter(currentTimeNanos, expiresAtNanos);
            }

            private void invalidate()
            {
                state = INVALIDATED;
                waitQueue.signal();
            }

            private boolean call()
            {
                return stateUpdater.compareAndSet(this, WAITING, CALLED);
            }

            private boolean reset()
            {
                return stateUpdater.compareAndSet(this, CALLED, WAITING);
            }
        }
    }

    public interface OnHandlerClosed
    {
        void call(InboundMessageHandler handler);
    }
}
