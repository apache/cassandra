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
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.net.FrameDecoder.CorruptFrame;
import org.apache.cassandra.net.FrameDecoder.Frame;
import org.apache.cassandra.net.FrameDecoder.FrameProcessor;
import org.apache.cassandra.net.FrameDecoder.IntactFrame;
import org.apache.cassandra.net.Message.Header;
import org.apache.cassandra.net.ResourceLimits.Limit;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.cassandra.net.Crc.InvalidCrc;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

/**
 * Core logic for handling inbound message deserialization and execution (in tandem with {@link FrameDecoder}).
 *
 * Handles small and large messages, corruption, flow control, dispatch of message processing to a suitable
 * consumer.
 *
 * # Interaction with {@link FrameDecoder}
 *
 * An {@link AbstractMessageHandler} implementation sits on top of a {@link FrameDecoder} in the Netty pipeline,
 * and is tightly coupled with it.
 *
 * {@link FrameDecoder} decodes inbound frames and relies on a supplied {@link FrameProcessor} to act on them.
 * {@link AbstractMessageHandler} provides two implementations of that interface:
 *  - {@link #process(Frame)} is the default, primary processor, and is expected to be implemented by subclasses
 *  - {@link UpToOneMessageFrameProcessor}, supplied to the decoder when the handler is reactivated after being
 *    put in waiting mode due to lack of acquirable reserve memory capacity permits
 *
 * Return value of {@link FrameProcessor#process(Frame)} determines whether the decoder should keep processing
 * frames (if {@code true} is returned) or stop until explicitly reactivated (if {@code false} is). To reactivate
 * the decoder (once notified of available resource permits), {@link FrameDecoder#reactivate()} is invoked.
 *
 * # Frames
 *
 * {@link AbstractMessageHandler} operates on frames of messages, and there are several kinds of them:
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
 * See {@link LargeMessage} and subclasses for concrete {@link AbstractMessageHandler} implementations for details
 * of the large-message accumulating state-machine, and {@link InboundMessageHandler.ProcessMessage} and its inheritors 
 * for the differences in execution.
 *
 * # Flow control (backpressure)
 *
 * To prevent message producers from overwhelming and bringing nodes down with more inbound messages that
 * can be processed in a timely manner, {@link AbstractMessageHandler} provides support for implementations to
 * provide their own flow control policy.
 *
 * Before we attempt to process a message fully, we first infer its size from the stream. This inference is
 * delegated to implementations as the encoding of the message size is protocol specific. Having assertained
 * the size of the incoming message, we then attempt to acquire the corresponding number of memory permits.
 * If we succeed, then we move on actually process the message. If we fail, the frame decoder deactivates
 * until sufficient permits are released for the message to be processed and the handler is activated again.
 * Permits are released back once the message has been fully processed - the definition of which is again
 * delegated to the concrete implementations.
 *
 * Every connection has an exclusive number of permits allocated to it. In addition to it, there is a per-endpoint
 * reserve capacity and a global reserve capacity {@link Limit}, shared between all connections from the same host
 * and all connections, respectively. So long as long as the handler stays within its exclusive limit, it doesn't
 * need to tap into reserve capacity.
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
public abstract class AbstractMessageHandler extends ChannelInboundHandlerAdapter implements FrameProcessor
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractMessageHandler.class);
    
    protected final FrameDecoder decoder;

    protected final Channel channel;

    protected final int largeThreshold;
    protected LargeMessage<?> largeMessage;

    protected final long queueCapacity;
    volatile long queueSize = 0L;
    private static final AtomicLongFieldUpdater<AbstractMessageHandler> queueSizeUpdater =
        AtomicLongFieldUpdater.newUpdater(AbstractMessageHandler.class, "queueSize");

    protected final Limit endpointReserveCapacity;
    protected final WaitQueue endpointWaitQueue;

    protected final Limit globalReserveCapacity;
    protected final WaitQueue globalWaitQueue;

    protected final OnHandlerClosed onClosed;

    // wait queue handle, non-null if we overrun endpoint or global capacity and request to be resumed once it's released
    private WaitQueue.Ticket ticket = null;

    protected long corruptFramesRecovered, corruptFramesUnrecovered;
    protected long receivedCount, receivedBytes;
    protected long throttledCount, throttledNanos;

    private boolean isClosed;

    public AbstractMessageHandler(FrameDecoder decoder,

                                  Channel channel,
                                  int largeThreshold,

                                  long queueCapacity,
                                  Limit endpointReserveCapacity,
                                  Limit globalReserveCapacity,
                                  WaitQueue endpointWaitQueue,
                                  WaitQueue globalWaitQueue,

                                  OnHandlerClosed onClosed)
    {
        this.decoder = decoder;

        this.channel = channel;
        this.largeThreshold = largeThreshold;

        this.queueCapacity = queueCapacity;
        this.endpointReserveCapacity = endpointReserveCapacity;
        this.endpointWaitQueue = endpointWaitQueue;
        this.globalReserveCapacity = globalReserveCapacity;
        this.globalWaitQueue = globalWaitQueue;

        this.onClosed = onClosed;
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

    protected abstract boolean processOneContainedMessage(ShareableBytes bytes, Limit endpointReserve, Limit globalReserve) throws IOException;


    /*
     * Handling of multi-frame large messages
     */

    protected abstract boolean processFirstFrameOfLargeMessage(IntactFrame frame, Limit endpointReserve, Limit globalReserve) throws IOException;

    protected boolean processSubsequentFrameOfLargeMessage(Frame frame)
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
    protected abstract void processCorruptFrame(CorruptFrame frame) throws InvalidCrc;

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
             * Process up to one message using supplied overridden reserves - one of them pre-allocated,
             * and guaranteed to be enough for one message - then, if no obstacles encountered, reactivate
             * the frame decoder using normal reserve capacities.
             */
            if (processUpToOneMessage(endpointReserve, globalReserve))
            {
                decoder.reactivate();

                if (decoder.isActive())
                    ClientMetrics.instance.unpauseConnection();
            }
        }
        catch (Throwable t)
        {
            fatalExceptionCaught(t);
        }
    }

    protected abstract void fatalExceptionCaught(Throwable t);

    // return true if the handler should be reactivated - if no new hurdles were encountered,
    // like running out of the other kind of reserve capacity
    protected boolean processUpToOneMessage(Limit endpointReserve, Limit globalReserve) throws IOException
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
    protected boolean acquireCapacity(Limit endpointReserve, Limit globalReserve, int bytes, long currentTimeNanos, long expiresAtNanos)
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

    protected ResourceLimits.Outcome acquireCapacity(Limit endpointReserve, Limit globalReserve, int bytes)
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
            return ResourceLimits.Outcome.INSUFFICIENT_ENDPOINT;
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

    public void releaseCapacity(int bytes)
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

    protected abstract String id();

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
    protected abstract class LargeMessage<H>
    {
        protected final int size;
        protected final H header;

        protected final List<ShareableBytes> buffers = new ArrayList<>();
        protected int received;

        protected final long expiresAtNanos;

        protected boolean isExpired;
        protected boolean isCorrupt;

        protected LargeMessage(int size, H header, long expiresAtNanos, boolean isExpired)
        {
            this.size = size;
            this.header = header;
            this.expiresAtNanos = expiresAtNanos;
            this.isExpired = isExpired;
        }

        protected LargeMessage(int size, H header, long expiresAtNanos, ShareableBytes bytes)
        {
            this(size, header, expiresAtNanos, false);
            buffers.add(bytes);
        }

        /**
         * Return true if this was the last frame of the large message.
         */
        public boolean supply(Frame frame)
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
            boolean expires = approxTime.isAfter(expiresAtNanos);
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
            isExpired |= approxTime.isAfter(expiresAtNanos);
        }

        protected abstract void onComplete();

        protected abstract void abort();

        protected void releaseBuffers()
        {
            buffers.forEach(ShareableBytes::release); buffers.clear();
        }

        protected void releaseBuffersAndCapacity()
        {
            releaseBuffers(); releaseCapacity(size);
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

        private Ticket register(AbstractMessageHandler handler, int bytesRequested, long registeredAtNanos, long expiresAtNanos)
        {
            Ticket ticket = new Ticket(this, handler, bytesRequested, registeredAtNanos, expiresAtNanos);
            Ticket previous = queue.relaxedPeekLastAndOffer(ticket);
            if (null == previous || !previous.isWaiting())
                signal(); // only signal the queue if this handler is first to register
            return ticket;
        }

        @VisibleForTesting
        public void signal()
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
            private final AbstractMessageHandler handler;
            private final int bytesRequested;
            private final long reigsteredAtNanos;
            private final long expiresAtNanos;

            private Ticket(WaitQueue waitQueue, AbstractMessageHandler handler, int bytesRequested, long registeredAtNanos, long expiresAtNanos)
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
        void call(AbstractMessageHandler handler);
    }
}
