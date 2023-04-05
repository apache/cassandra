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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.net.FrameEncoderCrc;
import org.apache.cassandra.net.FrameEncoderLZ4;
import org.apache.cassandra.transport.Message.Response;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.BufferPool;

import static org.apache.cassandra.transport.CQLMessageHandler.envelopeSize;

abstract class Flusher implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(Flusher.class);
    @VisibleForTesting
    public static final int MAX_FRAMED_PAYLOAD_SIZE =
        Math.min(BufferPool.NORMAL_CHUNK_SIZE,
                 FrameEncoder.Payload.MAX_SIZE - Math.max(FrameEncoderCrc.HEADER_AND_TRAILER_LENGTH, FrameEncoderLZ4.HEADER_AND_TRAILER_LENGTH));

    static class FlushItem<T>
    {
        enum Kind {FRAMED, UNFRAMED}

        final Kind kind;
        final Channel channel;
        final T response;
        final Envelope request;
        final Consumer<FlushItem<T>> tidy;

        FlushItem(Kind kind, Channel channel, T response, Envelope request, Consumer<FlushItem<T>> tidy)
        {
            this.kind = kind;
            this.channel = channel;
            this.request = request;
            this.response = response;
            this.tidy = tidy;
        }

        void release()
        {
            tidy.accept(this);
        }

        static class Framed extends FlushItem<Envelope>
        {
            final FrameEncoder.PayloadAllocator allocator;
            Framed(Channel channel,
                   Envelope response,
                   Envelope request,
                   FrameEncoder.PayloadAllocator allocator,
                   Consumer<FlushItem<Envelope>> tidy)
            {
                super(Kind.FRAMED, channel, response, request, tidy);
                this.allocator = allocator;
            }
        }

        static class Unframed extends FlushItem<Response>
        {
            Unframed(Channel channel, Response response, Envelope request, Consumer<FlushItem<Response>> tidy)
            {
                super(Kind.UNFRAMED, channel, response, request, tidy);
            }
        }
    }

    static Flusher legacy(EventLoop loop)
    {
       return new LegacyFlusher(loop);
    }

    static Flusher immediate(EventLoop loop)
    {
        return new ImmediateFlusher(loop);
    }

    protected final EventLoop eventLoop;
    private final ConcurrentLinkedQueue<FlushItem<?>> queued = new ConcurrentLinkedQueue<>();
    protected final AtomicBoolean scheduled = new AtomicBoolean(false);
    protected final List<FlushItem<?>> processed = new ArrayList<>();
    private final HashSet<Channel> channels = new HashSet<>();
    private final Map<Channel, FlushBuffer> payloads = new HashMap<>();

    void start()
    {
        if (!scheduled.get() && scheduled.compareAndSet(false, true))
        {
            this.eventLoop.execute(this);
        }
    }

    private Flusher(EventLoop eventLoop)
    {
        this.eventLoop = eventLoop;
    }

    void enqueue(FlushItem<?> item)
    {
       queued.add(item);
    }

    FlushItem<?> poll()
    {
        return queued.poll();
    }

    boolean isEmpty()
    {
        return queued.isEmpty();
    }

    private void processUnframedResponse(FlushItem.Unframed flush)
    {
        flush.channel.write(flush.response, flush.channel.voidPromise());
        channels.add(flush.channel);
    }

    private void processFramedResponse(FlushItem.Framed flush)
    {
        Envelope outbound = flush.response;
        if (envelopeSize(outbound.header) >= MAX_FRAMED_PAYLOAD_SIZE)
        {
            flushLargeMessage(flush.channel, outbound, flush.allocator);
        }
        else
        {
            payloads.computeIfAbsent(flush.channel, channel -> new FlushBuffer(channel, flush.allocator, 5))
                    .add(flush.response);
        }
    }

    private void flushLargeMessage(Channel channel, Envelope outbound, FrameEncoder.PayloadAllocator allocator)
    {
        FrameEncoder.Payload payload;
        ByteBuffer buf;
        ByteBuf body = outbound.body;
        boolean firstFrame = true;
        // Highly unlikely that the body of a large message would be empty, but the check is cheap
        while (body.readableBytes() > 0 || firstFrame)
        {
            int payloadSize = Math.min(body.readableBytes(), MAX_FRAMED_PAYLOAD_SIZE);
            payload = allocator.allocate(false, payloadSize);
            if (logger.isTraceEnabled())
            {
                logger.trace("Allocated initial buffer of {} for 1 large item",
                             FBUtilities.prettyPrintMemory(payload.buffer.capacity()));
            }

            buf = payload.buffer;
            // BufferPool may give us a buffer larger than we asked for.
            // FrameEncoder may object if buffer.remaining is >= MAX_SIZE.
            if (payloadSize >= MAX_FRAMED_PAYLOAD_SIZE)
                buf.limit(MAX_FRAMED_PAYLOAD_SIZE);

            if (firstFrame)
            {
                outbound.encodeHeaderInto(buf);
                firstFrame = false;
            }

            int remaining = Math.min(buf.remaining(), body.readableBytes());
            if (remaining > 0)
                buf.put(body.slice(body.readerIndex(), remaining).nioBuffer());

            body.readerIndex(body.readerIndex() + remaining);
            writeAndFlush(channel, payload);
        }
    }

    private void writeAndFlush(Channel channel, FrameEncoder.Payload payload)
    {
        // we finish, but not "release" here since we're passing the buffer ownership to FrameEncoder#encode
        payload.finish();
        channel.writeAndFlush(payload, channel.voidPromise());
    }

    protected boolean processQueue()
    {
        boolean doneWork = false;
        FlushItem<?> flush;
        while ((flush = poll()) != null)
        {
            if (flush.kind == FlushItem.Kind.FRAMED)
                processFramedResponse((FlushItem.Framed) flush);
            else
                processUnframedResponse((FlushItem.Unframed) flush);

            processed.add(flush);
            doneWork = true;
        }
        return doneWork;
    }

    protected void flushWrittenChannels()
    {
        // flush the channels pre-V5 to which messages were written in writeSingleResponse
        for (Channel channel : channels)
            channel.flush();

        // Framed messages (V5) are grouped by channel, now encode them into payloads, write and flush
        for (FlushBuffer buffer : payloads.values())
            buffer.finish();

        // Ultimately, this passes the flush item to the Consumer<FlushItem> configured in
        // whichever Dispatcher.FlushItemConverter implementation created it. Due to the quite
        // different ways in which resource allocation is handled in protocol V5 and later
        // there are distinct implementations for V5 and pre-V5 connections:
        //   * o.a.c.t.CQLMessageHandler::toFlushItem for V5, which relates to FlushItem.Framed.
        //   * o.a.c.t.PreV5Handlers.LegacyDispatchHandler::toFlushItem, relating to FlushItem.Unframed
        // In both cases, the Consumer releases the buffers for the source envelope and returns the
        // capacity claimed for message processing back to the global and per-endpoint reserves.
        // Those reserves are used to determine if capacity is available for any inbound message
        // or whether we should attempt to shed load or apply backpressure.
        // The response buffers are handled differently though. In V5, CQL message envelopes are
        // collated into frames, and so their buffers can be released immediately after flushing.
        // In V4 however, the buffers containing each CQL envelope are emitted from Envelope.Encoder
        // and so releasing them is handled by Netty internally.
        for (FlushItem<?> item : processed)
            item.release();

        payloads.clear();
        channels.clear();
        processed.clear();
    }

    private class FlushBuffer extends ArrayList<Envelope>
    {
        private final Channel channel;
        private final FrameEncoder.PayloadAllocator allocator;
        private int sizeInBytes = 0;

        FlushBuffer(Channel channel, FrameEncoder.PayloadAllocator allocator, int initialCapacity)
        {
            super(initialCapacity);
            this.channel = channel;
            this.allocator = allocator;
        }

        public boolean add(Envelope toFlush)
        {
            sizeInBytes += envelopeSize(toFlush.header);
            return super.add(toFlush);
        }

        private FrameEncoder.Payload allocate(int requiredBytes, int maxItems)
        {
            int bufferSize = Math.min(requiredBytes, MAX_FRAMED_PAYLOAD_SIZE);
            FrameEncoder.Payload payload = allocator.allocate(true, bufferSize);
            // BufferPool may give us a buffer larger than we asked for.
            // FrameEncoder may object if buffer.remaining is >= MAX_SIZE.
            if (payload.remaining() >= MAX_FRAMED_PAYLOAD_SIZE)
                payload.buffer.limit(payload.buffer.position() + bufferSize);

            if (logger.isTraceEnabled())
            {
                logger.trace("Allocated initial buffer of {} for up to {} items",
                             FBUtilities.prettyPrintMemory(payload.buffer.capacity()),
                             maxItems);
            }
            return payload;
        }

        public void finish()
        {
            int messageSize;
            int writtenBytes = 0;
            int messagesToWrite = this.size();
            FrameEncoder.Payload sending = allocate(sizeInBytes, messagesToWrite);
            for (Envelope f : this)
            {
                messageSize = envelopeSize(f.header);
                if (sending.remaining() < messageSize)
                {
                    writeAndFlush(channel, sending);
                    sending = allocate(sizeInBytes - writtenBytes, messagesToWrite);
                }

                f.encodeInto(sending.buffer);
                writtenBytes += messageSize;
                messagesToWrite--;
            }
            writeAndFlush(channel, sending);
        }
    }

    private static final class LegacyFlusher extends Flusher
    {
        int runsSinceFlush = 0;
        int runsWithNoWork = 0;

        private LegacyFlusher(EventLoop eventLoop)
        {
            super(eventLoop);
        }

        public void run()
        {
            boolean doneWork = processQueue();
            runsSinceFlush++;

            if (!doneWork || runsSinceFlush > 2 || processed.size() > 50)
            {
                flushWrittenChannels();
                runsSinceFlush = 0;
            }

            if (doneWork)
            {
                runsWithNoWork = 0;
            }
            else
            {
                // either reschedule or cancel
                if (++runsWithNoWork > 5)
                {
                    scheduled.set(false);
                    if (isEmpty() || !scheduled.compareAndSet(false, true))
                        return;
                }
            }

            eventLoop.schedule(this, 10000, TimeUnit.NANOSECONDS);
        }
    }

    private static final class ImmediateFlusher extends Flusher
    {
        private ImmediateFlusher(EventLoop eventLoop)
        {
            super(eventLoop);
        }

        public void run()
        {
            scheduled.set(false);
            try
            {
                processQueue();
            }
            finally
            {
                flushWrittenChannels();
            }
        }
    }
}
