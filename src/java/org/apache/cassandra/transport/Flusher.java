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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

import static org.apache.cassandra.transport.CQLMessageHandler.frameSize;

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
        final Frame sourceFrame;
        final Consumer<FlushItem<T>> tidy;

        FlushItem(Kind kind, Channel channel, T response, Frame sourceFrame, Consumer<FlushItem<T>> tidy)
        {
            this.kind = kind;
            this.channel = channel;
            this.sourceFrame = sourceFrame;
            this.response = response;
            this.tidy = tidy;
        }

        void release()
        {
            tidy.accept(this);
        }

        static class Framed extends FlushItem<Frame>
        {
            final FrameEncoder.PayloadAllocator allocator;
            Framed(Channel channel,
                   Frame responseFrame,
                   Frame sourceFrame,
                   FrameEncoder.PayloadAllocator allocator,
                   Consumer<FlushItem<Frame>> tidy)
            {
                super(Kind.FRAMED, channel, responseFrame, sourceFrame, tidy);
                this.allocator = allocator;
            }
        }

        static class Unframed extends FlushItem<Response>
        {
            Unframed(Channel channel, Response response, Frame sourceFrame, Consumer<FlushItem<Response>> tidy)
            {
                super(Kind.UNFRAMED, channel, response, sourceFrame, tidy);
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
    protected final List<FlushItem<?>> flushed = new ArrayList<>();
    private final HashSet<Channel> channels = new HashSet<>();
    private final Map<Channel, FrameSet> payloads = new HashMap<>();

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
        Frame outbound = flush.response;
        if (frameSize(outbound.header) >= FrameEncoder.Payload.MAX_SIZE)
        {
            flushLargeMessage(flush.channel, outbound, flush.allocator);
        }
        else
        {
            payloads.computeIfAbsent(flush.channel, channel -> new FrameSet(channel, flush.allocator, 5))
                    .add(flush.response);
        }
    }

    private void flushLargeMessage(Channel channel, Frame outbound, FrameEncoder.PayloadAllocator allocator)
    {
        FrameEncoder.Payload payload;
        ByteBuffer buf;
        ByteBuf body = outbound.body;
        boolean firstFrame = true;
        // Highly unlikely that the frame body of a large message would be empty, but the check is cheap
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

    protected void processItem(FlushItem<?> flush)
    {
        if (flush.kind == FlushItem.Kind.FRAMED)
            processFramedResponse((FlushItem.Framed)flush);
        else
            processUnframedResponse((FlushItem.Unframed)flush);

        flushed.add(flush);
    }

    protected void flushWrittenChannels()
    {
        // flush the channels pre-V5 to which messages were written in writeSingleResponse
        for (Channel channel : channels)
            channel.flush();

        // Framed messages (V5) are grouped by channel, now encode them into payloads, write and flush
        for (FrameSet frameset : payloads.values())
            frameset.finish();

        for (FlushItem<?> item : flushed)
            item.release();

        payloads.clear();
        channels.clear();
        flushed.clear();
    }

    private static void writeAndFlush(Channel channel, FrameEncoder.Payload payload)
    {
        payload.finish();
        channel.writeAndFlush(payload, channel.voidPromise());
        payload.release();
    }

    private static class FrameSet extends ArrayList<Frame>
    {
        private final Channel channel;
        private final FrameEncoder.PayloadAllocator allocator;
        private int sizeInBytes = 0;

        FrameSet(Channel channel, FrameEncoder.PayloadAllocator allocator, int initialCapacity)
        {
            super(initialCapacity);
            this.channel = channel;
            this.allocator = allocator;
        }

        public boolean add(Frame frame)
        {
            sizeInBytes += frameSize(frame.header);
            return super.add(frame);
        }

        private FrameEncoder.Payload allocate(int requiredBytes, int maxItems)
        {
            int bufferSize = Math.min(requiredBytes, MAX_FRAMED_PAYLOAD_SIZE);
            FrameEncoder.Payload payload = allocator.allocate(true, bufferSize);
            // BufferPool may give us a buffer larger than we asked for.
            // FrameEncoder may object if buffer.remaining is >= MAX_SIZE.
            if (bufferSize >= MAX_FRAMED_PAYLOAD_SIZE)
                payload.buffer.limit(bufferSize);

            if (logger.isTraceEnabled())
            {
                logger.trace("Allocated initial buffer of {} for up to {} items",
                             FBUtilities.prettyPrintMemory(payload.buffer.capacity()),
                             maxItems);
            }
            return payload;
        }

        private void finish()
        {
            int writtenBytes = 0;
            int framesToWrite = this.size();
            FrameEncoder.Payload sending = allocate(sizeInBytes, framesToWrite);
            for (Frame f : this)
            {
                if (sending.remaining() < frameSize(f.header))
                {
                    writeAndFlush(channel, sending);
                    sending = allocate(sizeInBytes - writtenBytes, framesToWrite);
                }
                f.encodeInto(sending.buffer);
                f.release();
                writtenBytes += frameSize(f.header);
                framesToWrite--;
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

            boolean doneWork = false;
            FlushItem<?> flush;
            while (null != (flush = poll()))
            {
                processItem(flush);
                doneWork = true;
            }

            runsSinceFlush++;

            if (!doneWork || runsSinceFlush > 2 || flushed.size() > 50)
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
            boolean doneWork = false;
            FlushItem<?> flush;
            scheduled.set(false);

            while (null != (flush = poll()))
            {
                processItem(flush);
                doneWork = true;
            }

            if (doneWork)
                flushWrittenChannels();
        }
    }
}
