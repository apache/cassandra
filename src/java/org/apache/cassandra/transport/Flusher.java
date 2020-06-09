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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.transport.Message.Response;

abstract class Flusher implements Runnable
{
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

    final EventLoop eventLoop;
    final ConcurrentLinkedQueue<FlushItem<?>> queued = new ConcurrentLinkedQueue<>();
    final AtomicBoolean scheduled = new AtomicBoolean(false);
    final HashSet<Channel> channels = new HashSet<>();
    final List<FlushItem<?>> flushed = new ArrayList<>();
    final Map<Channel, FrameEncoder.Payload> payloads = new HashMap<>();

    void start()
    {
        if (!scheduled.get() && scheduled.compareAndSet(false, true))
        {
            this.eventLoop.execute(this);
        }
    }

    public Flusher(EventLoop eventLoop)
    {
        this.eventLoop = eventLoop;
    }

    protected void encodeIntoFramedResponsePayload(FlushItem.Framed flush)
    {
        Frame outbound = flush.response;
        if (Frame.Header.LENGTH + outbound.header.bodySizeInBytes >= FrameEncoder.Payload.MAX_SIZE)
        {
            flushLargeMessage(flush.channel, outbound, flush.allocator);
        }
        else
        {
            FrameEncoder.Payload sending = payloads.get(flush.channel);
            if (null == sending)
            {
                sending = flush.allocator.allocate(true, FrameEncoder.Payload.MAX_SIZE - 1);
                // BufferPool may give us a buffer larger than we asked for. FrameEncoder may
                // object if buffer.remaining is >= MAX_SIZE.
                // TODO a better way to deal with this
                sending.buffer.limit(FrameEncoder.Payload.MAX_SIZE - 1);
                payloads.put(flush.channel, sending);
            }

            if (sending.remaining() < Frame.Header.LENGTH + outbound.header.bodySizeInBytes)
            {
                sending.finish();
                flush.channel.write(sending, flush.channel.voidPromise());
                sending.release();
                sending = flush.allocator.allocate(true, FrameEncoder.Payload.MAX_SIZE - 1);
                payloads.put(flush.channel, sending);
            }

            outbound.encodeInto(sending.buffer);
            outbound.release();
        }
    }

    protected void flushLargeMessage(Channel channel, Frame outbound, FrameEncoder.PayloadAllocator allocator)
    {
        FrameEncoder.Payload largePayload = allocator.allocate(false, FrameEncoder.Payload.MAX_SIZE - 1);
        ByteBuffer buf = largePayload.buffer;
        buf.limit(FrameEncoder.Payload.MAX_SIZE - 1);
        outbound.encodeHeaderInto(buf);

        int capacityRemaining = buf.limit() - buf.position();
        ByteBuf body = outbound.body;
        buf.put(body.slice(body.readerIndex(), capacityRemaining).nioBuffer());
        largePayload.finish();
        channel.writeAndFlush(largePayload, channel.voidPromise());
        body.readerIndex(capacityRemaining);
        int idx = body.readerIndex();

        while (body.readableBytes() >= FrameEncoder.Payload.MAX_SIZE)
        {
            largePayload = allocator.allocate(false, FrameEncoder.Payload.MAX_SIZE - 1);
            buf = largePayload.buffer;
            buf.limit(FrameEncoder.Payload.MAX_SIZE - 1);
            int remaining = Math.min(buf.remaining(), body.readableBytes());
            buf.put(body.slice(body.readerIndex(), remaining).nioBuffer());
            body.readerIndex(idx + remaining);
            idx = body.readerIndex();
            largePayload.finish();
            channel.writeAndFlush(largePayload, channel.voidPromise());
            largePayload.release();
        }

        if (body.readableBytes() > 0)
        {
            largePayload = allocator.allocate(false, body.readableBytes());
            buf = largePayload.buffer;
            buf.limit(buf.position() + body.readableBytes());
            buf.put(body.slice(body.readerIndex(), body.readableBytes()).nioBuffer());
            largePayload.finish();
            channel.writeAndFlush(largePayload, channel.voidPromise());
            largePayload.release();
        }
    }

    protected void writeUnframedResponse(FlushItem.Unframed flush)
    {
        flush.channel.write(flush.response, flush.channel.voidPromise());
        channels.add(flush.channel);
    }

    protected void processItem(FlushItem flush)
    {
        if (flush.kind == FlushItem.Kind.FRAMED)
            encodeIntoFramedResponsePayload((FlushItem.Framed)flush);
        else
            writeUnframedResponse((FlushItem.Unframed)flush);
        flushed.add(flush);
    }

    protected void flushWrittenChannels()
    {
        // flush the channels pre-V5 to which messages were written in writeSingleResponse
        for (Channel channel : channels)
            channel.flush();

        // Framed messages (V5) are accumulated into payloads which may now need to be written & flushed
        for (Map.Entry<Channel, FrameEncoder.Payload> entry : payloads.entrySet())
        {
            Channel channel = entry.getKey();
            FrameEncoder.Payload sending = entry.getValue();
            sending.finish();
            channel.writeAndFlush(sending, channel.voidPromise());
            sending.release();
            payloads.remove(channel);
        }

        for (FlushItem item : flushed)
            item.release();

        channels.clear();
        flushed.clear();
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
            FlushItem flush;
            while (null != (flush = queued.poll()))
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
                    if (queued.isEmpty() || !scheduled.compareAndSet(false, true))
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
            FlushItem flush;
            scheduled.set(false);

            while (null != (flush = queued.poll()))
            {
                processItem(flush);
                doneWork = true;
            }

            if (doneWork)
                flushWrittenChannels();
        }
    }
}
