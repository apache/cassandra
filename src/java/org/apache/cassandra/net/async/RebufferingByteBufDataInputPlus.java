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

package org.apache.cassandra.net.async;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelConfig;
import io.netty.util.ReferenceCountUtil;
import org.apache.cassandra.io.util.RebufferingInputStream;

public class RebufferingByteBufDataInputPlus extends RebufferingInputStream implements ReadableByteChannel
{
    /**
     * The parent, or owning, buffer of the current buffer being read from ({@link super#buffer}).
     */
    private ByteBuf currentBuf;

    private final BlockingQueue<ByteBuf> queue;

    /**
     * The count of live bytes in all {@link ByteBuf}s held by this instance.
     */
    private final AtomicInteger queuedByteCount;

    private final int lowWaterMark;
    private final int highWaterMark;
    private final ChannelConfig channelConfig;

    private volatile boolean closed;

    public RebufferingByteBufDataInputPlus(int lowWaterMark, int highWaterMark, ChannelConfig channelConfig)
    {
        super(Unpooled.EMPTY_BUFFER.nioBuffer());

        if (lowWaterMark > highWaterMark)
            throw new IllegalArgumentException(String.format("low water mark is greater than high water mark: %d vs %d", lowWaterMark, highWaterMark));

        currentBuf = Unpooled.EMPTY_BUFFER;
        this.lowWaterMark = lowWaterMark;
        this.highWaterMark = highWaterMark;
        this.channelConfig = channelConfig;
        queue = new LinkedBlockingQueue<>();
        queuedByteCount = new AtomicInteger();
    }

    /**
     * Append a {@link ByteBuf} to the end of the einternal queue.
     *
     * Note: it's expected this method is invoked on the netty event loop.
     */
    public void append(ByteBuf buf) throws IllegalStateException
    {
        assert buf != null : "buffer cannot be null";

        if (closed)
        {
            ReferenceCountUtil.release(buf);
            throw new IllegalStateException("stream is already closed, so cannot add another buffer");
        }

        // this slightly undercounts the live count as it doesn't include the currentBuf's size.
        // that's ok as the worst we'll do is allow another buffer in and add it to the queue,
        // and that point we'll disable auto-read. this is a tradeoff versus making some other member field
        // atomic or volatile.
        int queuedCount = queuedByteCount.addAndGet(buf.readableBytes());
        if (channelConfig.isAutoRead() && queuedCount > highWaterMark)
            channelConfig.setAutoRead(false);

        queue.add(buf);
    }

    /**
     * {@inheritDoc}
     *
     * Release open buffers and poll the {@link #queue} for more data.
     * <p>
     * This is best, and more or less expected, to be invoked on a consuming thread (not the event loop)
     * becasue if we block on the queue we can't fill it on the event loop (as that's where the buffers are coming from).
     */
    @Override
    protected void reBuffer() throws IOException
    {
        currentBuf.release();
        buffer = null;
        currentBuf = null;

        // possibly re-enable auto-read, *before* blocking on the queue, because if we block on the queue
        // without enabling auto-read we'll block forever :(
        if (!channelConfig.isAutoRead() && queuedByteCount.get() < lowWaterMark)
            channelConfig.setAutoRead(true);

        try
        {
            currentBuf = queue.take();
            int bytes;
            // if we get an explicitly empty buffer, we treat that as an indicator that the input is closed
            if (currentBuf == null || (bytes = currentBuf.readableBytes()) == 0)
            {
                releaseResources();
                throw new EOFException();
            }

            buffer = currentBuf.nioBuffer(currentBuf.readerIndex(), bytes);
            assert buffer.remaining() == bytes;
            queuedByteCount.addAndGet(-bytes);
            return;
        }
        catch (InterruptedException ie)
        {
            // nop - ignore
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException
    {
        int readLength = dst.remaining();
        int remaining = readLength;

        while (remaining > 0)
        {
            if (closed)
                throw new EOFException();

            if (!buffer.hasRemaining())
                reBuffer();
            int copyLength = Math.min(remaining, buffer.remaining());

            int originalLimit = buffer.limit();
            buffer.limit(buffer.position() + copyLength);
            dst.put(buffer);
            buffer.limit(originalLimit);
            remaining -= copyLength;
        }

        return readLength;
    }

    /**
     * {@inheritDoc}
     *
     * As long as this method is invoked on the consuming thread the returned value will be accurate.
     *
     * @throws EOFException thrown when no bytes are buffered and {@link #closed} is true.
     */
    @Override
    public int available() throws EOFException
    {
       final int availableBytes = queuedByteCount.get() + (buffer != null ? buffer.remaining() : 0);

        if (availableBytes == 0 && closed)
            throw new EOFException();

        if (!channelConfig.isAutoRead() && availableBytes < lowWaterMark)
            channelConfig.setAutoRead(true);

        return availableBytes;
    }

    @Override
    public boolean isOpen()
    {
        return !closed;
    }

    /**
     * {@inheritDoc}
     *
     * Note: This should invoked on the consuming thread.
     */
    @Override
    public void close()
    {
        closed = true;
        releaseResources();
    }

    private void releaseResources()
    {
        if (currentBuf != null)
        {
            if (currentBuf.refCnt() > 0)
                currentBuf.release(currentBuf.refCnt());
            currentBuf = null;
            buffer = null;
        }

        ByteBuf buf;
        while ((buf = queue.poll()) != null && buf.refCnt() > 0)
            buf.release(buf.refCnt());
    }

    /**
     * Mark this stream as closed, but do not release any of the resources.
     *
     * Note: this is best to be called from the producer thread.
     */
    public void markClose()
    {
        if (!closed)
        {
            closed = true;
            queue.add(Unpooled.EMPTY_BUFFER);
        }
    }

    /**
     * {@inheritDoc}
     *
     * Note: this is best to be called from the consumer thread.
     */
    @Override
    public String toString()
    {
        return new StringBuilder(128).append("RebufferingByteBufDataInputPlus: currentBuf = ").append(currentBuf)
                                  .append(" (super.buffer = ").append(buffer).append(')')
                                  .append(", queuedByteCount = ").append(queuedByteCount)
                                  .append(", queue buffers = ").append(queue)
                                  .append(", closed = ").append(closed)
                                  .toString();
    }

    public ByteBufAllocator getAllocator()
    {
        return channelConfig.getAllocator();
    }
}
