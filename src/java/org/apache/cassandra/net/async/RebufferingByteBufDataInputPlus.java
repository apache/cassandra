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
import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.util.ReferenceCountUtil;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.RebufferingInputStream;

// TODO:JEB add documentation!!
public class RebufferingByteBufDataInputPlus extends RebufferingInputStream implements ReadableByteChannel
{
    public static final Logger logger = LoggerFactory.getLogger(RebufferingByteBufDataInputPlus.class);

    private static final long DEFAULT_REBUFFER_BLOCK_IN_MILLIS = TimeUnit.MINUTES.toMillis(3);
    private final Channel channel;

    /**
     * The parent, or owning, buffer of the current buffer being read from ({@link super#buffer}).
     */
    private ByteBuf currentBuf;

    private final BlockingQueue<ByteBuf> queue;

    private final long rebufferBlockInMillis;

    private volatile boolean closed;

    public RebufferingByteBufDataInputPlus(Channel channel)
    {
        this (channel, DEFAULT_REBUFFER_BLOCK_IN_MILLIS);
    }

    public RebufferingByteBufDataInputPlus(Channel channel, long rebufferBlockInMillis)
    {
        super(Unpooled.EMPTY_BUFFER.nioBuffer());

        currentBuf = Unpooled.EMPTY_BUFFER;
        this.rebufferBlockInMillis = rebufferBlockInMillis;
        queue = new LinkedBlockingQueue<>();

        this.channel = channel;
        channel.config().setAutoRead(false);

        // TODO:JEB doc me
        channel.read();
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

        queue.add(buf);
    }

    /**
     * {@inheritDoc}
     *
     * Release open buffers and poll the {@link #queue} for more data.
     * <p>
     * This is best, and more or less expected, to be invoked on a consuming thread (not the event loop)
     * becasue if we block on the queue we can't fill it on the event loop (as that's where the buffers are coming from).
     *
     * @throws EOFException when no further reading from this instance should occur. Implies this instance is closed.
     * @throws InputTimeoutException when no new buffers arrive for reading before
     * the {@link #rebufferBlockInMillis} elapses while blocking. Implies this instance can still be used.
     */
    @Override
    protected void reBuffer() throws EOFException, InputTimeoutException
    {
        if (queue.isEmpty())
            channel.read();

        try
        {
            ByteBuf next = queue.poll(rebufferBlockInMillis, TimeUnit.MILLISECONDS);
            int bytes;
            if (next == null)
            {
                throw new InputTimeoutException();
            }
            else if ((bytes = next.readableBytes()) == 0)
            {
                // if we get an explicitly empty buffer, we treat that as an indicator that the input is closed
                releaseResources();
                throw new EOFException();
            }

            // delay releasing the buffers in case a InputTimeoutException is thrown.
            // we need to be be able to try reading again
            currentBuf.release();
            currentBuf = next;
            buffer = next.nioBuffer(currentBuf.readerIndex(), bytes);
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
    public int unsafeAvailable()
    {
        long count = buffer != null ? buffer.remaining() : 0;
        for (ByteBuf buf : queue)
            count += buf.readableBytes();

        return Ints.checkedCast(count);
    }

    // TODO:JEB add docs
    // TL;DR if there's no Bufs open anywhere here, issue a channle read to try and grab data.
    public void maybeIssueRead()
    {
        if (isEmpty())
            channel.read();
    }

    public boolean isEmpty()
    {
        return queue.isEmpty() && buffer.remaining() == 0;
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
                                  .append(", (super.buffer = ").append(buffer).append(')')
                                  .append(", queue buffers = ").append(queue)
                                  .append(", closed = ").append(closed)
                                  .toString();
    }

    public ByteBufAllocator getAllocator()
    {
        return channel.alloc();
    }

    /**
     * Consumes bytes in the stream until the given length
     *
     * @param writer
     * @param len
     * @return
     * @throws IOException
     */
    public long consumeUntil(BufferedDataOutputStreamPlus writer, long len) throws IOException
    {
        long copied = 0; // number of bytes copied
        while (copied < len)
        {
            if (buffer.remaining() == 0)
            {
                try
                {
                    reBuffer();
                }
                catch (EOFException e)
                {
                    throw new EOFException("EOF after " + copied + " bytes out of " + len);
                }
                if (buffer.remaining() == 0 && copied < len)
                    throw new AssertionError("reBuffer() failed to return data");
            }

            int originalLimit = buffer.limit();
            int toCopy = (int) Math.min(len - copied, buffer.remaining());
            buffer.limit(buffer.position() + toCopy);
            int written = writer.applyToChannel(c -> c.write(buffer));
            buffer.limit(originalLimit);
            copied += written;
        }

        return copied;
    }

    public static class InputTimeoutException extends IOException
    { }
}
