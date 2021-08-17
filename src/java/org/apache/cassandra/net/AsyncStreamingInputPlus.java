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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.apache.cassandra.io.util.RebufferingInputStream;

// TODO: rewrite
public class AsyncStreamingInputPlus extends RebufferingInputStream
{
    public static class InputTimeoutException extends IOException
    {
    }

    private static final long DEFAULT_REBUFFER_BLOCK_IN_MILLIS = TimeUnit.MINUTES.toMillis(3);

    private final Channel channel;

    /**
     * The parent, or owning, buffer of the current buffer being read from ({@link super#buffer}).
     */
    private ByteBuf currentBuf;

    private final BlockingQueue<ByteBuf> queue;

    private final long rebufferTimeoutNanos;

    private volatile boolean isClosed;

    public AsyncStreamingInputPlus(Channel channel)
    {
        this(channel, DEFAULT_REBUFFER_BLOCK_IN_MILLIS, TimeUnit.MILLISECONDS);
    }

    AsyncStreamingInputPlus(Channel channel, long rebufferTimeout, TimeUnit rebufferTimeoutUnit)
    {
        super(Unpooled.EMPTY_BUFFER.nioBuffer());
        currentBuf = Unpooled.EMPTY_BUFFER;

        queue = new LinkedBlockingQueue<>();
        rebufferTimeoutNanos = rebufferTimeoutUnit.toNanos(rebufferTimeout);

        this.channel = channel;
        channel.config().setAutoRead(false);
    }

    /**
     * Append a {@link ByteBuf} to the end of the einternal queue.
     *
     * Note: it's expected this method is invoked on the netty event loop.
     */
    public boolean append(ByteBuf buf) throws IllegalStateException
    {
        if (isClosed) return false;

        queue.add(buf);

        /*
         * it's possible for append() to race with close(), so we need to ensure
         * that the bytebuf gets released in that scenario
         */
        if (isClosed)
            while ((buf = queue.poll()) != null)
                buf.release();

        return true;
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
     * the {@link #rebufferTimeoutNanos} elapses while blocking. It's then not safe to reuse this instance again.
     */
    @Override
    protected void reBuffer() throws EOFException, InputTimeoutException
    {
        if (queue.isEmpty())
            channel.read();

        currentBuf.release();
        currentBuf = null;
        buffer = null;

        ByteBuf next = null;
        try
        {
            next = queue.poll(rebufferTimeoutNanos, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException ie)
        {
            // nop
        }

        if (null == next)
            throw new InputTimeoutException();

        if (next == Unpooled.EMPTY_BUFFER) // Unpooled.EMPTY_BUFFER is the indicator that the input is closed
            throw new EOFException();

        currentBuf = next;
        buffer = next.nioBuffer();
    }

    public interface Consumer
    {
        int accept(ByteBuffer buffer) throws IOException;
    }

    /**
     * Consumes bytes in the stream until the given length
     */
    public void consume(Consumer consumer, long length) throws IOException
    {
        while (length > 0)
        {
            if (!buffer.hasRemaining())
                reBuffer();

            final int position = buffer.position();
            final int limit = buffer.limit();

            buffer.limit(position + (int) Math.min(length, limit - position));
            try
            {
                int copied = consumer.accept(buffer);
                buffer.position(position + copied);
                length -= copied;
            }
            finally
            {
                buffer.limit(limit);
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * As long as this method is invoked on the consuming thread the returned value will be accurate.
     */
    @VisibleForTesting
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
        return queue.isEmpty() && (buffer == null || !buffer.hasRemaining());
    }

    /**
     * {@inheritDoc}
     *
     * Note: This should invoked on the consuming thread.
     */
    @Override
    public void close()
    {
        if (isClosed)
            return;

        if (currentBuf != null)
        {
            currentBuf.release();
            currentBuf = null;
            buffer = null;
        }

        while (true)
        {
            try
            {
                ByteBuf buf = queue.poll(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                if (buf == Unpooled.EMPTY_BUFFER)
                    break;
                else
                    buf.release();
            }
            catch (InterruptedException e)
            {
                //
            }
        }

        isClosed = true;
    }

    /**
     * Mark this stream as closed, but do not release any of the resources.
     *
     * Note: this is best to be called from the producer thread.
     */
    public void requestClosure()
    {
        queue.add(Unpooled.EMPTY_BUFFER);
    }

    // TODO: let's remove this like we did for AsyncChannelOutputPlus
    public ByteBufAllocator getAllocator()
    {
        return channel.alloc();
    }
}
