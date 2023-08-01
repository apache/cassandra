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
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.BlockingQueue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.streaming.StreamingDataInputPlus;

import static org.apache.cassandra.utils.concurrent.BlockingQueues.newBlockingQueue;

/*
 * This class expects a single producer (Netty event loop) and single consumer thread (StreamingDeserializerTask).
 */
public class AsyncStreamingInputPlus extends RebufferingInputStream implements StreamingDataInputPlus
{
    private final Channel channel;

    /**
     * The parent, or owning, buffer of the current buffer being read from ({@link super#buffer}).
     */
    private ByteBuf currentBuf;

    private final BlockingQueue<ByteBuf> queue;

    private boolean isProducerClosed = false;
    private boolean isConsumerClosed = false;

    public AsyncStreamingInputPlus(Channel channel)
    {
        super(Unpooled.EMPTY_BUFFER.nioBuffer());
        currentBuf = Unpooled.EMPTY_BUFFER;

        queue = newBlockingQueue();

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
        if (isProducerClosed)
            return false; // buf should be released in NettyStreamingChannel.channelRead

        queue.add(buf);

        return true;
    }

    /**
     * {@inheritDoc}
     *
     * Release open buffers and poll the {@link #queue} for more data.
     * <p>
     * This is invoked on a consuming thread (not the event loop)
     * because if we block on the queue we can't fill it on the event loop (as that's where the buffers are coming from).
     *
     * @throws ClosedChannelException when no further reading from this instance should occur. Implies this instance is closed.
     */
    @Override
    protected void reBuffer() throws ClosedChannelException
    {
        if (isConsumerClosed)
            throw new ClosedChannelException();

        if (queue.isEmpty())
            channel.read();

        currentBuf.release();
        currentBuf = null;
        buffer = null;

        ByteBuf next = null;
        do
        {
            try
            {
                next = queue.take(); // rely on sentinel being sent to terminate this loop
            }
            catch (InterruptedException ie)
            {
                // ignore interruptions, retry and rely on being shut down by requestClosure
            }
        } while (next == null);

        if (next == Unpooled.EMPTY_BUFFER) // the indicator that the input is closed
        {
            isConsumerClosed = true;
            throw new ClosedChannelException();
        }

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

    public boolean isEmpty()
    {
        return isConsumerClosed || (queue.isEmpty() && (buffer == null || !buffer.hasRemaining()));
    }

    /**
     * {@inheritDoc}
     *
     * Note: This should invoked on the consuming thread.
     */
    @Override
    public void close()
    {
        if (isConsumerClosed)
            return;

        isConsumerClosed = true;

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
                ByteBuf buf = queue.take();
                if (buf == Unpooled.EMPTY_BUFFER)
                    break;
                buf.release();
            }
            catch (InterruptedException e)
            {
                // ignore and rely on requestClose having been called
            }
        }
    }

    /**
     * Mark this stream as closed, but do not release any of the resources.
     *
     * Note: this is best to be called from the producer thread.
     */
    public void requestClosure()
    {
        if (!isProducerClosed)
        {
            queue.add(Unpooled.EMPTY_BUFFER);
            isProducerClosed = true;
        }
    }

    // TODO: let's remove this like we did for AsyncChannelOutputPlus
    public ByteBufAllocator getAllocator()
    {
        return channel.alloc();
    }
}
