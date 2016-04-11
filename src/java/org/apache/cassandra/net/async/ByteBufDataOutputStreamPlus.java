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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.streaming.StreamSession;

/**
 * A {@link DataOutputStreamPlus} that writes to a {@link ByteBuf}. The novelty here is that all writes
 * actually get written in to a {@link ByteBuffer} that shares a backing buffer with a {@link ByteBuf}.
 * The trick to do that is allocate the ByteBuf, get a ByteBuffer from it by calling {@link ByteBuf#nioBuffer()},
 * and passing that to the super class as {@link #buffer}. When the {@link #buffer} is full or {@link #doFlush(int)}
 * is invoked, the {@link #currentBuf} is published to the netty channel.
 */
public class ByteBufDataOutputStreamPlus extends BufferedDataOutputStreamPlus
{
    private final StreamSession session;
    private final Channel channel;
    private final int bufferSize;

    /**
     * Tracks how many bytes we've written to the netty channel. This more or less follows the channel's
     * high/low water marks and ultimately the 'writablility' status of the channel. Unfortunately there's
     * no notification mechanism that can poke a producer to let it know when the channel becomes writable
     * (after it was unwritable); hence, the use of a {@link Semaphore}.
     */
    private final Semaphore channelRateLimiter;

    /**
     * This *must* be the owning {@link ByteBuf} for the {@link BufferedDataOutputStreamPlus#buffer}
     */
    private ByteBuf currentBuf;

    private ByteBufDataOutputStreamPlus(StreamSession session, Channel channel, ByteBuf buffer, int bufferSize)
    {
        super(buffer.nioBuffer(0, bufferSize));
        this.session = session;
        this.channel = channel;
        this.currentBuf = buffer;
        this.bufferSize = bufferSize;

        channelRateLimiter = new Semaphore(channel.config().getWriteBufferHighWaterMark(), true);
    }

    @Override
    protected WritableByteChannel newDefaultChannel()
    {
        return new WritableByteChannel()
        {
            @Override
            public int write(ByteBuffer src) throws IOException
            {
                assert src == buffer;
                int size = src.position();
                doFlush(size);
                return size;
            }

            @Override
            public boolean isOpen()
            {
                return channel.isOpen();
            }

            @Override
            public void close()
            {   }
        };
    }

    public static ByteBufDataOutputStreamPlus create(StreamSession session, Channel channel, int bufferSize)
    {
        ByteBuf buf = channel.alloc().directBuffer(bufferSize, bufferSize);
        return new ByteBufDataOutputStreamPlus(session, channel, buf, bufferSize);
    }

    /**
     * Writes the incoming buffer directly to the backing {@link #channel}, without copying to the intermediate {@link #buffer}.
     */
    public ChannelFuture writeToChannel(ByteBuf buf) throws IOException
    {
        doFlush(buffer.position());

        int byteCount = buf.readableBytes();
        if (!Uninterruptibles.tryAcquireUninterruptibly(channelRateLimiter, byteCount, 5, TimeUnit.MINUTES))
            throw new IOException("outbound channel was not writable");

        // the (possibly naive) assumption that we should always flush after each incoming buf
        ChannelFuture channelFuture = channel.writeAndFlush(buf);
        channelFuture.addListener(future -> handleBuffer(future, byteCount));
        return channelFuture;
    }

    /**
     * Writes the incoming buffer directly to the backing {@link #channel}, without copying to the intermediate {@link #buffer}.
     * The incoming buffer will be automatically released when the netty channel invokes the listeners of success/failure to
     * send the buffer.
     */
    public ChannelFuture writeToChannel(ByteBuffer buffer) throws IOException
    {
        ChannelFuture channelFuture = writeToChannel(Unpooled.wrappedBuffer(buffer));
        channelFuture.addListener(future -> FileUtils.clean(buffer));
        return channelFuture;
    }

    @Override
    protected void doFlush(int count) throws IOException
    {
        // flush the current backing write buffer only if there's any pending data
        if (buffer.position() > 0 && channel.isOpen())
        {
            int byteCount = buffer.position();
            currentBuf.writerIndex(byteCount);

            if (!Uninterruptibles.tryAcquireUninterruptibly(channelRateLimiter, byteCount, 2, TimeUnit.MINUTES))
                throw new IOException("outbound channel was not writable");

            channel.writeAndFlush(currentBuf).addListener(future -> handleBuffer(future, byteCount));
            currentBuf = channel.alloc().directBuffer(bufferSize, bufferSize);
            buffer = currentBuf.nioBuffer(0, bufferSize);
        }
    }

    /**
     * Handles the result of publishing a buffer to the channel.
     *
     * Note: this will be executed on the event loop.
     */
    private void handleBuffer(Future<? super Void> future, int bytesWritten)
    {
        channelRateLimiter.release(bytesWritten);

        if (!future.isSuccess() && channel.isOpen())
            session.onError(future.cause());
    }

    public ByteBufAllocator getAllocator()
    {
        return channel.alloc();
    }

    /**
     * {@inheritDoc}
     *
     * Flush any last buffered (if the channel is open), and release any buffers. *Not* responsible for closing
     * the netty channel as we might use it again for transferring more files.
     *
     * Note: should be called on the producer thread, not the netty event loop.
     */
    @Override
    public void close() throws IOException
    {
        doFlush(0);
        if (currentBuf.refCnt() > 0)
            currentBuf.release();
        currentBuf = null;
        buffer = null;
    }
}
