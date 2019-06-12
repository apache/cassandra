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
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.WriteBufferWaterMark;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.SharedDefaultFileRegion.SharedFileChannel;
import org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import org.apache.cassandra.utils.memory.BufferPool;

import static java.lang.Math.min;

/**
 * A {@link DataOutputStreamPlus} that writes ASYNCHRONOUSLY to a Netty Channel.
 *
 * The close() and flush() methods synchronously wait for pending writes, and will propagate any exceptions
 * encountered in writing them to the wire.
 *
 * The correctness of this class depends on the ChannelPromise we create against a Channel always being completed,
 * which appears to be a guarantee provided by Netty so long as the event loop is running.
 */
public class AsyncStreamingOutputPlus extends AsyncChannelOutputPlus
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncStreamingOutputPlus.class);

    final int defaultLowWaterMark;
    final int defaultHighWaterMark;

    public AsyncStreamingOutputPlus(Channel channel)
    {
        super(channel);
        WriteBufferWaterMark waterMark = channel.config().getWriteBufferWaterMark();
        this.defaultLowWaterMark = waterMark.low();
        this.defaultHighWaterMark = waterMark.high();
        allocateBuffer();
    }

    private void allocateBuffer()
    {
        // this buffer is only used for small quantities of data
        buffer = BufferPool.getAtLeast(8 << 10, BufferType.OFF_HEAP);
    }

    @Override
    protected void doFlush(int count) throws IOException
    {
        if (!channel.isOpen())
            throw new ClosedChannelException();

        // flush the current backing write buffer only if there's any pending data
        ByteBuffer flush = buffer;
        if (flush.position() == 0)
            return;

        flush.flip();
        int byteCount = flush.limit();
        ChannelPromise promise = beginFlush(byteCount, 0, Integer.MAX_VALUE);
        channel.writeAndFlush(GlobalBufferPoolAllocator.wrap(flush), promise);
        allocateBuffer();
    }

    public long position()
    {
        return flushed() + buffer.position();
    }

    public interface BufferSupplier
    {
        /**
         * Request a buffer with at least the given capacity.
         * This method may only be invoked once, and the lifetime of buffer it returns will be managed
         * by the AsyncChannelOutputPlus it was created for.
         */
        ByteBuffer get(int capacity) throws IOException;
    }

    public interface Write
    {
        /**
         * Write to a buffer, and flush its contents to the channel.
         * <p>
         * The lifetime of the buffer will be managed by the AsyncChannelOutputPlus you issue this Write to.
         * If the method exits successfully, the contents of the buffer will be written to the channel, otherwise
         * the buffer will be cleaned and the exception propagated to the caller.
         */
        void write(BufferSupplier supplier) throws IOException;
    }

    /**
     * Provide a lambda that can request a buffer of suitable size, then fill the buffer and have
     * that buffer written and flushed to the underlying channel, without having to handle buffer
     * allocation, lifetime or cleanup, including in case of exceptions.
     * <p>
     * Any exception thrown by the Write will be propagated to the caller, after any buffer is cleaned up.
     */
    public int writeToChannel(Write write, StreamRateLimiter limiter) throws IOException
    {
        doFlush(0);
        class Holder
        {
            ChannelPromise promise;
            ByteBuffer buffer;
        }
        Holder holder = new Holder();

        try
        {
            write.write(size -> {
                if (holder.buffer != null)
                    throw new IllegalStateException("Can only allocate one ByteBuffer");
                limiter.acquire(size);
                holder.promise = beginFlush(size, defaultLowWaterMark, defaultHighWaterMark);
                holder.buffer = BufferPool.get(size);
                return holder.buffer;
            });
        }
        catch (Throwable t)
        {
            // we don't currently support cancelling the flush, but at this point we are recoverable if we want
            if (holder.buffer != null)
                BufferPool.put(holder.buffer);
            if (holder.promise != null)
                holder.promise.tryFailure(t);
            throw t;
        }

        ByteBuffer buffer = holder.buffer;
        BufferPool.putUnusedPortion(buffer);

        int length = buffer.limit();
        channel.writeAndFlush(GlobalBufferPoolAllocator.wrap(buffer), holder.promise);
        return length;
    }

    /**
     * <p>
     * Writes all data in file channel to stream, 1MiB at a time, with at most 2MiB in flight at once.
     * This method takes ownership of the provided {@code FileChannel}.
     * <p>
     * WARNING: this method blocks only for permission to write to the netty channel; it exits before
     * the write is flushed to the network.
     */
    public long writeFileToChannel(FileChannel file, StreamRateLimiter limiter) throws IOException
    {
        // write files in 1MiB chunks, since there may be blocking work performed to fetch it from disk,
        // the data is never brought in process and is gated by the wire anyway
        return writeFileToChannel(file, limiter, 1 << 20, 1 << 20, 2 << 20);
    }

    public long writeFileToChannel(FileChannel file, StreamRateLimiter limiter, int batchSize, int lowWaterMark, int highWaterMark) throws IOException
    {
        final long length = file.size();
        long bytesTransferred = 0;

        final SharedFileChannel sharedFile = SharedDefaultFileRegion.share(file);
        try
        {
            while (bytesTransferred < length)
            {
                int toWrite = (int) min(batchSize, length - bytesTransferred);

                limiter.acquire(toWrite);
                ChannelPromise promise = beginFlush(toWrite, lowWaterMark, highWaterMark);

                SharedDefaultFileRegion fileRegion = new SharedDefaultFileRegion(sharedFile, bytesTransferred, toWrite);
                channel.writeAndFlush(fileRegion, promise);

                if (logger.isTraceEnabled())
                    logger.trace("Writing {} bytes at position {} of {}", toWrite, bytesTransferred, length);
                bytesTransferred += toWrite;
            }

            return bytesTransferred;
        }
        finally
        {
            sharedFile.release();
        }
    }

    /**
     * Discard any buffered data, and the buffers that contain it.
     * May be invoked instead of {@link #close()} if we terminate exceptionally.
     */
    public void discard()
    {
        if (buffer != null)
        {
            BufferPool.put(buffer);
            buffer = null;
        }
    }
}