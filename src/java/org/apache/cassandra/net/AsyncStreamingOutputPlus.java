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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.ssl.SslHandler;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.SharedDefaultFileRegion.SharedFileChannel;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;

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
public class AsyncStreamingOutputPlus extends AsyncChannelOutputPlus implements StreamingDataOutputPlus
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncStreamingOutputPlus.class);

    private final BufferPool bufferPool = BufferPools.forNetworking();

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
        buffer = bufferPool.getAtLeast(8 << 10, BufferType.OFF_HEAP);
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

    /**
     * Provide a lambda that can request a buffer of suitable size, then fill the buffer and have
     * that buffer written and flushed to the underlying channel, without having to handle buffer
     * allocation, lifetime or cleanup, including in case of exceptions.
     * <p>
     * Any exception thrown by the Write will be propagated to the caller, after any buffer is cleaned up.
     */
    public int writeToChannel(Write write, RateLimiter limiter) throws IOException
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
                holder.buffer = bufferPool.get(size, BufferType.OFF_HEAP);
                return holder.buffer;
            });
        }
        catch (Throwable t)
        {
            // we don't currently support cancelling the flush, but at this point we are recoverable if we want
            if (holder.buffer != null)
                bufferPool.put(holder.buffer);
            if (holder.promise != null)
                holder.promise.tryFailure(t);
            throw t;
        }

        ByteBuffer buffer = holder.buffer;
        bufferPool.putUnusedPortion(buffer);

        int length = buffer.limit();
        channel.writeAndFlush(GlobalBufferPoolAllocator.wrap(buffer), holder.promise);
        return length;
    }

    /**
     * Writes all data in file channel to stream: <br>
     * * For zero-copy-streaming, 1MiB at a time, with at most 2MiB in flight at once. <br>
     * * For streaming with SSL, 64KiB at a time, with at most 32+64KiB (default low water mark + batch size) in flight. <br>
     * <p>
     * This method takes ownership of the provided {@link FileChannel}.
     * <p>
     * WARNING: this method blocks only for permission to write to the netty channel; it exits before
     * the {@link FileRegion}(zero-copy) or {@link ByteBuffer}(ssl) is flushed to the network.
     */
    public long writeFileToChannel(FileChannel file, RateLimiter limiter) throws IOException
    {
        if (channel.pipeline().get(SslHandler.class) != null)
            // each batch is loaded into ByteBuffer, 64KiB is more BufferPool friendly.
            return writeFileToChannel(file, limiter, 1 << 16);
        else
            // write files in 1MiB chunks, since there may be blocking work performed to fetch it from disk,
            // the data is never brought in process and is gated by the wire anyway
            return writeFileToChannelZeroCopy(file, limiter, 1 << 20, 1 << 20, 2 << 20);
    }

    @VisibleForTesting
    long writeFileToChannel(FileChannel fc, RateLimiter limiter, int batchSize) throws IOException
    {
        final long length = fc.size();
        long bytesTransferred = 0;

        try
        {
            while (bytesTransferred < length)
            {
                int toWrite = (int) min(batchSize, length - bytesTransferred);
                final long position = bytesTransferred;

                writeToChannel(bufferSupplier -> {
                    ByteBuffer outBuffer = bufferSupplier.get(toWrite);
                    long read = fc.read(outBuffer, position);
                    if (read != toWrite)
                        throw new IOException(String.format("could not read required number of bytes from " +
                                                            "file to be streamed: read %d bytes, wanted %d bytes",
                                                            read, toWrite));
                    outBuffer.flip();
                }, limiter);

                if (logger.isTraceEnabled())
                    logger.trace("Writing {} bytes at position {} of {}", toWrite, bytesTransferred, length);
                bytesTransferred += toWrite;
            }
        }
        finally
        {
            // we don't need to wait until byte buffer is flushed by netty
            fc.close();
        }

        return bytesTransferred;
    }

    @VisibleForTesting
    long writeFileToChannelZeroCopy(FileChannel file, RateLimiter limiter, int batchSize, int lowWaterMark, int highWaterMark) throws IOException
    {
        if (!limiter.isRateLimited())
            return writeFileToChannelZeroCopyUnthrottled(file);
        else
            return writeFileToChannelZeroCopyThrottled(file, limiter, batchSize, lowWaterMark, highWaterMark);
    }

    private long writeFileToChannelZeroCopyUnthrottled(FileChannel file) throws IOException
    {
        final long length = file.size();

        if (logger.isTraceEnabled())
            logger.trace("Writing {} bytes", length);

        ChannelPromise promise = beginFlush(length, 0, length);
        final DefaultFileRegion defaultFileRegion = new DefaultFileRegion(file, 0, length);
        channel.writeAndFlush(defaultFileRegion, promise);

        return length;
    }

    private long writeFileToChannelZeroCopyThrottled(FileChannel file, RateLimiter limiter, int batchSize, int lowWaterMark, int highWaterMark) throws IOException
    {
        final long length = file.size();
        long bytesTransferred = 0;

        final SharedFileChannel sharedFile = SharedDefaultFileRegion.share(file);
        try
        {
            int toWrite;
            while (bytesTransferred < length)
            {
                toWrite = (int) min(batchSize, length - bytesTransferred);

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
            bufferPool.put(buffer);
            buffer = null;
        }
    }
}
