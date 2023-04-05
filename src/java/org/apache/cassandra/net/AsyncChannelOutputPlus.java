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
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.locks.LockSupport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;

import static java.lang.Math.max;

/**
 * A {@link DataOutputStreamPlus} that writes ASYNCHRONOUSLY to a Netty Channel.
 *
 * The close() and flush() methods synchronously wait for pending writes, and will propagate any exceptions
 * encountered in writing them to the wire.
 *
 * The correctness of this class depends on the ChannelPromise we create against a Channel always being completed,
 * which appears to be a guarantee provided by Netty so long as the event loop is running.
 *
 * There are two logical threads accessing the state in this class: the eventLoop of the channel, and the writer
 * (the writer thread may change, so long as only one utilises the class at any time).
 * Each thread has exclusive write access to certain state in the class, with the other thread only viewing the state,
 * simplifying concurrency considerations.
 */
public abstract class AsyncChannelOutputPlus extends BufferedDataOutputStreamPlus
{
    public static class FlushException extends IOException
    {
        public FlushException(String message)
        {
            super(message);
        }

        public FlushException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }

    final Channel channel;

    /** the number of bytes we have begun flushing; updated only by writer */
    private volatile long flushing;
    /** the number of bytes we have finished flushing, successfully or otherwise; updated only by eventLoop */
    private volatile long flushed;
    /** the number of bytes we have finished flushing to the network; updated only by eventLoop */
    private          long flushedToNetwork;
    /** any error that has been thrown during a flush; updated only by eventLoop */
    private volatile Throwable flushFailed;

    /**
     * state for pausing until flushing has caught up - store the number of bytes we need to be flushed before
     * we should be signalled, and store ourselves in {@link #waiting}; once the flushing thread exceeds this many
     * total bytes flushed, any Thread stored in waiting will be signalled.
     *
     * This works exactly like using a WaitQueue, except that we only need to manage a single waiting thread.
     */
    private volatile long signalWhenFlushed; // updated only by writer
    private volatile Thread waiting; // updated only by writer

    public AsyncChannelOutputPlus(Channel channel)
    {
        super(null, null);
        this.channel = channel;
    }

    /**
     * Create a ChannelPromise for a flush of the given size.
     * <p>
     * This method will not return until the write is permitted by the provided watermarks and in flight bytes,
     * and on its completion will mark the requested bytes flushed.
     * <p>
     * If this method returns normally, the ChannelPromise MUST be writtenAndFlushed, or else completed exceptionally.
     */
    protected ChannelPromise beginFlush(long byteCount, long lowWaterMark, long highWaterMark) throws IOException
    {
        waitForSpace(byteCount, lowWaterMark, highWaterMark);

        return AsyncChannelPromise.withListener(channel, future -> {
            if (future.isSuccess() && null == flushFailed)
            {
                flushedToNetwork += byteCount;
                releaseSpace(byteCount);
            }
            else if (null == flushFailed)
            {
                Throwable cause = future.cause();
                if (cause == null)
                {
                    cause = new FlushException("Flush failed for unknown reason");
                    cause.fillInStackTrace();
                }
                flushFailed = cause;
                releaseSpace(flushing - flushed);
            }
            else
            {
                assert flushing == flushed;
            }
        });
    }

    /**
     * Imposes our lowWaterMark/highWaterMark constraints, and propagates any exceptions thrown by prior flushes.
     *
     * If we currently have lowWaterMark or fewer bytes flushing, we are good to go.
     * If our new write will not take us over our highWaterMark, we are good to go.
     * Otherwise, we wait until either of these conditions are met.
     *
     * This may only be invoked by the writer thread, never by the eventLoop.
     *
     * @throws IOException if a prior asynchronous flush failed
     */
    private void waitForSpace(long bytesToWrite, long lowWaterMark, long highWaterMark) throws IOException
    {
        // decide when we would be willing to carry on writing
        // we are always writable if we have lowWaterMark or fewer bytes, no matter how many bytes we are flushing
        // our callers should not be supplying more than (highWaterMark - lowWaterMark) bytes, but we must work correctly if they do
        long wakeUpWhenFlushing = highWaterMark - bytesToWrite;
        waitUntilFlushed(max(lowWaterMark, wakeUpWhenFlushing), lowWaterMark);
        flushing += bytesToWrite;
    }

    /**
     * Implementation of waitForSpace, which calculates what flushed points we need to wait for,
     * parks if necessary and propagates flush failures.
     *
     * This may only be invoked by the writer thread, never by the eventLoop.
     */
    void waitUntilFlushed(long wakeUpWhenExcessBytesWritten, long signalWhenExcessBytesWritten) throws IOException
    {
        // we assume that we are happy to wake up at least as early as we will be signalled; otherwise we will never exit
        assert signalWhenExcessBytesWritten <= wakeUpWhenExcessBytesWritten;
        // flushing shouldn't change during this method invocation, so our calculations for signal and flushed are consistent
        long wakeUpWhenFlushed = flushing - wakeUpWhenExcessBytesWritten;
        if (flushed < wakeUpWhenFlushed)
            parkUntilFlushed(wakeUpWhenFlushed, flushing - signalWhenExcessBytesWritten);
        propagateFailedFlush();
    }

    /**
     * Utility method for waitUntilFlushed, which actually parks the current thread until the necessary
     * number of bytes have been flushed
     *
     * This may only be invoked by the writer thread, never by the eventLoop.
     */
    protected void parkUntilFlushed(long wakeUpWhenFlushed, long signalWhenFlushed)
    {
        assert wakeUpWhenFlushed <= signalWhenFlushed;
        assert waiting == null;
        this.waiting = Thread.currentThread();
        this.signalWhenFlushed = signalWhenFlushed;

        while (flushed < wakeUpWhenFlushed)
            LockSupport.park();
        waiting = null;
    }

    /**
     * Update our flushed count, and signal any waiters.
     *
     * This may only be invoked by the eventLoop, never by the writer thread.
     */
    protected void releaseSpace(long bytesFlushed)
    {
        long newFlushed = flushed + bytesFlushed;
        flushed = newFlushed;

        Thread thread = waiting;
        if (thread != null && signalWhenFlushed <= newFlushed)
            LockSupport.unpark(thread);
    }

    private void propagateFailedFlush() throws IOException
    {
        Throwable t = flushFailed;
        if (t != null)
        {
            if (SocketFactory.isCausedByConnectionReset(t))
                throw new FlushException("The channel this output stream was writing to has been closed", t);
            throw new FlushException("This output stream is in an unsafe state after an asynchronous flush failed", t);
        }
    }

    @Override
    abstract protected void doFlush(int count) throws IOException;

    abstract public long position();

    public long flushed()
    {
        // external flushed (that which has had flush() invoked implicitly or otherwise) == internal flushing
        return flushing;
    }

    public long flushedToNetwork()
    {
        return flushedToNetwork;
    }

    /**
     * Perform an asynchronous flush, then waits until all outstanding flushes have completed
     *
     * @throws IOException if any flush fails
     */
    @Override
    public void flush() throws IOException
    {
        doFlush(0);
        waitUntilFlushed(0, 0);
    }

    /**
     * Flush any remaining writes, and release any buffers.
     *
     * The channel is not closed, as it is assumed to be managed externally.
     *
     * WARNING: This method requires mutual exclusivity with all other producer methods to run safely.
     * It should only be invoked by the owning thread, never the eventLoop; the eventLoop should propagate
     * errors to {@link #flushFailed}, which will propagate them to the producer thread no later than its
     * final invocation to {@link #close()} or {@link #flush()} (that must not be followed by any further writes).
     */
    @Override
    public void close() throws IOException
    {
        try
        {
            flush();
        }
        finally
        {
            discard();
        }
    }

    /**
     * Discard any buffered data, and the buffers that contain it.
     * May be invoked instead of {@link #close()} if we terminate exceptionally.
     */
    public abstract void discard();

    @Override
    protected WritableByteChannel newDefaultChannel()
    {
        throw new UnsupportedOperationException();
    }

}
