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

package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.io.compress.BufferType;
import org.jctools.queues.MpmcArrayQueue;

/**
 * A very simple Bytebuffer pool with a fixed allocation size and a cached max allocation count. Will allow
 * you to go past the "max", freeing all buffers allocated beyond the max buffer count on release.
 *
 * Has a reusable thread local ByteBuffer that users can make use of.
 */
public class SimpleCachedBufferPool
{
    private final ThreadLocalByteBufferHolder bufferHolder;

    private final Queue<ByteBuffer> bufferPool;

    /**
     * The number of buffers currently used.
     */
    private AtomicInteger usedBuffers = new AtomicInteger(0);

    /**
     * Maximum number of buffers in the compression pool. Any buffers above this count that are allocated will be cleaned
     * upon release rather than held and re-used.
     */
    private final int maxBufferPoolSize;

    /**
     * Size of individual buffer segments on allocation.
     */
    private final int bufferSize;

    private final BufferType preferredReusableBufferType;

    public SimpleCachedBufferPool(int maxBufferPoolSize, int bufferSize, BufferType preferredReusableBufferType)
    {
        // We want to use a bounded queue to ensure that we do not pool more buffers than maxBufferPoolSize
        this.bufferPool = new MpmcArrayQueue<>(maxBufferPoolSize);
        this.maxBufferPoolSize = maxBufferPoolSize;
        this.bufferSize = bufferSize;
        this.preferredReusableBufferType = preferredReusableBufferType;
        this.bufferHolder = new ThreadLocalByteBufferHolder(preferredReusableBufferType);
    }

    public ByteBuffer createBuffer()
    {
        usedBuffers.incrementAndGet();
        ByteBuffer buf = bufferPool.poll();
        if (buf != null)
        {
            buf.clear();
            return buf;
        }
        return preferredReusableBufferType.allocate(bufferSize);
    }

    public ByteBuffer getThreadLocalReusableBuffer(int size)
    {
        return bufferHolder.getBuffer(size);
    }

    public void releaseBuffer(ByteBuffer buffer)
    {
        assert buffer != null;
        assert preferredReusableBufferType == BufferType.typeOf(buffer);

        usedBuffers.decrementAndGet();

        // We use a bounded queue. By consequence if we have reached the maximum size for the buffer pool
        // offer will return false and we know that we can simply get rid of the buffer.
        if (!bufferPool.offer(buffer))
            FileUtils.clean(buffer);
    }

    /**
     * Empties the buffer pool.
     */
    public void emptyBufferPool()
    {
        ByteBuffer buffer = bufferPool.poll();
        while(buffer != null)
        {
            FileUtils.clean(buffer);
            buffer = bufferPool.poll();
        }
    }

    /**
     * Checks if the number of used buffers has exceeded the maximum number of cached buffers.
     *
     * @return {@code true} if the number of used buffers has exceeded the maximum number of cached buffers,
     * {@code false} otherwise.
     */
    public boolean atLimit()
    {
        return usedBuffers.get() >= maxBufferPoolSize;
    }

    @Override
    public String toString()
    {
        return new StringBuilder()
               .append("SimpleBufferPool:")
               .append(" usedBuffers:").append(usedBuffers.get())
               .append(", maxBufferPoolSize:").append(maxBufferPoolSize)
               .append(", bufferSize:").append(bufferSize)
               .toString();
    }
}
