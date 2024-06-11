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

import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledUnsafeDirectByteBuf;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;

import static java.lang.Integer.max;

/**
 * A trivial wrapper around BufferPool for integrating with Netty, but retaining ownership of pooling behaviour
 * that is integrated into Cassandra's other pooling.
 */
public abstract class BufferPoolAllocator extends AbstractByteBufAllocator
{
    private static final BufferPool bufferPool = BufferPools.forNetworking();

    BufferPoolAllocator()
    {
        super(true);
    }

    @Override
    public boolean isDirectBufferPooled()
    {
        return true;
    }

    /** shouldn't be invoked */
    @Override
    protected ByteBuf newHeapBuffer(int minCapacity, int maxCapacity)
    {
        return Unpooled.buffer(minCapacity, maxCapacity);
    }

    @Override
    protected ByteBuf newDirectBuffer(int minCapacity, int maxCapacity)
    {
        ByteBuf result = new Wrapped(this, getAtLeast(minCapacity), maxCapacity);
        result.clear();
        return result;
    }

    ByteBuffer get(int size)
    {
        return bufferPool.get(size, BufferType.OFF_HEAP);
    }

    ByteBuffer getAtLeast(int size)
    {
        return bufferPool.getAtLeast(size, BufferType.OFF_HEAP);
    }

    void put(ByteBuffer buffer)
    {
        bufferPool.put(buffer);
    }

    void putUnusedPortion(ByteBuffer buffer)
    {
        bufferPool.putUnusedPortion(buffer);
    }

    @VisibleForTesting
    public long usedSizeInBytes()
    {
        return bufferPool.usedSizeInBytes();
    }

    void release()
    {
    }

    /**
     * A simple extension to UnpooledUnsafeDirectByteBuf that returns buffers to BufferPool on deallocate,
     * and permits extracting the buffer from it to take ownership and use directly.
     */
    public static class Wrapped extends UnpooledUnsafeDirectByteBuf
    {
        private ByteBuffer wrapped;

        Wrapped(BufferPoolAllocator allocator, ByteBuffer wrap, int maxCapacity)
        {
            super(allocator, wrap, max(wrap.capacity(), maxCapacity));
            wrapped = wrap;
        }

        @Override
        public ByteBuf capacity(int newCapacity)
        {
            if (newCapacity == capacity())
                return this;

            ByteBuf newBuffer = super.capacity(newCapacity);
            ByteBuffer nioBuffer = newBuffer.nioBuffer(0, newBuffer.capacity());

            bufferPool.put(wrapped);
            wrapped = nioBuffer;
            return newBuffer;
        }

        @Override
        protected ByteBuffer allocateDirect(int initialCapacity)
        {
            return bufferPool.getAtLeast(initialCapacity, BufferType.OFF_HEAP);
        }

        @Override
        protected void freeDirect(ByteBuffer buffer)
        {
            // noop
            // buffer is put back into the pool by deallocate()
        }

        @Override
        public void deallocate()
        {
            super.deallocate();
            if (wrapped != null)
                bufferPool.put(wrapped);
        }

        public ByteBuffer adopt()
        {
            if (refCnt() > 1)
                throw new IllegalStateException();
            ByteBuffer adopt = wrapped;
            adopt.position(readerIndex()).limit(writerIndex());
            wrapped = null;
            return adopt;
        }
    }
}
