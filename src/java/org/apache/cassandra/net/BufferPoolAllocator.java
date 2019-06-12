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

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledUnsafeDirectByteBuf;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;

/**
 * A trivial wrapper around BufferPool for integrating with Netty, but retaining ownership of pooling behaviour
 * that is integrated into Cassandra's other pooling.
 */
abstract class BufferPoolAllocator extends AbstractByteBufAllocator
{
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
        ByteBuf result = new Wrapped(this, getAtLeast(minCapacity));
        result.clear();
        return result;
    }

    ByteBuffer get(int size)
    {
        return BufferPool.get(size, BufferType.OFF_HEAP);
    }

    ByteBuffer getAtLeast(int size)
    {
        return BufferPool.getAtLeast(size, BufferType.OFF_HEAP);
    }

    void put(ByteBuffer buffer)
    {
        BufferPool.put(buffer);
    }

    void putUnusedPortion(ByteBuffer buffer)
    {
        BufferPool.putUnusedPortion(buffer);
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

        Wrapped(BufferPoolAllocator allocator, ByteBuffer wrap)
        {
            super(allocator, wrap, wrap.capacity());
            wrapped = wrap;
        }

        @Override
        public void deallocate()
        {
            if (wrapped != null)
                BufferPool.put(wrapped);
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
