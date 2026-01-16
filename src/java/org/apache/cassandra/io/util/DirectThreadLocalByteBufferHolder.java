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
import java.util.concurrent.ConcurrentHashMap;

import org.agrona.BitUtil;
import org.agrona.BufferUtil;

import org.apache.cassandra.utils.memory.MemoryUtil;

import io.netty.util.concurrent.FastThreadLocal;
import sun.nio.ch.DirectBuffer;

/**
 * Thread-local holder for block-aligned direct ByteBuffers used in Direct I/O operations.
 * <p>
 * Buffers are shared across all holders with the same block size and reused per-thread,
 * growing as needed. This mirrors {@link ThreadLocalByteBufferHolder}'s design.
 */
public final class DirectThreadLocalByteBufferHolder implements ByteBufferHolder
{
    private static final ConcurrentHashMap<Integer, FastThreadLocal<ByteBuffer>> buffersByBlockSize = new ConcurrentHashMap<>();

    private final FastThreadLocal<ByteBuffer> threadLocal;
    private final int blockSize;

    public DirectThreadLocalByteBufferHolder(int blockSize)
    {
        this.blockSize = blockSize;
        this.threadLocal = buffersByBlockSize.computeIfAbsent(blockSize, k -> new FastThreadLocal<>());
    }

    @Override
    public ByteBuffer getBuffer(int size)
    {
        int alignedSize = BitUtil.align(size, blockSize);
        ByteBuffer buffer = threadLocal.get();

        if (buffer != null && buffer.capacity() >= alignedSize)
        {
            buffer.clear().limit(alignedSize);
            return buffer;
        }

        if (buffer != null)
            cleanBuffer(buffer);

        buffer = BufferUtil.allocateDirectAligned(alignedSize, blockSize);
        threadLocal.set(buffer);
        return buffer;
    }

    private static void cleanBuffer(ByteBuffer buffer)
    {
        // Aligned buffers are slices; clean the backing buffer (attachment)
        DirectBuffer db = (DirectBuffer) buffer;
        ByteBuffer attachment = (ByteBuffer) db.attachment();
        MemoryUtil.clean(attachment != null ? attachment : buffer);
    }

}