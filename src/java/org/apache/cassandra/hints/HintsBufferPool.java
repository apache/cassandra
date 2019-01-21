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
package org.apache.cassandra.hints;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.net.MessagingService;
import sun.nio.ch.DirectBuffer;

/**
 * A primitive pool of {@link HintsBuffer} buffers. Under normal conditions should only hold two buffers - the currently
 * written to one, and a reserve buffer to switch to when the first one is beyond capacity.
 */
final class HintsBufferPool implements Closeable
{
    interface FlushCallback
    {
        void flush(HintsBuffer buffer, HintsBufferPool pool);
    }

    static final int MAX_ALLOCATED_BUFFERS = Integer.getInteger(Config.PROPERTY_PREFIX + "MAX_HINT_BUFFERS", 3);
    private volatile HintsBuffer currentBuffer;
    private final BlockingQueue<HintsBuffer> reserveBuffers;
    private final int bufferSize;
    private final FlushCallback flushCallback;
    private int allocatedBuffers = 0;

    HintsBufferPool(int bufferSize, FlushCallback flushCallback)
    {
        reserveBuffers = new LinkedBlockingQueue<>();
        this.bufferSize = bufferSize;
        this.flushCallback = flushCallback;
    }

    /**
     * @param hostIds host ids of the hint's target nodes
     * @param hint the hint to store
     */
    void write(Iterable<UUID> hostIds, Hint hint)
    {
        int hintSize = (int) Hint.serializer.serializedSize(hint, MessagingService.current_version);
        try (HintsBuffer.Allocation allocation = allocate(hintSize))
        {
            allocation.write(hostIds, hint);
        }
    }

    private HintsBuffer.Allocation allocate(int hintSize)
    {
        HintsBuffer current = currentBuffer();

        while (true)
        {
            HintsBuffer.Allocation allocation = current.allocate(hintSize);
            if (allocation != null)
                return allocation;

            // allocation failed due to insufficient size remaining in the buffer
            if (switchCurrentBuffer(current))
                flushCallback.flush(current, this);

            current = currentBuffer;
        }
    }

    void offer(HintsBuffer buffer)
    {
        if (!reserveBuffers.offer(buffer))
            throw new RuntimeException("Failed to store buffer");
    }

    // A wrapper to ensure a non-null currentBuffer value on the first call.
    HintsBuffer currentBuffer()
    {
        if (currentBuffer == null)
            initializeCurrentBuffer();

        return currentBuffer;
    }

    private synchronized void initializeCurrentBuffer()
    {
        if (currentBuffer == null)
            currentBuffer = createBuffer();
    }

    private synchronized boolean switchCurrentBuffer(HintsBuffer previous)
    {
        if (currentBuffer != previous)
            return false;

        HintsBuffer buffer = reserveBuffers.poll();
        if (buffer == null && allocatedBuffers >= MAX_ALLOCATED_BUFFERS)
        {
            try
            {
                //This BlockingQueue.take is a target for byteman in HintsBufferPoolTest
                buffer = reserveBuffers.take();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
        currentBuffer = buffer == null ? createBuffer() : buffer;

        return true;
    }

    private HintsBuffer createBuffer()
    {
        allocatedBuffers++;
        return HintsBuffer.create(bufferSize);
    }

    public void close()
    {
        currentBuffer.free();
    }
}
