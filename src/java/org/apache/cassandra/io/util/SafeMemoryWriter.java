/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SafeMemoryWriter extends DataOutputBuffer
{
    private SafeMemory memory;

    @SuppressWarnings("resource")
    public SafeMemoryWriter(long initialCapacity)
    {
        this(new SafeMemory(initialCapacity));
    }

    private SafeMemoryWriter(SafeMemory memory)
    {
        super(tailBuffer(memory).order(ByteOrder.BIG_ENDIAN));
        this.memory = memory;
    }

    public SafeMemory currentBuffer()
    {
        return memory;
    }

    @Override
    protected void expandToFit(long count)
    {
        resizeTo(calculateNewSize(count));
    }

    private void resizeTo(long newCapacity)
    {
        if (newCapacity != capacity())
        {
            long position = length();
            ByteOrder order = buffer.order();

            SafeMemory oldBuffer = memory;
            memory = this.memory.copy(newCapacity);
            buffer = tailBuffer(memory);

            int newPosition = (int) (position - tailOffset(memory));
            buffer.position(newPosition);
            buffer.order(order);

            oldBuffer.free();
        }
    }

    public void trim()
    {
        resizeTo(length());
    }

    public void close()
    {
        memory.close();
    }

    public Throwable close(Throwable accumulate)
    {
        return memory.close(accumulate);
    }

    public long length()
    {
        return tailOffset(memory) +  buffer.position();
    }

    public long capacity()
    {
        return memory.size();
    }

    @Override
    public SafeMemoryWriter order(ByteOrder order)
    {
        super.order(order);
        return this;
    }

    @Override
    public long validateReallocation(long newSize)
    {
        // Make sure size does not grow by more than the max buffer size, otherwise we'll hit an exception
        // when setting up the buffer position.
        return Math.min(newSize, length() + Integer.MAX_VALUE);
    }

    private static long tailOffset(Memory memory)
    {
        return Math.max(0, memory.size - Integer.MAX_VALUE);
    }

    private static ByteBuffer tailBuffer(Memory memory)
    {
        return memory.asByteBuffer(tailOffset(memory), (int) Math.min(memory.size, Integer.MAX_VALUE));
    }
}
