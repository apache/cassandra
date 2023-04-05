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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.apache.cassandra.utils.memory.MemoryUtil;

public class MemoryInputStream extends RebufferingInputStream implements DataInput
{
    private final Memory mem;
    private final int bufferSize;
    private long offset;


    public MemoryInputStream(Memory mem)
    {
        this(mem, Ints.saturatedCast(mem.size));
    }

    @VisibleForTesting
    public MemoryInputStream(Memory mem, int bufferSize)
    {
        super(getByteBuffer(mem.peer, bufferSize));
        this.mem = mem;
        this.bufferSize = bufferSize;
        this.offset = mem.peer + bufferSize;
    }

    @Override
    protected void reBuffer() throws IOException
    {
        if (offset - mem.peer >= mem.size())
            return;

        buffer = getByteBuffer(offset, Math.min(bufferSize, Ints.saturatedCast(memRemaining())));
        offset += buffer.capacity();
    }

    @Override
    public int available()
    {
        return Ints.saturatedCast(buffer.remaining() + memRemaining());
    }

    private long memRemaining()
    {
        return mem.size + mem.peer - offset;
    }

    private static ByteBuffer getByteBuffer(long offset, int length)
    {
        return MemoryUtil.getByteBuffer(offset, length, ByteOrder.BIG_ENDIAN);
    }
}
