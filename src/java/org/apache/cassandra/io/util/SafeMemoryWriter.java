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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SafeMemoryWriter extends AbstractDataOutput implements DataOutputPlus
{
    private ByteOrder order = ByteOrder.BIG_ENDIAN;
    private SafeMemory buffer;
    private long length;

    public SafeMemoryWriter(long initialCapacity)
    {
        buffer = new SafeMemory(initialCapacity);
    }

    @Override
    public void write(byte[] buffer, int offset, int count)
    {
        long newLength = ensureCapacity(count);
        this.buffer.setBytes(this.length, buffer, offset, count);
        this.length = newLength;
    }

    @Override
    public void write(int oneByte)
    {
        long newLength = ensureCapacity(1);
        buffer.setByte(length++, (byte) oneByte);
        length = newLength;
    }

    @Override
    public void writeShort(int val) throws IOException
    {
        if (order != ByteOrder.nativeOrder())
            val = Short.reverseBytes((short) val);
        long newLength = ensureCapacity(2);
        buffer.setShort(length, (short) val);
        length = newLength;
    }

    @Override
    public void writeInt(int val)
    {
        if (order != ByteOrder.nativeOrder())
            val = Integer.reverseBytes(val);
        long newLength = ensureCapacity(4);
        buffer.setInt(length, val);
        length = newLength;
    }

    @Override
    public void writeLong(long val)
    {
        if (order != ByteOrder.nativeOrder())
            val = Long.reverseBytes(val);
        long newLength = ensureCapacity(8);
        buffer.setLong(length, val);
        length = newLength;
    }

    @Override
    public void write(ByteBuffer buffer)
    {
        long newLength = ensureCapacity(buffer.remaining());
        this.buffer.setBytes(length, buffer);
        length = newLength;
    }

    @Override
    public void write(Memory memory, long offset, long size)
    {
        long newLength = ensureCapacity(size);
        buffer.put(length, memory, offset, size);
        length = newLength;
    }

    private long ensureCapacity(long size)
    {
        long newLength = this.length + size;
        if (newLength > buffer.size())
            setCapacity(Math.max(newLength, buffer.size() + (buffer.size() / 2)));
        return newLength;
    }

    public SafeMemory currentBuffer()
    {
        return buffer;
    }

    public void setCapacity(long newCapacity)
    {
        if (newCapacity != capacity())
        {
            SafeMemory oldBuffer = buffer;
            buffer = this.buffer.copy(newCapacity);
            oldBuffer.free();
        }
    }

    public void close()
    {
        buffer.close();
    }

    public long length()
    {
        return length;
    }

    public long capacity()
    {
        return buffer.size();
    }

    // TODO: consider hoisting this into DataOutputPlus, since most implementations can copy with this gracefully
    // this would simplify IndexSummary.IndexSummarySerializer.serialize()
    public SafeMemoryWriter order(ByteOrder order)
    {
        this.order = order;
        return this;
    }
}
