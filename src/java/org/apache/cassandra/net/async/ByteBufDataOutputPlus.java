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

package org.apache.cassandra.net.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import com.google.common.base.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.UnbufferedDataOutputStreamPlus;
import org.apache.cassandra.utils.memory.MemoryUtil;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * A {@link DataOutputPlus} that uses a {@link ByteBuf} as a backing buffer. This class is completely thread unsafe and
 * it is expected that the backing buffer is sized correctly for all the writes you want to do (or the buffer needs
 * to be growable).
 */
public class ByteBufDataOutputPlus extends ByteBufOutputStream implements DataOutputPlus
{
    private final ByteBuf buffer;

    /**
     * ByteBuffer to use for defensive copies of direct {@link ByteBuffer}s - see {@link #write(ByteBuffer)}.
     */
    private final ByteBuffer hollowBuffer = MemoryUtil.getHollowDirectByteBuffer();

    public ByteBufDataOutputPlus(ByteBuf buffer)
    {
        super(buffer);
        this.buffer = buffer;
    }

    /**
     * {@inheritDoc} - "write the buffer without modifying its position"
     *
     * Unfortunately, netty's {@link ByteBuf#writeBytes(ByteBuffer)} modifies the byteBuffer's position,
     * and that is unsafe in our world wrt multithreading. Hence we need to be careful: reference the backing array
     * on heap ByteBuffers, and use a reusable "hollow" ByteBuffer ({@link #hollowBuffer}) for direct ByteBuffers.
     */
    @Override
    public void write(ByteBuffer byteBuffer) throws IOException
    {
        if (byteBuffer.hasArray())
        {
            write(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), byteBuffer.remaining());
        }
        else
        {
            assert byteBuffer.isDirect();
            MemoryUtil.duplicateDirectByteBuffer(byteBuffer, hollowBuffer);
            buffer.writeBytes(hollowBuffer);
        }
    }

    @Override
    public void write(Memory memory, long offset, long length) throws IOException
    {
        for (ByteBuffer buffer : memory.asByteBuffers(offset, length))
            write(buffer);
    }

    @Override
    public <R> R applyToChannel(Function<WritableByteChannel, R> c) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeVInt(long v) throws IOException
    {
        writeUnsignedVInt(VIntCoding.encodeZigZag64(v));
    }

    @Override
    public void writeUnsignedVInt(long v) throws IOException
    {
        int size = VIntCoding.computeUnsignedVIntSize(v);
        if (size == 1)
        {
            buffer.writeByte((byte) (v & 0xFF));
            return;
        }

        buffer.writeBytes(VIntCoding.encodeVInt(v, size), 0, size);
    }

    @Override
    public void write(int b) throws IOException
    {
        buffer.writeByte((byte) (b & 0xFF));
    }

    @Override
    public void writeByte(int v) throws IOException
    {
        buffer.writeByte((byte) (v & 0xFF));
    }

    @Override
    public void writeBytes(String s) throws IOException
    {
        for (int index = 0; index < s.length(); index++)
            buffer.writeByte(s.charAt(index) & 0xFF);
    }

    @Override
    public void writeChars(String s) throws IOException
    {
        for (int index = 0; index < s.length(); index++)
            buffer.writeChar(s.charAt(index));
    }

    @Override
    public void writeUTF(String s) throws IOException
    {
        UnbufferedDataOutputStreamPlus.writeUTF(s, this);
    }
}
