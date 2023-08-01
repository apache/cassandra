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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.utils.FastByteOperations;

import static org.apache.cassandra.config.CassandraRelevantProperties.NIO_DATA_OUTPUT_STREAM_PLUS_BUFFER_SIZE;

/**
 * An implementation of the DataOutputStreamPlus interface using a ByteBuffer to stage writes
 * before flushing them to an underlying channel.
 *
 * This class is completely thread unsafe.
 */
public class BufferedDataOutputStreamPlus extends DataOutputStreamPlus
{
    private static final int DEFAULT_BUFFER_SIZE = NIO_DATA_OUTPUT_STREAM_PLUS_BUFFER_SIZE.getInt();

    protected ByteBuffer buffer;

    public BufferedDataOutputStreamPlus(WritableByteChannel wbc)
    {
        this(wbc, DEFAULT_BUFFER_SIZE);
    }

    public BufferedDataOutputStreamPlus(WritableByteChannel wbc, int bufferSize)
    {
        this(wbc, ByteBuffer.allocateDirect(bufferSize));
        Preconditions.checkNotNull(wbc);
        Preconditions.checkArgument(bufferSize >= 8, "Buffer size must be large enough to accommodate a long/double");
    }

    @VisibleForTesting
    public BufferedDataOutputStreamPlus(WritableByteChannel channel, ByteBuffer buffer)
    {
        super(channel);
        this.buffer = buffer;
    }

    protected BufferedDataOutputStreamPlus(ByteBuffer buffer)
    {
        super();
        this.buffer = buffer;
    }

    protected BufferedDataOutputStreamPlus(int size)
    {
        this.buffer = allocate(size);
    }

    protected ByteBuffer allocate(int size)
    {
        return ByteBuffer.allocate(size);
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        assert buffer != null : "Attempt to use a closed data output";
        if (b == null)
            throw new NullPointerException();

        // avoid int overflow
        if (off < 0 || off > b.length || len < 0
            || len > b.length - off)
            throw new IndexOutOfBoundsException();

        if (len == 0)
            return;

        int copied = 0;
        while (copied < len)
        {
            if (buffer.hasRemaining())
            {
                int toCopy = Math.min(len - copied, buffer.remaining());
                buffer.put(b, off + copied, toCopy);
                copied += toCopy;
            }
            else
            {
                doFlush(len - copied);
            }
        }
    }

    /*
     * Makes a defensive copy of the incoming ByteBuffer and don't modify the position or limit
     * even temporarily so it is thread-safe WRT to the incoming buffer
     * (non-Javadoc)
     * @see org.apache.cassandra.io.util.DataOutputPlus#write(java.nio.ByteBuffer)
     */
    @Override
    public void write(ByteBuffer src) throws IOException
    {
        assert buffer != null : "Attempt to use a closed data output";
        int srcPos = src.position();
        int srcCount;
        int trgAvailable;
        while ((srcCount = src.limit() - srcPos) > (trgAvailable = buffer.remaining()))
        {
            FastByteOperations.copy(src, srcPos, buffer, buffer.position(), trgAvailable);
            buffer.position(buffer.position() + trgAvailable);
            srcPos += trgAvailable;
            doFlush(src.limit() - srcPos);
        }
        FastByteOperations.copy(src, srcPos, buffer, buffer.position(), srcCount);
        buffer.position(buffer.position() + srcCount);
    }

    @Override
    public void write(int b) throws IOException
    {
        assert buffer != null : "Attempt to use a closed data output";
        if (!buffer.hasRemaining())
            doFlush(1);
        buffer.put((byte) (b & 0xFF));
    }

    @Override
    public void writeBoolean(boolean v) throws IOException
    {
        assert buffer != null : "Attempt to use a closed data output";
        if (!buffer.hasRemaining())
            doFlush(1);
        buffer.put(v ? (byte)1 : (byte)0);
    }

    @Override
    public void writeByte(int v) throws IOException
    {
        write(v);
    }

    @Override
    public void writeMostSignificantBytes(long register, int bytes) throws IOException
    {
        assert buffer != null : "Attempt to use a closed data output";
        if (buffer.remaining() < Long.BYTES)
        {
            super.writeMostSignificantBytes(register, bytes);
        }
        else
        {
            int pos = buffer.position();
            buffer.putLong(pos, register);
            buffer.position(pos + bytes);
        }
    }

    @Override
    public void writeShort(int v) throws IOException
    {
        writeChar(v);
    }

    @Override
    public void writeChar(int v) throws IOException
    {
        assert buffer != null : "Attempt to use a closed data output";
        if (buffer.remaining() < 2)
            writeSlow(v, 2);
        else
            buffer.putChar((char) v);
    }

    @Override
    public void writeInt(int v) throws IOException
    {
        assert buffer != null : "Attempt to use a closed data output";
        if (buffer.remaining() < 4)
            writeSlow(v, 4);
        else
            buffer.putInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException
    {
        assert buffer != null : "Attempt to use a closed data output";
        if (buffer.remaining() < 8)
            writeSlow(v, 8);
        else
            buffer.putLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException
    {
        writeInt(Float.floatToRawIntBits(v));
    }

    @Override
    public void writeDouble(double v) throws IOException
    {
        writeLong(Double.doubleToRawLongBits(v));
    }

    @DontInline
    private void writeSlow(long bytes, int count) throws IOException
    {
        assert buffer != null : "Attempt to use a closed data output";
        int origCount = count;
        if (ByteOrder.BIG_ENDIAN == buffer.order())
            while (count > 0) writeByte((int) (bytes >>> (8 * --count)));
        else
            while (count > 0) writeByte((int) (bytes >>> (8 * (origCount - count--))));
    }

    @Override
    public void writeBytes(String s) throws IOException
    {
        for (int index = 0; index < s.length(); index++)
            writeByte(s.charAt(index));
    }

    @Override
    public void writeChars(String s) throws IOException
    {
        for (int index = 0; index < s.length(); index++)
            writeChar(s.charAt(index));
    }

    @Override
    public void writeUTF(String s) throws IOException
    {
        assert buffer != null : "Attempt to use a closed data output";
        UnbufferedDataOutputStreamPlus.writeUTF(s, this);
    }

    /*
     * Count is the number of bytes remaining to write ignoring already remaining capacity
     */
    @DontInline
    protected void doFlush(int count) throws IOException
    {
        assert buffer != null : "Attempt to use a closed data output";
        buffer.flip();

        while (buffer.hasRemaining())
            channel.write(buffer);

        buffer.clear();
    }

    @Override
    public void flush() throws IOException
    {
        doFlush(0);
    }

    @Override
    public void close() throws IOException
    {
        if (buffer == null)
            return;

        doFlush(0);
        channel.close();
        FileUtils.clean(buffer);
        buffer = null;
    }

    public BufferedDataOutputStreamPlus order(ByteOrder order)
    {
        assert buffer != null : "Attempt to use a closed data output";
        this.buffer.order(order);
        return this;
    }
}