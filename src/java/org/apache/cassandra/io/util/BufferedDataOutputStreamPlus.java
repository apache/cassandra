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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import net.nicoulaj.compilecommand.annotations.DontInline;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.utils.memory.MemoryUtil;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * An implementation of the DataOutputStreamPlus interface using a ByteBuffer to stage writes
 * before flushing them to an underlying channel.
 *
 * This class is completely thread unsafe.
 */
public class BufferedDataOutputStreamPlus extends DataOutputStreamPlus
{
    private static final int DEFAULT_BUFFER_SIZE = Integer.getInteger(Config.PROPERTY_PREFIX + "nio_data_output_stream_plus_buffer_size", 1024 * 32);

    protected ByteBuffer buffer;

    //Allow derived classes to specify writing to the channel
    //directly shouldn't happen because they intercept via doFlush for things
    //like compression or checksumming
    //Another hack for this value is that it also indicates that flushing early
    //should not occur, flushes aligned with buffer size are desired
    //Unless... it's the last flush. Compression and checksum formats
    //expect block (same as buffer size) alignment for everything except the last block
    protected boolean strictFlushing = false;

    public BufferedDataOutputStreamPlus(RandomAccessFile ras)
    {
        this(ras.getChannel());
    }

    public BufferedDataOutputStreamPlus(RandomAccessFile ras, int bufferSize)
    {
        this(ras.getChannel(), bufferSize);
    }

    public BufferedDataOutputStreamPlus(FileOutputStream fos)
    {
        this(fos.getChannel());
    }

    public BufferedDataOutputStreamPlus(FileOutputStream fos, int bufferSize)
    {
        this(fos.getChannel(), bufferSize);
    }

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

    protected BufferedDataOutputStreamPlus(WritableByteChannel channel, ByteBuffer buffer)
    {
        super(channel);
        this.buffer = buffer;
    }

    protected BufferedDataOutputStreamPlus(ByteBuffer buffer)
    {
        super();
        this.buffer = buffer;
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
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

    // ByteBuffer to use for defensive copies
    private final ByteBuffer hollowBuffer = MemoryUtil.getHollowDirectByteBuffer();

    /*
     * Makes a defensive copy of the incoming ByteBuffer and don't modify the position or limit
     * even temporarily so it is thread-safe WRT to the incoming buffer
     * (non-Javadoc)
     * @see org.apache.cassandra.io.util.DataOutputPlus#write(java.nio.ByteBuffer)
     */
    @Override
    public void write(ByteBuffer toWrite) throws IOException
    {
        if (toWrite.hasArray())
        {
            write(toWrite.array(), toWrite.arrayOffset() + toWrite.position(), toWrite.remaining());
        }
        else
        {
            assert toWrite.isDirect();
            MemoryUtil.duplicateDirectByteBuffer(toWrite, hollowBuffer);
            int toWriteRemaining = toWrite.remaining();

            if (toWriteRemaining > buffer.remaining())
            {
                if (strictFlushing)
                {
                    writeExcessSlow();
                }
                else
                {
                    doFlush(toWriteRemaining - buffer.remaining());
                    while (hollowBuffer.remaining() > buffer.capacity())
                        channel.write(hollowBuffer);
                }
            }

            buffer.put(hollowBuffer);
        }
    }

    // writes anything we can't fit into the buffer
    @DontInline
    private void writeExcessSlow() throws IOException
    {
        int originalLimit = hollowBuffer.limit();
        while (originalLimit - hollowBuffer.position() > buffer.remaining())
        {
            hollowBuffer.limit(hollowBuffer.position() + buffer.remaining());
            buffer.put(hollowBuffer);
            doFlush(originalLimit - hollowBuffer.position());
        }
        hollowBuffer.limit(originalLimit);
    }

    @Override
    public void write(int b) throws IOException
    {
        if (!buffer.hasRemaining())
            doFlush(1);
        buffer.put((byte) (b & 0xFF));
    }

    @Override
    public void writeBoolean(boolean v) throws IOException
    {
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
    public void writeShort(int v) throws IOException
    {
        writeChar(v);
    }

    @Override
    public void writeChar(int v) throws IOException
    {
        if (buffer.remaining() < 2)
            writeSlow(v, 2);
        else
            buffer.putChar((char) v);
    }

    @Override
    public void writeInt(int v) throws IOException
    {
        if (buffer.remaining() < 4)
            writeSlow(v, 4);
        else
            buffer.putInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException
    {
        if (buffer.remaining() < 8)
            writeSlow(v, 8);
        else
            buffer.putLong(v);
    }

    @Override
    public void writeVInt(long value) throws IOException
    {
        writeUnsignedVInt(VIntCoding.encodeZigZag64(value));
    }

    @Override
    public void writeUnsignedVInt(long value) throws IOException
    {
        int size = VIntCoding.computeUnsignedVIntSize(value);
        if (size == 1)
        {
            write((int) value);
            return;
        }

        write(VIntCoding.encodeVInt(value, size), 0, size);
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
        UnbufferedDataOutputStreamPlus.writeUTF(s, this);
    }

    @Override
    public void write(Memory memory, long offset, long length) throws IOException
    {
        for (ByteBuffer buffer : memory.asByteBuffers(offset, length))
            write(buffer);
    }

    /*
     * Count is the number of bytes remaining to write ignoring already remaining capacity
     */
    @DontInline
    protected void doFlush(int count) throws IOException
    {
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
        doFlush(0);
        channel.close();
        FileUtils.clean(buffer);
        buffer = null;
    }

    @Override
    public <R> R applyToChannel(Function<WritableByteChannel, R> f) throws IOException
    {
        if (strictFlushing)
            throw new UnsupportedOperationException();
        //Don't allow writes to the underlying channel while data is buffered
        flush();
        return f.apply(channel);
    }

    public BufferedDataOutputStreamPlus order(ByteOrder order)
    {
        this.buffer.order(order);
        return this;
    }
}
