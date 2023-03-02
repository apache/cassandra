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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.common.base.Preconditions;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.vint.VIntCoding;

import static java.lang.Math.min;

/**
 * Rough equivalent of BufferedInputStream and DataInputStream wrapping a ByteBuffer that can be refilled
 * via rebuffer. Implementations provide this buffer from various channels (socket, file, memory, etc).
 * <p>
 * RebufferingInputStream is not thread safe.
 */
public abstract class RebufferingInputStream extends DataInputStreamPlus implements DataInputPlus, Closeable
{
    protected ByteBuffer buffer;

    protected RebufferingInputStream(ByteBuffer buffer)
    {
        Preconditions.checkArgument(buffer == null || buffer.order() == ByteOrder.BIG_ENDIAN, "Buffer must have BIG ENDIAN byte ordering");
        this.buffer = buffer;
    }

    /**
     * Implementations must implement this method to refill the buffer.
     * They can expect the buffer to be empty when this method is invoked.
     * @throws IOException
     */
    protected abstract void reBuffer() throws IOException;

    @Override
    public void readFully(byte[] b) throws IOException
    {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException
    {
        int read = read(b, off, len);
        if (read < len)
            throw new EOFException("EOF after " + read + " bytes out of " + len);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        // avoid int overflow
        if (off < 0 || off > b.length || len < 0 || len > b.length - off)
            throw new IndexOutOfBoundsException();

        if (len == 0)
            return 0;

        int copied = 0;
        while (copied < len)
        {
            int position = buffer.position();
            int remaining = buffer.limit() - position;
            if (remaining == 0)
            {
                reBuffer();
                position = buffer.position();
                remaining = buffer.limit() - position;
                if (remaining == 0)
                    return copied == 0 ? -1 : copied;
            }
            int toCopy = min(len - copied, remaining);
            FastByteOperations.copy(buffer, position, b, off + copied, toCopy);
            buffer.position(position + toCopy);
            copied += toCopy;
        }

        return copied;
    }

    /**
     * Equivalent to {@link #read(byte[], int, int)}, where offset is {@code dst.position()} and length is {@code dst.remaining()}
     */
    public void readFully(ByteBuffer dst) throws IOException
    {
        int offset = dst.position();
        int len = dst.limit() - offset;

        int copied = 0;
        while (copied < len)
        {
            int position = buffer.position();
            int remaining = buffer.limit() - position;

            if (remaining == 0)
            {
                reBuffer();

                position = buffer.position();
                remaining = buffer.limit() - position;

                if (remaining == 0)
                    throw new EOFException("EOF after " + copied + " bytes out of " + len);
            }

            int toCopy = min(len - copied, remaining);
            FastByteOperations.copy(buffer, position, dst, offset + copied, toCopy);
            buffer.position(position + toCopy);
            copied += toCopy;
        }
    }

    @DontInline
    protected long readPrimitiveSlowly(int bytes) throws IOException
    {
        long result = 0;
        for (int i = 0; i < bytes; i++)
            result = (result << 8) | (readByte() & 0xFFL);
        return result;
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        if (n <= 0)
            return 0;
        int requested = n;
        int position = buffer.position(), limit = buffer.limit(), remaining;
        while ((remaining = limit - position) < n)
        {
            n -= remaining;
            buffer.position(limit);
            reBuffer();
            position = buffer.position();
            limit = buffer.limit();
            if (position == limit)
                return requested - n;
        }
        buffer.position(position + n);
        return requested;
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        return readByte() != 0;
    }

    @Override
    public byte readByte() throws IOException
    {
        if (!buffer.hasRemaining())
        {
            reBuffer();
            if (!buffer.hasRemaining())
                throw new EOFException();
        }

        return buffer.get();
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        return readByte() & 0xff;
    }

    @Override
    public short readShort() throws IOException
    {
        if (buffer.remaining() >= 2)
            return buffer.getShort();
        else
            return (short) readPrimitiveSlowly(2);
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public char readChar() throws IOException
    {
        if (buffer.remaining() >= 2)
            return buffer.getChar();
        else
            return (char) readPrimitiveSlowly(2);
    }

    @Override
    public int readInt() throws IOException
    {
        if (buffer.remaining() >= 4)
            return buffer.getInt();
        else
            return (int) readPrimitiveSlowly(4);
    }

    @Override
    public long readLong() throws IOException
    {
        if (buffer.remaining() >= 8)
            return buffer.getLong();
        else
            return readPrimitiveSlowly(8);
    }

    @Override
    public long readVInt() throws IOException
    {
        return VIntCoding.decodeZigZag64(readUnsignedVInt());
    }

    @Override
    public int readVInt32() throws IOException
    {
        return VIntCoding.checkedCast(VIntCoding.decodeZigZag64(readUnsignedVInt()));
    }

    @Override
    public long readUnsignedVInt() throws IOException
    {
        //If 9 bytes aren't available use the slow path in VIntCoding
        if (buffer.remaining() < 9)
            return VIntCoding.readUnsignedVInt(this);

        byte firstByte = buffer.get();

        //Bail out early if this is one byte, necessary or it fails later
        if (firstByte >= 0)
            return firstByte;

        int extraBytes = VIntCoding.numberOfExtraBytesToRead(firstByte);

        int position = buffer.position();
        int extraBits = extraBytes * 8;

        long retval = buffer.getLong(position);
        if (buffer.order() == ByteOrder.LITTLE_ENDIAN)
            retval = Long.reverseBytes(retval);
        buffer.position(position + extraBytes);

        // truncate the bytes we read in excess of those we needed
        retval >>>= 64 - extraBits;
        // remove the non-value bits from the first byte
        firstByte &= VIntCoding.firstByteValueMask(extraBytes);
        // shift the first byte up to its correct position
        retval |= (long) firstByte << extraBits;
        return retval;
    }

    @Override
    public int readUnsignedVInt32() throws IOException
    {
        return VIntCoding.checkedCast(readUnsignedVInt());
    }

    @Override
    public float readFloat() throws IOException
    {
        if (buffer.remaining() >= 4)
            return buffer.getFloat();
        else
            return Float.intBitsToFloat((int)readPrimitiveSlowly(4));
    }

    @Override
    public double readDouble() throws IOException
    {
        if (buffer.remaining() >= 8)
            return buffer.getDouble();
        else
            return Double.longBitsToDouble(readPrimitiveSlowly(8));
    }

    @Override
    public String readLine() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() throws IOException
    {
        return DataInputStream.readUTF(this);
    }

    @Override
    public int read() throws IOException
    {
        try
        {
            return readUnsignedByte();
        }
        catch (EOFException ex)
        {
            return -1;
        }
    }
}
