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
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;

import com.google.common.base.Preconditions;

/**
 * Rough equivalent of BufferedInputStream and DataInputStream wrapping the input stream of a File or Socket
 * Created to work around the fact that when BIS + DIS delegate to NIO for socket IO they will allocate large
 * thread local direct byte buffers when a large array is used to read.
 *
 * There may also be some performance improvement due to using a DBB as the underlying buffer for IO and the removal
 * of some indirection and delegation when it comes to reading out individual values, but that is not the goal.
 *
 * Closing NIODataInputStream will invoke close on the ReadableByteChannel provided at construction.
 *
 * NIODataInputStream is not thread safe.
 */
public class NIODataInputStream extends InputStream implements DataInput, Closeable
{
    private final ReadableByteChannel rbc;
    private final ByteBuffer buf;


    public NIODataInputStream(ReadableByteChannel rbc, int bufferSize)
    {
        Preconditions.checkNotNull(rbc);
        Preconditions.checkArgument(bufferSize >= 8, "Buffer size must be large enough to accomadate a long/double");
        this.rbc = rbc;
        buf = ByteBuffer.allocateDirect(bufferSize);
        buf.position(0);
        buf.limit(0);
    }

    @Override
    public void readFully(byte[] b) throws IOException
    {
        readFully(b, 0, b.length);
    }


    @Override
    public void readFully(byte[] b, int off, int len) throws IOException
    {
        int copied = 0;
        while (copied < len)
        {
            int read = read(b, off + copied, len - copied);
            if (read < 0)
                throw new EOFException();
            copied += read;
        }
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (b == null)
            throw new NullPointerException();

        // avoid int overflow
        if (off < 0 || off > b.length || len < 0
                || len > b.length - off)
            throw new IndexOutOfBoundsException();

        if (len == 0)
            return 0;

        int copied = 0;
        while (copied < len)
        {
            if (buf.hasRemaining())
            {
                int toCopy = Math.min(len - copied, buf.remaining());
                buf.get(b, off + copied, toCopy);
                copied += toCopy;
            }
            else
            {
                int read = readNext();
                if (read < 0 && copied == 0) return -1;
                if (read <= 0) return copied;
            }
        }

        return copied;
    }

    /*
     * Refill the buffer, preserving any unread bytes remaining in the buffer
     */
    private int readNext() throws IOException
    {
        Preconditions.checkState(buf.remaining() != buf.capacity());
        assert(buf.remaining() < 8);

        /*
         * If there is data already at the start of the buffer, move the position to the end
         * If there is data but not at the start, move it to the start
         * Otherwise move the position to 0 so writes start at the beginning of the buffer
         *
         * We go to the trouble of shuffling the bytes remaining for cases where the buffer isn't fully drained
         * while retrieving a multi-byte value while the position is in the middle.
         */
        if (buf.position() == 0 && buf.hasRemaining())
        {
            buf.position(buf.limit());
        }
        else if (buf.hasRemaining())
        {
            ByteBuffer dup = buf.duplicate();
            buf.clear();
            buf.put(dup);
        }
        else
        {
            buf.position(0);
        }

        buf.limit(buf.capacity());

        int read = 0;
        while ((read = rbc.read(buf)) == 0) {}

        buf.flip();

        return read;
    }

    /*
     * Read at least minimum bytes and throw EOF if that fails
     */
    private void readMinimum(int minimum) throws IOException
    {
        assert(buf.remaining() < 8);
        while (buf.remaining() < minimum)
        {
            int read = readNext();
            if (read == -1)
            {
                //DataInputStream consumes the bytes even if it doesn't get the entire value, match the behavior here
                buf.position(0);
                buf.limit(0);
                throw new EOFException();
            }
        }
    }

    /*
     * Ensure the buffer contains the minimum number of readable bytes
     */
    private void prepareReadPrimitive(int minimum) throws IOException
    {
        if (buf.remaining() < minimum) readMinimum(minimum);
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        int skipped = 0;

        while (skipped < n)
        {
            int skippedThisTime = (int)skip(n - skipped);
            if (skippedThisTime <= 0) break;
            skipped += skippedThisTime;
        }

        return skipped;
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        prepareReadPrimitive(1);
        return buf.get() != 0;
    }

    @Override
    public byte readByte() throws IOException
    {
        prepareReadPrimitive(1);
        return buf.get();
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        prepareReadPrimitive(1);
        return buf.get() & 0xff;
    }

    @Override
    public short readShort() throws IOException
    {
        prepareReadPrimitive(2);
        return buf.getShort();
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public char readChar() throws IOException
    {
        prepareReadPrimitive(2);
        return buf.getChar();
    }

    @Override
    public int readInt() throws IOException
    {
        prepareReadPrimitive(4);
        return buf.getInt();
    }

    @Override
    public long readLong() throws IOException
    {
        prepareReadPrimitive(8);
        return buf.getLong();
    }

    @Override
    public float readFloat() throws IOException
    {
        prepareReadPrimitive(4);
        return buf.getFloat();
    }

    @Override
    public double readDouble() throws IOException
    {
        prepareReadPrimitive(8);
        return buf.getDouble();
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
    public void close() throws IOException
    {
            rbc.close();
    }

    @Override
    public int read() throws IOException
    {
        return readUnsignedByte();
    }

    @Override
    public int available() throws IOException
    {
        if (rbc instanceof SeekableByteChannel)
        {
            SeekableByteChannel sbc = (SeekableByteChannel)rbc;
            long remainder = Math.max(0, sbc.size() - sbc.position());
            return (remainder > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int)(remainder + buf.remaining());
        }
        return buf.remaining();
    }

    @Override
    public void reset() throws IOException
    {
        throw new IOException("mark/reset not supported");
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }
}
