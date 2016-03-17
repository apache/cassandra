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
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

/**
 * Adds mark/reset functionality to another input stream by caching read bytes to a memory buffer and
 * spilling to disk if necessary.
 *
 * When the stream is marked via {@link this#mark()} or {@link this#mark(int)}, up to
 * <code>maxMemBufferSize</code> will be cached in memory (heap). If more than
 * <code>maxMemBufferSize</code> bytes are read while the stream is marked, the
 * following bytes are cached on the <code>spillFile</code> for up to <code>maxDiskBufferSize</code>.
 *
 * Please note that successive calls to {@link this#mark()} and {@link this#reset()} will write
 * sequentially to the same <code>spillFile</code> until <code>maxDiskBufferSize</code> is reached.
 * At this point, if less than <code>maxDiskBufferSize</code> bytes are currently cached on the
 * <code>spillFile</code>, the remaining bytes are written to the beginning of the file,
 * treating the <code>spillFile</code> as a circular buffer.
 *
 * If more than <code>maxMemBufferSize + maxDiskBufferSize</code> are cached while the stream is marked,
 * the following {@link this#reset()} invocation will throw a {@link IllegalStateException}.
 *
 */
public class RewindableDataInputStreamPlus extends FilterInputStream implements RewindableDataInput, Closeable
{
    private boolean marked = false;
    private boolean exhausted = false;
    private AtomicBoolean closed = new AtomicBoolean(false);

    protected int memAvailable = 0;
    protected int diskTailAvailable = 0;
    protected int diskHeadAvailable = 0;

    private final File spillFile;
    private final int initialMemBufferSize;
    private final int maxMemBufferSize;
    private final int maxDiskBufferSize;

    private volatile byte memBuffer[];
    private int memBufferSize;
    private RandomAccessFile spillBuffer;

    private final DataInputPlus dataReader;

    public RewindableDataInputStreamPlus(InputStream in, int initialMemBufferSize, int maxMemBufferSize,
                                         File spillFile, int maxDiskBufferSize)
    {
        super(in);
        dataReader = new DataInputStreamPlus(this);
        this.initialMemBufferSize = initialMemBufferSize;
        this.maxMemBufferSize = maxMemBufferSize;
        this.spillFile = spillFile;
        this.maxDiskBufferSize = maxDiskBufferSize;
    }

    /* RewindableDataInput methods */

    /**
     * Marks the current position of a stream to return to this position later via the {@link this#reset(DataPosition)} method.
     * @return An empty @link{DataPosition} object
     */
    public DataPosition mark()
    {
        mark(0);
        return new RewindableDataInputPlusMark();
    }

    /**
     * Rewinds to the previously marked position via the {@link this#mark()} method.
     * @param mark it's not possible to return to a custom position, so this parameter is ignored.
     * @throws IOException if an error ocurs while resetting
     */
    public void reset(DataPosition mark) throws IOException
    {
        reset();
    }

    public long bytesPastMark(DataPosition mark)
    {
        return maxMemBufferSize - memAvailable + (diskTailAvailable == -1? 0 : maxDiskBufferSize - diskHeadAvailable - diskTailAvailable);
    }


    protected static class RewindableDataInputPlusMark implements DataPosition
    {
    }

    /* InputStream methods */

    public boolean markSupported()
    {
        return true;
    }

    /**
     * Marks the current position of a stream to return to this position
     * later via the {@link this#reset()} method.
     * @param readlimit the maximum amount of bytes to cache
     */
    public synchronized void mark(int readlimit)
    {
        if (marked)
            throw new IllegalStateException("Cannot mark already marked stream.");

        if (memAvailable > 0 || diskHeadAvailable > 0 || diskTailAvailable > 0)
            throw new IllegalStateException("Can only mark stream after reading previously marked data.");

        marked = true;
        memAvailable = maxMemBufferSize;
        diskHeadAvailable = -1;
        diskTailAvailable = -1;
    }

    public synchronized void reset() throws IOException
    {
        if (!marked)
            throw new IOException("Must call mark() before calling reset().");

        if (exhausted)
            throw new IOException(String.format("Read more than capacity: %d bytes.", maxMemBufferSize + maxDiskBufferSize));

        memAvailable = maxMemBufferSize - memAvailable;
        memBufferSize = memAvailable;

        if (diskTailAvailable == -1)
        {
            diskHeadAvailable = 0;
            diskTailAvailable = 0;
        }
        else
        {
            int initialPos = diskTailAvailable > 0 ? 0 : (int)getIfNotClosed(spillBuffer).getFilePointer();
            int diskMarkpos = initialPos + diskHeadAvailable;
            getIfNotClosed(spillBuffer).seek(diskMarkpos);

            diskHeadAvailable = diskMarkpos - diskHeadAvailable;
            diskTailAvailable = (maxDiskBufferSize - diskTailAvailable) - diskMarkpos;
        }

        marked = false;
    }

    public int available() throws IOException
    {

        return super.available() + (marked? 0 : memAvailable + diskHeadAvailable + diskTailAvailable);
    }

    public int read() throws IOException
    {
        int read = readOne();
        if (read == -1)
            return read;

        if (marked)
        {
            //mark exhausted
            if (isExhausted(1))
            {
                exhausted = true;
                return read;
            }

            writeOne(read);
        }

        return read;
    }

    public int read(byte[] b, int off, int len) throws IOException
    {
        int readBytes = readMulti(b, off, len);
        if (readBytes == -1)
            return readBytes;

        if (marked)
        {
            //check we have space on buffer
            if (isExhausted(readBytes))
            {
                exhausted = true;
                return readBytes;
            }

            writeMulti(b, off, readBytes);
        }

        return readBytes;
    }

    private void maybeCreateDiskBuffer() throws IOException
    {
        if (spillBuffer == null)
        {
            if (!spillFile.getParentFile().exists())
                spillFile.getParentFile().mkdirs();
            spillFile.createNewFile();

            this.spillBuffer = new RandomAccessFile(spillFile, "rw");
        }
    }


    private int readOne() throws IOException
    {
        if (!marked)
        {
            if (memAvailable > 0)
            {
                int pos = memBufferSize - memAvailable;
                memAvailable--;
                return getIfNotClosed(memBuffer)[pos] & 0xff;
            }

            if (diskTailAvailable > 0 || diskHeadAvailable > 0)
            {
                int read = getIfNotClosed(spillBuffer).read();
                if (diskTailAvailable > 0)
                    diskTailAvailable--;
                else if (diskHeadAvailable > 0)
                    diskHeadAvailable++;
                if (diskTailAvailable == 0)
                    spillBuffer.seek(0);
                return read;
            }
        }

        return getIfNotClosed(in).read();
    }

    private boolean isExhausted(int readBytes)
    {
        return exhausted || readBytes > memAvailable + (long)(diskTailAvailable == -1? maxDiskBufferSize : diskTailAvailable + diskHeadAvailable);
    }

    private int readMulti(byte[] b, int off, int len) throws IOException
    {
        int readBytes = 0;
        if (!marked)
        {
            if (memAvailable > 0)
            {
                readBytes += memAvailable < len ? memAvailable : len;
                int pos = memBufferSize - memAvailable;
                System.arraycopy(memBuffer, pos, b, off, readBytes);
                memAvailable -= readBytes;
                off += readBytes;
                len -= readBytes;
            }
            if (len > 0 && diskTailAvailable > 0)
            {
                int readFromTail = diskTailAvailable < len? diskTailAvailable : len;
                getIfNotClosed(spillBuffer).read(b, off, readFromTail);
                readBytes += readFromTail;
                diskTailAvailable -= readFromTail;
                off += readFromTail;
                len -= readFromTail;
                if (diskTailAvailable == 0)
                    spillBuffer.seek(0);
            }
            if (len > 0 && diskHeadAvailable > 0)
            {
                int readFromHead = diskHeadAvailable < len? diskHeadAvailable : len;
                getIfNotClosed(spillBuffer).read(b, off, readFromHead);
                readBytes += readFromHead;
                diskHeadAvailable -= readFromHead;
                off += readFromHead;
                len -= readFromHead;
            }
        }

        if (len > 0)
            readBytes += getIfNotClosed(in).read(b, off, len);

        return readBytes;
    }

    private void writeMulti(byte[] b, int off, int len) throws IOException
    {
        if (memAvailable > 0)
        {
            if (memBuffer == null)
                memBuffer = new byte[initialMemBufferSize];
            int pos = maxMemBufferSize - memAvailable;
            int memWritten = memAvailable < len? memAvailable : len;
            if (pos + memWritten >= getIfNotClosed(memBuffer).length)
                growMemBuffer(pos, memWritten);
            System.arraycopy(b, off, memBuffer, pos, memWritten);
            off += memWritten;
            len -= memWritten;
            memAvailable -= memWritten;
        }

        if (len > 0)
        {
            if (diskTailAvailable == -1)
            {
                maybeCreateDiskBuffer();
                diskHeadAvailable = (int)spillBuffer.getFilePointer();
                diskTailAvailable = maxDiskBufferSize - diskHeadAvailable;
            }

            if (len > 0 && diskTailAvailable > 0)
            {
                int diskTailWritten = diskTailAvailable < len? diskTailAvailable : len;
                getIfNotClosed(spillBuffer).write(b, off, diskTailWritten);
                off += diskTailWritten;
                len -= diskTailWritten;
                diskTailAvailable -= diskTailWritten;
                if (diskTailAvailable == 0)
                    spillBuffer.seek(0);
            }

            if (len > 0 && diskTailAvailable > 0)
            {
                int diskHeadWritten = diskHeadAvailable < len? diskHeadAvailable : len;
                getIfNotClosed(spillBuffer).write(b, off, diskHeadWritten);
            }
        }
    }

    private void writeOne(int value) throws IOException
    {
        if (memAvailable > 0)
        {
            if (memBuffer == null)
                memBuffer = new byte[initialMemBufferSize];
            int pos = maxMemBufferSize - memAvailable;
            if (pos == getIfNotClosed(memBuffer).length)
                growMemBuffer(pos, 1);
            getIfNotClosed(memBuffer)[pos] = (byte)value;
            memAvailable--;
            return;
        }

        if (diskTailAvailable == -1)
        {
            maybeCreateDiskBuffer();
            diskHeadAvailable = (int)spillBuffer.getFilePointer();
            diskTailAvailable = maxDiskBufferSize - diskHeadAvailable;
        }

        if (diskTailAvailable > 0 || diskHeadAvailable > 0)
        {
            getIfNotClosed(spillBuffer).write(value);
            if (diskTailAvailable > 0)
                diskTailAvailable--;
            else if (diskHeadAvailable > 0)
                diskHeadAvailable--;
            if (diskTailAvailable == 0)
                spillBuffer.seek(0);
            return;
        }
    }

    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    private void growMemBuffer(int pos, int writeSize)
    {
        int newSize = Math.min(2 * (pos + writeSize), maxMemBufferSize);
        byte newBuffer[] = new byte[newSize];
        System.arraycopy(memBuffer, 0, newBuffer, 0, (int)pos);
        memBuffer = newBuffer;
    }

    public long skip(long n) throws IOException
    {
        long skipped = 0;

        if (marked)
        {
            //if marked, we need to cache skipped bytes
            while (n-- > 0 && read() != -1)
            {
                skipped++;
            }
            return skipped;
        }

        if (memAvailable > 0)
        {
            skipped += memAvailable < n ? memAvailable : n;
            memAvailable -= skipped;
            n -= skipped;
        }
        if (n > 0 && diskTailAvailable > 0)
        {
            int skipFromTail = diskTailAvailable < n? diskTailAvailable : (int)n;
            getIfNotClosed(spillBuffer).skipBytes(skipFromTail);
            diskTailAvailable -= skipFromTail;
            skipped += skipFromTail;
            n -= skipFromTail;
            if (diskTailAvailable == 0)
                spillBuffer.seek(0);
        }
        if (n > 0 && diskHeadAvailable > 0)
        {
            int skipFromHead = diskHeadAvailable < n? diskHeadAvailable : (int)n;
            getIfNotClosed(spillBuffer).skipBytes(skipFromHead);
            diskHeadAvailable -= skipFromHead;
            skipped += skipFromHead;
            n -= skipFromHead;
        }

        if (n > 0)
            skipped += getIfNotClosed(in).skip(n);

        return skipped;
    }

    private <T> T getIfNotClosed(T in) throws IOException {
        if (closed.get())
            throw new IOException("Stream closed");
        return in;
    }

    public void close() throws IOException
    {
        close(true);
    }

    public void close(boolean closeUnderlying) throws IOException
    {
        if (closed.compareAndSet(false, true))
        {
            Throwable fail = null;
            if (closeUnderlying)
            {
                try
                {
                    super.close();
                }
                catch (IOException e)
                {
                    fail = merge(fail, e);
                }
            }
            try
            {
                if (spillBuffer != null)
                {
                    this.spillBuffer.close();
                    this.spillBuffer = null;
                }
            } catch (IOException e)
            {
                fail = merge(fail, e);
            }
            try {
                if (spillFile.exists())
                {
                    spillFile.delete();
                }
            }
            catch (Throwable e)
            {
                fail = merge(fail, e);
            }
            maybeFail(fail, IOException.class);
        }
    }

    /* DataInputPlus methods */

    public void readFully(byte[] b) throws IOException
    {
        dataReader.readFully(b);
    }

    public void readFully(byte[] b, int off, int len) throws IOException
    {
        dataReader.readFully(b, off, len);
    }

    public int skipBytes(int n) throws IOException
    {
        return dataReader.skipBytes(n);
    }

    public boolean readBoolean() throws IOException
    {
        return dataReader.readBoolean();
    }

    public byte readByte() throws IOException
    {
        return dataReader.readByte();
    }

    public int readUnsignedByte() throws IOException
    {
        return dataReader.readUnsignedByte();
    }

    public short readShort() throws IOException
    {
        return dataReader.readShort();
    }

    public int readUnsignedShort() throws IOException
    {
        return dataReader.readUnsignedShort();
    }

    public char readChar() throws IOException
    {
        return dataReader.readChar();
    }

    public int readInt() throws IOException
    {
        return dataReader.readInt();
    }

    public long readLong() throws IOException
    {
        return dataReader.readLong();
    }

    public float readFloat() throws IOException
    {
        return dataReader.readFloat();
    }

    public double readDouble() throws IOException
    {
        return dataReader.readDouble();
    }

    public String readLine() throws IOException
    {
        return dataReader.readLine();
    }

    public String readUTF() throws IOException
    {
        return dataReader.readUTF();
    }
}
