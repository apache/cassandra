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

import java.io.*;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.BufferPool;

public class RandomAccessReader extends AbstractDataInput implements FileDataInput
{
    public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    // the IO channel to the file, we do not own a reference to this due to
    // performance reasons (CASSANDRA-9379) so it's up to the owner of the RAR to
    // ensure that the channel stays open and that it is closed afterwards
    protected final ChannelProxy channel;

    // buffer which will cache file blocks
    protected ByteBuffer buffer;

    // `bufferOffset` is the offset of the beginning of the buffer
    // `markedPointer` folds the offset of the last file mark
    protected long bufferOffset, markedPointer;

    // this can be overridden at construction to a value shorter than the true length of the file;
    // if so, it acts as an imposed limit on reads, rather than a convenience property
    private final long fileLength;

    protected RandomAccessReader(ChannelProxy channel, int bufferSize, long overrideLength, BufferType bufferType)
    {
        this.channel = channel;

        if (bufferSize <= 0)
            throw new IllegalArgumentException("bufferSize must be positive");

        // we can cache file length in read-only mode
        fileLength = overrideLength <= 0 ? channel.size() : overrideLength;

        buffer = allocateBuffer(getBufferSize(bufferSize), bufferType);
        buffer.limit(0);
    }

    protected int getBufferSize(int size)
    {
        return (int)Math.min(fileLength, size);
    }

    protected ByteBuffer allocateBuffer(int size, BufferType bufferType)
    {
        return BufferPool.get(size, bufferType);
    }

    // A wrapper of the RandomAccessReader that closes the channel when done.
    // For performance reasons RAR does not increase the reference count of
    // a channel but assumes the owner will keep it open and close it,
    // see CASSANDRA-9379, this thin class is just for those cases where we do
    // not have a shared channel.
    private static class RandomAccessReaderWithChannel extends RandomAccessReader
    {
        @SuppressWarnings("resource")
        RandomAccessReaderWithChannel(File file)
        {
            super(new ChannelProxy(file), DEFAULT_BUFFER_SIZE, -1L, BufferType.OFF_HEAP);
        }

        @Override
        public void close()
        {
            try
            {
                super.close();
            }
            finally
            {
                channel.close();
            }
        }
    }

    public static RandomAccessReader open(File file)
    {
        return new RandomAccessReaderWithChannel(file);
    }

    public static RandomAccessReader open(ChannelProxy channel)
    {
        return open(channel, -1L);
    }

    public static RandomAccessReader open(ChannelProxy channel, long overrideSize)
    {
        return open(channel, DEFAULT_BUFFER_SIZE, overrideSize);
    }

    public static RandomAccessReader open(ChannelProxy channel, int bufferSize, long overrideSize)
    {
        return new RandomAccessReader(channel, bufferSize, overrideSize, BufferType.OFF_HEAP);
    }

    public ChannelProxy getChannel()
    {
        return channel;
    }

    /**
     * Read data from file starting from current currentOffset to populate buffer.
     */
    protected void reBuffer()
    {
        bufferOffset += buffer.position();
        buffer.clear();
        assert bufferOffset < fileLength;

        long position = bufferOffset;
        long limit = bufferOffset;
        while (buffer.hasRemaining() && limit < fileLength)
        {
            int n = channel.read(buffer, position);
            if (n < 0)
                break;
            position += n;
            limit = bufferOffset + buffer.position();
        }
        if (limit > fileLength)
            buffer.position((int)(fileLength - bufferOffset));
        buffer.flip();
    }

    @Override
    public long getFilePointer()
    {
        return current();
    }

    protected long current()
    {
        return bufferOffset + (buffer == null ? 0 : buffer.position());
    }

    public String getPath()
    {
        return channel.filePath();
    }

    public int getTotalBufferSize()
    {
        //This may NPE so we make a ref
        //https://issues.apache.org/jira/browse/CASSANDRA-7756
        ByteBuffer ref = buffer;
        return ref != null ? ref.capacity() : 0;
    }

    public void reset()
    {
        seek(markedPointer);
    }

    public long bytesPastMark()
    {
        long bytes = current() - markedPointer;
        assert bytes >= 0;
        return bytes;
    }

    public FileMark mark()
    {
        markedPointer = current();
        return new BufferedRandomAccessFileMark(markedPointer);
    }

    public void reset(FileMark mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        seek(((BufferedRandomAccessFileMark) mark).pointer);
    }

    public long bytesPastMark(FileMark mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        long bytes = current() - ((BufferedRandomAccessFileMark) mark).pointer;
        assert bytes >= 0;
        return bytes;
    }

    /**
     * @return true if there is no more data to read
     */
    public boolean isEOF()
    {
        return getFilePointer() == length();
    }

    public long bytesRemaining()
    {
        return length() - getFilePointer();
    }

    @Override
    public void close()
    {
	    //make idempotent
        if (buffer == null)
            return;


        bufferOffset += buffer.position();
        BufferPool.put(buffer);
        buffer = null;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" + "filePath='" + channel + "')";
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class BufferedRandomAccessFileMark implements FileMark
    {
        final long pointer;

        public BufferedRandomAccessFileMark(long pointer)
        {
            this.pointer = pointer;
        }
    }

    @Override
    public void seek(long newPosition)
    {
        if (newPosition < 0)
            throw new IllegalArgumentException("new position should not be negative");

        if (buffer == null)
            throw new IllegalStateException("Attempted to seek in a closed RAR");

        if (newPosition >= length()) // it is save to call length() in read-only mode
        {
            if (newPosition > length())
                throw new IllegalArgumentException(String.format("Unable to seek to position %d in %s (%d bytes) in read-only mode",
                                                             newPosition, getPath(), length()));
            buffer.limit(0);
            bufferOffset = newPosition;
            return;
        }

        if (newPosition >= bufferOffset && newPosition < bufferOffset + buffer.limit())
        {
            buffer.position((int) (newPosition - bufferOffset));
            return;
        }
        // Set current location to newPosition and clear buffer so reBuffer calculates from newPosition
        bufferOffset = newPosition;
        buffer.clear();
        reBuffer();
        assert current() == newPosition;
    }

    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read()
    {
        if (buffer == null)
            throw new AssertionError("Attempted to read from closed RAR");

        if (isEOF())
            return -1; // required by RandomAccessFile

        if (!buffer.hasRemaining())
            reBuffer();

        return (int)buffer.get() & 0xff;
    }

    @Override
    public int read(byte[] buffer)
    {
        return read(buffer, 0, buffer.length);
    }

    @Override
    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read(byte[] buff, int offset, int length)
    {
        if (buffer == null)
            throw new IllegalStateException("Attempted to read from closed RAR");

        if (length == 0)
            return 0;

        if (isEOF())
            return -1;

        if (!buffer.hasRemaining())
            reBuffer();

        int toCopy = Math.min(length, buffer.remaining());
        buffer.get(buff, offset, toCopy);
        return toCopy;
    }

    public ByteBuffer readBytes(int length) throws EOFException
    {
        assert length >= 0 : "buffer length should not be negative: " + length;

        if (buffer == null)
            throw new IllegalStateException("Attempted to read from closed RAR");

        try
        {
            ByteBuffer result = ByteBuffer.allocate(length);
            while (result.hasRemaining())
            {
                if (isEOF())
                    throw new EOFException();
                if (!buffer.hasRemaining())
                    reBuffer();
                ByteBufferUtil.put(buffer, result);
            }
            result.flip();
            return result;
        }
        catch (EOFException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new FSReadError(e, channel.toString());
        }
    }

    public long length()
    {
        return fileLength;
    }

    public long getPosition()
    {
        return bufferOffset + (buffer == null ? 0 : buffer.position());
    }

    public long getPositionLimit()
    {
        return length();
    }
}
