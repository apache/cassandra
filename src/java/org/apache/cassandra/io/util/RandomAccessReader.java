/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.io.util;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

import org.apache.cassandra.utils.CLibrary;

public class RandomAccessReader extends RandomAccessFile implements FileDataInput
{
    public static final long MAX_BYTES_IN_PAGE_CACHE = (long) Math.pow(2, 27); // 128mb

    // default buffer size, 64Kb
    public static final int DEFAULT_BUFFER_SIZE = 65536;

    // absolute filesystem path to the file
    private final String filePath;

    // buffer which will cache file blocks
    protected byte[] buffer;

    // `current` as current position in file
    // `bufferOffset` is the offset of the beginning of the buffer
    // `markedPointer` folds the offset of the last file mark
    protected long bufferOffset, current = 0, markedPointer;
    // `validBufferBytes` is the number of bytes in the buffer that are actually valid;
    //  this will be LESS than buffer capacity if buffer is not full!
    protected int validBufferBytes = 0;

    // channel liked with the file, used to retrieve data and force updates.
    protected final FileChannel channel;

    private final boolean skipIOCache;

    // file descriptor
    private final int fd;

    // used if skip I/O cache was enabled
    private long bytesSinceCacheFlush = 0;

    private final long fileLength;

    public RandomAccessReader(File file, int bufferSize, boolean skipIOCache) throws IOException
    {
        super(file, "r");

        channel = super.getChannel();
        filePath = file.getAbsolutePath();

        // allocating required size of the buffer
        if (bufferSize <= 0)
            throw new IllegalArgumentException("bufferSize must be positive");
        buffer = new byte[bufferSize];

        this.skipIOCache = skipIOCache;
        fd = CLibrary.getfd(getFD());

        // we can cache file length in read-only mode
        fileLength = channel.size();
        validBufferBytes = -1; // that will trigger reBuffer() on demand by read/seek operations
    }

    public static RandomAccessReader open(File file, boolean skipIOCache) throws IOException
    {
        return open(file, DEFAULT_BUFFER_SIZE, skipIOCache);
    }

    public static RandomAccessReader open(File file) throws IOException
    {
        return open(file, DEFAULT_BUFFER_SIZE, false);
    }

    public static RandomAccessReader open(File file, int bufferSize) throws IOException
    {
        return open(file, bufferSize, false);
    }

    public static RandomAccessReader open(File file, int bufferSize, boolean skipIOCache) throws IOException
    {
        return new RandomAccessReader(file, bufferSize, skipIOCache);
    }

    // convert open into open
    public static RandomAccessReader open(SequentialWriter writer) throws IOException
    {
        return open(new File(writer.getPath()), DEFAULT_BUFFER_SIZE);
    }

    /**
     * Read data from file starting from current currentOffset to populate buffer.
     * @throws IOException on any I/O error.
     */
    protected void reBuffer() throws IOException
    {
        resetBuffer();

        if (bufferOffset >= channel.size())
            return;

        channel.position(bufferOffset); // setting channel position

        int read = 0;

        while (read < buffer.length)
        {
            int n = super.read(buffer, read, buffer.length - read);
            if (n < 0)
                break;
            read += n;
        }

        validBufferBytes = read;

        bytesSinceCacheFlush += read;

        if (skipIOCache && bytesSinceCacheFlush >= MAX_BYTES_IN_PAGE_CACHE)
        {
            // with random I/O we can't control what we are skipping so
            // it will be more appropriate to just skip a whole file after
            // we reach threshold
            CLibrary.trySkipCache(this.fd, 0, 0);
            bytesSinceCacheFlush = 0;
        }
    }

    @Override
    public long getFilePointer()
    {
        return current;
    }

    public String getPath()
    {
        return filePath;
    }

    public void reset() throws IOException
    {
        seek(markedPointer);
    }

    public long bytesPastMark()
    {
        long bytes = current - markedPointer;
        assert bytes >= 0;
        return bytes;
    }

    public FileMark mark()
    {
        markedPointer = current;
        return new BufferedRandomAccessFileMark(markedPointer);
    }

    public void reset(FileMark mark) throws IOException
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        seek(((BufferedRandomAccessFileMark) mark).pointer);
    }

    public long bytesPastMark(FileMark mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        long bytes = current - ((BufferedRandomAccessFileMark) mark).pointer;
        assert bytes >= 0;
        return bytes;
    }

    /**
     * @return true if there is no more data to read
     * @throws IOException on any I/O error.
     */
    public boolean isEOF() throws IOException
    {
        return getFilePointer() == length();
    }

    public long bytesRemaining() throws IOException
    {
        return length() - getFilePointer();
    }

    protected int bufferCursor()
    {
        return (int) (current - bufferOffset);
    }

    protected void resetBuffer()
    {
        bufferOffset = current;
        validBufferBytes = 0;
    }

    @Override
    public void close() throws IOException
    {
        buffer = null;

        if (skipIOCache && bytesSinceCacheFlush > 0)
            CLibrary.trySkipCache(fd, 0, 0);

        super.close();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" + "filePath='" + filePath + "'" + ", skipIOCache=" + skipIOCache + ")";
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class BufferedRandomAccessFileMark implements FileMark
    {
        long pointer;

        public BufferedRandomAccessFileMark(long pointer)
        {
            this.pointer = pointer;
        }
    }

    @Override
    public void seek(long newPosition) throws IOException
    {
        if (newPosition < 0)
            throw new IllegalArgumentException("new position should not be negative");

        if (newPosition > length()) // it is save to call length() in read-only mode
            throw new EOFException(String.format("unable to seek to position %d in %s (%d bytes) in read-only mode",
                                                 newPosition, getPath(), length()));

        current = newPosition;

        if (newPosition > (bufferOffset + validBufferBytes) || newPosition < bufferOffset)
            reBuffer();
    }

    @Override
    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read() throws IOException
    {
        if (buffer == null)
            throw new ClosedChannelException();

        if (isEOF())
            return -1; // required by RandomAccessFile

        if (current >= bufferOffset + buffer.length || validBufferBytes == -1)
            reBuffer();

        assert current >= bufferOffset && current < bufferOffset + validBufferBytes;

        return ((int) buffer[(int) (current++ - bufferOffset)]) & 0xff;
    }

    @Override
    public int read(byte[] buffer) throws IOException
    {
        return read(buffer, 0, buffer.length);
    }

    @Override
    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read(byte[] buff, int offset, int length) throws IOException
    {
        if (buffer == null)
            throw new ClosedChannelException();

        if (length == 0)
            return 0;

        if (isEOF())
            return -1;

        if (current >= bufferOffset + buffer.length || validBufferBytes == -1)
            reBuffer();

        assert current >= bufferOffset && current < bufferOffset + validBufferBytes
                : String.format("File (%s), current offset %d, buffer offset %d, buffer limit %d",
                                getPath(),
                                current,
                                bufferOffset,
                                validBufferBytes);

        int toCopy = Math.min(length, validBufferBytes - bufferCursor());

        System.arraycopy(buffer, bufferCursor(), buff, offset, toCopy);
        current += toCopy;

        return toCopy;
    }

    public ByteBuffer readBytes(int length) throws IOException
    {
        assert length >= 0 : "buffer length should not be negative: " + length;

        byte[] buff = new byte[length];
        readFully(buff); // reading data buffer

        return ByteBuffer.wrap(buff);
    }

    @Override
    public long length() throws IOException
    {
        return fileLength;
    }

    @Override
    public void write(int value) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] buffer) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] buffer, int offset, int length) throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
