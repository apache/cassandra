/**
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.cassandra.utils.CLibrary;

/**
 * A <code>BufferedRandomAccessFile</code> is like a
 * <code>RandomAccessFile</code>, but it uses a private buffer so that most
 * operations do not require a disk access.
 * <P>
 * 
 * Note: The operations on this class are unmonitored. Also, the correct
 * functioning of the <code>RandomAccessFile</code> methods that are not
 * overridden here relies on the implementation of those methods in the
 * superclass.
 */
public class BufferedRandomAccessFile extends RandomAccessFile implements FileDataInput
{
    private static final long MAX_BYTES_IN_PAGE_CACHE = (long) Math.pow(2, 27); // 128mb
    
    // absolute filesystem path to the file
    private final String filePath;

    // default buffer size, 64Kb
    public static final int DEFAULT_BUFFER_SIZE = 65535;

    // isDirty - true if this.buffer contains any un-synced bytes
    // hitEOF - true if buffer capacity is less then it's maximal size
    private boolean isDirty, syncNeeded, hitEOF = false;

    // buffer which will cache file blocks
    private ByteBuffer buffer;

    // `current` as current position in file
    // `bufferOffset` is the offset of the beginning of the buffer
    // `bufferEnd` is `bufferOffset` + count of bytes read from file, i.e. the lowest position we can't read from the buffer
    private long bufferOffset, bufferEnd, current = 0;

    // constant, used for caching purpose, -1 if file is open in "rw" mode
    // otherwise this will hold cached file length
    private final long fileLength;

    // channel liked with the file, used to retrieve data and force updates.
    private final FileChannel channel;

    private long markedPointer;

    // file descriptor
    private int fd;

    // skip cache - used for commit log and sstable writing w/ posix_fadvise
    private final boolean skipCache;

    private long bytesSinceCacheFlush = 0;
    private long minBufferOffset = Long.MAX_VALUE;

    /*
     * Open a new <code>BufferedRandomAccessFile</code> on the file named
     * <code>name</code> in mode <code>mode</code>, which should be "r" for
     * reading only, or "rw" for reading and writing.
     */
    public BufferedRandomAccessFile(String name, String mode) throws IOException
    {
        this(new File(name), mode, 0);
    }

    public BufferedRandomAccessFile(String name, String mode, int bufferSize) throws IOException
    {
        this(new File(name), mode, bufferSize);
    }

    /*
     * Open a new <code>BufferedRandomAccessFile</code> on <code>file</code> in
     * mode <code>mode</code>, which should be "r" for reading only, or "rw" for
     * reading and writing.
     */
    public BufferedRandomAccessFile(File file, String mode) throws IOException
    {
        this(file, mode, 0);
    }

    public BufferedRandomAccessFile(File file, String mode, int bufferSize) throws IOException
    {
        this(file, mode, bufferSize, false);
    }

    public BufferedRandomAccessFile(File file, String mode, int bufferSize, boolean skipCache) throws IOException
    {
        super(file, mode);

        this.skipCache = skipCache;

        channel = super.getChannel();
        filePath = file.getAbsolutePath();

        // allocating required size of the buffer
        buffer = ByteBuffer.allocate(bufferSize);

        // if in read-only mode, caching file size
        fileLength = (mode.equals("r")) ? this.channel.size() : -1;
        bufferEnd = reBuffer(); // bufferBottom equals to the bytes read
        fd = CLibrary.getfd(this.getFD());
    }

    public void sync() throws IOException
    {
        if (syncNeeded)
        {
            flush();

            channel.force(true); // true, because file length counts as
                                 // "meta-data"

            if (skipCache)
            {
                // clear entire file from page cache
                CLibrary.trySkipCache(this.fd, 0, 0);

                minBufferOffset = Long.MAX_VALUE;
                bytesSinceCacheFlush = 0;
            }

            syncNeeded = false;
        }
    }

    public void flush() throws IOException
    {
        if (isDirty)
        {
            if (channel.position() != bufferOffset)
                channel.position(bufferOffset);

            int lengthToWrite = (int) (bufferEnd - bufferOffset);

            super.write(buffer.array(), 0, lengthToWrite);

            if (skipCache)
            {

                // we don't know when the data reaches disk since we aren't
                // calling flush
                // so we continue to clear pages we don't need from the first
                // offset we see
                // periodically we update this starting offset
                bytesSinceCacheFlush += lengthToWrite;

                if (bufferOffset < minBufferOffset)
                    minBufferOffset = bufferOffset;

                if (bytesSinceCacheFlush >= MAX_BYTES_IN_PAGE_CACHE)
                {
                    CLibrary.trySkipCache(this.fd, (int) minBufferOffset, 0);
                    minBufferOffset = bufferOffset;
                    bytesSinceCacheFlush = 0;
                }

            }

            isDirty = false;
        }
    }

    private long reBuffer() throws IOException
    {
        flush(); // synchronizing buffer and file on disk
        buffer.clear();
        bufferOffset = current;

        if (bufferOffset >= channel.size())
        {
            buffer.rewind();
            bufferEnd = bufferOffset;
            hitEOF = true;

            return 0;
        }

        if (bufferOffset < minBufferOffset)
            minBufferOffset = bufferOffset;

        channel.position(bufferOffset); // setting channel position
        long bytesRead = channel.read(buffer); // reading from that position

        hitEOF = (bytesRead < buffer.capacity()); // buffer is not fully loaded with
                                              // data
        bufferEnd = bufferOffset + bytesRead;

        buffer.rewind();

        bytesSinceCacheFlush += bytesRead;

        if (skipCache && bytesSinceCacheFlush >= MAX_BYTES_IN_PAGE_CACHE)
        {
            CLibrary.trySkipCache(this.fd, (int) minBufferOffset, 0);
            bytesSinceCacheFlush = 0;
            minBufferOffset = Long.MAX_VALUE;
        }

        return bytesRead;
    }

    @Override
    // -1 will be returned if EOF is reached, RandomAccessFile is responsible
    // for
    // throwing EOFException
    public int read() throws IOException
    {
        if (isEOF())
            return -1; // required by RandomAccessFile

        if (current < bufferOffset || current >= bufferEnd)
        {
            reBuffer();

            if (current == bufferEnd && hitEOF)
                return -1; // required by RandomAccessFile
        }

        byte result = buffer.get();
        current++;

        return ((int) result) & 0xFF;
    }

    @Override
    public int read(byte[] buffer) throws IOException
    {
        return read(buffer, 0, buffer.length);
    }

    @Override
    // -1 will be returned if EOF is reached; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read(byte[] buff, int offset, int length) throws IOException
    {
        int bytesCount = 0;

        while (length > 0)
        {
            int bytesRead = readAtMost(buff, offset, length);
            if (bytesRead == -1)
                return -1; // EOF

            offset += bytesRead;
            length -= bytesRead;
            bytesCount += bytesRead;
        }

        return bytesCount;
    }

    private int readAtMost(byte[] buff, int offset, int length) throws IOException
    {
        if (length > bufferEnd && hitEOF)
            return -1;

        final int left = buffer.capacity() - buffer.position();
        if (current < bufferOffset || left < length)
            reBuffer();

        length = Math.min(length, buffer.capacity() - buffer.position());
        buffer.get(buff, offset, length);
        current += length;

        return length;
    }

    public ByteBuffer readBytes(int length) throws IOException
    {
        assert length >= 0 : "buffer length should not be negative: " + length;

        byte[] buff = new byte[length];
        readFully(buff); // reading data buffer

        return ByteBuffer.wrap(buff);
    }

    @Override
    public void write(int val) throws IOException
    {
        byte[] b = new byte[1];
        b[0] = (byte) val;
        this.write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] buff, int offset, int length) throws IOException
    {
        while (length > 0)
        {
            int n = writeAtMost(buff, offset, length);
            offset += n;
            length -= n;
            isDirty = true;
            syncNeeded = true;
        }
    }

    /*
     * Write at most "length" bytes from "b" starting at position "offset", and
     * return the number of bytes written. caller is responsible for setting
     * isDirty.
     */
    private int writeAtMost(byte[] buff, int offset, int length) throws IOException
    {
        final int left = buffer.capacity() - buffer.position();
        if (current < bufferOffset || left < length)
            reBuffer();

        // logic is the following: we need to add bytes to the end of the buffer
        // starting from current buffer position and return this length
        length = Math.min(length, buffer.capacity() - buffer.position());

        buffer.put(buff, offset, length);
        current += length;

        if (current > bufferEnd)
            bufferEnd = current;

        return length;
    }

    @Override
    public void seek(long newPosition) throws IOException
    {
        current = newPosition;

        if (newPosition >= bufferEnd || newPosition < bufferOffset)
        {
            reBuffer(); // this will set bufferEnd for us
        }

        final int delta = (int) (newPosition - bufferOffset);
        buffer.position(delta);
    }

    @Override
    public int skipBytes(int count) throws IOException
    {
        if (count > 0)
        {
            long currentPos = getFilePointer(), eof = length();
            int newCount = (int) ((currentPos + count > eof) ? eof - currentPos : count);

            seek(currentPos + newCount);
            return newCount;
        }

        return 0;
    }

    public long length() throws IOException
    {
        return (fileLength == -1) ? Math.max(current, channel.size()) : fileLength;
    }

    public long getFilePointer()
    {
        return bufferOffset + buffer.position();
    }

    public String getPath()
    {
        return filePath;
    }

    public boolean isEOF() throws IOException
    {
        return getFilePointer() == length();
    }

    public long bytesRemaining() throws IOException
    {
        return length() - getFilePointer();
    }

    @Override
    public void close() throws IOException
    {
        sync();
        buffer = null;

        if (skipCache && bytesSinceCacheFlush > 0)
        {
            CLibrary.trySkipCache(this.fd, 0, 0);
        }

        super.close();
    }

    public void reset() throws IOException
    {
        seek(markedPointer);
    }

    public int bytesPastMark()
    {
        long bytes = getFilePointer() - markedPointer;

        assert bytes >= 0;
        if (bytes > Integer.MAX_VALUE)
            throw new UnsupportedOperationException("Overflow: " + bytes);
        return (int) bytes;
    }

    public FileMark mark()
    {
        markedPointer = getFilePointer();
        return new BufferedRandomAccessFileMark(markedPointer);
    }

    public void reset(FileMark mark) throws IOException
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        seek(((BufferedRandomAccessFileMark) mark).pointer);
    }

    public int bytesPastMark(FileMark mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        long bytes = getFilePointer() - ((BufferedRandomAccessFileMark) mark).pointer;

        assert bytes >= 0;
        if (bytes > Integer.MAX_VALUE)
            throw new UnsupportedOperationException("Overflow: " + bytes);
        return (int) bytes;
    }

    public static BufferedRandomAccessFile getUncachingReader(String filename) throws IOException
    {
        return new BufferedRandomAccessFile(new File(filename), "r", 8 * 1024 * 1024, true);
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
    public String toString() {
        return getClass().getSimpleName() + "(" +
               "filePath='" + filePath + "'" +
               ", length=" + fileLength +
               ", skipCache=" + skipCache + ")";
    }
}
