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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
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
    private boolean isDirty, syncNeeded;

    // buffer which will cache file blocks
    private byte[] buffer;

    // `current` as current position in file
    // `bufferOffset` is the offset of the beginning of the buffer
    // `validBufferBytes` is the number of bytes in the buffer that are actually valid; this will be LESS than buffer capacity if buffer is not full!
    private long bufferOffset, current = 0;
    private int validBufferBytes = 0;

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
        this(new File(name), mode, DEFAULT_BUFFER_SIZE);
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
        this(file, mode, DEFAULT_BUFFER_SIZE);
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
        if (bufferSize <= 0)
            throw new IllegalArgumentException("bufferSize must be positive");
        buffer = new byte[bufferSize];
        reBuffer();

        // if in read-only mode, caching file size
        fileLength = (mode.equals("r")) ? this.channel.size() : -1;
        fd = CLibrary.getfd(this.getFD());
    }

    /**
     * Flush (flush()) whatever writes are pending, and block until the data has been persistently committed (fsync()).
     */
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

    /**
     * If we are dirty, flush dirty contents to the operating system. Does not imply fsync().
     *
     * Currently, for implementation reasons, this also invalidates the buffer.
     */
    public void flush() throws IOException
    {
        if (isDirty)
        {
            if (channel.position() != bufferOffset)
                channel.position(bufferOffset);

            super.write(buffer, 0, validBufferBytes);

            if (skipCache)
            {

                // we don't know when the data reaches disk since we aren't
                // calling flush
                // so we continue to clear pages we don't need from the first
                // offset we see
                // periodically we update this starting offset
                bytesSinceCacheFlush += validBufferBytes;

                if (bufferOffset < minBufferOffset)
                    minBufferOffset = bufferOffset;

                if (bytesSinceCacheFlush >= MAX_BYTES_IN_PAGE_CACHE)
                {
                    CLibrary.trySkipCache(this.fd, (int) minBufferOffset, 0);
                    minBufferOffset = bufferOffset;
                    bytesSinceCacheFlush = 0;
                }

            }

            // Remember that we wrote, so we don't write it again on next flush().
            resetBuffer();

            isDirty = false;
        }
    }

    @Override
    public void setLength(long newLength) throws IOException
    {
        if (newLength < 0)
            throw new IllegalArgumentException();

        // account for dirty data in buffers
        if (isDirty)
        {
            if (newLength < bufferOffset)
            {
                // buffer is garbage
                validBufferBytes = 0;
            }
            else if (newLength > (bufferOffset + validBufferBytes))
            {
                // flush everything in buffer
                flush();
            }
            else // buffer within range
            {
                // truncate buffer and flush
                validBufferBytes = (int)(newLength - bufferOffset);
                flush();
            }
        }

        // at this point all dirty buffer data is flushed
        super.setLength(newLength);

        validBufferBytes = 0;
        current = newLength;
        reBuffer();
    }

    private void resetBuffer()
    {
        bufferOffset = current;
        validBufferBytes = 0;
    }

    private void reBuffer() throws IOException
    {
        flush(); // synchronizing buffer and file on disk
        resetBuffer();
        if (bufferOffset >= channel.size())
            return;

        if (bufferOffset < minBufferOffset)
            minBufferOffset = bufferOffset;

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
        if (skipCache && bytesSinceCacheFlush >= MAX_BYTES_IN_PAGE_CACHE)
        {
            CLibrary.trySkipCache(this.fd, (int) minBufferOffset, 0);
            bytesSinceCacheFlush = 0;
            minBufferOffset = Long.MAX_VALUE;
        }
    }

    @Override
    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read() throws IOException
    {
        if (isEOF())
            return -1; // required by RandomAccessFile

        if (current >= bufferOffset + buffer.length)
            reBuffer();
        assert current >= bufferOffset && current < bufferOffset + validBufferBytes;

        return ((int) buffer[(int) (current++ - bufferOffset)]) & 0xFF;
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
        if (length == 0)
            return 0;

        if (isEOF())
            return -1;

        if (current >= bufferOffset + buffer.length)
            reBuffer();
        assert current >= bufferOffset && current < bufferOffset + validBufferBytes;

        int toCopy = Math.min(length, validBufferBytes - (int) (current - bufferOffset));
        System.arraycopy(buffer, (int) (current - bufferOffset), buff, offset, toCopy);
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

    private final byte[] singleByteBuffer = new byte[1]; // so we can use the write(byte[]) path w/o tons of new byte[] allocations
    @Override
    public void write(int val) throws IOException
    {
        singleByteBuffer[0] = (byte) val;
        this.write(singleByteBuffer, 0, 1);
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] buff, int offset, int length) throws IOException
    {
        if (buffer == null)
            throw new ClosedChannelException();

        if (isReadOnly())
            throw new IOException("Unable to write: file is in the read-only mode.");

        while (length > 0)
        {
            int n = writeAtMost(buff, offset, length);
            offset += n;
            length -= n;
            isDirty = true;
            syncNeeded = true;
        }
    }

    private boolean isReadOnly()
    {
        return fileLength != -1;
    }

    /*
     * Write at most "length" bytes from "b" starting at position "offset", and
     * return the number of bytes written. caller is responsible for setting
     * isDirty.
     */
    private int writeAtMost(byte[] buff, int offset, int length) throws IOException
    {
        if (current >= bufferOffset + buffer.length)
            reBuffer();
        assert current < bufferOffset + buffer.length;

        int positionWithinBuffer = (int) (current - bufferOffset);
        int toCopy = Math.min(length, buffer.length - positionWithinBuffer);
        System.arraycopy(buff, offset, buffer, positionWithinBuffer, toCopy);
        current += toCopy;
        validBufferBytes = Math.max(validBufferBytes, positionWithinBuffer + toCopy);
        assert current <= bufferOffset + buffer.length;

        return toCopy;
    }

    @Override
    public void seek(long newPosition) throws IOException
    {
        if (newPosition < 0)
            throw new IllegalArgumentException("new position should not be negative");

        if (isReadOnly() && newPosition > fileLength)
            throw new EOFException(String.format("unable to seek to position %d in %s (%d bytes) in read-only mode",
                                                 newPosition, filePath, fileLength));

        current = newPosition;

        if (newPosition > (bufferOffset + validBufferBytes) || newPosition < bufferOffset)
            reBuffer(); // this will set bufferEnd for us
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
        return (fileLength == -1) ? Math.max(Math.max(current, channel.size()), bufferOffset + validBufferBytes) : fileLength;
    }

    public long getFilePointer()
    {
        return current;
    }

    public String getPath()
    {
        return filePath;
    }

    /**
     * @return true if there is no more data to read
     */
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

    public long bytesPastMark()
    {
        long bytes = getFilePointer() - markedPointer;
        assert bytes >= 0;
        return bytes;
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

    public long bytesPastMark(FileMark mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        long bytes = getFilePointer() - ((BufferedRandomAccessFileMark) mark).pointer;
        assert bytes >= 0;
        return bytes;
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
