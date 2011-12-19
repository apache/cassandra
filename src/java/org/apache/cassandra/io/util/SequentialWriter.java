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

import java.io.*;
import java.nio.channels.ClosedChannelException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.cassandra.utils.CLibrary;

public class SequentialWriter extends OutputStream
{
    // isDirty - true if this.buffer contains any un-synced bytes
    protected boolean isDirty = false, syncNeeded = false;

    // absolute path to the given file
    private final String filePath;

    // so we can use the write(int) path w/o tons of new byte[] allocations
    private final byte[] singleByteBuffer = new byte[1];

    protected byte[] buffer;
    private final boolean skipIOCache;
    private final int fd;

    protected long current = 0, bufferOffset;
    protected int validBufferBytes;

    protected final RandomAccessFile out;

    // used if skip I/O cache was enabled
    private long ioCacheStartOffset = 0, bytesSinceCacheFlush = 0;

    public final DataOutputStream stream;
    private MessageDigest digest;

    public SequentialWriter(File file, int bufferSize, boolean skipIOCache) throws IOException
    {
        out = new RandomAccessFile(file, "rw");

        filePath = file.getAbsolutePath();

        buffer = new byte[bufferSize];
        this.skipIOCache = skipIOCache;
        fd = CLibrary.getfd(out.getFD());
        stream = new DataOutputStream(this);
    }

    public static SequentialWriter open(File file) throws IOException
    {
        return open(file, RandomAccessReader.DEFAULT_BUFFER_SIZE, false);
    }

    public static SequentialWriter open(File file, boolean skipIOCache) throws IOException
    {
        return open(file, RandomAccessReader.DEFAULT_BUFFER_SIZE, skipIOCache);
    }

    public static SequentialWriter open(File file, int bufferSize, boolean skipIOCache) throws IOException
    {
        return new SequentialWriter(file, bufferSize, skipIOCache);
    }

    public void write(int value) throws IOException
    {
        singleByteBuffer[0] = (byte) value;
        write(singleByteBuffer, 0, 1);
    }

    public void write(byte[] buffer) throws IOException
    {
        write(buffer, 0, buffer.length);
    }

    public void write(byte[] data, int offset, int length) throws IOException
    {
        if (buffer == null)
            throw new ClosedChannelException();

        while (length > 0)
        {
            int n = writeAtMost(data, offset, length);
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
    private int writeAtMost(byte[] data, int offset, int length) throws IOException
    {
        if (current >= bufferOffset + buffer.length)
            reBuffer();

        assert current < bufferOffset + buffer.length
                : String.format("File (%s) offset %d, buffer offset %d.", getPath(), current, bufferOffset);


        int toCopy = Math.min(length, buffer.length - bufferCursor());

        // copy bytes from external buffer
        System.arraycopy(data, offset, buffer, bufferCursor(), toCopy);

        assert current <= bufferOffset + buffer.length
                : String.format("File (%s) offset %d, buffer offset %d.", getPath(), current, bufferOffset);

        validBufferBytes = Math.max(validBufferBytes, bufferCursor() + toCopy);
        current += toCopy;

        return toCopy;
    }

    /**
     * Synchronize file contents with disk.
     * @throws java.io.IOException on any I/O error.
     */
    public void sync() throws IOException
    {
        syncInternal();
    }

    protected void syncInternal() throws IOException
    {
        if (syncNeeded)
        {
            flushInternal();
            out.getFD().sync();

            syncNeeded = false;
        }
    }

    /**
     * If buffer is dirty, flush it's contents to the operating system. Does not imply fsync().
     *
     * Currently, for implementation reasons, this also invalidates the buffer.
     *
     * @throws java.io.IOException on any I/O error.
     */
    @Override
    public void flush() throws IOException
    {
        flushInternal();
    }

    protected void flushInternal() throws IOException
    {
        if (isDirty)
        {
            flushData();

            if (skipIOCache)
            {
                // we don't know when the data reaches disk since we aren't
                // calling flush
                // so we continue to clear pages we don't need from the first
                // offset we see
                // periodically we update this starting offset
                bytesSinceCacheFlush += validBufferBytes;

                if (bytesSinceCacheFlush >= RandomAccessReader.MAX_BYTES_IN_PAGE_CACHE)
                {
                    CLibrary.trySkipCache(this.fd, ioCacheStartOffset, 0);
                    ioCacheStartOffset = bufferOffset;
                    bytesSinceCacheFlush = 0;
                }
            }

            // Remember that we wrote, so we don't write it again on next flush().
            resetBuffer();

            isDirty = false;
        }
    }

    /**
     * Override this method instead of overriding flush()
     * @throws IOException on any I/O error.
     */
    protected void flushData() throws IOException
    {
        out.write(buffer, 0, validBufferBytes);
        if (digest != null)
            digest.update(buffer, 0, validBufferBytes);
    }

    public long getFilePointer()
    {
        return current;
    }

    public long length() throws IOException
    {
        return Math.max(Math.max(current, out.length()), bufferOffset + validBufferBytes);
    }

    public String getPath()
    {
        return filePath;
    }

    protected void reBuffer() throws IOException
    {
        flushInternal();
        resetBuffer();
    }

    protected void resetBuffer()
    {
        bufferOffset = current;
        validBufferBytes = 0;
    }

    private int bufferCursor()
    {
        return (int) (current - bufferOffset);
    }

    public FileMark mark()
    {
        return new BufferedFileWriterMark(current);
    }

    public void resetAndTruncate(FileMark mark) throws IOException
    {
        assert mark instanceof BufferedFileWriterMark;

        long previous = current;
        current = ((BufferedFileWriterMark) mark).pointer;

        if (previous - current <= validBufferBytes) // current buffer
        {
            validBufferBytes = validBufferBytes - ((int) (previous - current));
            return;
        }

        // synchronize current buffer with disk
        // because we don't want any data loss
        syncInternal();

        // truncate file to given position
        truncate(current);

        // reset channel position
        out.seek(current);

        resetBuffer();
    }

    public void truncate(long toSize) throws IOException
    {
        out.getChannel().truncate(toSize);
    }

    @Override
    public void close() throws IOException
    {
        if (buffer == null)
            return; // already closed

        syncInternal();

        buffer = null;

        if (skipIOCache && bytesSinceCacheFlush > 0)
            CLibrary.trySkipCache(fd, 0, 0);

        out.close();
    }

    /**
     * Turn on digest computation on this writer.
     * This can only be called before any data is written to this write,
     * otherwise an IllegalStateException is thrown.
     */
    public void setComputeDigest()
    {
        if (current != 0)
            throw new IllegalStateException();

        try
        {
            digest = MessageDigest.getInstance("SHA-1");
        }
        catch (NoSuchAlgorithmException e)
        {
            // SHA-1 is standard in java 6
            throw new RuntimeException(e);
        }
    }

    /**
     * Return the digest associated to this file or null if no digest was
     * created.
     * This can only be called once the file is fully created, i.e. after
     * close() has been called. Otherwise an IllegalStateException is thrown.
     */
    public byte[] digest()
    {
        if (buffer != null)
            throw new IllegalStateException();

        return digest == null ? null : digest.digest();
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class BufferedFileWriterMark implements FileMark
    {
        long pointer;

        public BufferedFileWriterMark(long pointer)
        {
            this.pointer = pointer;
        }
    }
}
