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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CLibrary;

/**
 * Adds buffering, mark, and fsyncing to OutputStream.  We always fsync on close; we may also
 * fsync incrementally if Config.trickle_fsync is enabled.
 */
public class SequentialWriter extends OutputStream implements WritableByteChannel
{
    private static final Logger logger = LoggerFactory.getLogger(SequentialWriter.class);

    // isDirty - true if this.buffer contains any un-synced bytes
    protected boolean isDirty = false, syncNeeded = false;

    // absolute path to the given file
    private final String filePath;

    protected byte[] buffer;
    private final int fd;
    private int directoryFD;
    // directory should be synced only after first file sync, in other words, only once per file
    private boolean directorySynced = false;

    protected long current = 0, bufferOffset;
    protected int validBufferBytes;

    protected final RandomAccessFile out;

    // whether to do trickling fsync() to avoid sudden bursts of dirty buffer flushing by kernel causing read
    // latency spikes
    private boolean trickleFsync;
    private int trickleFsyncByteInterval;
    private int bytesSinceTrickleFsync = 0;

    public final DataOutputPlus stream;
    protected long lastFlushOffset;

    protected Runnable runPostFlush;

    public SequentialWriter(File file, int bufferSize)
    {
        try
        {
            out = new RandomAccessFile(file, "rw");
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }

        filePath = file.getAbsolutePath();

        buffer = new byte[bufferSize];
        this.trickleFsync = DatabaseDescriptor.getTrickleFsync();
        this.trickleFsyncByteInterval = DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024;

        try
        {
            fd = CLibrary.getfd(out.getFD());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e); // shouldn't happen
        }

        directoryFD = CLibrary.tryOpenDirectory(file.getParent());
        stream = new DataOutputStreamAndChannel(this, this);
    }

    public static SequentialWriter open(File file)
    {
        return open(file, RandomAccessReader.DEFAULT_BUFFER_SIZE);
    }

    public static SequentialWriter open(File file, int bufferSize)
    {
        return new SequentialWriter(file, bufferSize);
    }

    public static ChecksummedSequentialWriter open(File file, File crcPath)
    {
        return new ChecksummedSequentialWriter(file, RandomAccessReader.DEFAULT_BUFFER_SIZE, crcPath);
    }

    public static CompressedSequentialWriter open(String dataFilePath,
                                                  String offsetsPath,
                                                  CompressionParameters parameters,
                                                  MetadataCollector sstableMetadataCollector)
    {
        return new CompressedSequentialWriter(new File(dataFilePath), offsetsPath, parameters, sstableMetadataCollector);
    }

    public void write(int value) throws ClosedChannelException
    {
        if (current >= bufferOffset + buffer.length)
            reBuffer();

        assert current < bufferOffset + buffer.length
                : String.format("File (%s) offset %d, buffer offset %d.", getPath(), current, bufferOffset);

        buffer[bufferCursor()] = (byte) value;

        validBufferBytes += 1;
        current += 1;
        isDirty = true;
        syncNeeded = true;
    }

    public void write(byte[] buffer) throws ClosedChannelException
    {
        write(buffer, 0, buffer.length);
    }

    public void write(byte[] data, int offset, int length) throws ClosedChannelException
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

    public int write(ByteBuffer src) throws IOException
    {
        if (buffer == null)
            throw new ClosedChannelException();

        int length = src.remaining();
        int offset = src.position();
        while (length > 0)
        {
            int n = writeAtMost(src, offset, length);
            offset += n;
            length -= n;
            isDirty = true;
            syncNeeded = true;
        }
        src.position(offset);
        return length;
    }

    /*
     * Write at most "length" bytes from "data" starting at position "offset", and
     * return the number of bytes written. caller is responsible for setting
     * isDirty.
     */
    private int writeAtMost(ByteBuffer data, int offset, int length)
    {
        if (current >= bufferOffset + buffer.length)
            reBuffer();

        assert current < bufferOffset + buffer.length
        : String.format("File (%s) offset %d, buffer offset %d.", getPath(), current, bufferOffset);


        int toCopy = Math.min(length, buffer.length - bufferCursor());

        // copy bytes from external buffer
        ByteBufferUtil.arrayCopy(data, offset, buffer, bufferCursor(), toCopy);

        assert current <= bufferOffset + buffer.length
        : String.format("File (%s) offset %d, buffer offset %d.", getPath(), current, bufferOffset);

        validBufferBytes = Math.max(validBufferBytes, bufferCursor() + toCopy);
        current += toCopy;

        return toCopy;
    }

    /*
     * Write at most "length" bytes from "data" starting at position "offset", and
     * return the number of bytes written. caller is responsible for setting
     * isDirty.
     */
    private int writeAtMost(byte[] data, int offset, int length)
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
     */
    public void sync()
    {
        syncInternal();
    }

    protected void syncDataOnlyInternal()
    {
        try
        {
            out.getFD().sync();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    protected void syncInternal()
    {
        if (syncNeeded)
        {
            flushInternal();
            syncDataOnlyInternal();

            if (!directorySynced)
            {
                CLibrary.trySync(directoryFD);
                directorySynced = true;
            }

            syncNeeded = false;
        }
    }

    /**
     * If buffer is dirty, flush it's contents to the operating system. Does not imply fsync().
     *
     * Currently, for implementation reasons, this also invalidates the buffer.
     */
    @Override
    public void flush()
    {
        flushInternal();
    }

    protected void flushInternal()
    {
        if (isDirty)
        {
            flushData();

            if (trickleFsync)
            {
                bytesSinceTrickleFsync += validBufferBytes;
                if (bytesSinceTrickleFsync >= trickleFsyncByteInterval)
                {
                    syncDataOnlyInternal();
                    bytesSinceTrickleFsync = 0;
                }
            }

            // Remember that we wrote, so we don't write it again on next flush().
            resetBuffer();

            isDirty = false;
        }
    }

    public void setPostFlushListener(Runnable runPostFlush)
    {
        assert this.runPostFlush == null;
        this.runPostFlush = runPostFlush;
    }

    /**
     * Override this method instead of overriding flush()
     * @throws FSWriteError on any I/O error.
     */
    protected void flushData()
    {
        try
        {
            out.write(buffer, 0, validBufferBytes);
            lastFlushOffset += validBufferBytes;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
        if (runPostFlush != null)
            runPostFlush.run();
    }

    public long getFilePointer()
    {
        return current;
    }

    /**
     * Return the current file pointer of the underlying on-disk file.
     * Note that since write works by buffering data, the value of this will increase by buffer
     * size and not every write to the writer will modify this value.
     * Furthermore, for compressed files, this value refers to compressed data, while the
     * writer getFilePointer() refers to uncompressedFile
     */
    public long getOnDiskFilePointer()
    {
        return getFilePointer();
    }

    public long length()
    {
        try
        {
            return Math.max(Math.max(current, out.length()), bufferOffset + validBufferBytes);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }
    }

    public String getPath()
    {
        return filePath;
    }

    protected void reBuffer()
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

    public void resetAndTruncate(FileMark mark)
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
        try
        {
            out.seek(current);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }

        resetBuffer();
    }

    public long getLastFlushOffset()
    {
        return lastFlushOffset;
    }

    public void truncate(long toSize)
    {
        try
        {
            out.getChannel().truncate(toSize);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    public boolean isOpen()
    {
        return out.getChannel().isOpen();
    }

    @Override
    public void close()
    {
        if (buffer == null)
            return; // already closed

        syncInternal();

        buffer = null;

        cleanup(true);
    }

    public void abort()
    {
        cleanup(false);
    }

    private void cleanup(boolean throwExceptions)
    {
        if (directoryFD >= 0)
        {
            try { CLibrary.tryCloseFD(directoryFD); }
            catch (Throwable t) { handle(t, throwExceptions); }
            directoryFD = -1;
        }

        // close is idempotent
        try { out.close(); }
        catch (Throwable t) { handle(t, throwExceptions); }
    }

    private void handle(Throwable t, boolean throwExceptions)
    {
        if (!throwExceptions)
            logger.warn("Suppressing exception thrown while aborting writer", t);
        else
            throw new FSWriteError(t, getPath());
    }

    // hack to make life easier for subclasses
    public void writeFullChecksum(Descriptor descriptor)
    {
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class BufferedFileWriterMark implements FileMark
    {
        final long pointer;

        public BufferedFileWriterMark(long pointer)
        {
            this.pointer = pointer;
        }
    }
}
