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
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
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

    protected ByteBuffer buffer;
    private final int fd;
    private int directoryFD;
    // directory should be synced only after first file sync, in other words, only once per file
    private boolean directorySynced = false;

    // Offset for start of buffer relative to underlying file
    protected long bufferOffset;

    protected final FileChannel channel;

    // whether to do trickling fsync() to avoid sudden bursts of dirty buffer flushing by kernel causing read
    // latency spikes
    private boolean trickleFsync;
    private int trickleFsyncByteInterval;
    private int bytesSinceTrickleFsync = 0;

    public final DataOutputPlus stream;
    protected long lastFlushOffset;

    protected Runnable runPostFlush;

    public SequentialWriter(File file, int bufferSize, boolean offheap)
    {
        try
        {
            if (file.exists())
                channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
            else
                channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        filePath = file.getAbsolutePath();

        // Allow children to allocate buffer as direct (snappy compression) if necessary
        buffer = offheap ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);

        this.trickleFsync = DatabaseDescriptor.getTrickleFsync();
        this.trickleFsyncByteInterval = DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024;

        fd = CLibrary.getfd(channel);

        directoryFD = CLibrary.tryOpenDirectory(file.getParent());
        stream = new DataOutputStreamAndChannel(this, this);
    }

    /**
     * Open a heap-based, non-compressed SequentialWriter
     */
    public static SequentialWriter open(File file)
    {
        return new SequentialWriter(file, RandomAccessReader.DEFAULT_BUFFER_SIZE, false);
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
        if (buffer == null)
            throw new ClosedChannelException();

        if (!buffer.hasRemaining())
        {
            reBuffer();
        }

        buffer.put((byte) value);

        isDirty = true;
        syncNeeded = true;
    }

    public void write(byte[] buffer) throws IOException
    {
        write(ByteBuffer.wrap(buffer, 0, buffer.length));
    }

    public void write(byte[] data, int offset, int length) throws IOException
    {
        write(ByteBuffer.wrap(data, offset, length));
    }

    public int write(ByteBuffer src) throws IOException
    {
        if (buffer == null)
            throw new ClosedChannelException();

        int length = src.remaining();
        int finalLimit = src.limit();
        while (src.hasRemaining())
        {
            if (!buffer.hasRemaining())
                reBuffer();

            if (buffer.remaining() < src.remaining())
                src.limit(src.position() + buffer.remaining());
            buffer.put(src);
            src.limit(finalLimit);

            isDirty = true;
            syncNeeded = true;
        }
        return length;
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
            channel.force(false);
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
                bytesSinceTrickleFsync += buffer.position();
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
            buffer.flip();
            channel.write(buffer);
            lastFlushOffset += buffer.position();
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
        return current();
    }

    /**
     * Returns the current file pointer of the underlying on-disk file.
     * Note that since write works by buffering data, the value of this will increase by buffer
     * size and not every write to the writer will modify this value.
     * Furthermore, for compressed files, this value refers to compressed data, while the
     * writer getFilePointer() refers to uncompressedFile
     *
     * @return the current file pointer
     */
    public long getOnDiskFilePointer()
    {
        return getFilePointer();
    }

    public long length()
    {
        try
        {
            return Math.max(current(), channel.size());
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
        bufferOffset = current();
        buffer.clear();
    }

    protected long current()
    {
        return bufferOffset + (buffer == null ? 0 : buffer.position());
    }

    public FileMark mark()
    {
        return new BufferedFileWriterMark(current());
    }

    /**
     * Drops all buffered data that's past the limits of our new file mark + buffer capacity, or syncs and truncates
     * the underlying file to the marked position
     */
    public void resetAndTruncate(FileMark mark)
    {
        assert mark instanceof BufferedFileWriterMark;

        long previous = current();
        long truncateTarget = ((BufferedFileWriterMark) mark).pointer;

        // If we're resetting to a point within our buffered data, just adjust our buffered position to drop bytes to
        // the right of the desired mark.
        if (previous - truncateTarget <= buffer.position())
        {
            buffer.position(buffer.position() - ((int) (previous - truncateTarget)));
            return;
        }

        // synchronize current buffer with disk - we don't want any data loss
        syncInternal();

        // truncate file to given position
        truncate(truncateTarget);

        try
        {
            channel.position(truncateTarget);
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
            channel.truncate(toSize);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    public boolean isOpen()
    {
        return channel.isOpen();
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
        try { channel.close(); }
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
