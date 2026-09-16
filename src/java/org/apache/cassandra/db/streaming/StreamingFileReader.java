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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db.streaming;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

import com.sun.nio.file.ExtendedOpenOption;

import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.memory.MemoryUtil;

import sun.nio.ch.DirectBuffer;

/** Reads stored bytes, without decompression, for both partial and entire SSTable streaming. */
final class StreamingFileReader implements AutoCloseable
{
    static final int BUFFER_SIZE = 64 * 1024;
    // Direct I/O disables read-ahead and each read is synchronous, so stage far more than a network batch.
    static final int STAGING_BUFFER_SIZE = 1024 * 1024;

    private static final Logger logger = LoggerFactory.getLogger(StreamingFileReader.class);

    private final FileChannel channel;
    private final int blockSize;
    private final long size;
    private ByteBuffer buffer;
    private long bufferOffset = -1;

    static StreamingFileReader open(File file) throws IOException
    {
        if (DatabaseDescriptor.getBackgroundReadDiskAccessMode() == DiskAccessMode.direct)
        {
            String reason = "unusable block size";
            try
            {
                // An oversized or non-power-of-two unit cannot be aligned against, or would dwarf the staging window.
                int blockSize = FileUtils.getFileBlockSize(file);
                if (blockSize > 0 && blockSize <= STAGING_BUFFER_SIZE && (blockSize & (blockSize - 1)) == 0)
                    return new StreamingFileReader(FileChannel.open(file.toPath(), StandardOpenOption.READ, ExtendedOpenOption.DIRECT), blockSize);
            }
            catch (IOException | RuntimeException e)
            {
                // Probe the actual file, not a temporary file on a potentially different backend.
                // Genuine file access failures still propagate from the buffered open below.
                reason = e.toString();
            }
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES,
                             "Direct I/O is unavailable for streaming {} ({}), falling back to buffered reads", file, reason);
        }
        return new StreamingFileReader(file.newReadChannel(), 0);
    }

    private StreamingFileReader(FileChannel channel, int blockSize) throws IOException
    {
        this.channel = channel;
        this.blockSize = blockSize;
        try
        {
            size = channel.size();
        }
        catch (Throwable t)
        {
            channel.close();
            throw t;
        }
    }

    boolean isDirect()
    {
        return blockSize > 0;
    }

    long size()
    {
        return size;
    }

    void readFully(ByteBuffer destination, long position) throws IOException
    {
        if (position < 0 || position > size || destination.remaining() > size - position)
            throw new EOFException("Streaming read outside file: " + position + " + " + destination.remaining() + " > " + size);

        if (!isDirect())
        {
            while (destination.hasRemaining())
                position += read(channel, destination, position);
            return;
        }

        if (buffer == null)
        {
            // Never stage more than the file itself; whole-SSTable streaming opens a reader per component.
            int window = size >= STAGING_BUFFER_SIZE ? STAGING_BUFFER_SIZE : BitUtil.align((int) Math.max(size, 1), blockSize);
            buffer = BufferUtil.allocateDirectAligned(window, blockSize);
            buffer.limit(0);
        }
        while (destination.hasRemaining())
        {
            if (position < bufferOffset || position >= bufferOffset + buffer.limit())
            {
                bufferOffset = position - position % buffer.capacity();
                buffer.clear();
                int expected = (int) Math.min(buffer.capacity(), size - bufferOffset);
                // Address, offset and length are aligned; only the final physical read may be short.
                while (buffer.position() < expected)
                {
                    int count = read(channel, buffer, bufferOffset + buffer.position());
                    if (buffer.position() < expected && count % blockSize != 0)
                        throw new EOFException("Unaligned short direct streaming read at " + bufferOffset);
                }
                buffer.flip();
            }
            int offset = (int) (position - bufferOffset);
            int count = Math.min(destination.remaining(), buffer.limit() - offset);
            int limit = buffer.limit();
            buffer.position(offset).limit(offset + count);
            destination.put(buffer);
            buffer.limit(limit);
            position += count;
        }
    }

    private static int read(FileChannel channel, ByteBuffer destination, long position) throws IOException
    {
        int count = channel.read(destination, position);
        if (count <= 0)
            throw new EOFException("No streaming read progress at " + position);
        return count;
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            channel.close();
        }
        finally
        {
            if (buffer != null)
            {
                MemoryUtil.clean((ByteBuffer) ((DirectBuffer) buffer).attachment());
                buffer = null;
            }
        }
    }
}
