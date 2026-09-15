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

import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.utils.Closeable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A read-ahead buffer for sequential scans of a single file.
 */
@NotThreadSafe
public class ReadAheadBuffer implements Closeable
{
    private final ChannelProxy channel;
    private final BufferType bufferType;
    private final long channelSize;
    private final int bufferSize;

    private ByteBuffer buffer;
    private int index = -1;
    private boolean released;

    public ReadAheadBuffer(ChannelProxy channel, int bufferSize, BufferType bufferType)
    {
        this.channel = channel;
        this.channelSize = channel.size();
        this.bufferSize = bufferSize;
        this.bufferType = bufferType;
    }

    public boolean hasBuffer()
    {
        return buffer != null;
    }

    @VisibleForTesting
    int bufferSize()
    {
        return bufferSize;
    }

    public int remaining()
    {
        return allocatedBuffer().remaining();
    }

    @VisibleForTesting
    void allocateBuffer()
    {
        getBuffer();
    }

    private ByteBuffer getBuffer()
    {
        if (released)
            throw new IllegalStateException("read-ahead buffer for " + channel.filePath() + " has been released");

        if (buffer == null)
        {
            buffer = bufferType.allocate(bufferSize);
            buffer.clear();
        }
        return buffer;
    }

    private ByteBuffer allocatedBuffer()
    {
        if (buffer == null)
            throw new IllegalStateException("read-ahead buffer for " + channel.filePath() + " is not allocated");

        return buffer;
    }

    public void fill(long position) throws CorruptBlockException
    {
        ByteBuffer blockBuffer = getBuffer();
        if (position >= channelSize)
            throw new CorruptBlockException(channel.filePath(), position, bufferSize);

        int blockNo = (int) (position / bufferSize);
        long blockPosition = blockNo * (long) bufferSize;

        long remaining = channelSize - blockPosition;
        int sizeToRead = (int) Math.min(remaining, bufferSize);
        if (index != blockNo)
        {
            blockBuffer.flip();
            loadBlock(blockBuffer, blockPosition, sizeToRead);
            index = blockNo;
        }

        blockBuffer.flip();
        blockBuffer.limit(sizeToRead);
        blockBuffer.position((int) (position - blockPosition));
    }

    protected void loadBlock(ByteBuffer blockBuffer, long blockPosition, int sizeToRead) throws CorruptBlockException
    {
        blockBuffer.limit(sizeToRead);
        if (channel.read(blockBuffer, blockPosition) != sizeToRead)
            throw new CorruptBlockException(channel.filePath(), blockPosition, sizeToRead);
    }

    public int read(ByteBuffer dest, int length)
    {
        ByteBuffer blockBuffer = allocatedBuffer();
        ByteBuffer tmp = blockBuffer.duplicate();
        tmp.limit(tmp.position() + length);
        dest.put(tmp);
        blockBuffer.position(blockBuffer.position() + length);

        return length;
    }

    @Override
    public void close()
    {
        released = true;
        if (buffer == null)
            return;

        index = -1;
        buffer.clear();
        FileUtils.clean(buffer);
        buffer = null;
    }
}
