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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;

public final class ThreadLocalReadAheadBuffer
{

    private final ChannelProxy channel;

    private final BufferType bufferType;

    private static final Map<String, FastThreadLocal<ByteBuffer>> blockBufferHolders = new ConcurrentHashMap<>();

    private static final Map<String, FastThreadLocal<Integer>> storedBlockNos = new ConcurrentHashMap<>();

    private final FastThreadLocal<ByteBuffer> blockBufferHolder;

    private final FastThreadLocal<Integer> storedBlockNo;

    private final int bufferSize;

    public ThreadLocalReadAheadBuffer(ChannelProxy channel, int bufferSize, BufferType bufferType)
    {
        this.channel = channel;
        this.bufferSize = bufferSize;
        this.bufferType = bufferType;
        blockBufferHolders.putIfAbsent(channel.filePath(), new FastThreadLocal<>()
        {
            @Override
            protected ByteBuffer initialValue() throws Exception
            {
                return null;
            }
        });
        storedBlockNos.putIfAbsent(channel.filePath(), new FastThreadLocal<>() {
            @Override
            protected Integer initialValue() throws Exception
            {
                return -1;
            }
        });
        blockBufferHolder = blockBufferHolders.get(channel.filePath());
        storedBlockNo = storedBlockNos.get(channel.filePath());
    }

    public boolean hasBuffer()
    {
        return blockBufferHolder.get() != null;
    }

    public int remaining()
    {
        return getBlockBuffer().remaining();
    }


    public ByteBuffer getBlockBuffer()
    {
        ByteBuffer buffer = blockBufferHolder.get();
        if (buffer == null)
        {
            buffer = bufferType.allocate(bufferSize);
            buffer.clear();
            blockBufferHolder.set(buffer);
        }
        return buffer;
    }

    public void fill(long position) throws CorruptBlockException
    {
        ByteBuffer blockBuffer = getBlockBuffer();
        long channelSize = channel.size();
        long realPosition = Math.min(channelSize, position);
        long blockLength = blockBuffer.limit();
        int blockNo = (int) (realPosition / blockLength);
        long blockPosition = blockNo * blockLength;

        if (storedBlockNo.get() != blockNo)
        {
            long remaining = channelSize - realPosition;
            int sizeToRead = (int) Math.min(remaining, blockLength);

            blockBuffer.flip();
            blockBuffer.limit(sizeToRead);
            if (channel.read(blockBuffer, blockPosition) != sizeToRead)
                throw new CorruptSSTableException(null, channel.filePath());

            storedBlockNo.set(blockNo);
        }

        blockBuffer.flip();
        blockBuffer.limit((int) blockLength);
        blockBuffer.position((int) (realPosition - blockPosition));
    }

    public int read(ByteBuffer dest, int length)
    {
        ByteBuffer blockBuffer = getBlockBuffer();
        ByteBuffer tmp = blockBuffer.duplicate();
        tmp.limit(tmp.position() + length);
        dest.put(tmp);
        blockBuffer.position(blockBuffer.position() + length);

        return length;
    }

    public void clear(boolean deallocate)
    {
        storedBlockNo.remove();

        ByteBuffer blockBuffer = blockBufferHolder.get();
        if (blockBuffer != null)
        {
            blockBuffer.clear();
            if (deallocate)
            {
                FileUtils.clean(blockBuffer);
                blockBufferHolder.remove();
            }
        }
    }

    public void close()
    {
        clear(true);
        blockBufferHolders.remove(channel.filePath(), blockBufferHolder);
        storedBlockNos.remove(channel.filePath(), storedBlockNo);
    }
}
