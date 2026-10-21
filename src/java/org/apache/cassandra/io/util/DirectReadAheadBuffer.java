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

import org.agrona.BitUtil;
import org.agrona.BufferUtil;

import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.memory.MemoryUtil;

import sun.nio.ch.DirectBuffer;

public final class DirectReadAheadBuffer extends ReadAheadBuffer
{

    private final int blockSize;

    public DirectReadAheadBuffer(ChannelProxy channel, int bufferSize, int blockSize)
    {
        super(channel, () -> BufferUtil.allocateDirectAligned(BitUtil.align(bufferSize, blockSize), blockSize));
        this.blockSize = blockSize;
    }

    @Override
    protected void loadBlock(ByteBuffer blockBuffer, long blockPosition, int sizeToRead)
    {
        int alignedSizeToRead = BitUtil.align(sizeToRead, blockSize);

        blockBuffer.limit(alignedSizeToRead);

        if (channel.read(blockBuffer, blockPosition) < sizeToRead)
            throw new CorruptSSTableException(null, channel.filePath());
    }

    @Override
    protected void cleanBuffer(ByteBuffer buffer)
    {
        // BufferUtil.allocateDirectAligned returns an aligned slice with no cleaner; free the backing
        // allocation through the attachment, matching DirectThreadLocalByteBufferHolder.
        MemoryUtil.clean((ByteBuffer) ((DirectBuffer) buffer).attachment());
    }
}
