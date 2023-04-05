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
package org.apache.cassandra.net;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;

import static org.apache.cassandra.net.FrameEncoderCrc.HEADER_LENGTH;
import static org.apache.cassandra.net.FrameEncoderCrc.writeHeader;

/**
 * A frame encoder that writes frames, just without any modification or payload protection.
 * This is non-standard, and useful for systems that have a trusted transport layer that want
 * to avoid incurring the (very low) cost of computing a CRC.
 *
 * Please see {@link FrameDecoderUnprotected} for description of the framing produced by this encoder.
 */
@ChannelHandler.Sharable
class FrameEncoderUnprotected extends FrameEncoder
{
    static final FrameEncoderUnprotected instance = new FrameEncoderUnprotected();
    static final PayloadAllocator allocator = (isSelfContained, capacity) ->
        new Payload(isSelfContained, capacity, HEADER_LENGTH, 0);

    public PayloadAllocator allocator()
    {
        return allocator;
    }

    ByteBuf encode(boolean isSelfContained, ByteBuffer frame)
    {
        try
        {
            int frameLength = frame.remaining();
            int dataLength = frameLength - HEADER_LENGTH;
            if (dataLength >= 1 << 17)
                throw new IllegalArgumentException("Maximum uncompressed payload size is 128KiB");

            writeHeader(frame, isSelfContained, dataLength);
            return GlobalBufferPoolAllocator.wrap(frame);
        }
        catch (Throwable t)
        {
            bufferPool.put(frame);
            throw t;
        }
    }
}
