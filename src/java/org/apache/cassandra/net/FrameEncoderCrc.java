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
import java.nio.ByteOrder;
import java.util.zip.CRC32;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;

import static org.apache.cassandra.net.Crc.*;

/**
 * Please see {@link FrameDecoderCrc} for description of the framing produced by this encoder.
 */
@ChannelHandler.Sharable
public class FrameEncoderCrc extends FrameEncoder
{
    static final int HEADER_LENGTH = 6;
    private static final int TRAILER_LENGTH = 4;
    public static final int HEADER_AND_TRAILER_LENGTH = 10;

    public static final FrameEncoderCrc instance = new FrameEncoderCrc();
    static final PayloadAllocator allocator = (isSelfContained, capacity) ->
        new Payload(isSelfContained, capacity, HEADER_LENGTH, TRAILER_LENGTH);

    public PayloadAllocator allocator()
    {
        return allocator;
    }

    static void writeHeader(ByteBuffer frame, boolean isSelfContained, int dataLength)
    {
        int header3b = dataLength;
        if (isSelfContained)
            header3b |= 1 << 17;
        int crc = crc24(header3b, 3);
        put3b(frame, 0, header3b);
        put3b(frame, 3, crc);
    }

    private static void put3b(ByteBuffer frame, int index, int put3b)
    {
        frame.put(index    , (byte) put3b        );
        frame.put(index + 1, (byte)(put3b >>> 8) );
        frame.put(index + 2, (byte)(put3b >>> 16));
    }

    ByteBuf encode(boolean isSelfContained, ByteBuffer frame)
    {
        try
        {
            int frameLength = frame.remaining();
            int dataLength = frameLength - HEADER_AND_TRAILER_LENGTH;
            if (dataLength >= 1 << 17)
                throw new IllegalArgumentException("Maximum payload size is 128KiB");

            writeHeader(frame, isSelfContained, dataLength);

            CRC32 crc = crc32();
            frame.position(HEADER_LENGTH);
            frame.limit(dataLength + HEADER_LENGTH);
            crc.update(frame);

            int frameCrc = (int) crc.getValue();
            if (frame.order() == ByteOrder.BIG_ENDIAN)
                frameCrc = Integer.reverseBytes(frameCrc);

            frame.limit(frameLength);
            frame.putInt(frameLength - TRAILER_LENGTH, frameCrc);
            frame.position(0);

            return GlobalBufferPoolAllocator.wrap(frame);
        }
        catch (Throwable t)
        {
            bufferPool.put(frame);
            throw t;
        }
    }
}
