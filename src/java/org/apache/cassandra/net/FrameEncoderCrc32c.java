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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import static org.apache.cassandra.utils.FBUtilities.updateChecksum;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Please see {@link org.apache.cassandra.net.FrameDecoderCrc32c} for description of the framing produced by this encoder.
 */
@ChannelHandler.Sharable
public final class FrameEncoderCrc32c extends FrameEncoder
{
    private static final Supplier<Checksum> CHECKSUM_FACTORY = CRC32C::new;
    private static final int HEADER_LENGTH = 7;
    private static final int TRAILER_LENGTH = 4;
    public static final int HEADER_AND_TRAILER_LENGTH = 11;

    public static final FrameEncoderCrc32c instance = new FrameEncoderCrc32c();
    static final FrameEncoder.PayloadAllocator allocator = (isSelfContained, capacity) ->
        new FrameEncoder.Payload(isSelfContained, capacity, HEADER_LENGTH, TRAILER_LENGTH);

    public FrameEncoder.PayloadAllocator allocator()
    {
        return allocator;
    }

    private static void writeHeader(ByteBuffer frame, boolean isSelfContained, int dataLength)
    {
        int header3b = dataLength & 0x1FFFF;
        if (isSelfContained)
            header3b |= 1 << 17;

        Checksum crc32c = CHECKSUM_FACTORY.get();
        updateChecksumInt(crc32c, header3b);

        frame.put(0, (byte) header3b);
        frame.put(1, (byte) (header3b >>> 8));
        frame.put(2, (byte) (header3b >>> 16));

        int checksum = (int) crc32c.getValue();

        frame.put(3, (byte) checksum);
        frame.put(4, (byte) (checksum >>> 8));
        frame.put(5, (byte) (checksum >>> 16));
        frame.put(6, (byte) (checksum >>> 24));
    }

    public ByteBuf encode(boolean isSelfContained, ByteBuffer frame)
    {
        try
        {
            int frameLength = frame.remaining();
            int dataLength = frameLength - HEADER_AND_TRAILER_LENGTH;
            if (dataLength >= 1 << 17)
                throw new IllegalArgumentException("Maximum payload size is 128KiB");

            writeHeader(frame, isSelfContained, dataLength);

            Checksum crc = CHECKSUM_FACTORY.get();
            updateChecksum(crc, frame, HEADER_LENGTH, dataLength);

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
