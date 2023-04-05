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
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.net.Crc.*;

/**
 * Please see {@link FrameDecoderLZ4} for description of the framing produced by this encoder.
 */
@ChannelHandler.Sharable
public
class FrameEncoderLZ4 extends FrameEncoder
{
    public static final FrameEncoderLZ4 fastInstance = new FrameEncoderLZ4(LZ4Factory.fastestInstance().fastCompressor());

    private final LZ4Compressor compressor;

    private FrameEncoderLZ4(LZ4Compressor compressor)
    {
        this.compressor = compressor;
    }

    private static final int HEADER_LENGTH = 8;
    public static final int HEADER_AND_TRAILER_LENGTH = 12;

    private static void writeHeader(ByteBuffer frame, boolean isSelfContained, long compressedLength, long uncompressedLength)
    {
        long header5b = compressedLength | (uncompressedLength << 17);
        if (isSelfContained)
            header5b |= 1L << 34;

        long crc = crc24(header5b, 5);

        long header8b = header5b | (crc << 40);
        if (frame.order() == ByteOrder.BIG_ENDIAN)
            header8b = Long.reverseBytes(header8b);

        frame.putLong(0, header8b);
    }

    public ByteBuf encode(boolean isSelfContained, ByteBuffer in)
    {
        ByteBuffer frame = null;
        try
        {
            int uncompressedLength = in.remaining();
            if (uncompressedLength >= 1 << 17)
                throw new IllegalArgumentException("Maximum uncompressed payload size is 128KiB");

            int maxOutputLength = compressor.maxCompressedLength(uncompressedLength);
            frame = bufferPool.getAtLeast(HEADER_AND_TRAILER_LENGTH + maxOutputLength, BufferType.OFF_HEAP);

            int compressedLength = compressor.compress(in, in.position(), uncompressedLength, frame, HEADER_LENGTH, maxOutputLength);

            if (compressedLength >= uncompressedLength)
            {
                ByteBufferUtil.copyBytes(in, in.position(), frame, HEADER_LENGTH, uncompressedLength);
                compressedLength = uncompressedLength;
                uncompressedLength = 0;
            }

            writeHeader(frame, isSelfContained, compressedLength, uncompressedLength);

            CRC32 crc = crc32();
            frame.position(HEADER_LENGTH);
            frame.limit(compressedLength + HEADER_LENGTH);
            crc.update(frame);

            int frameCrc = (int) crc.getValue();
            if (frame.order() == ByteOrder.BIG_ENDIAN)
                frameCrc = Integer.reverseBytes(frameCrc);
            int frameLength = compressedLength + HEADER_AND_TRAILER_LENGTH;

            frame.limit(frameLength);
            frame.putInt(frameCrc);
            frame.position(0);

            bufferPool.putUnusedPortion(frame);
            return GlobalBufferPoolAllocator.wrap(frame);
        }
        catch (Throwable t)
        {
            if (frame != null)
                bufferPool.put(frame);
            throw t;
        }
        finally
        {
            bufferPool.put(in);
        }
    }
}
