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
import java.util.Collection;
import java.util.zip.CRC32;

import io.netty.channel.ChannelPipeline;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import static org.apache.cassandra.net.Crc.*;

/**
 * Framing format that compresses payloads with LZ4, and protects integrity of data in movement with CRCs
 * (of both header and payload).
 *
 * Every on-wire frame contains:
 * 1. Compressed length            (17 bits)
 * 2. Uncompressed length          (17 bits)
 * 3. {@code isSelfContained} flag (1 bit)
 * 4. Header padding               (5 bits)
 * 5. CRC24 of Header contents     (24 bits)
 * 6. Compressed Payload           (up to 2 ^ 17 - 1 bits)
 * 7. CRC32 of Compressed Payload  (32 bits)
 *
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |        Compressed Length        |     Uncompressed Length
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |C|         |                 CRC24 of Header               |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                                                               |
 * +                                                               +
 * |                      Compressed Payload                       |
 * +                                                               +
 * |                                                               |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                  CRC32 of Compressed Payload                  |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 */
public final class FrameDecoderLZ4 extends FrameDecoderWith8bHeader
{
    public static FrameDecoderLZ4 fast(BufferPoolAllocator allocator)
    {
        return new FrameDecoderLZ4(allocator, LZ4Factory.fastestInstance().safeDecompressor());
    }

    private static final int HEADER_LENGTH = 8;
    private static final int TRAILER_LENGTH = 4;
    private static final int HEADER_AND_TRAILER_LENGTH = 12;

    private static int compressedLength(long header8b)
    {
        return ((int) header8b) & 0x1FFFF;
    }
    private static int uncompressedLength(long header8b)
    {
        return ((int) (header8b >>> 17)) & 0x1FFFF;
    }
    private static boolean isSelfContained(long header8b)
    {
        return 0 != (header8b & (1L << 34));
    }
    private static int headerCrc(long header8b)
    {
        return ((int) (header8b >>> 40)) & 0xFFFFFF;
    }

    private final LZ4SafeDecompressor decompressor;

    private FrameDecoderLZ4(BufferPoolAllocator allocator, LZ4SafeDecompressor decompressor)
    {
        super(allocator);
        this.decompressor = decompressor;
    }

    final long readHeader(ByteBuffer frame, int begin)
    {
        long header8b = frame.getLong(begin);
        if (frame.order() == ByteOrder.BIG_ENDIAN)
            header8b = Long.reverseBytes(header8b);
        return header8b;
    }

    final CorruptFrame verifyHeader(long header8b)
    {
        int computeLengthCrc = crc24(header8b, 5);
        int readLengthCrc = headerCrc(header8b);

        return readLengthCrc == computeLengthCrc ? null : CorruptFrame.unrecoverable(readLengthCrc, computeLengthCrc);
    }

    final int frameLength(long header8b)
    {
        return compressedLength(header8b) + HEADER_AND_TRAILER_LENGTH;
    }

    final Frame unpackFrame(ShareableBytes bytes, int begin, int end, long header8b)
    {
        ByteBuffer input = bytes.get();

        boolean isSelfContained = isSelfContained(header8b);
        int uncompressedLength = uncompressedLength(header8b);

        CRC32 crc = crc32();
        int readFullCrc = input.getInt(end - TRAILER_LENGTH);
        if (input.order() == ByteOrder.BIG_ENDIAN)
            readFullCrc = Integer.reverseBytes(readFullCrc);

        updateCrc32(crc, input, begin + HEADER_LENGTH, end - TRAILER_LENGTH);
        int computeFullCrc = (int) crc.getValue();

        if (readFullCrc != computeFullCrc)
            return CorruptFrame.recoverable(isSelfContained, uncompressedLength, readFullCrc, computeFullCrc);

        if (uncompressedLength == 0)
        {
            return new IntactFrame(isSelfContained, bytes.slice(begin + HEADER_LENGTH, end - TRAILER_LENGTH));
        }
        else
        {
            ByteBuffer out = allocator.get(uncompressedLength);
            try
            {
                int sourceLength = end - (begin + HEADER_LENGTH + TRAILER_LENGTH);
                decompressor.decompress(input, begin + HEADER_LENGTH, sourceLength, out, 0, uncompressedLength);
                return new IntactFrame(isSelfContained, ShareableBytes.wrap(out));
            }
            catch (Throwable t)
            {
                allocator.put(out);
                throw t;
            }
        }
    }

    void decode(Collection<Frame> into, ShareableBytes bytes)
    {
        // TODO: confirm in assembly output that we inline the relevant nested method calls
        decode(into, bytes, HEADER_LENGTH);
    }

    void addLastTo(ChannelPipeline pipeline)
    {
        pipeline.addLast("frameDecoderLZ4", this);
    }
}
