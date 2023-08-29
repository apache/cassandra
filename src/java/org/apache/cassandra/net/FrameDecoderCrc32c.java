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

import io.netty.channel.ChannelPipeline;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.function.Supplier;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import static org.apache.cassandra.utils.FBUtilities.updateChecksum;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Framing format that protects integrity of data in movement with CRCs (of both header and payload).
 * <p>
 * Every on-wire frame contains:
 * 1. Payload length               (17 bits)
 * 2. {@code isSelfContained} flag (1 bit)
 * 3. Header padding               (6 bits)
 * 4. CRC32С of the header         (32 bits)
 * 5. Payload                      (up to 2 ^ 17 - 1 bits)
 * 6. Payload CRC32С               (32 bits)
 * <p>
 * <pre>
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |          Payload Length         |C|           |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *          CRC32С of Header       |                               |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
 * |                                                               |
 * +                                                               +
 * |                            Payload                            |
 * +                                                               +
 * |                                                               |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                       CRC32С of Payload                       |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * </pre>
 */
public final class FrameDecoderCrc32c extends FrameDecoderWith8bHeader
{
    private static final Supplier<Checksum> CHECKSUM_FACTORY = CRC32C::new;

    public FrameDecoderCrc32c(BufferPoolAllocator allocator)
    {
        super(allocator);
    }

    public static FrameDecoderCrc32c create(BufferPoolAllocator allocator)
    {
        return new FrameDecoderCrc32c(allocator);
    }

    private static final int HEADER_LENGTH = 7;
    private static final int TRAILER_LENGTH = 4;
    private static final int HEADER_AND_TRAILER_LENGTH = 11;
    private static final int BITMASK_32 = 0xFFFFFFFF;
    private static final int BITMASK_24 = 0xFFFFFF;
    private static final int BITMASK_17 = 0x1FFFF;

    static boolean isSelfContained(long header7b)
    {
        return 0 != (header7b & (1L << 17));
    }

    static int payloadLength(long header7b)
    {
        return ((int) header7b) & BITMASK_17;
    }

    private static int headerCrc(long header7b)
    {
        return ((int) (header7b >>> 24)) & BITMASK_32;
    }

    static long readheader7b(ByteBuffer frame, int begin)
    {
        long header7b;
        if (frame.limit() - begin >= 8)
        {
            header7b = frame.getLong(begin);
            if (frame.order() == ByteOrder.BIG_ENDIAN)
                header7b = Long.reverseBytes(header7b);
            header7b &= 0xffffffffffffffL;
        } else
        {
            header7b = 0;
            for (int i = 0; i < HEADER_LENGTH; ++i)
                header7b |= (0xffL & frame.get(begin + i)) << (8 * i);
        }
        return header7b;
    }

    static FrameDecoder.CorruptFrame verifyheader7b(long header7b)
    {
        Checksum crc32c = CHECKSUM_FACTORY.get();

        updateChecksumInt(crc32c, (int) (header7b & BITMASK_24));
        int computeLengthCrc = (int) crc32c.getValue();
        int readLengthCrc = headerCrc(header7b);

        return readLengthCrc == computeLengthCrc ? null : FrameDecoder.CorruptFrame.unrecoverable(readLengthCrc, computeLengthCrc);
    }

    @Override
    long readHeader(ByteBuffer frame, int begin)
    {
        return readheader7b(frame, begin);
    }

    @Override
    FrameDecoder.CorruptFrame verifyHeader(long header7b)
    {
        return verifyheader7b(header7b);
    }

    int frameLength(long header7b)
    {
        return payloadLength(header7b) + HEADER_AND_TRAILER_LENGTH;
    }

    FrameDecoder.Frame unpackFrame(ShareableBytes bytes, int begin, int end, long header7b)
    {
        ByteBuffer in = bytes.get();
        boolean isSelfContained = isSelfContained(header7b);

        Checksum crc = CHECKSUM_FACTORY.get();
        int readFullCrc = in.getInt(end - TRAILER_LENGTH);
        if (in.order() == ByteOrder.BIG_ENDIAN)
            readFullCrc = Integer.reverseBytes(readFullCrc);

        updateChecksum(crc, in, begin + HEADER_LENGTH, payloadLength(header7b));
        int computeFullCrc = (int) crc.getValue();

        if (readFullCrc != computeFullCrc)
            return FrameDecoder.CorruptFrame.recoverable(isSelfContained, (end - begin) - HEADER_AND_TRAILER_LENGTH, readFullCrc, computeFullCrc);

        return new FrameDecoder.IntactFrame(isSelfContained, bytes.slice(begin + HEADER_LENGTH, end - TRAILER_LENGTH));
    }

    public void decode(Collection<Frame> into, ShareableBytes bytes)
    {
        decode(into, bytes, HEADER_LENGTH);
    }

    void addLastTo(ChannelPipeline pipeline)
    {
        pipeline.addLast("frameDecoderCrc32c", this);
    }
}
