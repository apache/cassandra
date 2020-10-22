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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;

import static java.lang.Integer.reverseBytes;
import static java.lang.String.format;
import static org.apache.cassandra.net.LegacyLZ4Constants.*;
import static org.apache.cassandra.utils.ByteBufferUtil.copyBytes;

/**
 * A {@link FrameDecoder} consisting of two chained handlers:
 *  1. A legacy LZ4 block decoder, described below in the description of {@link LZ4Decoder}, followed by
 *  2. An instance of {@link FrameDecoderLegacy} - transforming the raw messages in the uncompressed stream
 *     into properly formed frames expected by {@link InboundMessageHandler}
 */
class FrameDecoderLegacyLZ4 extends FrameDecoderLegacy
{
    private static final BufferPool bufferPool = BufferPools.forNetworking();

    FrameDecoderLegacyLZ4(BufferPoolAllocator allocator, int messagingVersion)
    {
        super(allocator, messagingVersion);
    }

    @Override
    void addLastTo(ChannelPipeline pipeline)
    {
        pipeline.addLast("legacyLZ4Decoder", new LZ4Decoder(allocator));
        pipeline.addLast("frameDecoderNone", this);
    }

    /**
     * An implementation of LZ4 decoder, used for legacy (3.0, 3.11) connections.
     *
     * Netty's provided implementation - {@link Lz4FrameDecoder} couldn't be reused for
     * two reasons:
     *   1. It has very poor performance when coupled with xxHash, which we use for legacy connections -
     *      allocating a single-byte array and making a JNI call <em>for every byte of the payload</em>
     *   2. It was tricky to efficiently integrate with upstream {@link FrameDecoder}, and impossible
     *      to make it play nicely with flow control - Netty's implementation, based on
     *      {@link io.netty.handler.codec.ByteToMessageDecoder}, would potentially keep triggering
     *      reads on its own volition for as long as its last read had no completed frames to supply
     *      - defying our goal to only ever trigger channel reads when explicitly requested
     *
     * Since the original LZ4 block format does not contains size of compressed block and size of original data
     * this encoder uses format like <a href="https://github.com/idelpivnitskiy/lz4-java">LZ4 Java</a> library
     * written by Adrien Grand and approved by Yann Collet (author of original LZ4 library), as implemented by
     * Netty's {@link Lz4FrameDecoder}, but adapted for our interaction model.
     *
     *  0                   1                   2                   3
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |                                                               |
     * +                             Magic                             +
     * |                                                               |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |T|                      Compressed Length
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *   |                     Uncompressed Length
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *   |               xxHash32 of Uncompressed Payload
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *   |                                                             |
     * +-+                                                             +
     * |                                                               |
     * +                            Payload                            +
     * |                                                               |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     */
    private static class LZ4Decoder extends ChannelInboundHandlerAdapter
    {
        private static final XXHash32 xxhash =
            XXHashFactory.fastestInstance().hash32();

        private static final LZ4SafeDecompressor decompressor =
            LZ4Factory.fastestInstance().safeDecompressor();

        private final BufferPoolAllocator allocator;

        LZ4Decoder(BufferPoolAllocator allocator)
        {
            this.allocator = allocator;
        }

        private final Deque<ShareableBytes> frames = new ArrayDeque<>(4);

        // total # of frames decoded between two subsequent invocations of channelReadComplete()
        private int decodedFrameCount = 0;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws CorruptLZ4Frame
        {
            assert msg instanceof BufferPoolAllocator.Wrapped;
            ByteBuffer buf = ((BufferPoolAllocator.Wrapped) msg).adopt();
            // netty will probably have mis-predicted the space needed
            bufferPool.putUnusedPortion(buf);

            CorruptLZ4Frame error = null;
            try
            {
                decode(frames, ShareableBytes.wrap(buf));
            }
            catch (CorruptLZ4Frame e)
            {
                error = e;
            }
            finally
            {
                decodedFrameCount += frames.size();
                while (!frames.isEmpty())
                    ctx.fireChannelRead(frames.poll());
            }

            if (null != error)
                throw error;
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx)
        {
            /*
             * If no frames have been decoded from the entire batch of channelRead() calls,
             * then we must trigger another channel read explicitly, or else risk stalling
             * forever without bytes to complete the current in-flight frame.
             */
            if (null != stash && decodedFrameCount == 0 && !ctx.channel().config().isAutoRead())
                ctx.read();

            decodedFrameCount = 0;
            ctx.fireChannelReadComplete();
        }

        private void decode(Collection<ShareableBytes> into, ShareableBytes newBytes) throws CorruptLZ4Frame
        {
            try
            {
                doDecode(into, newBytes);
            }
            finally
            {
                newBytes.release();
            }
        }

        private void doDecode(Collection<ShareableBytes> into, ShareableBytes newBytes) throws CorruptLZ4Frame
        {
            ByteBuffer in = newBytes.get();

            if (null != stash)
            {
                if (!copyToSize(in, stash, HEADER_LENGTH))
                    return;

                header.read(stash, 0);
                header.validate();

                int frameLength = header.frameLength();
                stash = ensureCapacity(stash, frameLength);

                if (!copyToSize(in, stash, frameLength))
                    return;

                stash.flip();
                ShareableBytes stashed = ShareableBytes.wrap(stash);
                stash = null;

                try
                {
                    into.add(decompressFrame(stashed, 0, frameLength, header));
                }
                finally
                {
                    stashed.release();
                }
            }

            int begin = in.position();
            int limit = in.limit();
            while (begin < limit)
            {
                int remaining = limit - begin;
                if (remaining < HEADER_LENGTH)
                {
                    stash(newBytes, HEADER_LENGTH, begin, remaining);
                    return;
                }

                header.read(in, begin);
                header.validate();

                int frameLength = header.frameLength();
                if (remaining < frameLength)
                {
                    stash(newBytes, frameLength, begin, remaining);
                    return;
                }

                into.add(decompressFrame(newBytes, begin, begin + frameLength, header));
                begin += frameLength;
            }
        }

        private ShareableBytes decompressFrame(ShareableBytes bytes, int begin, int end, Header header) throws CorruptLZ4Frame
        {
            ByteBuffer buf = bytes.get();

            if (header.uncompressedLength == 0)
                return bytes.slice(begin + HEADER_LENGTH, end);

            if (!header.isCompressed())
            {
                validateChecksum(buf, begin + HEADER_LENGTH, header);
                return bytes.slice(begin + HEADER_LENGTH, end);
            }

            ByteBuffer out = allocator.get(header.uncompressedLength);
            try
            {
                int sourceLength = end - (begin + HEADER_LENGTH);
                decompressor.decompress(buf, begin + HEADER_LENGTH, sourceLength, out, 0, header.uncompressedLength);
                validateChecksum(out, 0, header);
                return ShareableBytes.wrap(out);
            }
            catch (Throwable t)
            {
                bufferPool.put(out);
                throw t;
            }
        }

        private void validateChecksum(ByteBuffer buf, int begin, Header header) throws CorruptLZ4Frame
        {
            int checksum = xxhash.hash(buf, begin, header.uncompressedLength, XXHASH_SEED) & XXHASH_MASK;
            if (checksum != header.checksum)
                except("Invalid checksum detected: %d (expected: %d)", checksum, header.checksum);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx)
        {
            if (null != stash)
            {
                bufferPool.put(stash);
                stash = null;
            }

            while (!frames.isEmpty())
                frames.poll().release();

            ctx.fireChannelInactive();
        }

        /* reusable container for deserialized header fields */
        private static final class Header
        {
            long magicNumber;
            byte token;
            int compressedLength;
            int uncompressedLength;
            int checksum;

            int frameLength()
            {
                return HEADER_LENGTH + compressedLength;
            }

            boolean isCompressed()
            {
                return (token & 0xF0) == 0x20;
            }

            int maxUncompressedLength()
            {
                return 1 << ((token & 0x0F) + 10);
            }

            void read(ByteBuffer in, int begin)
            {
                magicNumber        =              in.getLong(begin + MAGIC_NUMBER_OFFSET        );
                token              =              in.get    (begin + TOKEN_OFFSET               );
                compressedLength   = reverseBytes(in.getInt (begin + COMPRESSED_LENGTH_OFFSET  ));
                uncompressedLength = reverseBytes(in.getInt (begin + UNCOMPRESSED_LENGTH_OFFSET));
                checksum           = reverseBytes(in.getInt (begin + CHECKSUM_OFFSET           ));
            }

            void validate() throws CorruptLZ4Frame
            {
                if (magicNumber != MAGIC_NUMBER)
                    except("Invalid magic number at the beginning of an LZ4 block: %d", magicNumber);

                int blockType = token & 0xF0;
                if (!(blockType == BLOCK_TYPE_COMPRESSED || blockType == BLOCK_TYPE_NON_COMPRESSED))
                    except("Invalid block type encountered: %d", blockType);

                if (compressedLength < 0 || compressedLength > MAX_BLOCK_LENGTH)
                    except("Invalid compressedLength: %d (expected: 0-%d)", compressedLength, MAX_BLOCK_LENGTH);

                if (uncompressedLength < 0 || uncompressedLength > maxUncompressedLength())
                    except("Invalid uncompressedLength: %d (expected: 0-%d)", uncompressedLength, maxUncompressedLength());

                if (   uncompressedLength == 0 && compressedLength != 0
                    || uncompressedLength != 0 && compressedLength == 0
                    || !isCompressed() && uncompressedLength != compressedLength)
                {
                    except("Stream corrupted: compressedLength(%d) and decompressedLength(%d) mismatch", compressedLength, uncompressedLength);
                }
            }
        }
        private final Header header = new Header();

        /**
         * @return {@code in} if has sufficient capacity, otherwise a replacement from {@code BufferPool} that {@code in} is copied into
         */
        private ByteBuffer ensureCapacity(ByteBuffer in, int capacity)
        {
            if (in.capacity() >= capacity)
                return in;

            ByteBuffer out = allocator.getAtLeast(capacity);
            in.flip();
            out.put(in);
            bufferPool.put(in);
            return out;
        }

        private ByteBuffer stash;

        private void stash(ShareableBytes in, int stashLength, int begin, int length)
        {
            ByteBuffer out = allocator.getAtLeast(stashLength);
            copyBytes(in.get(), begin, out, 0, length);
            out.position(length);
            stash = out;
        }

        static final class CorruptLZ4Frame extends IOException
        {
            CorruptLZ4Frame(String message)
            {
                super(message);
            }
        }

        private static void except(String format, Object... args) throws CorruptLZ4Frame
        {
            throw new CorruptLZ4Frame(format(format, args));
        }
    }
}
