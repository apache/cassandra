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
import java.nio.ByteOrder;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.zip.CRC32;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.FrameDecoder.Frame;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.BufferPools;
import org.apache.cassandra.utils.vint.VIntCoding;

import io.netty.buffer.ByteBuf;

import static java.lang.Math.min;
import static org.apache.cassandra.net.ShareableBytes.wrap;

// TODO: test corruption
// TODO: use a different random seed each time
// TODO: use quick theories
public class FramingTest
{
    private static final Logger logger = LoggerFactory.getLogger(FramingTest.class);

    @BeforeClass
    public static void begin() throws NoSuchFieldException, IllegalAccessException
    {
        DatabaseDescriptor.daemonInitialization();
        Verb._TEST_1.unsafeSetSerializer(() -> new IVersionedSerializer<byte[]>()
        {

            public void serialize(byte[] t, DataOutputPlus out, int version) throws IOException
            {
                out.writeUnsignedVInt32(t.length);
                out.write(t);
            }

            public byte[] deserialize(DataInputPlus in, int version) throws IOException
            {
                byte[] r = new byte[in.readUnsignedVInt32()];
                in.readFully(r);
                return r;
            }

            public long serializedSize(byte[] t, int version)
            {
                return VIntCoding.computeUnsignedVIntSize(t.length) + t.length;
            }
        });
    }

    @AfterClass
    public static void after() throws NoSuchFieldException, IllegalAccessException
    {
        Verb._TEST_1.unsafeSetSerializer(() -> null);
    }

    private static class SequenceOfFrames
    {
        final List<byte[]> original;
        final int[] boundaries;
        final ShareableBytes frames;

        private SequenceOfFrames(List<byte[]> original, int[] boundaries, ByteBuffer frames)
        {
            this.original = original;
            this.boundaries = boundaries;
            this.frames = wrap(frames);
        }
    }

    @Test
    public void testRandomLZ4()
    {
        testSomeFrames(FrameEncoderLZ4.fastInstance, FrameDecoderLZ4.fast(GlobalBufferPoolAllocator.instance));
    }

    @Test
    public void testRandomCrc()
    {
        testSomeFrames(FrameEncoderCrc.instance, FrameDecoderCrc.create(GlobalBufferPoolAllocator.instance));
    }

    @Test
    public void testLZ4FrameWithInconsistentUncompressedLengthIsRejected()
    {
        GlobalBufferPoolAllocator allocator = GlobalBufferPoolAllocator.instance;

        int declaredLength = 4096;
        byte[] payload = new byte[16]; // decodes to fewer bytes than the header declares

        ByteBuffer frame = lz4FrameWithDeclaredLength(payload, declaredLength);

        FrameDecoderLZ4 decoder = FrameDecoderLZ4.fast(allocator);
        Collection<Frame> out = new ArrayDeque<>();
        decoder.decode(out, wrap(frame));

        Assert.assertEquals(1, out.size());
        Assert.assertTrue("frame with inconsistent uncompressed length should be rejected",
                          out.iterator().next() instanceof FrameDecoder.CorruptFrame);
    }

    private static ByteBuffer lz4FrameWithDeclaredLength(byte[] payload, int declaredLength)
    {
        int headerLength = 8;
        int trailerLength = 4;

        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
        int maxOut = compressor.maxCompressedLength(payload.length);
        byte[] block = new byte[maxOut];
        int compressedLength = compressor.compress(payload, 0, payload.length, block, 0, maxOut);
        Assert.assertTrue(compressedLength < declaredLength);

        ByteBuffer frame = ByteBuffer.allocate(headerLength + compressedLength + trailerLength)
                                     .order(ByteOrder.LITTLE_ENDIAN);

        long header5b = (compressedLength & 0x1FFFFL) | ((declaredLength & 0x1FFFFL) << 17);
        header5b |= 1L << 34; // isSelfContained
        long crc24 = Crc.crc24(header5b, 5) & 0xFFFFFF;
        frame.putLong(0, header5b | (crc24 << 40));

        frame.position(headerLength);
        frame.put(block, 0, compressedLength);

        CRC32 crc32 = Crc.crc32();
        crc32.update(block, 0, compressedLength);
        frame.putInt(headerLength + compressedLength, (int) crc32.getValue());

        frame.clear();
        return frame;
    }

    private void testSomeFrames(FrameEncoder encoder, FrameDecoder decoder)
    {
        long seed = new SecureRandom().nextLong();
        logger.info("seed: {}, decoder: {}", seed, decoder.getClass().getSimpleName());
        Random random = new Random(seed);
        for (int i = 0 ; i < 1000 ; ++i)
            testRandomSequenceOfFrames(random, encoder, decoder);
    }

    private void testRandomSequenceOfFrames(Random random, FrameEncoder encoder, FrameDecoder decoder)
    {
        SequenceOfFrames sequenceOfFrames = sequenceOfFrames(random, encoder);

        List<byte[]> uncompressed = sequenceOfFrames.original;
        ShareableBytes frames = sequenceOfFrames.frames;
        int[] boundaries = sequenceOfFrames.boundaries;

        int end = frames.get().limit();
        List<FrameDecoder.Frame> out = new ArrayList<>();
        int prevBoundary = -1;
        for (int i = 0 ; i < end ; )
        {
            int limit = i + random.nextInt(1 + end - i);
            decoder.decode(out, frames.slice(i, limit));
            int boundary = Arrays.binarySearch(boundaries, limit);
            if (boundary < 0) boundary = -2 -boundary;

            while (prevBoundary < boundary)
            {
                ++prevBoundary;
                Assert.assertTrue(out.size() >= 1 + prevBoundary);
                verify(uncompressed.get(prevBoundary), ((FrameDecoder.IntactFrame) out.get(prevBoundary)).contents);
            }
            i = limit;
        }
        for (FrameDecoder.Frame frame : out)
            frame.release();
        frames.release();
        Assert.assertNull(decoder.stash);
        Assert.assertTrue(decoder.frames.isEmpty());
    }

    private static void verify(byte[] expect, ShareableBytes actual)
    {
        verify(expect, 0, expect.length, actual);
    }

    private static void verify(byte[] expect, int start, int end, ShareableBytes actual)
    {
        byte[] fetch = new byte[end - start];
        Assert.assertEquals(end - start, actual.remaining());
        actual.get().get(fetch);
        boolean equals = true;
        for (int i = start ; equals && i < end ; ++i)
            equals = expect[i] == fetch[i - start];
        if (!equals)
            Assert.assertArrayEquals(Arrays.copyOfRange(expect, start, end), fetch);
    }

    private static SequenceOfFrames sequenceOfFrames(Random random, FrameEncoder encoder)
    {
        int frameCount = 1 + random.nextInt(8);
        List<byte[]> uncompressed = new ArrayList<>();
        List<ByteBuf> compressed = new ArrayList<>();
        int[] cumulativeCompressedLength = new int[frameCount];
        for (int i = 0 ; i < frameCount ; ++i)
        {
            byte[] bytes = randomishBytes(random, 1, 1 << 15);
            uncompressed.add(bytes);

            FrameEncoder.Payload payload = encoder.allocator().allocate(true, bytes.length);
            payload.buffer.put(bytes);
            payload.finish();

            ByteBuf buffer = encoder.encode(true, payload.buffer);
            compressed.add(buffer);
            cumulativeCompressedLength[i] = (i == 0 ? 0 : cumulativeCompressedLength[i - 1]) + buffer.readableBytes();
        }

        ByteBuffer frames = BufferPools.forNetworking().getAtLeast(cumulativeCompressedLength[frameCount - 1], BufferType.OFF_HEAP);
        for (ByteBuf buffer : compressed)
        {
            frames.put(buffer.internalNioBuffer(buffer.readerIndex(), buffer.readableBytes()));
            buffer.release();
        }
        frames.flip();
        return new SequenceOfFrames(uncompressed, cumulativeCompressedLength, frames);
    }

    @Test
    public void testSerializeSizeMatchesEdgeCases() // See CASSANDRA-16103
    {
        int v40 = MessagingService.Version.VERSION_40.value;
        Consumer<Long> subTest = timeGapInMillis ->
        {
            long createdAt = 0;
            long expiresAt = createdAt + TimeUnit.MILLISECONDS.toNanos(timeGapInMillis);
            Message<NoPayload> message = Message.builder(Verb.READ_REPAIR_RSP, NoPayload.noPayload)
                                                .from(FBUtilities.getBroadcastAddressAndPort())
                                                .withCreatedAt(createdAt)
                                                .withExpiresAt(expiresAt)
                                                .build();

            try (DataOutputBuffer out = new DataOutputBuffer(20))
            {
                Message.serializer.serialize(message, out, v40);
                Assert.assertEquals(message.serializedSize(v40), out.getLength());
            }
            catch (IOException ioe)
            {
                Assert.fail("Unexpected IOEception during test. " + ioe.getMessage());
            }
        };

        // test cases
        subTest.accept(-1L);
        subTest.accept(1L << 7 - 1);
        subTest.accept(1L << 14 - 1);
    }

    public static byte[] randomishBytes(Random random, int minLength, int maxLength)
    {
        byte[] bytes = new byte[minLength + random.nextInt(Math.max(1, maxLength - minLength))];
        int runLength = 1 + random.nextInt(255);
        for (int i = 0 ; i < bytes.length ; i += runLength)
        {
            byte b = (byte) random.nextInt(256);
            Arrays.fill(bytes, i, min(bytes.length, i + runLength), b);
        }
        return bytes;
    }

}
