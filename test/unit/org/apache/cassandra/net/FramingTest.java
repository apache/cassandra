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
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.BufferPools;
import org.apache.cassandra.utils.vint.VIntCoding;

import static java.lang.Math.*;
import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.MessagingService.minimum_version;
import static org.apache.cassandra.net.OutboundConnections.LARGE_MESSAGE_THRESHOLD;
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
                out.writeUnsignedVInt(t.length);
                out.write(t);
            }

            public byte[] deserialize(DataInputPlus in, int version) throws IOException
            {
                byte[] r = new byte[(int) in.readUnsignedVInt()];
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
    public void burnRandomLegacy()
    {
        burnRandomLegacy(1000);
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

    private void burnRandomLegacy(int count)
    {
        SecureRandom seed = new SecureRandom();
        Random random = new Random();
        for (int i = 0 ; i < count ; ++i)
        {
            long innerSeed = seed.nextLong();
            float ratio = seed.nextFloat();
            int version = minimum_version + random.nextInt(1 + current_version - minimum_version);
            logger.debug("seed: {}, ratio: {}, version: {}", innerSeed, ratio, version);
            random.setSeed(innerSeed);
            testRandomSequenceOfMessages(random, ratio, version, new FrameDecoderLegacy(GlobalBufferPoolAllocator.instance, version));
        }
    }

    @Test
    public void testRandomLegacy()
    {
        testRandomLegacy(250);
    }

    private void testRandomLegacy(int count)
    {
        SecureRandom seeds = new SecureRandom();
        for (int messagingVersion : new int[] { VERSION_30, VERSION_3014, current_version})
        {
            FrameDecoder decoder = new FrameDecoderLegacy(GlobalBufferPoolAllocator.instance, messagingVersion);
            testSomeMessages(seeds.nextLong(), count, 0.0f, messagingVersion, decoder);
            testSomeMessages(seeds.nextLong(), count, 0.1f, messagingVersion, decoder);
            testSomeMessages(seeds.nextLong(), count, 0.95f, messagingVersion, decoder);
            testSomeMessages(seeds.nextLong(), count, 1.0f, messagingVersion, decoder);
        }
    }

    private void testSomeMessages(long seed, int count, float largeRatio, int messagingVersion, FrameDecoder decoder)
    {
        logger.info("seed: {}, iterations: {}, largeRatio: {}, messagingVersion: {}, decoder: {}", seed, count, largeRatio, messagingVersion, decoder.getClass().getSimpleName());
        Random random = new Random(seed);
        for (int i = 0 ; i < count ; ++i)
        {
            long innerSeed = random.nextLong();
            logger.debug("inner seed: {}, iteration: {}", innerSeed, i);
            random.setSeed(innerSeed);
            testRandomSequenceOfMessages(random, largeRatio, messagingVersion, decoder);
        }
    }

    private void testRandomSequenceOfMessages(Random random, float largeRatio, int messagingVersion, FrameDecoder decoder)
    {
        SequenceOfFrames sequenceOfMessages = sequenceOfMessages(random, largeRatio, messagingVersion);

        List<byte[]> messages = sequenceOfMessages.original;
        ShareableBytes stream = sequenceOfMessages.frames;

        int end = stream.get().limit();
        List<FrameDecoder.Frame> out = new ArrayList<>();

        int messageStart = 0;
        int messageIndex = 0;
        for (int i = 0 ; i < end ; )
        {
            int limit = i + random.nextInt(1 + end - i);
            decoder.decode(out, stream.slice(i, limit));

            int outIndex = 0;
            byte[] message = messages.get(messageIndex);
            if (i > messageStart)
            {
                int start;
                if (message.length <= LARGE_MESSAGE_THRESHOLD)
                {
                    start = 0;
                }
                else  if (!lengthIsReadable(message, i - messageStart, messagingVersion))
                {
                    // we should have an initial frame containing only some prefix of the message (probably 64 bytes)
                    // that was stashed only to decide how big the message was
                    FrameDecoder.IntactFrame frame = (FrameDecoder.IntactFrame) out.get(outIndex++);
                    Assert.assertFalse(frame.isSelfContained);
                    start = frame.contents.remaining();
                    verify(message, 0, frame.contents.remaining(), frame.contents);
                }
                else
                {
                    start = i - messageStart;
                }

                if (limit >= message.length + messageStart)
                {
                    FrameDecoder.IntactFrame frame = (FrameDecoder.IntactFrame) out.get(outIndex++);
                    Assert.assertEquals(start == 0, frame.isSelfContained);
                    // verify remainder of a large message, or a single fully stashed small message
                    verify(message, start, message.length, frame.contents);

                    messageStart += message.length;
                    if (++messageIndex < messages.size())
                        message = messages.get(messageIndex);
                }
                else if (message.length > LARGE_MESSAGE_THRESHOLD)
                {
                    FrameDecoder.IntactFrame frame = (FrameDecoder.IntactFrame) out.get(outIndex++);
                    Assert.assertFalse(frame.isSelfContained);
                    // verify next portion of a large message
                    verify(message, start, limit - messageStart, frame.contents);

                    Assert.assertEquals(outIndex, out.size());
                    for (FrameDecoder.Frame f : out)
                        f.release();
                    out.clear();
                    i = limit;
                    continue;
                }
            }

            // message is fresh
            int beginFrameIndex = messageIndex;
            while (messageStart + message.length <= limit)
            {
                messageStart += message.length;
                if (++messageIndex < messages.size())
                    message = messages.get(messageIndex);
            }

            if (beginFrameIndex < messageIndex)
            {
                FrameDecoder.IntactFrame frame = (FrameDecoder.IntactFrame) out.get(outIndex++);
                Assert.assertTrue(frame.isSelfContained);
                while (beginFrameIndex < messageIndex)
                {
                    byte[] m = messages.get(beginFrameIndex);
                    ShareableBytes bytesToVerify = frame.contents.sliceAndConsume(m.length);
                    verify(m, bytesToVerify);
                    bytesToVerify.release();
                    ++beginFrameIndex;
                }
                Assert.assertFalse(frame.contents.hasRemaining());
            }

            if (limit > messageStart
                && message.length > LARGE_MESSAGE_THRESHOLD
                && lengthIsReadable(message, limit - messageStart, messagingVersion))
            {
                FrameDecoder.IntactFrame frame = (FrameDecoder.IntactFrame) out.get(outIndex++);
                Assert.assertFalse(frame.isSelfContained);
                verify(message, 0, limit - messageStart, frame.contents);
            }

            Assert.assertEquals(outIndex, out.size());
            for (FrameDecoder.Frame frame : out)
                frame.release();
            out.clear();

            i = limit;
        }
        stream.release();
        Assert.assertTrue(stream.isReleased());
        Assert.assertNull(decoder.stash);
        Assert.assertTrue(decoder.frames.isEmpty());
    }

    private static boolean lengthIsReadable(byte[] message, int limit, int messagingVersion)
    {
        try
        {
            return Message.serializer.inferMessageSize(ByteBuffer.wrap(message), 0, limit, messagingVersion) >= 0;
        }
        catch (Message.InvalidLegacyProtocolMagic e)
        {
            throw new IllegalStateException(e);
        }
    }

    private static SequenceOfFrames sequenceOfMessages(Random random, float largeRatio, int messagingVersion)
    {
        int messageCount = 1 + random.nextInt(63);
        List<byte[]> messages = new ArrayList<>();
        int[] cumulativeLength = new int[messageCount];
        for (int i = 0 ; i < messageCount ; ++i)
        {
            byte[] payload;
            if (random.nextFloat() < largeRatio) payload = randomishBytes(random, 1 << 16, 1 << 17);
            else payload = randomishBytes(random, 1, 1 << 16);
            Message<byte[]> messageObj = Message.out(Verb._TEST_1, payload);

            byte[] message;
            try (DataOutputBuffer out = new DataOutputBuffer(messageObj.serializedSize(messagingVersion)))
            {
                Message.serializer.serialize(messageObj, out, messagingVersion);
                message = out.toByteArray();
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }
            messages.add(message);

            cumulativeLength[i] = (i == 0 ? 0 : cumulativeLength[i - 1]) + message.length;
        }

        ByteBuffer frames = BufferPools.forNetworking().getAtLeast(cumulativeLength[messageCount - 1], BufferType.OFF_HEAP);
        for (byte[] buffer : messages)
            frames.put(buffer);
        frames.flip();
        return new SequenceOfFrames(messages, cumulativeLength, frames);
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
