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

package org.apache.cassandra.transport.frame.checksum;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Random;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.frame.compress.Compressor;
import org.apache.cassandra.transport.frame.compress.LZ4Compressor;
import org.apache.cassandra.transport.frame.compress.SnappyCompressor;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;

import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.*;

public class ChecksummingTransformerTest
{
    private static final int DEFAULT_BLOCK_SIZE = 1 << 15;
    private static final int MAX_INPUT_SIZE = 1 << 18;
    private static final EnumSet<Frame.Header.Flag> FLAGS = EnumSet.of(Frame.Header.Flag.COMPRESSED, Frame.Header.Flag.CHECKSUMMED);

    @BeforeClass
    public static void init()
    {
        // required as static ChecksummingTransformer instances read default block size from config
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void roundTripSafetyProperty()
    {
        qt().withExamples(500)
            .forAll(inputs(),
                    compressors(),
                    checksumTypes(),
                    blocksizes())
            .checkAssert(this::roundTrip);
    }

    @Test
    public void roundTripZeroLengthInput()
    {
        qt().withExamples(20)
            .forAll(zeroLengthInputs(),
                    compressors(),
                    checksumTypes(),
                    blocksizes())
            .checkAssert(this::roundTrip);
    }

    @Test
    public void corruptionCausesFailure()
    {
        qt().withExamples(500)
            .forAll(inputWithCorruptablePosition(),
                    integers().between(0, Byte.MAX_VALUE).map(Integer::byteValue),
                    compressors(),
                    checksumTypes())
            .checkAssert(this::roundTripWithCorruption);
    }

    private void roundTripWithCorruption(Pair<String, Integer> inputAndCorruptablePosition,
                                         byte corruptionValue,
                                         Compressor compressor,
                                         ChecksumType checksum)
    {
        String input = inputAndCorruptablePosition.left;
        ByteBuf expectedBuf = Unpooled.wrappedBuffer(input.getBytes());
        int byteToCorrupt = inputAndCorruptablePosition.right;
        ChecksummingTransformer transformer = new ChecksummingTransformer(checksum, DEFAULT_BLOCK_SIZE, compressor);
        ByteBuf outbound = transformer.transformOutbound(expectedBuf);

        // make sure we're actually expecting to produce some corruption
        if (outbound.getByte(byteToCorrupt) == corruptionValue)
            return;

        if (byteToCorrupt >= outbound.writerIndex())
            return;

        try
        {
            int oldIndex = outbound.writerIndex();
            outbound.writerIndex(byteToCorrupt);
            outbound.writeByte(corruptionValue);
            outbound.writerIndex(oldIndex);
            ByteBuf inbound = transformer.transformInbound(outbound, FLAGS);

            // verify that the content was actually corrupted
            expectedBuf.readerIndex(0);
            Assert.assertEquals(expectedBuf, inbound);
        } catch(ProtocolException e)
        {
            return;
        }

    }

    @Test
    public void roundTripWithSingleUncompressableChunk()
    {
        byte[] bytes = new byte[]{1};
        ChecksummingTransformer transformer = new ChecksummingTransformer(ChecksumType.CRC32, DEFAULT_BLOCK_SIZE, LZ4Compressor.INSTANCE);
        ByteBuf expectedBuf = Unpooled.wrappedBuffer(bytes);

        ByteBuf outbound = transformer.transformOutbound(expectedBuf);
        ByteBuf inbound = transformer.transformInbound(outbound, FLAGS);

        // reset reader index on expectedBuf back to 0 as it will have been entirely consumed by the transformOutbound() call
        expectedBuf.readerIndex(0);
        Assert.assertEquals(expectedBuf, inbound);
    }

    @Test
    public void roundTripWithCompressableAndUncompressableChunks() throws IOException
    {
        Compressor compressor = LZ4Compressor.INSTANCE;
        Random random = new Random();
        int inputLen = 127;

        byte[] uncompressable = new byte[inputLen];
        for (int i = 0; i < uncompressable.length; i++)
            uncompressable[i] = (byte) random.nextInt(127);

        byte[] compressed = new byte[compressor.maxCompressedLength(uncompressable.length)];
        Assert.assertTrue(compressor.compress(uncompressable, 0, uncompressable.length, compressed, 0) > uncompressable.length);

        byte[] compressable = new byte[inputLen];
        for (int i = 0; i < compressable.length; i++)
            compressable[i] = (byte)1;
        Assert.assertTrue(compressor.compress(compressable, 0, compressable.length, compressable, 0) < compressable.length);

        ChecksummingTransformer transformer = new ChecksummingTransformer(ChecksumType.CRC32, uncompressable.length, LZ4Compressor.INSTANCE);
        byte[] expectedBytes = new byte[inputLen * 3];
        ByteBuf expectedBuf = Unpooled.wrappedBuffer(expectedBytes);
        expectedBuf.writerIndex(0);
        expectedBuf.writeBytes(uncompressable);
        expectedBuf.writeBytes(uncompressable);
        expectedBuf.writeBytes(compressable);

        ByteBuf outbound = transformer.transformOutbound(expectedBuf);
        ByteBuf inbound = transformer.transformInbound(outbound, FLAGS);

        // reset reader index on expectedBuf back to 0 as it will have been entirely consumed by the transformOutbound() call
        expectedBuf.readerIndex(0);
        Assert.assertEquals(expectedBuf, inbound);
    }

    private void roundTrip(String input, Compressor compressor, ChecksumType checksum, int blockSize)
    {
        ChecksummingTransformer transformer = new ChecksummingTransformer(checksum, blockSize, compressor);
        byte[] expectedBytes = input.getBytes();
        ByteBuf expectedBuf = Unpooled.wrappedBuffer(expectedBytes);

        ByteBuf outbound = transformer.transformOutbound(expectedBuf);
        ByteBuf inbound = transformer.transformInbound(outbound, FLAGS);

        // reset reader index on expectedBuf back to 0 as it will have been entirely consumed by the transformOutbound() call
        expectedBuf.readerIndex(0);
        Assert.assertEquals(expectedBuf, inbound);
    }

    private Gen<Pair<String, Integer>> inputWithCorruptablePosition()
    {
        // we only generate corruption for byte 2 onward. This is to skip introducing corruption in the number
        // of chunks (which isn't checksummed
        return inputs().flatMap(s -> integers().between(2, s.length() + 2).map(i -> Pair.create(s, i)));
    }

    private Gen<String> inputs()
    {
        Gen<String> randomStrings = strings().basicMultilingualPlaneAlphabet().ofLengthBetween(0, MAX_INPUT_SIZE);
        Gen<String> highlyCompressable = strings().betweenCodePoints('c', 'e').ofLengthBetween(1, MAX_INPUT_SIZE);
        return randomStrings.mix(highlyCompressable, 50);
    }

    private Gen<String> zeroLengthInputs()
    {
        return strings().ascii().ofLength(0);
    }

    private Gen<Compressor> compressors()
    {
        return arbitrary().pick(null, LZ4Compressor.INSTANCE, SnappyCompressor.INSTANCE);
    }

    private Gen<ChecksumType> checksumTypes()
    {
        return arbitrary().enumValuesWithNoOrder(ChecksumType.class);
    }

    private Gen<Integer> blocksizes()
    {
        return arbitrary().constant(DEFAULT_BLOCK_SIZE);
    }

}
