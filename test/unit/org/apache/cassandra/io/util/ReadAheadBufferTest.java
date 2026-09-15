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

package org.apache.cassandra.io.util;


import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quicktheories.WithQuickTheories;
import org.quicktheories.core.Gen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.Pair;

import static java.lang.Math.max;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_IO_TMPDIR;
import static org.junit.Assert.*;

public class ReadAheadBufferTest implements WithQuickTheories
{
    private static final int numFiles = 5;
    private static final Logger logger = LoggerFactory.getLogger(ReadAheadBufferTest.class);
    protected static final File[] files = new File[numFiles];
    protected static Integer seed;

    @BeforeClass
    public static void setup()
    {
        seed = new Random().nextInt();
        logger.info("Seed: {}", seed);

        for (int i = 0; i < numFiles; i++)
        {
            int size = new Random(seed).nextInt((Integer.MAX_VALUE - 1) / 8);
            files[i] = writeFile(seed, size);
        }
    }

    @AfterClass
    public static void cleanup()
    {
        for (File f : files)
        {
            try
            {
                f.delete();
            }
            catch (Exception e)
            {
                // ignore
            }
        }
    }

    @Test
    public void testLastBlockReads()
    {
        qt().withFixedSeed(seed).forAll(lastBlockReads())
            .checkAssert(this::testReads);
    }

    @Test
    public void testReadsLikeChannelProxy()
    {
        qt().withFixedSeed(seed).forAll(reads())
            .checkAssert(this::testReads);
    }

    @Test
    public void allocateInitialisesBufferSizeFromCapacity()
    {
        int bufferSize = new DataStorageSpec.IntKibibytesBound("256KiB").toBytes();
        try (ChannelProxy channel = new ChannelProxy(files[0]))
        {
            ReadAheadBuffer buffer = new ReadAheadBuffer(channel, bufferSize, BufferType.OFF_HEAP);
            try
            {
                assertFalse(buffer.hasBuffer());
                buffer.allocateBuffer();

                // Allocation must record the actual buffer size, not leave it unset.
                assertTrue(buffer.hasBuffer());
                assertEquals("allocate must initialise bufferSize from capacity", bufferSize, buffer.bufferSize());
            }
            finally
            {
                buffer.close();
            }
        }
    }

    /**
     * Two instances over the same file must not share buffer state.
     */
    @Test
    public void independentInstancesDoNotShareBuffer() throws CorruptBlockException
    {
        
        File file = files[0];
        int bufferSize = new DataStorageSpec.IntKibibytesBound("256KiB").toBytes();
        try (ChannelProxy channel = new ChannelProxy(file))
        {
            ReadAheadBuffer a = new ReadAheadBuffer(channel, bufferSize, BufferType.OFF_HEAP);
            ReadAheadBuffer b = new ReadAheadBuffer(channel, bufferSize, BufferType.OFF_HEAP);
            try
            {
                a.fill(0);

                // Advance b to a different block if the file is large enough; else keep it on block 0.
                long secondBlock = bufferSize;
                b.fill(channel.size() > secondBlock ? secondBlock : 0);

                // a must still read block 0. A shared buffer would return b's block here.
                int readSize = Math.min(100, (int) channel.size());
                ByteBuffer expected = ByteBuffer.allocate(readSize);
                channel.read(expected, 0);
                expected.flip();

                ByteBuffer actual = ByteBuffer.allocate(readSize);
                a.read(actual, readSize);
                actual.flip();

                assertEquals(expected, actual);
            }
            finally
            {
                b.close();
                a.close();
            }
        }
    }

    @Test
    public void closeFreesBufferAndIsIdempotent() throws CorruptBlockException
    {
        int bufferSize = new DataStorageSpec.IntKibibytesBound("256KiB").toBytes();
        try (ChannelProxy channel = new ChannelProxy(files[0]))
        {
            ReadAheadBuffer buffer = new ReadAheadBuffer(channel, bufferSize, BufferType.OFF_HEAP);
            buffer.allocateBuffer();
            assertTrue(buffer.hasBuffer());

            buffer.close();
            assertFalse("close must free the owned buffer", buffer.hasBuffer());

            // A second close must not double-free.
            buffer.close();
            assertFalse(buffer.hasBuffer());
        }
    }

    protected void testReads(InputData propertyInputs)
    {
        try (ChannelProxy channel = new ChannelProxy(propertyInputs.file);
             ReadAheadBuffer rab = new ReadAheadBuffer(channel, new DataStorageSpec.IntKibibytesBound("256KiB").toBytes(), BufferType.OFF_HEAP); )
        {
            for (Pair<Long, Integer> read : propertyInputs.positionsAndLengths)
            {
                testRead(read, channel, rab);
            }
        }
    }

    protected static void testRead(Pair<Long, Integer> read, ChannelProxy bufferedChannel, ReadAheadBuffer rab)
    {
        int readSize = Math.min(read.right, (int) (bufferedChannel.size() - read.left));
        ByteBuffer buf1 = ByteBuffer.allocate(readSize);
        bufferedChannel.read(buf1, read.left);

        ByteBuffer buf2 = ByteBuffer.allocate(readSize);
        try
        {
            int copied = 0;
            while (copied < readSize)
            {
                rab.fill(read.left + copied);
                int leftToRead = readSize - copied;
                if (rab.remaining() >= leftToRead)
                    copied += rab.read(buf2, leftToRead);
                else
                    copied += rab.read(buf2, rab.remaining());
            }
        }
        catch (CorruptSSTableException | CorruptBlockException e)
        {
            throw new RuntimeException(e);
        }

        assertEquals(buf1, buf2);
    }

    protected Gen<InputData> reads()
    {
        return arbitrary().pick(List.of(files))
                          .flatMap((file) ->
                                   lists().of(longs().between(0, fileSize(file)).zip(integers().between(1, 100), Pair::create))
                                          .ofSizeBetween(5, 10)
                                          .map(positionsAndLengths -> new InputData(file, positionsAndLengths)));
    }

    protected Gen<InputData> lastBlockReads()
    {
        int blockSize = new DataStorageSpec.IntKibibytesBound("256KiB").toBytes();
        return arbitrary().pick(List.of(files))
                          .flatMap((file) ->
                                   lists().of(longs().between(max(0, fileSize(file) - blockSize), fileSize(file)).zip(integers().between(1, 100), Pair::create))
                                          .ofSizeBetween(5, 10)
                                          .map(positionsAndLengths -> new InputData(file, positionsAndLengths)));
    }

    private long fileSize(File file)
    {
        try
        {
            return Files.size(file.toPath());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected static class InputData
    {

        protected final File file;
        protected final List<Pair<Long, Integer>> positionsAndLengths;

        public InputData(File file, List<Pair<Long, Integer>> positionsAndLengths)
        {
            this.file = file;
            this.positionsAndLengths = positionsAndLengths;
        }
    }

    private static File writeFile(int seed, int length)
    {
        String fileName = "data+" + length + ".bin";

        byte[] dataChunk = new byte[4096 * 8];
        java.util.Random random = new Random(seed);
        int writtenData = 0;

        File file = new File(JAVA_IO_TMPDIR.getString(), fileName);
        try (FileOutputStream fos = new FileOutputStream(file.toJavaIOFile()))
        {
            while (writtenData < length)
            {
                random.nextBytes(dataChunk);
                int toWrite = Math.min((length - writtenData), dataChunk.length);
                fos.write(dataChunk, 0, toWrite);
                writtenData += toWrite;
            }
            fos.flush();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        return file;
    }
}
