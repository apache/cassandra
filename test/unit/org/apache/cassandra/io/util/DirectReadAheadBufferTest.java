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

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.utils.Pair;

public class DirectReadAheadBufferTest extends ReadAheadBufferTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testBufferSizes()
    {
        int blockSize = FileUtils.getFileBlockSize(files[0]);

        for (Integer bufferSize : Arrays.asList(blockSize,                   // block equal alignment
                                                blockSize / 2,               // sub-block alignment
                                                blockSize + (blockSize / 2), // straddling alignment
                                                blockSize * 16))             // multi-block alignment
        {
            qt().withFixedSeed(seed).forAll(reads())
                .checkAssert(propertyInputs -> testReads(propertyInputs, bufferSize, blockSize));
        }
    }

    @Override
    protected void testReads(InputData propertyInputs)
    {
        int blockSize = FileUtils.getFileBlockSize(propertyInputs.file);
        testReads(propertyInputs, blockSize * 64, blockSize);
    }

    private void testReads(InputData propertyInputs, int bufferSize, int blockSize)
    {
        try (ChannelProxy bufferedChannel = new ChannelProxy(propertyInputs.file);
             ChannelProxy directChannel = new ChannelProxy(propertyInputs.file, ChannelProxy.IOMode.DIRECT))
        {
            ReadAheadBuffer rab = new DirectReadAheadBuffer(directChannel, bufferSize, blockSize);
            for (Pair<Long, Integer> read : propertyInputs.positionsAndLengths)
            {
                testRead(read, bufferedChannel, rab);
            }
            rab.close();
        }
    }

    @Test
    public void testDirectMemoryIsCleanedOnClose()
    {
        BufferPoolMXBean directPool = getDirectBufferPool();
        int blockSize = FileUtils.getFileBlockSize(files[0]);
        int bufferSize = 64 * 1024 * 1024; // 64MB - large enough to reliably detect

        try (ChannelProxy channel = new ChannelProxy(files[0], ChannelProxy.IOMode.DIRECT))
        {
            DirectReadAheadBuffer rab =
                new DirectReadAheadBuffer(channel, bufferSize, blockSize);

            // Force buffer allocation
            rab.allocateBuffer();

            long memoryUsedBefore = directPool.getMemoryUsed();

            // Close should clean the direct memory
            rab.close();

            long memoryUsedAfter = directPool.getMemoryUsed();

            // Memory should decrease by approximately buffer size (+ alignment overhead)
            long expectedDecrease = bufferSize;
            long actualDecrease = memoryUsedBefore - memoryUsedAfter;

            Assert.assertTrue(
                "Direct memory should decrease after close(). " +
                "Before: " + memoryUsedBefore + ", After: " + memoryUsedAfter +
                ", Expected decrease: ~" + expectedDecrease + ", Actual: " + actualDecrease,
                actualDecrease >= expectedDecrease * 0.9); // 10% tolerance for alignment
        }
    }

    @Test
    public void testShortReadThrowsCorruptBlockException() throws Exception
    {
        int blockSize = FileUtils.getFileBlockSize(files[0]);
        File file = new File(tempFolder.newFile("direct_read_ahead_short_read.bin"));
        byte[] data = new byte[blockSize * 4];
        new Random(seed).nextBytes(data);
        Files.write(file.toPath(), data);

        try (ChannelProxy channel = new ChannelProxy(file, ChannelProxy.IOMode.DIRECT))
        {
            // The buffer captures the channel size on construction, so truncating afterwards makes the
            // next block load come up short, as it would for an SSTable truncated underneath a scan.
            DirectReadAheadBuffer rab = new DirectReadAheadBuffer(channel, blockSize * 4, blockSize);
            try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE))
            {
                fileChannel.truncate(blockSize + blockSize / 2);
            }

            try
            {
                rab.fill(0);
                Assert.fail("Expected CorruptBlockException for a short read");
            }
            catch (CorruptBlockException e)
            {
                // CorruptBlockException, not CorruptSSTableException, so that CompressedChunkReader.Direct.readChunk
                // resets its output buffer before rethrowing
                Assert.assertTrue(e.getMessage().contains(file.name()));
            }
            finally
            {
                rab.close();
            }
        }
    }

    private static BufferPoolMXBean getDirectBufferPool()
    {
        List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for (BufferPoolMXBean pool : pools)
          if (pool.getName().equals("direct"))
                return pool;

        throw new IllegalStateException("Direct buffer pool not found");
    }

}
