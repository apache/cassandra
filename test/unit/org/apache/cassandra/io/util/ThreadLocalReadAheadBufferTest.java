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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.WithQuickTheories;
import org.quicktheories.core.Gen;

import static java.lang.Math.max;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_IO_TMPDIR;

public class ThreadLocalReadAheadBufferTest implements WithQuickTheories
{
    private static final int numFiles = 5;
    private static final File[] files = new File[numFiles];
    private static final Logger logger = LoggerFactory.getLogger(ThreadLocalReadAheadBufferTest.class);
    private static Integer seed;

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

    private void testReads(InputData propertyInputs)
    {
        try (ChannelProxy channel = new ChannelProxy(propertyInputs.file))
        {
            ThreadLocalReadAheadBuffer trlab = new ThreadLocalReadAheadBuffer(channel, new DataStorageSpec.IntKibibytesBound("256KiB").toBytes(), BufferType.OFF_HEAP);
            for (Pair<Long, Integer> read : propertyInputs.positionsAndLengths)
            {
                int readSize = Math.min(read.right,(int) (channel.size() - read.left));
                ByteBuffer buf1 = ByteBuffer.allocate(readSize);
                channel.read(buf1, read.left);

                ByteBuffer buf2 = ByteBuffer.allocate(readSize);
                try
                {
                    int copied = 0;
                    while (copied < readSize) {
                        trlab.fill(read.left + copied);
                        int leftToRead = readSize - copied;
                        if (trlab.remaining() >= leftToRead)
                            copied += trlab.read(buf2, leftToRead);
                        else
                            copied += trlab.read(buf2, trlab.remaining());
                    }
                }
                catch (CorruptSSTableException e)
                {
                    throw new RuntimeException(e);
                }

                Assert.assertEquals(buf1, buf2);
            }
        }
    }

    private Gen<InputData> reads()
    {
        return arbitrary().pick(List.of(files))
                          .flatMap((file) ->
                                   lists().of(longs().between(0, fileSize(file)).zip(integers().between(1, 100), Pair::create))
                                          .ofSizeBetween(5, 10)
                                          .map(positionsAndLengths -> new InputData(file, positionsAndLengths)));

    }

    private Gen<InputData> lastBlockReads()
    {
        int blockSize = new DataStorageSpec.IntKibibytesBound("256KiB").toBytes();
        return arbitrary().pick(List.of(files))
                         .flatMap((file) ->
                                  lists().of(longs().between(max(0, fileSize(file) - blockSize), fileSize(file)).zip(integers().between(1, 100), Pair::create))
                                         .ofSizeBetween(5, 10)
                                         .map(positionsAndLengths -> new InputData(file, positionsAndLengths)));

    }

    // need this becasue generators don't handle the IOException
    private long fileSize(File file)
    {
        try
        {
            return Files.size(file.toPath());
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static class InputData
    {

        private final File file;
        private final List<Pair<Long, Integer>> positionsAndLengths;

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
