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

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.utils.Pair;

public class DirectThreadLocalReadAheadBufferTest extends ThreadLocalReadAheadBufferTest
{

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
            ThreadLocalReadAheadBuffer tlrab = new DirectThreadLocalReadAheadBuffer(directChannel, bufferSize, blockSize);
            for (Pair<Long, Integer> read : propertyInputs.positionsAndLengths)
            {
                testRead(read, bufferedChannel, tlrab);
            }
            tlrab.close();
        }
    }
}