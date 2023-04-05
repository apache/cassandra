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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DiskOptimizationStrategyTest
{
    @Test
    public void testRoundingBufferSize()
    {
        DiskOptimizationStrategy strategy = new SsdDiskOptimizationStrategy(0.95);
        assertEquals(4096, strategy.roundBufferSize(-1L));
        assertEquals(4096, strategy.roundBufferSize(0));
        assertEquals(4096, strategy.roundBufferSize(1));
        assertEquals(4096, strategy.roundBufferSize(2013));
        assertEquals(4096, strategy.roundBufferSize(4095));
        assertEquals(4096, strategy.roundBufferSize(4096));
        assertEquals(8192, strategy.roundBufferSize(4097));
        assertEquals(8192, strategy.roundBufferSize(8191));
        assertEquals(8192, strategy.roundBufferSize(8192));
        assertEquals(12288, strategy.roundBufferSize(8193));
        assertEquals(65536, strategy.roundBufferSize(65535));
        assertEquals(65536, strategy.roundBufferSize(65536));
        assertEquals(65536, strategy.roundBufferSize(65537));
        assertEquals(65536, strategy.roundBufferSize(10000000000000000L));
    }

    @Test
    public void testBufferSize_ssd()
    {
        DiskOptimizationStrategy strategy = new SsdDiskOptimizationStrategy(0.1);

        assertEquals(4096, strategy.bufferSize(0));
        assertEquals(4096, strategy.bufferSize(10));
        assertEquals(4096, strategy.bufferSize(100));
        assertEquals(4096, strategy.bufferSize(4096));
        assertEquals(8192, strategy.bufferSize(4505));   // just < (4096 + 4096 * 0.1)
        assertEquals(12288, strategy.bufferSize(4506));  // just > (4096 + 4096 * 0.1)

        strategy = new SsdDiskOptimizationStrategy(0.5);
        assertEquals(8192, strategy.bufferSize(4506));  // just > (4096 + 4096 * 0.1)
        assertEquals(8192, strategy.bufferSize(6143));  // < (4096 + 4096 * 0.5)
        assertEquals(12288, strategy.bufferSize(6144));  // = (4096 + 4096 * 0.5)
        assertEquals(12288, strategy.bufferSize(6145));  // > (4096 + 4096 * 0.5)

        strategy = new SsdDiskOptimizationStrategy(1.0); // never add a page
        assertEquals(8192, strategy.bufferSize(8191));
        assertEquals(8192, strategy.bufferSize(8192));

        strategy = new SsdDiskOptimizationStrategy(0.0); // always add a page
        assertEquals(8192, strategy.bufferSize(10));
        assertEquals(8192, strategy.bufferSize(4096));
    }

    @Test
    public void testBufferSize_spinning()
    {
        DiskOptimizationStrategy strategy = new SpinningDiskOptimizationStrategy();

        assertEquals(4096, strategy.bufferSize(0));
        assertEquals(8192, strategy.bufferSize(10));
        assertEquals(8192, strategy.bufferSize(100));
        assertEquals(8192, strategy.bufferSize(4096));
        assertEquals(12288, strategy.bufferSize(4097));
    }

    @Test
    public void testRoundUpForCaching()
    {
        assertEquals(4096, DiskOptimizationStrategy.roundForCaching(-1, true));
        assertEquals(4096, DiskOptimizationStrategy.roundForCaching(0, true));
        assertEquals(4096, DiskOptimizationStrategy.roundForCaching(1, true));
        assertEquals(4096, DiskOptimizationStrategy.roundForCaching(4095, true));
        assertEquals(4096, DiskOptimizationStrategy.roundForCaching(4096, true));
        assertEquals(8192, DiskOptimizationStrategy.roundForCaching(4097, true));
        assertEquals(8192, DiskOptimizationStrategy.roundForCaching(4098, true));
        assertEquals(8192, DiskOptimizationStrategy.roundForCaching(8192, true));
        assertEquals(16384, DiskOptimizationStrategy.roundForCaching(8193, true));
        assertEquals(16384, DiskOptimizationStrategy.roundForCaching(12288, true));
        assertEquals(16384, DiskOptimizationStrategy.roundForCaching(16384, true));
        assertEquals(65536, DiskOptimizationStrategy.roundForCaching(65536, true));
        assertEquals(65536, DiskOptimizationStrategy.roundForCaching(65537, true));
        assertEquals(65536, DiskOptimizationStrategy.roundForCaching(131072, true));

        for (int cs = 4096; cs < 65536; cs <<= 1) // 4096, 8192, 12288, ..., 65536
        {
            for (int i = (cs - 4095); i <= cs; i++) // 1 -> 4096, 4097 -> 8192, ...
            {
                assertEquals(cs, DiskOptimizationStrategy.roundForCaching(i, true));
            }
        }
    }

    @Test
    public void testRoundDownForCaching()
    {
        assertEquals(4096, DiskOptimizationStrategy.roundForCaching(-1, false));
        assertEquals(4096, DiskOptimizationStrategy.roundForCaching(0, false));
        assertEquals(4096, DiskOptimizationStrategy.roundForCaching(1, false));
        assertEquals(4096, DiskOptimizationStrategy.roundForCaching(4095, false));
        assertEquals(4096, DiskOptimizationStrategy.roundForCaching(4096, false));
        assertEquals(4096, DiskOptimizationStrategy.roundForCaching(4097, false));
        assertEquals(4096, DiskOptimizationStrategy.roundForCaching(4098, false));
        assertEquals(8192, DiskOptimizationStrategy.roundForCaching(8192, false));
        assertEquals(8192, DiskOptimizationStrategy.roundForCaching(8193, false));
        assertEquals(8192, DiskOptimizationStrategy.roundForCaching(12288, false));
        assertEquals(16384, DiskOptimizationStrategy.roundForCaching(16384, false));
        assertEquals(65536, DiskOptimizationStrategy.roundForCaching(65536, false));
        assertEquals(65536, DiskOptimizationStrategy.roundForCaching(65537, false));
        assertEquals(65536, DiskOptimizationStrategy.roundForCaching(131072, false));

        for (int cs = 4096; cs < 65536; cs <<= 1) // 4096, 8192, 12288, ..., 65536
        {
            for (int i = cs; i < cs * 2 - 1; i++) // 4096 -> 8191, 8192 -> 12287, ...
            {
                assertEquals(cs, DiskOptimizationStrategy.roundForCaching(i, false));
            }
        }
    }
}
