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

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;

public class SegmentedFileTest
{
    @Test
    public void testRoundingBufferSize()
    {
        assertEquals(4096, SegmentedFile.Builder.roundBufferSize(-1L));
        assertEquals(4096, SegmentedFile.Builder.roundBufferSize(0));
        assertEquals(4096, SegmentedFile.Builder.roundBufferSize(1));
        assertEquals(4096, SegmentedFile.Builder.roundBufferSize(2013));
        assertEquals(4096, SegmentedFile.Builder.roundBufferSize(4095));
        assertEquals(4096, SegmentedFile.Builder.roundBufferSize(4096));
        assertEquals(8192, SegmentedFile.Builder.roundBufferSize(4097));
        assertEquals(8192, SegmentedFile.Builder.roundBufferSize(8191));
        assertEquals(8192, SegmentedFile.Builder.roundBufferSize(8192));
        assertEquals(12288, SegmentedFile.Builder.roundBufferSize(8193));
        assertEquals(65536, SegmentedFile.Builder.roundBufferSize(65535));
        assertEquals(65536, SegmentedFile.Builder.roundBufferSize(65536));
        assertEquals(65536, SegmentedFile.Builder.roundBufferSize(65537));
        assertEquals(65536, SegmentedFile.Builder.roundBufferSize(10000000000000000L));
    }

    @Test
    public void testBufferSize_ssd()
    {
        DatabaseDescriptor.setDiskOptimizationStrategy(Config.DiskOptimizationStrategy.ssd);
        DatabaseDescriptor.setDiskOptimizationPageCrossChance(0.1);

        assertEquals(4096, SegmentedFile.Builder.bufferSize(0));
        assertEquals(4096, SegmentedFile.Builder.bufferSize(10));
        assertEquals(4096, SegmentedFile.Builder.bufferSize(100));
        assertEquals(4096, SegmentedFile.Builder.bufferSize(4096));
        assertEquals(8192, SegmentedFile.Builder.bufferSize(4505));   // just < (4096 + 4096 * 0.1)
        assertEquals(12288, SegmentedFile.Builder.bufferSize(4506));  // just > (4096 + 4096 * 0.1)

        DatabaseDescriptor.setDiskOptimizationPageCrossChance(0.5);
        assertEquals(8192, SegmentedFile.Builder.bufferSize(4506));  // just > (4096 + 4096 * 0.1)
        assertEquals(8192, SegmentedFile.Builder.bufferSize(6143));  // < (4096 + 4096 * 0.5)
        assertEquals(12288, SegmentedFile.Builder.bufferSize(6144));  // = (4096 + 4096 * 0.5)
        assertEquals(12288, SegmentedFile.Builder.bufferSize(6145));  // > (4096 + 4096 * 0.5)

        DatabaseDescriptor.setDiskOptimizationPageCrossChance(1.0); // never add a page
        assertEquals(8192, SegmentedFile.Builder.bufferSize(8191));
        assertEquals(8192, SegmentedFile.Builder.bufferSize(8192));

        DatabaseDescriptor.setDiskOptimizationPageCrossChance(0.0); // always add a page
        assertEquals(8192, SegmentedFile.Builder.bufferSize(10));
        assertEquals(8192, SegmentedFile.Builder.bufferSize(4096));
    }

    @Test
    public void testBufferSize_spinning()
    {
        DatabaseDescriptor.setDiskOptimizationStrategy(Config.DiskOptimizationStrategy.spinning);

        assertEquals(4096, SegmentedFile.Builder.bufferSize(0));
        assertEquals(8192, SegmentedFile.Builder.bufferSize(10));
        assertEquals(8192, SegmentedFile.Builder.bufferSize(100));
        assertEquals(8192, SegmentedFile.Builder.bufferSize(4096));
        assertEquals(12288, SegmentedFile.Builder.bufferSize(4097));
    }
}
