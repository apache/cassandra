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

package org.apache.cassandra.utils;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.github.jamm.MemoryMeter;
import org.github.jamm.MemoryMeter.Guess;

import static org.github.jamm.MemoryMeter.ByteBufferMode.SLAB_ALLOCATION_NO_SLICE;
import static org.junit.Assert.assertEquals;

public class ObjectSizesTest
{
    // We use INSTRUMENTATION as principal strategy as it is our reference strategy
    private static final MemoryMeter meter = MemoryMeter.builder()
                                                        .withGuessing(Guess.INSTRUMENTATION, Guess.UNSAFE)
                                                        .build();

    @Test
    public void testSizeOnHeapExcludingData()
    {
        // single empty heap buffer
        checkBufferSizeExcludingData(ByteBuffer.allocate(0), 0);

        // single non-empty heap buffer
        checkBufferSizeExcludingData(ByteBuffer.allocate(10), 10);

        // single empty direct buffer
        checkBufferSizeExcludingData(ByteBuffer.allocateDirect(0), 0);

        // single non-empty direct buffer
        checkBufferSizeExcludingData(ByteBuffer.allocateDirect(10), 0);

        // heap buffer being a prefix slab
        ByteBuffer buffer = (ByteBuffer) ByteBuffer.allocate(10).duplicate().limit(8);
        checkBufferSizeExcludingData(buffer, 8);

        // heap buffer being a suffix slab
        buffer = (ByteBuffer) ByteBuffer.allocate(10).duplicate().position(1);
        checkBufferSizeExcludingData(buffer, 9);

        // heap buffer being an infix slab
        buffer = (ByteBuffer) ByteBuffer.allocate(10).duplicate().position(1).limit(8);
        checkBufferSizeExcludingData(buffer, 7);
    }

    private void checkBufferSizeExcludingData(ByteBuffer buffer, int dataSize)
    {
        assertEquals(meter.measureDeep(buffer, SLAB_ALLOCATION_NO_SLICE) - dataSize, ObjectSizes.sizeOnHeapExcludingDataOf(buffer));
    }

    @Test
    public void testSizeOnHeapExcludingDataArray()
    {
        checkBufferSizeExcludingDataArray(0, new ByteBuffer[0]);

        // single heap buffer
        checkBufferSizeExcludingDataArray(0, ByteBuffer.allocate(0));

        // multiple buffers
        checkBufferSizeExcludingDataArray(10, ByteBuffer.allocate(0), ByteBuffer.allocate(10), ByteBuffer.allocateDirect(10));

        // heap buffer being a prefix slab
        ByteBuffer prefix = (ByteBuffer) ByteBuffer.allocate(10).duplicate().limit(8);

        // heap buffer being a suffix slab
        ByteBuffer suffix = (ByteBuffer) ByteBuffer.allocate(10).duplicate().position(1);
        checkBufferSizeExcludingDataArray(8 + 9, prefix, suffix);
    }

    private void checkBufferSizeExcludingDataArray(int dataSize, ByteBuffer... buffers)
    {
        assertEquals(meter.measureDeep(buffers, SLAB_ALLOCATION_NO_SLICE) - dataSize, ObjectSizes.sizeOnHeapExcludingDataOf(buffers));
    }

    @Test
    public void testSizeOnHeapOf()
    {
        // single empty heap buffer
        checkBufferSize(ByteBuffer.allocate(0));

        // single non-empty heap buffer
        checkBufferSize(ByteBuffer.allocate(10));

        // single empty direct buffer
        checkBufferSize(ByteBuffer.allocateDirect(0));

        // single non-empty direct buffer
        checkBufferSize(ByteBuffer.allocateDirect(10));

        // heap buffer being a prefix slab
        ByteBuffer buffer = (ByteBuffer) ByteBuffer.allocate(10).duplicate().limit(8);
        checkBufferSize(buffer);

        // heap buffer being a suffix slab
        buffer = (ByteBuffer) ByteBuffer.allocate(10).duplicate().position(1);
        checkBufferSize(buffer);

        // heap buffer being an infix slab
        buffer = (ByteBuffer) ByteBuffer.allocate(10).duplicate().position(1).limit(8);
        checkBufferSize(buffer);
    }

    private void checkBufferSize(ByteBuffer buffer)
    {
        assertEquals(meter.measureDeep(buffer, SLAB_ALLOCATION_NO_SLICE), ObjectSizes.sizeOnHeapOf(buffer));
    }

    @Test
    public void testSizeOnHeapOfArray()
    {
        checkBufferArraySize(new ByteBuffer[0]);

        // single heap buffer
        checkBufferArraySize(ByteBuffer.allocate(0));

        // multiple buffers
        checkBufferArraySize(ByteBuffer.allocate(0), ByteBuffer.allocate(10), ByteBuffer.allocateDirect(10));
    }

    private void checkBufferArraySize(ByteBuffer... buffers)
    {
        assertEquals(meter.measureDeep(buffers, SLAB_ALLOCATION_NO_SLICE), ObjectSizes.sizeOnHeapOf(buffers));
    }
}