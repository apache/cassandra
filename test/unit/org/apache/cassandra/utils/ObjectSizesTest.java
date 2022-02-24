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

import org.github.jamm.MemoryLayoutSpecification;
import org.github.jamm.MemoryMeter;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectSizesTest
{
    private static final MemoryMeter meter = new MemoryMeter().withGuessing(MemoryMeter.Guess.FALLBACK_UNSAFE).omitSharedBufferOverhead().ignoreKnownSingletons();

    private static final long EMPTY_HEAP_BUFFER_RAW_SIZE = meter.measure(ByteBuffer.allocate(0));
    private static final long EMPTY_OFFHEAP_BUFFER_RAW_SIZE = meter.measure(ByteBuffer.allocateDirect(0));
    private static final ByteBuffer[] EMPTY_BYTE_BUFFER_ARRAY = new ByteBuffer[0];

    public static final long REF_ARRAY_0_SIZE = MemoryLayoutSpecification.sizeOfArray(0, MemoryLayoutSpecification.SPEC.getReferenceSize());
    public static final long REF_ARRAY_1_SIZE = MemoryLayoutSpecification.sizeOfArray(1, MemoryLayoutSpecification.SPEC.getReferenceSize());
    public static final long REF_ARRAY_2_SIZE = MemoryLayoutSpecification.sizeOfArray(2, MemoryLayoutSpecification.SPEC.getReferenceSize());

    public static final long BYTE_ARRAY_0_SIZE = MemoryLayoutSpecification.sizeOfArray(0, 1);
    public static final long BYTE_ARRAY_10_SIZE = MemoryLayoutSpecification.sizeOfArray(10, 1);
    public static final long BYTE_ARRAY_10_EXCEPT_DATA_SIZE = MemoryLayoutSpecification.sizeOfArray(10, 1) - 10;

    private ByteBuffer buf10 = ByteBuffer.allocate(10);
    private ByteBuffer prefixBuf8 = buf10.duplicate();
    private ByteBuffer suffixBuf9 = buf10.duplicate();
    private ByteBuffer infixBuf7 = buf10.duplicate();

    {
        prefixBuf8.limit(8);

        suffixBuf9.position(1);
        suffixBuf9 = suffixBuf9.slice();

        infixBuf7.limit(8);
        infixBuf7.position(1);
        infixBuf7 = infixBuf7.slice();
    }

    @Test
    public void testSizeOnHeapExcludingData()
    {
        // empty array of byte buffers
        ByteBuffer[] buffers = EMPTY_BYTE_BUFFER_ARRAY;
        assertThat(ObjectSizes.sizeOnHeapExcludingData(buffers)).isEqualTo(REF_ARRAY_0_SIZE);

        // single empty heap buffer
        buffers = new ByteBuffer[]{ ByteBuffer.allocate(0) };
        assertThat(ObjectSizes.sizeOnHeapExcludingData(buffers)).isEqualTo(REF_ARRAY_1_SIZE + EMPTY_HEAP_BUFFER_RAW_SIZE + BYTE_ARRAY_0_SIZE);

        // single non-empty heap buffer
        buffers = new ByteBuffer[]{ buf10 };
        assertThat(ObjectSizes.sizeOnHeapExcludingData(buffers)).isEqualTo(REF_ARRAY_1_SIZE + EMPTY_HEAP_BUFFER_RAW_SIZE + BYTE_ARRAY_10_EXCEPT_DATA_SIZE);

        // single empty direct buffer
        buffers = new ByteBuffer[]{ ByteBuffer.allocateDirect(0) };
        assertThat(ObjectSizes.sizeOnHeapExcludingData(buffers)).isEqualTo(REF_ARRAY_1_SIZE + EMPTY_OFFHEAP_BUFFER_RAW_SIZE);

        // single non-empty direct buffer
        buffers = new ByteBuffer[]{ ByteBuffer.allocateDirect(10) };
        assertThat(ObjectSizes.sizeOnHeapExcludingData(buffers)).isEqualTo(REF_ARRAY_1_SIZE + EMPTY_OFFHEAP_BUFFER_RAW_SIZE);

        // two different empty byte buffers
        buffers = new ByteBuffer[]{ ByteBuffer.allocate(0), ByteBuffer.allocateDirect(0) };
        assertThat(ObjectSizes.sizeOnHeapExcludingData(buffers)).isEqualTo(REF_ARRAY_2_SIZE + EMPTY_HEAP_BUFFER_RAW_SIZE + BYTE_ARRAY_0_SIZE + EMPTY_OFFHEAP_BUFFER_RAW_SIZE);

        // two different non-empty byte buffers
        buffers = new ByteBuffer[]{ buf10, ByteBuffer.allocateDirect(500) };
        assertThat(ObjectSizes.sizeOnHeapExcludingData(buffers)).isEqualTo(REF_ARRAY_2_SIZE + EMPTY_HEAP_BUFFER_RAW_SIZE + BYTE_ARRAY_10_EXCEPT_DATA_SIZE + EMPTY_OFFHEAP_BUFFER_RAW_SIZE);

        // heap buffer being a prefix slice of other buffer
        buffers = new ByteBuffer[]{ prefixBuf8 };
        assertThat(ObjectSizes.sizeOnHeapExcludingData(buffers)).isEqualTo(REF_ARRAY_1_SIZE + EMPTY_HEAP_BUFFER_RAW_SIZE);

        // heap buffer being a suffix slice of other buffer
        buffers = new ByteBuffer[]{ suffixBuf9 };
        assertThat(ObjectSizes.sizeOnHeapExcludingData(buffers)).isEqualTo(REF_ARRAY_1_SIZE + EMPTY_HEAP_BUFFER_RAW_SIZE);

        // heap buffer being an infix slice of other buffer
        buffers = new ByteBuffer[]{ infixBuf7 };
        assertThat(ObjectSizes.sizeOnHeapExcludingData(buffers)).isEqualTo(REF_ARRAY_1_SIZE + EMPTY_HEAP_BUFFER_RAW_SIZE);
    }

    @Test
    public void testSizeOnHeapOf()
    {
        // empty array of byte buffers
        ByteBuffer[] buffers = EMPTY_BYTE_BUFFER_ARRAY;
        assertThat(ObjectSizes.sizeOnHeapOf(buffers)).isEqualTo(REF_ARRAY_0_SIZE);

        // single empty heap buffer
        buffers = new ByteBuffer[]{ ByteBuffer.allocate(0) };
        assertThat(ObjectSizes.sizeOnHeapOf(buffers)).isEqualTo(REF_ARRAY_1_SIZE + EMPTY_HEAP_BUFFER_RAW_SIZE + BYTE_ARRAY_0_SIZE);

        // single non-empty heap buffer
        buffers = new ByteBuffer[]{ buf10 };
        assertThat(ObjectSizes.sizeOnHeapOf(buffers)).isEqualTo(REF_ARRAY_1_SIZE + EMPTY_HEAP_BUFFER_RAW_SIZE + BYTE_ARRAY_10_SIZE);

        // single empty direct buffer
        buffers = new ByteBuffer[]{ ByteBuffer.allocateDirect(0) };
        assertThat(ObjectSizes.sizeOnHeapOf(buffers)).isEqualTo(REF_ARRAY_1_SIZE + EMPTY_OFFHEAP_BUFFER_RAW_SIZE);

        // single non-empty direct buffer
        buffers = new ByteBuffer[]{ ByteBuffer.allocateDirect(10) };
        assertThat(ObjectSizes.sizeOnHeapOf(buffers)).isEqualTo(REF_ARRAY_1_SIZE + EMPTY_OFFHEAP_BUFFER_RAW_SIZE);

        // two different empty byte buffers
        buffers = new ByteBuffer[]{ ByteBuffer.allocate(0), ByteBuffer.allocateDirect(0) };
        assertThat(ObjectSizes.sizeOnHeapOf(buffers)).isEqualTo(REF_ARRAY_2_SIZE + EMPTY_HEAP_BUFFER_RAW_SIZE + BYTE_ARRAY_0_SIZE + EMPTY_OFFHEAP_BUFFER_RAW_SIZE);

        // two different non-empty byte buffers
        buffers = new ByteBuffer[]{ buf10, ByteBuffer.allocateDirect(500) };
        assertThat(ObjectSizes.sizeOnHeapOf(buffers)).isEqualTo(REF_ARRAY_2_SIZE + EMPTY_HEAP_BUFFER_RAW_SIZE + BYTE_ARRAY_10_SIZE + EMPTY_OFFHEAP_BUFFER_RAW_SIZE);

        // heap buffer being a prefix slice of other buffer
        buffers = new ByteBuffer[]{ prefixBuf8 };
        assertThat(ObjectSizes.sizeOnHeapOf(buffers)).isEqualTo(REF_ARRAY_1_SIZE + EMPTY_HEAP_BUFFER_RAW_SIZE + 8);

        // heap buffer being a suffix slice of other buffer
        buffers = new ByteBuffer[]{ suffixBuf9 };
        assertThat(ObjectSizes.sizeOnHeapOf(buffers)).isEqualTo(REF_ARRAY_1_SIZE + EMPTY_HEAP_BUFFER_RAW_SIZE + 9);

        // heap buffer being an infix slice of other buffer
        buffers = new ByteBuffer[]{ infixBuf7 };
        assertThat(ObjectSizes.sizeOnHeapOf(buffers)).isEqualTo(REF_ARRAY_1_SIZE + EMPTY_HEAP_BUFFER_RAW_SIZE + 7);
    }
}