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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class BufferPoolAllocatorTest
{

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.clientInitialization();
        // cache size hould be more than a macro chunk size for proper pool testing
        // if it is 0 or less than a macro chunk size we actually do not pool
        DatabaseDescriptor.getRawConfig().networking_cache_size_in_mb = 128;
    }

    @Test
    public void testAdoptedBufferContentAfterResize() {
        ByteBuf buffer = allocateByteBuf(200, 500);
        int originalCapacity = buffer.capacity();
        byte[] content = new byte[300];

        Random rand = new Random();
        rand.nextBytes(content);

        buffer.writeBytes(Arrays.copyOfRange(content, 0, 200));
        assertEquals(originalCapacity, GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        buffer.writeBytes(Arrays.copyOfRange(content, 200, 300));
        int increasedCapacity = buffer.capacity();
        assertEquals(increasedCapacity, GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        byte[] bufferContent = new byte[300];

        BufferPoolAllocator.Wrapped wrapped = (BufferPoolAllocator.Wrapped) buffer;
        ByteBuffer adopted = wrapped.adopt();
        adopted.get(bufferContent);
        assertArrayEquals(content, bufferContent);
        assertEquals(increasedCapacity, GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        GlobalBufferPoolAllocator.instance.put(adopted);
        ensureThatAllMemoryIsReturnedBackToBufferPool();
    }

    @Test
    public void testAdoptedBufferContentBeforeResize() {
        ByteBuf buffer = allocateByteBuf(200, 300);
        int originalCapacity = buffer.capacity();

        byte[] content = new byte[200];

        Random rand = new Random();
        rand.nextBytes(content);

        buffer.writeBytes(content);
        assertEquals(originalCapacity, GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        byte[] bufferContent = new byte[200];

        BufferPoolAllocator.Wrapped wrapped = (BufferPoolAllocator.Wrapped) buffer;
        ByteBuffer adopted = wrapped.adopt();
        adopted.get(bufferContent);
        assertArrayEquals(content, bufferContent);

        GlobalBufferPoolAllocator.instance.put(adopted);
        ensureThatAllMemoryIsReturnedBackToBufferPool();
    }

    @Test
    public void testPutPooledBufferBackIntoPool() {
        ByteBuf buffer = allocateByteBuf(200, 500);
        buffer.writeBytes(new byte[200]);

        buffer.release();
        ensureThatAllMemoryIsReturnedBackToBufferPool();
    }

    @Test
    public void testPutResizedBufferBackIntoPool() {
        ByteBuf buffer = allocateByteBuf(200, 500);
        buffer.writeBytes(new byte[500]);

        buffer.release();
        ensureThatAllMemoryIsReturnedBackToBufferPool();
    }

    @Test
    public void testBufferDefaultMaxCapacity()
    {
        ByteBuf noMaxCapacity = GlobalBufferPoolAllocator.instance.buffer(100);
        noMaxCapacity.writeBytes(new byte[100]);
        assertEquals(100, noMaxCapacity.readableBytes());
        noMaxCapacity.release();
        ensureThatAllMemoryIsReturnedBackToBufferPool();
    }

    @Test
    public void testBufferWithMaxCapacity()
    {
        ByteBuf buffer = allocateByteBuf(100, 500);
        buffer.writeBytes(new byte[500]);
        assertEquals(500, buffer.readableBytes());
        assertEquals(buffer.capacity(), GlobalBufferPoolAllocator.instance.usedSizeInBytes());
        buffer.release();
        ensureThatAllMemoryIsReturnedBackToBufferPool();
    }

    @Test
    public void testBufferContentAfterResize()
    {
        ByteBuf buffer = allocateByteBuf(200, 300);
        int originalCapacity = buffer.capacity();

        byte[] content = new byte[300];
        Random rand = new Random();
        rand.nextBytes(content);

        buffer.writeBytes(Arrays.copyOfRange(content, 0, 200));
        assertEquals(originalCapacity, GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        buffer.writeBytes(Arrays.copyOfRange(content, 200, 300));

        byte[] bufferContent = new byte[300];
        buffer.readBytes(bufferContent);
        assertArrayEquals(content, bufferContent);
        Assertions.assertThat(buffer.capacity()).isGreaterThanOrEqualTo(300);
        assertEquals(buffer.capacity(), GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        buffer.release();
        ensureThatAllMemoryIsReturnedBackToBufferPool();

    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testBufferExceedMaxCapacity()
    {
        ByteBuf maxCapacity = allocateByteBuf(100, 200);
        try
        {
            maxCapacity.writeBytes(new byte[300]);
        } finally {
            maxCapacity.release();
            ensureThatAllMemoryIsReturnedBackToBufferPool();
        }
    }

    @Test
    public void testResizeBufferMultipleTimes()
    {
        ByteBuf buffer = allocateByteBuf(100, 2000);
        buffer.writeBytes(new byte[200]);
        assertEquals(200, buffer.readableBytes());
        assertEquals(buffer.capacity(), GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        buffer.writeBytes(new byte[100]);
        assertEquals(300, buffer.readableBytes());
        assertEquals(buffer.capacity(), GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        buffer.writeBytes(new byte[300]);
        assertEquals(600, buffer.readableBytes());
        assertEquals(buffer.capacity(), GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        buffer.release();
        ensureThatAllMemoryIsReturnedBackToBufferPool();
    }

    private static ByteBuf allocateByteBuf(int initialCapacity, int maxCapacity)
    {
        ByteBuf buffer = GlobalBufferPoolAllocator.instance.buffer(initialCapacity, maxCapacity);
        int originalCapacity = buffer.capacity();

        // BufferPool can allocate more capacity than requested to avoid fragmentation
        Assertions.assertThat(originalCapacity).isGreaterThanOrEqualTo(initialCapacity);
        assertEquals(originalCapacity, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
        return buffer;
    }

    private static void ensureThatAllMemoryIsReturnedBackToBufferPool()
    {
        assertEquals(0, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
        assertEquals(0, GlobalBufferPoolAllocator.instance.overflowMemoryInBytes());
    }
}
