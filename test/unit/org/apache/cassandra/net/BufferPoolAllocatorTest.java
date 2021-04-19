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

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class BufferPoolAllocatorTest
{
    @Test
    public void testAdoptedBufferContentAfterResize() {
        DatabaseDescriptor.clientInitialization();
        ByteBuf buffer = GlobalBufferPoolAllocator.instance.buffer(200, 500);
        assertEquals(200, GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        byte[] content = new byte[300];

        Random rand = new Random();
        rand.nextBytes(content);

        buffer.writeBytes(Arrays.copyOfRange(content, 0, 200));
        assertEquals(200, GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        buffer.writeBytes(Arrays.copyOfRange(content, 200, 300));

        byte[] bufferContent = new byte[300];

        BufferPoolAllocator.Wrapped wrapped = (BufferPoolAllocator.Wrapped) buffer;
        ByteBuffer adopted = wrapped.adopt();
        adopted.get(bufferContent);
        assertArrayEquals(content, bufferContent);
        assertEquals(500, GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        GlobalBufferPoolAllocator.instance.put(adopted);
        assertEquals(0, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
    }

    @Test
    public void testAdoptedBufferContentBeforeResize() {
        DatabaseDescriptor.clientInitialization();
        ByteBuf buffer = GlobalBufferPoolAllocator.instance.buffer(200, 300);
        assertEquals(200, GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        byte[] content = new byte[200];

        Random rand = new Random();
        rand.nextBytes(content);

        buffer.writeBytes(content);
        assertEquals(200, GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        byte[] bufferContent = new byte[200];

        BufferPoolAllocator.Wrapped wrapped = (BufferPoolAllocator.Wrapped) buffer;
        ByteBuffer adopted = wrapped.adopt();
        adopted.get(bufferContent);
        assertArrayEquals(content, bufferContent);

        GlobalBufferPoolAllocator.instance.put(adopted);
        assertEquals(0, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
    }

    @Test
    public void testPutPooledBufferBackIntoPool() {
        DatabaseDescriptor.clientInitialization();
        ByteBuf buffer = GlobalBufferPoolAllocator.instance.buffer(200, 500);
        assertEquals(200, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
        buffer.writeBytes(new byte[200]);

        buffer.release();
        assertEquals(0, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
    }

    @Test
    public void testPutResizedBufferBackIntoPool() {
        DatabaseDescriptor.clientInitialization();
        ByteBuf buffer = GlobalBufferPoolAllocator.instance.buffer(200, 500);
        assertEquals(200, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
        buffer.writeBytes(new byte[500]);

        buffer.release();
        assertEquals(0, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
    }

    @Test
    public void testBufferDefaultMaxCapacity()
    {
        DatabaseDescriptor.clientInitialization();
        ByteBuf noMaxCapacity = GlobalBufferPoolAllocator.instance.buffer(100);
        noMaxCapacity.writeBytes(new byte[100]);
        assertEquals(100, noMaxCapacity.readableBytes());
        noMaxCapacity.release();
        assertEquals(0, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
    }

    @Test
    public void testBufferWithMaxCapacity()
    {
        DatabaseDescriptor.clientInitialization();
        ByteBuf buffer = GlobalBufferPoolAllocator.instance.buffer(100, 500);
        buffer.writeBytes(new byte[500]);
        assertEquals(500, buffer.readableBytes());
        assertEquals(500, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
        buffer.release();
        assertEquals(0, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
    }

    @Test
    public void testBufferContentAfterResize()
    {
        DatabaseDescriptor.clientInitialization();
        ByteBuf buffer = GlobalBufferPoolAllocator.instance.buffer(200, 300);
        assertEquals(200, GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        byte[] content = new byte[300];

        Random rand = new Random();
        rand.nextBytes(content);

        buffer.writeBytes(Arrays.copyOfRange(content, 0, 200));
        assertEquals(200, GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        buffer.writeBytes(Arrays.copyOfRange(content, 200, 300));

        byte[] bufferContent = new byte[300];
        buffer.readBytes(bufferContent);
        assertArrayEquals(content, bufferContent);
        assertEquals(300, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
        buffer.release();
        assertEquals(0, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testBufferExceedMaxCapacity()
    {
        DatabaseDescriptor.clientInitialization();
        ByteBuf maxCapacity = GlobalBufferPoolAllocator.instance.buffer(100, 200);
        try
        {
            maxCapacity.writeBytes(new byte[300]);
        } finally {
            maxCapacity.release();
            assertEquals(0, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
        }
    }

    @Test
    public void testResizeBufferMultipleTimes()
    {
        DatabaseDescriptor.clientInitialization();
        ByteBuf buffer = GlobalBufferPoolAllocator.instance.buffer(100, 2000);
        buffer.writeBytes(new byte[200]);
        assertEquals(200, buffer.readableBytes());
        assertEquals(256, buffer.capacity());
        assertEquals(256, GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        buffer.writeBytes(new byte[100]);
        assertEquals(300, buffer.readableBytes());
        assertEquals(512, buffer.capacity());
        assertEquals(512, GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        buffer.writeBytes(new byte[300]);
        assertEquals(600, buffer.readableBytes());
        assertEquals(1024, buffer.capacity());
        assertEquals(1024, GlobalBufferPoolAllocator.instance.usedSizeInBytes());

        buffer.release();
        assertEquals(0, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
    }

}
