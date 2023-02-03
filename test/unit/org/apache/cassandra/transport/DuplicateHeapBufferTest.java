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

package org.apache.cassandra.transport;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

import org.junit.Assert;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.cassandra.utils.memory.MemoryUtil;

public class DuplicateHeapBufferTest
{
    private static final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
    private ByteBuf buf;

    @Test
    public void heapByteBuffer()
    {
        heapTest(50, false);
    }
    @Test
    public void readOnlyHeapByteBuffer()
    {
        heapTest(50, true);
    }

    @Test
    public void directByteBuffer()
    {
        directTest(50, false);
    }
    @Test
    public void readOnlyDirectByteBuffer()
    {
        directTest(50, true);
    }

    @Test
    public void CBUtilWriteValueTest()
    {
        ByteBuffer bb = ByteBuffer.allocate(50);
        int size = bb.capacity();
        buf = allocator.heapBuffer(size);

        CBUtil.writeValue(bb, buf);
        Assert.assertEquals(0, buf.readerIndex());
        Assert.assertEquals(bb, CBUtil.readValue(buf));
        Assert.assertEquals(buf.writerIndex(), buf.readerIndex());
    }

    private void directTest(int capacity, boolean readOnly)
    {
        ByteBuffer hollowBuffer = MemoryUtil.getHollowByteBuffer();
        ByteBuffer bb = readOnly ? ByteBuffer.allocateDirect(capacity).asReadOnlyBuffer() : ByteBuffer.allocateDirect(capacity);

        int size = bb.capacity();
        buf = allocator.directBuffer(size);

        try {
            MemoryUtil.duplicateHeapByteBuffer(bb, hollowBuffer);
        }
        catch (Error e) {
            Assert.assertEquals(new AssertionError().getClass(), e.getClass());
        }
    }

    private void heapTest(int capacity, boolean readOnly)
    {
        ByteBuffer hollowBuffer = MemoryUtil.getHollowByteBuffer();
        ByteBuffer bb = readOnly ? ByteBuffer.allocate(capacity).asReadOnlyBuffer() : ByteBuffer.allocate(capacity);

        int size = bb.capacity();
        buf = allocator.heapBuffer(size);

        try {
            ByteBuffer temp = MemoryUtil.duplicateHeapByteBuffer(bb, hollowBuffer);
            Assert.assertEquals(bb.position(), temp.position());
            Assert.assertEquals(bb.limit(), temp.limit());
            Assert.assertEquals(bb.capacity(), temp.capacity());
            Assert.assertEquals(bb.array(), temp.array());
        }
        catch (Exception e){
            Assert.assertEquals(new ReadOnlyBufferException().getClass(), e.getClass());
        }
    }
}
