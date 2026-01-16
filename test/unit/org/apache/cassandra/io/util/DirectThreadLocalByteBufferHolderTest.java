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

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

public class DirectThreadLocalByteBufferHolderTest
{
    @Test
    public void testGetBuffer()
    {
        int blockSize = 4096;
        int bufferSize = blockSize * 4;

        DirectThreadLocalByteBufferHolder holder = new DirectThreadLocalByteBufferHolder(blockSize);

        // Initial buffer creation
        ByteBuffer buffer = holder.getBuffer(bufferSize);
        Assert.assertEquals(bufferSize, buffer.limit());
        Assert.assertEquals(0, buffer.position());
        buffer.put(new byte[bufferSize]);

        // Reuse same buffer
        ByteBuffer sameBuffer = holder.getBuffer(bufferSize);
        Assert.assertSame(buffer, sameBuffer);
        Assert.assertEquals(0, sameBuffer.position());
    }

    @Test
    public void testBufferGrows()
    {
        int blockSize = 4096;
        DirectThreadLocalByteBufferHolder holder = new DirectThreadLocalByteBufferHolder(blockSize);

        ByteBuffer small = holder.getBuffer(blockSize);
        ByteBuffer large = holder.getBuffer(blockSize * 4);

        Assert.assertNotSame(small, large);
        Assert.assertEquals(blockSize * 4, large.capacity());
    }

    @Test
    public void testAlignmentRoundsUp()
    {
        int blockSize = 4096;
        DirectThreadLocalByteBufferHolder holder = new DirectThreadLocalByteBufferHolder(blockSize);

        // Request non-aligned size
        ByteBuffer buffer = holder.getBuffer(blockSize + 100);

        // Should be rounded up to next block boundary
        Assert.assertEquals(blockSize * 2, buffer.capacity());
        Assert.assertEquals(blockSize * 2, buffer.limit());
    }

    @Test
    public void testSharedAcrossHolders()
    {
        int blockSize = 4096;

        DirectThreadLocalByteBufferHolder holder1 = new DirectThreadLocalByteBufferHolder(blockSize);
        ByteBuffer buffer1 = holder1.getBuffer(blockSize);

        DirectThreadLocalByteBufferHolder holder2 = new DirectThreadLocalByteBufferHolder(blockSize);
        ByteBuffer buffer2 = holder2.getBuffer(blockSize);

        // Same block size = same underlying thread-local = same buffer
        Assert.assertSame(buffer1, buffer2);
    }

    @Test
    public void testDifferentBlockSizesAreSeparate()
    {
        DirectThreadLocalByteBufferHolder holder4k = new DirectThreadLocalByteBufferHolder(4096);
        DirectThreadLocalByteBufferHolder holder8k = new DirectThreadLocalByteBufferHolder(8192);

        ByteBuffer buffer4k = holder4k.getBuffer(4096);
        ByteBuffer buffer8k = holder8k.getBuffer(8192);

        Assert.assertNotSame(buffer4k, buffer8k);
    }

}