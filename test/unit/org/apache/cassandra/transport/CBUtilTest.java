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

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

public class CBUtilTest
{
    private static final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
    private ByteBuf buf;

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release(buf.refCnt());
    }

    @Test
    public void writeAndReadString()
    {
        final String text = "if you're happy and you know it, write your tests";
        int size = CBUtil.sizeOfString(text);

        buf = allocator.heapBuffer(size);
        CBUtil.writeString(text, buf);
        Assert.assertEquals(size, buf.writerIndex());
        Assert.assertEquals(0, buf.readerIndex());
        Assert.assertEquals(text, CBUtil.readString(buf));
        Assert.assertEquals(buf.writerIndex(), buf.readerIndex());
    }

    @Test
    public void writeAndReadLongString()
    {
        final String text = "if you're happy and you know it, write your tests";
        int size = CBUtil.sizeOfLongString(text);

        buf = allocator.heapBuffer(size);
        CBUtil.writeLongString(text, buf);
        Assert.assertEquals(size, buf.writerIndex());
        Assert.assertEquals(0, buf.readerIndex());
        Assert.assertEquals(text, CBUtil.readLongString(buf));
        Assert.assertEquals(buf.writerIndex(), buf.readerIndex());
    }

    @Test
    public void writeAndReadAsciiString()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < 128; i++)
            sb.append((char) i);
        String write = sb.toString();
        int size = CBUtil.sizeOfString(write);
        buf = allocator.heapBuffer(size);
        CBUtil.writeAsciiString(write, buf);
        String read = CBUtil.readString(buf);
        Assert.assertEquals(write, read);
    }

    @Test
    public void writeAndReadAsciiStringMismatchWithNonUSAscii()
    {
        String invalidAsciiStr = "\u0080 \u0123 \u0321"; // a valid string contains no char > 0x007F
        int size = CBUtil.sizeOfString(invalidAsciiStr);
        buf = allocator.heapBuffer(size);
        CBUtil.writeAsciiString(invalidAsciiStr, buf);
        Assert.assertNotEquals("Characters (> 0x007F) is considered as 2 bytes in sizeOfString, meanwhile writeAsciiString writes just 1 byte",
                               size,
                               buf.writerIndex());
    }
}
