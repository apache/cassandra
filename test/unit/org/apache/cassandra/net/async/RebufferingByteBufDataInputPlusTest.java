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

package org.apache.cassandra.net.async;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;

public class RebufferingByteBufDataInputPlusTest
{
    private EmbeddedChannel channel;
    private RebufferingByteBufDataInputPlus inputPlus;
    private ByteBuf buf;

    @Before
    public void setUp()
    {
        channel = new EmbeddedChannel();
        inputPlus = new RebufferingByteBufDataInputPlus(1 << 10, 1 << 11, channel.config());
    }

    @After
    public void tearDown()
    {
        inputPlus.close();
        channel.close();

        if (buf != null && buf.refCnt() > 0)
            buf.release(buf.refCnt());
    }

    @Test (expected = IllegalArgumentException.class)
    public void ctor_badWaterMarks()
    {
        inputPlus = new RebufferingByteBufDataInputPlus(2, 1, null);
    }

    @Test
    public void isOpen()
    {
        Assert.assertTrue(inputPlus.isOpen());
        inputPlus.markClose();
        Assert.assertFalse(inputPlus.isOpen());
    }

    @Test (expected = IllegalStateException.class)
    public void append_closed()
    {
        inputPlus.markClose();
        buf = channel.alloc().buffer(4);
        inputPlus.append(buf);
    }

    @Test
    public void append_normal() throws EOFException
    {
        int size = 4;
        buf = channel.alloc().buffer(size);
        buf.writerIndex(size);
        inputPlus.append(buf);
        Assert.assertEquals(buf.readableBytes(), inputPlus.available());
    }

    @Test
    public void read() throws IOException
    {
        // put two buffers of 8 bytes each into the queue.
        // then read an int, then a long. the latter tests offset into the inputPlus, as well as spanning across queued buffers.
        // the values of those int/long will both be '42', but spread across both queue buffers.
        ByteBuf buf = channel.alloc().buffer(8);
        buf.writeInt(42);
        buf.writerIndex(8);
        inputPlus.append(buf);
        buf = channel.alloc().buffer(8);
        buf.writeInt(42);
        buf.writerIndex(8);
        inputPlus.append(buf);
        Assert.assertEquals(16, inputPlus.available());

        ByteBuffer out = ByteBuffer.allocate(4);
        int readCount = inputPlus.read(out);
        Assert.assertEquals(4, readCount);
        out.flip();
        Assert.assertEquals(42, out.getInt());
        Assert.assertEquals(12, inputPlus.available());

        out = ByteBuffer.allocate(8);
        readCount = inputPlus.read(out);
        Assert.assertEquals(8, readCount);
        out.flip();
        Assert.assertEquals(42, out.getLong());
        Assert.assertEquals(4, inputPlus.available());
    }

    @Test (expected = EOFException.class)
    public void read_closed() throws IOException
    {
        inputPlus.markClose();
        ByteBuffer buf = ByteBuffer.allocate(1);
        inputPlus.read(buf);
    }

    @Test (expected = EOFException.class)
    public void available_closed() throws EOFException
    {
        inputPlus.markClose();
        inputPlus.available();
    }

    @Test
    public void available_HappyPath() throws EOFException
    {
        int size = 4;
        buf = channel.alloc().heapBuffer(size);
        buf.writerIndex(size);
        inputPlus.append(buf);
        Assert.assertEquals(size, inputPlus.available());
    }

    @Test
    public void available_ClosedButWithBytes() throws EOFException
    {
        int size = 4;
        buf = channel.alloc().heapBuffer(size);
        buf.writerIndex(size);
        inputPlus.append(buf);
        inputPlus.markClose();
        Assert.assertEquals(size, inputPlus.available());
    }
}
