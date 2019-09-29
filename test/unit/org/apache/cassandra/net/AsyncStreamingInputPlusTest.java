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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.net.AsyncStreamingInputPlus;
import org.apache.cassandra.net.AsyncStreamingInputPlus.InputTimeoutException;

import static org.junit.Assert.assertFalse;

public class AsyncStreamingInputPlusTest
{
    private EmbeddedChannel channel;
    private AsyncStreamingInputPlus inputPlus;
    private ByteBuf buf;

    @Before
    public void setUp()
    {
        channel = new EmbeddedChannel();
    }

    @After
    public void tearDown()
    {
        channel.close();

        if (buf != null && buf.refCnt() > 0)
            buf.release(buf.refCnt());
    }

//    @Test
//    public void isOpen()
//    {
//        Assert.assertTrue(inputPlus.isOpen());
//        inputPlus.requestClosure();
//        Assert.assertFalse(inputPlus.isOpen());
//    }

    @Test
    public void append_closed()
    {
        inputPlus = new AsyncStreamingInputPlus(channel);
        inputPlus.requestClosure();
        inputPlus.close();
        buf = channel.alloc().buffer(4);
        assertFalse(inputPlus.append(buf));
    }

    @Test
    public void append_normal()
    {
        inputPlus = new AsyncStreamingInputPlus(channel);
        int size = 4;
        buf = channel.alloc().buffer(size);
        buf.writerIndex(size);
        inputPlus.append(buf);
        Assert.assertEquals(buf.readableBytes(), inputPlus.unsafeAvailable());
    }

    @Test
    public void read() throws IOException
    {
        inputPlus = new AsyncStreamingInputPlus(channel);
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
        Assert.assertEquals(16, inputPlus.unsafeAvailable());

//        ByteBuffer out = ByteBuffer.allocate(4);
//        int readCount = inputPlus.read(out);
//        Assert.assertEquals(4, readCount);
//        out.flip();
//        Assert.assertEquals(42, out.getInt());
//        Assert.assertEquals(12, inputPlus.unsafeAvailable());

//        out = ByteBuffer.allocate(8);
//        readCount = inputPlus.read(out);
//        Assert.assertEquals(8, readCount);
//        out.flip();
//        Assert.assertEquals(42, out.getLong());
//        Assert.assertEquals(4, inputPlus.unsafeAvailable());
    }

//    @Test (expected = EOFException.class)
//    public void read_closed() throws IOException
//    {
//        inputPlus.requestClosure();
//        ByteBuffer buf = ByteBuffer.allocate(1);
//        inputPlus.read(buf);
//    }

    @Test
    public void available_closed()
    {
        inputPlus = new AsyncStreamingInputPlus(channel);
        inputPlus.requestClosure();
        inputPlus.unsafeAvailable();
    }

    @Test
    public void available_HappyPath()
    {
        inputPlus = new AsyncStreamingInputPlus(channel);
        int size = 4;
        buf = channel.alloc().heapBuffer(size);
        buf.writerIndex(size);
        inputPlus.append(buf);
        Assert.assertEquals(size, inputPlus.unsafeAvailable());
    }

    @Test
    public void available_ClosedButWithBytes()
    {
        inputPlus = new AsyncStreamingInputPlus(channel);
        int size = 4;
        buf = channel.alloc().heapBuffer(size);
        buf.writerIndex(size);
        inputPlus.append(buf);
        inputPlus.requestClosure();
        Assert.assertEquals(size, inputPlus.unsafeAvailable());
    }

    @Test
    public void consumeUntil_SingleBuffer_Partial_HappyPath() throws IOException
    {
        consumeUntilTestCycle(1, 8, 0, 4);
    }

    @Test
    public void consumeUntil_SingleBuffer_AllBytes_HappyPath() throws IOException
    {
        consumeUntilTestCycle(1, 8, 0, 8);
    }

    @Test
    public void consumeUntil_MultipleBufferr_Partial_HappyPath() throws IOException
    {
        consumeUntilTestCycle(2, 8, 0, 13);
    }

    @Test
    public void consumeUntil_MultipleBuffer_AllBytes_HappyPath() throws IOException
    {
        consumeUntilTestCycle(2, 8, 0, 16);
    }

    @Test(expected = EOFException.class)
    public void consumeUntil_SingleBuffer_Fails() throws IOException
    {
        consumeUntilTestCycle(1, 8, 0, 9);
    }

    @Test(expected = EOFException.class)
    public void consumeUntil_MultipleBuffer_Fails() throws IOException
    {
        consumeUntilTestCycle(2, 8, 0, 17);
    }

    private void consumeUntilTestCycle(int nBuffs, int buffSize, int startOffset, int len) throws IOException
    {
        inputPlus = new AsyncStreamingInputPlus(channel);

        byte[] expectedBytes = new byte[len];
        int count = 0;
        for (int j=0; j < nBuffs; j++)
        {
            ByteBuf buf = channel.alloc().buffer(buffSize);
            for (int i = 0; i < buf.capacity(); i++)
            {
                buf.writeByte(j);
                if (count >= startOffset && (count - startOffset) < len)
                    expectedBytes[count - startOffset] = (byte)j;
                count++;
            }

            inputPlus.append(buf);
        }
        inputPlus.requestClosure();

        TestableWritableByteChannel wbc = new TestableWritableByteChannel(len);

        inputPlus.skipBytesFully(startOffset);
        BufferedDataOutputStreamPlus writer = new BufferedDataOutputStreamPlus(wbc);
        inputPlus.consume(buffer -> { writer.write(buffer); return buffer.remaining(); }, len);
        writer.close();

        Assert.assertEquals(String.format("Test with %d buffers starting at %d consuming %d bytes", nBuffs, startOffset, len),
                            len, wbc.writtenBytes.readableBytes());

        Assert.assertArrayEquals(expectedBytes, wbc.writtenBytes.array());
    }

    private static class TestableWritableByteChannel implements WritableByteChannel
    {
        private boolean isOpen = true;
        public ByteBuf writtenBytes;

        public TestableWritableByteChannel(int initialCapacity)
        {
             writtenBytes = Unpooled.buffer(initialCapacity);
        }

        public int write(ByteBuffer src)
        {
            int size = src.remaining();
            writtenBytes.writeBytes(src);
            return size;
        }

        public boolean isOpen()
        {
            return isOpen;
        }

        public void close()
        {
            isOpen = false;
        }
    }

    @Test
    public void rebufferTimeout() throws IOException
    {
        long timeoutMillis = 1000;
        inputPlus = new AsyncStreamingInputPlus(channel, timeoutMillis, TimeUnit.MILLISECONDS);

        long startNanos = System.nanoTime();
        try
        {
            inputPlus.readInt();
            Assert.fail("should not have been able to read from the queue");
        }
        catch (InputTimeoutException e)
        {
            // this is the success case, and is expected. any other exception is a failure.
        }

        long durationNanos = System.nanoTime() - startNanos;
        Assert.assertTrue(TimeUnit.MILLISECONDS.toNanos(timeoutMillis) <= durationNanos);
    }
}
