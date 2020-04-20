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

package org.apache.cassandra.streaming.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.MessagingService;

public class StreamCompressionSerializerTest
{
    private static final int VERSION = MessagingService.current_version;
    private static final Random random = new Random(2347623847623L);

    private final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
    private final StreamCompressionSerializer serializer = new StreamCompressionSerializer(allocator);
    private final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    private final LZ4SafeDecompressor decompressor = LZ4Factory.fastestInstance().safeDecompressor();

    private ByteBuffer input;
    private ByteBuffer compressed;
    private ByteBuf output;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void tearDown()
    {
        if (input != null)
            FileUtils.clean(input);
        if (compressed != null)
            FileUtils.clean(compressed);
        if (output != null && output.refCnt() > 0)
            output.release(output.refCnt());
    }

    @Test
    public void roundTrip_HappyPath_NotReadabaleByteBuffer() throws IOException
    {
        populateInput();
        StreamCompressionSerializer.serialize(compressor, input, VERSION).write(size -> compressed = ByteBuffer.allocateDirect(size));
        input.flip();
        output = serializer.deserialize(decompressor, new DataInputBuffer(compressed, false), VERSION);
        validateResults();
    }

    private void populateInput()
    {
        int bufSize = 1 << 14;
        input = ByteBuffer.allocateDirect(bufSize);
        for (int i = 0; i < bufSize; i += 4)
            input.putInt(random.nextInt());
        input.flip();
    }

    private void validateResults()
    {
        Assert.assertEquals(input.remaining(), output.readableBytes());
        for (int i = 0; i < input.remaining(); i++)
            Assert.assertEquals(input.get(i), output.readByte());
    }

    @Test
    public void roundTrip_HappyPath_ReadabaleByteBuffer() throws IOException
    {
        populateInput();
        StreamCompressionSerializer.serialize(compressor, input, VERSION)
                                   .write(size -> {
                                       if (compressed != null)
                                           FileUtils.clean(compressed);
                                       return compressed = ByteBuffer.allocateDirect(size);
                                   });
        input.flip();
        output = serializer.deserialize(decompressor, new ByteBufRCH(Unpooled.wrappedBuffer(compressed)), VERSION);
        validateResults();
    }

    private static class ByteBufRCH extends DataInputBuffer implements ReadableByteChannel
    {
        public ByteBufRCH(ByteBuf compressed)
        {
            super (compressed.nioBuffer(0, compressed.readableBytes()), false);
        }

        @Override
        public int read(ByteBuffer dst) throws IOException
        {
            int len = dst.remaining();
            dst.put(buffer);
            return len;
        }

        @Override
        public boolean isOpen()
        {
            return true;
        }
    }
}
