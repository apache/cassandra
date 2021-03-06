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

package org.apache.cassandra.streaming.compress;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.net.AsyncStreamingInputPlus;
import org.apache.cassandra.streaming.async.StreamCompressionSerializer;

public class StreamCompressionInputStream extends RebufferingInputStream implements AutoCloseable
{
    /**
     * The stream which contains buffers of compressed data that came from the peer.
     */
    private final DataInputPlus dataInputPlus;

    private final LZ4SafeDecompressor decompressor;
    private final int protocolVersion;
    private final StreamCompressionSerializer deserializer;

    /**
     * The parent, or owning, buffer of the current buffer being read from ({@link super#buffer}).
     */
    private ByteBuf currentBuf;

    public StreamCompressionInputStream(DataInputPlus dataInputPlus, int protocolVersion)
    {
        super(Unpooled.EMPTY_BUFFER.nioBuffer());
        currentBuf = Unpooled.EMPTY_BUFFER;

        this.dataInputPlus = dataInputPlus;
        this.protocolVersion = protocolVersion;
        this.decompressor = LZ4Factory.fastestInstance().safeDecompressor();

        ByteBufAllocator allocator = dataInputPlus instanceof AsyncStreamingInputPlus
                                     ? ((AsyncStreamingInputPlus)dataInputPlus).getAllocator()
                                     : PooledByteBufAllocator.DEFAULT;
        deserializer = new StreamCompressionSerializer(allocator);
    }

    @Override
    public void reBuffer() throws IOException
    {
        currentBuf.release();
        currentBuf = deserializer.deserialize(decompressor, dataInputPlus, protocolVersion);
        buffer = currentBuf.nioBuffer(0, currentBuf.readableBytes());
    }

    /**
     * {@inheritDoc}
     *
     * Close resources except {@link #dataInputPlus} as that needs to remain open for other streaming activity.
     */
    @Override
    public void close()
    {
        currentBuf.release();
    }
}
