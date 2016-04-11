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
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.net.async.ByteBufDataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import org.apache.cassandra.streaming.async.StreamCompressionSerializer;
import org.apache.cassandra.streaming.messages.StreamMessage;

/**
 * The intent of this class is to only be used in a very narrow use-case: on the stream compression path of streaming.
 * This class should really only get calls to {@link #write(ByteBuffer)}, where the incoming buffer is compressed and sent
 * downstream.
 */
public class ByteBufCompressionDataOutputStreamPlus extends WrappedDataOutputStreamPlus
{
    private final StreamRateLimiter limiter;
    private final LZ4Compressor compressor;
    private final StreamCompressionSerializer serializer;

    public ByteBufCompressionDataOutputStreamPlus(DataOutputStreamPlus out, StreamRateLimiter limiter)
    {
        super(out);
        assert out instanceof ByteBufDataOutputStreamPlus;
        compressor = LZ4Factory.fastestInstance().fastCompressor();
        serializer = new StreamCompressionSerializer(((ByteBufDataOutputStreamPlus)out).getAllocator());
        this.limiter = limiter;
    }

    /**
     * {@inheritDoc}
     *
     * Compress the incoming buffer and send the result downstream. The buffer parameter will not be used nor passed
     * to downstream components, and thus callers can safely free the buffer upon return.
     */
    @Override
    public void write(ByteBuffer buffer) throws IOException
    {
        ByteBuf compressed = serializer.serialize(compressor, buffer, StreamMessage.CURRENT_VERSION);

        // this is a blocking call - you have been warned
        limiter.acquire(compressed.readableBytes());

        ((ByteBufDataOutputStreamPlus)out).writeToChannel(compressed);
    }

    @Override
    public void close()
    {
        // explicitly overriding close() to avoid closing the wrapped stream; it will be closed via other means
    }
}
