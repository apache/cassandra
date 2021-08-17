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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;

public abstract class FrameEncoder extends ChannelOutboundHandlerAdapter
{
    protected static final BufferPool bufferPool = BufferPools.forNetworking();

    /**
     * An abstraction useful for transparently allocating buffers that can be written to upstream
     * of the {@code FrameEncoder} without knowledge of the encoder's frame layout, while ensuring
     * enough space to write the remainder of the frame's contents is reserved.
     */
    public static class Payload
    {
        public static final int MAX_SIZE = 1 << 17;
        // isSelfContained is a flag in the Frame API, indicating if the contents consists of only complete messages
        private boolean isSelfContained;
        // the buffer to write to
        public final ByteBuffer buffer;
        // the number of header bytes to reserve
        final int headerLength;
        // the number of trailer bytes to reserve
        final int trailerLength;
        // an API-misuse detector
        private boolean isFinished = false;

        Payload(boolean isSelfContained, int payloadCapacity)
        {
            this(isSelfContained, payloadCapacity, 0, 0);
        }

        Payload(boolean isSelfContained, int payloadCapacity, int headerLength, int trailerLength)
        {
            this.isSelfContained = isSelfContained;
            this.headerLength = headerLength;
            this.trailerLength = trailerLength;

            buffer = bufferPool.getAtLeast(payloadCapacity + headerLength + trailerLength, BufferType.OFF_HEAP);
            assert buffer.capacity() >= payloadCapacity + headerLength + trailerLength;
            buffer.position(headerLength);
            buffer.limit(buffer.capacity() - trailerLength);
        }

        void setSelfContained(boolean isSelfContained)
        {
            this.isSelfContained = isSelfContained;
        }

        // do not invoke after finish()
        int length()
        {
            assert !isFinished;
            return buffer.position() - headerLength;
        }

        // do not invoke after finish()
        public int remaining()
        {
            assert !isFinished;
            return buffer.remaining();
        }

        // do not invoke after finish()
        void trim(int length)
        {
            assert !isFinished;
            buffer.position(headerLength + length);
        }

        // may not be written to or queried, after this is invoked; must be passed straight to an encoder (or release called)
        public void finish()
        {
            assert !isFinished;
            isFinished = true;
            buffer.limit(buffer.position() + trailerLength);
            buffer.position(0);
            bufferPool.putUnusedPortion(buffer);
        }

        public void release()
        {
            bufferPool.put(buffer);
        }
    }

    public interface PayloadAllocator
    {
        public static final PayloadAllocator simple = Payload::new;
        Payload allocate(boolean isSelfContained, int capacity);
    }

    public PayloadAllocator allocator()
    {
        return PayloadAllocator.simple;
    }

    /**
     * Takes ownership of the lifetime of the provided buffer, which can be assumed to be managed by BufferPool
     */
    abstract ByteBuf encode(boolean isSelfContained, ByteBuffer buffer);

    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
    {
        if (!(msg instanceof Payload))
            throw new IllegalStateException("Unexpected type: " + msg);

        Payload payload = (Payload) msg;
        ByteBuf write = encode(payload.isSelfContained, payload.buffer);
        ctx.write(write, promise);
    }
}
