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

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import com.google.common.net.InetAddresses;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.FileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.locator.InetAddressAndPort;

public class TestChannel extends EmbeddedChannel
{
    public static final InetAddressAndPort REMOTE_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.2"), 0);

    final int inFlightLimit;
    int inFlight;

    ChannelOutboundBuffer flush;
    long flushBytes;

    public TestChannel()
    {
        this(Integer.MAX_VALUE);
    }

    public TestChannel(int inFlightLimit)
    {
        this.inFlightLimit = inFlightLimit;
    }

    @Override
    public SocketAddress remoteAddress()
    {
        return REMOTE_ADDR;
    }

    // we override ByteBuf to prevent retain() from working, to avoid release() since it is not needed in our usage
    // since the lifetime must live longer, we simply copy any outbound ByteBuf here for our tests
    protected void doWrite(ChannelOutboundBuffer in)
    {
        assert flush == null || flush == in;
        doWrite(in, in.totalPendingWriteBytes());
    }

    private void doWrite(ChannelOutboundBuffer flush, long flushBytes)
    {
        while (true) {
            Object msg = flush.current();
            if (msg == null) {
                this.flush = null;
                this.flushBytes = 0;
                return;
            }

            if (inFlight >= inFlightLimit)
            {
                this.flush = flush;
                this.flushBytes = flushBytes;
                return;
            }

            ByteBuf buf;
            if (msg instanceof FileRegion)
            {
                buf = GlobalBufferPoolAllocator.instance.directBuffer((int) ((FileRegion) msg).count());
                try
                {
                    ((FileRegion) msg).transferTo(new WritableByteChannel()
                    {
                        public int write(ByteBuffer src)
                        {
                            buf.setBytes(0, src);
                            return buf.writerIndex();
                        }

                        public boolean isOpen() { return true; }

                        public void close() { }
                    }, 0);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            else if (msg instanceof ByteBuf)
            {
                buf = ((ByteBuf)msg).copy();
            }
            else if (msg instanceof FrameEncoder.Payload)
            {
                buf = Unpooled.wrappedBuffer(((FrameEncoder.Payload)msg).buffer).copy();
            }
            else
            {
                System.err.println("Unexpected message type " + msg);
                throw new IllegalArgumentException();
            }

            inFlight += buf.readableBytes();
            handleOutboundMessage(buf);
            flush.remove();
        }
    }

    public <T> T readOutbound()
    {
        T msg = super.readOutbound();
        if (msg instanceof ByteBuf)
        {
            inFlight -= ((ByteBuf) msg).readableBytes();
            if (flush != null && inFlight < inFlightLimit)
                doWrite(flush, flushBytes);
        }
        return msg;
    }
}

