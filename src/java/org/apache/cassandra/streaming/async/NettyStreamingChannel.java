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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import org.apache.cassandra.net.AsyncChannelPromise;
import org.apache.cassandra.net.AsyncStreamingInputPlus;
import org.apache.cassandra.net.AsyncStreamingOutputPlus;
import org.apache.cassandra.net.GlobalBufferPoolAllocator;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.StreamingDataInputPlus;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.streaming.StreamingDataOutputPlusFixed;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static io.netty.util.AttributeKey.valueOf;
import static java.lang.Boolean.FALSE;

public class NettyStreamingChannel extends ChannelInboundHandlerAdapter implements StreamingChannel
{
    private static final Logger logger = LoggerFactory.getLogger(NettyStreamingChannel.class);
    private static volatile boolean trackInboundHandlers = false;
    private static Collection<NettyStreamingChannel> inboundHandlers;

    @VisibleForTesting
    static final AttributeKey<Boolean> TRANSFERRING_FILE_ATTR = valueOf("transferringFile");
    final Channel channel;

    /**
     * A collection of {@link ByteBuf}s that are yet to be processed. Incoming buffers are first dropped into this
     * structure, and then consumed.
     * <p>
     * For thread safety, this structure's resources are released on the consuming thread
     * (via {@link AsyncStreamingInputPlus#close()},
     * but the producing side calls {@link AsyncStreamingInputPlus#requestClosure()} to notify the input that it should close.
     */
    @VisibleForTesting
    final AsyncStreamingInputPlus in;

    private volatile boolean closed;

    public NettyStreamingChannel(Channel channel, Kind kind)
    {
        this.channel = channel;
        channel.attr(TRANSFERRING_FILE_ATTR).set(FALSE);
        if (kind == Kind.CONTROL)
        {
            if (trackInboundHandlers)
                inboundHandlers.add(this);
            in = new AsyncStreamingInputPlus(channel);
        }
        else in = null;
    }

    @Override
    public Object id()
    {
        return channel.id();
    }

    @Override
    public String description()
    {
        return "channel.remote " + channel.remoteAddress() +
               " channel.local " + channel.localAddress() +
               " channel.id " + channel.id();
    }

    @Override
    public InetSocketAddress peer()
    {
        return (InetSocketAddress) channel.remoteAddress();
    }

    @Override
    public InetSocketAddress connectedTo()
    {
        return peer();
    }

    @Override
    public boolean connected()
    {
        return channel.isOpen();
    }

    public StreamingDataInputPlus in()
    {
        return in;
    }

    public StreamingDataOutputPlus acquireOut()
    {
        if (!channel.attr(TRANSFERRING_FILE_ATTR).compareAndSet(false, true))
            throw new IllegalStateException("channel's transferring state is currently set to true. refusing to start new stream");

        return new AsyncStreamingOutputPlus(channel)
        {
            @Override
            public void close() throws IOException
            {
                try
                {
                    super.close();
                }
                finally
                {
                    NettyStreamingChannel.this.channel.attr(TRANSFERRING_FILE_ATTR).set(FALSE);
                }
            }
        };
    }

    public Future<?> send(Send send)
    {
        class Factory implements IntFunction<StreamingDataOutputPlus>
        {
            ByteBuf buf;
            ByteBuffer buffer;

            @Override
            public StreamingDataOutputPlus apply(int size)
            {
                buf = GlobalBufferPoolAllocator.instance.buffer(size);
                buffer = buf.nioBuffer(buf.writerIndex(), size);
                return new StreamingDataOutputPlusFixed(buffer);
            }
        }

        Factory factory = new Factory();
        try
        {
            send.send(factory);
            ByteBuf buf = factory.buf;
            ByteBuffer buffer = factory.buffer;
            try
            {
                assert buffer.position() == buffer.limit();
                buf.writerIndex(buffer.position());
                AsyncChannelPromise promise = new AsyncChannelPromise(channel);
                channel.writeAndFlush(buf, promise);
                return promise;
            }
            catch (Throwable t)
            {
                buf.release();
                throw t;
            }
        }
        catch (Throwable t)
        {
            return ImmediateFuture.failure(t);
        }
    }

    @Override
    public synchronized io.netty.util.concurrent.Future<?> close()
    {
        if (closed)
            return channel.closeFuture();

        closed = true;
        if (in != null)
        {
            in.requestClosure();
            if (trackInboundHandlers)
                inboundHandlers.remove(this);
        }

        return channel.close();
    }

    @Override
    public void onClose(Runnable runOnClose)
    {
        channel.closeFuture().addListener(ignore -> runOnClose.run());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message)
    {
        if (closed || !(message instanceof ByteBuf) || !in.append((ByteBuf) message))
            ReferenceCountUtil.release(message);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        close();
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        if (cause instanceof IOException)
            logger.trace("connection problem while streaming", cause);
        else
            logger.warn("exception occurred while in processing streaming data", cause);
        close();
    }

    /** Shutdown for in-JVM tests. For any other usage, tracking of active inbound streaming handlers
     *  should be revisted first and in-JVM shutdown refactored with it.
     *  This does not prevent new inbound handlers being added after shutdown, nor is not thread-safe
     *  around new inbound handlers being opened during shutdown.
     */
    @VisibleForTesting
    public static void shutdown()
    {
        assert trackInboundHandlers : "in-JVM tests required tracking of inbound streaming handlers";

        inboundHandlers.forEach(NettyStreamingChannel::close);
        inboundHandlers.clear();
    }

    public static void trackInboundHandlers()
    {
        inboundHandlers = Collections.newSetFromMap(new ConcurrentHashMap<>());
        trackInboundHandlers = true;
    }

}
