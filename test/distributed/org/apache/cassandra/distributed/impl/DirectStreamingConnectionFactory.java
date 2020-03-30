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

package org.apache.cassandra.distributed.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntFunction;

import io.netty.util.concurrent.Future;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.net.OutboundConnectionSettings;
import org.apache.cassandra.streaming.StreamDeserializingTask;
import org.apache.cassandra.streaming.StreamingDataInputPlus;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.StreamingDataOutputPlusFixed;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.locator.InetAddressAndPort.getByAddress;
import static org.apache.cassandra.net.MessagingService.*;

// TODO: Simulator should schedule based on some streaming data rate
public class DirectStreamingConnectionFactory
{
    static class DirectConnection
    {
        private static final AtomicInteger nextId = new AtomicInteger();

        final int protocolVersion;
        final long sendBufferSize;

        // TODO rename
        private static class Buffer
        {
            final Queue<byte[]> pending = new ArrayDeque<>();
            boolean isClosed;
            int pendingBytes = 0;
        }

        @SuppressWarnings({"InnerClassMayBeStatic","unused"}) // helpful for debugging
        class DirectStreamingChannel implements StreamingChannel
        {
            class Out extends BufferedDataOutputStreamPlus implements StreamingDataOutputPlus
            {
                private final Buffer out;
                private Thread thread;
                private boolean inUse;

                Out(Buffer out)
                {
                    super(ByteBuffer.allocate(16 << 10));
                    this.out = out;
                }

                protected void doFlush(int count) throws IOException
                {
                    if (buffer.position() == 0)
                        return;

                    try
                    {
                        synchronized (out)
                        {
                            while (out.pendingBytes > 0 && count + out.pendingBytes > sendBufferSize && !out.isClosed)
                                out.wait();

                            if (out.isClosed)
                                throw new ClosedChannelException();

                            buffer.flip();
                            out.pendingBytes += buffer.remaining();
                            out.pending.add(ByteBufferUtil.getArray(buffer));
                            buffer.clear();

                            out.notify();
                        }
                    }
                    catch (InterruptedException e)
                    {
                        throw new UncheckedInterruptedException(e);
                    }
                }

                public synchronized Out acquire()
                {
                    if (inUse)
                        throw new IllegalStateException();
                    inUse = true;
                    thread = Thread.currentThread();
                    return this;
                }

                public synchronized void close() throws IOException
                {
                    flush();
                    inUse = false;
                }

                void realClose()
                {
                    synchronized (out)
                    {
                        out.isClosed = true;
                        out.notifyAll();
                    }
                }

                @Override
                public int writeToChannel(Write write, RateLimiter limiter) throws IOException
                {
                    class Holder
                    {
                        ByteBuffer buffer;
                    }
                    Holder holder = new Holder();

                    write.write(size -> {
                        if (holder.buffer != null)
                            throw new IllegalStateException("Can only allocate one ByteBuffer");
                        holder.buffer = ByteBuffer.allocate(size);
                        return holder.buffer;
                    });

                    ByteBuffer buffer = holder.buffer;
                    int length = buffer.limit();
                    write(buffer);
                    return length;
                }

                // TODO (future): support RateLimiter
                @Override
                public long writeFileToChannel(FileChannel file, RateLimiter limiter) throws IOException
                {
                    long count = 0;
                    while (file.read(buffer) >= 0)
                    {
                        count += buffer.position();
                        doFlush(0);
                    }
                    return count;
                }
            }

            class In extends RebufferingInputStream implements StreamingDataInputPlus
            {
                private final Buffer in;
                private Thread thread;

                In(Buffer in)
                {
                    super(ByteBuffer.allocate(0));
                    this.in = in;
                }

                protected void reBuffer() throws IOException
                {
                    try
                    {
                        synchronized (in)
                        {
                            while (in.pendingBytes == 0 && !in.isClosed)
                                in.wait();

                            if (in.pendingBytes == 0)
                                throw new ClosedChannelException();

                            byte[] bytes = in.pending.poll();
                            if (bytes == null)
                                throw new IllegalStateException();

                            in.pendingBytes -= bytes.length;
                            buffer = ByteBuffer.wrap(bytes);
                            in.notify();
                        }
                    }
                    catch (InterruptedException e)
                    {
                        throw new UncheckedInterruptedException(e);
                    }
                }

                public void close()
                {
                    DirectStreamingChannel.this.close();
                }

                public void realClose()
                {
                    synchronized (in)
                    {
                        in.isClosed = true;
                        in.notifyAll();
                    }
                }
            }

            final InetSocketAddress remoteAddress;

            private final In in;
            private final Out out;
            private final Integer id = nextId.incrementAndGet();
            Runnable onClose;
            boolean isClosed;

            DirectStreamingChannel(InetSocketAddress remoteAddress, Buffer outBuffer, Buffer inBuffer)
            {
                this.remoteAddress = remoteAddress;
                this.in = new In(inBuffer);
                this.out = new Out(outBuffer);
            }

            public StreamingDataOutputPlus acquireOut()
            {
                return out.acquire();
            }

            @Override
            public synchronized Future<?> send(Send send) throws IOException
            {
                class Factory implements IntFunction<StreamingDataOutputPlus>
                {
                    ByteBuffer buffer;
                    @Override
                    public StreamingDataOutputPlus apply(int size)
                    {
                        buffer = ByteBuffer.allocate(size);
                        return new StreamingDataOutputPlusFixed(buffer);
                    }
                }
                Factory factory = new Factory();
                send.send(factory);
                factory.buffer.flip();
                try (StreamingDataOutputPlus out = acquireOut())
                {
                    out.write(factory.buffer);
                }
                return ImmediateFuture.success(true);
            }

            @Override
            public Object id()
            {
                return id;
            }

            @Override
            public String description()
            {
                return remoteAddress.getAddress().getHostAddress() + "/in@" + id;
            }

            public StreamingDataInputPlus in()
            {
                in.thread = Thread.currentThread();
                return in;
            }

            public InetSocketAddress peer()
            {
                return remoteAddress;
            }

            @Override
            public InetSocketAddress connectedTo()
            {
                return remoteAddress;
            }

            @Override
            public boolean connected()
            {
                return true;
            }

            @Override
            public Future<?> close()
            {
                in.realClose();
                out.realClose();
                synchronized (this)
                {
                    if (!isClosed)
                    {
                        isClosed = true;
                        if (onClose != null)
                            onClose.run();
                    }
                }
                return ImmediateFuture.success(null);
            }

            @Override
            public synchronized void onClose(Runnable runOnClose)
            {
                if (isClosed) runOnClose.run();
                else if (onClose == null) onClose = runOnClose;
                else { Runnable tmp = onClose; onClose = () -> { tmp.run(); runOnClose.run(); }; }
            }
        }

        private final DirectStreamingChannel outToRecipient, outToOriginator;

        DirectConnection(int protocolVersion, long sendBufferSize, InetSocketAddress originator, InetSocketAddress recipient)
        {
            this.protocolVersion = protocolVersion;
            this.sendBufferSize = sendBufferSize;
            Buffer buffer1 = new Buffer(), buffer2 = new Buffer();
            outToRecipient = new DirectStreamingChannel(recipient, buffer1, buffer2);
            outToOriginator = new DirectStreamingChannel(originator, buffer2, buffer1);
        }

        StreamingChannel get(InetSocketAddress remoteAddress)
        {
            if (remoteAddress.equals(outToOriginator.remoteAddress)) return outToOriginator;
            else if (remoteAddress.equals(outToRecipient.remoteAddress)) return outToRecipient;
            else throw new IllegalArgumentException();
        }
    }

    public class Factory implements StreamingChannel.Factory
    {
        final InetSocketAddress from;
        Factory(InetSocketAddress from)
        {
            this.from = from;
        }

        @Override
        public StreamingChannel create(InetSocketAddress to, int messagingVersion, StreamingChannel.Kind kind)
        {
            long sendBufferSize = new OutboundConnectionSettings(getByAddress(to)).socketSendBufferSizeInBytes();
            if (sendBufferSize <= 0)
                sendBufferSize = 1 << 14;
            
            DirectConnection connection = new DirectConnection(messagingVersion, sendBufferSize, from, to);
            IInvokableInstance instance = cluster.get(to);
            instance.unsafeAcceptOnThisThread((channel, version) -> executorFactory().startThread(channel.description(), new StreamDeserializingTask(null, channel, version)),
                         connection.get(from), messagingVersion);
            return connection.get(to);
        }
    }

    final ICluster<IInvokableInstance> cluster;
    final int protocolVersion;

    private DirectStreamingConnectionFactory(ICluster<IInvokableInstance> cluster)
    {
        this.cluster = cluster;
        // we don't invoke this on the host ClassLoader as it initiates state like DatabaseDescriptor,
        // potentially leading to resource leaks on the hosts (particularly in validateHeader which runs on the host threads)
        this.protocolVersion = current_version;
    }

    public static Function<IInvokableInstance, StreamingChannel.Factory> create(ICluster<IInvokableInstance> cluster)
    {
        return cluster.get(1).unsafeApplyOnThisThread(c -> new DirectStreamingConnectionFactory(c)::get, cluster);
    }

    public static void setup(ICluster<IInvokableInstance> cluster)
    {
        Function<IInvokableInstance, StreamingChannel.Factory> streamingConnectionFactory = create(cluster);
        cluster.stream().forEach(i -> i.unsafeAcceptOnThisThread(StreamingChannel.Factory.Global::unsafeSet, streamingConnectionFactory.apply(i)));
    }

    public Factory get(IInvokableInstance instance)
    {
        return new Factory(instance.config().broadcastAddress());
    }
}
