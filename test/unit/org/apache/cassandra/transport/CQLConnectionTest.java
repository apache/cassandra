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

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.primitives.Ints;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.AllowAllAuthorizer;
import org.apache.cassandra.auth.AllowAllNetworkAuthorizer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static org.apache.cassandra.net.FramingTest.randomishBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class CQLConnectionTest
{
    private static final Logger logger = LoggerFactory.getLogger(CQLConnectionTest.class);

    private Random random;
    private InetAddress address;
    private int port;

    @Before
    public void setup()
    {
        DatabaseDescriptor.toolInitialization();
        DatabaseDescriptor.setAuthenticator(new AllowAllAuthenticator());
        DatabaseDescriptor.setAuthorizer(new AllowAllAuthorizer());
        DatabaseDescriptor.setNetworkAuthorizer(new AllowAllNetworkAuthorizer());
        long seed = new SecureRandom().nextLong();
        logger.info("seed: {}", seed);
        random = new Random(seed);
        address = InetAddress.getLoopbackAddress();
        try
        {
            try (ServerSocket serverSocket = new ServerSocket(0))
            {
                port = serverSocket.getLocalPort();
            }
            Thread.sleep(250);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAquireAndRelease() throws Exception
    {
        testAcquireAndRelease(10, 100);
        testAcquireAndRelease(100, 1000);
        testAcquireAndRelease(1000, 10000);
    }

    private void testAcquireAndRelease(int minMessages, int maxMessages) throws IOException, InterruptedException
    {
        // set connection-local queue size to 0 so that all capacity is allocated from reserves
        DatabaseDescriptor.setNativeTransportReceiveQueueCapacityInBytes(0);
        final int messageCount = minMessages + random.nextInt(maxMessages - minMessages);
        logger.info("Sending total of {} messages", messageCount);
        final ConcurrentLinkedQueue<Flusher.FlushItem<?>> out = new ConcurrentLinkedQueue<>();
        CountDownLatch processed = new CountDownLatch(messageCount);

        // A simple consumer which "serves" a static response and just adds the item to be flushed
        final Message.Response response = new ResultMessage.Void();
        CQLMessageHandler.MessageConsumer<Message.Request> messageConsumer =
            (channel, message, toFlushItem) -> {
                Flusher.FlushItem<?> item = toFlushItem.toFlushItem(channel, message, response);
                item.release();
                out.add(item);
                processed.countDown();
            };

        AllocationObserver allocationObserver = new AllocationObserver();
        ServerConfigurator configurator = new ServerConfigurator(messageConsumer, allocationObserver);
        Server server = new Server.Builder().withHost(address)
                                            .withPort(port)
                                            .withPipelineConfigurator(configurator)
                                            .build();
        Client client = new Client(new FrameEncoderCrc());
        long totalBytes = 0;
        try
        {
            server.start();
            client.connect(address, port);
            assertTrue(configurator.waitUntilReady());

            for (int i = 0; i < messageCount; i++)
            {
                byte[] bytes = randomishBytes(random, 100, 1024);
                Frame cqlFrame = Frame.create(Message.Type.OPTIONS,
                                              1,
                                              ProtocolVersion.V5,
                                              EnumSet.of(Frame.Header.Flag.USE_BETA),
                                              Unpooled.wrappedBuffer(bytes));
                // we only allocate enough bytes for the frame body, so track that
                totalBytes += cqlFrame.header.bodySizeInBytes;
                client.accumulate(cqlFrame);
            }
            // flush anything remaining in the client buffer
            client.maybeWrite();

            processed.await(100, TimeUnit.SECONDS);

            // verify that all messages went through the pipeline & our test message consumer
            assertThat(out.size()).isEqualTo(messageCount);
            Flusher.FlushItem.Framed flushed;
            while ((flushed = (Flusher.FlushItem.Framed) out.poll()) != null)
                assertThat(Message.Type.RESULT).isEqualTo(flushed.response.header.type);

            // verify that we did have to acquire some resources from the global/endpoint reserves
            assertThat(allocationObserver.endpointAllocationTotal()).isEqualTo(totalBytes);
            assertThat(allocationObserver.globalAllocationTotal()).isEqualTo(totalBytes);
            // and that we released it all
            assertThat(allocationObserver.endpointReleaseTotal()).isEqualTo(totalBytes);
            assertThat(allocationObserver.globalReleaseTotal()).isEqualTo(totalBytes);

            // assert that we definitely have no outstanding resources acquired from the reserves
            Server.EndpointPayloadTracker tracker = Server.EndpointPayloadTracker.get(FBUtilities.getJustLocalAddress());
            assertThat(tracker.endpointAndGlobalPayloadsInFlight.endpoint().using()).isEqualTo(0);
            assertThat(tracker.endpointAndGlobalPayloadsInFlight.global().using()).isEqualTo(0);
        }
        catch (Exception e)
        {
            logger.error("Unexpected error", e);
            throw new RuntimeException(e);
        }
        finally
        {
            client.stop();
            server.stop();

        }
    }

    static class ServerConfigurator extends PipelineConfigurator
    {
        private final SimpleCondition pipelineReady = new SimpleCondition();
        private final CQLMessageHandler.MessageConsumer<Message.Request> consumer;
        private final AllocationObserver allocationObserver;

        public ServerConfigurator(CQLMessageHandler.MessageConsumer<Message.Request> consumer,
                                  AllocationObserver observer)
        {
            super(NativeTransportService.useEpoll(), false, false, EncryptionOptions.DISABLED);
            this.consumer = consumer;
            this.allocationObserver = observer;
        }

        // Every CQL Frame received will be parsed as this OptionsMessage, which is trivial to execute
        // on the server. This means we can randomise the actual content of the CQL frames to test
        // resource allocation/release (which is based on frame size, not message), without having to
        // worry about processing of the actual messages.
        protected Message.Decoder<Message.Request> messageDecoder()
        {
            return new Message.Decoder<Message.Request>()
            {
                Message.Request decode(Channel channel, Frame frame)
                {
                    Message.Request request = new OptionsMessage();
                    request.setSourceFrame(frame);
                    return request;
                }
            };
        }

        protected void onPipelineReady()
        {
            pipelineReady.signalAll();
        }

        private boolean waitUntilReady() throws InterruptedException
        {
            return pipelineReady.await(10, TimeUnit.SECONDS);
        }

        protected ResourceLimits.Limit endpointReserve(Server.EndpointPayloadTracker tracker)
        {
            return allocationObserver.endpoint(tracker);
        }

        protected ResourceLimits.Limit globalReserve(Server.EndpointPayloadTracker tracker)
        {
            return allocationObserver.global(tracker);
        }

        protected CQLMessageHandler.MessageConsumer<Message.Request> messageConsumer()
        {
            return consumer;
        }
    }

    static class AllocationObserver
    {
        InstrumentedLimit endpoint;
        InstrumentedLimit global;

        long endpointAllocationTotal()
        {
            assert endpoint != null;
            return endpoint.totalAllocated.get();
        }

        long endpointReleaseTotal()
        {
            assert endpoint != null;
            return endpoint.totalReleased.get();
        }

        long globalAllocationTotal()
        {
            assert global != null;
            return global.totalAllocated.get();
        }

        long globalReleaseTotal()
        {
            assert global != null;
            return global.totalReleased.get();
        }

        synchronized InstrumentedLimit endpoint(Server.EndpointPayloadTracker tracker)
        {
            if (endpoint == null)
                endpoint = new InstrumentedLimit(tracker.endpointAndGlobalPayloadsInFlight.endpoint());
            return endpoint;
        }

        synchronized InstrumentedLimit global(Server.EndpointPayloadTracker tracker)
        {
            if (global == null)
                global = new InstrumentedLimit(tracker.endpointAndGlobalPayloadsInFlight.global());
            return global;
        }
    }

    static class Client
    {
        private final FrameEncoder frameEncoder;
        private Channel channel;

        Client(FrameEncoder frameEncoder)
        {
            this.frameEncoder = frameEncoder;
        }

        private void connect(InetAddress address, int port) throws IOException, InterruptedException
        {
            Bootstrap bootstrap = new Bootstrap()
                                    .group(new NioEventLoopGroup())
                                    .channel(io.netty.channel.socket.nio.NioSocketChannel.class)
                                    .option(ChannelOption.TCP_NODELAY, true);

            bootstrap.handler(new ChannelInitializer<Channel>()
            {
                protected void initChannel(Channel channel) throws Exception
                {
                    ChannelPipeline pipeline = channel.pipeline();
                    pipeline.addLast("cqlFrameEncoder", Frame.Encoder.instance);
                    pipeline.addLast("cqlMessageEncoder", PreV5Handlers.ProtocolEncoder.instance);
                }
            });

            ChannelFuture future = bootstrap.connect(address, port);

            // Wait until the connection attempt succeeds or fails.
            channel = future.awaitUninterruptibly().channel();
            if (!future.isSuccess())
            {
                bootstrap.group().shutdownGracefully();
                throw new IOException("Connection Error", future.cause());
            }

            Map<String, String> options = new HashMap<>();
            options.put(StartupMessage.CQL_VERSION, QueryProcessor.CQL_VERSION.toString());
            ChannelPromise promise = AsyncChannelPromise.withListener(channel, f ->
            {
                if (f.isSuccess())
                {
                    channel.pipeline().remove("cqlFrameEncoder");
                    channel.pipeline().remove("cqlMessageEncoder");
                    channel.pipeline().addLast("messageFrameEncoder", frameEncoder);
                }
            });

            Connection connection = new Connection(channel, ProtocolVersion.V5, new Connection.Tracker()
            {
                public void addConnection(Channel ch, Connection connection)
                {
                }
            });
            channel.attr(Connection.attributeKey).set(connection);
            channel.writeAndFlush(new StartupMessage(options), promise).sync();
        }

        long totalFrameSize = 0;
        List<Frame> cqlFrames = new ArrayList<>();
        private void accumulate(Frame frame)
        {
            if (totalFrameSize + Frame.Header.LENGTH + frame.header.bodySizeInBytes > Flusher.MAX_FRAMED_PAYLOAD_SIZE)
                maybeWrite();

            totalFrameSize += Frame.Header.LENGTH + frame.header.bodySizeInBytes;
            cqlFrames.add(frame);
        }

        private void maybeWrite()
        {
            if (cqlFrames.isEmpty())
                return;

            FrameEncoder.Payload payload = frameEncoder.allocator().allocate(true, Ints.checkedCast(totalFrameSize));

            for (Frame f : cqlFrames)
                f.encodeInto(payload.buffer);

            logger.info("Sending frame with {} messages and total size {}",
                        cqlFrames.size(),
                        FBUtilities.prettyPrintMemory(totalFrameSize));

            payload.finish();
            channel.writeAndFlush(payload, channel.voidPromise());
            cqlFrames.clear();
            totalFrameSize = 0;
        }

        private void stop()
        {
            if (channel.isOpen())
                channel.close();
        }
    }

    static class InstrumentedLimit extends DelegatingLimit
    {
        AtomicLong totalAllocated = new AtomicLong(0);
        AtomicLong totalReleased = new AtomicLong(0);

        InstrumentedLimit(ResourceLimits.Limit wrapped)
        {
            super(wrapped);
        }

        public boolean tryAllocate(long amount)
        {
            totalAllocated.addAndGet(amount);
            return super.tryAllocate(amount);
        }

        public ResourceLimits.Outcome release(long amount)
        {
            totalReleased.addAndGet(amount);
            return super.release(amount);
        }

    }

    static class DelegatingLimit implements ResourceLimits.Limit
    {
        private final ResourceLimits.Limit wrapped;

        DelegatingLimit(ResourceLimits.Limit wrapped)
        {
            this.wrapped = wrapped;
        }

        public long limit()
        {
            return wrapped.limit();
        }

        public long setLimit(long newLimit)
        {
            return wrapped.setLimit(newLimit);
        }

        public long remaining()
        {
            return wrapped.remaining();
        }

        public long using()
        {
            return wrapped.using();
        }

        public boolean tryAllocate(long amount)
        {
            return wrapped.tryAllocate(amount);
        }

        public void allocate(long amount)
        {
            wrapped.allocate(amount);
        }

        public ResourceLimits.Outcome release(long amount)
        {
            return wrapped.release(amount);
        }
    }
}
