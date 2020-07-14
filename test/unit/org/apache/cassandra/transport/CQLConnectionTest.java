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
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

import com.google.common.primitives.Ints;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.AllowAllAuthorizer;
import org.apache.cassandra.auth.AllowAllNetworkAuthorizer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.transport.CQLMessageHandler.MessageConsumer;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static org.apache.cassandra.net.FramingTest.randomishBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CQLConnectionTest
{
    private static final Logger logger = LoggerFactory.getLogger(CQLConnectionTest.class);

    private Random random;
    private InetAddress address;
    private int port;
    private BufferPoolAllocator alloc;

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
        alloc = GlobalBufferPoolAllocator.instance;
    }

    @Test
    public void testAquireAndRelease()
    {
        acquireAndRelease(10, 100, Codec.crc(alloc));
        acquireAndRelease(10, 100, Codec.lz4(alloc));

        acquireAndRelease(100, 1000, Codec.crc(alloc));
        acquireAndRelease(100, 1000, Codec.lz4(alloc));

        acquireAndRelease(1000, 10000, Codec.crc(alloc));
        acquireAndRelease(1000, 10000, Codec.lz4(alloc));
    }

    private void acquireAndRelease(int minMessages, int maxMessages, Codec codec)
    {
        final int messageCount = minMessages + random.nextInt(maxMessages - minMessages);
        logger.info("Sending total of {} messages", messageCount);


        TestConsumer consumer = new TestConsumer(messageCount, new ResultMessage.Void(), codec.encoder);
        AllocationObserver observer = new AllocationObserver();
        Message.Decoder<Message.Request> decoder = new FixedDecoder();
        Predicate<Frame.Header> responseMatcher = h -> h.type == Message.Type.RESULT;
        ServerConfigurator configurator = ServerConfigurator.builder()
                                                            .withConsumer(consumer)
                                                            .withAllocationObserver(observer)
                                                            .withDecoder(decoder)
                                                            .build();

        runTest(configurator, codec, messageCount, responseMatcher, observer.verifier());
    }

    @Test
    public void testMessageDecodingErrorEncounteredMidFrame()
    {
        messageDecodingErrorEncounteredMidFrame(10, Codec.crc(alloc));
        messageDecodingErrorEncounteredMidFrame(10, Codec.lz4(alloc));

        messageDecodingErrorEncounteredMidFrame(100, Codec.crc(alloc));
        messageDecodingErrorEncounteredMidFrame(100, Codec.lz4(alloc));

        messageDecodingErrorEncounteredMidFrame(1000, Codec.crc(alloc));
        messageDecodingErrorEncounteredMidFrame(1000, Codec.lz4(alloc));
    }

    private void messageDecodingErrorEncounteredMidFrame(int messageCount, Codec codec)
    {
        final int streamWithError = messageCount / 2;
        TestConsumer consumer = new TestConsumer(messageCount, new ResultMessage.Void(), codec.encoder);
        AllocationObserver observer = new AllocationObserver();
        Message.Decoder<Message.Request> decoder = new FixedDecoder()
        {
            Message.Request decode(Channel channel, Frame frame)
            {
                if (frame.header.streamId != streamWithError)
                    return super.decode(channel, frame);

                throw new RequestExecutionException(ExceptionCode.SYNTAX_ERROR,
                                                    "Error decoding message " + frame.header.streamId)
                {/*test exception*/};
            }
        };

        Predicate<Frame.Header> responseMatcher =
            h -> (h.streamId == streamWithError && h.type == Message.Type.ERROR) || h.type == Message.Type.RESULT;

        ServerConfigurator configurator = ServerConfigurator.builder()
                                                            .withConsumer(consumer)
                                                            .withAllocationObserver(observer)
                                                            .withDecoder(decoder)
                                                            .build();

        runTest(configurator, codec, messageCount, responseMatcher, observer.verifier());
    }

    private void runTest(ServerConfigurator configurator,
                         Codec codec,
                         int messageCount,
                         Predicate<Frame.Header> responseMatcher,
                         LongConsumer allocationVerifier)
    {
        // set connection-local queue size to 0 so that all capacity is allocated from reserves
        DatabaseDescriptor.setNativeTransportReceiveQueueCapacityInBytes(0);
        Server server = server(configurator);
        Client client = new Client(codec, messageCount);
        try
        {
            server.start();
            client.connect(address, port);
            assertTrue(configurator.waitUntilReady());

            for (int i = 0; i < messageCount; i++)
                client.send(randomFrame(i, Message.Type.OPTIONS));

            long totalBytes = client.sendSize;

            // verify that all messages went through the pipeline & our test message consumer
            client.awaitResponses();
            Frame response;
            while ((response = client.pollResponses()) != null)
                assertThat(response.header).matches(responseMatcher);

            // verify that we did have to acquire some resources from the global/endpoint reserves
            allocationVerifier.accept(totalBytes);

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

    private Server server(ServerConfigurator configurator)
    {
        return new Server.Builder().withHost(address)
                                   .withPort(port)
                                   .withPipelineConfigurator(configurator)
                                   .build();
    }

    private Frame randomFrame(int streamId, Message.Type type)
    {
        byte[] bytes = randomishBytes(random, 100, 1024);
        return Frame.create(type,
                            streamId,
                            ProtocolVersion.V5,
                            EnumSet.of(Frame.Header.Flag.USE_BETA),
                            Unpooled.wrappedBuffer(bytes));
    }

    // Every CQL Frame received will be parsed as an OptionsMessage, which is trivial to execute
    // on the server. This means we can randomise the actual content of the CQL frames to test
    // resource allocation/release (which is based on frame size, not message), without having to
    // worry about processing of the actual messages.
    static class FixedDecoder extends Message.Decoder<Message.Request>
    {
        Message.Request decode(Channel channel, Frame frame)
        {
            Message.Request request = new OptionsMessage();
            request.setSourceFrame(frame);
            request.setStreamId(frame.header.streamId);
            return request;
        }
    }

    // A simple consumer which "serves" a static response and implements a naive flusher
    static class TestConsumer implements MessageConsumer<Message.Request>
    {
        final EnumSet<Frame.Header.Flag> flags = EnumSet.of(Frame.Header.Flag.USE_BETA);
        final int messageCount;
        final Message.Response fixedResponse;
        final Frame responseTemplate;
        final FrameEncoder frameEncoder;
        FrameAccumulator accumulator;

        TestConsumer(int messageCount, Message.Response fixedResponse, FrameEncoder frameEncoder)
        {
            this.messageCount = messageCount;
            this.fixedResponse = fixedResponse;
            this.responseTemplate = fixedResponse.encode(ProtocolVersion.V5);
            this.frameEncoder = frameEncoder;
        }

        public void accept(Channel channel, Message.Request message, Dispatcher.FlushItemConverter toFlushItem)
        {
            if (accumulator == null)
                accumulator = new FrameAccumulator(channel, frameEncoder);

            Flusher.FlushItem.Framed item = (Flusher.FlushItem.Framed)toFlushItem.toFlushItem(channel, message, fixedResponse);
            Frame response = Frame.create(responseTemplate.header.type,
                                          message.getStreamId(),
                                          ProtocolVersion.V5,
                                          responseTemplate.header.flags,
                                          responseTemplate.body.duplicate());
            item.release();

            accumulator.accumulate(response);
            channel.eventLoop().schedule(() -> accumulator.maybeWrite(), 10, TimeUnit.MILLISECONDS);
        }
    }

    static class ServerConfigurator extends PipelineConfigurator
    {
        private final SimpleCondition pipelineReady = new SimpleCondition();
        private final MessageConsumer<Message.Request> consumer;
        private final AllocationObserver allocationObserver;
        private final Message.Decoder<Message.Request> decoder;

        public ServerConfigurator(Builder builder)
        {
            super(NativeTransportService.useEpoll(), false, false, EncryptionOptions.DISABLED);
            this.consumer = builder.consumer;
            this.decoder = builder.decoder;
            this.allocationObserver = builder.observer;
        }

        static Builder builder()
        {
            return new Builder();
        }

        static class Builder
        {
           MessageConsumer<Message.Request> consumer;
           AllocationObserver observer;
           Message.Decoder<Message.Request> decoder;

           Builder withConsumer(MessageConsumer<Message.Request> consumer)
           {
               this.consumer = consumer;
               return this;
           }

           Builder withDecoder(Message.Decoder<Message.Request> decoder)
           {
               this.decoder = decoder;
               return this;
           }

           Builder withAllocationObserver(AllocationObserver observer)
           {
               this.observer = observer;
               return this;
           }

           ServerConfigurator build()
           {
               return new ServerConfigurator(this);
           }
        }

        protected Message.Decoder<Message.Request> messageDecoder()
        {
            return decoder == null ? super.messageDecoder() : decoder;
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
            return allocationObserver == null ? super.endpointReserve(tracker) : allocationObserver.endpoint(tracker);
        }

        protected ResourceLimits.Limit globalReserve(Server.EndpointPayloadTracker tracker)
        {
            return allocationObserver == null ? super.globalReserve(tracker) : allocationObserver.global(tracker);
        }

        protected MessageConsumer<Message.Request> messageConsumer()
        {
            return consumer == null ? super.messageConsumer() : consumer;
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

        LongConsumer verifier()
        {
            return totalBytes -> {
                // verify that we did have to acquire some resources from the global/endpoint reserves
                assertThat(endpointAllocationTotal()).isEqualTo(totalBytes);
                assertThat(globalAllocationTotal()).isEqualTo(totalBytes);
                // and that we released it all
                assertThat(endpointReleaseTotal()).isEqualTo(totalBytes);
                assertThat(globalReleaseTotal()).isEqualTo(totalBytes);
            };
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

    static class Codec
    {
        final FrameEncoder encoder;
        final FrameDecoder decoder;
        Codec(FrameEncoder encoder, FrameDecoder decoder)
        {
            this.encoder = encoder;
            this.decoder = decoder;
        }

        static Codec lz4(BufferPoolAllocator alloc)
        {
            return new Codec(FrameEncoderLZ4.fastInstance, FrameDecoderLZ4.fast(alloc));
        }

        static Codec crc(BufferPoolAllocator alloc)
        {
            return new Codec(FrameEncoderCrc.instance, new FrameDecoderCrc(alloc));
        }
    }

    static class Client
    {
        private final Codec codec;
        private Channel channel;
        final int expectedResponses;
        final CountDownLatch processed;

        final Queue<Frame> inboundFrames = new LinkedBlockingQueue<>();
        FrameAccumulator outboundFrames ;
        long sendSize = 0;

        Client(Codec codec, int expectedResponses)
        {
            this.codec = codec;
            this.expectedResponses = expectedResponses;
            this.processed = new CountDownLatch(expectedResponses);
        }

        private void connect(InetAddress address, int port) throws IOException, InterruptedException
        {
            final CountDownLatch ready = new CountDownLatch(1);
            Bootstrap bootstrap = new Bootstrap()
                                    .group(new NioEventLoopGroup())
                                    .channel(io.netty.channel.socket.nio.NioSocketChannel.class)
                                    .option(ChannelOption.TCP_NODELAY, true);
            bootstrap.handler(new ChannelInitializer<Channel>()
            {
                protected void initChannel(Channel channel) throws Exception
                {
                    BufferPoolAllocator allocator = GlobalBufferPoolAllocator.instance;
                    channel.config().setOption(ChannelOption.ALLOCATOR, allocator);
                    ChannelPipeline pipeline = channel.pipeline();
                    // Outbound handlers to enable us to send the initial STARTUP
                    pipeline.addLast("cqlFrameEncoder", Frame.Encoder.instance);
                    pipeline.addLast("cqlMessageEncoder", PreV5Handlers.ProtocolEncoder.instance);
                    pipeline.addLast("cqlFrameDecoder", new Frame.Decoder());
                    // Inbound handler to perform the handshake & modify the pipeline on receipt of a READY
                    pipeline.addLast("handshake", new MessageToMessageDecoder<Frame>()
                    {
                        final Frame.Decoder cqlFrameDecoder = new Frame.Decoder();
                        protected void decode(ChannelHandlerContext ctx, Frame msg, List<Object> out) throws Exception
                        {
                            // As soon as we receive a READY message, modify the pipeline
                            assert msg.header.type == Message.Type.READY;

                            // just split the messaging frame into cql frames and stash them for verification
                            FrameDecoder.FrameProcessor processor = frame -> {
                                if (frame instanceof FrameDecoder.IntactFrame)
                                {
                                    ByteBuffer bytes = ((FrameDecoder.IntactFrame)frame).contents.get();
                                    while(bytes.hasRemaining())
                                    {
                                        ByteBuf buffer = Unpooled.wrappedBuffer(bytes);
                                        try
                                        {
                                            inboundFrames.add(cqlFrameDecoder.decodeFrame(buffer));
                                            processed.countDown();
                                        }
                                        catch (Exception e)
                                        {
                                            throw new IOException(e);
                                        }
                                        bytes.position(bytes.position() + buffer.readerIndex());
                                    }
                                }
                                return true;
                            };

                            // for testing purposes, don't actually encode CQL messages and Frames,
                            // we supply message frames directly to this client
                            channel.pipeline().remove("cqlFrameEncoder");
                            channel.pipeline().remove("cqlMessageEncoder");
                            channel.pipeline().remove("cqlFrameDecoder");

                            // replace this handshake handler with an inbound message frame decoder
                            channel.pipeline().replace(this, "messageFrameDecoder", codec.decoder);
                            // add an outbound message frame encoder
                            channel.pipeline().addLast("messageFrameEncoder", codec.encoder);
                            codec.decoder.activate(processor);
                            ready.countDown();
                        }
                    });
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

            outboundFrames = new FrameAccumulator(channel, codec.encoder);

            // Send an initial STARTUP message to kick off the handshake with the server
            Map<String, String> options = new HashMap<>();
            options.put(StartupMessage.CQL_VERSION, QueryProcessor.CQL_VERSION.toString());
            if (codec.encoder instanceof FrameEncoderLZ4)
                options.put(StartupMessage.COMPRESSION, "LZ4");
            Connection connection = new Connection(channel, ProtocolVersion.V5, (ch, connection1) -> {});
            channel.attr(Connection.attributeKey).set(connection);
            channel.writeAndFlush(new StartupMessage(options)).sync();

            if (!ready.await(10, TimeUnit.SECONDS))
                throw new RuntimeException("Failed to establish client connection in 10s");
        }

        void send(Frame frame)
        {
            outboundFrames.accumulate(frame);
            sendSize += frame.header.bodySizeInBytes;
        }

        private void awaitResponses() throws InterruptedException
        {
            outboundFrames.maybeWrite();
            if (!processed.await(5, TimeUnit.SECONDS))
                fail(String.format("Didn't receive all responses, expected %d, actual %d",
                                   expectedResponses,
                                   expectedResponses - processed.getCount()));
        }

        private Frame pollResponses()
        {
            return inboundFrames.poll();
        }

        private void stop()
        {
            if (channel.isOpen())
                channel.close();
        }
    }

    private static class FrameAccumulator
    {
        long sendSize = 0;
        final List<Frame> outboundFrames = new ArrayList<>();
        final FrameEncoder frameEncoder;
        final Channel channel;

        FrameAccumulator(Channel channel, FrameEncoder frameEncoder)
        {
            this.frameEncoder = frameEncoder;
            this.channel = channel;
        }

        private void accumulate(Frame frame)
        {
            if (sendSize + Frame.Header.LENGTH + frame.header.bodySizeInBytes > Flusher.MAX_FRAMED_PAYLOAD_SIZE)
                maybeWrite();

            sendSize += Frame.Header.LENGTH + frame.header.bodySizeInBytes;
            outboundFrames.add(frame);
        }

        private void maybeWrite()
        {
            if (outboundFrames.isEmpty())
                return;

            FrameEncoder.Payload payload = frameEncoder.allocator().allocate(true, Ints.checkedCast(sendSize));

            for (Frame f : outboundFrames)
                f.encodeInto(payload.buffer);

            logger.trace("Sending frame with {} messages and total size {}",
                        outboundFrames.size(),
                        FBUtilities.prettyPrintMemory(sendSize));

            payload.finish();
            channel.writeAndFlush(payload, channel.voidPromise());
            outboundFrames.clear();
            sendSize = 0;
        }
    }
}
