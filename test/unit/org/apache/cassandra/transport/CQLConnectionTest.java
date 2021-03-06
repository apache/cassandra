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
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

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
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.net.*;
import org.apache.cassandra.net.proxy.InboundProxyHandler;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.transport.CQLMessageHandler.MessageConsumer;
import org.apache.cassandra.transport.messages.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static org.apache.cassandra.config.EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED;
import static org.apache.cassandra.net.FramingTest.randomishBytes;
import static org.apache.cassandra.transport.Flusher.MAX_FRAMED_PAYLOAD_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
        // set connection-local queue size to 0 so that all capacity is allocated from reserves
        DatabaseDescriptor.setNativeTransportReceiveQueueCapacityInBytes(0);
    }

    @Test
    public void handleErrorDuringNegotiation() throws Throwable
    {
        int messageCount = 0;
        Codec codec = Codec.crc(alloc);
        AllocationObserver observer = new AllocationObserver();
        InboundProxyHandler.Controller controller = new InboundProxyHandler.Controller();
        // Force protocol version to an unsupported version
        controller.withPayloadTransform(msg -> {
            ByteBuf bb = (ByteBuf)msg;
            bb.setByte(0, 99 & Envelope.PROTOCOL_VERSION_MASK);
            return msg;
        });

        ServerConfigurator configurator = ServerConfigurator.builder()
                                                            .withAllocationObserver(observer)
                                                            .withProxyController(controller)
                                                            .build();
        Server server = server(configurator);
        Client client = new Client(codec, messageCount);
        server.start();
        client.connect(address, port);
        assertFalse(client.isConnected());
        assertThat(client.getConnectionError())
            .isNotNull()
            .matches(message ->
                message.error.getMessage()
                             .equals("Invalid or unsupported protocol version (99); " +
                                     "supported versions are (3/v3, 4/v4, 5/v5, 6/v6-beta)"));
        server.stop();

        // the failure happens before any capacity is allocated
        observer.verifier().accept(0);
    }

    @Test
    public void handleCorruptionAfterNegotiation() throws Throwable
    {
        // A corrupt messaging frame should terminate the connection as clients
        // generally don't track which stream IDs are present in the frame, and the
        // server has no way to signal which streams are affected.
        // Before closing, the server should send an ErrorMessage to inform the
        // client of the corrupt message.
        int messageCount = 10;
        Codec codec = Codec.crc(alloc);
        AllocationObserver observer = new AllocationObserver();
        InboundProxyHandler.Controller controller = new InboundProxyHandler.Controller();

        ServerConfigurator configurator = ServerConfigurator.builder()
                                                            .withAllocationObserver(observer)
                                                            .withProxyController(controller)
                                                            .build();
        Server server = server(configurator);
        Client client = new Client(codec, messageCount);
        server.start();
        client.connect(address, port);
        assertTrue(client.isConnected());

        // Only install the transform after protocol negotiation is complete
        controller.withPayloadTransform(msg -> {
            // Corrupt frame
            ByteBuf bb = (ByteBuf) msg;
            bb.setByte(bb.readableBytes() / 2, 0xffff);
            return msg;
        });

        for (int i=0; i < messageCount; i++)
            client.send(randomEnvelope(i, Message.Type.OPTIONS));

        client.awaitResponses();
        // Client has disconnected
        assertFalse(client.isConnected());
        // But before it did, it sent an error response
        Envelope receieved = client.inboundMessages.poll();
        assertNotNull(receieved);
        Message.Response response = Message.responseDecoder().decode(client.channel, receieved);
        assertEquals(Message.Type.ERROR, response.type);
        assertTrue(((ErrorMessage)response).error.getMessage().contains("unrecoverable CRC mismatch detected in frame body"));

        // the failure happens before any capacity is allocated
        observer.verifier().accept(0);

        server.stop();
    }

    @Test
    public void handleCorruptionOfLargeMessage() throws Throwable
    {
        // A corrupt messaging frame should terminate the connection as clients
        // generally don't track which stream IDs are present in the frame, and the
        // server has no way to signal which streams are affected.
        // Before closing, the server should send an ErrorMessage to inform the
        // client of the corrupt message.
        // Client needs to expect multiple responses or else awaitResponses returns
        // after the error is first received and we race between handling the exception
        // caused by remote disconnection and checking the connection status.
        int messageCount = 2;
        Codec codec = Codec.crc(alloc);
        AllocationObserver observer = new AllocationObserver();
        InboundProxyHandler.Controller controller = new InboundProxyHandler.Controller();

        ServerConfigurator configurator = ServerConfigurator.builder()
                                                            .withAllocationObserver(observer)
                                                            .withProxyController(controller)
                                                            .build();
        Server server = server(configurator);
        Client client = new Client(codec, messageCount);
        server.start();
        client.connect(address, port);
        assertTrue(client.isConnected());

        // Only install the corrupting transform after protocol negotiation is complete
        controller.withPayloadTransform(new Function<Object, Object>()
        {
            // Don't corrupt the first frame as this would fail early and bypass capacity allocation.
            // Instead, allow enough bytes to fill the first frame through untouched. Then, corrupt
            // a byte which will be in the second frame of the large message .
            int seenBytes = 0;
            int corruptedByte = 0;
            public Object apply(Object o)
            {
                // If we've already injected some corruption, pass through
                if (corruptedByte > 0)
                    return o;

                // Will the current buffer size take us into the second frame? If so, corrupt it
                ByteBuf bb = (ByteBuf)o;
                if (seenBytes + bb.readableBytes() > MAX_FRAMED_PAYLOAD_SIZE + 100)
                {
                    int frameBoundary = MAX_FRAMED_PAYLOAD_SIZE - seenBytes;
                    corruptedByte = bb.readerIndex() + frameBoundary + 100;
                    bb.setByte(corruptedByte, 0xffff);
                }
                else
                {
                    seenBytes += bb.readableBytes();
                }

                return bb;
            }
        });

        int totalBytes = MAX_FRAMED_PAYLOAD_SIZE * 2;
        client.send(randomEnvelope(0, Message.Type.OPTIONS, totalBytes, totalBytes));
        client.awaitResponses();
        // Client has disconnected
        assertFalse(client.isConnected());
        // But before it did, it received an error response
        Envelope received = client.inboundMessages.poll();
        assertNotNull(received);
        Message.Response response = Message.responseDecoder().decode(client.channel, received);
        assertEquals(Message.Type.ERROR, response.type);
        assertTrue(((ErrorMessage)response).error.getMessage().contains("unrecoverable CRC mismatch detected in frame"));
        // total capacity is aquired when the first frame is read
        observer.verifier().accept(totalBytes);
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

        TestConsumer consumer = new TestConsumer(new ResultMessage.Void(), codec.encoder);
        AllocationObserver observer = new AllocationObserver();
        Message.Decoder<Message.Request> decoder = new FixedDecoder();
        Predicate<Envelope.Header> responseMatcher = h -> h.type == Message.Type.RESULT;
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
        TestConsumer consumer = new TestConsumer(new ResultMessage.Void(), codec.encoder);
        AllocationObserver observer = new AllocationObserver();
        Message.Decoder<Message.Request> decoder = new FixedDecoder()
        {
            Message.Request decode(Channel channel, Envelope source)
            {
                if (source.header.streamId != streamWithError)
                    return super.decode(channel, source);

                throw new RequestExecutionException(ExceptionCode.SYNTAX_ERROR,
                                                    "Error decoding message " + source.header.streamId)
                {/*test exception*/};
            }
        };

        Predicate<Envelope.Header> responseMatcher =
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
                         Predicate<Envelope.Header> responseMatcher,
                         LongConsumer allocationVerifier)
    {
        Server server = server(configurator);
        Client client = new Client(codec, messageCount);
        try
        {
            server.start();
            client.connect(address, port);
            assertTrue(configurator.waitUntilReady());

            for (int i = 0; i < messageCount; i++)
                client.send(randomEnvelope(i, Message.Type.OPTIONS));

            long totalBytes = client.sendSize;

            // verify that all messages went through the pipeline & our test message consumer
            client.awaitResponses();
            Envelope response;
            while ((response = client.pollResponses()) != null)
            {
                response.release();
                assertThat(response.header).matches(responseMatcher);
            }

            // verify that we did have to acquire some resources from the global/endpoint reserves
            allocationVerifier.accept(totalBytes);
        }
        catch (Throwable t)
        {
            logger.error("Unexpected error", t);
            throw new RuntimeException(t);
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

    private Envelope randomEnvelope(int streamId, Message.Type type)
    {
        return randomEnvelope(streamId, type, 100, 1024);
    }

    private Envelope randomEnvelope(int streamId, Message.Type type, int minSize, int maxSize)
    {
        byte[] bytes = randomishBytes(random, minSize, maxSize);
        return Envelope.create(type,
                               streamId,
                               ProtocolVersion.V5,
                               EnumSet.of(Envelope.Header.Flag.USE_BETA),
                               Unpooled.wrappedBuffer(bytes));
    }

    // Every CQL Envelope received will be parsed as an OptionsMessage, which is trivial to execute
    // on the server. This means we can randomise the actual content of the CQL messages to test
    // resource allocation/release (which is based purely on request size), without having to
    // worry about processing of the actual messages.
    static class FixedDecoder extends Message.Decoder<Message.Request>
    {
        Message.Request decode(Channel channel, Envelope source)
        {
            Message.Request request = new OptionsMessage();
            request.setSource(source);
            request.setStreamId(source.header.streamId);
            return request;
        }
    }

    // A simple consumer which "serves" a static response and employs a naive flusher
    static class TestConsumer implements MessageConsumer<Message.Request>
    {
        final Message.Response fixedResponse;
        final Envelope responseTemplate;
        final FrameEncoder frameEncoder;
        SimpleClient.SimpleFlusher flusher;

        TestConsumer(Message.Response fixedResponse, FrameEncoder frameEncoder)
        {
            this.fixedResponse = fixedResponse;
            this.responseTemplate = fixedResponse.encode(ProtocolVersion.V5);
            this.frameEncoder = frameEncoder;
        }

        public void accept(Channel channel, Message.Request message, Dispatcher.FlushItemConverter toFlushItem)
        {
            if (flusher == null)
                flusher = new SimpleClient.SimpleFlusher(frameEncoder);

            Flusher.FlushItem.Framed item = (Flusher.FlushItem.Framed)toFlushItem.toFlushItem(channel, message, fixedResponse);
            Envelope response = Envelope.create(responseTemplate.header.type,
                                                message.getStreamId(),
                                                ProtocolVersion.V5,
                                                responseTemplate.header.flags,
                                                responseTemplate.body.copy());
            item.release();
            flusher.enqueue(response);

            // Schedule the proto-flusher to collate any messages to be served
            // and flush them to the outbound pipeline
            flusher.schedule(channel.pipeline().lastContext());
        }
    }

    static class ServerConfigurator extends PipelineConfigurator
    {
        private final SimpleCondition pipelineReady = new SimpleCondition();
        private final MessageConsumer<Message.Request> consumer;
        private final AllocationObserver allocationObserver;
        private final Message.Decoder<Message.Request> decoder;
        private final InboundProxyHandler.Controller proxyController;

        public ServerConfigurator(Builder builder)
        {
            super(NativeTransportService.useEpoll(), false, false, UNENCRYPTED);
            this.consumer = builder.consumer;
            this.decoder = builder.decoder;
            this.allocationObserver = builder.observer;
            this.proxyController = builder.proxyController;
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
           InboundProxyHandler.Controller proxyController;

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

           Builder withProxyController(InboundProxyHandler.Controller proxyController)
           {
               this.proxyController = proxyController;
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

        protected void onInitialPipelineReady(ChannelPipeline pipeline)
        {
            if (proxyController != null)
            {
                InboundProxyHandler proxy = new InboundProxyHandler(proxyController);
                pipeline.addFirst("PROXY", proxy);
            }
        }

        protected void onNegotiationComplete(ChannelPipeline pipeline)
        {
            pipelineReady.signalAll();
        }

        private boolean waitUntilReady() throws InterruptedException
        {
            return pipelineReady.await(10, TimeUnit.SECONDS);
        }

        protected ClientResourceLimits.ResourceProvider resourceProvider(ClientResourceLimits.Allocator limits)
        {
            final ClientResourceLimits.ResourceProvider.Default delegate =
                new ClientResourceLimits.ResourceProvider.Default(limits);

            if (null == allocationObserver)
                return delegate;

            return new ClientResourceLimits.ResourceProvider()
            {
                public ResourceLimits.Limit globalLimit()
                {
                    return allocationObserver.global(delegate.globalLimit());
                }

                public AbstractMessageHandler.WaitQueue globalWaitQueue()
                {
                    return delegate.globalWaitQueue();
                }

                public ResourceLimits.Limit endpointLimit()
                {
                    return allocationObserver.endpoint(delegate.endpointLimit());
                }

                public AbstractMessageHandler.WaitQueue endpointWaitQueue()
                {
                    return delegate.endpointWaitQueue();
                }

                public void release()
                {
                    delegate.release();
                }
            };
        }

        protected MessageConsumer<Message.Request> messageConsumer()
        {
            return consumer == null ? super.messageConsumer() : consumer;
        }
    }

    static class AllocationObserver
    {
        volatile InstrumentedLimit endpoint;
        volatile InstrumentedLimit global;

        long endpointAllocationTotal()
        {
            return endpoint == null ? 0 : endpoint.totalAllocated.get();
        }

        long endpointReleaseTotal()
        {
            return endpoint == null ? 0 : endpoint.totalReleased.get();
        }

        long globalAllocationTotal()
        {
            return global == null ? 0 : global.totalAllocated.get();
        }

        long globalReleaseTotal()
        {
            return global == null ? 0 : global.totalReleased.get();
        }

        synchronized InstrumentedLimit endpoint(ResourceLimits.Limit delegate)
        {
            if (endpoint == null)
                endpoint = new InstrumentedLimit(delegate);
            return endpoint;
        }

        synchronized InstrumentedLimit global(ResourceLimits.Limit delegate)
        {
            if (global == null)
                global = new InstrumentedLimit(delegate);
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
                // assert that we definitely have no outstanding resources acquired from the reserves
                ClientResourceLimits.Allocator tracker =
                    ClientResourceLimits.getAllocatorForEndpoint(FBUtilities.getJustLocalAddress());
                assertThat(tracker.endpointUsing()).isEqualTo(0);
                assertThat(tracker.globallyUsing()).isEqualTo(0);
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
        final CountDownLatch responsesReceived;
        private volatile boolean connected = false;

        final Queue<Envelope> inboundMessages = new LinkedBlockingQueue<>();
        long sendSize = 0;
        SimpleClient.SimpleFlusher flusher;
        ErrorMessage connectionError;
        Throwable disconnectionError;

        Client(Codec codec, int expectedResponses)
        {
            this.codec = codec;
            this.expectedResponses = expectedResponses;
            this.responsesReceived = new CountDownLatch(expectedResponses);
            flusher = new SimpleClient.SimpleFlusher(codec.encoder);
        }

        private void connect(InetAddress address, int port) throws IOException, InterruptedException
        {
            final CountDownLatch ready = new CountDownLatch(1);
            Bootstrap bootstrap = new Bootstrap()
                                    .group(new NioEventLoopGroup(0, new NamedThreadFactory("TEST-CLIENT")))
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
                    pipeline.addLast("envelopeEncoder", Envelope.Encoder.instance);
                    pipeline.addLast("messageEncoder", PreV5Handlers.ProtocolEncoder.instance);
                    pipeline.addLast("envelopeDecoder", new Envelope.Decoder());
                    // Inbound handler to perform the handshake & modify the pipeline on receipt of a READY
                    pipeline.addLast("handshake", new MessageToMessageDecoder<Envelope>()
                    {
                        final Envelope.Decoder decoder = new Envelope.Decoder();
                        protected void decode(ChannelHandlerContext ctx, Envelope msg, List<Object> out) throws Exception
                        {
                            // Handle ERROR responses during initial connection and protocol negotiation
                            if ( msg.header.type == Message.Type.ERROR)
                            {
                                connectionError = (ErrorMessage)Message.responseDecoder()
                                                                       .decode(ctx.channel(), msg);

                                msg.release();
                                logger.info("ERROR");
                                stop();
                                ready.countDown();
                                return;
                            }

                            // As soon as we receive a READY message, modify the pipeline
                            assert msg.header.type == Message.Type.READY;
                            msg.release();

                            // just split the messaging into cql messages and stash them for verification
                            FrameDecoder.FrameProcessor processor =  frame -> {
                                if (frame instanceof FrameDecoder.IntactFrame)
                                {
                                    ByteBuffer bytes = ((FrameDecoder.IntactFrame)frame).contents.get();
                                    while(bytes.hasRemaining())
                                    {
                                        ByteBuf buffer = Unpooled.wrappedBuffer(bytes);
                                        try
                                        {
                                            inboundMessages.add(decoder.decode(buffer));
                                            responsesReceived.countDown();
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

                            // for testing purposes, don't actually encode CQL messages,
                            // we supply messaging frames directly to this client
                            channel.pipeline().remove("envelopeEncoder");
                            channel.pipeline().remove("messageEncoder");
                            channel.pipeline().remove("envelopeDecoder");

                            // replace this handshake handler with an inbound message frame decoder
                            channel.pipeline().replace(this, "frameDecoder", codec.decoder);
                            // add an outbound message frame encoder
                            channel.pipeline().addLast("frameEncoder", codec.encoder);
                            channel.pipeline().addLast("errorHandler", new ChannelInboundHandlerAdapter()
                            {
                                @Override
                                public void exceptionCaught(final ChannelHandlerContext ctx, Throwable cause) throws Exception
                                {
                                    // if the connection is closed finish early as
                                    // we don't want to wait for expected responses
                                    if (cause instanceof IOException)
                                    {
                                        connected = false;
                                        disconnectionError = cause;
                                        int remaining = (int) responsesReceived.getCount();
                                        for (int i=0; i < remaining; i++)
                                            responsesReceived.countDown();
                                    }
                                }
                            });
                            codec.decoder.activate(processor);
                            connected = true;
                            // Schedule the proto-flusher to collate any messages that have been
                            // written, via enqueue(Envelope message), and flush them to the outbound pipeline
                            flusher.schedule(channel.pipeline().lastContext());
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

        void send(Envelope request)
        {
            flusher.enqueue(request);
            sendSize += request.header.bodySizeInBytes;
        }

        private void awaitResponses() throws InterruptedException
        {
            if (!responsesReceived.await(10, TimeUnit.SECONDS))
            {
                fail(String.format("Didn't receive all responses, expected %d, actual %d",
                                   expectedResponses,
                                   inboundMessages.size()));
            }
        }

        private boolean isConnected()
        {
            return connected;
        }

        private ErrorMessage getConnectionError()
        {
            return connectionError;
        }

        private Envelope pollResponses()
        {
            return inboundMessages.poll();
        }

        private void stop()
        {
            if (channel != null && channel.isOpen())
                channel.close().awaitUninterruptibly();

            flusher.releaseAll();

            Envelope f;
            while ((f = inboundMessages.poll()) != null)
                f.release();
        }
    }
}
