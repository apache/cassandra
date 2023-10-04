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
import java.util.function.*;

import com.google.common.util.concurrent.Uninterruptibles;
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
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.net.*;
import org.apache.cassandra.net.proxy.InboundProxyHandler;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.transport.CQLMessageHandler.MessageConsumer;
import org.apache.cassandra.transport.ClientResourceLimits.Overload;
import org.apache.cassandra.transport.messages.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.NonBlockingRateLimiter;
import org.awaitility.Awaitility;

import static org.apache.cassandra.config.EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED;
import static org.apache.cassandra.io.util.FileUtils.ONE_MIB;
import static org.apache.cassandra.net.FramingTest.randomishBytes;
import static org.apache.cassandra.transport.Flusher.MAX_FRAMED_PAYLOAD_SIZE;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;
import static org.apache.cassandra.utils.concurrent.NonBlockingRateLimiter.NO_OP_LIMITER;
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
        // set transport to max frame size possible
        DatabaseDescriptor.setNativeTransportMaxFrameSize(256 * (int) ONE_MIB);
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
    public void handleFrameCorruptionAfterNegotiation() throws Throwable
    {
        // A corrupt messaging frame should terminate the connection as clients
        // generally don't track which stream IDs are present in the frame, and the
        // server has no way to signal which streams are affected.
        // Before closing, the server should send an ErrorMessage to inform the
        // client of the corrupt message.
        Function<ByteBuf, ByteBuf> corruptor = msg -> {
            flipBit(msg, msg.readableBytes() / 2);
            return msg;
        };
        IntFunction<Envelope> envelopeProvider = i -> randomEnvelope(i, Message.Type.OPTIONS);
        Predicate<ErrorMessage> errorCheck =
            error -> error.error.getMessage().contains("unrecoverable CRC mismatch detected in frame body");

        // expected allocated bytes are 0 as the errors happen before allocation
        testFrameCorruption(1, Codec.crc(alloc), envelopeProvider, corruptor, 0, errorCheck);
        testFrameCorruption(1, Codec.lz4(alloc), envelopeProvider, corruptor, 0, errorCheck);

        // we don't do more rounds with higher message count as the connection
        // will be closed when the first corrupt frame is encountered
    }

    @Test
    public void handleCorruptionOfLargeMessageFrame() throws Throwable
    {
        // A corrupt messaging frame should terminate the connection as clients
        // generally don't track which stream IDs are present in the frame, and the
        // server has no way to signal which streams are affected.
        // Before closing, the server should send an ErrorMessage to inform the
        // client of the corrupt message.
        // Client needs to expect multiple responses or else awaitResponses returns
        // after the error is first received and we race between handling the exception
        // caused by remote disconnection and checking the connection status.

        Function<ByteBuf, ByteBuf> corruptor = new Function<ByteBuf, ByteBuf>()
        {
            // Don't corrupt the first frame as this would fail early and bypass capacity allocation.
            // Instead, allow enough bytes to fill the first frame through untouched. Then, corrupt
            // a byte which will be in the second frame of the large message .
            int seenBytes = 0;
            int corruptedByte = 0;

            public ByteBuf apply(ByteBuf msg)
            {
                // Will the current buffer size take us into the second frame? If so, corrupt it
                if (corruptedByte == 0 && seenBytes + msg.readableBytes() > MAX_FRAMED_PAYLOAD_SIZE * 3 / 2)
                {
                    logger.info("Corrupting");
                    corruptedByte = msg.readerIndex() + 100;
                    flipBit(msg, corruptedByte);
                }
                else
                {
                    logger.info("Skipping");
                    seenBytes += msg.readableBytes();
                }

                return msg;
            }
        };

        int totalBytesPerEnvelope = MAX_FRAMED_PAYLOAD_SIZE * 3 / 2; // make sure we send 2 frame and no more
        IntFunction<Envelope> envelopeProvider = i -> randomEnvelope(i, Message.Type.OPTIONS, totalBytesPerEnvelope, totalBytesPerEnvelope);
        Predicate<ErrorMessage> errorCheck =
            error -> error.error.getMessage().contains("unrecoverable CRC mismatch detected in frame body");

        testFrameCorruption(1, Codec.crc(alloc), envelopeProvider, corruptor, totalBytesPerEnvelope, errorCheck);
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

        runTest(configurator, codec, messageCount, (i) -> randomEnvelope(i, Message.Type.OPTIONS), responseMatcher, observer.verifier());
    }

    @Test
    public void testRecoverableEnvelopeDecodingErrors()
    {
        // If an error is encountered while decoding an Envelope header,
        // it should be possible to continue processing subsequent Envelopes
        // by skipping the Envelope body. For instance, a ProtocolException
        // caused by an invalid opcode or version flag in the header should
        // not require the connection to be terminated. Instead, an error
        // response should be returned with the correct stream id and further
        // Envelopes processed as normal.

        // every other message should error while extracting the Envelope header
        IntPredicate shouldError = i -> i % 2 == 0;
        testEnvelopeDecodingErrors(10, shouldError, Codec.crc(alloc));
        testEnvelopeDecodingErrors(10, shouldError, Codec.lz4(alloc));

        testEnvelopeDecodingErrors(100, shouldError, Codec.crc(alloc));
        testEnvelopeDecodingErrors(100, shouldError, Codec.lz4(alloc));

        testEnvelopeDecodingErrors(1000, shouldError, Codec.crc(alloc));
        testEnvelopeDecodingErrors(1000, shouldError, Codec.lz4(alloc));
    }

    private void testEnvelopeDecodingErrors(int messageCount, IntPredicate shouldError, Codec codec)
    {
        TestConsumer consumer = new TestConsumer(new ResultMessage.Void(), codec.encoder);
        AllocationObserver observer = new AllocationObserver(false);
        Message.Decoder<Message.Request> decoder = new FixedDecoder();

        // mutate the request from the erroring streams to have an invalid opcode (99)
        IntFunction<Envelope> envelopeProvider = mutatedEnvelopeProvider(shouldError, b -> b.put(4, (byte)99));

        Predicate<Envelope.Header> responseMatcher =
        h ->  (shouldError.test(h.streamId) && h.type == Message.Type.ERROR) || h.type == Message.Type.RESULT;

        ServerConfigurator configurator = ServerConfigurator.builder()
                                                            .withConsumer(consumer)
                                                            .withAllocationObserver(observer)
                                                            .withDecoder(decoder)
                                                            .build();

        runTest(configurator, codec, messageCount, envelopeProvider, responseMatcher, observer.verifier());
    }

    @Test
    public void testUnrecoverableEnvelopeDecodingErrors()
    {
        // If multiple consecutive Envelopes in a Frame cause protocol
        // exceptions during decoding, we fail fast and close the connection.
        // The reason for this is that while some protocol errors may be
        // non-fatal (e.g. an incorrect opcode, or missing BETA flag), a
        // badly behaved client could also include garbage which may render
        // any following bytes in the Frame unusable, even though the Frame
        // level CRC32 is valid for the payload.
        int maxErrorCount = DatabaseDescriptor.getConsecutiveMessageErrorsThreshold();

        // mutate the request from the erroring streams to have an invalid opcode (99)
        IntFunction<Envelope> envelopeProvider = mutatedEnvelopeProvider(ignored -> true, b -> b.put(4, (byte)99));

        Predicate<ErrorMessage> errorCheck = error -> error.error.getMessage().contains("Unknown opcode 99");
        testFrameCorruption(maxErrorCount + 1, Codec.crc(alloc), envelopeProvider, Function.identity(), 0, errorCheck);
    }

    @Test
    public void testNegativeEnvelopeBodySize()
    {
        // A negative value for the body length of an envelope is essentially a
        // fatal exception as the stream of bytes is unrecoverable

        // the first message should error while extracting the Envelope header
        IntPredicate shouldError = i -> i == 0;
        // set the bodyLength byte to a negative value
        IntFunction<Envelope> envelopeProvider = mutatedEnvelopeProvider(shouldError, b -> b.putInt(5, -10));
        Predicate<ErrorMessage> errorCheck = error ->
            error.error.getMessage().contains("Invalid value for envelope header body length field: -10");
        testFrameCorruption(1, Codec.crc(alloc), envelopeProvider, Function.identity(), 0, errorCheck);
    }

    @Test
    public void testRecoverableMessageDecodingErrors()
    {
        // If an error is encountered while decoding a CQL message body
        // then it is usually safe to continue processing subsequent
        // Envelopes provided that the error is localised to the message
        // body. If, following such an error, we are able to successfully
        // extract an Envelope header from the Frame payload we continue
        // processing as normal. However, if the subsequent header cannot
        // be extracted, we infer that the corruption of the previous message
        // has rendered the entire Frame unrecoverable and close the client
        // connection.
        recoverableMessageDecodingErrorEncounteredMidFrame(10, Codec.crc(alloc));
        recoverableMessageDecodingErrorEncounteredMidFrame(10, Codec.lz4(alloc));

        recoverableMessageDecodingErrorEncounteredMidFrame(100, Codec.crc(alloc));
        recoverableMessageDecodingErrorEncounteredMidFrame(100, Codec.lz4(alloc));

        recoverableMessageDecodingErrorEncounteredMidFrame(1000, Codec.crc(alloc));
        recoverableMessageDecodingErrorEncounteredMidFrame(1000, Codec.lz4(alloc));
    }

    private void recoverableMessageDecodingErrorEncounteredMidFrame(int messageCount, Codec codec)
    {
        // Message bodies are consistent with Envelope headers, but decoding a message
        // mid-frame generates an error. A concrete example would be a BatchMessage
        // which contains SELECT statements.
        final int streamWithError = messageCount / 2;
        TestConsumer consumer = new TestConsumer(new ResultMessage.Void(), codec.encoder);
        AllocationObserver observer = new AllocationObserver();
        Message.Decoder<Message.Request> decoder =
            new FixedDecoder(i -> i == streamWithError,
                             new ProtocolException("An exception was encountered when decoding a CQL message"));

        Predicate<Envelope.Header> responseMatcher =
            h -> (h.streamId == streamWithError && h.type == Message.Type.ERROR) || h.type == Message.Type.RESULT;

        ServerConfigurator configurator = ServerConfigurator.builder()
                                                            .withConsumer(consumer)
                                                            .withAllocationObserver(observer)
                                                            .withDecoder(decoder)
                                                            .build();

        runTest(configurator, codec, messageCount, (i) -> randomEnvelope(i, Message.Type.OPTIONS), responseMatcher, observer.verifier());
    }

    @Test
    public void testUnrecoverableMessageDecodingErrors()
    {
        // If multiple consecutive CQL Messages in a Frame cause protocol
        // exceptions during message decoding, we fail fast and close
        // the connection. The reason for this is that while some protocol
        // errors may be non-fatal (e.g. a SELECT statement contained in a
        // BatchMessage, or unknown consistency level), a badly behaved
        // client could also send garbage which may render any following
        // bytes in the Frame unusable, even though the Frame level CRC32
        // is valid for the payload.
        int maxErrorCount = DatabaseDescriptor.getConsecutiveMessageErrorsThreshold();
        final ProtocolException protocolError = new ProtocolException("Unknown opcode 99");
        IntFunction<Envelope> envelopeProvider = (i) -> randomEnvelope(i, Message.Type.OPTIONS);
        FixedDecoder decoder = new FixedDecoder(ignored -> true, protocolError);
        Function<ByteBuf, ByteBuf> frameTransform = Function.identity();
        Predicate<ErrorMessage> errorCheck = error -> error.error.getMessage().contains(protocolError.getMessage());
        testFrameCorruption(maxErrorCount + 1, Codec.crc(alloc), envelopeProvider, frameTransform, 0, decoder, errorCheck);
    }

    private void runTest(ServerConfigurator configurator,
                         Codec codec,
                         int messageCount,
                         IntFunction<Envelope> envelopeProvider,
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
                client.send(envelopeProvider.apply(i));
            client.awaitFlushed();

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
            fail();
        }
        finally
        {
            client.stop();
            server.stop();
        }
    }

    private void testFrameCorruption(int messageCount,
                                     Codec codec,
                                     IntFunction<Envelope> envelopeProvider,
                                     Function<ByteBuf, ByteBuf> transform,
                                     long expectedBytesAllocated,
                                     Predicate<ErrorMessage> errorPredicate)
    {
        testFrameCorruption(messageCount, codec, envelopeProvider, transform, expectedBytesAllocated, new FixedDecoder(), errorPredicate);
    }

    private void testFrameCorruption(int messageCount,
                                     Codec codec,
                                     IntFunction<Envelope> envelopeProvider,
                                     Function<ByteBuf, ByteBuf> transform,
                                     long expectedBytesAllocated,
                                     FixedDecoder requestDecoder,
                                     Predicate<ErrorMessage> errorPredicate)
    {
        AllocationObserver observer = new AllocationObserver(false);
        InboundProxyHandler.Controller controller = new InboundProxyHandler.Controller();

        ServerConfigurator configurator = ServerConfigurator.builder()
                                                            .withAllocationObserver(observer)
                                                            .withProxyController(controller)
                                                            .withDecoder(requestDecoder)
                                                            .build();
        Server server = server(configurator);
        Client client = new Client(codec, messageCount);
        server.start();
        try
        {
            client.connect(address, port);
            assertTrue(client.isConnected());

            // Only install the transform after protocol negotiation is complete
            controller.withPayloadTransform(transform);

            for (int i = 0; i < messageCount; i++)
                client.send(envelopeProvider.apply(i));
            client.awaitFlushed();

            client.awaitResponses();
            // Client has disconnected
            client.awaitState(false);
            // But before it did, it sent an error response
            Envelope received = client.inboundMessages.poll();
            assertNotNull(received);
            Message.Response response = Message.responseDecoder().decode(client.channel, received);
            assertEquals(Message.Type.ERROR, response.type);
            assertTrue(errorPredicate.test((ErrorMessage) response));

            observer.verifier().accept(expectedBytesAllocated);
        }
        catch (Exception e)
        {
            logger.error("Unexpected error", e);
            fail();
        }
        finally
        {
            server.stop();
        }
    }

    private Server server(ServerConfigurator configurator)
    {
        Server server = new Server.Builder().withHost(address)
                                  .withPort(port)
                                  .withPipelineConfigurator(configurator)
                                  .build();
        ClientMetrics.instance.init(Collections.singleton(server));
        return server;
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

    private IntFunction<Envelope> mutatedEnvelopeProvider(IntPredicate streamIdMatcher, Consumer<ByteBuffer> headerMutator)
    {
        // enables tests to mutate Envelope headers as they're serialized into a Frame
        // payload. For instance, a test may modify the header length or set the opcode
        // to something invalid to simulate a buggy client. Frame level CRCs will remain
        // valid, so this can be used to exercise the CQL encoding layer.
        return (i) -> new MutableEnvelope(randomEnvelope(i, Message.Type.OPTIONS))
        {
            @Override
            Consumer<ByteBuffer> headerTransform()
            {
                if (streamIdMatcher.test(i))
                    return headerMutator;

                return super.headerTransform();
            }
        };
    }

    private void flipBit(ByteBuf buf, int index)
    {
        buf.setByte(index, buf.getByte(index) ^ (1 << 4));
    }

    private static class MutableEnvelope extends Envelope
    {
        public MutableEnvelope(Envelope source)
        {
            super(source.header, source.body);
        }

        Consumer<ByteBuffer> headerTransform()
        {
            return byteBuffer -> {};
        }

        @Override
        public void encodeHeaderInto(ByteBuffer buf)
        {
            int before = buf.position();
            super.encodeHeaderInto(buf);
            int after = buf.position();

            // slice the output buffer to get another
            // which shares the same backing bytes but
            // is limited to the size of an Envelope.Header
            buf.position(before);
            ByteBuffer slice = buf.slice();
            slice.limit(after - before);
            buf.position(after);

            // Apply the transformation to the header bytes
            headerTransform().accept(slice);
        }
    }

    // Every CQL Envelope received will be parsed as an OptionsMessage, which is trivial to execute
    // on the server. This means we can randomise the actual content of the CQL messages to test
    // resource allocation/release (which is based purely on request size), without having to
    // worry about processing of the actual messages.
    static class FixedDecoder extends Message.Decoder<Message.Request>
    {
        IntPredicate isErrorStream;
        ProtocolException error;

        FixedDecoder()
        {
           this(i -> false, null);
        }

        FixedDecoder(IntPredicate isErrorStream, ProtocolException error)
        {
           this.isErrorStream = isErrorStream;
           this.error = error;
        }

        Message.Request decode(Channel channel, Envelope source)
        {
            if (isErrorStream.test(source.header.streamId))
                throw error;

            Message.Request request = new OptionsMessage();
            request.setSource(source);
            request.setStreamId(source.header.streamId);
            Connection connection = channel.attr(Connection.attributeKey).get();
            request.attach(connection);

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

        public void accept(Channel channel, Message.Request message, Dispatcher.FlushItemConverter toFlushItem, Overload backpressure)
        {
            if (flusher == null)
                flusher = new SimpleClient.SimpleFlusher(frameEncoder);

            Envelope response = Envelope.create(responseTemplate.header.type,
                                                message.getStreamId(),
                                                ProtocolVersion.V5,
                                                responseTemplate.header.flags,
                                                responseTemplate.body.copy());
            flusher.enqueue(response);
            // Schedule the proto-flusher to collate any messages to be served
            // and flush them to the outbound pipeline
            flusher.schedule(channel.pipeline().lastContext());
            // this simulates the release of the allocated resources that a real flusher would do
            Flusher.FlushItem.Framed item = (Flusher.FlushItem.Framed)toFlushItem.toFlushItem(channel, message, fixedResponse);
            item.release();
        }
    }

    static class ServerConfigurator extends PipelineConfigurator
    {
        private final Condition pipelineReady = newOneTimeCondition();
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

                @Override
                public NonBlockingRateLimiter requestRateLimiter()
                {
                    return NO_OP_LIMITER;
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

        final boolean strict;

        AllocationObserver()
        {
            this(true);
        }

        AllocationObserver(boolean strict)
        {
            this.strict = strict;
        }

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
                // if strict mode (the default), verify that we did have to acquire the expected resources
                // from the global/endpoint reserves and that we released the same amount. If any errors
                // were encountered before allocation (i.e. decoding Envelope headers), the message bytes
                // are never allocated (and so neither are they released).
                if (strict)
                {
                    assertThat(endpointAllocationTotal()).isEqualTo(totalBytes);
                    assertThat(globalAllocationTotal()).isEqualTo(totalBytes);
                    // and that we released it all
                    assertThat(endpointReleaseTotal()).isEqualTo(totalBytes);
                    assertThat(globalReleaseTotal()).isEqualTo(totalBytes);
                }
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
                                    if (cause instanceof IOException)
                                    {
                                        connected = false;
                                        disconnectionError = cause;
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
            assertTrue(responsesReceived.await(10, TimeUnit.SECONDS));
        }

        private void awaitState(boolean connected)
        {
            Awaitility.await()
                      .atMost(10, TimeUnit.SECONDS)
                      .until(() -> this.connected == connected);
        }

        private void awaitFlushed()
        {
            int lastSize = flusher.outbound.size();
            long lastUpdate = System.currentTimeMillis();
            while (!flusher.outbound.isEmpty())
            {
                int newSize = flusher.outbound.size();
                if (newSize < lastSize)
                {
                    lastSize = newSize;
                    lastUpdate = System.currentTimeMillis();
                }
                else if (System.currentTimeMillis() - lastUpdate > 30000)
                {
                    throw new RuntimeException("Timeout");
                }
                logger.info("Waiting for flush to complete - outbound queue size: {}", flusher.outbound.size());
                Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
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
