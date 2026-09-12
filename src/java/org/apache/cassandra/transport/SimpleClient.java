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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture; // checkstyle: permit this import
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.SynchronousQueue; // checkstyle: permit this import
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.net.AbstractMessageHandler;
import org.apache.cassandra.net.AsyncChannelPromise;
import org.apache.cassandra.net.BufferPoolAllocator;
import org.apache.cassandra.net.FrameDecoder;
import org.apache.cassandra.net.FrameDecoderCrc;
import org.apache.cassandra.net.FrameDecoderLZ4;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.net.FrameEncoderCrc;
import org.apache.cassandra.net.FrameEncoderLZ4;
import org.apache.cassandra.net.GlobalBufferPoolAllocator;
import org.apache.cassandra.net.ResourceLimits;
import org.apache.cassandra.security.ISslContextFactory;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.transport.ClientResourceLimits.Overload;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.utils.concurrent.NonBlockingRateLimiter;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Promise; // checkstyle: permit this import
import io.netty.util.concurrent.PromiseCombiner;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;

import static org.apache.cassandra.net.SocketFactory.newSslHandler;
import static org.apache.cassandra.transport.CQLMessageHandler.envelopeSize;
import static org.apache.cassandra.transport.Flusher.MAX_FRAMED_PAYLOAD_SIZE;
import static org.apache.cassandra.transport.PipelineConfigurator.SSL_FACTORY_CONTEXT_DESCRIPTION;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.concurrent.BlockingQueues.newBlockingQueue;
import static org.apache.cassandra.utils.concurrent.NonBlockingRateLimiter.NO_OP_LIMITER;

public class SimpleClient implements Closeable
{

    public static final int TIMEOUT_SECONDS = 10;

    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    private static final Logger logger = LoggerFactory.getLogger(SimpleClient.class);

    public final String host;
    public final int port;
    private final EncryptionOptions.ClientEncryptionOptions encryptionOptions;
    private final int largeMessageThreshold;

    protected final ResponseHandler responseHandler = new ResponseHandler(this);
    protected final Connection.Tracker tracker = new ConnectionTracker();
    protected final ProtocolVersion version;
    // We don't track connection really, so we don't need one Connection per channel
    protected Connection connection;
    protected Bootstrap bootstrap;
    protected Channel channel;
    protected ChannelFuture lastWriteFuture;
    private final AtomicBoolean draining = new AtomicBoolean(false);
    private volatile CompletableFuture<Void> inFlight = CompletableFuture.completedFuture(null);
    protected String compression;

    public static class Builder
    {
        private final String host;
        private final int port;
        private EncryptionOptions.ClientEncryptionOptions encryptionOptions = new EncryptionOptions.ClientEncryptionOptions();
        private ProtocolVersion version = ProtocolVersion.CURRENT;
        private boolean useBeta = false;
        private int largeMessageThreshold = FrameEncoder.Payload.MAX_SIZE;

        private Builder(String host, int port)
        {
            this.host = host;
            this.port = port;
        }

        public Builder encryption(EncryptionOptions.ClientEncryptionOptions options)
        {
            this.encryptionOptions = options;
            return this;
        }

        public Builder useBeta()
        {
            this.useBeta = true;
            return this;
        }

        public Builder protocolVersion(ProtocolVersion version)
        {
            this.version = version;
            return this;
        }

        public Builder largeMessageThreshold(int bytes)
        {
            largeMessageThreshold = bytes;
            return this;
        }

        public SimpleClient build()
        {
            if (version.isBeta() && !useBeta)
                throw new IllegalArgumentException(String.format("Beta version of server used (%s), but USE_BETA flag is not set", version));
            return new SimpleClient(this);
        }
    }


    public static Builder builder(String host, int port)
    {
        return new Builder(host, port);
    }

    private SimpleClient(Builder builder)
    {
        this.host = builder.host;
        this.port = builder.port;
        this.version = builder.version;
        this.encryptionOptions = builder.encryptionOptions.applyConfig();
        this.largeMessageThreshold = builder.largeMessageThreshold;
    }

    public SimpleClient(String host, int port, ProtocolVersion version, EncryptionOptions.ClientEncryptionOptions encryptionOptions)
    {
        this(host, port, version, false, encryptionOptions);
    }

    public SimpleClient(String host, int port, EncryptionOptions.ClientEncryptionOptions encryptionOptions)
    {
        this(host, port, ProtocolVersion.CURRENT, encryptionOptions);
    }

    public SimpleClient(String host, int port, ProtocolVersion version)
    {
        this(host, port, version, new EncryptionOptions.ClientEncryptionOptions());
    }

    public SimpleClient(String host, int port, ProtocolVersion version, boolean useBeta, EncryptionOptions.ClientEncryptionOptions encryptionOptions)
    {
        this.host = host;
        this.port = port;
        if (version.isBeta() && !useBeta)
            throw new IllegalArgumentException(String.format("Beta version of server used (%s), but USE_BETA flag is not set", version));

        this.version = version;
        this.encryptionOptions = encryptionOptions.applyConfig();
        this.largeMessageThreshold = FrameEncoder.Payload.MAX_SIZE -
                                        Math.max(FrameEncoderCrc.HEADER_AND_TRAILER_LENGTH,
                                                 FrameEncoderLZ4.HEADER_AND_TRAILER_LENGTH);
    }

    public SimpleClient(String host, int port)
    {
        this(host, port, new EncryptionOptions.ClientEncryptionOptions());
    }

    public SimpleClient connect(boolean useCompression) throws IOException
    {
        return connect(useCompression, false);
    }

    public SimpleClient connect(boolean useCompression, boolean throwOnOverload) throws IOException
    {
        establishConnection();

        Map<String, String> options = new HashMap<>();
        options.put(StartupMessage.CQL_VERSION, "3.0.0");
        if (throwOnOverload)
            options.put(StartupMessage.THROW_ON_OVERLOAD, "1");
        connection.setThrowOnOverload(throwOnOverload);

        if (useCompression)
        {
            options.put(StartupMessage.COMPRESSION, "LZ4");
            connection.setCompressor(Compressor.LZ4Compressor.instance);
        }
        execute(new StartupMessage(options));

        return this;
    }

    public void setEventHandler(EventHandler eventHandler)
    {
        responseHandler.eventHandler = eventHandler;
    }

    @VisibleForTesting
    void establishConnection() throws IOException
    {
        // Configure the client.
        bootstrap = new Bootstrap()
                    .group(new NioEventLoopGroup(new NamedThreadFactory("SimpleClient-nioEventLoopGroup")))
                    .channel(io.netty.channel.socket.nio.NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true);

        // Configure the pipeline factory.
        if(encryptionOptions.getEnabled())
        {
            bootstrap.handler(new SecureInitializer(largeMessageThreshold));
        }
        else
        {
            bootstrap.handler(new Initializer(largeMessageThreshold));
        }
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));

        // Wait until the connection attempt succeeds or fails.
        channel = future.awaitUninterruptibly().channel();
        if (!future.isSuccess())
        {
            bootstrap.group().shutdownGracefully();
            throw new IOException("Connection Error", future.cause());
        }
    }

    public ResultMessage execute(String query, ConsistencyLevel consistency)
    {
        return execute(query, Collections.<ByteBuffer>emptyList(), consistency);
    }

    public ResultMessage execute(String query, List<ByteBuffer> values, ConsistencyLevel consistencyLevel)
    {
        Message.Response msg = execute(new QueryMessage(query, QueryOptions.forInternalCalls(consistencyLevel, values)));
        assert msg instanceof ResultMessage;
        return (ResultMessage)msg;
    }

    public ResultMessage.Prepared prepare(String query)
    {
        Message.Response msg = execute(new PrepareMessage(query, null));
        assert msg instanceof ResultMessage.Prepared;
        return (ResultMessage.Prepared)msg;
    }

    public ResultMessage executePrepared(ResultMessage.Prepared prepared, List<ByteBuffer> values, ConsistencyLevel consistency)
    {
        Message.Response msg = execute(new ExecuteMessage(prepared.statementId, prepared.resultMetadataId, QueryOptions.forInternalCalls(consistency, values)));
        assert msg instanceof ResultMessage;
        return (ResultMessage)msg;
    }

    public void close()
    {
        // Wait until all messages are flushed before closing the channel.
        if (lastWriteFuture != null)
            lastWriteFuture.awaitUninterruptibly();

        // Close the connection.  Make sure the close operation ends because
        // all I/O operations are asynchronous in Netty.
        channel.close().awaitUninterruptibly();

        // Shut down all thread pools to exit.
        bootstrap.group().shutdownGracefully();
    }

    public Message.Response execute(Message.Request request)
    {
        return execute(request, true);
    }


    public Message.Response execute(Message.Request request, boolean throwOnErrorResponse)
    {
        if (draining.get())
            throw new RuntimeException("Connection is draining (GRACEFUL_DISCONNECT received)");

        CompletableFuture<Void> requestCompletion = new CompletableFuture<>();
        inFlight = requestCompletion;
        try
        {
            request.attach(connection);
            lastWriteFuture = channel.writeAndFlush(Collections.singletonList(request));
            Message.Response msg = responseHandler.responses.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (msg == null)
                throw new RuntimeException("timeout");
            if (throwOnErrorResponse && msg instanceof ErrorMessage)
                throw new RuntimeException((Throwable)((ErrorMessage)msg).error);
            return msg;
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        finally
        {
            requestCompletion.complete(null);
        }
    }

    public Map<Message.Request, Message.Response> execute(List<Message.Request> requests)
    {
        try
        {
            Map<Message.Request, Message.Response> rrMap = new HashMap<>();

            if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
            {
                for (int i = 0; i < requests.size(); i++)
                {
                    Message.Request message = requests.get(i);
                    message.setSource(new Envelope(Envelope.Header.dummy(i, message.type), null));
                    message.attach(connection);
                }
                lastWriteFuture = channel.writeAndFlush(requests);

                long deadline = currentTimeMillis() + TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS);
                for (int i = 0; i < requests.size(); i++)
                {
                    Message.Response msg = responseHandler.responses.poll(deadline - currentTimeMillis(), TimeUnit.MILLISECONDS);
                    if (msg == null)
                        throw new RuntimeException("timeout");
                    if (msg instanceof ErrorMessage)
                        throw new RuntimeException((Throwable) ((ErrorMessage) msg).error);
                    rrMap.put(requests.get(msg.getSource().header.streamId), msg);
                }
            }
            else
            {
                // V4 doesn't support batching
                for (Message.Request request : requests)
                    rrMap.put(request, execute(request));
            }

            return rrMap;
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }

    /**
     * The stream id to frame an outbound client request with. SimpleClient carries the intended id on the
     * request's (dummy) source envelope (see {@link #execute(List)} and callers that pipeline requests).
     * When no source has been assigned, we fall back to 0, which is sufficient for the non-pipelined path
     * where only a single request is ever in flight.
     */
    private static int outboundStreamId(Message message)
    {
        Envelope source = message.getSource();
        return source == null ? 0 : source.header.streamId;
    }

    public interface EventHandler
    {
        void onEvent(Event event);
    }

    private void handleGracefulDisconnect()
    {
        draining.set(true);
        inFlight.thenRun(() -> channel.eventLoop().execute(channel::close));
        channel.closeFuture().addListener(f -> bootstrap.group().shutdownGracefully());
    }

    public static class SimpleEventHandler implements EventHandler
    {
        public final BlockingQueue<Event> queue = newBlockingQueue();

        public void onEvent(Event event)
        {
            queue.add(event);
        }
    }

    private static class ConnectionTracker implements Connection.Tracker
    {
        public void addConnection(Channel ch, Connection connection) {}

        @Override
        public boolean isRunning()
        {
            return true;
        }
    }

    private static class HandlerNames
    {
        private static final String ENVELOPE_DECODER        = "envelopeDecoder";
        private static final String ENVELOPE_ENCODER        = "envelopeEncoder";
        private static final String COMPRESSOR              = "compressor";
        private static final String DECOMPRESSOR            = "decompressor";
        private static final String MESSAGE_DECODER         = "messageDecoder";
        private static final String MESSAGE_ENCODER         = "messageEncoder";

        private static final String INITIAL_HANDLER         = "intitialHandler";
        private static final String RESPONSE_HANDLER        = "responseHandler";

        private static final String FRAME_DECODER           = "frameDecoder";
        private static final String FRAME_ENCODER           = "frameEncoder";
        private static final String PROCESSOR               = "processor";
    }

    private static class InitialHandler extends MessageToMessageDecoder<Envelope>
    {
        final ProtocolVersion version;
        final ResponseHandler responseHandler;
        final int largeMessageThreshold;
        InitialHandler(ProtocolVersion version, ResponseHandler responseHandler, int largeMessageThreshold)
        {
            this.version = version;
            this.responseHandler = responseHandler;
            this.largeMessageThreshold = largeMessageThreshold;
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, Envelope request, List<Object> results)
        {
            switch(request.header.type)
            {
                case READY:
                case AUTHENTICATE:
                    if (request.header.version.isGreaterOrEqualTo(ProtocolVersion.V5))
                    {
                        configureModernPipeline(ctx, request, largeMessageThreshold);
                        // consuming the message is done when setting up the pipeline
                    }
                    else
                    {
                        configureLegacyPipeline(ctx);
                        // really just removes self from the pipeline, so pass this message on
                        ctx.pipeline().context(Envelope.Decoder.class).fireChannelRead(request);
                    }
                    break;
                case SUPPORTED:
                case ERROR:
                    // just pass through
                    results.add(request);
                    break;
                default:
                    throw new ProtocolException(String.format("Unexpected %s request expecting " +
                                                              "READY, AUTHENTICATE, ERROR or SUPPORTED",
                                                              request.header.type));
            }
        }

        private void configureModernPipeline(ChannelHandlerContext ctx, Envelope request, int largeMessageThreshold)
        {
            logger.info("Configuring modern pipeline");
            ChannelPipeline pipeline = ctx.pipeline();
            pipeline.remove(HandlerNames.ENVELOPE_DECODER);
            pipeline.remove(HandlerNames.MESSAGE_DECODER);
            pipeline.remove(HandlerNames.MESSAGE_ENCODER);
            pipeline.remove(HandlerNames.RESPONSE_HANDLER);

            BufferPoolAllocator allocator = GlobalBufferPoolAllocator.instance;
            Channel channel = ctx.channel();
            channel.config().setOption(ChannelOption.ALLOCATOR, allocator);
            int queueCapacity = 1 << 20;  // 1MiB

            Envelope.Decoder envelopeDecoder = new Envelope.Decoder();
            Message.Decoder<Message.Response> messageDecoder = Message.responseDecoder();
            FrameDecoder frameDecoder = frameDecoder(ctx, allocator);
            FrameEncoder frameEncoder = frameEncoder(ctx);
            FrameEncoder.PayloadAllocator payloadAllocator = frameEncoder.allocator();

            CQLMessageHandler.MessageConsumer<Message.Response> responseConsumer = new CQLMessageHandler.MessageConsumer<Message.Response>()
            {
                public <P> void dispatch(Channel channel, Message.Response message, Dispatcher.FlushItemConverter<P> toFlushItem, P param, Overload backpressure)
                {
                    responseHandler.handleResponse(channel, message);
                }

                public boolean hasQueueCapacity()
                {
                    return true;
                }
            };

            CQLMessageHandler.ErrorHandler errorHandler = (error) -> {
                throw new RuntimeException("Unexpected error", error);
            };

            ClientResourceLimits.ResourceProvider resources = new ClientResourceLimits.ResourceProvider()
            {
                final ResourceLimits.Limit endpointReserve = new ResourceLimits.Basic(1024 * 1024 * 64);
                final AbstractMessageHandler.WaitQueue endpointQueue = AbstractMessageHandler.WaitQueue.endpoint(endpointReserve);

                final ResourceLimits.Limit globalReserve = new ResourceLimits.Basic(1024 * 1024 * 64);
                final AbstractMessageHandler.WaitQueue globalQueue = AbstractMessageHandler.WaitQueue.global(endpointReserve);

                public ResourceLimits.Limit globalLimit()
                {
                    return globalReserve;
                }

                public AbstractMessageHandler.WaitQueue globalWaitQueue()
                {
                    return globalQueue;
                }

                public ResourceLimits.Limit endpointLimit()
                {
                    return endpointReserve;
                }

                public AbstractMessageHandler.WaitQueue endpointWaitQueue()
                {
                    return endpointQueue;
                }

                @Override
                public NonBlockingRateLimiter requestRateLimiter()
                {
                    return NO_OP_LIMITER;
                }

                public void release()
                {
                }
            };

            CQLMessageHandler<Message.Response> processor =
                new CQLMessageHandler<Message.Response>(ctx.channel(),
                                        null,
                                        version,
                                        frameDecoder,
                                        envelopeDecoder,
                                        messageDecoder,
                                        responseConsumer,
                                        payloadAllocator,
                                        queueCapacity,
                                        QueueBackpressure.NO_OP,
                                        resources,
                                        handler -> {},
                                        errorHandler,
                                        ctx.channel().attr(Connection.attributeKey).get().isThrowOnOverload())
                {
                    protected boolean processRequest(Envelope request, Overload overload)
                    {
                        boolean continueProcessing = super.processRequest(request, overload);
                        releaseCapacity(Ints.checkedCast(request.header.bodySizeInBytes));
                        return continueProcessing;
                    }
                };

            pipeline.addLast(HandlerNames.FRAME_DECODER, frameDecoder);
            pipeline.addLast(HandlerNames.FRAME_ENCODER, frameEncoder);
            pipeline.addLast(HandlerNames.PROCESSOR, processor);
            pipeline.addLast(HandlerNames.MESSAGE_ENCODER, new ChannelOutboundHandlerAdapter() {

                public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
                {
                    if (!(msg instanceof List))
                    {
                        ctx.write(msg, promise);
                        return;
                    }
                    Connection connection = ctx.channel().attr(Connection.attributeKey).get();
                    // The only case the connection can be null is when we send the initial STARTUP message (client side thus)
                    ProtocolVersion version = connection == null ? ProtocolVersion.CURRENT : connection.getVersion();
                    SimpleFlusher flusher = new SimpleFlusher(frameEncoder, largeMessageThreshold);
                    for (Message message : (List<Message>) msg)
                        flusher.enqueue(message.encode(version, outboundStreamId(message)));

                    flusher.maybeWrite(ctx, promise);
                }
            });
            pipeline.remove(this);

            Message.Response message = messageDecoder.decode(ctx.channel(), request);
            responseConsumer.dispatch(channel, message, (p, ch, req, resp) -> null, null, Overload.NONE);
        }

        private FrameDecoder frameDecoder(ChannelHandlerContext ctx, BufferPoolAllocator allocator)
        {
            Connection conn = ctx.channel().attr(Connection.attributeKey).get();
            if (conn.getCompressor() == null)
                return FrameDecoderCrc.create(allocator);
            if (conn.getCompressor() instanceof Compressor.LZ4Compressor)
                return FrameDecoderLZ4.fast(allocator);
            throw new ProtocolException("Unsupported compressor: " + conn.getCompressor().getClass().getCanonicalName());
        }

        private FrameEncoder frameEncoder(ChannelHandlerContext ctx)
        {
            Connection conn = ctx.channel().attr(Connection.attributeKey).get();
            if (conn.getCompressor() == null)
                return FrameEncoderCrc.instance;
            if (conn.getCompressor() instanceof Compressor.LZ4Compressor)
                return FrameEncoderLZ4.fastInstance;
            throw new ProtocolException("Unsupported compressor: " + conn.getCompressor().getClass().getCanonicalName());
        }

        private void configureLegacyPipeline(ChannelHandlerContext ctx)
        {
            logger.info("Configuring legacy pipeline");
            ChannelPipeline pipeline = ctx.pipeline();
            pipeline.remove(this);
            pipeline.addAfter(HandlerNames.ENVELOPE_ENCODER, HandlerNames.DECOMPRESSOR, Envelope.Decompressor.instance);
            pipeline.addAfter(HandlerNames.DECOMPRESSOR, HandlerNames.COMPRESSOR, Envelope.Compressor.instance);
        }
    }

    @ChannelHandler.Sharable
     static class MessageBatchEncoder extends MessageToMessageEncoder<List<Message>>
    {
        public static final MessageBatchEncoder instance = new MessageBatchEncoder();
        private MessageBatchEncoder(){}

        public void encode(ChannelHandlerContext ctx, List<Message> messages, List<Object> results)
        {
            Connection connection = ctx.channel().attr(Connection.attributeKey).get();
            // The only case the connection can be null is when we send the initial STARTUP message (client side thus)
            ProtocolVersion version = connection == null ? ProtocolVersion.CURRENT : connection.getVersion();
            assert messages.size() == 1;
            Message message = messages.get(0);
            results.add(message.encode(version, outboundStreamId(message)));
        }
    }

    private class Initializer extends ChannelInitializer<Channel>
    {
        private int largeMessageThreshold;
        Initializer(int largeMessageThreshold)
        {
            this.largeMessageThreshold = largeMessageThreshold;
        }

        protected void initChannel(Channel channel) throws Exception
        {
            connection = new Connection(channel, version, tracker);
            channel.attr(Connection.attributeKey).set(connection);

            ChannelPipeline pipeline = channel.pipeline();
//            pipeline.addLast("debug", new LoggingHandler(LogLevel.INFO));
            pipeline.addLast(HandlerNames.ENVELOPE_DECODER, new Envelope.Decoder());
            pipeline.addLast(HandlerNames.ENVELOPE_ENCODER, Envelope.Encoder.instance);
            pipeline.addLast(HandlerNames.INITIAL_HANDLER, new InitialHandler(version, responseHandler, largeMessageThreshold));
            pipeline.addLast(HandlerNames.MESSAGE_DECODER, PreV5Handlers.ProtocolDecoder.instance);
            pipeline.addLast(HandlerNames.MESSAGE_ENCODER, MessageBatchEncoder.instance);
            pipeline.addLast(HandlerNames.RESPONSE_HANDLER, responseHandler);
        }
    }

    private class SecureInitializer extends Initializer
    {
        SecureInitializer(int largeMessageThreshold)
        {
            super(largeMessageThreshold);
        }

        protected void initChannel(Channel channel) throws Exception
        {
            super.initChannel(channel);
            SslContext sslContext = SSLFactory.getOrCreateSslContext(encryptionOptions, encryptionOptions.getClientAuth(),
                                                                     ISslContextFactory.SocketType.CLIENT, SSL_FACTORY_CONTEXT_DESCRIPTION);
            InetSocketAddress peer = encryptionOptions.require_endpoint_verification ? new InetSocketAddress(host, port) : null;
            channel.pipeline().addFirst("ssl", newSslHandler(channel, sslContext, peer));
        }
    }

    @ChannelHandler.Sharable
    static class ResponseHandler extends SimpleChannelInboundHandler<Message.Response>
    {
        public final BlockingQueue<Message.Response> responses = new SynchronousQueue<>(true);
        private final SimpleClient client;
        public EventHandler eventHandler;
        public ResponseHandler(SimpleClient client)
        {
            this.client = client;
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, Message.Response r)
        {
            handleResponse(ctx.channel(), r);
        }

        public void handleResponse(Channel channel, Message.Response r)
        {
            try
            {
                Envelope cloned = r.getSource().clone();
                r.getSource().release();
                r.setSource(cloned);

                if (r instanceof EventMessage)
                {
                    Event event = ((EventMessage)r).event;

                    if (event.type == Event.Type.GRACEFUL_DISCONNECT)
                    {
                        logger.info("Received GRACEFUL_DISCONNECT. Entering draining mode.");
                        if (eventHandler != null)
                            eventHandler.onEvent(event);
                        client.handleGracefulDisconnect();
                        return;
                    }

                    if (eventHandler != null)
                        eventHandler.onEvent(event);
                }
                else
                    responses.put(r);
            }
            catch (InterruptedException e)
            {
                throw new UncheckedInterruptedException(e);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
        {
            if (this == ctx.pipeline().last())
            {
                logger.error("Exception in response", cause);
            }
            else
            {
                ctx.fireExceptionCaught(cause);
            }
        }
    }

    // Simple stand-in for Flusher for use in test code. Writers push CQL messages onto a queue and
    // this collates them into frames and flushes them to the channel.
    // Can be either scheduled to run on an EventExecutor or fired manually. If calling maybeWrite manually,
    // as SimpleClient itself does, the call must be made on the event loop.
    public static class SimpleFlusher
    {
        private static final ChannelFuture[] EMPTY_FUTURES_ARRAY = new ChannelFuture[0];
        final Queue<Envelope> outbound = new ConcurrentLinkedQueue<>();
        final FrameEncoder frameEncoder;
        private final AtomicBoolean scheduled = new AtomicBoolean(false);
        private final int largeMessageThreshold;

        SimpleFlusher(FrameEncoder frameEncoder, int largeMessageThreshold)
        {
            this.frameEncoder = frameEncoder;
            this.largeMessageThreshold = largeMessageThreshold;
        }

        SimpleFlusher(FrameEncoder frameEncoder)
        {
            this(frameEncoder, MAX_FRAMED_PAYLOAD_SIZE);
        }

        public void enqueue(Envelope message)
        {
            outbound.offer(message);
        }

        public void releaseAll()
        {
            Envelope e;
            while ((e = outbound.poll()) != null)
                e.release();
        }

        public void schedule(ChannelHandlerContext ctx)
        {
            if (scheduled.compareAndSet(false, true))
                ctx.executor().scheduleAtFixedRate(() -> maybeWrite(ctx, ctx.voidPromise()),
                                                   10, 10, TimeUnit.MILLISECONDS);
        }

        public void maybeWrite(ChannelHandlerContext ctx, Promise<Void> promise)
        {
            if (outbound.isEmpty())
            {
                promise.setSuccess(null);
                return;
            }

            PromiseCombiner combiner = new PromiseCombiner(ctx.executor());
            List<Envelope> buffer = new ArrayList<>();
            long bufferSize = 0L;
            boolean pending = false;
            Envelope f;
            while ((f = outbound.poll()) != null)
            {
                if (f.header.bodySizeInBytes > largeMessageThreshold)
                {
                    combiner.addAll(writeLargeMessage(ctx, f));
                }
                else
                {
                    int messageSize = envelopeSize(f.header);
                    if (bufferSize + messageSize >= largeMessageThreshold)
                    {
                        logger.trace("Sending frame of size: {}", bufferSize);
                        combiner.add(flushBuffer(ctx, buffer, bufferSize));
                        buffer = new ArrayList<>();
                        bufferSize = 0;
                    }
                    buffer.add(f);
                    bufferSize += messageSize;
                    pending = true;
                }
            }

            if (pending)
            {
                logger.trace("Sending frame of size: {}", bufferSize);
                combiner.add(flushBuffer(ctx, buffer, bufferSize));
            }
            combiner.finish(promise);
        }

        private ChannelFuture flushBuffer(ChannelHandlerContext ctx, List<Envelope> messages, long bufferSize)
        {
            FrameEncoder.Payload payload = allocate(Ints.checkedCast(bufferSize), true);

            for (Envelope e : messages)
                e.encodeInto(payload.buffer);

            payload.finish();
            ChannelPromise release = AsyncChannelPromise.withListener(ctx, future -> {
                logger.trace("Sent frame of size: {}", bufferSize);
                for (Envelope e : messages)
                    e.release();
            });
            return ctx.writeAndFlush(payload, release);
        }

        private FrameEncoder.Payload allocate(int size, boolean selfContained)
        {
            FrameEncoder.Payload payload = frameEncoder.allocator()
                                                       .allocate(selfContained, Math.min(size, largeMessageThreshold));
            if (size >= largeMessageThreshold)
                payload.buffer.limit(largeMessageThreshold);

            return payload;
        }

        private ChannelFuture[] writeLargeMessage(ChannelHandlerContext ctx, Envelope f)
        {
            List<ChannelFuture> futures = new ArrayList<>();
            FrameEncoder.Payload payload;
            ByteBuffer buf;
            boolean firstFrame = true;
            while (f.body.readableBytes() > 0 || firstFrame)
            {
                int payloadSize = Math.min(f.body.readableBytes(), largeMessageThreshold);
                payload = allocate(f.body.readableBytes(), false);

                buf = payload.buffer;
                // BufferPool may give us a buffer larger than we asked for.
                // FrameEncoder may object if buffer.remaining is >= MAX_SIZE.
                if (payloadSize >= largeMessageThreshold)
                    buf.limit(largeMessageThreshold);

                if (firstFrame)
                {
                    f.encodeHeaderInto(buf);
                    firstFrame = false;
                }

                int remaining = Math.min(buf.remaining(), f.body.readableBytes());
                if (remaining > 0)
                    buf.put(f.body.slice(f.body.readerIndex(), remaining).nioBuffer());

                f.body.readerIndex(f.body.readerIndex() + remaining);
                payload.finish();
                ChannelPromise promise = ctx.newPromise();
                logger.trace("Sending frame of large message: {}", remaining);
                futures.add(ctx.writeAndFlush(payload, promise));
                promise.addListener(result -> {
                    if (!result.isSuccess())
                        logger.warn("Failed to send frame of large message, size: " + remaining, result.cause());
                    else
                        logger.trace("Sent frame of large message, size: {}", remaining);
                });
            }
            f.release();
            return futures.toArray(EMPTY_FUTURES_ARRAY);
        }
    }
}