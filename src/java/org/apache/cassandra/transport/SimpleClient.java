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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.net.AbstractMessageHandler;
import org.apache.cassandra.net.BufferPoolAllocator;
import org.apache.cassandra.net.FrameDecoder;
import org.apache.cassandra.net.FrameDecoderCrc;
import org.apache.cassandra.net.FrameDecoderLZ4;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.net.FrameEncoderCrc;
import org.apache.cassandra.net.FrameEncoderLZ4;
import org.apache.cassandra.net.GlobalBufferPoolAllocator;
import org.apache.cassandra.net.ResourceLimits;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import org.apache.cassandra.utils.FBUtilities;

public class SimpleClient implements Closeable
{
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    private static final Logger logger = LoggerFactory.getLogger(SimpleClient.class);

    public final String host;
    public final int port;
    private final EncryptionOptions encryptionOptions;
    private final int largeMessageThreshold;

    protected final ResponseHandler responseHandler = new ResponseHandler();
    protected final Connection.Tracker tracker = new ConnectionTracker();
    protected final ProtocolVersion version;
    // We don't track connection really, so we don't need one Connection per channel
    protected Connection connection;
    protected Bootstrap bootstrap;
    protected Channel channel;
    protected ChannelFuture lastWriteFuture;

    protected String compression;

    private final Connection.Factory connectionFactory = new Connection.Factory()
    {
        public Connection newConnection(Channel channel, ProtocolVersion version)
        {
            return connection;
        }
    };

    public static class Builder
    {
        private final String host;
        private final int port;
        private EncryptionOptions encryptionOptions = new EncryptionOptions();
        private ProtocolVersion version = ProtocolVersion.CURRENT;
        private boolean useBeta = false;
        private int largeMessageThreshold = FrameEncoder.Payload.MAX_SIZE;

        private Builder(String host, int port)
        {
            this.host = host;
            this.port = port;
        }

        public Builder encryption(EncryptionOptions options)
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
        this.encryptionOptions = builder.encryptionOptions;
        this.largeMessageThreshold = builder.largeMessageThreshold;
    }

    public SimpleClient(String host, int port, ProtocolVersion version, EncryptionOptions encryptionOptions)
    {
        this(host, port, version, false, encryptionOptions);
    }

    public SimpleClient(String host, int port, EncryptionOptions encryptionOptions)
    {
        this(host, port, ProtocolVersion.CURRENT, encryptionOptions);
    }

    public SimpleClient(String host, int port, ProtocolVersion version)
    {
        this(host, port, version, new EncryptionOptions());
    }

    public SimpleClient(String host, int port, ProtocolVersion version, boolean useBeta, EncryptionOptions encryptionOptions)
    {
        this.host = host;
        this.port = port;
        if (version.isBeta() && !useBeta)
            throw new IllegalArgumentException(String.format("Beta version of server used (%s), but USE_BETA flag is not set", version));

        this.version = version;
        this.encryptionOptions = encryptionOptions;
        this.largeMessageThreshold = FrameEncoder.Payload.MAX_SIZE -
                                        Math.max(FrameEncoderCrc.HEADER_AND_TRAILER_LENGTH,
                                                 FrameEncoderLZ4.HEADER_AND_TRAILER_LENGTH);
    }

    public SimpleClient(String host, int port)
    {
        this(host, port, new EncryptionOptions());
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
            options.put(StartupMessage.COMPRESSION, "snappy");
            connection.setCompressor(FrameCompressor.SnappyCompressor.instance);
        }
        execute(new StartupMessage(options));

        return this;
    }

    public void setEventHandler(EventHandler eventHandler)
    {
        responseHandler.eventHandler = eventHandler;
    }

    protected void establishConnection() throws IOException
    {
        // Configure the client.
        bootstrap = new Bootstrap()
                    .group(new NioEventLoopGroup())
                    .channel(io.netty.channel.socket.nio.NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true);

        // Configure the pipeline factory.
        if(encryptionOptions.enabled)
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
        try
        {
            request.attach(connection);
            lastWriteFuture = channel.writeAndFlush(request);
            Message.Response msg = responseHandler.responses.take();
            if (msg instanceof ErrorMessage)
                throw new RuntimeException((Throwable)((ErrorMessage)msg).error);
            return msg;
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public interface EventHandler
    {
        void onEvent(Event event);
    }

    public static class SimpleEventHandler implements EventHandler
    {
        public final LinkedBlockingQueue<Event> queue = new LinkedBlockingQueue<>();

        public void onEvent(Event event)
        {
            queue.add(event);
        }
    }

    // Stateless handlers
    private static final Message.ProtocolDecoder messageDecoder = Message.ProtocolDecoder.instance;
    private static final Message.ProtocolEncoder messageEncoder = Message.ProtocolEncoder.instance;

    private static class ConnectionTracker implements Connection.Tracker
    {
        public void addConnection(Channel ch, Connection connection) {}

        public boolean isRegistered(Event.Type type, Channel ch)
        {
            return false;
        }
    }

    private static class InitialHandler extends MessageToMessageDecoder<Frame>
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

        protected void decode(ChannelHandlerContext ctx, Frame frame, List<Object> results) throws Exception
        {
            switch(frame.header.type)
            {
                case READY:
                case AUTHENTICATE:
                    if (frame.header.version.isGreaterOrEqualTo(ProtocolVersion.V5))
                    {
                        configureModernPipeline(ctx, frame);
                        // consuming the message is done when setting up the pipeline
                    }
                    else
                    {
                        configureLegacyPipeline(ctx);
                        // really just removes self from the pipeline, so pass this message on
                        ctx.pipeline().context(Frame.Decoder.class).fireChannelRead(frame);
                    }
                    break;
                case SUPPORTED:
                    // just pass through
                    results.add(frame);
                    break;
                default:
                    throw new ProtocolException(String.format("Unexpected %s response expecting " +
                                                              "READY, AUTHENTICATE or SUPPORTED",
                                                              frame.header.type));
            }
        }

        private void configureModernPipeline(ChannelHandlerContext ctx, Frame frame)
        {
            Message.ProtocolEncoder messageEncoder = Message.ProtocolEncoder.instance;
            Message.ProtocolDecoder messageDecoder = Message.ProtocolDecoder.instance;
            Message response = messageDecoder.decodeMessage(ctx.channel(), frame);
            logger.info("Configuring modern pipeline");
            ChannelPipeline pipeline = ctx.pipeline();
            pipeline.remove("frameDecoder");
            pipeline.remove("messageDecoder");
            pipeline.remove("messageEncoder");
            pipeline.remove("responseHandler");

            BufferPoolAllocator allocator = GlobalBufferPoolAllocator.instance;
            Channel channel = ctx.channel();
            channel.config().setOption(ChannelOption.ALLOCATOR, allocator);
            int queueCapacity = 1 << 20;  // 1MiB
            ResourceLimits.Limit endpointReserve = new ResourceLimits.Basic(1024 * 1024 * 64);
            ResourceLimits.Limit globalReserve = new ResourceLimits.Basic(1024 * 1024 * 64);

            Frame.Decoder cqlFrameDecoder = new Frame.Decoder();
            FrameDecoder messageFrameDecoder = frameDecoder(ctx, allocator);
            FrameEncoder messageFrameEncoder = frameEncoder(ctx);
            FrameEncoder.PayloadAllocator payloadAllocator = messageFrameEncoder.allocator();

            CQLMessageHandler.MessageConsumer messageConsumer = (c, message , converter) -> {
                responseHandler.handleResponse(c, (Message.Response)message);
            };

            CQLMessageHandler.ErrorHandler errorHandler = (error) -> {
                throw new RuntimeException("Unexpected error", error);
            };

            CQLMessageHandler processor = new CQLMessageHandler(ctx.channel(),
                                                                messageFrameDecoder,
                                                                cqlFrameDecoder,
                                                                messageDecoder,
                                                                messageEncoder,
                                                                messageConsumer,
                                                                payloadAllocator,
                                                                queueCapacity,
                                                                endpointReserve,
                                                                globalReserve,
                                                                AbstractMessageHandler.WaitQueue.endpoint(endpointReserve),
                                                                AbstractMessageHandler.WaitQueue.global(globalReserve),
                                                                handler -> {},
                                                                errorHandler,
                                                                ctx.channel().attr(Connection.attributeKey).get().isThrowOnOverload());

            pipeline.addLast("messageFrameDecoder", messageFrameDecoder);
            pipeline.addLast("messageFrameEncoder", messageFrameEncoder);
            pipeline.addLast("processor", processor);
            pipeline.addLast("payloadEncoder", new PayloadEncoder(messageFrameEncoder.allocator(), largeMessageThreshold));
            pipeline.addLast("cqlMessageEncoder", Message.ProtocolEncoder.instance);
            pipeline.remove(this);
            messageConsumer.accept(channel, response, (ch, req, resp) -> null);
        }

        private FrameDecoder frameDecoder(ChannelHandlerContext ctx, BufferPoolAllocator allocator)
        {
            Connection conn = ctx.channel().attr(Connection.attributeKey).get();
            if (conn.getCompressor() == null)
                return FrameDecoderCrc.create(allocator);
            if (conn.getCompressor() instanceof FrameCompressor.LZ4Compressor)
                return FrameDecoderLZ4.fast(allocator);
            throw new ProtocolException("Unsupported compressor: " + conn.getCompressor().getClass().getCanonicalName());
        }

        private FrameEncoder frameEncoder(ChannelHandlerContext ctx)
        {
            Connection conn = ctx.channel().attr(Connection.attributeKey).get();
            if (conn.getCompressor() == null)
                return FrameEncoderCrc.instance;
            if (conn.getCompressor() instanceof FrameCompressor.LZ4Compressor)
                return FrameEncoderLZ4.fastInstance;
            throw new ProtocolException("Unsupported compressor: " + conn.getCompressor().getClass().getCanonicalName());
        }

        private void configureLegacyPipeline(ChannelHandlerContext ctx)
        {
            logger.info("Configuring legacy pipeline");
            ChannelPipeline pipeline = ctx.pipeline();
            pipeline.remove(this);
            pipeline.addAfter("frameEncoder", "frameDecompressor", Frame.Decompressor.instance);
            pipeline.addAfter("frameDecompressor", "frameCompressor", Frame.Compressor.instance);
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
            pipeline.addLast("frameDecoder", new Frame.Decoder());
            pipeline.addLast("frameEncoder", Frame.Encoder.instance);
            pipeline.addLast("initial", new InitialHandler(version, responseHandler, largeMessageThreshold));
            pipeline.addLast("messageDecoder", messageDecoder);
            pipeline.addLast("messageEncoder", messageEncoder);
            pipeline.addLast("responseHandler",  responseHandler);
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
            SslContext sslContext = SSLFactory.getOrCreateSslContext(encryptionOptions, encryptionOptions.require_client_auth,
                                                                     SSLFactory.SocketType.CLIENT);
            channel.pipeline().addFirst("ssl", sslContext.newHandler(channel.alloc()));
        }
    }

    private static class PayloadEncoder extends ChannelOutboundHandlerAdapter
    {
        private final FrameEncoder.PayloadAllocator allocator;
        private final int largeMessageThreshold;
        PayloadEncoder(FrameEncoder.PayloadAllocator allocator, int largeMessageThreshold)
        {
            this.allocator = allocator;
            this.largeMessageThreshold = largeMessageThreshold;
        }

        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
        {
            Frame outbound = (Frame)msg;
            if (CQLMessageHandler.frameSize(outbound.header) >= largeMessageThreshold)
            {
                FrameEncoder.Payload payload;
                ByteBuffer buf;
                ByteBuf body = outbound.body;
                boolean firstFrame = true;
                // Highly unlikely that the frame body of a large message would be empty, but the check is cheap
                while (body.readableBytes() > 0 || firstFrame)
                {
                    int payloadSize = Math.min(body.readableBytes(), largeMessageThreshold);
                    payload = allocator.allocate(false, payloadSize);
                    if (logger.isTraceEnabled())
                    {
                        logger.trace("Allocated initial buffer of {} for 1 large item",
                                     FBUtilities.prettyPrintMemory(payload.buffer.capacity()));
                    }

                    buf = payload.buffer;
                    // BufferPool may give us a buffer larger than we asked for.
                    // FrameEncoder may object if buffer.remaining is >= MAX_SIZE.
                    if (payloadSize >= largeMessageThreshold)
                        buf.limit(largeMessageThreshold);

                    if (firstFrame)
                    {
                        outbound.encodeHeaderInto(buf);
                        firstFrame = false;
                    }

                    int remaining = Math.min(buf.remaining(), body.readableBytes());
                    if (remaining > 0)
                        buf.put(body.slice(body.readerIndex(), remaining).nioBuffer());

                    body.readerIndex(body.readerIndex() + remaining);
                    payload.finish();
                    ctx.writeAndFlush(payload,
                                      body.readableBytes() == 0 ? promise : ctx.voidPromise());
                    payload.release();
                }
            }
            else
            {
                FrameEncoder.Payload sending = allocator.allocate(true, CQLMessageHandler.frameSize(outbound.header));
                outbound.encodeInto(sending.buffer);
                sending.finish();
                ctx.writeAndFlush(sending, promise);
                sending.release();
            }
        }
    }

    @ChannelHandler.Sharable
    private static class ResponseHandler extends SimpleChannelInboundHandler<Message.Response>
    {
        public final BlockingQueue<Message.Response> responses = new SynchronousQueue<>(true);
        public EventHandler eventHandler;

        @Override
        public void channelRead0(ChannelHandlerContext ctx, Message.Response r)
        {
            handleResponse(ctx.channel(), r);
        }

        public void handleResponse(Channel channel, Message.Response r)
        {
            try
            {
                if (r instanceof EventMessage)
                {
                    if (eventHandler != null)
                        eventHandler.onEvent(((EventMessage) r).event);
                }
                else
                    responses.put(r);
            }
            catch (InterruptedException ie)
            {
                throw new RuntimeException(ie);
            }
        }

        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
        {
            if (this == ctx.pipeline().last())
                logger.error("Exception in response", cause);
            ctx.fireExceptionCaught(cause);
        }
    }
}
