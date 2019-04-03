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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.transport.frame.checksum.ChecksummingTransformer;
import org.apache.cassandra.transport.frame.compress.CompressingTransformer;
import org.apache.cassandra.transport.frame.compress.Compressor;
import org.apache.cassandra.transport.frame.compress.LZ4Compressor;
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
import org.apache.cassandra.utils.ChecksumType;

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

    protected final ResponseHandler responseHandler = new ResponseHandler();
    protected final Connection.Tracker tracker = new ConnectionTracker();
    protected final ProtocolVersion version;
    // We don't track connection really, so we don't need one Connection per channel
    protected Connection connection;
    protected Bootstrap bootstrap;
    protected Channel channel;
    protected ChannelFuture lastWriteFuture;

    private final Connection.Factory connectionFactory = new Connection.Factory()
    {
        public Connection newConnection(Channel channel, ProtocolVersion version)
        {
            return connection;
        }
    };

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
    }

    public SimpleClient(String host, int port)
    {
        this(host, port, new EncryptionOptions());
    }

    public SimpleClient connect(boolean useCompression, boolean useChecksums) throws IOException
    {
        establishConnection();

        Map<String, String> options = new HashMap<>();
        options.put(StartupMessage.CQL_VERSION, "3.0.0");

        if (useChecksums)
        {
            Compressor compressor = useCompression ? LZ4Compressor.INSTANCE : null;
            connection.setTransformer(ChecksummingTransformer.getTransformer(ChecksumType.CRC32, compressor));
            options.put(StartupMessage.CHECKSUM, "crc32");
            options.put(StartupMessage.COMPRESSION, "lz4");
        }
        else if (useCompression)
        {
            connection.setTransformer(CompressingTransformer.getTransformer(LZ4Compressor.INSTANCE));
            options.put(StartupMessage.COMPRESSION, "lz4");
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
            bootstrap.handler(new SecureInitializer());
        }
        else
        {
            bootstrap.handler(new Initializer());
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
    private static final Message.ProtocolDecoder messageDecoder = new Message.ProtocolDecoder();
    private static final Message.ProtocolEncoder messageEncoder = new Message.ProtocolEncoder();
    private static final Frame.InboundBodyTransformer inboundFrameTransformer = new Frame.InboundBodyTransformer();
    private static final Frame.OutboundBodyTransformer outboundFrameTransformer = new Frame.OutboundBodyTransformer();
    private static final Frame.Encoder frameEncoder = new Frame.Encoder();

    private static class ConnectionTracker implements Connection.Tracker
    {
        public void addConnection(Channel ch, Connection connection) {}

        public boolean isRegistered(Event.Type type, Channel ch)
        {
            return false;
        }
    }

    private class Initializer extends ChannelInitializer<Channel>
    {
        protected void initChannel(Channel channel) throws Exception
        {
            connection = new Connection(channel, version, tracker);
            channel.attr(Connection.attributeKey).set(connection);

            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast("frameDecoder", new Frame.Decoder(connectionFactory));
            pipeline.addLast("frameEncoder", frameEncoder);

            pipeline.addLast("inboundFrameTransformer", inboundFrameTransformer);
            pipeline.addLast("outboundFrameTransformer", outboundFrameTransformer);

            pipeline.addLast("messageDecoder", messageDecoder);
            pipeline.addLast("messageEncoder", messageEncoder);

            pipeline.addLast("handler", responseHandler);
        }
    }

    private class SecureInitializer extends Initializer
    {
        protected void initChannel(Channel channel) throws Exception
        {
            super.initChannel(channel);
            SslContext sslContext = SSLFactory.getOrCreateSslContext(encryptionOptions, encryptionOptions.require_client_auth,
                                                                     SSLFactory.SocketType.CLIENT);
            channel.pipeline().addFirst("ssl", sslContext.newHandler(channel.alloc()));
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
