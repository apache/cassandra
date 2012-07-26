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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.*;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;

import org.apache.cassandra.transport.messages.*;
import org.apache.cassandra.service.ClientState;

public class SimpleClient
{
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    public final String host;
    public final int port;

    protected final ResponseHandler responseHandler = new ResponseHandler();
    protected final ClientConnection connection = new ClientConnection();
    protected final Connection.Tracker tracker = new ConnectionTracker();
    protected ClientBootstrap bootstrap;
    protected Channel channel;
    protected ChannelFuture lastWriteFuture;

    private final Connection.Factory connectionFactory = new Connection.Factory()
    {
        public Connection newConnection()
        {
            return connection;
        }
    };

    public SimpleClient(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    public void connect(boolean useCompression) throws IOException
    {
        establishConnection();

        EnumMap<StartupMessage.Option, Object> options = new EnumMap<StartupMessage.Option, Object>(StartupMessage.Option.class);
        if (useCompression)
        {
            options.put(StartupMessage.Option.COMPRESSION, "snappy");
            connection.setCompressor(FrameCompressor.SnappyCompressor.instance);
        }
        execute(new StartupMessage("3.0.0", options));
    }

    protected void establishConnection() throws IOException
    {
        // Configure the client.
        bootstrap = new ClientBootstrap(
                        new NioClientSocketChannelFactory(
                            Executors.newCachedThreadPool(),
                            Executors.newCachedThreadPool()));

        // Configure the pipeline factory.
        bootstrap.setPipelineFactory(new PipelineFactory());
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));

        // Wait until the connection attempt succeeds or fails.
        channel = future.awaitUninterruptibly().getChannel();
        if (!future.isSuccess())
        {
            bootstrap.releaseExternalResources();
            throw new IOException("Connection Error", future.getCause());
        }
    }

    public void login(Map<String, String> credentials)
    {
        CredentialsMessage msg = new CredentialsMessage();
        msg.credentials.putAll(credentials);
        execute(msg);
    }

    public ResultMessage execute(String query)
    {
        Message.Response msg = execute(new QueryMessage(query));
        assert msg instanceof ResultMessage;
        return (ResultMessage)msg;
    }

    public ResultMessage.Prepared prepare(String query)
    {
        Message.Response msg = execute(new PrepareMessage(query));
        assert msg instanceof ResultMessage.Prepared;
        return (ResultMessage.Prepared)msg;
    }

    public ResultMessage executePrepared(int statementId, List<ByteBuffer> values)
    {
        Message.Response msg = execute(new ExecuteMessage(statementId, values));
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
        bootstrap.releaseExternalResources();
    }

    protected Message.Response execute(Message.Request request)
    {
        try
        {
            request.attach(connection);
            lastWriteFuture = channel.write(request);
            Message.Response msg = responseHandler.responses.take();
            if (msg instanceof ErrorMessage)
                throw new RuntimeException(((ErrorMessage)msg).errorMsg);
            return msg;
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected static class ClientConnection extends Connection
    {
        public ClientState clientState()
        {
            return null;
        }

        public void validateNewMessage(Message.Type type) {}

        public void applyStateTransition(Message.Type requestType, Message.Type responseType) {}
    }

    // Stateless handlers
    private static final Message.ProtocolDecoder messageDecoder = new Message.ProtocolDecoder();
    private static final Message.ProtocolEncoder messageEncoder = new Message.ProtocolEncoder();
    private static final Frame.Decompressor frameDecompressor = new Frame.Decompressor();
    private static final Frame.Compressor frameCompressor = new Frame.Compressor();
    private static final Frame.Encoder frameEncoder = new Frame.Encoder();

    private static class ConnectionTracker implements Connection.Tracker
    {
        public void addConnection(Channel ch, Connection connection) {}
        public void closeAll() {}
    }

    private class PipelineFactory implements ChannelPipelineFactory
    {
        public ChannelPipeline getPipeline() throws Exception
        {
            ChannelPipeline pipeline = Channels.pipeline();

            //pipeline.addLast("debug", new LoggingHandler());

            pipeline.addLast("frameDecoder", new Frame.Decoder(tracker, connectionFactory));
            pipeline.addLast("frameEncoder", frameEncoder);

            pipeline.addLast("frameDecompressor", frameDecompressor);
            pipeline.addLast("frameCompressor", frameCompressor);

            pipeline.addLast("messageDecoder", messageDecoder);
            pipeline.addLast("messageEncoder", messageEncoder);

            pipeline.addLast("handler", responseHandler);

            return pipeline;
        }
    }

    private static class ResponseHandler extends SimpleChannelUpstreamHandler
    {
        public final BlockingQueue<Message.Response> responses = new SynchronousQueue<Message.Response>(true);

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        {
            assert e.getMessage() instanceof Message.Response;
            try
            {
                responses.put((Message.Response)e.getMessage());
            }
            catch (InterruptedException ie)
            {
                throw new RuntimeException(ie);
            }
        }
    }
}
