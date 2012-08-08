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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.EnumMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.*;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.transport.messages.EventMessage;

public class Server implements CassandraDaemon.Server
{
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final ConnectionTracker connectionTracker = new ConnectionTracker();

    public final InetSocketAddress socket;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private ChannelFactory factory;
    private ExecutionHandler executionHandler;

    public Server(InetSocketAddress socket)
    {
        this.socket = socket;
        Gossiper.instance.register(new EventNotifier(this));
    }

    public Server(String hostname, int port)
    {
        this(new InetSocketAddress(hostname, port));
    }

    public Server(InetAddress host, int port)
    {
        this(new InetSocketAddress(host, port));
    }

    public Server(int port)
    {
        this(new InetSocketAddress(port));
    }

    public void start()
    {
        if (isRunning.compareAndSet(false, true))
            run();
    }

    public void stop()
    {
        if (isRunning.compareAndSet(true, false))
            close();
    }

    public boolean isRunning()
    {
        return isRunning.get();
    }

    public void run()
    {
        // Configure the server.
        executionHandler = new ExecutionHandler(new RequestThreadPoolExecutor());
        factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
        ServerBootstrap bootstrap = new ServerBootstrap(factory);

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory(new PipelineFactory(this));

        // Bind and start to accept incoming connections.
        logger.info("Starting listening for CQL clients on " + socket + "...");
        Channel channel = bootstrap.bind(socket);
        connectionTracker.allChannels.add(channel);
    }

    public void close()
    {
        // Close opened connections
        connectionTracker.closeAll();
        factory.releaseExternalResources();
        factory = null;
        executionHandler.releaseExternalResources();
        executionHandler = null;
    }

    public static class ConnectionTracker implements Connection.Tracker
    {
        public final ChannelGroup allChannels = new DefaultChannelGroup();
        private final EnumMap<Event.Type, ChannelGroup> groups = new EnumMap<Event.Type, ChannelGroup>(Event.Type.class);

        public ConnectionTracker()
        {
            for (Event.Type type : Event.Type.values())
                groups.put(type, new DefaultChannelGroup(type.toString()));
        }

        public void addConnection(Channel ch, Connection connection)
        {
            allChannels.add(ch);
        }

        public void register(Event.Type type, Channel ch)
        {
            groups.get(type).add(ch);
        }

        public void unregister(Channel ch)
        {
            for (ChannelGroup group : groups.values())
                group.remove(ch);
        }

        public void send(Event event)
        {
            groups.get(event.type).write(new EventMessage(event));
        }

        public void closeAll()
        {
            allChannels.close().awaitUninterruptibly();
        }
    }

    private static class PipelineFactory implements ChannelPipelineFactory
    {
        // Stateless handlers
        private static final Message.ProtocolDecoder messageDecoder = new Message.ProtocolDecoder();
        private static final Message.ProtocolEncoder messageEncoder = new Message.ProtocolEncoder();
        private static final Frame.Decompressor frameDecompressor = new Frame.Decompressor();
        private static final Frame.Compressor frameCompressor = new Frame.Compressor();
        private static final Frame.Encoder frameEncoder = new Frame.Encoder();
        private static final Message.Dispatcher dispatcher = new Message.Dispatcher();

        private final Server server;

        public PipelineFactory(Server server)
        {
            this.server = server;
        }

        public ChannelPipeline getPipeline() throws Exception
        {
            ChannelPipeline pipeline = Channels.pipeline();

            //pipeline.addLast("debug", new LoggingHandler());

            pipeline.addLast("frameDecoder", new Frame.Decoder(server.connectionTracker, ServerConnection.FACTORY));
            pipeline.addLast("frameEncoder", frameEncoder);

            pipeline.addLast("frameDecompressor", frameDecompressor);
            pipeline.addLast("frameCompressor", frameCompressor);

            pipeline.addLast("messageDecoder", messageDecoder);
            pipeline.addLast("messageEncoder", messageEncoder);

            pipeline.addLast("executor", server.executionHandler);

            pipeline.addLast("dispatcher", dispatcher);

            return pipeline;
      }
    }

    private static class EventNotifier implements IEndpointStateChangeSubscriber
    {
        private final Server server;

        private EventNotifier(Server server)
        {
            this.server = server;
        }

        public void onJoin(InetAddress endpoint, EndpointState epState)
        {
            // TODO: we don't gossip the native protocol ip/port yet, so use the
            // endpoint address and ip on which this server is listening instead.
            server.connectionTracker.send(Event.TopologyChange.newNode(endpoint, server.socket.getPort()));
        }

        public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
        {
        }

        public void onAlive(InetAddress endpoint, EndpointState state)
        {
            server.connectionTracker.send(Event.StatusChange.nodeUp(endpoint, server.socket.getPort()));
        }

        public void onDead(InetAddress endpoint, EndpointState state)
        {
            server.connectionTracker.send(Event.StatusChange.nodeDown(endpoint, server.socket.getPort()));
        }

        public void onRemove(InetAddress endpoint)
        {
            server.connectionTracker.send(Event.TopologyChange.removedNode(endpoint, server.socket.getPort()));
        }

        public void onRestart(InetAddress endpoint, EndpointState state)
        {
            server.connectionTracker.send(Event.StatusChange.nodeUp(endpoint, server.socket.getPort()));
        }
    }
}
