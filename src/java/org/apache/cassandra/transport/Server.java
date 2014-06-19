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
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.EnumMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import io.netty.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.ISaslAwareAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.*;
import org.apache.cassandra.transport.messages.EventMessage;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.ssl.SslHandler;

public class Server implements CassandraDaemon.Server
{
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    public static final int VERSION_3 = 3;
    public static final int CURRENT_VERSION = VERSION_3;

    private final ConnectionTracker connectionTracker = new ConnectionTracker();

    private final Connection.Factory connectionFactory = new Connection.Factory()
    {
        public Connection newConnection(Channel channel, int version)
        {
            return new ServerConnection(channel, version, connectionTracker);
        }
    };

    public final InetSocketAddress socket;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private EventLoopGroup workerGroup;
    private EventExecutor eventExecutorGroup;

    public Server(InetSocketAddress socket)
    {
        this.socket = socket;
        EventNotifier notifier = new EventNotifier(this);
        StorageService.instance.register(notifier);
        MigrationManager.instance.register(notifier);
        registerMetrics();
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
	    if(!isRunning())
	    {
            run();
	    }
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

    private void run()
    {
        // Check that a SaslAuthenticator can be provided by the configured
        // IAuthenticator. If not, don't start the server.
        IAuthenticator authenticator = DatabaseDescriptor.getAuthenticator();
        if (authenticator.requireAuthentication() && !(authenticator instanceof ISaslAwareAuthenticator))
        {
            logger.error("Not starting native transport as the configured IAuthenticator is not capable of SASL authentication");
            isRunning.compareAndSet(true, false);
            return;
        }

        // Configure the server.
        eventExecutorGroup = new RequestThreadPoolExecutor();
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap()
                                    .group(workerGroup)
                                    .channel(NioServerSocketChannel.class)
                                    .childOption(ChannelOption.TCP_NODELAY, true)
                                    .childOption(ChannelOption.SO_KEEPALIVE, DatabaseDescriptor.getRpcKeepAlive())
                                    .childOption(ChannelOption.ALLOCATOR, CBUtil.allocator)
                                    .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
                                    .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);

        final EncryptionOptions.ClientEncryptionOptions clientEnc = DatabaseDescriptor.getClientEncryptionOptions();
        if (clientEnc.enabled)
        {
            logger.info("Enabling encrypted CQL connections between client and server");
            bootstrap.childHandler(new SecureInitializer(this, clientEnc));
        }
        else
        {
            bootstrap.childHandler(new Initializer(this));
        }

        // Bind and start to accept incoming connections.
        logger.info("Using Netty Version: {}", Version.identify().entrySet());
        logger.info("Starting listening for CQL clients on {}...", socket);
        Channel channel = bootstrap.bind(socket).channel();
        connectionTracker.allChannels.add(channel);
        isRunning.set(true);
    }

    private void registerMetrics()
    {
        ClientMetrics.instance.addCounter("connectedNativeClients", new Callable<Integer>()
        {
            @Override
            public Integer call() throws Exception
            {
                return connectionTracker.getConnectedClients();
            }
        });
    }

    private void close()
    {
        // Close opened connections
        connectionTracker.closeAll();
        workerGroup.shutdownGracefully();
        workerGroup = null;

        eventExecutorGroup.shutdown();
        eventExecutorGroup = null;
        logger.info("Stop listening for CQL clients");
    }


    public static class ConnectionTracker implements Connection.Tracker
    {
        // TODO: should we be using the GlobalEventExecutor or defining our own?
        public final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        private final EnumMap<Event.Type, ChannelGroup> groups = new EnumMap<Event.Type, ChannelGroup>(Event.Type.class);

        public ConnectionTracker()
        {
            for (Event.Type type : Event.Type.values())
                groups.put(type, new DefaultChannelGroup(type.toString(), GlobalEventExecutor.INSTANCE));
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
            groups.get(event.type).writeAndFlush(new EventMessage(event));
        }

        public void closeAll()
        {
            allChannels.close().awaitUninterruptibly();
        }

        public int getConnectedClients()
        {
            /*
              - When server is running: allChannels contains all clients' connections (channels) 
                plus one additional channel used for the server's own bootstrap.
               - When server is stopped: the size is 0
            */
            return allChannels.size() != 0 ? allChannels.size() - 1 : 0;
        }
    }

    private static class Initializer extends ChannelInitializer
    {
        // Stateless handlers
        private static final Message.ProtocolDecoder messageDecoder = new Message.ProtocolDecoder();
        private static final Message.ProtocolEncoder messageEncoder = new Message.ProtocolEncoder();
        private static final Frame.Decompressor frameDecompressor = new Frame.Decompressor();
        private static final Frame.Compressor frameCompressor = new Frame.Compressor();
        private static final Frame.Encoder frameEncoder = new Frame.Encoder();
        private static final Message.Dispatcher dispatcher = new Message.Dispatcher();

        private final Server server;

        public Initializer(Server server)
        {
            this.server = server;
        }

        protected void initChannel(Channel channel) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();

            //pipeline.addLast("debug", new LoggingHandler());

            pipeline.addLast("frameDecoder", new Frame.Decoder(server.connectionFactory));
            pipeline.addLast("frameEncoder", frameEncoder);

            pipeline.addLast("frameDecompressor", frameDecompressor);
            pipeline.addLast("frameCompressor", frameCompressor);

            pipeline.addLast("messageDecoder", messageDecoder);
            pipeline.addLast("messageEncoder", messageEncoder);

            pipeline.addLast(server.eventExecutorGroup, "executor", dispatcher);
        }
    }

    private static class SecureInitializer extends Initializer
    {
        private final SSLContext sslContext;
        private final EncryptionOptions encryptionOptions;

        public SecureInitializer(Server server, EncryptionOptions encryptionOptions)
        {
            super(server);
            this.encryptionOptions = encryptionOptions;
            try
            {
                this.sslContext = SSLFactory.createSSLContext(encryptionOptions, encryptionOptions.require_client_auth);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Failed to setup secure pipeline", e);
            }
        }

        protected void initChannel(Channel channel) throws Exception
        {
            SSLEngine sslEngine = sslContext.createSSLEngine();
            sslEngine.setUseClientMode(false);
            sslEngine.setEnabledCipherSuites(encryptionOptions.cipher_suites);
            sslEngine.setNeedClientAuth(encryptionOptions.require_client_auth);

            SslHandler sslHandler = new SslHandler(sslEngine);
            super.initChannel(channel);
            channel.pipeline().addFirst("ssl", sslHandler);
        }
    }

    private static class EventNotifier implements IEndpointLifecycleSubscriber, IMigrationListener
    {
        private final Server server;
        private static final InetAddress bindAll;
        static {
            try
            {
                bindAll = InetAddress.getByAddress(new byte[4]);
            }
            catch (UnknownHostException e)
            {
                throw new AssertionError(e);
            }
        }

        private EventNotifier(Server server)
        {
            this.server = server;
        }

        private InetAddress getRpcAddress(InetAddress endpoint)
        {
            try
            {
                InetAddress rpcAddress = InetAddress.getByName(StorageService.instance.getRpcaddress(endpoint));
                // If rpcAddress == 0.0.0.0 (i.e. bound on all addresses), returning that is not very helpful,
                // so return the internal address (which is ok since "we're bound on all addresses").
                // Note that after all nodes are running a version that includes CASSANDRA-5899, rpcAddress should
                // never be 0.0.0.0, so this can eventually be removed.
                return rpcAddress.equals(bindAll) ? endpoint : rpcAddress;
            }
            catch (UnknownHostException e)
            {
                // That should not happen, so log an error, but return the
                // endpoint address since there's a good change this is right
                logger.error("Problem retrieving RPC address for {}", endpoint, e);
                return endpoint;
            }
        }

        public void onJoinCluster(InetAddress endpoint)
        {
            server.connectionTracker.send(Event.TopologyChange.newNode(getRpcAddress(endpoint), server.socket.getPort()));
        }

        public void onLeaveCluster(InetAddress endpoint)
        {
            server.connectionTracker.send(Event.TopologyChange.removedNode(getRpcAddress(endpoint), server.socket.getPort()));
        }

        public void onMove(InetAddress endpoint)
        {
            server.connectionTracker.send(Event.TopologyChange.movedNode(getRpcAddress(endpoint), server.socket.getPort()));
        }

        public void onUp(InetAddress endpoint)
        {
            server.connectionTracker.send(Event.StatusChange.nodeUp(getRpcAddress(endpoint), server.socket.getPort()));
        }

        public void onDown(InetAddress endpoint)
        {
            server.connectionTracker.send(Event.StatusChange.nodeDown(getRpcAddress(endpoint), server.socket.getPort()));
        }

        public void onCreateKeyspace(String ksName)
        {
            server.connectionTracker.send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, ksName));
        }

        public void onCreateColumnFamily(String ksName, String cfName)
        {
            server.connectionTracker.send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, ksName, cfName));
        }

        public void onCreateUserType(String ksName, String typeName)
        {
            server.connectionTracker.send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TYPE, ksName, typeName));
        }

        public void onUpdateKeyspace(String ksName)
        {
            server.connectionTracker.send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, ksName));
        }

        public void onUpdateColumnFamily(String ksName, String cfName)
        {
            server.connectionTracker.send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, ksName, cfName));
        }

        public void onUpdateUserType(String ksName, String typeName)
        {
            server.connectionTracker.send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TYPE, ksName, typeName));
        }

        public void onDropKeyspace(String ksName)
        {
            server.connectionTracker.send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, ksName));
        }

        public void onDropColumnFamily(String ksName, String cfName)
        {
            server.connectionTracker.send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TABLE, ksName, cfName));
        }

        public void onDropUserType(String ksName, String typeName)
        {
            server.connectionTracker.send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TYPE, ksName, typeName));
        }
    }
}
