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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.Version;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.net.ResourceLimits;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.*;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.utils.FBUtilities;

public class Server implements CassandraDaemon.Server
{
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private static final boolean useEpoll = NativeTransportService.useEpoll();

    private final ConnectionTracker connectionTracker = new ConnectionTracker();

    private final Connection.Factory connectionFactory = new Connection.Factory()
    {
        public Connection newConnection(Channel channel, ProtocolVersion version)
        {
            return new ServerConnection(channel, version, connectionTracker);
        }
    };

    public final InetSocketAddress socket;
    public boolean useSSL = false;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private EventLoopGroup workerGroup;
    private final ProtocolVersionLimit protocolVersionLimit;

    private Server (Builder builder)
    {
        this.socket = builder.getSocket();
        this.useSSL = builder.useSSL;
        this.protocolVersionLimit = builder.getProtocolVersionLimit();

        if (builder.workerGroup != null)
        {
            workerGroup = builder.workerGroup;
        }
        else
        {
            if (useEpoll)
                workerGroup = new EpollEventLoopGroup();
            else
                workerGroup = new NioEventLoopGroup();
        }
        EventNotifier notifier = new EventNotifier(this);
        StorageService.instance.register(notifier);
        MigrationManager.instance.register(notifier);
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

    public synchronized void start()
    {
        if(isRunning())
            return;

        // Configure the server.
        ServerBootstrap bootstrap = new ServerBootstrap()
                                    .channel(useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                                    .childOption(ChannelOption.TCP_NODELAY, true)
                                    .childOption(ChannelOption.SO_LINGER, 0)
                                    .childOption(ChannelOption.SO_KEEPALIVE, DatabaseDescriptor.getRpcKeepAlive())
                                    .childOption(ChannelOption.ALLOCATOR, CBUtil.allocator)
                                    .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
                                    .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);
        if (workerGroup != null)
            bootstrap = bootstrap.group(workerGroup);

        if (this.useSSL)
        {
            final EncryptionOptions.ClientEncryptionOptions clientEnc = DatabaseDescriptor.getClientEncryptionOptions();

            if (clientEnc.optional)
            {
                logger.info("Enabling optionally encrypted CQL connections between client and server");
                bootstrap.childHandler(new OptionalSecureInitializer(this, clientEnc));
            }
            else
            {
                logger.info("Enabling encrypted CQL connections between client and server");
                bootstrap.childHandler(new SecureInitializer(this, clientEnc));
            }
        }
        else
        {
            bootstrap.childHandler(new Initializer(this));
        }

        // Bind and start to accept incoming connections.
        logger.info("Using Netty Version: {}", Version.identify().entrySet());
        logger.info("Starting listening for CQL clients on {} ({})...", socket, this.useSSL ? "encrypted" : "unencrypted");

        ChannelFuture bindFuture = bootstrap.bind(socket);
        if (!bindFuture.awaitUninterruptibly().isSuccess())
            throw new IllegalStateException(String.format("Failed to bind port %d on %s.", socket.getPort(), socket.getAddress().getHostAddress()));

        connectionTracker.allChannels.add(bindFuture.channel());
        isRunning.set(true);
    }

    public int getConnectedClients()
    {
        return connectionTracker.getConnectedClients();
    }

    private void close()
    {
        // Close opened connections
        connectionTracker.closeAll();

        logger.info("Stop listening for CQL clients");
    }

    public static class Builder
    {
        private EventLoopGroup workerGroup;
        private EventExecutor eventExecutorGroup;
        private boolean useSSL = false;
        private InetAddress hostAddr;
        private int port = -1;
        private InetSocketAddress socket;
        private ProtocolVersionLimit versionLimit;

        public Builder withSSL(boolean useSSL)
        {
            this.useSSL = useSSL;
            return this;
        }

        public Builder withEventLoopGroup(EventLoopGroup eventLoopGroup)
        {
            this.workerGroup = eventLoopGroup;
            return this;
        }

        public Builder withHost(InetAddress host)
        {
            this.hostAddr = host;
            this.socket = null;
            return this;
        }

        public Builder withPort(int port)
        {
            this.port = port;
            this.socket = null;
            return this;
        }

        public Builder withProtocolVersionLimit(ProtocolVersionLimit limit)
        {
            this.versionLimit = limit;
            return this;
        }

        ProtocolVersionLimit getProtocolVersionLimit()
        {
            if (versionLimit == null)
                throw new IllegalArgumentException("Missing protocol version limiter");
            return versionLimit;
        }

        public Server build()
        {
            return new Server(this);
        }

        private InetSocketAddress getSocket()
        {
            if (this.socket != null)
                return this.socket;
            else
            {
                if (this.port == -1)
                    throw new IllegalStateException("Missing port number");
                if (this.hostAddr != null)
                    this.socket = new InetSocketAddress(this.hostAddr, this.port);
                else
                    throw new IllegalStateException("Missing host");
                return this.socket;
            }
        }
    }

    public static class ConnectionTracker implements Connection.Tracker
    {
        // TODO: should we be using the GlobalEventExecutor or defining our own?
        public final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        private final EnumMap<Event.Type, ChannelGroup> groups = new EnumMap<>(Event.Type.class);

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

    // global inflight payload across all channels across all endpoints
    private static final ResourceLimits.Concurrent globalRequestPayloadInFlight = new ResourceLimits.Concurrent(DatabaseDescriptor.getNativeTransportMaxConcurrentRequestsInBytes());

    public static class EndpointPayloadTracker
    {
        // inflight payload per endpoint across corresponding channels
        private static final ConcurrentMap<InetAddress, EndpointPayloadTracker> requestPayloadInFlightPerEndpoint = new ConcurrentHashMap<>();

        private final AtomicInteger refCount = new AtomicInteger(0);
        private final InetAddress endpoint;

        final ResourceLimits.EndpointAndGlobal endpointAndGlobalPayloadsInFlight = new ResourceLimits.EndpointAndGlobal(new ResourceLimits.Concurrent(DatabaseDescriptor.getNativeTransportMaxConcurrentRequestsInBytesPerIp()),
                                                                                                                         globalRequestPayloadInFlight);

        private EndpointPayloadTracker(InetAddress endpoint)
        {
            this.endpoint = endpoint;
        }

        public static EndpointPayloadTracker get(InetAddress endpoint)
        {
            while (true)
            {
                EndpointPayloadTracker result = requestPayloadInFlightPerEndpoint.computeIfAbsent(endpoint, EndpointPayloadTracker::new);
                if (result.acquire())
                    return result;

                requestPayloadInFlightPerEndpoint.remove(endpoint, result);
            }
        }

        private boolean acquire()
        {
            return 0 < refCount.updateAndGet(i -> i < 0 ? i : i + 1);
        }

        public void release()
        {
            if (-1 == refCount.updateAndGet(i -> i == 1 ? -1 : i - 1))
                requestPayloadInFlightPerEndpoint.remove(endpoint, this);
        }
    }

    private static class Initializer extends ChannelInitializer<Channel>
    {
        // Stateless handlers
        private static final Message.ProtocolDecoder messageDecoder = new Message.ProtocolDecoder();
        private static final Frame.Decompressor frameDecompressor = new Frame.Decompressor();
        private static final Frame.Compressor frameCompressor = new Frame.Compressor();
        private static final Frame.Encoder frameEncoder = new Frame.Encoder();
        private static final Message.ExceptionHandler exceptionHandler = new Message.ExceptionHandler();
        private static final ConnectionLimitHandler connectionLimitHandler = new ConnectionLimitHandler();

        private final Server server;

        public Initializer(Server server)
        {
            this.server = server;
        }

        protected void initChannel(Channel channel) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();

            // Add the ConnectionLimitHandler to the pipeline if configured to do so.
            if (DatabaseDescriptor.getNativeTransportMaxConcurrentConnections() > 0
                    || DatabaseDescriptor.getNativeTransportMaxConcurrentConnectionsPerIp() > 0)
            {
                // Add as first to the pipeline so the limit is enforced as first action.
                pipeline.addFirst("connectionLimitHandler", connectionLimitHandler);
            }

            //pipeline.addLast("debug", new LoggingHandler());

            pipeline.addLast("frameDecoder", new Frame.Decoder(server.connectionFactory, server.protocolVersionLimit));
            pipeline.addLast("frameEncoder", frameEncoder);

            pipeline.addLast("frameDecompressor", frameDecompressor);
            pipeline.addLast("frameCompressor", frameCompressor);

            pipeline.addLast("messageDecoder", messageDecoder);
            pipeline.addLast("messageEncoder", new Message.ProtocolEncoder(server.protocolVersionLimit));

            pipeline.addLast("executor", new Message.Dispatcher(DatabaseDescriptor.useNativeTransportLegacyFlusher(),
                                                                EndpointPayloadTracker.get(((InetSocketAddress) channel.remoteAddress()).getAddress())));

            // The exceptionHandler will take care of handling exceptionCaught(...) events while still running
            // on the same EventLoop as all previous added handlers in the pipeline. This is important as the used
            // eventExecutorGroup may not enforce strict ordering for channel events.
            // As the exceptionHandler runs in the EventLoop as the previous handlers we are sure all exceptions are
            // correctly handled before the handler itself is removed.
            // See https://issues.apache.org/jira/browse/CASSANDRA-13649
            pipeline.addLast("exceptionHandler", exceptionHandler);
        }
    }

    protected abstract static class AbstractSecureIntializer extends Initializer
    {
        private final SSLContext sslContext;
        private final EncryptionOptions encryptionOptions;

        protected AbstractSecureIntializer(Server server, EncryptionOptions encryptionOptions)
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

        protected final SslHandler createSslHandler()
        {
            SSLEngine sslEngine = sslContext.createSSLEngine();
            sslEngine.setUseClientMode(false);
            String[] suites = SSLFactory.filterCipherSuites(sslEngine.getSupportedCipherSuites(), encryptionOptions.cipher_suites);
            sslEngine.setEnabledCipherSuites(suites);
            sslEngine.setNeedClientAuth(encryptionOptions.require_client_auth);
            return new SslHandler(sslEngine);
        }
    }

    private static class OptionalSecureInitializer extends AbstractSecureIntializer
    {
        public OptionalSecureInitializer(Server server, EncryptionOptions encryptionOptions)
        {
            super(server, encryptionOptions);
        }

        protected void initChannel(final Channel channel) throws Exception
        {
            super.initChannel(channel);
            channel.pipeline().addFirst("sslDetectionHandler", new ByteToMessageDecoder()
            {
                @Override
                protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception
                {
                    if (byteBuf.readableBytes() < 5)
                    {
                        // To detect if SSL must be used we need to have at least 5 bytes, so return here and try again
                        // once more bytes a ready.
                        return;
                    }
                    if (SslHandler.isEncrypted(byteBuf))
                    {
                        // Connection uses SSL/TLS, replace the detection handler with a SslHandler and so use
                        // encryption.
                        SslHandler sslHandler = createSslHandler();
                        channelHandlerContext.pipeline().replace(this, "ssl", sslHandler);
                    }
                    else
                    {
                        // Connection use no TLS/SSL encryption, just remove the detection handler and continue without
                        // SslHandler in the pipeline.
                        channelHandlerContext.pipeline().remove(this);
                    }
                }
            });
        }
    }

    private static class SecureInitializer extends AbstractSecureIntializer
    {
        public SecureInitializer(Server server, EncryptionOptions encryptionOptions)
        {
            super(server, encryptionOptions);
        }

        protected void initChannel(Channel channel) throws Exception
        {
            SslHandler sslHandler = createSslHandler();
            super.initChannel(channel);
            channel.pipeline().addFirst("ssl", sslHandler);
        }
    }

    private static class LatestEvent
    {
        public final Event.StatusChange.Status status;
        public final Event.TopologyChange.Change topology;

        private LatestEvent(Event.StatusChange.Status status, Event.TopologyChange.Change topology)
        {
            this.status = status;
            this.topology = topology;
        }

        @Override
        public String toString()
        {
            return String.format("Status %s, Topology %s", status, topology);
        }

        public static LatestEvent forStatusChange(Event.StatusChange.Status status, LatestEvent prev)
        {
            return new LatestEvent(status,
                                   prev == null ?
                                           null :
                                           prev.topology);
        }

        public static LatestEvent forTopologyChange(Event.TopologyChange.Change change, LatestEvent prev)
        {
            return new LatestEvent(prev == null ?
                                           null :
                                           prev.status,
                                           change);
        }
    }

    private static class EventNotifier extends MigrationListener implements IEndpointLifecycleSubscriber
    {
        private final Server server;

        // We keep track of the latest status change events we have sent to avoid sending duplicates
        // since StorageService may send duplicate notifications (CASSANDRA-7816, CASSANDRA-8236, CASSANDRA-9156)
        private final Map<InetAddress, LatestEvent> latestEvents = new ConcurrentHashMap<>();
        // We also want to delay delivering a NEW_NODE notification until the new node has set its RPC ready
        // state. This tracks the endpoints which have joined, but not yet signalled they're ready for clients
        private final Set<InetAddress> endpointsPendingJoinedNotification = ConcurrentHashMap.newKeySet();


        private static final InetAddress bindAll;
        static
        {
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

        private void send(InetAddress endpoint, Event.NodeEvent event)
        {
            if (logger.isTraceEnabled())
                logger.trace("Sending event for endpoint {}, rpc address {}", endpoint, event.nodeAddress());

            // If the endpoint is not the local node, extract the node address
            // and if it is the same as our own RPC broadcast address (which defaults to the rcp address)
            // then don't send the notification. This covers the case of rpc_address set to "localhost",
            // which is not useful to any driver and in fact may cauase serious problems to some drivers,
            // see CASSANDRA-10052
            if (!endpoint.equals(FBUtilities.getBroadcastAddress()) &&
                event.nodeAddress().equals(FBUtilities.getBroadcastRpcAddress()))
                return;

            send(event);
        }

        private void send(Event event)
        {
            server.connectionTracker.send(event);
        }

        public void onJoinCluster(InetAddress endpoint)
        {
            if (!StorageService.instance.isRpcReady(endpoint))
                endpointsPendingJoinedNotification.add(endpoint);
            else
                onTopologyChange(endpoint, Event.TopologyChange.newNode(getRpcAddress(endpoint), server.socket.getPort()));
        }

        public void onLeaveCluster(InetAddress endpoint)
        {
            onTopologyChange(endpoint, Event.TopologyChange.removedNode(getRpcAddress(endpoint), server.socket.getPort()));
        }

        public void onMove(InetAddress endpoint)
        {
            onTopologyChange(endpoint, Event.TopologyChange.movedNode(getRpcAddress(endpoint), server.socket.getPort()));
        }

        public void onUp(InetAddress endpoint)
        {
            if (endpointsPendingJoinedNotification.remove(endpoint))
                onJoinCluster(endpoint);

            onStatusChange(endpoint, Event.StatusChange.nodeUp(getRpcAddress(endpoint), server.socket.getPort()));
        }

        public void onDown(InetAddress endpoint)
        {
            onStatusChange(endpoint, Event.StatusChange.nodeDown(getRpcAddress(endpoint), server.socket.getPort()));
        }

        private void onTopologyChange(InetAddress endpoint, Event.TopologyChange event)
        {
            if (logger.isTraceEnabled())
                logger.trace("Topology changed event : {}, {}", endpoint, event.change);

            LatestEvent prev = latestEvents.get(endpoint);
            if (prev == null || prev.topology != event.change)
            {
                LatestEvent ret = latestEvents.put(endpoint, LatestEvent.forTopologyChange(event.change, prev));
                if (ret == prev)
                    send(endpoint, event);
            }
        }

        private void onStatusChange(InetAddress endpoint, Event.StatusChange event)
        {
            if (logger.isTraceEnabled())
                logger.trace("Status changed event : {}, {}", endpoint, event.status);

            LatestEvent prev = latestEvents.get(endpoint);
            if (prev == null || prev.status != event.status)
            {
                LatestEvent ret = latestEvents.put(endpoint, LatestEvent.forStatusChange(event.status, null));
                if (ret == prev)
                    send(endpoint, event);
            }
        }

        public void onCreateKeyspace(String ksName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, ksName));
        }

        public void onCreateColumnFamily(String ksName, String cfName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, ksName, cfName));
        }

        public void onCreateUserType(String ksName, String typeName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TYPE, ksName, typeName));
        }

        public void onCreateFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.FUNCTION,
                                        ksName, functionName, AbstractType.asCQLTypeStringList(argTypes)));
        }

        public void onCreateAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.AGGREGATE,
                                        ksName, aggregateName, AbstractType.asCQLTypeStringList(argTypes)));
        }

        public void onUpdateKeyspace(String ksName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, ksName));
        }

        public void onUpdateColumnFamily(String ksName, String cfName, boolean affectsStatements)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, ksName, cfName));
        }

        public void onUpdateUserType(String ksName, String typeName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TYPE, ksName, typeName));
        }

        public void onUpdateFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.FUNCTION,
                                        ksName, functionName, AbstractType.asCQLTypeStringList(argTypes)));
        }

        public void onUpdateAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.AGGREGATE,
                                        ksName, aggregateName, AbstractType.asCQLTypeStringList(argTypes)));
        }

        public void onDropKeyspace(String ksName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, ksName));
        }

        public void onDropColumnFamily(String ksName, String cfName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TABLE, ksName, cfName));
        }

        public void onDropUserType(String ksName, String typeName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TYPE, ksName, typeName));
        }

        public void onDropFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.FUNCTION,
                                        ksName, functionName, AbstractType.asCQLTypeStringList(argTypes)));
        }

        public void onDropAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.AGGREGATE,
                                        ksName, aggregateName, AbstractType.asCQLTypeStringList(argTypes)));
        }
    }
}
