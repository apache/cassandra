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
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelMatcher;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableMetadata;
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
    public final EncryptionOptions.TlsEncryptionPolicy tlsEncryptionPolicy;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final PipelineConfigurator pipelineConfigurator;
    private final EventLoopGroup workerGroup;

    private Server (Builder builder)
    {
        this.socket = builder.getSocket();
        this.tlsEncryptionPolicy = builder.tlsEncryptionPolicy;
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

        pipelineConfigurator = builder.pipelineConfigurator != null
                               ? builder.pipelineConfigurator
                               : new PipelineConfigurator(useEpoll,
                                                          DatabaseDescriptor.getRpcKeepAlive(),
                                                          DatabaseDescriptor.useNativeTransportLegacyFlusher(),
                                                          builder.tlsEncryptionPolicy);

        EventNotifier notifier = builder.eventNotifier != null ? builder.eventNotifier : new EventNotifier();
        notifier.registerConnectionTracker(connectionTracker);
        StorageService.instance.register(notifier);
        Schema.instance.registerListener(notifier);
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
        ChannelFuture bindFuture = pipelineConfigurator.initializeChannel(workerGroup, socket, connectionFactory);
        if (!bindFuture.awaitUninterruptibly().isSuccess())
            throw new IllegalStateException(String.format("Failed to bind port %d on %s.", socket.getPort(), socket.getAddress().getHostAddress()),
                                            bindFuture.cause());

        connectionTracker.allChannels.add(bindFuture.channel());
        isRunning.set(true);
    }

    public int countConnectedClients()
    {
        return connectionTracker.countConnectedClients();
    }

    public Map<String, Integer> countConnectedClientsByUser()
    {
        return connectionTracker.countConnectedClientsByUser();
    }

    public List<ConnectedClient> getConnectedClients()
    {
        List<ConnectedClient> result = new ArrayList<>();
        for (Channel c : connectionTracker.allChannels)
        {
            Connection conn = c.attr(Connection.attributeKey).get();
            if (conn instanceof ServerConnection)
                result.add(new ConnectedClient((ServerConnection) conn));
        }
        return result;
    }

    public List<ClientStat> recentClientStats()
    {
        return connectionTracker.protocolVersionTracker.getAll();
    }

    @Override
    public void clearConnectionHistory()
    {
        connectionTracker.protocolVersionTracker.clear();
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
        private EncryptionOptions.TlsEncryptionPolicy tlsEncryptionPolicy = EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED;
        private InetAddress hostAddr;
        private int port = -1;
        private InetSocketAddress socket;
        private PipelineConfigurator pipelineConfigurator;
        private EventNotifier eventNotifier;

        public Builder withTlsEncryptionPolicy(EncryptionOptions.TlsEncryptionPolicy tlsEncryptionPolicy)
        {
            this.tlsEncryptionPolicy = tlsEncryptionPolicy;
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

        public Builder withPipelineConfigurator(PipelineConfigurator configurator)
        {
            this.pipelineConfigurator = configurator;
            return this;
        }

        public Builder withEventNotifier(EventNotifier eventNotifier)
        {
            this.eventNotifier = eventNotifier;
            return this;
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
        private static final ChannelMatcher PRE_V5_CHANNEL = channel -> channel.attr(Connection.attributeKey)
                                                                               .get()
                                                                               .getVersion()
                                                                               .isSmallerThan(ProtocolVersion.V5);

        // TODO: should we be using the GlobalEventExecutor or defining our own?
        public final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        private final EnumMap<Event.Type, ChannelGroup> groups = new EnumMap<>(Event.Type.class);
        private final ProtocolVersionTracker protocolVersionTracker = new ProtocolVersionTracker();

        public ConnectionTracker()
        {
            for (Event.Type type : Event.Type.values())
                groups.put(type, new DefaultChannelGroup(type.toString(), GlobalEventExecutor.INSTANCE));
        }

        public void addConnection(Channel ch, Connection connection)
        {
            allChannels.add(ch);

            if (ch.remoteAddress() instanceof InetSocketAddress)
                protocolVersionTracker.addConnection(((InetSocketAddress) ch.remoteAddress()).getAddress(), connection.getVersion());
        }

        public void register(Event.Type type, Channel ch)
        {
            groups.get(type).add(ch);
        }

        public void send(Event event)
        {
            ChannelGroup registered = groups.get(event.type);
            EventMessage message = new EventMessage(event);

            // Deliver event to pre-v5 channels
            registered.writeAndFlush(message, PRE_V5_CHANNEL);

            // Deliver event to post-v5 channels
            for (Channel c : registered)
                if (!PRE_V5_CHANNEL.matches(c))
                    c.attr(Dispatcher.EVENT_DISPATCHER).get().accept(message);
        }

        void closeAll()
        {
            allChannels.close().awaitUninterruptibly();
        }

        int countConnectedClients()
        {
            /*
              - When server is running: allChannels contains all clients' connections (channels)
                plus one additional channel used for the server's own bootstrap.
               - When server is stopped: the size is 0
            */
            return allChannels.size() != 0 ? allChannels.size() - 1 : 0;
        }

        Map<String, Integer> countConnectedClientsByUser()
        {
            Map<String, Integer> result = new HashMap<>();
            for (Channel c : allChannels)
            {
                Connection connection = c.attr(Connection.attributeKey).get();
                if (connection instanceof ServerConnection)
                {
                    ServerConnection conn = (ServerConnection) connection;
                    AuthenticatedUser user = conn.getClientState().getUser();
                    String name = (null != user) ? user.getName() : null;
                    result.put(name, result.getOrDefault(name, 0) + 1);
                }
            }
            return result;
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

    public static class EventNotifier implements SchemaChangeListener, IEndpointLifecycleSubscriber
    {
        private ConnectionTracker connectionTracker;

        // We keep track of the latest status change events we have sent to avoid sending duplicates
        // since StorageService may send duplicate notifications (CASSANDRA-7816, CASSANDRA-8236, CASSANDRA-9156)
        private final Map<InetAddressAndPort, LatestEvent> latestEvents = new ConcurrentHashMap<>();
        // We also want to delay delivering a NEW_NODE notification until the new node has set its RPC ready
        // state. This tracks the endpoints which have joined, but not yet signalled they're ready for clients
        private final Set<InetAddressAndPort> endpointsPendingJoinedNotification = ConcurrentHashMap.newKeySet();

        private void registerConnectionTracker(ConnectionTracker connectionTracker)
        {
            this.connectionTracker = connectionTracker;
        }

        private InetAddressAndPort getNativeAddress(InetAddressAndPort endpoint)
        {
            try
            {
                return InetAddressAndPort.getByName(StorageService.instance.getNativeaddress(endpoint, true));
            }
            catch (UnknownHostException e)
            {
                // That should not happen, so log an error, but return the
                // endpoint address since there's a good change this is right
                logger.error("Problem retrieving RPC address for {}", endpoint, e);
                return InetAddressAndPort.getByAddressOverrideDefaults(endpoint.getAddress(), DatabaseDescriptor.getNativeTransportPort());
            }
        }

        private void send(InetAddressAndPort endpoint, Event.NodeEvent event)
        {
            if (logger.isTraceEnabled())
                logger.trace("Sending event for endpoint {}, rpc address {}", endpoint, event.nodeAddressAndPort());

            // If the endpoint is not the local node, extract the node address
            // and if it is the same as our own RPC broadcast address (which defaults to the rcp address)
            // then don't send the notification. This covers the case of rpc_address set to "localhost",
            // which is not useful to any driver and in fact may cauase serious problems to some drivers,
            // see CASSANDRA-10052
            if (!endpoint.equals(FBUtilities.getBroadcastAddressAndPort()) &&
                event.nodeAddressAndPort().equals(FBUtilities.getBroadcastNativeAddressAndPort()))
                return;

            send(event);
        }

        private void send(Event event)
        {
            connectionTracker.send(event);
        }

        @Override
        public void onJoinCluster(InetAddressAndPort endpoint)
        {
            if (!StorageService.instance.isRpcReady(endpoint))
                endpointsPendingJoinedNotification.add(endpoint);
            else
                onTopologyChange(endpoint, Event.TopologyChange.newNode(getNativeAddress(endpoint)));
        }

        @Override
        public void onLeaveCluster(InetAddressAndPort endpoint)
        {
            onTopologyChange(endpoint, Event.TopologyChange.removedNode(getNativeAddress(endpoint)));
        }

        @Override
        public void onMove(InetAddressAndPort endpoint)
        {
            onTopologyChange(endpoint, Event.TopologyChange.movedNode(getNativeAddress(endpoint)));
        }

        @Override
        public void onUp(InetAddressAndPort endpoint)
        {
            if (endpointsPendingJoinedNotification.remove(endpoint))
                onJoinCluster(endpoint);

            onStatusChange(endpoint, Event.StatusChange.nodeUp(getNativeAddress(endpoint)));
        }

        @Override
        public void onDown(InetAddressAndPort endpoint)
        {
            onStatusChange(endpoint, Event.StatusChange.nodeDown(getNativeAddress(endpoint)));
        }

        private void onTopologyChange(InetAddressAndPort endpoint, Event.TopologyChange event)
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

        private void onStatusChange(InetAddressAndPort endpoint, Event.StatusChange event)
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

        @Override
        public void onCreateKeyspace(KeyspaceMetadata keyspace)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, keyspace.name));
        }

        @Override
        public void onCreateTable(TableMetadata table)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, table.keyspace, table.name));
        }

        @Override
        public void onCreateType(UserType type)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TYPE, type.keyspace, type.getNameAsString()));
        }

        @Override
        public void onCreateFunction(UDFunction function)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.FUNCTION,
                                        function.name().keyspace, function.name().name, AbstractType.asCQLTypeStringList(function.argTypes())));
        }

        @Override
        public void onCreateAggregate(UDAggregate aggregate)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.AGGREGATE,
                                        aggregate.name().keyspace, aggregate.name().name, AbstractType.asCQLTypeStringList(aggregate.argTypes())));
        }

        @Override
        public void onAlterKeyspace(KeyspaceMetadata before, KeyspaceMetadata after)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, after.name));
        }

        @Override
        public void onAlterTable(TableMetadata before, TableMetadata after, boolean affectsStatements)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, after.keyspace, after.name));
        }

        @Override
        public void onAlterType(UserType before, UserType after)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TYPE, after.keyspace, after.getNameAsString()));
        }

        @Override
        public void onAlterFunction(UDFunction before, UDFunction after)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.FUNCTION,
                                        after.name().keyspace, after.name().name, AbstractType.asCQLTypeStringList(after.argTypes())));
        }

        @Override
        public void onAlterAggregate(UDAggregate before, UDAggregate after)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.AGGREGATE,
                                        after.name().keyspace, after.name().name, AbstractType.asCQLTypeStringList(after.argTypes())));
        }

        @Override
        public void onDropKeyspace(KeyspaceMetadata keyspace, boolean dropData)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, keyspace.name));
        }

        @Override
        public void onDropTable(TableMetadata table, boolean dropData)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TABLE, table.keyspace, table.name));
        }

        @Override
        public void onDropType(UserType type)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TYPE, type.keyspace, type.getNameAsString()));
        }

        @Override
        public void onDropFunction(UDFunction function)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.FUNCTION,
                                        function.name().keyspace, function.name().name, AbstractType.asCQLTypeStringList(function.argTypes())));
        }

        @Override
        public void onDropAggregate(UDAggregate aggregate)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.AGGREGATE,
                                        aggregate.name().keyspace, aggregate.name().name, AbstractType.asCQLTypeStringList(aggregate.argTypes())));
        }
    }
}
