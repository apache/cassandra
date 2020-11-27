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

package org.apache.cassandra.distributed.impl;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationListener;

import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IListen;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.LogAction;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.mock.nodetool.InternalNodeProbe;
import org.apache.cassandra.distributed.mock.nodetool.InternalNodeProbeFactory;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.IndexSummaryManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.LegacySchemaMigrator;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.DefaultFSErrorHandler;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.tools.Output;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.DiagnosticSnapshotService;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.memory.BufferPool;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class Instance extends IsolatedExecutor implements IInvokableInstance
{
    public final IInstanceConfig config;

    // should never be invoked directly, so that it is instantiated on other class loader;
    // only visible for inheritance
    Instance(IInstanceConfig config, ClassLoader classLoader)
    {
        super("node" + config.num(), classLoader);
        this.config = config;
        Object clusterId = Objects.requireNonNull(config.get(Constants.KEY_DTEST_API_CLUSTER_ID), "cluster_id is not defined");
        ClusterIDDefiner.setId("cluster-" + clusterId);
        InstanceIDDefiner.setInstanceId(config.num());
        FBUtilities.setBroadcastInetAddress(config.broadcastAddress().getAddress());

        // Set the config at instance creation, possibly before startup() has run on all other instances.
        // setMessagingVersions below will call runOnInstance which will instantiate
        // the MessagingService and dependencies preventing later changes to network parameters.
        Config single = loadConfig(config);
        Config.setOverrideLoadConfig(() -> single);
    }

    @Override
    public boolean getLogsEnabled()
    {
        return true;
    }

    @Override
    public LogAction logs()
    {
        // the path used is defined by test/conf/logback-dtest.xml and looks like the following
        // ./build/test/logs/${cassandra.testtag}/${suitename}/${cluster_id}/${instance_id}/system.log
        String tag = System.getProperty("cassandra.testtag", "cassandra.testtag_IS_UNDEFINED");
        String suite = System.getProperty("suitename", "suitename_IS_UNDEFINED");
        String clusterId = ClusterIDDefiner.getId();
        String instanceId = InstanceIDDefiner.getInstanceId();
        return new FileLogAction(new File(String.format("build/test/logs/%s/%s/%s/%s/system.log", tag, suite, clusterId, instanceId)));
    }

    @Override
    public IInstanceConfig config()
    {
        return config;
    }

    @Override
    public ICoordinator coordinator()
    {
        return new Coordinator(this);
    }

    public IListen listen()
    {
        return new Listen(this);
    }

    @Override
    public InetSocketAddress broadcastAddress() { return config.broadcastAddress(); }

    @Override
    public SimpleQueryResult executeInternalWithResult(String query, Object... args)
    {
        return sync(() -> {
            ParsedStatement.Prepared prepared = QueryProcessor.prepareInternal(query);
            ResultMessage result = prepared.statement.executeInternal(QueryProcessor.internalQueryState(),
                                                                      QueryProcessor.makeInternalOptions(prepared, args));

            return RowUtil.toQueryResult(result);
        }).call();
    }

    @Override
    public UUID schemaVersion()
    {
        // we do not use method reference syntax here, because we need to sync on the node-local schema instance
        //noinspection Convert2MethodRef
        return Schema.instance.getVersion();
    }

    public void startup()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isShutdown()
    {
        return isolatedExecutor.isShutdown();
    }

    @Override
    public void schemaChangeInternal(String query)
    {
        sync(() -> {
            try
            {
                ClientState state = ClientState.forInternalCalls();
                state.setKeyspace(SchemaConstants.SYSTEM_KEYSPACE_NAME);
                QueryState queryState = new QueryState(state);

                CQLStatement statement = QueryProcessor.parseStatement(query, queryState).statement;
                statement.validate(state);

                QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
                statement.executeInternal(queryState, options);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error setting schema for test (query was: " + query + ")", e);
            }
        }).run();
    }

    private void registerMockMessaging(ICluster<IInstance> cluster)
    {
        BiConsumer<InetSocketAddress, IMessage> deliverToInstance = (to, message) -> cluster.get(to).receiveMessage(message);
        BiConsumer<InetSocketAddress, IMessage> deliverToInstanceIfNotFiltered = (to, message) -> {
            int fromNum = config().num();
            int toNum = cluster.get(to).config().num();

            if (cluster.filters().permitOutbound(fromNum, toNum, message)
                && cluster.filters().permitInbound(fromNum, toNum, message))
                deliverToInstance.accept(to, message);
        };

        Map<InetAddress, InetSocketAddress> addressAndPortMap = new HashMap<>();
        cluster.stream().forEach(instance -> {
            InetSocketAddress addressAndPort = instance.broadcastAddress();
            if (!addressAndPort.equals(instance.config().broadcastAddress()))
                throw new IllegalStateException("addressAndPort mismatch: " + addressAndPort + " vs " + instance.config().broadcastAddress());
            InetSocketAddress prev = addressAndPortMap.put(addressAndPort.getAddress(),
                                                                        addressAndPort);
            if (null != prev)
                throw new IllegalStateException("This version of Cassandra does not support multiple nodes with the same InetAddress: " + addressAndPort + " vs " + prev);
        });

        MessagingService.instance().addMessageSink(new MessageDeliverySink(deliverToInstanceIfNotFiltered, addressAndPortMap::get));
    }

    // unnecessary if registerMockMessaging used
    private void registerFilters(ICluster cluster)
    {
        IInstance instance = this;
        MessagingService.instance().addMessageSink(new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress toAddress)
            {
                if (isShutdown())
                    return false;

                // Port is not passed in, so take a best guess at the destination port from this instance
                IInstance to = cluster.get(NetworkTopology.addressAndPort(toAddress,
                                                                          instance.config().broadcastAddress().getPort()));
                int fromNum = config().num();
                int toNum = to.config().num();
                return cluster.filters().permitOutbound(fromNum, toNum, serializeMessage(message, id,
                                                                                 broadcastAddress(),
                                                                                 to.config().broadcastAddress()));
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                if (isShutdown())
                    return false;

                // Port is not passed in, so take a best guess at the destination port from this instance
                IInstance from = cluster.get(NetworkTopology.addressAndPort(message.from,
                                                                            instance.config().broadcastAddress().getPort()));
                int fromNum = from.config().num();
                int toNum = config().num();

                IMessage msg = serializeMessage(message, id, from.config().broadcastAddress(), broadcastAddress());

                return cluster.filters().permitInbound(fromNum, toNum, msg);
            }
        });
    }

    public static IMessage serializeMessage(MessageOut messageOut, int id, InetSocketAddress from, InetSocketAddress to)
    {
        try (DataOutputBuffer out = new DataOutputBuffer(1024))
        {
            int version = MessagingService.instance().getVersion(to.getAddress());

            out.writeInt(MessagingService.PROTOCOL_MAGIC);
            out.writeInt(id);
            long timestamp = System.currentTimeMillis();
            out.writeInt((int) timestamp);
            messageOut.serialize(out, version);
            byte[] bytes = out.toByteArray();
            if (messageOut.serializedSize(version) + 12 != bytes.length)
                throw new AssertionError(String.format("Message serializedSize(%s) does not match what was written with serialize(out, %s) for verb %s and serializer %s; " +
                                                       "expected %s, actual %s", version, version, messageOut.verb, messageOut.serializer.getClass(),
                                                       messageOut.serializedSize(version) + 12, bytes.length));
            return new MessageImpl(messageOut.verb.ordinal(), bytes, id, version, from);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static IMessage serializeMessage(MessageIn messageIn, int id, InetSocketAddress from, InetSocketAddress to)
    {
        try (DataOutputBuffer out = new DataOutputBuffer(1024))
        {
            int version = MessagingService.instance().getVersion(to.getAddress());

            out.writeInt(MessagingService.PROTOCOL_MAGIC);
            out.writeInt(id);
            long timestamp = System.currentTimeMillis();
            out.writeInt((int) timestamp);

            MessageOut.serialize(out,
                                 from.getAddress(),
                                 messageIn.verb,
                                 messageIn.parameters,
                                 messageIn.payload,
                                 version);

            return new MessageImpl(messageIn.verb.ordinal(), out.toByteArray(), id, version, from);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private class MessageDeliverySink implements IMessageSink
    {
        private final BiConsumer<InetSocketAddress, IMessage> deliver;
        private final Function<InetAddress, InetSocketAddress> lookupAddressAndPort;

        MessageDeliverySink(BiConsumer<InetSocketAddress, IMessage> deliver,
                            Function<InetAddress, InetSocketAddress> lookupAddressAndPort)
        {
            this.deliver = deliver;
            this.lookupAddressAndPort = lookupAddressAndPort;
        }

        public boolean allowOutgoingMessage(MessageOut messageOut, int id, InetAddress to)
        {
            InetSocketAddress from = broadcastAddress();
            assert from.equals(lookupAddressAndPort.apply(messageOut.from));

            // Tracing logic - similar to org.apache.cassandra.net.OutboundTcpConnection.writeConnected
            byte[] sessionBytes = (byte[]) messageOut.parameters.get(Tracing.TRACE_HEADER);
            if (sessionBytes != null)
            {
                UUID sessionId = UUIDGen.getUUID(ByteBuffer.wrap(sessionBytes));
                TraceState state = Tracing.instance.get(sessionId);
                String message = String.format("Sending %s message to %s", messageOut.verb, to);
                // session may have already finished; see CASSANDRA-5668
                if (state == null)
                {
                    byte[] traceTypeBytes = (byte[]) messageOut.parameters.get(Tracing.TRACE_TYPE);
                    Tracing.TraceType traceType = traceTypeBytes == null ? Tracing.TraceType.QUERY : Tracing.TraceType.deserialize(traceTypeBytes[0]);
                    Tracing.instance.trace(ByteBuffer.wrap(sessionBytes), message, traceType.getTTL());
                }
                else
                {
                    state.trace(message);
                    if (messageOut.verb == MessagingService.Verb.REQUEST_RESPONSE)
                        Tracing.instance.doneWithNonLocalSession(state);
                }
            }

            InetSocketAddress toFull = lookupAddressAndPort.apply(to);
            deliver.accept(toFull,
                           serializeMessage(messageOut, id, broadcastAddress(), toFull));

            return false;
        }

        public boolean allowIncomingMessage(MessageIn message, int id)
        {
            // we can filter to our heart's content on the outgoing message; no need to worry about incoming
            return true;
        }
    }

    public static MessageIn<Object> deserializeMessage(IMessage imessage)
    {
        // Based on org.apache.cassandra.net.IncomingTcpConnection.receiveMessage
        try (DataInputBuffer input = new DataInputBuffer(imessage.bytes()))
        {
            int version = imessage.version();
            if (version > MessagingService.current_version)
            {
                throw new IllegalStateException(String.format("Received message version %d but current version is %d",
                                                              version,
                                                              MessagingService.current_version));
            }

            MessagingService.validateMagic(input.readInt());
            int id;
            if (version < MessagingService.VERSION_20)
                id = Integer.parseInt(input.readUTF());
            else
                id = input.readInt();
            long currentTime = ApproximateTime.currentTimeMillis();
            return MessageIn.read(input, version, id, MessageIn.readConstructionTime(imessage.from().getAddress(), input, currentTime));
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    public void receiveMessage(IMessage imessage)
    {
        sync(() -> {
            // Based on org.apache.cassandra.net.IncomingTcpConnection.receiveMessage
            try
            {
                MessageIn message = deserializeMessage(imessage);
                if (message == null)
                {
                    // callback expired; nothing to do
                    return;
                }
                if (message.version <= MessagingService.current_version)
                {
                    MessagingService.instance().receive(message, imessage.id());
                }
                // else ignore message
            }
            catch (Throwable t)
            {
                throw new RuntimeException("Exception occurred on node " + broadcastAddress(), t);
            }
        }).run();
    }

    public int getMessagingVersion()
    {
        return callsOnInstance(() -> MessagingService.current_version).call();
    }

    public void setMessagingVersion(InetSocketAddress endpoint, int version)
    {
        runOnInstance(() -> MessagingService.instance().setVersion(endpoint.getAddress(), version));
    }

    public String getReleaseVersionString()
    {
        return callsOnInstance(() -> FBUtilities.getReleaseVersionString()).call();
    }

    public void flush(String keyspace)
    {
        runOnInstance(() -> FBUtilities.waitOnFutures(Keyspace.open(keyspace).flush()));
    }

    public void forceCompact(String keyspace, String table)
    {
        runOnInstance(() -> {
            try
            {
                Keyspace.open(keyspace).getColumnFamilyStore(table).forceMajorCompaction();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void startup(ICluster cluster)
    {
        sync(() -> {
            try
            {
                mkdirs();

                assert config.networkTopology().contains(config.broadcastAddress());
                DistributedTestSnitch.assign(config.networkTopology());

                DatabaseDescriptor.daemonInitialization();
                FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
                DatabaseDescriptor.createAllDirectories();

                // We need to  persist this as soon as possible after startup checks.
                // This should be the first write to SystemKeyspace (CASSANDRA-11742)
                SystemKeyspace.persistLocalMetadata();
                LegacySchemaMigrator.migrate();

                try
                {
                    // load schema from disk
                    Schema.instance.loadFromDisk();
                }
                catch (Exception e)
                {
                    throw e;
                }

                Keyspace.setInitialized();

                // Replay any CommitLogSegments found on disk
                try
                {
                    CommitLog.instance.recoverSegmentsOnDisk();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }

                if (config.has(NETWORK))
                {
                    registerFilters(cluster);
                    MessagingService.instance().listen();
                }
                else
                {
                    // Even though we don't use MessagingService, access the static SocketFactory
                    // instance here so that we start the static event loop state
//                    -- not sure what that means?  SocketFactory.instance.getClass();
                    registerMockMessaging(cluster);
                }

                // TODO: this is more than just gossip
                StorageService.instance.registerDaemon(CassandraDaemon.getInstanceForTesting());
                if (config.has(GOSSIP))
                {
                    StorageService.instance.initServer();
                    StorageService.instance.removeShutdownHook();
                }
                else
                {
                    initializeRing(cluster);
                }

                StorageService.instance.ensureTraceKeyspace();

                SystemKeyspace.finishStartup();

                CassandraDaemon.getInstanceForTesting().setupCompleted();

                if (config.has(NATIVE_PROTOCOL))
                {
                    CassandraDaemon.getInstanceForTesting().initializeClientTransports();
                    CassandraDaemon.getInstanceForTesting().start();
                }

                if (!FBUtilities.getBroadcastAddress().equals(broadcastAddress().getAddress()))
                    throw new IllegalStateException();
                if (DatabaseDescriptor.getStoragePort() != broadcastAddress().getPort())
                    throw new IllegalStateException();
            }
            catch (Throwable t)
            {
                if (t instanceof RuntimeException)
                    throw (RuntimeException) t;
                throw new RuntimeException(t);
            }
        }).run();
    }

    private void mkdirs()
    {
        new File(config.getString("saved_caches_directory")).mkdirs();
        new File(config.getString("hints_directory")).mkdirs();
        new File(config.getString("commitlog_directory")).mkdirs();
        for (String dir : (String[]) config.get("data_file_directories"))
            new File(dir).mkdirs();
    }

    private static Config loadConfig(IInstanceConfig overrides)
    {
        Map<String,Object> params = ((InstanceConfig) overrides).getParams();
        boolean check = true;
        if (overrides.get(Constants.KEY_DTEST_API_CONFIG_CHECK) != null)
            check = (boolean) overrides.get(Constants.KEY_DTEST_API_CONFIG_CHECK);
        return YamlConfigurationLoader.fromMap(params, check, Config.class);
    }

    public static void addToRing(boolean bootstrapping, IInstance peer)
    {
        try
        {
            IInstanceConfig config = peer.config();
            IPartitioner partitioner = FBUtilities.newPartitioner(config.getString("partitioner"));
            Token token = partitioner.getTokenFactory().fromString(config.getString("initial_token"));
            InetAddress address = config.broadcastAddress().getAddress();

            UUID hostId = config.hostId();
            Gossiper.runInGossipStageBlocking(() -> {
                Gossiper.instance.initializeNodeUnsafe(address, hostId, 1);
                Gossiper.instance.injectApplicationState(address,
                        ApplicationState.TOKENS,
                        new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(token)));
                StorageService.instance.onChange(address,
                        ApplicationState.STATUS,
                        bootstrapping
                                ? new VersionedValue.VersionedValueFactory(partitioner).bootstrapping(Collections.singleton(token))
                                : new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(token)));
                Gossiper.instance.realMarkAlive(address, Gossiper.instance.getEndpointStateForEndpoint(address));
            });
            int messagingVersion = peer.isShutdown()
                    ? MessagingService.current_version
                    : Math.min(MessagingService.current_version, peer.getMessagingVersion());
            MessagingService.instance().setVersion(address, messagingVersion);

            if (!bootstrapping)
                assert StorageService.instance.getTokenMetadata().isMember(address);
            PendingRangeCalculatorService.instance.blockUntilFinished();
        }
        catch (Throwable e) // UnknownHostException
        {
            throw new RuntimeException(e);
        }
    }

    public static void removeFromRing(IInstance peer)
    {
        try
        {
            IInstanceConfig config = peer.config();
            IPartitioner partitioner = FBUtilities.newPartitioner(config.getString("partitioner"));
            Token token = partitioner.getTokenFactory().fromString(config.getString("initial_token"));
            InetAddress address = config.broadcastAddress().getAddress();

            Gossiper.runInGossipStageBlocking(() -> {
                StorageService.instance.onChange(address,
                        ApplicationState.STATUS,
                        new VersionedValue.VersionedValueFactory(partitioner).left(Collections.singleton(token), 0L));
                Gossiper.instance.removeEndpoint(address);
            });
            PendingRangeCalculatorService.instance.blockUntilFinished();
        }
        catch (Throwable e) // UnknownHostException
        {
            throw new RuntimeException(e);
        }
    }

    public static void addToRingNormal(IInstance peer)
    {
        addToRing(false, peer);
        assert StorageService.instance.getTokenMetadata().isMember(peer.broadcastAddress().getAddress());
    }

    public static void addToRingBootstrapping(IInstance peer)
    {
        addToRing(true, peer);
    }

    private static void initializeRing(ICluster cluster)
    {
        for (int i = 1 ; i <= cluster.size() ; ++i)
            addToRing(false, cluster.get(i));

        for (int i = 1; i <= cluster.size(); ++i)
            assert StorageService.instance.getTokenMetadata().isMember(cluster.get(i).broadcastAddress().getAddress());

        StorageService.instance.setNormalModeUnsafe();
    }

    public Future<Void> shutdown()
    {
        return shutdown(true);
    }

    @Override
    public Future<Void> shutdown(boolean graceful)
    {
        if (!graceful)
            MessagingService.instance().shutdown(false);

        Future<?> future = async((ExecutorService executor) -> {
            Throwable error = null;

            error = parallelRun(error, executor,
                    () -> StorageService.instance.setRpcReady(false),
                    CassandraDaemon.getInstanceForTesting()::destroyClientTransports);

            if (config.has(GOSSIP) || config.has(NETWORK))
            {
                StorageService.instance.shutdownServer();
            }

            error = parallelRun(error, executor,
                                () -> Gossiper.instance.stopShutdownAndWait(1L, MINUTES),
                                CompactionManager.instance::forceShutdown,
                                () -> BatchlogManager.instance.shutdownAndWait(1L, MINUTES),
                                HintsService.instance::shutdownBlocking,
                                () -> StreamCoordinator.shutdownAndWait(1L, MINUTES),
                                () -> StreamSession.shutdownAndWait(1L, MINUTES),
                                () -> SecondaryIndexManager.shutdownAndWait(1L, MINUTES),
                                () -> IndexSummaryManager.instance.shutdownAndWait(1L, MINUTES),
                                () -> ColumnFamilyStore.shutdownExecutorsAndWait(1L, MINUTES),
                                () -> PendingRangeCalculatorService.instance.shutdownExecutor(1L, MINUTES),
                                () -> BufferPool.shutdownLocalCleaner(1L, MINUTES),
                                () -> Ref.shutdownReferenceReaper(1L, MINUTES),
                                () -> Memtable.MEMORY_POOL.shutdownAndWait(1L, MINUTES),
                                () -> SSTableReader.shutdownBlocking(1L, MINUTES),
                                () -> DiagnosticSnapshotService.instance.shutdownAndWait(1L, MINUTES)
            );
            error = parallelRun(error, executor,
                                () -> ScheduledExecutors.shutdownAndWait(1L, MINUTES),
                                (IgnoreThrowingRunnable) MessagingService.instance()::shutdown
            );
            error = parallelRun(error, executor,
                                () -> StageManager.shutdownAndWait(1L, MINUTES),
                                () -> SharedExecutorPool.SHARED.shutdownAndWait(1L, MINUTES)
            );
            error = parallelRun(error, executor,
                                CommitLog.instance::shutdownBlocking
            );

            Throwables.maybeFail(error);
        }).apply(isolatedExecutor);

        return CompletableFuture.runAsync(ThrowingRunnable.toRunnable(future::get), isolatedExecutor)
                                .thenRun(super::shutdown);
    }

    public int liveMemberCount()
    {
        return sync(() -> {
            if (!DatabaseDescriptor.isDaemonInitialized() || !Gossiper.instance.isEnabled())
                return 0;
            return Gossiper.instance.getLiveMembers().size();
        }).call();
    }

    public NodeToolResult nodetoolResult(boolean withNotifications, String... commandAndArgs)
    {
        return sync(() -> {
            try (CapturingOutput output = new CapturingOutput())
            {
                DTestNodeTool nodetool = new DTestNodeTool(withNotifications, output.delegate);
                int rc = nodetool.execute(commandAndArgs);
                return new NodeToolResult(commandAndArgs, rc,
                                          new ArrayList<>(nodetool.notifications.notifications),
                                          nodetool.latestError,
                                          output.getOutString(),
                                          output.getErrString());
            }
        }).call();
    }

    private static class CapturingOutput implements Closeable
    {
        @SuppressWarnings("resource")
        private final ByteArrayOutputStream outBase = new ByteArrayOutputStream();
        @SuppressWarnings("resource")
        private final ByteArrayOutputStream errBase = new ByteArrayOutputStream();

        public final PrintStream out;
        public final PrintStream err;
        private final Output delegate;

        public CapturingOutput()
        {
            PrintStream out = new PrintStream(outBase, true);
            PrintStream err = new PrintStream(errBase, true);
            this.delegate = new Output(out, err);
            this.out = out;
            this.err = err;
        }

        public String getOutString()
        {
            out.flush();
            return outBase.toString();
        }

        public String getErrString()
        {
            err.flush();
            return errBase.toString();
        }

        public void close()
        {
            out.close();
            err.close();
        }
    }

    public static class DTestNodeTool extends NodeTool {
        private final StorageServiceMBean storageProxy;
        private final CollectingNotificationListener notifications = new CollectingNotificationListener();

        private Throwable latestError;

        public DTestNodeTool(boolean withNotifications, Output output) {
            super(new InternalNodeProbeFactory(withNotifications), output);
            storageProxy = new InternalNodeProbe(withNotifications).getStorageService();
            storageProxy.addNotificationListener(notifications, null, null);
        }

        public int execute(String... args)
        {
            try
            {
                return super.execute(args);
            }
            finally
            {
                try
                {
                    storageProxy.removeNotificationListener(notifications, null, null);
                }
                catch (ListenerNotFoundException e)
                {
                    // ignored
                }
            }
        }

        protected void badUse(Exception e)
        {
            super.badUse(e);
            latestError = e;
        }

        protected void err(Throwable e)
        {
            super.err(e);
            latestError = e;
        }
    }

    private static final class CollectingNotificationListener implements NotificationListener
    {
        private final List<Notification> notifications = new CopyOnWriteArrayList<>();

        public void handleNotification(Notification notification, Object handback)
        {
            notifications.add(notification);
        }
    }

    public void uncaughtException(Thread thread, Throwable throwable)
    {
        System.out.println(String.format("Exception %s occurred on thread %s", throwable.getMessage(), thread.getName()));
        throwable.printStackTrace();
    }

    public long killAttempts()
    {
        return callOnInstance(InstanceKiller::getKillAttempts);
    }

    private static void shutdownAndWait(List<ExecutorService> executors) throws TimeoutException, InterruptedException
    {
        ExecutorUtils.shutdownNow(executors);
        ExecutorUtils.awaitTermination(1L, MINUTES, executors);
    }

    private static Throwable parallelRun(Throwable accumulate, ExecutorService runOn, ThrowingRunnable ... runnables)
    {
        List<Future<Throwable>> results = new ArrayList<>();
        for (ThrowingRunnable runnable : runnables)
        {
            results.add(runOn.submit(() -> {
                try
                {
                    runnable.run();
                    return null;
                }
                catch (Throwable t)
                {
                    return t;
                }
            }));
        }
        for (Future<Throwable> future : results)
        {
            try
            {
                Throwable t = future.get();
                if (t != null)
                    throw t;
            }
            catch (Throwable t)
            {
                accumulate = Throwables.merge(accumulate, t);
            }
        }
        return accumulate;
    }

    @FunctionalInterface
    private interface IgnoreThrowingRunnable extends ThrowingRunnable
    {
        void doRun() throws Throwable;

        @Override
        default void run()
        {
            try
            {
                doRun();
            }
            catch (Throwable e)
            {
                JVMStabilityInspector.inspectThrowable(e);
            }
        }
    }
}
