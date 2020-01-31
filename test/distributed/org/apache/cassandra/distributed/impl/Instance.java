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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.SchemaConstants;
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
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IListen;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.mock.nodetool.InternalNodeProbeFactory;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.IndexSummaryManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.LegacySchemaMigrator;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
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
        InstanceIDDefiner.setInstanceId(config.num());
        FBUtilities.setBroadcastInetAddress(config.broadcastAddressAndPort().address);
        // Set the config at instance creation, possibly before startup() has run on all other instances.
        // setMessagingVersions below will call runOnInstance which will instantiate
        // the MessagingService and dependencies preventing later changes to network parameters.
        Config.setOverrideLoadConfig(() -> loadConfig(config));
    }

    public IInstanceConfig config()
    {
        return config;
    }

    public ICoordinator coordinator()
    {
        return new Coordinator(this);
    }

    public IListen listen()
    {
        return new Listen(this);
    }

    @Override
    public InetAddressAndPort broadcastAddressAndPort() { return config.broadcastAddressAndPort(); }

    public Object[][] executeInternal(String query, Object... args)
    {
        return sync(() -> {
            ParsedStatement.Prepared prepared = QueryProcessor.prepareInternal(query);
            ResultMessage result = prepared.statement.executeInternal(QueryProcessor.internalQueryState(),
                                                                      QueryProcessor.makeInternalOptions(prepared, args));

            if (result instanceof ResultMessage.Rows)
                return RowUtil.toObjects((ResultMessage.Rows)result);
            else
                return null;
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
        throw new UnsupportedOperationException();
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

    private void registerMockMessaging(ICluster cluster)
    {
        BiConsumer<InetAddressAndPort, IMessage> deliverToInstance = (to, message) -> cluster.get(to).receiveMessage(message);
        BiConsumer<InetAddressAndPort, IMessage> deliverToInstanceIfNotFiltered = (to, message) -> {
            int fromNum = config().num();
            int toNum = cluster.get(to).config().num();

            if (cluster.filters().permit(fromNum, toNum, message))
                deliverToInstance.accept(to, message);
        };

        Map<InetAddress, InetAddressAndPort> addressAndPortMap = new HashMap<>();
        cluster.stream().forEach(instance -> {
            InetAddressAndPort addressAndPort = instance.broadcastAddressAndPort();
            if (!addressAndPort.equals(instance.config().broadcastAddressAndPort()))
                throw new IllegalStateException("addressAndPort mismatch: " + addressAndPort + " vs " + instance.config().broadcastAddressAndPort());
            InetAddressAndPort prev = addressAndPortMap.put(addressAndPort.address, addressAndPort);
            if (null != prev)
                throw new IllegalStateException("This version of Cassandra does not support multiple nodes with the same InetAddress: " + addressAndPort + " vs " + prev);
        });

        MessagingService.instance().addMessageSink(
                new MessageDeliverySink(deliverToInstanceIfNotFiltered, addressAndPortMap::get));
    }

    // unnecessary if registerMockMessaging used
    private void registerFilter(ICluster cluster)
    {
        IInstance instance = this;
        MessagingService.instance().addMessageSink(new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress toAddress)
            {
                // Port is not passed in, so take a best guess at the destination port from this instance
                IInstance to = cluster.get(InetAddressAndPort.getByAddressOverrideDefaults(toAddress, instance.config().broadcastAddressAndPort().port));
                int fromNum = config().num();
                int toNum = to.config().num();
                return cluster.filters().permit(fromNum, toNum, serializeMessage(message, id, broadcastAddressAndPort(), to.broadcastAddressAndPort()));
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return true;
            }
        });
    }

    public static IMessage serializeMessage(MessageOut messageOut, int id, InetAddressAndPort from, InetAddressAndPort to)
    {
        try (DataOutputBuffer out = new DataOutputBuffer(1024))
        {
            int version = MessagingService.instance().getVersion(to.address);

            out.writeInt(MessagingService.PROTOCOL_MAGIC);
            out.writeInt(id);
            long timestamp = System.currentTimeMillis();
            out.writeInt((int) timestamp);
            messageOut.serialize(out, version);
            return new Message(messageOut.verb.ordinal(), out.toByteArray(), id, version, from);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private class MessageDeliverySink implements IMessageSink
    {
        private final BiConsumer<InetAddressAndPort, IMessage> deliver;
        private final Function<InetAddress, InetAddressAndPort> lookupAddressAndPort;

        MessageDeliverySink(BiConsumer<InetAddressAndPort, IMessage> deliver, Function<InetAddress, InetAddressAndPort> lookupAddressAndPort)
        {
            this.deliver = deliver;
            this.lookupAddressAndPort = lookupAddressAndPort;
        }

        public boolean allowOutgoingMessage(MessageOut messageOut, int id, InetAddress to)
        {

            InetAddressAndPort from = broadcastAddressAndPort();
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

            InetAddressAndPort toFull = lookupAddressAndPort.apply(to);
            deliver.accept(toFull, serializeMessage(messageOut, id, from, toFull));

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
            return MessageIn.read(input, version, id, MessageIn.readConstructionTime(imessage.from().address, input, currentTime));
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
                throw new RuntimeException("Exception occurred on node " + broadcastAddressAndPort(), t);
            }
        }).run();
    }

    public int getMessagingVersion()
    {
        return callsOnInstance(() -> MessagingService.current_version).call();
    }

    public void setMessagingVersion(InetAddressAndPort endpoint, int version)
    {
        runOnInstance(() -> MessagingService.instance().setVersion(endpoint.address, version));
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
    public int nodetool(String... commandAndArgs)
    {
        return sync(() -> new NodeTool(new InternalNodeProbeFactory()).execute(commandAndArgs)).call();
    }

    @Override
    public void startup(ICluster cluster)
    {
        sync(() -> {
            try
            {
                mkdirs();

                assert config.networkTopology().contains(config.broadcastAddressAndPort());
                DistributedTestSnitch.assign(config.networkTopology());

                DatabaseDescriptor.daemonInitialization();
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
                    registerFilter(cluster);
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

                if (config.has(NATIVE_PROTOCOL))
                {
                    CassandraDaemon.getInstanceForTesting().initializeNativeTransport();
                    CassandraDaemon.getInstanceForTesting().startNativeTransport();
                    StorageService.instance.setRpcReady(true);
                }

                if (!FBUtilities.getBroadcastAddress().equals(broadcastAddressAndPort().address))
                    throw new IllegalStateException();
                if (DatabaseDescriptor.getStoragePort() != broadcastAddressAndPort().port)
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
        Config config = new Config();
        overrides.propagate(config);
        return config;
    }

    private void initializeRing(ICluster cluster)
    {
        // This should be done outside instance in order to avoid serializing config
        String partitionerName = config.getString("partitioner");
        List<String> initialTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();
        for (int i = 1 ; i <= cluster.size() ; ++i)
        {
            IInstanceConfig config = cluster.get(i).config();
            initialTokens.add(config.getString("initial_token"));
            hosts.add(config.broadcastAddressAndPort());
            hostIds.add(config.hostId());
        }

        try
        {
            IPartitioner partitioner = FBUtilities.newPartitioner(partitionerName);
            StorageService storageService = StorageService.instance;
            List<Token> tokens = new ArrayList<>();
            for (String token : initialTokens)
                tokens.add(partitioner.getTokenFactory().fromString(token));

            for (int i = 0; i < tokens.size(); i++)
            {
                InetAddressAndPort ep = hosts.get(i);
                UUID hostId = hostIds.get(i);
                Token token = tokens.get(i);
                Gossiper.runInGossipStageBlocking(() -> {
                    Gossiper.instance.initializeNodeUnsafe(ep.address, hostId, 1);
                    Gossiper.instance.injectApplicationState(ep.address,
                                                             ApplicationState.TOKENS,
                                                             new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(token)));
                    storageService.onChange(ep.address,
                                            ApplicationState.STATUS,
                                            new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(token)));
                    Gossiper.instance.realMarkAlive(ep.address, Gossiper.instance.getEndpointStateForEndpoint(ep.address));
                });

                int messagingVersion = cluster.get(ep).isShutdown()
                                       ? MessagingService.current_version
                                       : Math.min(MessagingService.current_version, cluster.get(ep).getMessagingVersion());
                MessagingService.instance().setVersion(ep.address, messagingVersion);
            }

            // check that all nodes are in token metadata
            for (int i = 0; i < tokens.size(); ++i)
                assert storageService.getTokenMetadata().isMember(hosts.get(i).address);
        }
        catch (Throwable e) // UnknownHostException
        {
            throw new RuntimeException(e);
        }
    }

    public Future<Void> shutdown()
    {
        return shutdown(true);
    }

    public Future<Void> shutdown(boolean graceful)
    {
        if (!graceful)
            MessagingService.instance().shutdown(false);

        Future<?> future = async((ExecutorService executor) -> {
            Throwable error = null;

            error = parallelRun(error, executor,
                    () -> StorageService.instance.setRpcReady(false),
                    CassandraDaemon.getInstanceForTesting()::destroyNativeTransport);

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
                                () -> SSTableReader.shutdownBlocking(1L, MINUTES)
            );
            error = parallelRun(error, executor,
                                () -> ScheduledExecutors.shutdownAndWait(1L, MINUTES),
                                MessagingService.instance()::shutdown
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
}
