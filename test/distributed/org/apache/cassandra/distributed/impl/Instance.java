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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

import io.netty.util.concurrent.GlobalEventExecutor;

import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.SystemKeyspaceMigrator40;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IListen;
import org.apache.cassandra.distributed.api.IMessage;
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
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.async.StreamingInboundHandler;
import org.apache.cassandra.streaming.StreamReceiveTask;
import org.apache.cassandra.streaming.StreamTransferTask;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.memory.BufferPool;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
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
        FBUtilities.setBroadcastInetAddressAndPort(config.broadcastAddressAndPort());
        // Set the config at instance creation, possibly before startup() has run on all other instances.
        // setMessagingVersions below will call runOnInstance which will instantiate
        // the MessagingService and dependencies preventing later changes to network parameters.
        Config.setOverrideLoadConfig(() -> loadConfig(config));

        // Enable streaming inbound handler tracking so they can be closed properly without leaking
        // the blocking IO thread.
        StreamingInboundHandler.trackInboundHandlers();
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
    public InetAddressAndPort broadcastAddressAndPort() { return config.broadcastAddressAndPort(); }

    @Override
    public Object[][] executeInternal(String query, Object... args)
    {
        return sync(() -> {
            QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(query);
            ResultMessage result = prepared.statement.executeLocally(QueryProcessor.internalQueryState(),
                                                                     QueryProcessor.makeInternalOptions(prepared.statement, args));

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
                ClientState state = ClientState.forInternalCalls(SchemaConstants.SYSTEM_KEYSPACE_NAME);
                QueryState queryState = new QueryState(state);

                CQLStatement statement = QueryProcessor.parseStatement(query, queryState.getClientState());
                statement.validate(state);

                QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
                statement.executeLocally(queryState, options);
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
            if (cluster.filters().permit(this, cluster.get(to), message.verb()))
                deliverToInstance.accept(to, message);
        };

        MessagingService.instance().outboundSink.add(new MessageDeliverySink(deliverToInstanceIfNotFiltered));
    }

    // unnecessary if registerMockMessaging used
    private void registerFilter(ICluster cluster)
    {
        MessagingService.instance().outboundSink.add((message, to) -> cluster.filters().permit(this, cluster.get(to), message.verb().id));
    }

    private class MessageDeliverySink implements BiPredicate<Message<?>, InetAddressAndPort>
    {
        private final BiConsumer<InetAddressAndPort, IMessage> deliver;
        MessageDeliverySink(BiConsumer<InetAddressAndPort, IMessage> deliver)
        {
            this.deliver = deliver;
        }

        @Override
        public boolean test(Message<?> messageOut, InetAddressAndPort to)
        {
            try (DataOutputBuffer out = new DataOutputBuffer(1024))
            {
                InetAddressAndPort from = broadcastAddressAndPort();
                Tracing.instance.traceOutgoingMessage(messageOut, to);
                Message.serializer.serialize(messageOut, out, MessagingService.current_version);
                deliver.accept(to, new MessageImpl(messageOut.verb().id, out.toByteArray(), messageOut.id(), MessagingService.current_version, from));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return false;
        }
    }

    @Override
    public void receiveMessage(IMessage message)
    {
        sync(() -> {
            try (DataInputBuffer in = new DataInputBuffer(message.bytes()))
            {
                Message<?> messageIn = Message.serializer.deserialize(in, message.from(), message.version());
                Message.Header header = messageIn.header;
                TraceState state = Tracing.instance.initializeFromMessage(header);
                if (state != null) state.trace("{} message received from {}", header.verb, header.from);
                StageManager.getStage(header.verb.stage).execute(
                    ThrowingRunnable.toRunnable(() -> messageIn.verb().handler().doVerb((Message<Object>) messageIn)),
                    ExecutorLocals.create(state));
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
        MessagingService.instance().versions.set(endpoint, version);
    }

    @Override
    public void startup(ICluster cluster)
    {
        sync(() -> {
            try
            {
                if (config.has(GOSSIP))
                {
                    // TODO: hacky
                    System.setProperty("cassandra.ring_delay_ms", "5000");
                    System.setProperty("cassandra.consistent.rangemovement", "false");
                    System.setProperty("cassandra.consistent.simultaneousmoves.allow", "true");
                }

                mkdirs();

                assert config.networkTopology().contains(config.broadcastAddressAndPort());
                DistributedTestSnitch.assign(config.networkTopology());

                DatabaseDescriptor.daemonInitialization();
                DatabaseDescriptor.createAllDirectories();

                // We need to persist this as soon as possible after startup checks.
                // This should be the first write to SystemKeyspace (CASSANDRA-11742)
                SystemKeyspace.persistLocalMetadata();
                SystemKeyspaceMigrator40.migrate();

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
                }
                else
                {
                    initializeRing(cluster);
                }

                StorageService.instance.ensureTraceKeyspace();

                SystemKeyspace.finishStartup();

                if (!FBUtilities.getBroadcastAddressAndPort().equals(broadcastAddressAndPort()))
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
                    Gossiper.instance.initializeNodeUnsafe(ep, hostId, 1);
                    Gossiper.instance.injectApplicationState(ep,
                                                             ApplicationState.TOKENS,
                                                             new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(token)));
                    storageService.onChange(ep,
                                            ApplicationState.STATUS_WITH_PORT,
                                            new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(token)));
                    storageService.onChange(ep,
                                            ApplicationState.STATUS,
                                            new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(token)));
                    Gossiper.instance.realMarkAlive(ep, Gossiper.instance.getEndpointStateForEndpoint(ep));
                });
                MessagingService.instance().versions.set(ep, MessagingService.current_version);
            }

            // check that all nodes are in token metadata
            for (int i = 0; i < tokens.size(); ++i)
                assert storageService.getTokenMetadata().isMember(hosts.get(i));
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

    @Override
    public Future<Void> shutdown(boolean graceful)
    {
        Future<?> future = async((ExecutorService executor) -> {
            Throwable error = null;

            if (config.has(GOSSIP))
            {
                StorageService.instance.shutdownServer();
            }

            error = parallelRun(error, executor,
                                () -> Gossiper.instance.stopShutdownAndWait(1L, MINUTES),
                                CompactionManager.instance::forceShutdown,
                                () -> BatchlogManager.instance.shutdownAndWait(1L, MINUTES),
                                HintsService.instance::shutdownBlocking,
                                StreamingInboundHandler::shutdown,
                                () -> StreamReceiveTask.shutdownAndWait(1L, MINUTES),
                                () -> StreamTransferTask.shutdownAndWait(1L, MINUTES),
                                () -> SecondaryIndexManager.shutdownAndWait(1L, MINUTES),
                                () -> IndexSummaryManager.instance.shutdownAndWait(1L, MINUTES),
                                () -> ColumnFamilyStore.shutdownExecutorsAndWait(1L, MINUTES),
                                () -> PendingRangeCalculatorService.instance.shutdownAndWait(1L, MINUTES),
                                () -> BufferPool.shutdownLocalCleaner(1L, MINUTES),
                                () -> Ref.shutdownReferenceReaper(1L, MINUTES),
                                () -> Memtable.MEMORY_POOL.shutdownAndWait(1L, MINUTES),
                                () -> ScheduledExecutors.shutdownAndWait(1L, MINUTES),
                                () -> SSTableReader.shutdownBlocking(1L, MINUTES),
                                () -> shutdownAndWait(Collections.singletonList(ActiveRepairService.repairCommandExecutor)),
                                () -> ScheduledExecutors.shutdownAndWait(1L, MINUTES)
            );

            error = parallelRun(error, executor,
                                CommitLog.instance::shutdownBlocking,
                                () -> MessagingService.instance().shutdown(1L, MINUTES, false, true)
            );
            error = parallelRun(error, executor,
                                () -> GlobalEventExecutor.INSTANCE.awaitInactivity(1l, MINUTES),
                                () -> StageManager.shutdownAndWait(1L, MINUTES),
                                () -> SharedExecutorPool.SHARED.shutdownAndWait(1L, MINUTES)
            );

            Throwables.maybeFail(error);
        }).apply(isolatedExecutor);

        return CompletableFuture.runAsync(ThrowingRunnable.toRunnable(future::get), isolatedExecutor)
                                .thenRun(super::shutdown);
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
}
