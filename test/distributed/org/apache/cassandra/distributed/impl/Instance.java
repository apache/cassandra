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
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import org.apache.cassandra.batchlog.BatchlogManager;
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
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageDeliveryTask;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.MessageInHandler;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.memory.BufferPool;

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
        BiConsumer<InetAddressAndPort, IMessage> deliverToInstanceIfNotFiltered = cluster.filters().filter(deliverToInstance);

        MessagingService.instance().addMessageSink(new MessageDeliverySink(deliverToInstanceIfNotFiltered));
    }

    private class MessageDeliverySink implements IMessageSink
    {
        private final BiConsumer<InetAddressAndPort, IMessage> deliver;
        MessageDeliverySink(BiConsumer<InetAddressAndPort, IMessage> deliver)
        {
            this.deliver = deliver;
        }

        @Override
        public boolean allowOutgoingMessage(MessageOut messageOut, int id, InetAddressAndPort to)
        {
            try (DataOutputBuffer out = new DataOutputBuffer(1024))
            {
                InetAddressAndPort from = broadcastAddressAndPort();
                int version = MessagingService.instance().getVersion(to);
                messageOut.serialize(out, version);
                deliver.accept(to, new Message(messageOut.verb.getId(), out.toByteArray(), id, version, from));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return false;
        }

        @Override
        public boolean allowIncomingMessage(MessageIn message, int id)
        {
            // we can filter to our heart's content on the outgoing message; no need to worry about incoming
            return true;
        }
    }

    @Override
    public void receiveMessage(IMessage message)
    {
        sync(() -> {
            try (DataInputBuffer in = new DataInputBuffer(message.bytes()))
            {
                MessageIn<?> messageIn = MessageInHandler.deserialize(in, message.id(), message.version(), message.from());
                Runnable deliver = new MessageDeliveryTask(messageIn, message.id());
                deliver.run();
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
        runOnInstance(() -> MessagingService.instance().setVersion(endpoint, version));
    }

    @Override
    public void startup(ICluster cluster)
    {
        sync(() -> {
            try
            {
                mkdirs();
                Config.setOverrideLoadConfig(() -> loadConfig(config));
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

                // Even though we don't use MessagingService, access the static NettyFactory
                // instance here so that we start the static event loop state
                // (e.g. acceptGroup, inboundGroup, outboundGroup, etc ...). We can remove this
                // once we actually use the MessagingService to communicate between nodes
                NettyFactory.instance.getClass();
                initializeRing(cluster);
                registerMockMessaging(cluster);

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
                int version = Math.min(MessagingService.current_version, cluster.get(ep).getMessagingVersion());
                MessagingService.instance().setVersion(ep, version);
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

    @Override
    public Future<Void> shutdown()
    {
        Future<?> future = async((ExecutorService executor) -> {
            Throwable error = null;
            error = parallelRun(error, executor,
                    Gossiper.instance::stop,
                    CompactionManager.instance::forceShutdown,
                    BatchlogManager.instance::shutdown,
                    HintsService.instance::shutdownBlocking,
                    CommitLog.instance::shutdownBlocking,
                    SecondaryIndexManager::shutdownExecutors,
                    ColumnFamilyStore::shutdownFlushExecutor,
                    ColumnFamilyStore::shutdownPostFlushExecutor,
                    ColumnFamilyStore::shutdownReclaimExecutor,
                    ColumnFamilyStore::shutdownPerDiskFlushExecutors,
                    PendingRangeCalculatorService.instance::shutdownExecutor,
                    BufferPool::shutdownLocalCleaner,
                    Ref::shutdownReferenceReaper,
                    Memtable.MEMORY_POOL::shutdown,
                    ScheduledExecutors::shutdownAndWait,
                    SSTableReader::shutdownBlocking,
                    () -> shutdownAndWait(ActiveRepairService.repairCommandExecutor)
            );
            error = parallelRun(error, executor,
                                MessagingService.instance()::shutdown
            );
            error = parallelRun(error, executor,
                                StageManager::shutdownAndWait,
                                SharedExecutorPool.SHARED::shutdown
            );

            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            loggerContext.stop();
            Throwables.maybeFail(error);
        }).apply(isolatedExecutor);

        return CompletableFuture.runAsync(ThrowingRunnable.toRunnable(future::get), isolatedExecutor)
                                .thenRun(super::shutdown);
    }

    private static void shutdownAndWait(ExecutorService executor)
    {
        try
        {
            executor.shutdownNow();
            executor.awaitTermination(20, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        assert executor.isTerminated() && executor.isShutdown() : executor;
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
