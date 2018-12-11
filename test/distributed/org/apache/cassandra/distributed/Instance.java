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

package org.apache.cassandra.distributed;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleSeedProvider;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageDeliveryTask;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.MessageInHandler;
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

public class Instance extends InvokableInstance
{
    public final InstanceConfig config;

    public Instance(InstanceConfig config, ClassLoader classLoader)
    {
        super(classLoader);
        this.config = config;
    }

    public InetAddressAndPort getBroadcastAddress() { return callOnInstance(FBUtilities::getBroadcastAddressAndPort); }

    public Object[][] executeInternal(String query, Object... args)
    {
        return callOnInstance(() ->
        {
            QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(query);
            ResultMessage result = prepared.statement.executeLocally(QueryProcessor.internalQueryState(),
                    QueryProcessor.makeInternalOptions(prepared.statement, args));

            if (result instanceof ResultMessage.Rows)
                return RowUtil.toObjects((ResultMessage.Rows)result);
            else
                return null;
        });
    }

    public UUID getSchemaVersion()
    {
        // we do not use method reference syntax here, because we need to invoke on the node-local schema instance
        //noinspection Convert2MethodRef
        return callOnInstance(() -> Schema.instance.getVersion());
    }

    public void schemaChange(String query)
    {
        runOnInstance(() ->
        {
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
        });
    }

    private void registerMockMessaging(TestCluster cluster)
    {
        BiConsumer<InetAddressAndPort, Message> deliverToInstance = (to, message) -> cluster.get(to).receiveMessage(message);
        BiConsumer<InetAddressAndPort, Message> deliverToInstanceIfNotFiltered = cluster.filters().filter(deliverToInstance);

        acceptsOnInstance((BiConsumer<InetAddressAndPort, Message> deliver) ->
                MessagingService.instance().addMessageSink(new MessageDeliverySink(deliver))
        ).accept(deliverToInstanceIfNotFiltered);
    }

    private static class MessageDeliverySink implements IMessageSink
    {
        private final BiConsumer<InetAddressAndPort, Message> deliver;
        MessageDeliverySink(BiConsumer<InetAddressAndPort, Message> deliver)
        {
            this.deliver = deliver;
        }

        public boolean allowOutgoingMessage(MessageOut messageOut, int id, InetAddressAndPort to)
        {
            try (DataOutputBuffer out = new DataOutputBuffer(1024))
            {
                InetAddressAndPort from = FBUtilities.getBroadcastAddressAndPort();
                messageOut.serialize(out, MessagingService.current_version);
                deliver.accept(to, new Message(messageOut.verb.getId(), out.toByteArray(), id, MessagingService.current_version, from));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return false;
        }

        public boolean allowIncomingMessage(MessageIn message, int id)
        {
            // we can filter to our heart's content on the outgoing message; no need to worry about incoming
            return true;
        }
    }

    private void receiveMessage(Message message)
    {
        acceptsOnInstance((Message m) ->
        {
            try (DataInputBuffer in = new DataInputBuffer(m.bytes))
            {
                MessageIn<?> messageIn = MessageInHandler.deserialize(in, m.id, m.version, m.from);
                Runnable deliver = new MessageDeliveryTask(messageIn, m.id);
                deliver.run();
            }
            catch (Throwable t)
            {
                throw new RuntimeException("Exception occurred on the node " + FBUtilities.getBroadcastAddressAndPort(), t);
            }

        }).accept(message);
    }

    void launch(TestCluster cluster)
    {
        try
        {
            mkdirs();
            int id = config.num;
            runOnInstance(() -> InstanceIDDefiner.instanceId = id); // for logging

            startup();
            initializeRing(cluster);
            registerMockMessaging(cluster);
        }
        catch (Throwable t)
        {
            if (t instanceof RuntimeException)
                throw (RuntimeException) t;
            throw new RuntimeException(t);
        }
    }

    private void mkdirs()
    {
        new File(config.saved_caches_directory).mkdirs();
        new File(config.hints_directory).mkdirs();
        new File(config.commitlog_directory).mkdirs();
        for (String dir : config.data_file_directories)
            new File(dir).mkdirs();
    }

    private void startup()
    {
        acceptsOnInstance((InstanceConfig config) ->
        {
            DatabaseDescriptor.daemonInitialization(() -> loadConfig(config));

            DatabaseDescriptor.createAllDirectories();
            Keyspace.setInitialized();
            SystemKeyspace.persistLocalMetadata();
        }).accept(config);
    }


    public static Config loadConfig(InstanceConfig overrides)
    {
        Config config = new Config();
        // Defaults
        config.commitlog_sync = Config.CommitLogSync.batch;
        config.endpoint_snitch = SimpleSnitch.class.getName();
        config.seed_provider = new ParameterizedClass(SimpleSeedProvider.class.getName(),
                                                      Collections.singletonMap("seeds", "127.0.0.1:7010"));
        config.diagnostic_events_enabled = true; // necessary for schema change monitoring

        // Overrides
        config.partitioner = overrides.partitioner;
        config.broadcast_address = overrides.broadcast_address;
        config.listen_address = overrides.listen_address;
        config.broadcast_rpc_address = overrides.broadcast_rpc_address;
        config.rpc_address = overrides.rpc_address;
        config.saved_caches_directory = overrides.saved_caches_directory;
        config.data_file_directories = overrides.data_file_directories;
        config.commitlog_directory = overrides.commitlog_directory;
        config.hints_directory = overrides.hints_directory;
        config.cdc_raw_directory = overrides.cdc_directory;
        config.concurrent_writes = overrides.concurrent_writes;
        config.concurrent_counter_writes = overrides.concurrent_counter_writes;
        config.concurrent_materialized_view_writes = overrides.concurrent_materialized_view_writes;
        config.concurrent_reads = overrides.concurrent_reads;
        config.memtable_flush_writers = overrides.memtable_flush_writers;
        config.concurrent_compactors = overrides.concurrent_compactors;
        config.memtable_heap_space_in_mb = overrides.memtable_heap_space_in_mb;
        config.initial_token = overrides.initial_token;
        return config;
    }

    private void initializeRing(TestCluster cluster)
    {
        // This should be done outside instance in order to avoid serializing config
        String partitionerName = config.partitioner;
        List<String> initialTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();
        for (int i = 1 ; i <= cluster.size() ; ++i)
        {
            InstanceConfig config = cluster.get(i).config;
            initialTokens.add(config.initial_token);
            try
            {
                hosts.add(InetAddressAndPort.getByName(config.broadcast_address));
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
            hostIds.add(config.hostId);
        }

        runOnInstance(() ->
        {
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
                    Gossiper.instance.initializeNodeUnsafe(ep, hostIds.get(i), 1);
                    Gossiper.instance.injectApplicationState(ep,
                            ApplicationState.TOKENS,
                            new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(tokens.get(i))));
                    storageService.onChange(ep,
                            ApplicationState.STATUS_WITH_PORT,
                            new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(tokens.get(i))));
                    storageService.onChange(ep,
                            ApplicationState.STATUS,
                            new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(tokens.get(i))));
                    Gossiper.instance.realMarkAlive(ep, Gossiper.instance.getEndpointStateForEndpoint(ep));
                    MessagingService.instance().setVersion(ep, MessagingService.current_version);
                }

                // check that all nodes are in token metadata
                for (int i = 0; i < tokens.size(); ++i)
                    assert storageService.getTokenMetadata().isMember(hosts.get(i));
            }
            catch (Throwable e) // UnknownHostException
            {
                throw new RuntimeException(e);
            }
        });
    }

    void shutdown()
    {
        runOnInstance(() -> {
            Throwable error = null;
            error = runAndMergeThrowable(error,
                    BatchlogManager.instance::shutdown,
                    HintsService.instance::shutdownBlocking,
                    CommitLog.instance::shutdownBlocking,
                    CompactionManager.instance::forceShutdown,
                    Gossiper.instance::stop,
                    SecondaryIndexManager::shutdownExecutors,
                    MessagingService.instance()::shutdown,
                    ColumnFamilyStore::shutdownFlushExecutor,
                    ColumnFamilyStore::shutdownPostFlushExecutor,
                    ColumnFamilyStore::shutdownReclaimExecutor,
                    ColumnFamilyStore::shutdownPerDiskFlushExecutors,
                    PendingRangeCalculatorService.instance::shutdownExecutor,
                    BufferPool::shutdownLocalCleaner,
                    Ref::shutdownReferenceReaper,
                    StageManager::shutdownAndWait,
                    SharedExecutorPool.SHARED::shutdown,
                    Memtable.MEMORY_POOL::shutdown,
                    ScheduledExecutors::shutdownAndWait);
            error = shutdownAndWait(error, ActiveRepairService.repairCommandExecutor);
            Throwables.maybeFail(error);
        });
    }

    private static Throwable shutdownAndWait(Throwable existing, ExecutorService executor)
    {
        return runAndMergeThrowable(existing, () -> {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
            assert executor.isTerminated() && executor.isShutdown() : executor;
        });
    }

    private static Throwable runAndMergeThrowable(Throwable existing, ThrowingRunnable runnable)
    {
        try
        {
            runnable.run();
        }
        catch (Throwable t)
        {
            return Throwables.merge(existing, t);
        }

        return existing;
    }

    private static Throwable runAndMergeThrowable(Throwable existing, ThrowingRunnable ... runnables)
    {
        for (ThrowingRunnable runnable : runnables)
        {
            try
            {
                runnable.run();
            }
            catch (Throwable t)
            {
                existing = Throwables.merge(existing, t);
            }
        }
        return existing;
    }

    public static interface ThrowingRunnable
    {
        public void run() throws Throwable;
    }
}
