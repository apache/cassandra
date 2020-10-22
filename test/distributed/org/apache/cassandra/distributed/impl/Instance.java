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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationListener;

import com.google.common.annotations.VisibleForTesting;

import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.SystemKeyspaceMigrator40;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.action.GossipHelper;
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
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.sstable.IndexSummaryManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.DefaultFSErrorHandler;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StartupChecks;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.streaming.StreamReceiveTask;
import org.apache.cassandra.streaming.StreamTransferTask;
import org.apache.cassandra.streaming.async.StreamingInboundHandler;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.Output;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.DiagnosticSnapshotService;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.progress.jmx.JMXBroadcastExecutor;
import org.apache.cassandra.utils.memory.BufferPools;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.impl.DistributedTestSnitch.fromCassandraInetAddressAndPort;
import static org.apache.cassandra.distributed.impl.DistributedTestSnitch.toCassandraInetAddressAndPort;

public class Instance extends IsolatedExecutor implements IInvokableInstance
{
    private static final Map<Class<?>, Function<Object, Object>> mapper = new HashMap<Class<?>, Function<Object, Object>>() {{
        this.put(IInstanceConfig.ParameterizedClass.class, (obj) -> {
            IInstanceConfig.ParameterizedClass pc = (IInstanceConfig.ParameterizedClass) obj;
            return new org.apache.cassandra.config.ParameterizedClass(pc.class_name, pc.parameters);
        });
    }};

    public final IInstanceConfig config;
    private final long startedAt = System.nanoTime();

    // should never be invoked directly, so that it is instantiated on other class loader;
    // only visible for inheritance
    Instance(IInstanceConfig config, ClassLoader classLoader)
    {
        super("node" + config.num(), classLoader);
        this.config = config;
        Object clusterId = Objects.requireNonNull(config.get("dtest.api.cluster_id"), "cluster_id is not defined");
        ClusterIDDefiner.setId("cluster-" + clusterId);
        InstanceIDDefiner.setInstanceId(config.num());
        FBUtilities.setBroadcastInetAddressAndPort(InetAddressAndPort.getByAddressOverrideDefaults(config.broadcastAddress().getAddress(),
                                                                                                   config.broadcastAddress().getPort()));

        // Set the config at instance creation, possibly before startup() has run on all other instances.
        // setMessagingVersions below will call runOnInstance which will instantiate
        // the MessagingService and dependencies preventing later changes to network parameters.
        Config single = loadConfig(config);
        Config.setOverrideLoadConfig(() -> single);

        // Enable streaming inbound handler tracking so they can be closed properly without leaking
        // the blocking IO thread.
        StreamingInboundHandler.trackInboundHandlers();
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
            QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(query);
            ResultMessage result = prepared.statement.executeLocally(QueryProcessor.internalQueryState(),
                                                                     QueryProcessor.makeInternalOptions(prepared.statement, args));
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
        MessagingService.instance().outboundSink.add((message, to) -> {
            InetSocketAddress toAddr = fromCassandraInetAddressAndPort(to);
            cluster.get(toAddr).receiveMessage(serializeMessage(message.from(), to, message));
            return false;
        });
    }

    private void registerInboundFilter(ICluster cluster)
    {
        MessagingService.instance().inboundSink.add(message -> {
            IMessage serialized = serializeMessage(message.from(), toCassandraInetAddressAndPort(broadcastAddress()), message);
            int fromNum = cluster.get(serialized.from()).config().num();
            int toNum = config.num(); // since this instance is reciving the message, to will always be this instance
            return cluster.filters().permitInbound(fromNum, toNum, serialized);
        });
    }

    private void registerOutboundFilter(ICluster cluster)
    {
        MessagingService.instance().outboundSink.add((message, to) -> {
            IMessage serialzied = serializeMessage(message.from(), to, message);
            int fromNum = config.num(); // since this instance is sending the message, from will always be this instance
            int toNum = cluster.get(fromCassandraInetAddressAndPort(to)).config().num();
            return cluster.filters().permitOutbound(fromNum, toNum, serialzied);
        });
    }

    public void uncaughtException(Thread thread, Throwable throwable)
    {
        sync(CassandraDaemon::uncaughtException).accept(thread, throwable);
    }

    private static IMessage serializeMessage(InetAddressAndPort from, InetAddressAndPort to, Message<?> messageOut)
    {
        int fromVersion = MessagingService.instance().versions.get(from);
        int toVersion = MessagingService.instance().versions.get(to);

        // If we're re-serializing a pre-4.0 message for filtering purposes, take into account possible empty payload
        // See CASSANDRA-16207 for details.
        if (fromVersion < MessagingService.current_version &&
            ((messageOut.verb().serializer() == ((IVersionedAsymmetricSerializer) NoPayload.serializer) || messageOut.payload == null)))
        {
            return new MessageImpl(messageOut.verb().id,
                                   ByteArrayUtil.EMPTY_BYTE_ARRAY,
                                   messageOut.id(),
                                   toVersion,
                                   fromCassandraInetAddressAndPort(from));
        }

        try (DataOutputBuffer out = new DataOutputBuffer(1024))
        {
            Message.serializer.serialize(messageOut, out, toVersion);
            byte[] bytes = out.toByteArray();
            if (messageOut.serializedSize(toVersion) != bytes.length)
                throw new AssertionError(String.format("Message serializedSize(%s) does not match what was written with serialize(out, %s) for verb %s and serializer %s; " +
                                                       "expected %s, actual %s", toVersion, toVersion, messageOut.verb(), messageOut.serializer.getClass(),
                                                       messageOut.serializedSize(toVersion), bytes.length));
            return new MessageImpl(messageOut.verb().id, bytes, messageOut.id(), toVersion, fromCassandraInetAddressAndPort(from));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    public static Message<?> deserializeMessage(IMessage message)
    {
        try (DataInputBuffer in = new DataInputBuffer(message.bytes()))
        {
            return Message.serializer.deserialize(in, toCassandraInetAddressAndPort(message.from()), message.version());
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Can not deserialize message " + message, t);
        }
    }

    @Override
    public void receiveMessage(IMessage message)
    {
        sync(() -> {
            if (message.version() > MessagingService.current_version)
            {
                throw new IllegalStateException(String.format("Node%d received message version %d but current version is %d",
                                                              this.config.num(),
                                                              message.version(),
                                                              MessagingService.current_version));
            }

            Message<?> messageIn = deserializeMessage(message);
            Message.Header header = messageIn.header;
            TraceState state = Tracing.instance.initializeFromMessage(header);
            if (state != null) state.trace("{} message received from {}", header.verb, header.from);
            header.verb.stage.execute(() -> MessagingService.instance().inboundSink.accept(messageIn),
                                      ExecutorLocals.create(state));
        }).run();
    }

    public int getMessagingVersion()
    {
        return callsOnInstance(() -> MessagingService.current_version).call();
    }

    @Override
    public void setMessagingVersion(InetSocketAddress endpoint, int version)
    {
        MessagingService.instance().versions.set(toCassandraInetAddressAndPort(endpoint), version);
    }

    @Override
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
                if (config.has(GOSSIP))
                {
                    // TODO: hacky
                    System.setProperty("cassandra.ring_delay_ms", "5000");
                    System.setProperty("cassandra.consistent.rangemovement", "false");
                    System.setProperty("cassandra.consistent.simultaneousmoves.allow", "true");
                }

                mkdirs();

                assert config.networkTopology().contains(config.broadcastAddress()) : String.format("Network topology %s doesn't contain the address %s",
                                                                                                    config.networkTopology(), config.broadcastAddress());
                DistributedTestSnitch.assign(config.networkTopology());

                DatabaseDescriptor.daemonInitialization();
                FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
                DatabaseDescriptor.createAllDirectories();
                CommitLog.instance.start();

                try
                {
                    new StartupChecks().withDefaultTests().verify();
                } catch (StartupException e)
                {
                    throw e;
                }

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

                // Start up virtual table support
                CassandraDaemon.getInstanceForTesting().setupVirtualKeyspaces();

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

                Verb.REQUEST_RSP.unsafeSetSerializer(() -> ReadResponse.serializer);

                if (config.has(NETWORK))
                {
                    MessagingService.instance().listen();
                }
                else
                {
                    // Even though we don't use MessagingService, access the static SocketFactory
                    // instance here so that we start the static event loop state
//                    -- not sure what that means?  SocketFactory.instance.getClass();
                    registerMockMessaging(cluster);
                }
                registerInboundFilter(cluster);
                registerOutboundFilter(cluster);

                JVMStabilityInspector.replaceKiller(new InstanceKiller());

                // TODO: this is more than just gossip
                StorageService.instance.registerDaemon(CassandraDaemon.getInstanceForTesting());
                if (config.has(GOSSIP))
                {
                    MigrationManager.setUptimeFn(() -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
                    StorageService.instance.initServer();
                    StorageService.instance.removeShutdownHook();
                    Gossiper.waitToSettle();
                }
                else
                {
                    cluster.stream().forEach(peer -> {
                        if (cluster instanceof Cluster)
                            GossipHelper.statusToNormal((IInvokableInstance) peer).accept(this);
                        else
                            GossipHelper.unsafeStatusToNormal(this, (IInstance) peer);
                    });

                }

                StorageService.instance.ensureTraceKeyspace();
                SystemKeyspace.finishStartup();

                CassandraDaemon.getInstanceForTesting().setupCompleted();

                if (config.has(NATIVE_PROTOCOL))
                {
                    CassandraDaemon.getInstanceForTesting().initializeClientTransports();
                    CassandraDaemon.getInstanceForTesting().start();
                }

                if (!FBUtilities.getBroadcastAddressAndPort().address.equals(broadcastAddress().getAddress()) ||
                    FBUtilities.getBroadcastAddressAndPort().port != broadcastAddress().getPort())
                    throw new IllegalStateException(String.format("%s != %s", FBUtilities.getBroadcastAddressAndPort(), broadcastAddress()));

                ActiveRepairService.instance.start();
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

    private Config loadConfig(IInstanceConfig overrides)
    {
        Config config = new Config();
        overrides.propagate(config, mapper);
        return config;
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
                                StreamingInboundHandler::shutdown,
                                () -> StreamReceiveTask.shutdownAndWait(1L, MINUTES),
                                () -> StreamTransferTask.shutdownAndWait(1L, MINUTES),
                                () -> SecondaryIndexManager.shutdownAndWait(1L, MINUTES),
                                () -> IndexSummaryManager.instance.shutdownAndWait(1L, MINUTES),
                                () -> ColumnFamilyStore.shutdownExecutorsAndWait(1L, MINUTES),
                                () -> PendingRangeCalculatorService.instance.shutdownAndWait(1L, MINUTES),
                                () -> BufferPools.shutdownLocalCleaner(1L, MINUTES),
                                () -> Ref.shutdownReferenceReaper(1L, MINUTES),
                                () -> Memtable.MEMORY_POOL.shutdownAndWait(1L, MINUTES),
                                () -> DiagnosticSnapshotService.instance.shutdownAndWait(1L, MINUTES),
                                () -> ScheduledExecutors.shutdownAndWait(1L, MINUTES),
                                () -> SSTableReader.shutdownBlocking(1L, MINUTES),
                                () -> shutdownAndWait(Collections.singletonList(ActiveRepairService.repairCommandExecutor())),
                                () -> ScheduledExecutors.shutdownAndWait(1L, MINUTES)
            );

            error = parallelRun(error, executor,
                                CommitLog.instance::shutdownBlocking,
                                // can only shutdown message once, so if the test shutsdown an instance, then ignore the failure
                                (IgnoreThrowingRunnable) () -> MessagingService.instance().shutdown(1L, MINUTES, false, true)
            );
            error = parallelRun(error, executor,
                                () -> GlobalEventExecutor.INSTANCE.awaitInactivity(1l, MINUTES),
                                () -> Stage.shutdownAndWait(1L, MINUTES),
                                () -> SharedExecutorPool.SHARED.shutdownAndWait(1L, MINUTES)
            );
            error = parallelRun(error, executor,
                                () -> shutdownAndWait(Collections.singletonList(JMXBroadcastExecutor.executor))
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

        public List<Notification> getNotifications()
        {
            return new ArrayList<>(notifications.notifications);
        }

        public Throwable getLatestError()
        {
            return latestError;
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
